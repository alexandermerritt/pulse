/*
 * Copyright (C) 2015-2016 Alexander Merritt, merritt.alex@gatech.edu
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Synthetic workload generation for testing malloc implementations.
 */

#include <fstream>
#include <random>
#include <sstream>
#include <iostream>
#include <array>
#include <deque>
#include <functional>

#include <cassert>
#include <cstddef>

#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <string.h>

using namespace std;

#define MB  (1UL<<20)
#define GB  (1UL<<30)

#define PAGE_SHIFT  12
#define PAGE_MASK   ((1UL<<PAGE_SHIFT)-1)

static inline long ceil4K(long bytes)
{
    long pgs = bytes >> PAGE_SHIFT;
    pgs += !!(bytes & PAGE_MASK);
    return (pgs << PAGE_SHIFT);
}

static inline void clear_line(void)
{
    for (int i = 0; i < 128; i++)
        printf("\b");
    fflush(stdout);
}

typedef enum {
    STAT_VMSIZE = 23,
    STAT_RSS = 24,
} statfield_t;

// getstat(STAT_VMSIZE)
// getstat(STAT_RSS, 12)        because RSS is reported in pages
long getstat(statfield_t t, size_t shift = 0UL)
{
    long value;
    string line;
    array<char, 512> cline;
    pid_t pid = getpid();
    stringstream input;
    const char *val = NULL;
    input << "/proc/" << pid << "/stat";
    ifstream ifs(input.str());
    getline(ifs, line);
    memcpy(cline.data(), line.c_str(), min(line.size(),cline.size()));
    strtok(cline.data(), " ");
    for (int f = 0; f < t - 1; f++)
        val = strtok(NULL, " ");
    sscanf(val, "%ld", &value);
    return (value<<shift);
}

static inline
void *map_alloc(size_t bytes)
{
    void *v = mmap(NULL, bytes, PROT_READ|PROT_WRITE,
            MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (v == MAP_FAILED) {
        fprintf(stderr, "mmap: bytes = %lu\n", bytes);
        perror("mmap");
        return NULL;
    }
    memset(v, 0, bytes);
    return v;
}

class LiveSet
{
    public:
        LiveSet(long bytes)
            : max_live(bytes), live(0L),
            last_idx(0),
            locs(nullptr), sizes(nullptr),
            fltop(0L), gen(rd()), dropRand(0, nlocs-1) {
            if (bytes < 1)
                throw runtime_error("LiveSet with empty set?");
            locs = (uintptr_t*)map_alloc(nlocs*sizeof(*locs));
            sizes = (uint32_t*)map_alloc(nlocs*sizeof(*sizes));
            assert( locs && sizes );
            freelist.fill(-1);
        }

        ~LiveSet() {
            freeAll();
            munmap(locs, nlocs*sizeof(*locs));
            munmap(sizes, nlocs*sizeof(*sizes));
        }

        // long func(void)
        typedef function<long (void)> numGen_f;

        void injectValues(numGen_f genf, long injectSz) {
            off_t idx, nn(0L), objsize, total(injectSz);
            if (injectSz < 1)
                return;
            drop(genf()<<2);
            while (injectSz > 0) {
                if (fltop == 0) {
                    if ((idx = ++last_idx) >= nlocs)
                        throw runtime_error("Exceeded nlocs");
                } else {
                    idx = freelist[--fltop];
                    assert( idx > -1 );
                }
                try { objsize = genf(); }
                // throw this in your lambda to prematurely exit
                catch (out_of_range &e) { break; }
                assert( !locs[idx] );
                assert( locs[idx] = (uintptr_t)malloc(objsize) );
                sizes[idx] = objsize;
                live += objsize;
                injectSz -= objsize;
                nobjs++;
                drop(objsize<<2);
                // print progress
                if (!(nn++%10000L)) {
                    clear_line(); printf("# %3.3lf %%  ",
                        100.*(float)(total-injectSz)/total);
                    fflush(stdout);
                }
            }
            printf("\n");
        }

        // free objects at random until we are below threshold
        void drop(off_t atleast = 0UL) {
            if (live <= (max_live - atleast))
                return;
            long idx;
            while (live > (max_live - atleast)) {
                idx = dropRand(gen) % last_idx;
                if (!locs[idx])
                    continue;
                free((void*)locs[idx]);
                live -= sizes[idx];
                if (fltop >= flmax)
                    throw runtime_error("Exceeded freelist");
                freelist[fltop++] = idx;
                nobjs--;
                locs[idx] = sizes[idx] = 0;
            }
        }

        void freeAll() {
            for (long n = 0; n < nlocs; n++) {
                if (locs[n]) {
                    free((void*)locs[n]);
                    locs[n] = sizes[n] = 0;
                }
            }
            freelist.fill(-1);
            live = 0L;
            nobjs = 0L;
            fltop = 0L;
            last_idx = 0L;
        }

        size_t overhead() {
            return ceil4K(nlocs*sizeof(*locs))
                + ceil4K(nlocs*sizeof(*sizes))
                + ceil4K(sizeof(LiveSet));
        }

        // Class variables

        long max_live, live;    // working set max and current, bytes
        long nobjs;             // pointers allocated
        long last_idx;          // bound in locs for locating pointers

        // quantity of pointers we track
        constexpr static long nlocs = ((long)200e6);
        uintptr_t *locs; // addresses of allocated objects
        uint32_t *sizes; // sizes of allocated objects

    private:
        constexpr static off_t flmax = nlocs;
        array<off_t, flmax> freelist; // indexes of free slots in locs
        off_t fltop;

        random_device rd;
        mt19937 gen;
        uniform_int_distribution<long> dropRand;

        LiveSet();
};

//
// Modeled after synthetic workload from: Rumble et al. FAST'14
//
static random_device rd;
static mt19937 gen(rd());
auto W1before = [] () { return 100L; };
auto W2before = W1before;
auto W3before = W1before;
auto W4before = [] () {
    const long m1 = 100, m2 = 150;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W5before = W4before;
auto W6before = [] () {
    const long m1 = 100, m2 = 250;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W7before = [] () {
    const long m1 = 1000, m2 = 2000;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W8before = [] () {
    const long m1 = 50, m2 = 150;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};

auto W2after  = [] () { return 130L; };
auto W3after  = W2after;
auto W4after  = [] () {
    const long m1 = 200, m2 = 250;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W5after  = W4after;
auto W6after  = W7before;
auto W7after  = [] () {
    const long m1 = 1500, m2 = 2500;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W8after  = [] () {
    const long m1 = 5000, m2 = 15000;
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};

void dumpstats(const char *prog, const char *test,
        long injectwss, LiveSet *liveset)
{
    float mem = (float)getstat(STAT_VMSIZE);
    float wss = mem - liveset->overhead();
    float eff = wss/liveset->live;
    printf("prog cmd live inject mem wss eff LSoverhead nobjs\n");
    printf("%s %s %.2lf %.2lf %.2lf %.2lf %.4lf %.2lf %ld\n",
            prog, test,
            (float)liveset->live/MB,
            (float)injectwss/MB,
            mem/MB, wss/MB, eff,
            (float)liveset->overhead()/MB, liveset->nobjs);
}

void sizes_on_stdin(deque<long> &values)
{
    string line;
    long s;
    while (cin >> line) {
        s = strtoll(line.c_str(), NULL, 10);
        values.push_back(s);
    }
}

int main(int narg, char *args[])
{
    if (narg != 4) {
        cerr << "Usage: " << *args
            << " command live_wss inject_wss"
            << endl << "\t(wss specified in MiB)"
            << endl;
        exit(1);
    }

    string cmd(args[1]);
    long livewss = strtoll(args[2], NULL, 10) * MB;
    long injectwss = strtoll(args[3], NULL, 10) * MB;

    // Must allocate liveset on heap because freelist array is huge.
    // Additionally, do this outside of the purview of malloc.
    LiveSet *liveset = 
        new (map_alloc(sizeof(LiveSet))) LiveSet(livewss);

    if (cmd == "w1") {
        liveset->injectValues(W1before, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w2") {
        liveset->injectValues(W2before, injectwss);
        liveset->injectValues(W2after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w3") {
        liveset->injectValues(W3before, injectwss);
        liveset->drop(livewss * 0.9);
        liveset->injectValues(W3after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w4") {
        liveset->injectValues(W4before, injectwss);
        liveset->injectValues(W4after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w5") {
        liveset->injectValues(W5before, injectwss);
        liveset->drop(livewss * 0.9);
        liveset->injectValues(W5after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w6") {
        liveset->injectValues(W6before, injectwss);
        liveset->drop(livewss * 0.5);
        liveset->injectValues(W6after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w7") {
        liveset->injectValues(W7before, injectwss);
        liveset->drop(livewss * 0.9);
        liveset->injectValues(W7after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "w8") {
        liveset->injectValues(W8before, injectwss);
        liveset->drop(livewss * 0.9);
        liveset->injectValues(W8after, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else if (cmd == "stdin") {
        static deque<long> values;
        sizes_on_stdin(values);
        LiveSet::numGen_f vfn = [&] () -> long {
            if (values.empty()) throw out_of_range("");
            long v = values.front();
            values.pop_front();
            return v;
        };
        liveset->injectValues(vfn, injectwss);
        dumpstats(*args, args[1], injectwss, liveset);
    } else {
        cerr << "Unknown workload to run." << endl;
        exit(1);
    }

    return 0;
}

