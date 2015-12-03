#include <fstream>
#include <random>
#include <sstream>
#include <iostream>
#include <array>
#include <functional>

#include <cassert>
#include <cstddef>

#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

using namespace std;

#define MB  (1UL<<20)
#define GB  (1UL<<30)

#define PAGE_SHIFT  12
#define PAGE_MASK   ((1UL<<PAGE_SHIFT)-1)

typedef struct { int len; uintptr_t addr; } loc;

static inline void clear_line(void)
{
    for (int i = 0; i < 128; i++)
        printf("\b");
    fflush(stdout);
}

// returns bytes
long get_rss(void)
{
    long rss;
    string line;
    const int field = 24; // RSS
    array<char, 512> cline;
    pid_t pid = getpid();
    stringstream input;
    const char *val = NULL;
    input << "/proc/" << pid << "/stat";
    ifstream ifs(input.str());
    getline(ifs, line);
    memcpy(cline.data(), line.c_str(), min(line.size(),cline.size()));
    strtok(cline.data(), " ");
    for (int f = 0; f < field - 1; f++)
        val = strtok(NULL, " ");
    sscanf(val, "%ld", &rss);
    return (rss<<12);
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

static inline
void *map_remap(void* oldp, size_t oldb, size_t newb)
{
    void *newp = mremap(oldp, oldb, newb, MREMAP_MAYMOVE, -1, 0);
    if (newp == MAP_FAILED) {
        fprintf(stderr, "mremap: oldb = %lu newb = %lu\n",
                oldb, newb);
        perror("mremap");
        return NULL;
    }
    return newp;
}

class LiveSet
{
    public:
        LiveSet(long bytes)
            : max_live(bytes), live(0L),
            last_idx(0), drop_misses(0),
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

        typedef function<long (void)> numGen_f;

        void injectValues(long injectSz, numGen_f genf) {
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
                objsize = genf();
                assert( !locs[idx] );
                assert( locs[idx] = (uintptr_t)malloc(objsize) );
                sizes[idx] = objsize;
                live += objsize;
                injectSz -= objsize;
                nobjs++;
                drop(objsize<<2);
                // print progress
                if (!(nn++%100000L)) {
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
                idx = dropRand(gen) % last_idx; // should hit most of the time
                if (locs[idx]) {
                    free((void*)locs[idx]);
                    live -= sizes[idx];
                    if (fltop >= flmax)
                        throw runtime_error("Exceeded freelist");
                    freelist[fltop++] = idx;
                    nobjs--;
                    locs[idx] = sizes[idx] = 0;
                } else {
                    drop_misses++;
                }
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

        static inline size_t ceil4K(size_t val) {
            long pgs = (val>>PAGE_SHIFT);
            pgs += (val & PAGE_MASK) ? 1 : 0;
            return (pgs<<PAGE_SHIFT);
        }

        size_t overhead() {
            return ceil4K(nlocs*sizeof(*locs))
                + ceil4K(nlocs*sizeof(*sizes))
                + ceil4K(sizeof(LiveSet));
        }

        // Class variables

        long max_live, live;    // working set max and current, bytes
        long nobjs;             // pointers allocated
        long last_idx, drop_misses;

        constexpr static long nlocs = ((long)200e6);    // quantity of pointers we track
        uintptr_t *locs;        // array with nlocs items
        uint32_t *sizes;        // array with nlocs items

    private:
        constexpr static off_t flmax = nlocs>>2;
        array<off_t, flmax> freelist;
        off_t fltop;

        random_device rd;
        mt19937 gen;
        uniform_int_distribution<long> dropRand;

        LiveSet();
};

//
// Workloads from FAST'14 paper.
//
auto W1before = [] () { return 100L; };
auto W2before = W1before;
auto W3before = W1before;
auto W4before = [] () {
    const long m1 = 100, m2 = 150;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W5before = W4before;
auto W6before = [] () {
    const long m1 = 100, m2 = 250;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W7before = [] () {
    const long m1 = 1000, m2 = 2000;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W8before = [] () {
    const long m1 = 50, m2 = 150;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};

auto W2after  = [] () { return 130L; };
auto W3after  = W2after;
auto W4after  = [] () {
    const long m1 = 200, m2 = 250;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W5after  = W4after;
auto W6after  = W7before;
auto W7after  = [] () {
    const long m1 = 1500, m2 = 2500;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};
auto W8after  = [] () {
    const long m1 = 5000, m2 = 15000;
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<long> d(m1, m2);
    return d(gen);
};

void dumpstats(const char *name, LiveSet *liveset)
{
    float rss = get_rss() - liveset->overhead();
    float eff = rss/liveset->live;
    printf("%16s %16s %12.3f %12.5f\n",
            name, "W1", rss/MB, eff);
}

// modeled after synthetic workload from: Rumble et al. FAST'14
int testlive(const char *name, long livewss, long injectwss)
{
    // must allocate liveset on heap because freelist array is huge
    LiveSet *liveset(nullptr);
    assert( liveset = new LiveSet(livewss) );
    printf("# size of LiveSet: %lf MiB\n",
            (float)sizeof(LiveSet)/(1<<20));
    printf("# overhead of LiveSet: %lf MiB\n",
            (float)liveset->overhead()/(1<<20));

    printf("cmd testname-sz rss-MB eff\n");

    liveset->injectValues(injectwss, W2before);
    liveset->injectValues(injectwss, W2after);
    dumpstats(name, liveset);
    liveset->freeAll();

    liveset->injectValues(injectwss, W2before);
    liveset->injectValues(injectwss, W2after);
    dumpstats(name, liveset);
    liveset->freeAll();

    liveset->injectValues(injectwss, W2before);
    liveset->injectValues(injectwss, W2after);
    dumpstats(name, liveset);
    liveset->freeAll();

    delete liveset;
    return 0;
}

int main(int narg, char *args[])
{
    if (mlockall(MCL_FUTURE) || mlockall(MCL_CURRENT)) {
        perror("mlockall");
        return 1;
    }
    long live_wss = 1UL<<29;
    long inject_wss = 1UL<<31;
    return testlive(args[0], live_wss, inject_wss);
}
