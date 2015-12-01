#include <fstream>
#include <cassert>
#include <random>
#include <sstream>
#include <iostream>
#include <array>
#include <deque>

#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include "bitops.h"

using namespace std;

#define MB  (1UL<<20)
#define GB  (1UL<<30)

#define PAGE_SHIFT  12
#define PAGE_MASK   ((1<<PAGE_SHIFT)-1)

typedef struct { int len; uintptr_t addr; } loc;

static inline void clear_line(void)
{
    for (int i = 0; i < 64; i++)
        printf("\b");
}

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

int testinput(int narg, char *args[])
{
    string line;
    struct timespec clk1, clk2;

    if (narg != 2)
        return 1;

    size_t total_sz(0);
    size_t max_vals, nvals(0);

    size_t locs_sz(0);
    loc *locs(nullptr);

    size_t len, accum(0);

    for (int i = 0; i < 2; i++) {
        if (i == 1) {
            locs_sz = max_vals * sizeof(loc);
            assert(locs_sz > 0);
            locs = (loc*)map_alloc(locs_sz);
            assert(locs);
        }
        string input(args[1]);
        ifstream ifs(input);
        while (getline(ifs, line)) {
            nvals++;
            if (i == 0)
                continue;
            stringstream ss(line);
            ss >> len;
            total_sz += len;
            locs[nvals-1].len = len;
        }
        ifs.close();
        if (i == 0)
            max_vals = nvals;
        nvals = 0;
    }

    long every = max_vals / 10.;
    clock_gettime(CLOCK_REALTIME, &clk1);
    for (nvals = 0; nvals < max_vals; nvals++) {
        len = locs[nvals].len;
        locs[nvals].addr = (uintptr_t)malloc(len);
        assert(locs[nvals].addr);
        memset((void*)locs[nvals].addr, 0, len);
        if (nvals % every == 0) {
            clear_line();
            printf("# %2.1f %%", 100.f * accum / total_sz);
            fflush(stdout);
        }
        if ((accum += locs[nvals].len) > total_sz)
            break;
    }
    clock_gettime(CLOCK_REALTIME, &clk2);
    printf("\n");

    long d = (clk2.tv_sec * 1e9 + clk2.tv_nsec)
        - (clk1.tv_sec * 1e9 + clk1.tv_nsec);

    long rss = get_rss(), actual = rss-locs_sz;
    cout << "total rss overhead msec" << endl;
    cout << total_sz;
    cout << " " << actual;
    cout << " " << (static_cast<float>(actual)/total_sz);
    cout << " " << (float)d/1e6;
    cout << endl;

    return 0;
}

int testdist(int narg, char *args[])
{
    random_device rd;
    mt19937 gen(rd());
    normal_distribution<float> d1(15000, 1000);
    //normal_distribution<float> d2(15000, 1000);
    struct timespec clk1, clk2;

    const size_t total_sz = 1UL<<33;
    size_t max_vals = 2*(total_sz / 15000), nvals = 0;

    size_t locs_sz = max_vals * sizeof(loc);
    loc *locs = (loc*)map_alloc(locs_sz);
    assert(locs);

    size_t accum(0);
    //printf("total_sz %lu\n", total_sz);

    clock_gettime(CLOCK_REALTIME, &clk1);
    for (nvals = 0; nvals < max_vals; nvals++) {
        locs[nvals].len  = static_cast<int>( d1(gen) );
        locs[nvals].addr = (uintptr_t)malloc( locs[nvals].len );
        assert(locs[nvals].addr);
        memset((void*)locs[nvals].addr, 0, locs[nvals].len);
        if (nvals % 1000 == 0) {
            clear_line();
            printf("# %2.1f %%", 100.f * accum / total_sz);
        }
        if ((accum += locs[nvals].len) > total_sz)
            break;
    }
    clock_gettime(CLOCK_REALTIME, &clk2);
    printf("\n");

    long d = (clk2.tv_sec * 1e9 + clk2.tv_nsec)
        - (clk1.tv_sec * 1e9 + clk1.tv_nsec);

    long rss = get_rss(), actual = rss-locs_sz;
    cout << "total rss overhead msec" << endl;
    cout << total_sz;
    cout << " " << actual;
    cout << " " << (static_cast<float>(actual)/total_sz);
    cout << " " << (float)d/1e6;
    cout << endl;

    return 0;
}

class LiveSet
{
    public:

        LiveSet(long bytes) : max_live(bytes), live(0L),
                            locs(nullptr), sizes(nullptr),
                            gen(rd()) {
            if (bytes < 1)
                throw runtime_error("LiveSet with empty set?");
            nlocs = static_cast<long>(200*1e6);
            locs = (uintptr_t*)map_alloc(nlocs*sizeof(*locs));
            sizes = (uint32_t*)map_alloc(nlocs*sizeof(*sizes));
            //alloc_mask = bitops_alloc(nlocs);
            //assert( locs && sizes && alloc_mask );
            assert( locs && sizes );
        }

        ~LiveSet() {
            freeAll();
            munmap(locs, nlocs*sizeof(*locs));
            munmap(sizes, nlocs*sizeof(*sizes));
            //bitops_free(alloc_mask);
        }

        void fillFixed(uint32_t objsize) {
            // if object size too small, we may need more slots to
            // reach max_live
            if ((nlocs * objsize) < max_live)
                resize(1.2*max_live/objsize);
            for (long n = 0; n < nlocs; n++) {
                assert( locs[n] = (uintptr_t)malloc(objsize) );
                sizes[n] = objsize;
                //bitops_set(alloc_mask, nlocs, n);
                live += objsize;
                if (live >= max_live)
                    break;
            }
        }

        // add new objects. if we exceed our allowance, delete random
        // objects until we are within allowance again.
        void injectFixed(long n, uint32_t objsize) {
            long idx;
            if (n == 0)
                return;
            drop();
            for ( ; n > 0; n--) {
                // if we cannot find an unused slot, increase our ptr
                // array to accomodate more (drop ensures we stay
                // within max_live)
                while ((idx = findNext()) < 0)
                    resize(1.2*max_live/objsize);
                assert( locs[idx] = (uintptr_t)malloc(objsize) );
                sizes[idx] = objsize;
                //bitops_set(alloc_mask, nlocs, idx);
                live += objsize;
                drop();
            }
        }

        void addDist(long mean, long std) {
            // TODO
            // random_device rd;
            // mt19937 gen(rd());
            // uniform_int_distribution<long> d(mean, std);
        }

        void freeAll() {
            for (long n = 0; n < nlocs; n++) {
                if (locs[n]) {
                    free((void*)locs[n]);
                    locs[n] = sizes[n] = 0;
                }
            }
            live = 0L;
            //bitops_setall(alloc_mask, nlocs, 0);
        }

        static inline size_t ceil4K(size_t val) {
            long pgs = (val>>PAGE_SHIFT);
            pgs += (val & PAGE_MASK) ? 1 : 0;
            return (pgs<<PAGE_SHIFT);
        }

        size_t overhead() {
            return ceil4K(nlocs*sizeof(*locs))
                + ceil4K(nlocs*sizeof(*sizes))
                + ceil4K(nlocs*sizeof(bitchunk_t))
                + ceil4K(sizeof(LiveSet));
        }

    private:
        long max_live, live;    // working set max and current, bytes

        long nlocs;             // quantity of pointers we track
        uintptr_t *locs;        // array with nlocs items
        uint32_t *sizes;        // array with nlocs items
        bitset_t alloc_mask;    // bitmask with nlocs bits

        random_device rd;
        mt19937 gen;

        LiveSet();

        long findNext() {

            for (long n = 0; n < nlocs; n++)
                if (!locs[n])
                    return n;
            return -1;
        }

        void resize(long nlocs_) {
            if (nlocs_ <= nlocs)
                return;
            void *p = (uintptr_t*)map_remap(locs,
                    nlocs*sizeof(*locs), nlocs_*sizeof(*locs));
            if (!p) throw runtime_error("remap failed");
            locs = (uintptr_t*)p;
            p = (uintptr_t*)map_remap(sizes,
                    nlocs*sizeof(*sizes), nlocs_*sizeof(*sizes));
            if (!p) throw runtime_error("remap failed");
            sizes = (uint32_t*)p;
            nlocs = nlocs_;
        }

        // free objects at random until we are below max
        void drop() {
            if (live < max_live)
                return;
            uniform_int_distribution<long> dist(0, nlocs-1);
            long idx;
            while (max_live >= live) {
                idx = dist(gen); // FIXME make this hit each time
                if (locs[idx]) {
                    free((void*)locs[idx]);
                    live -= sizes[idx];
                    locs[idx] = sizes[idx] = 0;
                }
            }
        }
};

// modeled after synthetic workload from: Rumble et al. FAST'14
int testlive(int narg, char *args[])
{
    float rss, eff;

    LiveSet *liveset(nullptr);
    const long live = 10L<<30;
    const long wss = 50L*(1L<<30);
    const uint32_t objsize = 100, objsize2 = 130;

    assert( liveset = new LiveSet(live) );

    // FIXME how to subtract out ELF segments, globals, SO objects?
    // supermalloc performs a huge heap alloc during lib init

#if 0
    cout << "# fillFixed " << objsize << endl;
    liveset->fillFixed(objsize);
    rss = get_rss() - liveset->overhead();
    eff = rss/live;
    printf("%16s %16s-%04d %12.3f %12.5f\n",
            args[0], "fillFixed", objsize, rss/MB, eff);

    cout << "# freeing all" << endl;
    liveset->freeAll();
#endif

    cout << "# injectFixed " << objsize << endl;
    liveset->injectFixed(wss/objsize, objsize);
    rss = get_rss() - liveset->overhead();
    eff = rss/live;
    printf("%16s %16s-%04d %12.3f %12.5f\n",
            args[0], "injectFixed", objsize, rss/MB, eff);

    cout << "# injectFixed " << objsize2 << endl;
    liveset->injectFixed(wss/objsize2, objsize2);
    rss = get_rss() - liveset->overhead();
    eff = rss/live;
    printf("%16s %16s-%04d %12.3f %12.5f\n",
            args[0], "injectFixed", objsize2, rss/MB, eff);

    delete liveset;

    return 0;
}

void printbits(bitset_t *set, off_t maxc)
{
    for (off_t i = 0; i < maxc && i < (set->nbits>>6); i++)
        printf("%016lx ", set->map[i]);
    printf("\n");
}

void testbitops()
{
    bitset_t set;
    off_t maxc = 9, pos;

    assert( !bitops_alloc(&set, 1) );

    printbits(&set, maxc);

    bitops_set(&set, 1);
    bitops_set(&set, 2);
    bitops_set(&set, 3);

    printbits(&set, maxc);

    printf("\n-----\n");

    bitops_setall(&set, 1);
    printbits(&set, maxc);
    bitops_setall(&set, 0);
    printbits(&set, maxc);

    printf("\n-----\n");

    bitops_set(&set, 513);
    printbits(&set, maxc);
    pos = bitops_ffset(&set);
    printf("pos %ld\n", pos);

    bitops_clear(&set, 513);
    printbits(&set, maxc);
    pos = bitops_ffset(&set);
    printf("pos %ld\n", pos);
}

int main(int narg, char *args[])
{
    if (mlockall(MCL_FUTURE) || mlockall(MCL_CURRENT)) {
        perror("mlockall");
        return 1;
    }
    //return testinput(narg, args);
    //return testdist(narg, args);
    //return testlive(narg, args);
    testbitops();
}
