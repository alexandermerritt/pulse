#include <fstream>
#include <random>
#include <stack>
#include <sstream>
#include <bitset>
#include <iostream>
#include <array>
#include <deque>

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

#if 0
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
#endif

class LiveSet
{
    public:

        LiveSet(long bytes) : max_live(bytes), live(0L),
                            last_idx(0), drop_misses(0),
                            locs(nullptr), sizes(nullptr),
                            fltop(0L),
                            gen(rd()), dist(0, nlocs-1) {
            if (bytes < 1)
                throw runtime_error("LiveSet with empty set?");
            locs = (uintptr_t*)map_alloc(nlocs*sizeof(*locs));
            sizes = (uint32_t*)map_alloc(nlocs*sizeof(*sizes));
            assert( locs && sizes );

            //freelist = new array<off_t, flmax>();
            //assert( freelist );
            freelist.fill(-1);
            fltop = 0L;
        }

        ~LiveSet() {
            freeAll();
            munmap(locs, nlocs*sizeof(*locs));
            munmap(sizes, nlocs*sizeof(*sizes));
            //delete freelist;
        }

        void fillFixed(uint32_t objsize) {
            if ((nlocs * objsize) < max_live)
                throw runtime_error("resize needed");
            for (long n = 0; n < nlocs; n++) {
                assert( locs[n] = (uintptr_t)malloc(objsize) );
                sizes[n] = objsize;
                live += objsize;
                nobjs++;
                if (live >= max_live) {
                    last_idx = n;
                    break;
                }
            }
            freelist.fill(-1);
            fltop = 0L;
        }

        // add new objects. if we exceed our allowance, delete random
        // objects until we are within allowance again.
        // Must invoke some fill method before invoking this!
        void injectFixed(long n, uint32_t objsize) {
            off_t idx;
            if (n == 0)
                return;
            drop(objsize<<2);
            for (long nn = 0; nn < n; nn++) {
                if (fltop == 0) {
                    if ((idx = ++last_idx) >= nlocs)
                        throw runtime_error("Exceeded nlocs");
                } else {
                    idx = freelist[--fltop];
                    assert( idx > -1 );
                }
                assert( !locs[idx] );
                assert( locs[idx] = (uintptr_t)malloc(objsize) );
                sizes[idx] = objsize;
                live += objsize;
                nobjs++;
                // print progress
                if ((nn%10000L)==0) {
                    clear_line(); printf("# %3.3lf %%  ",
                        (float)nn*100/n); fflush(stdout);
                }
                drop(objsize<<2);
            }
            printf("\n");
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
            live = nobjs = 0L;
            freelist.fill(-1);
            fltop = 0L;
        }

        static inline size_t ceil4K(size_t val) {
            long pgs = (val>>PAGE_SHIFT);
            pgs += (val & PAGE_MASK) ? 1 : 0;
            return (pgs<<PAGE_SHIFT);
        }

        size_t overhead() {
            return ceil4K(nlocs*sizeof(*locs))
                + ceil4K(nlocs*sizeof(*sizes))
                //+ ceil4K(flmax*sizeof(off_t)) // if allocated on heap
                + ceil4K(sizeof(LiveSet));
        }

        long get_nobjs(void) { return nobjs; }
        long get_live(void) { return live; }
        long get_maxlive(void) { return max_live; }
        long get_drops(void) { return drop_misses; }

    private:
        long max_live, live;    // working set max and current, bytes
        long nobjs;             // pointers allocated
        long last_idx, drop_misses;

        // locs and sizes are going to be nearly always non-NULL
        // so we maintain a freelist of recently free'd locations
        constexpr static long nlocs = ((long)200e6);    // quantity of pointers we track
        uintptr_t *locs;        // array with nlocs items
        uint32_t *sizes;        // array with nlocs items

        constexpr static off_t flmax = nlocs>>2;
        array<off_t, flmax> freelist;
        off_t fltop;

        random_device rd;
        mt19937 gen;
        uniform_int_distribution<long> dist;

        LiveSet();

        void resize(long nlocs_) {
            throw runtime_error("resize needed");
#if 0
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
#endif
        }

        // free objects at random until we are below max
        void drop(off_t atleast = 0UL) {
            if (live <= (max_live - atleast))
                return;
            long idx;
            while (live > (max_live - atleast)) {
                idx = dist(gen) % last_idx; // should hit most of the time
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
};

// modeled after synthetic workload from: Rumble et al. FAST'14
int testlive(int narg, char *args[])
{
    float rss, eff;

    LiveSet *liveset(nullptr);
    const long live = 1L<<30;
    const long wss = 1L<<32;
    const uint32_t objsize = 100, objsize2 = 130;

    assert( liveset = new LiveSet(live) );

    // FIXME how to subtract out ELF segments, globals, SO objects?
    // supermalloc performs a huge heap alloc during lib init

    printf("cmd testname-sz rss-MB eff\n");

    cout << "# fillFixed " << objsize << endl;
    liveset->fillFixed(objsize);
    cout << "# injectFixed " << objsize << endl;
    liveset->injectFixed(wss/objsize - liveset->get_nobjs(), objsize);
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

#if 0
void printbits(bitset_t *set, off_t maxc)
{
    for (off_t i = 0; i < maxc && i < (set->nbits>>6); i++)
        printf("%016lx ", set->map[i]);
    printf("\n");
}

// prints only first chunk of pgmap
void printpgbits(bitset_t *set)
{
    printf("%016lx ", set->pgmap[0]);
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
    assert(pos == 513);

    bitops_clear(&set, 513);
    printbits(&set, maxc);
    pos = bitops_ffset(&set);
    printf("pos %ld\n", pos);
    assert(pos == -1);

    printf("\n----- test pagemap bits: set all\n");

    bitops_setall(&set, 1);
    printbits(&set, maxc);
    printf("pagemap bits:\n");
    printpgbits(&set);

    printf("\n----- test pagemap bits: unset one\n");
    bitops_clear(&set, 128);
    printbits(&set, maxc);
    printf("pagemap bits:\n");
    printpgbits(&set);

}
#endif

int main(int narg, char *args[])
{
    if (mlockall(MCL_FUTURE) || mlockall(MCL_CURRENT)) {
        perror("mlockall");
        return 1;
    }
    //return testinput(narg, args);
    //return testdist(narg, args);
    return testlive(narg, args);
    //testbitops();
}