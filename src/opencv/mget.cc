#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sched.h>
#include <iostream>
#include <deque>
#include <stdexcept>
#include <set>
#include <sstream>
#include <random>
#include <assert.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <libmemcached/memcached.h>

using namespace std;

memcached_st *memc;
const int maxorder = 25, minorder = 0;
const int charsperkey = 64;
char **keys[maxorder+1];
char *data;

static inline int
keysper(int objsz_order) {
    const int cap = (1<<16);
    return min(cap, (1<<(3+maxorder-objsz_order)));
}

void setup(void)
{
    cout << "# composing keys..." << endl;
    for (int i = minorder; i <= maxorder; i++) {
        keys[i] = (char**)calloc(keysper(i), sizeof *keys);
        for (int j = 0; j < keysper(i); j++) {
            keys[i][j] = (char*)calloc(charsperkey, sizeof **keys);
            assert(keys[i][j]);
            stringstream ss;
            ss << "_____" << i << ":" << j;
            memcpy(keys[i][j], ss.str().c_str(), ss.str().length());
        }
    }
}

void init(void)
{
    string servers =
        "--SERVER=10.0.0.1:11211"
        " --SERVER=10.0.0.2:11211"
        " --SERVER=10.0.0.3:11211"
        " --SERVER=10.0.0.4:11211"
        " --SERVER=10.0.0.5:11211"
        " --SERVER=10.0.0.6:11211"
        " --SERVER=10.0.0.7:11211";

    memc = memcached(servers.c_str(), servers.length());
    assert(memc);

    setup();
}

void putstuff(void)
{
    memcached_return mret;
    data = (char*)malloc(1<<maxorder);
    char *obj;
    cout << "# writing values...";
    for (int i = minorder; i <= maxorder; i++) {
        cout << " " << (1<<i); cout.flush();
        for (int j = 0; j < keysper(i); j++) {
            assert(MEMCACHED_SUCCESS == memcached_set(memc, keys[i][j],
                        strlen(keys[i][j]), data, (1 << i), 0, 0));
        }
    }
    cout << endl;
    free(data);
}

static inline unsigned long
diff(struct timespec &c1, struct timespec &c2)
{
    unsigned long d1, d2;
    d1 = c1.tv_sec * 1e9 + c1.tv_nsec;
    d2 = c2.tv_sec * 1e9 + c2.tv_nsec;
    return (d2-d1);
}

static void pin(int cpu)
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    sched_setaffinity(getpid(), sizeof mask, &mask);
}

unsigned long run(int most, int one, int many, int n);

void other(void)
{
    int most = minorder; // >= minorder <= maxorder
    int p_start = 23;
    int n_p; // how many large objects to start with
    int n; // number of same-sized non-rogue objects

    n = 1024; n_p = 1;
    for (int p = p_start; p >= most; p--) {
        int n_rogue = (1 << (p_start - p)) * n_p;
        int n_ = n + n_rogue;
        printf("p %d nrogue %d n_ %d\n", p, n_rogue, n_);

        unsigned long l = 0, iter = 7;
        run(most, p, n_rogue, n_); // dry run
        for (int i = 0; i < (1<<0); i++)
            l += run(most, p, n_rogue, n_);
        cout << (l >> iter) << endl; // compute mean
    }

}

// return ms
unsigned long run(int most, int rogue, int many, int n)
{
    memcached_return mret, mret2;
    memcached_result_st *result;

    if (most < minorder || rogue > maxorder
            || rogue < minorder || many < 0 || n < 0)
        throw runtime_error("invalid args");
    if (most > maxorder || rogue > maxorder
            || many > keysper(rogue))
        throw runtime_error("invalid args");

    struct timespec c1, c2;

    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> dis(0, INT_MAX);

    size_t *lens = new size_t[n];
    char **names = new char*[n];
    assert(lens && names);
    for (int i = 0; i < n; i++) {
        int idx = dis(gen) % keysper(most);
        names[i] = keys[most][idx];
        lens[i] = strlen(names[i]);
    }
    set<int> idxs, idxs_rogue;
    while (idxs.size() < (size_t)many)
        idxs.insert(dis(gen) % n);
    while (idxs_rogue.size() < (size_t)many)
        idxs_rogue.insert(dis(gen) % keysper(rogue));
    auto riter = idxs_rogue.begin();
    for (int idx : idxs) {
        names[idx] = keys[rogue][*riter++];
        lens[idx]  = strlen(names[idx]);
    }

    clock_gettime(CLOCK_REALTIME, &c1);
    mret = memcached_mget(memc, names, lens, n);
    assert(mret == MEMCACHED_SUCCESS);

    result = memcached_result_create(memc, NULL);
    while (memcached_fetch_result(memc, result, &mret)) {
        if (mret != MEMCACHED_SUCCESS)
            break;
        // don't clear result between iterations
    }
    assert(mret == MEMCACHED_END);
    clock_gettime(CLOCK_REALTIME, &c2);

    delete [] lens;
    delete [] names;

    return diff(c1,c2);
}

static inline double
gmean(deque<unsigned long> values)
{
    double root = 1/(double)values.size();
    double ret = 1;
    for (unsigned long v : values)
        ret *= pow(v, root);
    return ret;
}

static inline double
amean(deque<unsigned long>  values)
{
    double ret = 0;
    for (unsigned long v : values)
        ret += v;
    return (ret / values.size());
}

int main(int argc, char *argv[])
{
    if (argc < 2)
        return 1;

    string arg(argv[1]);
    if (arg == "put") {
        pin(4); init();
        putstuff();
    } else if (arg == "run") {
        if (argc != 7) {
            cerr << "mget run order-all order-other other-cnt total-cnt iters" << endl;
            return 1;
        }
        int most(stoi(argv[2]));
        int one(stoi(argv[3]));
        int many(stoi(argv[4]));
        int n(stoi(argv[5]));
        int iters(stoi(argv[6]));
        if (most > maxorder || one > maxorder)
            return 1;
        if (many > n)
            return 1;
        pin(4); init();
        cout << "# mget set of " << n << " items" << endl;
        cout << "# filled with 2^" << most << "-byte items" << endl;
        cout << "# " << many
            << " of these replaced with items of size 2^"
            << one << " bytes" << endl;
        cout << "# " << iters << " iterations" << endl;
        run(most, one, many, n); // dry run
        deque<unsigned long> times(iters);
        for (int i = 0; i < iters; i++)
            times[i] = run(most, one, many, n);
        //cout << amean(times) << endl;
        for (unsigned long t : times)
            cout << t << endl;
    } else if (arg == "other") {
        pin(4); init();
        other();
    } else {
        return 1;
    }

    return 0;
}
