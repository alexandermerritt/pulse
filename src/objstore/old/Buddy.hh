/**
 *
 */

#pragma once

// C includes
#include <pthread.h>

// C++ includes
#include <cstring>
#include <list>
#include <set>
#include <vector>

// Local includes

namespace objstore
{

class CacheAllocator;

class Buddy
{
    friend CacheAllocator;

    constexpr static size_t MinPower = 4UL; // TODO integrate
    constexpr static size_t MaxPower = 30UL;
    constexpr static size_t MaxBlockSize = (1UL << MaxPower);

    struct header
    {
        size_t pow;
    }__attribute__((packed));

    typedef size_t offset_t;

    void *region;
    size_t region_len; // should be power of two
    pthread_mutex_t lock;

    // array of lists; each [] contains blocks of that pow of 2
    std::vector< std::set< offset_t > > blocks;

    void *doalloc(size_t pow);
    void dofree(void *addr);

    bool needAt(size_t pow);
    void *takeAt(size_t pow);

    void mergeUp(size_t powStop);
    void splitFrom(size_t pow);

    inline unsigned int log2(size_t val)
    {
        unsigned int log = 0;
        while (val >>= 1)
            log++;
        return log;
    }

    public:

    Buddy();

    int init(size_t pow = 20UL);
    void *alloc(size_t len);
    void free(void *addr);
    void reset(void);
    inline size_t maxPowAlloc(void) { return region_len; }

    void dumpStats(void);
};

};

