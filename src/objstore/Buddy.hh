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

class Buddy
{
    //friend Slab;

    constexpr static size_t MaxPower = 30UL;
    constexpr static size_t MaxBlockSize = (1UL << MaxPower);

    struct header
    {
        size_t pow;
    }__attribute__((packed));

    typedef size_t offset_t;

    void *region;
    size_t region_len;
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

    int init(size_t pow = 20UL);
    void *alloc(size_t len);
    void free(void *addr);
    void reset(void);

    void dumpStats(void);

    Buddy();
};

};

