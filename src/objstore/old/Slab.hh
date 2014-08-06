/**
 *
 */

#pragma once

// C includes

// C++ includes
#include <vector>

// Local includes
#include "Buddy.hh"

namespace objstore
{

class CacheAllocator
{
    // chunk given from buddy
    class Slab
    {
        friend CacheAllocator;
        void *region;
        size_t len;
        size_t slots, next;
    };

    // set of slabs for specific object size
    class Cache
    {
        friend CacheAllocator;
        std::list<Slab> slabs;
        size_t elemSize;
        Cache(size_t esz) : elemSize(esz) { }
    };

    Buddy buddy;
    std::vector<Cache> caches; // must be in incr order of elemSize

    int minRemPow(size_t bytes, size_t minPow, size_t maxPow);

    public:

    /* TODO add max size */
    CacheAllocator(float cacheGrowth = 2.0);

    void *alloc(size_t len);
    void free(void *addr);
};

};

