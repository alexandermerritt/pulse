/**
 *
 */

// C includes
#include <stdlib.h>

// C++ includes
#include <iostream>

// Local includes
#include "Slab.hh"

using namespace objstore;

CacheAllocator::CacheAllocator(float cacheGrowth)
{
    size_t size = 1UL << Buddy::MinPower;

    if (buddy.init())
        abort();

    while (size < buddy.maxPowAlloc()) {
        Cache c(size);
        caches.push_back(c);
        size *= cacheGrowth;
    }
}

// -> which slab size (as power of two) provides maximum use of space?
int CacheAllocator::minRemPow(size_t bytes,
        size_t minPow, size_t maxPow)
{
    size_t wasted[maxPow - minPow + 1];
    size_t pow = minPow;
    int powLowest = 0;

    while (pow <= maxPow)
        wasted[pow - minPow] = bytes % (1UL << pow);

    for (pow = minPow; pow <= maxPow; pow++)
        if (wasted[pow] < wasted[powLowest])
            powLowest = pow;

    return powLowest;
}

void* CacheAllocator::alloc(size_t len)
{
    void *addr = NULL;
    Cache *c;

    for (auto &_c: caches) {
        if (_c.elemSize >= len) {
            c = &_c;
            break;
        }
    }

    if (c->slabs.empty()) {
        size_t slabSize = minRemPow(len,
                Buddy::MinPower, buddy.maxPowAlloc());
        buddy.alloc(slabSize);
    }
    // TBD... just discovered 'jemalloc'

    return addr;
}

void free(void *addr)
{
}

