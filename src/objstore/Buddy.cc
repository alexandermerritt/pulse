/**
 *
 */

// C++ includes
#include <iostream>
#include <list>

// C includes
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>
#include <assert.h>

// Local includes
#include "Buddy.hh"

using namespace objstore;

Buddy::Buddy()
{
    pthread_mutex_init(&lock, NULL);
    blocks.resize(MaxPower + 1);
}

int Buddy::init(size_t pow)
{
    if (pow < 1)
        return -1;
    if (pow > MaxPower)
        return -1;

    errno = 0;
    region_len = (1UL << pow);
    region = mmap(NULL, region_len, PROT_READ|PROT_WRITE,
            MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    if (MAP_FAILED == region)
        return -1;

    blocks[pow].insert(0); // offset zero

    return 0;
}

void Buddy::dumpStats(void)
{
    std::cout << "pow count" << std::endl;
    for (size_t pow = 0; pow <= MaxPower; pow++)
        std::cout << pow << " " << blocks[pow].size() << std::endl;
}

void Buddy::reset(void)
{
    pthread_mutex_lock(&lock);
    for (auto &set : blocks)
        set.clear();
    pthread_mutex_unlock(&lock);
}

void Buddy::mergeUp(size_t powStop)
{
    offset_t curr, next;
    for (size_t pow = 0; pow < powStop; pow++) { 
        size_t bsize = (1UL << pow);
        auto &set = blocks[pow];
        auto it = set.begin();
        while (it != set.end()) {
            curr = *it;
            std::advance(it, 1);
            if (it == set.end())
                break; // end of list
            next = *it;
            if (curr + bsize == next) {
                std::advance(it, 1);
                set.erase(curr);
                set.erase(next);
                blocks[pow + 1].insert(curr);
            }
        }
    }
}

void Buddy::splitFrom(size_t _pow)
{
    bool found = false;
    offset_t offset;
    size_t pow = _pow;

    // TODO might be a faster way
    while (blocks[_pow].empty()) {
        for (pow = _pow, found = false; pow <= MaxPower; pow++) {
            auto &set = blocks[pow];
            if (set.empty())
                continue;
            auto item = set.begin();
            assert(item != set.end());
            offset = *item;
            set.erase(offset);
            found = true;
            break;
        }
        if (found) {
            pow--;
            blocks[pow].insert(offset);
            blocks[pow].insert(offset + (1UL << pow));
        }
        else
            break;
    }
}

// try to create a block of given power
// if no data exists to do so, return false
bool Buddy::needAt(size_t pow)
{
    if (pow > MaxPower)
        return false;
    auto &set = blocks[pow];
    if (set.empty())
        mergeUp(pow);
    if (set.empty())
        splitFrom(pow);
    return !set.empty();
}

void* Buddy::takeAt(size_t pow)
{
    uintptr_t addr;
    struct header *head;

    if (blocks[pow].empty())
        return NULL;

    auto &set = blocks[pow];
    auto item = set.begin();
    assert(item != set.end());
    offset_t offset = *item;
    set.erase(offset);

    addr = (uintptr_t)region + offset;
    head = (struct header*)addr;
    head->pow = pow;

    return (void*)(addr + sizeof(struct header));
}

void* Buddy::doalloc(size_t pow)
{
    needAt(pow);
    return takeAt(pow);
}

void Buddy::dofree(void *_addr)
{
    uintptr_t addr = ((uintptr_t)_addr) - sizeof(struct header);
    struct header *header = (struct header*)addr;
    offset_t offset = addr - (uintptr_t)region;
    blocks[header->pow].insert(offset);
}

void* Buddy::alloc(size_t _len)
{
    void *addr;

    if (_len == 0)
        return NULL;
    if (_len > MaxBlockSize)
        return NULL;

    size_t len = _len + sizeof(struct header);

    pthread_mutex_lock(&lock);
    addr = doalloc(log2(len) + 1);
    pthread_mutex_unlock(&lock);

    return addr;
}

void Buddy::free(void *addr)
{
    if (!addr)
        return;

    pthread_mutex_lock(&lock);
    dofree(addr);
    pthread_mutex_unlock(&lock);
}

