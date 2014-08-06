#include <iostream>
#include "Buddy.hh"

using namespace std;
using namespace objstore;

int main(void)
{
    Buddy b;
    std::list<void*> ptrs;
    void *ptr;

    (void)ptrs.size();

    b.init();
    b.dumpStats();

    size_t amt = (1<<19) - 32;
    while ((ptr = b.alloc(amt)))
        ptrs.push_back(ptr);
    for (void *p : ptrs)
        b.free(p);

    ptrs.clear();
    b.dumpStats();

    amt = 2000; // pow 11
    while ((ptr = b.alloc(amt)))
        ptrs.push_back(ptr);
    for (void *p : ptrs)
        b.free(p);

    ptrs.clear();
    b.dumpStats();

    amt = 50;
    while ((ptr = b.alloc(amt)))
        ptrs.push_back(ptr);
    for (void *p : ptrs)
        b.free(p);

    ptrs.clear();
    b.dumpStats();

    amt = 4000;
    while ((ptr = b.alloc(amt)))
        ptrs.push_back(ptr);
    for (void *p : ptrs)
        b.free(p);

    ptrs.clear();
    b.dumpStats();

    ptr = b.alloc((1<<20) - 32);
    if (!ptr)
        return -1;
    b.free(ptr);
    b.dumpStats();

    return 0;
}
