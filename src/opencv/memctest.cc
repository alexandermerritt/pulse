// read and dump out storm::Vertex objects from the KV store
#include <iostream>
#include <libmemcached/memcached.h>
#include "Objects.pb.h"
using namespace std;
int main(void)
{
    size_t len;
    void *val;
    memcached_return_t mret;
    uint32_t flags;
    string line;

    string servers =
        "--SERVER=10.0.0.1:11211"
        " --SERVER=10.0.0.2:11211"
        " --SERVER=10.0.0.3:11211"
        " --SERVER=10.0.0.4:11211"
        " --SERVER=10.0.0.5:11211"
        " --SERVER=10.0.0.6:11211"
        " --SERVER=10.0.0.7:11211";

    memcached_st *memc = memcached(servers.c_str(), servers.length());
    if (!memc)
        return 1;

    while ((cin >> line)) {
        val = memcached_get(memc, line.c_str(), line.length(),
                &len, &flags, &mret);
        if (!val) {
            if (mret == MEMCACHED_NOTFOUND)
                cerr << "x " << line << endl;
            else
                return 1;
        } else {
            cout << "y " << line << endl;
            storm::Vertex vobj;
            if (vobj.ParseFromArray(val, len)) {
                vobj.PrintDebugString();
            }
        }
    }
    return 0;
}
