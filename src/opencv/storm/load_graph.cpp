/*
 * clang++ --std=c++11 load_graph.cpp -o load_graph \
 *              -I/usr/include/jsoncpp -ljsoncpp -lmemcached
 *
 * graph file should be a 2-col file with elements of type unsigned int,
 * separated by a single space character
 */
#include <libmemcached/memcached.h>
#include <iostream>
#include <map>
#include <list>
#include <json/json.h>
#include <stdio.h>

const char config_string[] = "--SERVER=192.168.1.221"; // --SERVER=192.168.1.222";

// node and edge set
std::map< unsigned int, std::list< unsigned int > > graph;

int load(std::string &path)
{
    printf(">> Loading file...\n");
    char line[64];
    FILE *f = fopen(path.c_str(), "r");
    if (!f)
        return -1;
    while (fgets(line, 64, f)) {
        unsigned int n0, n1;
        if (2 != sscanf(line, "%u %u", &n0, &n1))
            return -1;
        // unordered graph
        graph[n0].push_back(n1);
        graph[n1].push_back(n0);
    }
    return 0;
}

int store(void)
{
    memcached_return_t mret;
    memcached_st *memc = NULL;

    printf(">> Writing to memcached...\n");

    memc = memcached(config_string, strlen(config_string));
    if (!memc)
        return -1;

    for (auto &key : graph) { // k is a pair<>
        char keystr[64];
        snprintf(keystr, 64, "%u", key.first);
        Json::Value v;
        for (unsigned int &e : key.second) {
            v.append(std::to_string(e));
        }
        Json::StyledWriter w;
        std::string val = w.write(v);
        mret = memcached_set(memc, keystr, strlen(keystr),
                val.c_str(), val.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;
    }

    if (memc)
        memcached_free(memc);
    return 0;
}

int
main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s graphfile\n", *argv);
        return -1;
    }

    std::string s(argv[1]);
    if (load(s))
        return -1;

    if (store())
        return -1;

    return 0;
}

