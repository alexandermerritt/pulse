/*
 * clang++ --std=c++11 load_graph.cpp -o load_graph \
 *              -I/usr/include/jsoncpp -ljsoncpp -lmemcached \
 *              -lopencv_core -lopencv_highgui
 *
 * we assume graph has vertices whose labels are unsigned int, existing on a
 * contiguous range from 0-N. this is needed so we can randomly pick a valid
 * vertex using rand() and to provide the range of values in a separate graph
 * key
 *
 * graph file should be a 2-col file with elements of type unsigned int,
 * separated by a single space character
 *
 * %u   node id in graph
 *      ["neighbors" : "%u", "%u", ... ]
 *      ["images"    : "<name>.jpg", ...]
 *
 * <name>.jpg   image container (metadata)
 *      ["data_key"     : "<name>.jpg__d" ]
 *      ["features_key" : "<name>.jpg__f" ]
 *      ["cols"         : int ]
 *      ["rows"         : int ]
 *      ["type"         : int ]
 *      ["pxsz"         : int ]
 *      ["flags"        : int ]
 *
 * <name>.jpg__d    image file as stored on disk (not JSON)
 *
 * <name>.jpg__f    serialized feature set of image (not JSON)
 *
 */
#include <libmemcached/memcached.h>
#include <iostream>
#include <map>
#include <list>
#include <json/json.h>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>

#include <opencv2/core/core.hpp>
#include <opencv2/highgui/highgui.hpp>

#include "Config.hpp"

// node and edge set
std::map< unsigned int, std::list< unsigned int > > graph;
unsigned int max_key = 0;

std::vector< std::string > imagelist;

memcached_st *memc = NULL;

int memc_exists(memcached_st *memc,
        const std::string &key)
{
    memcached_return_t mret;

    assert(memc);

    mret = memcached_exist(memc, key.c_str(), key.length());
    if (MEMCACHED_SUCCESS == mret)
        return true;
    if (MEMCACHED_NOTFOUND == mret)
        return false;

    fprintf(stderr, "error querying memcached for %s : %s\n",
            config->graph.infoKey.c_str(),
            memcached_strerror(memc, mret));
    abort();
    return false;
}

// full path to image per line
int load_images(std::string &path)
{
    memcached_return_t mret;
    std::string line;
    imagelist.clear();

    printf(">> Loading images...\n");

    std::ifstream file;
    file.open(path);
    if (!file.is_open()) {
        perror("open image file");
        return -1;
    }
    while (std::getline(file, line)) {
        if (line[0] == '#')
            continue;
        imagelist.push_back(line);
    }
    file.close();

    for (auto &l : imagelist) {
        std::cout << l << std::endl;
        Json::Value v;

        if (memc_exists(memc, l)) {
            printf("exists, not loading %s\n", l.c_str());
            continue;
        }

        cv::Mat img;
        img = cv::imread(l, 1);
        if (!img.data) {
            fprintf(stderr, "failed to read %s\n", l.c_str());
            return -1;
        }

        std::string imgdata_key = std::string(l + "__d");
        std::string feature_key = std::string(l + "__f");
        v["features_key"] = feature_key; // created by bolt
        v["data_key"] = imgdata_key;
        v["cols"] = img.cols;
        v["rows"] = img.rows;
        v["type"] = img.type();
        v["pxsz"] = static_cast<int>(img.elemSize());
        v["flags"] = img.flags;

        Json::FastWriter w;
        std::string _v = w.write(v);

        std::cout << _v << std::endl;

        mret = memcached_set(memc, l.c_str(), l.length(),
                _v.c_str(), _v.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }

        mret = memcached_set(memc, imgdata_key.c_str(), imgdata_key.length(),
                (const char*)img.data, img.elemSize() * img.total(), 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }
    }

    return 0;
}

int __load(std::string &path)
{
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
        max_key = std::max(max_key, std::max(n0, n1));
    }
    return 0;
}

// one edge per line, 'vertex vertex'
// vertices should be numbered from 0 through some max; all values should be
// accounted for (ie all contiguous)
int load_graph(std::string &path)
{
    memcached_return_t mret;

    if (__load(path))
        return -1;

    unsigned int seed = time(NULL) + getpid();

    printf(">> Writing to memcached...\n");

    { // store graph metadata
        Json::Value v;
        v[config->graph.infoKey_fieldMax] = std::to_string(max_key);
        Json::FastWriter w;
        std::string val = w.write(v);
        mret = memcached_set(memc, config->graph.infoKey.c_str(),
                config->graph.infoKey.length(),
                val.c_str(), val.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;
        printf(">> %s: %s\n", config->graph.infoKey.c_str(),
                val.c_str());
    }

    for (auto &key : graph) { // k is a pair<>
        std::string id;
        id = std::to_string(key.first);

        Json::Value v, arr;
        for (unsigned int &e : key.second) {
            arr.append(std::to_string(e));
        }
        v["neighbors"] = arr;

        arr.clear();
        int num = rand_r(&seed) % (config->load.maxImagesPerNode + 1);
        for (int n = 0; n < num; n++)
            arr.append(imagelist[rand_r(&seed) % imagelist.size()]);
        v["images"] = arr;
        
        Json::FastWriter w;
        std::string val = w.write(v);
        mret = memcached_set(memc, id.c_str(), id.length(),
                val.c_str(), val.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;
    }

    return 0;
}

int init_memc(void)
{
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc)
        return -1;
    return 0;
}

int
main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s graphlist imagelist config\n", *argv);
        return 1;
    }
    std::string g(argv[1]);
    std::string i(argv[2]);
    std::string c(argv[3]);

    if (init_config(c))
        return 1;

    if (init_memc())
        return 1;

    if (load_images(i))
        return 1;

    if (load_graph(g))
        return 1;

    return 0;
}

