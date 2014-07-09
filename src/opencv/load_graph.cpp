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
 *      ["data"     : "<name>.jpg__d" ]
 *      ["features" : "<name>.jpg__f" ]
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

const char config_string[] = "--SERVER=192.168.1.221"; // --SERVER=192.168.1.222";
const char info_key[] = "graph_info";
const int max_imgs_per_node = 16;

// node and edge set
std::map< unsigned int, std::list< unsigned int > > graph;
unsigned int max_key = 0;

std::vector< std::string > imagelist;

memcached_st *memc = NULL;

int load_images(std::string &path)
{
    memcached_return_t mret;
    std::string line;
    imagelist.clear();

    printf(">> Loading images...\n");

    std::ifstream file;
    file.open(path);
    if (!file.is_open())
        return -1;
    while (std::getline(file, line))
            imagelist.push_back(line);
    file.close();

    for (auto &l : imagelist) {
        std::cout << l << std::endl;
        Json::Value v;

        cv::Mat img;
        img = cv::imread(l, 0);
        if (!img.data)
            return -1;

        std::string imgdata_key = std::string(l + "__d");
        v["features_key"] = std::string(); // bolt will create this
        v["data_key"] = imgdata_key;
        v["cols"] = img.cols;
        v["rows"] = img.rows;
        v["type"] = img.type();
        v["pxsz"] = (int)img.elemSize();
        v["flags"] = img.flags;

        Json::FastWriter w;
        std::string _v = w.write(v);

        mret = memcached_set(memc, l.c_str(), l.length(),
                _v.c_str(), _v.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;

        mret = memcached_set(memc, imgdata_key.c_str(), imgdata_key.length(),
                (const char*)img.data, img.elemSize() * img.total(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;
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

int load_graph(std::string &path)
{
    memcached_return_t mret;

    if (__load(path))
        return -1;

    unsigned int seed = time(NULL) + getpid();

    printf(">> Writing to memcached...\n");

    { // store info_key describing graph
        Json::Value v;
        v["max"] = std::to_string(max_key);
        Json::FastWriter w;
        std::string val = w.write(v);
        mret = memcached_set(memc, info_key, strlen(info_key),
                val.c_str(), val.length(), 0, 0);
        if (mret != MEMCACHED_SUCCESS)
            return -1;
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
        int num = rand_r(&seed) % (max_imgs_per_node + 1);
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
    memc = memcached(config_string, strlen(config_string));
    if (!memc)
        return -1;
    return 0;
}

int
main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s graphlist imagelist\n", *argv);
        return 1;
    }
    std::string g(argv[1]);
    std::string i(argv[2]);

    if (init_memc())
        return 1;

    if (load_images(i))
        return 1;

    if (load_graph(g))
        return 1;

    return 0;
}

