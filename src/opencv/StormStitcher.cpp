/**
 * StormTopology.cpp
 *
 * Executable must be named 'stormstitcher' and exist in a PATH directory.
 *
 * Tuples emitted and processed in this file must be
 *  - arrays (Json::Value treated with integer indeces)
 *  - have a number of elements (or 'Fields') equal to what is declared in the
 *  StitcherTopology.java file's Spout and Bolt implementations.
 */

// C headers
#include <string.h>
#include <unistd.h>
#include <libmemcached/memcached.h>

#include <sys/types.h>
#include <sys/socket.h>

// C++ headers
#include <iostream>
#include <json/json.h>

// Local headers
#include "StormWrapper.h"       // unofficial 'storm' namespace
#include "StormStitcher.h"
#include "stitcher.hpp"

FILE *logfp;

// TODO add pid, machine name, to filename
#define L(str, ...) \
    do { \
        fprintf(logfp, ">> " str "\n", ##__VA_ARGS__); \
        fflush(logfp); \
    } while (0)

// duplicate stdout/stderr into a log file
int init_log(const char *prefix)
{
    char name[256];
    if (logfp)
        return 0;
    if (!prefix)
        return -1;
    snprintf(name, 256, "/tmp/%s.log", prefix);
    logfp = fopen(name, "w");
    if (!logfp)
        return -1;
    return 0;
}

/*
 * FeatureBolt
 */

FeatureBolt::FeatureBolt(void)
    : memc(NULL)
{
    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        abort();
    }
}

void FeatureBolt::Initialize(Json::Value conf, Json::Value context)
{
    if (init_log("bolt"))
        abort();
}

void FeatureBolt::Process(storm::Tuple &tuple)
{
    Json::Value tup(tuple.GetValues());
    PStitcher ps = PStitcher::createDefault();
    cv::Mat image;
}

/*
 * StitcherSpout
 */

StitcherSpout::StitcherSpout(void)
    : memc(NULL),
    // XXX To handle multiple spouts, we provide each with a namespace of
    // group_id:    <pid>0000000000..00
    // Each iteration increments in that space
    group_id(getpid() << 20),
    max_depth(1)
{
    if (init_log("spout"))
        abort();
}

int StitcherSpout::initmemc(void)
{
    memcached_return_t mret;
    size_t len;
    uint32_t flags;
    char *keyval = NULL;

    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        return -1;
    }
    // FIXME sometimes memc was allocated despite errors connecting

    mret = memcached_exist(memc, info_key, strlen(info_key));
    if (MEMCACHED_SUCCESS != mret) {
        if (MEMCACHED_NOTFOUND == mret)
            L("'%s' not found in memcached", info_key);
        else
            L("error querying memcached for %s : %s", info_key,
                    memcached_strerror(memc, mret));
        return -1;
    }

    keyval = memcached_get(memc, info_key, strlen(info_key),
            &len, &flags, &mret);
    if (!keyval) {
        L("could not get '%s': %s", info_key,
                memcached_strerror(memc, mret));
        return -1;
    }

    std::string valstr((char*)keyval);
    Json::Reader r;
    if (!r.parse(valstr, graph_info, false)) {
        L("error parsing json for '%s'", info_key);
        return -1;
    }
    L("max from '%s': %s", info_key, graph_info["max"].asCString());

    seed = time(NULL) + getpid();
    srand(seed);

    return 0;
}

void StitcherSpout::Initialize(Json::Value conf, Json::Value context)
{
    if (initmemc())
        abort();
}

// Pick random key, emit nodes while walking some steps.
void StitcherSpout::NextTuple(void)
{
    Json::Value v;
    memcached_return_t mret;
    uint32_t flags;
    size_t len;
    int key;
    char keystr[64];
    void *keyval = NULL;
    int num_nodes = graph_max() + 1;
    std::string groupstr = std::to_string(group_id++);

    // FIXME query some key to provide configuration
    key = rand_r(&seed) % num_nodes;
    snprintf(keystr, 64, "%d", key);

    int depth = 1 + (rand_r(&seed) % max_depth);
    for (int i = 0; i < depth; i++) {

        mret = memcached_exist(memc, keystr, strlen(keystr));
        if (MEMCACHED_SUCCESS != mret) {
            if (MEMCACHED_NOTFOUND == mret)
                L("'%s' not found", info_key);
            else
                L("error querying existence: %s",
                        memcached_strerror(memc, mret));
            abort();
        }

        keyval = memcached_get(memc, keystr, strlen(keystr),
                &len, &flags, &mret);
        if (!keyval) {
            L("could not retreive %s: %s", keystr,
                    memcached_strerror(memc, mret));
            abort();
        }
        std::string valstr((char*)keyval);
        free(keyval);
        keyval = NULL;

        Json::Reader r;
        if (!r.parse(valstr, v, false)) {
            L("error parsing");
            abort();
        }
        if (v.empty()) {
            L("idx %d is empty!", key);
            continue;
        }

        // emit all neighbors of node
        for (unsigned int i = 0; i < v.size(); i++) {
            Json::Value next;
            next[0] = groupstr;
            next[1] = v[i];
            storm::Tuple tup(next);
            storm::EmitSpout(tup, std::string(), -1, std::string());
        }

        // iterate
        key = atoi(v[0].asCString()); // TODO this is not BFS
        snprintf(keystr, 64, "%d", key);
    }
}

/*
 * Executable functions
 */

void badusage(int argc, char *argv[])
{
    std::cerr << "Usage: "
        << std::endl << "    " << *argv << " " << CMD_ARG_SPOUT
        << std::endl << "    " << *argv << " " << CMD_ARG_BOLT << "=<name>"
        << std::endl;
    exit(-1);
}

// We are instantiated by ShellSpout or ShellBolt constructors from Java.
int main(int argc, char *argv[])
{
    char *save, *arg = NULL;

    if (argc != 2)
        badusage(argc, argv);

    arg = strtok_r(argv[1], CMD_ARG_DELIM, &save);
    if (0 == strncmp(arg, CMD_ARG_SPOUT, strlen(CMD_ARG_SPOUT))) {
        StitcherSpout spout;
        spout.Run();
    } else {
        if (0 != strncmp(arg, CMD_ARG_BOLT, strlen(CMD_ARG_BOLT)))
            badusage(argc, argv);
        arg = strtok_r(NULL, CMD_ARG_DELIM, &save);
        if (0 == strncmp(arg, CMD_ARG_FEATURE, strlen(CMD_ARG_FEATURE))) {
            FeatureBolt bolt;
            bolt.Run();
        }
        // TODO add other bolts here
    }

    return 0;
}

