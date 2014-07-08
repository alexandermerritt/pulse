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

FILE *log;

// TODO add pid, machine name, to filename
#define LOG(str, ...) \
    do { \
        fprintf(log, ">> " str "\n", ##__VA_ARGS__); \
        fflush(log); \
    } while (0)

// duplicate stdout/stderr into a log file
int init_log(const char *prefix)
{
    char name[256];
    if (log)
        return 0;
    if (!prefix)
        return -1;
    snprintf(name, 256, "/tmp/%s.log", prefix);
    log = fopen(name, "w");
    if (!log)
        return -1;
    return 0;
}

/*
 * FeatureBolt
 */

void FeatureBolt::Initialize(Json::Value conf, Json::Value context)
{
    if (init_log("bolt"))
        abort();
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc)
        abort();
}

void FeatureBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());
    LOG("v0 %s", v[0].asCString());
    LOG("v1 %s", v[1].asCString());
}

/*
 * StitcherSpout
 */

StitcherSpout::StitcherSpout(void)
    : memc(NULL), group_id(0)
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
        LOG("Error connecting to memcached: %s", strerror(errno));
        LOG("    config: %s", memc_config);
        return -1;
    } else {
        const char *err = memcached_last_error_message(memc);
        if (err) {
            LOG("Error connecting to memcached: %s", err);
            LOG("    config: %s", memc_config);
            return -1;
        }
    }

    mret = memcached_exist(memc, info_key, strlen(info_key));
    if (MEMCACHED_SUCCESS != mret) {
        if (MEMCACHED_NOTFOUND == mret)
            LOG("'%s' not found", info_key);
        else
            LOG("error querying existence: %s",
                    memcached_strerror(memc, mret));
        return -1;
    }

    keyval = memcached_get(memc, info_key, strlen(info_key),
            &len, &flags, &mret);
    if (!keyval) {
        LOG("could not get '%s': %s", info_key,
                memcached_strerror(memc, mret));
        return -1;
    }

    std::string valstr((char*)keyval);
    Json::Reader r;
    if (!r.parse(valstr, graph_info, false)) {
        LOG("error parsing json for '%s'", info_key);
        return -1;
    }
    LOG("max from '%s': %s", info_key, graph_info["max"].asCString());

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

    // FIXME query some key to provide configuration
    key = rand_r(&seed) % num_nodes;
    snprintf(keystr, 64, "%d", key);

    for (int i = 0; i < 1; i++) {

        mret = memcached_exist(memc, keystr, strlen(keystr));
        if (MEMCACHED_SUCCESS != mret) {
            if (MEMCACHED_NOTFOUND == mret)
                LOG("'%s' not found", info_key);
            else
                LOG("error querying existence: %s",
                        memcached_strerror(memc, mret));
            abort();
        }

        keyval = memcached_get(memc, keystr, strlen(keystr),
                &len, &flags, &mret);
        if (!keyval) {
            LOG("could not retreive %s: %s", keystr,
                    memcached_strerror(memc, mret));
            abort();
        }
        std::string valstr((char*)keyval);
        free(keyval);
        keyval = NULL;

        // vertices will always have > 0 edges as otherwise the graph
        // would be disconnected... just might loop back and forth
        Json::Reader r;
        if (!r.parse(valstr, v, false)) {
            LOG("error parsing");
            abort();
        }
        if (v.empty()) {
            LOG("idx %d is empty!", key);
            continue;
        }

        // iterate
        key = atoi(v[0].asCString());
        snprintf(keystr, 64, "%d", key);

        Json::Value next;
        next[0] = "group_id";
        next[1] = v[0];
        storm::Tuple tup(next);
        storm::EmitSpout(tup, std::string(), -1, std::string());
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

