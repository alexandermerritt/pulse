/**
 * StitcherTopology.h
 */

#pragma once

#include <stdlib.h>

#include "StormWrapper.h"
#include <json/json.h>

#include <libmemcached/memcached.h>

const char CMD_ARG_DELIM[]   = "=";
const char CMD_ARG_SPOUT[]   = "--spout";
const char CMD_ARG_BOLT[]    = "--bolt";
const char CMD_ARG_FEATURE[] = "feature";

const char memc_config[] =
    "--SERVER=192.168.1.221:11211";
    //"--BINARY-PROTOCOL";


// Key in memcached that describes graph
// Refer to storm/load_graph.cpp
const char info_key[] = "graph_info";
const char graph_max_key[] = "max";

class GraphSpout : public storm::Spout
{
    public:
        GraphSpout(void);
        void Initialize(Json::Value conf, Json::Value context);
        void NextTuple(void);

    private:
        int initmemc(void);
        int graph_max(void)
        {
            return atoi(graph_info[graph_max_key].asCString());
        }

    private:
        memcached_st *memc;
        unsigned long group_id;
        unsigned int seed;
        unsigned int max_depth;
        Json::Value graph_info;

};

class UserBolt : public storm::BasicBolt
{
    public:
        UserBolt(void);
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);

    private:
        memcached_st *memc;
};

class FeatureBolt : public storm::BasicBolt
{
    public:
        FeatureBolt(void);
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);

    private:
        memcached_st *memc;
};

