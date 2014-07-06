/**
 * StitcherTopology.h
 */

#pragma once

#include "StormWrapper.h"
#include <json/json.h>

const char CMD_ARG_DELIM[]   = "=";
const char CMD_ARG_SPOUT[]   = "--spout";
const char CMD_ARG_BOLT[]    = "--bolt";
const char CMD_ARG_FEATURE[] = "feature";

class StitcherSpout : public storm::Spout
{
    public:
        StitcherSpout(void) { }
        void Initialize(Json::Value conf, Json::Value context);
        void NextTuple(void);
};

class FeatureBolt : public storm::BasicBolt
{
    public:
        FeatureBolt(void) { }
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);
};

