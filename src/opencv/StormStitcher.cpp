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

// C++ headers
#include <iostream>
#include <json/json.h>

// Local headers
#include "StormWrapper.h"       // unofficial 'storm' namespace
#include "StormStitcher.h"

FILE *log;

#define LOG(str, ...) \
    do { \
        fprintf(log, ">> " str "\n", ##__VA_ARGS__); \
        fflush(log); \
    } while (0)

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

void FeatureBolt::Initialize(Json::Value conf, Json::Value context)
{
    if (init_log("bolt"))
        abort();
}

void FeatureBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());
    LOG("v0 %s", v[0].asString().c_str());
    LOG("v1 %s", v[1].asString().c_str());
}

void StitcherSpout::Initialize(Json::Value conf, Json::Value context)
{
    if (init_log("spout"))
        abort();
}

void StitcherSpout::NextTuple(void)
{
    Json::Value v;
    v[0] = "key";
    v[1] = "";
    storm::Tuple t(v);
    storm::EmitSpout(t, std::string(), -1, std::string());
}

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

