/**
 * Config.hpp
 *
 * Class that holds configuration paramters across all components of the
 * application.
 */

#pragma once

#include <string>
#include <list>

class Config
{
    public:
        Config(void);
        ~Config(void);
        void init(void);

        int LoadConfig(const std::string &path);
        void Print(void);

        class GraphConfig
        {
            public:
            std::string prefix;

            // name of key holding info about the graph as json type
            std::string infoKeyPrefix;
            std::string infoKey;
            // name of field in json type holding max node ID
            std::string infoKey_fieldMax;
        };

        class DataLoadConfig
        {
            public:
            std::string prefix;

            std::string perNodePrefix;
            int maxImagesPerNode;
        };

        class MemcConfig
        {
            public:
            std::string prefix;

            std::string serversPrefix;
            std::string servers;
        };

        class StormConfig
        {
            public:

            // spout options
            std::string spoutPrefix;
            std::string sleepPrefix;
            int usleepTime;
            std::string depthPrefix;
            int maxDepth;

            // individual bolt options
        };

        GraphConfig     graph;
        DataLoadConfig  load;
        MemcConfig      memc;
        StormConfig     storm;

        int parseLine(std::list<std::string> &split);
        int split(const char *str, std::list<std::string> &tokens);
};

extern Config *config;

int init_config(const std::string &path);
void print_config(const Config &config);

