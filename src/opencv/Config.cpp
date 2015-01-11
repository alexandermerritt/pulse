/**
 * Config.cpp
 */

// C includes
#include <string.h>

// C++ includes
#include <list>
#include <iostream>

// Project includes
#include "Config.hpp"

Config *config;

void Config::init(void)
{
    graph.prefix        = std::string("graph");
    graph.idsFilePrefix = std::string("idsfile");

    memc.prefix         = std::string("memc");
    memc.serversPrefix  = std::string("serv");

    storm.spoutPrefix   = std::string("spout");
    storm.sleepPrefix   = std::string("usleep");
    storm.depthPrefix   = std::string("maxdepth");
}

Config::Config(void)
{
    init();
}

Config::~Config(void)
{
}

int Config::parseLine(std::list<std::string> &split)
{
    int ret = 0;
    if (split.size() < 3)
        return -1;
    const std::string prefix(split.front());
    split.pop_front();
    if (prefix == config->graph.prefix) {
        const std::string sub(split.front());
        split.pop_front();
        if (sub == config->graph.idsFilePrefix) {
            config->graph.idsFile = split.front();
        } else {
            ret = -1;
        }
    } else if (prefix == config->storm.spoutPrefix) {
        const std::string sub(split.front());
        split.pop_front();
        if (sub == config->storm.sleepPrefix) {
            config->storm.usleepTime =
                atoi(split.front().c_str());
        } else if (sub == config->storm.depthPrefix) {
            config->storm.maxDepth =
                atoi(split.front().c_str());
        } else {
            ret = -1;
        }
        // TODO bolt stuff
    } else if (prefix == config->memc.prefix) {
        // memcached config string is very picky..
        // cannot have trailing spaces on either end, etc
        const std::string sub(split.front());
        split.pop_front();
        if (sub == config->memc.serversPrefix) {
            config->memc.servers += split.front();
            split.pop_front();
            while (split.size() > 0) {
                config->memc.servers += " " + split.front();
                split.pop_front();
            }
        } else {
            ret = -1;
        }
    }
    // other config options are ignored
    return ret;
}

int Config::split(const char *str, std::list<std::string> &tokens)
{
    char *tok, *saveptr;
    char *strcopy = strdup(str);
    if (!strcopy)
        return -1;
    tokens.clear();

    // wipe new line characters, tabs, etc
    char *ptr = strcopy + strlen(strcopy);
    while (--ptr >= strcopy)
        if (*ptr == '\n' || *ptr == '\t')
            *ptr = ' ';

    tok = strtok_r(strcopy, " ", &saveptr);
    while (tok) {
        tokens.push_back(tok);
        tok = strtok_r(NULL, " ", &saveptr);
    }
    free(strcopy);
    return 0;
}

int Config::LoadConfig(const std::string &path)
{
    size_t len = 512;
    char *line = (char*)malloc(len);
    if (!line)
        return -1;

    FILE *fp = fopen(path.c_str(), "r");
    if (!fp)
        return -1;

    while (0 < getline(&line, &len, fp)) {
        std::list<std::string> tokens;
        if (split(line, tokens))
            return -1;
        if (parseLine(tokens))
            return -1;
    }

    free(line);
    fclose(fp);
    return 0;
}

int init_config(const std::string &path)
{
    if (config)
        delete config;
    config = new Config();
    return config->LoadConfig(path);
}

