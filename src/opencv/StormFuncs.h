/**
 * StitcherTopology.h
 */

#pragma once

#include <stdlib.h>

#include <opencv2/opencv.hpp>
#include <opencv2/stitching/detail/matchers.hpp>

#include "StormWrapper.h"
#include "Config.hpp"
#include <json/json.h>

#include <libmemcached/memcached.h>

const char CMD_ARG_DELIM     = '=';
const char CMD_ARG_CONF[]    = "--conf";
const char CMD_ARG_SPOUT[]   = "--spout";
const char CMD_ARG_BOLT[]    = "--bolt";
const char CMD_ARG_FEATURE[] = "feature";
const char CMD_ARG_USER[]    = "user";
const char CMD_ARG_MONTAGE[] = "montage";
const char CMD_ARG_REQSTAT[] = "reqstat";

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
            return atoi(graph_info[config->graph.infoKey_fieldMax].asCString());
        }

    private:
        memcached_st *memc;
        unsigned long group_id;
        unsigned int seed;
        Json::Value graph_info;
        void initReqStats(std::string &requestID, unsigned int numUsers);
};

class UserBolt : public storm::BasicBolt
{
    public:
        UserBolt(void);
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);

    private:
        memcached_st *memc;
        void emitToReqStats(std::string &requestID,
                std::string &userID, unsigned int numImages);
};

class ReqStatBolt : public storm::BasicBolt
{
    friend GraphSpout;

    public:
        ReqStatBolt(void);
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);

        static const char* numUsers_str(void)   { return "numUsers"; }
        static const char* updatesRem_str(void) { return "updatesRem"; }
        static const char* totImg_str(void)     { return "totalImages"; }
        static const char* complete_str(void)   { return "complete"; }

    private:
        struct stat
        {
            int numUsers, updatesRem;
            int totalImages;
            // complete = 1 means that the UserBolt has converted all user IDs
            // into imageIDs for a given request ID and that MontageBolt can
            // accurately know when to begin creation of a Montage to finalize
            // the request, as it needs all images as input.
            int complete; // boolean
        };

        memcached_st *memc;
        std::map< std::string /* reqID */, struct stat > cache;
};

class FeatureBolt : public storm::BasicBolt
{
    public:
        FeatureBolt(void);
        void Process(storm::Tuple &tuple);
        void doProcess(std::string &imageID);
        void Initialize(Json::Value conf, Json::Value context);

    private:
        memcached_st *memc;
};

class MontageBolt : public storm::BasicBolt
{
    public:
        MontageBolt(void);
        void Process(storm::Tuple &tuple);
        void Initialize(Json::Value conf, Json::Value context);
        void doProcess(std::string &reqID);
        void checkAllPending(void);

    private:
        memcached_st *memc;
        // requestID, [imageID, ...]
        std::map< std::string, std::list< std::string > > pending;
};

class JSONImageFeature : public Json::Value
{
    public:
        // serialize
        JSONImageFeature(cv::detail::ImageFeatures &f);

        // reconstitute
        JSONImageFeature(Json::Value &v);
        void GetImageFeatures(cv::detail::ImageFeatures &features,
                const void *descData);

        void GetDescKey(std::string &key);
        void *GetDescData(void);
        size_t GetDescLen(void)
        {
            return descriptors.elemSize() * descriptors.total();
        }

    private:
        JSONImageFeature(void);
        cv::Mat descriptors;
};

int memc_get(memcached_st *memc, const std::string &key,
        /* output */ void **val, size_t &len);

int memc_set(memcached_st *memc, const std::string &key,
        void *val, size_t len);

int memc_get_json(memcached_st *memc,
        const std::string &key, Json::Value &val);

int memc_set_json(memcached_st *memc,
        const std::string &key, Json::Value &val);

int memc_exists(memcached_st *memc,
        const std::string &key);

int memc_get_cvmat(memcached_st *memc, const std::string &imageID,
        cv::Mat &mat);

int memc_set_features(memcached_st *memc, const std::string &imageID,
        cv::detail::ImageFeatures &features);

int memc_get_features(memcached_st *memc, const std::string &imageID,
        cv::detail::ImageFeatures &features);

void resize_image(cv::Mat &image, unsigned int dim);

