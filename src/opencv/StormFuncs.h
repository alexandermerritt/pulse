/**
 * StitcherTopology.h
 */

#pragma once

#include <stdlib.h>
#include <deque>

#include <opencv2/opencv.hpp>
#include <opencv2/stitching/detail/matchers.hpp>

#include "StormWrapper.h"
#include "Config.hpp"
#include <google/protobuf/message_lite.h>
#include <json/json.h>

#include "Objects.pb.h" // generated
#include <libmemcached/memcached.h>

const char CMD_ARG_DELIM     = '=';
const char CMD_ARG_CONF[]    = "--conf";
const char CMD_ARG_BOLT[]    = "--bolt";
const char CMD_ARG_BFS[]     = "bfs";
const char CMD_ARG_FEATURE[] = "feature";
const char CMD_ARG_USER[]    = "user";
const char CMD_ARG_MONTAGE[] = "montage";

int init_log(const char *prefix);

extern FILE *logfp;

#define L(str, ...) \
    do { \
        fprintf(logfp, "%s::%s() " str "\n", __FILE__, __func__, ##__VA_ARGS__); \
        fflush(logfp); \
    } while (0)

class StormFuncs
{
    public:
        StormFuncs(void);
        int connect(std::string &servers);
        // graph-based functions
        int neighbors(std::string &vertex,
                std::deque<std::string> &others);
        int imagesOf(std::string &vertex,
                std::deque<std::string> &keys);
        // image-processing
        int feature(std::string &image_key, int &found);
        int match(std::deque<std::string> &imgkeys,
                std::deque<cv::detail::MatchesInfo> &matches);
        // TODO montage

    private:
        memcached_st *memc;

        int do_match(std::deque<cv::detail::ImageFeatures> &features,
                std::deque<cv::detail::MatchesInfo> &matches);

        int do_match_on(cv::Ptr<cv::detail::FeaturesMatcher> &matcher,
                const cv::detail::ImageFeatures &f1,
                const cv::detail::ImageFeatures &f2,
                cv::detail::MatchesInfo &matches_info,
                size_t thresh1 = 6, size_t thresh2 = 6);

        inline void marshal(cv::detail::ImageFeatures &cv_feat,
                storm::ImageFeatures &pb_feat,
                std::string &key);
        inline int unmarshal(cv::detail::ImageFeatures &cv_feat,
                const storm::ImageFeatures &fobj);

        inline void marshal(cv::KeyPoint &cv_kp,
                storm::KeyPoint *kobj);
        inline void unmarshal(cv::KeyPoint &cv_kp,
                const storm::KeyPoint &kobj);
};

#if 0

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

#endif

int memc_get(memcached_st *memc, const std::string &key,
        google::protobuf::MessageLite &msg);

int memc_get(memcached_st *memc, const std::string &key,
        /* output */ void **val, size_t &len);

int memc_set(memcached_st *memc, const std::string &key,
        const void *val, size_t len);

int memc_set(memcached_st *memc, const std::string &key,
        const google::protobuf::MessageLite &msg);

int memc_exists(memcached_st *memc,
        const std::string &key);

void resize_image(cv::Mat &image, unsigned int dim);

