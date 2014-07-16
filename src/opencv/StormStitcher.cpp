/**
 * StormStitcher.cpp
 *
 * Executable must be named 'stormstitcher' and exist in a PATH directory.
 *
 * Tuples emitted and processed in this file must be
 *  - arrays (Json::Value treated with integer indeces)
 *  - have a number of elements (or 'Fields') equal to what is declared in the
 *  StitcherTopology.java file's Spout and Bolt implementations.
 *
 * Refer to load_graph.cpp comment at top of file for JSON formats and structure
 * of data in memcached.
 */

// C headers
#include <string.h>
#include <unistd.h>
#include <libmemcached/memcached.h>
#include <uuid/uuid.h>

#include <sys/types.h>
#include <sys/socket.h>

// C++ headers
#include <iostream>
#include <json/json.h>
#include <opencv2/opencv.hpp>

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
 * JSONImageFeature
 */

JSONImageFeature::JSONImageFeature(cv::detail::ImageFeatures &f)
{
    (*this)["img_idx"] = f.img_idx;
    (*this)["width"]   = f.img_size.width;
    (*this)["height"]  = f.img_size.height;

    (*this)["num_keypoints"] = std::to_string(f.keypoints.size());
    Json::Value keypoints;
    for (auto &kp : f.keypoints) {
        Json::Value kj;
        kj["pt.x"]      = kp.pt.x;
        kj["pt.y"]      = kp.pt.y;
        kj["octave"]    = kp.octave;
        kj["class_id"]  = kp.class_id;
        // JSON doesn't handle float properly, so convert to string
        kj["size"]      = std::to_string(kp.size);
        kj["angle"]     = std::to_string(kp.angle);
        kj["response"]  = std::to_string(kp.response);
        keypoints.append(kj);
    }
    (*this)["keypoints"] = keypoints;

    Json::Value desc;
    uuid_t uuid;
    uuid_generate(uuid);
    char _uuid[64];
    uuid_unparse(uuid, _uuid);
    desc["data_key"] = std::string(_uuid); // cannot put matrix into JSON
    desc["cols"]     = f.descriptors.cols;
    desc["rows"]     = f.descriptors.rows;
    desc["type"]     = f.descriptors.type();
    desc["flags"]    = f.descriptors.flags;
    desc["pxsz"]     = static_cast<int>(f.descriptors.elemSize());
    (*this)["descriptor"] = desc;

    descriptors = f.descriptors;
}

JSONImageFeature::JSONImageFeature(Json::Value &v)
{
    abort();
}

void JSONImageFeature::GetDescKey(std::string &key)
{
    key = (*this)["descriptor"]["data_key"].asString();
}

void* JSONImageFeature::GetDescData(void)
{
    return descriptors.data;
}

void JSONImageFeature::GetImageFeatures(cv::detail::ImageFeatures &features,
        const void *descData)
{
    abort();
}

/*
 * UserBolt
 *
 * Retrieve and emit list of images associated with "user" (nodeID).
 */

UserBolt::UserBolt(void)
    : memc(NULL)
{
    // can't log anything here
}

void UserBolt::Initialize(Json::Value conf, Json::Value context)
{
    if (init_log("userbolt"))
        abort();

    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        abort();
    }

    L("UserBolt initialized");
}

void UserBolt::Process(storm::Tuple &tuple)
{
    memcached_return_t mret;
    Json::Value v(tuple.GetValues());
    size_t len;
    uint32_t flags;

    std::string nodeID = v[1].asString();
    L("Process() got %s", nodeID.c_str());

    // value is metadata describing node/user
    void *keyval = memcached_get(memc, nodeID.c_str(), nodeID.length(),
            &len, &flags, &mret);
    if (!keyval || (mret != MEMCACHED_SUCCESS)) {
        L("Error retrieving %s from memc", nodeID.c_str());
        abort();
    }
    std::string valstr((char*)keyval);
    free(keyval);
    Json::Reader r;
    Json::Value nodeInfo;
    if (!r.parse(valstr, nodeInfo, false)) {
        L("error parsing json for '%s'", nodeID.c_str());
        abort();
    }
    L("parsed %s", nodeID.c_str());

    // emit all images
    Json::Value images = nodeInfo["images"];
    L("nodeInfo['images'] has %d entries", images.size());
    for (unsigned int i = 0; i < images.size(); i++) {
        Json::Value next;
        next[0] = v[0]; // group ID
        next[1] = images[i];
        storm::Tuple tup(next);
        L("    emitting %s", next[1].asCString());
        storm::EmitSpout(tup, std::string(), -1, std::string());
    }
    L("done parsing nodeInfo['images']");
}

/*
 * FeatureBolt
 *
 * Given the image, retrieve from object store and find features.
 * Store features into object store.
 * Emit imageID to indicate completion.
 */

FeatureBolt::FeatureBolt(void)
    : memc(NULL)
{
    if (init_log("featurebolt"))
        abort();

    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        abort();
    }

    L("FeatureBolt initialized");
}

void FeatureBolt::Initialize(Json::Value conf, Json::Value context)
{
}

// separate function to allow for isolated debugging
void FeatureBolt::doProcess(std::string &imageID)
{
    memcached_return_t mret;
    size_t len;
    uint32_t flags;

    // keyval is JSON metadata describing image
    void *keyval = memcached_get(memc, imageID.c_str(), imageID.length(),
            &len, &flags, &mret);
    if (!keyval || (mret != MEMCACHED_SUCCESS)) {
        L("Error retrieving %s from memc", imageID.c_str());
        abort();
    }
    std::string valstr((char*)keyval);
    free(keyval);
    Json::Reader r;
    Json::Value imageInfo;
    if (!r.parse(valstr, imageInfo, false)) {
        L("error parsing json for '%s'", imageID.c_str());
        abort();
    }

    // 1. construct opencv image
    // 2. run feature detection on it
    // 3. store feature set back into memcached
    std::string rawkey = imageInfo["data_key"].asString();
    int cols, rows, type, pxsz, iflags;
    cols = imageInfo["cols"].asInt();
    rows = imageInfo["rows"].asInt();
    type = imageInfo["type"].asInt();
    pxsz = imageInfo["pxsz"].asInt();
    iflags = imageInfo["flags"].asInt();

    // fetch raw image data
    L("fetching raw image data at %s", rawkey.c_str());
    keyval = memcached_get(memc, rawkey.c_str(), rawkey.length(),
            &len, &flags, &mret);
    if (!keyval || (mret != MEMCACHED_SUCCESS)) {
        L("Error retrieving %s from memc", rawkey.c_str());
        abort();
    }
    // copy over data to object XXX bad for large objects...
    L("copying raw data to new cv::Mat");
    cv::Mat cvimg(rows, cols, type);
    assert(cvimg.data);
    memcpy(cvimg.data, keyval, len);
    free(keyval);

    // execute feature detection on image
    L("executing feature detection");
    cv::Ptr<cv::detail::FeaturesFinder> finder; // TODO make into class state
    cv::detail::ImageFeatures features;
    // TODO resize image before detection
    // TODO change params of orb to alter intensity of search, or use SURF
    // (sometimes not compiled into libraries)
    finder = new detail::OrbFeaturesFinder();
    try {
        (*finder)(cvimg, features);
    } catch (Exception &e) {
        L("    EXCEPTION caught");
        return;
    }
    finder->collectGarbage();
    // TODO measure time taken for computation & store in memc?

    // store features into memcached
    // first store the json metadata
    L("serializing keypoints");
    JSONImageFeature ji(features);
    Json::FastWriter w;
    std::string s = w.write(ji);
    std::string fkey = imageInfo["features_key"].asString();
    L("writing keypoints to memc at %s with size %lu",
            fkey.c_str(), s.size());
    mret = memcached_set(memc, fkey.c_str(), fkey.length(),
            s.c_str(), s.length(), 0, 0);
    if (MEMCACHED_SUCCESS != mret) {
        L("Error storing %s to memc", fkey.c_str());
        abort();
    }

    // second, store the raw descriptor matrix
    std::string desckey;
    ji.GetDescKey(desckey);
    L("writing keypoint descriptor matrix to memc at %s", desckey.c_str());
    mret = memcached_set(memc, desckey.c_str(), desckey.length(),
            (char*)ji.GetDescData(), ji.GetDescLen(), 0, 0);
    if (MEMCACHED_SUCCESS != mret) {
        L("Error storing %s to memc", rawkey.c_str());
        abort();
    }

}

void FeatureBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());
    std::string imageID = v[1].asString();
    L("Process() got %s", imageID.c_str());
    doProcess(imageID);
    // TODO emit something
    L("not emitting. done.");
}

/*
 * GraphSpout
 *
 * Pick random "person" (nodeID in graph).
 *      Emit all neighbor nodes.
 *      If depth >0, set a neighbor as current "person" and repeat.
 */

GraphSpout::GraphSpout(void)
    : memc(NULL),
    // XXX To handle multiple spouts, we provide each with a namespace of
    // group_id:    <pid>0000000000..00
    // Each iteration increments in that space
    group_id(getpid() << 20),
    max_depth(4)
{
    if (init_log("spout"))
        abort();
    assert(max_depth > 0);
}

int GraphSpout::initmemc(void)
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

void GraphSpout::Initialize(Json::Value conf, Json::Value context)
{
    if (initmemc())
        abort();
}

// Pick random key, emit nodes while walking some steps.
void GraphSpout::NextTuple(void)
{
    Json::Value v;
    memcached_return_t mret;
    uint32_t flags;
    size_t len;
    int key;
    std::string keystr;
    void *keyval = NULL;
    int num_nodes = graph_max() + 1;
    std::string groupstr = std::to_string(group_id++);

    L("NextTuple called");

    key = rand_r(&seed) % num_nodes;
    keystr = std::to_string(key);

    int depth = 1 + (rand_r(&seed) % max_depth);
    L("depth %d", depth);
    for (int i = 0; i < depth; i++) {

        L("picked %d", key);
        mret = memcached_exist(memc, keystr.c_str(), keystr.length());
        if (MEMCACHED_SUCCESS != mret) {
            if (MEMCACHED_NOTFOUND == mret)
                L("'%s' not found", info_key);
            else
                L("error querying existence: %s",
                        memcached_strerror(memc, mret));
            abort();
        }

        keyval = memcached_get(memc, keystr.c_str(), keystr.length(),
                &len, &flags, &mret);
        if (!keyval) {
            L("could not retreive %s: %s", keystr.c_str(),
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

        // emit all neighbors
        Json::Value neighbors = v["neighbors"];
        L("%d has %d neighbors, emitting", key, neighbors.size());
        for (unsigned int i = 0; i < neighbors.size(); i++) {
            Json::Value next;
            next[0] = groupstr;
            next[1] = neighbors[i];
            L("    emitting %s", neighbors[i].asCString());
            storm::Tuple tup(next);
            storm::EmitSpout(tup, std::string(), -1, std::string());
        }

        // iterate
        key = atoi(neighbors[0].asCString()); // TODO this is not BFS
        keystr = std::to_string(key);
    }
    L("sleeping 5");
    sleep(5); // XXX hack
}

/*
 * Executable functions
 */

void badusage(int argc, char *argv[])
{
    std::cerr << "Usage: "
        << std::endl << "    " << *argv << " " << CMD_ARG_SPOUT
        << std::endl << "    " << *argv << " " << CMD_ARG_BOLT << "=[user|feature]"
        << std::endl;
    exit(-1);
}

#include <sys/ptrace.h>

// We are instantiated by ShellSpout or ShellBolt constructors from Java.
int main(int argc, char *argv[])
{
    std::string arg, sub;

    // test if we are inside debugger; avoid all the other setup stuff
    if (ptrace(PTRACE_TRACEME, 0, NULL, NULL)) {
        std::string imageID; // initialize in debugger
        FeatureBolt bolt;
        bolt.doProcess(imageID);
        return 0;
    }

    if (argc != 2)
        badusage(argc, argv);
    arg = std::string(argv[1]);

    auto pos = arg.find_first_of('=');
    if ((pos == std::string::npos) && arg == CMD_ARG_SPOUT) {
        GraphSpout spout;
        spout.Run();
    } else if (pos != std::string::npos) {
        sub = arg.substr(0, pos);
        if (sub != CMD_ARG_BOLT)
            badusage(argc, argv);
        sub = arg.substr(pos + 1);
        if (sub == CMD_ARG_FEATURE) {
            FeatureBolt bolt;
            bolt.Run();
        } else if (sub == CMD_ARG_USER) {
            UserBolt bolt;
            bolt.Run();
        } else {
            badusage(argc, argv);
        }
    } else {
        badusage(argc, argv);
    }

    return 0;
}

