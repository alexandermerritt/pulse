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
#include <signal.h>

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

static void make_uuid(std::string &uuid_str)
{
    uuid_t uuid;
    uuid_generate(uuid);
    char _uuid[64];
    uuid_unparse(uuid, _uuid);
    uuid_str = std::string(_uuid);
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
    std::string uuid;
    make_uuid(uuid);
    desc["data_key"] = uuid; // cannot put matrix into JSON
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
    abort(); // TODO
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
    abort(); // TODO
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
    char fname[64];
    snprintf(fname, 64, "userbolt-%d", getpid());

    if (init_log(fname))
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

// Update metadata in memc to indicate more work has been added
// to this request ID. Used by montagebolt to figure out
// how many images to process.
void UserBolt::emitToReqStats(std::string &requestID,
        std::string &userID, unsigned int numImages)
{
    Json::Value v;
    v[0] = requestID;
    v[1] = userID;
    v[2] = numImages;
    storm::Tuple tup(v);
    L("    emitting info to ReqStatBolt : %s, %s, %u",
            requestID.c_str(), userID.c_str(), numImages);
    storm::Emit(tup, std::string("toStats"), -1);
}

void UserBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());

    std::string requestID = v[0].asString();
    std::string userID    = v[1].asString();
    L("Process() got user %s", userID.c_str());

    // value is metadata describing node/user
    Json::Value userInfo;
    if (memc_get_json(memc, userID, userInfo))
        abort();
    L("parsed %s", userID.c_str());

    // emit all user's images
    Json::Value images = userInfo["images"];
    L("userInfo['images'] has %d entries", images.size());
    emitToReqStats(requestID, userID, images.size());
    for (unsigned int i = 0; i < images.size(); i++) {
        Json::Value next;
        next[0] = requestID;
        next[1] = images[i];
        storm::Tuple tup(next);
        L("    emitting imageID %s to FeatureBolt", next[1].asCString());
        storm::Emit(tup, std::string("toFeature"), -1);
    }
    L("done parsing userInfo['images']");
}

/*
 * ReqStatBolt
 */

ReqStatBolt::ReqStatBolt(void)
    : memc(NULL)
{
}

void ReqStatBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());
    struct stat *stat = NULL;

    std::string reqID = v[0].asString();
    //std::string userID = v[1].asString(); // ignored for now
    int numImages = v[2].asInt();

    L("Process() with ( %s , %d )",
            reqID.c_str(), numImages);

    // check if new reqID
    auto search = cache.find(reqID);
    if (search == cache.end()) {
        L("    this is new request");
        Json::Value v;
        if (memc_get_json(memc, reqID, v))
            abort();
        struct stat s;
        s.complete    = 0; // == false
        s.totalImages = 0;
        s.updatesRem  = v[ ReqStatBolt::updatesRem_str() ].asInt();
        s.numUsers    = v[ ReqStatBolt::numUsers_str() ].asInt();
        cache.emplace(reqID, s);
    }

    stat = &cache.at(reqID);
    stat->updatesRem--;
    stat->totalImages += numImages;

    assert(stat->updatesRem >= 0);

    // this tells MontageBolt to do its work
    if (stat->updatesRem == 0) {
        L("    request image list known: %d with %d users",
                stat->totalImages, stat->numUsers);
        Json::Value v;
        v[ ReqStatBolt::updatesRem_str() ] = 0;
        v[ ReqStatBolt::numUsers_str() ]   = stat->numUsers;
        v[ ReqStatBolt::totImg_str() ]     = stat->totalImages;
        v[ ReqStatBolt::complete_str() ]   = 1; // == true
        if (memc_set_json(memc, reqID, v))
            abort();
    }
}

void ReqStatBolt::Initialize(Json::Value conf, Json::Value context)
{
    char fname[64];
    snprintf(fname, 64, "reqstatbolt-%d", getpid());

    if (init_log(fname))
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
    char fname[64];
    snprintf(fname, 64, "featurebolt-%d", getpid());

    if (init_log(fname))
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
    cv::Mat img;
    Json::Value imageInfo;

    L("doProcess on %s", imageID.c_str());

    if (memc_get_json(memc, imageID, imageInfo))
        abort();

    if (memc_get_cvmat(memc, imageID, img))
        abort();

    L("    executing feature detection");
    cv::Ptr<cv::detail::FeaturesFinder> finder;
    cv::detail::ImageFeatures features;
    // TODO resize image
    // TODO change params
    finder = new detail::OrbFeaturesFinder();
    try {
        // may segfart. if so, remove offending images from input
        (*finder)(img, features);
    } catch (Exception &e) {
        L("    EXCEPTION caught");
        return;
    }
    finder->collectGarbage();
    // TODO measure time taken for computation & store in memc?

    if (memc_set_features(memc, imageID, features))
        abort();
}

void FeatureBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());

    std::string imageID = v[1].asString();
    L("Process() got %s", imageID.c_str());
    doProcess(imageID);

    L("    emitting %s, %s", v[0].asString().c_str(), imageID.c_str());
    storm::Emit(tuple, std::string(), -1);
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
    char fname[64];
    snprintf(fname, 64, "spout-%d", getpid());

    if (init_log(fname))
        abort();

    if (initmemc())
        abort();

    assert(max_depth > 0);
}

int GraphSpout::initmemc(void)
{
    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        return -1;
    }
    // FIXME sometimes memc was allocated despite errors connecting

    if (!memc_exists(memc, info_key))
        abort();
    if (memc_get_json(memc, info_key, graph_info))
        abort();
    L("max from '%s': %s", info_key, graph_info["max"].asCString());

    seed = time(NULL) + getpid();
    srand(seed);

    return 0;
}

void GraphSpout::Initialize(Json::Value conf, Json::Value context)
{
    // nothing
}

void GraphSpout::initReqStats(std::string &requestID,
        unsigned int numUsers)
{
    Json::Value v;

    v[ ReqStatBolt::numUsers_str() ]   = numUsers;
    v[ ReqStatBolt::updatesRem_str() ] = numUsers;
    v[ ReqStatBolt::totImg_str() ]     = 0;
    v[ ReqStatBolt::complete_str() ]   = 0; // == false

    L("    adding new request stats for %s, %d users",
            requestID.c_str(), numUsers);
    if (memc_set_json(memc, requestID, v))
        abort();
}

// Pick random key, emit nodes while walking some steps.
void GraphSpout::NextTuple(void)
{
    Json::Value v;
    int key;
    std::string keystr;
    int num_nodes = graph_max() + 1;
    std::string requestID = "req-" + std::to_string(group_id++);

    L("NextTuple called - request %s", requestID.c_str());

    key = rand_r(&seed) % num_nodes;
    keystr = std::to_string(key);

    int req_num_users = 0;
    std::list< storm::Tuple > toEmit;

    int depth = 1 + (rand_r(&seed) % max_depth);
    L("depth %d", depth);
    for (int i = 0; i < depth; i++) {
        L("picked %d", key);
        if (!memc_exists(memc, keystr))
            abort();
        if (memc_get_json(memc, keystr, v))
            abort();
        // emit all neighbors
        Json::Value neighbors = v["neighbors"];
        L("%d has %d neighbors, emitting", key, neighbors.size());
        // TODO emit images of self too?
        for (unsigned int i = 0; i < neighbors.size(); i++) {
            Json::Value next;
            next[0] = requestID;
            next[1] = neighbors[i];
            L("    buffering for emit: %s", neighbors[i].asCString());
            storm::Tuple tup(next);
            toEmit.push_back(tup);
        }
        req_num_users += neighbors.size();

        // iterate
        key = atoi(neighbors[0].asCString()); // TODO this is not BFS
        keystr = std::to_string(key);
    }

    // avoidance of race condition: emitting a new tuple may trigger ReqStatBolt
    // to query memc for requestID key before we are able to add it. So we
    // buffer all tuples first, then write request ID to memc, then emit all
    // tuples.
    initReqStats(requestID, req_num_users);
    L("    emitting all tuples");
    for (auto &tup : toEmit)
        storm::EmitSpout(tup, std::string(), -1, std::string());

    int sleeptime = 2;
    L("sleeping %d", sleeptime);
    sleep(sleeptime); // XXX hack
}

/*
 * MontageBolt
 *
 * Put images together
 */

MontageBolt::MontageBolt(void)
    : memc(NULL)
{
    char fname[64];
    snprintf(fname, 64, "montagebolt-%d", getpid());

    if (init_log(fname))
        abort();

    errno = 0;
    memc = memcached(memc_config, strlen(memc_config));
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", memc_config);
        abort();
    }

    L("MontageBolt initialized");
}

void MontageBolt::doProcess(std::string &reqID)
{
    L("    (todo) request %s completed; making montage", reqID.c_str());
    std::list<   std::string > &imageIDList(pending[reqID]);
    int num_images = imageIDList.size();
    std::vector< cv::Mat > images(num_images);
    int dim = 512;

    L("    pulling in images");
    size_t idx = 0;
    for (std::string &id : imageIDList) {
        if (memc_get_cvmat(memc, id, images[idx]))
            abort();
        resize_image(images[idx], dim);
        idx++;
    }

    assert(images[0].rows == dim);
    assert(images[0].cols == dim);

    // create one unified image with each pasted into it
    int b = (int)(std::ceil(num_images * (9./16.))); // images per row
    int h = (int)std::ceil((float)num_images / b);   // images per col
    int cols = (int)(b * dim); // pixels per row
    int rows = (int)(h * dim); // pixels per col
    L("    montage template (%d, %d, type=0x%x", rows, cols, CV_8UC1);
    cv::Mat montage(rows, cols, CV_8UC1);
    int row = 0, col = 0;
    for (int i = 0; i < num_images; i++) {
        cv::Rect roi(col * dim, row * dim, dim, dim);
        L("    roi(%d, %d, %d, %d)", col*dim, row*dim, dim, dim);
        cv::Mat view = montage(roi);
        images[i].copyTo(view);
        col++;
        if (i == (b - 1)) {
            row++;
            col = 0;
        }
    }

    // stuff montage into object store and link with reqID
    // XXX ideally should write image json, too
    std::string uuid;
    make_uuid(uuid);
    if (memc_set(memc, uuid, montage.data,
                montage.elemSize() * montage.total()))
        abort();
    Json::Value v;
    if (memc_get_json(memc, reqID, v))
        abort();
    v["montage"] = uuid;
    if (memc_set_json(memc, reqID, v))
        abort();
}

void MontageBolt::checkAllPending(void)
{
    std::list<std::string> torm;

    for (auto &p : pending) {
        std::string reqID(p.first);
        std::list< std::string > &images(p.second);
        
        Json::Value v;
        if (memc_get_json(memc, reqID, v))
            abort();

        int totImgs = v[ ReqStatBolt::totImg_str() ] .asInt();
        int completed = v[ ReqStatBolt::complete_str() ].asInt();
        if (completed && ((int)images.size() == totImgs)) {
            doProcess(reqID);
            torm.push_back(reqID);
        }
    }

    L("    erasing completed items from pending list");
    for (std::string &id : torm)
        pending.erase(pending.find(id));
}

void MontageBolt::Process(storm::Tuple &tuple)
{
    Json::Value v(tuple.GetValues());
    std::string reqID   = v[0].asString();
    std::string imageID = v[1].asString();

    L("Process() got ( %s , %s )", reqID.c_str(), imageID.c_str());
    pending[reqID].push_back(imageID);

    checkAllPending();
}

void MontageBolt::Initialize(Json::Value conf, Json::Value context)
{
    // nothing
}

/*
 * Executable functions
 */

void badusage(int argc, char *argv[])
{
    std::cerr << "Error: invalid command arguments:";
    for (int i = 0; i < argc; i++)
        std::cerr << " " << argv[i];
    std::cerr << std::endl;
    std::cerr << "Usage: "
        << std::endl << "    " << *argv << " " << CMD_ARG_SPOUT
        << std::endl << "    " << *argv << " " << CMD_ARG_BOLT << "=[user|feature|montage]"
        << std::endl;
    exit(-1);
}

int memc_get(memcached_st *memc, const std::string &key,
        void **val, size_t &len)
{
    memcached_return_t mret;
    uint32_t flags;
    if (!memc || !val) return -1;
    *val = memcached_get(memc, key.c_str(), key.length(),
            &len, &flags, &mret);
    if (!*val || (mret != MEMCACHED_SUCCESS))
        return -1;
    return 0;
}

int memc_get_json(memcached_st *memc,
        const std::string &key, Json::Value &val)
{
    size_t len;
    void *_val;
    int ret;

    assert(memc);

    if (memc_get(memc, key, &_val, len))
        return -1;

    std::string valstr((char*)_val);
    Json::Reader r;
    ret = !r.parse(valstr, val, false);
    free(_val);

    return ret;
}

int memc_set(memcached_st *memc, const std::string &key,
        void *val, size_t len)
{
    memcached_return_t mret;
    if (!memc || !val) return -1;
    mret = memcached_set(memc, key.c_str(), key.length(),
            (char*)val, len, 0, 0);
    return !(mret == MEMCACHED_SUCCESS);
}

int memc_set_json(memcached_st *memc,
        const std::string &key, Json::Value &val)
{
    assert(memc);

    Json::FastWriter w;
    std::string valstr = w.write(val);
    if (memc_set(memc, key, (void*)valstr.c_str(), valstr.length()))
        return -1;

    return 0;
}

int memc_exists(memcached_st *memc,
        const std::string &key)
{
    memcached_return_t mret;

    assert(memc);

    mret = memcached_exist(memc, info_key, strlen(info_key));
    if (MEMCACHED_SUCCESS != mret) {
        if (MEMCACHED_NOTFOUND == mret)
            L("'%s' not found in memcached", info_key);
        else
            L("error querying memcached for %s : %s", info_key,
                    memcached_strerror(memc, mret));
        return false;
    }

    return true;
}

int memc_get_cvmat(memcached_st *memc, const std::string &imageID,
        cv::Mat &mat)
{
    memcached_return_t mret;
    Json::Value imageInfo;
    uint32_t flags;
    size_t len;
    int cols, rows, type, iflags;

    if (!memc)
        return -1;

    // keyval is JSON metadata describing image
    if (memc_get_json(memc, imageID, imageInfo))
        return -1;

    std::string data_key = imageInfo["data_key"].asString();
    cols = imageInfo["cols"].asInt();
    rows = imageInfo["rows"].asInt();
    type = imageInfo["type"].asInt();
    iflags = imageInfo["flags"].asInt();

    // now we fetch raw image data
    void *keyval = memcached_get(memc, data_key.c_str(), data_key.length(),
            &len, &flags, &mret);
    if (!keyval || (mret != MEMCACHED_SUCCESS))
        return -1;

    // copy over data to object XXX bad for large objects...
    mat = cv::Mat(rows, cols, type);
    if (!mat.data)
        return -1;
    memcpy(mat.data, keyval, len);
    free(keyval);
    mat.flags = iflags;

    return 0;
}

int memc_set_features(memcached_st *memc, const std::string &imageID,
        cv::detail::ImageFeatures &features)
{
    Json::Value imageInfo;
    memcached_return_t mret;

    if (!memc)
        return -1;

    if (memc_get_json(memc, imageID, imageInfo))
        return -1;

    // store features json
    std::string fkey = imageInfo["features_key"].asString();
    JSONImageFeature ji(features);
    if (memc_set_json(memc, fkey, ji))
        abort();

    // store feature desc matrix (raw data)
    std::string desckey;
    ji.GetDescKey(desckey);
    mret = memcached_set(memc, desckey.c_str(), desckey.length(),
            (char*)ji.GetDescData(), ji.GetDescLen(), 0, 0);
    if (MEMCACHED_SUCCESS != mret)
        return -1;

    return 0;
}

int memc_get_features(memcached_st *memc, const std::string &imageID,
        cv::detail::ImageFeatures &features)
{
    return -1;
}

void resize_image(cv::Mat &img, unsigned int dim)
{
    cv::Mat copy(img), scaled(img);
    float scaleby =  (float)dim / std::min(img.rows, img.cols);
    cv::cvtColor(img, copy, CV_BGR2GRAY);
    cv::resize(copy, scaled, cv::Size(), scaleby, scaleby);
    cv::Rect roi(0, 0, dim, dim);
    img = scaled(roi);
}

// We are instantiated by ShellSpout or ShellBolt constructors from Java.
int main(int argc, char *argv[])
{
    std::string arg, sub;

    // modify in debugger
    volatile bool debug = false;
    if (debug) {
        volatile int cmd = 0;
        if (cmd == 0) {
            GraphSpout s;
            s.NextTuple();
        } else if (cmd == 1) {
            MontageBolt bolt;
            std::string input("bull");
            bolt.doProcess(input);
        }
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
        } else if (sub == CMD_ARG_MONTAGE) {
            MontageBolt bolt;
            bolt.Run();
        } else if (sub == CMD_ARG_REQSTAT) {
            ReqStatBolt bolt;
            bolt.Run();
        } else {
            badusage(argc, argv);
        }
    } else {
        badusage(argc, argv);
    }

    return 0;
}

