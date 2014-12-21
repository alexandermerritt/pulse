/**
 * StormFuncs.cpp
 *
 * Executable must be named 'stormstitcher' and exist in a PATH directory.
 *
 * Tuples emitted and processed in this file must be
 *  - arrays (Json::Value treated with integer indeces)
 *  - have a number of elements (or 'Fields') equal to what is declared in the
 *  FuncsTopology.java file's Spout and Bolt implementations.
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
#include "StormFuncs.h"
#include "stitcher.hpp"
#include "Config.hpp"

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

BFS::BFS(void)
    : memc(nullptr), seed(0)
{
    char fname[64];
    snprintf(fname, 64, "userbolt-%d", getpid());
    if (init_log(fname))
        abort();
    L("BFS Bolt instantiated");
}

void BFS::Initialize(Json::Value conf, Json::Value context)
{
    L("BFS Bolt initialized");
#if 0
    errno = 0;
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc)
        abort();
#endif
}

void BFS::Process(storm::Tuple &tuple)
{
    Json::Value next;
    Json::Value v(tuple.GetValues());
    next[0] = v[0];
    for (int i = 0; i < 100; i++) {
        next[1] = v[1].asString() + "." + std::to_string(i);
        storm::Tuple tup(next);
        storm::Emit(tup);
    }
}

// -----------------------------------------------------------------------------


#if 0
FeatureBolt::FeatureBolt(void)
    : memc(NULL)
{
    char fname[64];
    snprintf(fname, 64, "featurebolt-%d", getpid());

    if (init_log(fname))
        abort();

    errno = 0;
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", config->memc.servers.c_str());
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
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc) {
        L("Error connecting to memcached: %s", strerror(errno));
        L("    config: %s", config->memc.servers.c_str());
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

#endif

/*
 * Executable functions
 */

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

int memc_set(memcached_st *memc, const std::string &key,
        void *val, size_t len)
{
    memcached_return_t mret;
    if (!memc || !val) return -1;
    mret = memcached_set(memc, key.c_str(), key.length(),
            (char*)val, len, 0, 0);
    return !(mret == MEMCACHED_SUCCESS);
}

int memc_exists(memcached_st *memc,
        const std::string &key)
{
    memcached_return_t mret;

    assert(memc);

    mret = memcached_exist(memc, key.c_str(), key.length());
    if (MEMCACHED_SUCCESS != mret) {
        //if (MEMCACHED_NOTFOUND == mret)
        return false;
    }

    return true;
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

void badusage(int argc, char *argv[])
{
    std::cerr << "Error: invalid command arguments:";
    for (int i = 0; i < argc; i++)
        std::cerr << " " << argv[i];
    std::cerr << std::endl;
    std::cerr << "Usage: "
        << std::endl << "    " << *argv << " --conf=/path/to/conf "
                                << CMD_ARG_SPOUT
        << std::endl << "    " << *argv << " --conf=/path/to/conf "
                                << CMD_ARG_BOLT << "=[user|feature|montage]"
        << std::endl;
    exit(-1);
}

// We are instantiated by ShellSpout or ShellBolt constructors from Java.
int main(int argc, char *argv[])
{
#if 1
    BFS bolt;
    bolt.Run();
#else
    char fname[64];
    snprintf(fname, 64, "userbolt-%d", getpid());
    if (init_log(fname))
        abort();
    L("BFS Bolt instantiated");

    std::string msg, line;
    bool read_line = false;
    while(1)
    {
        read_line = getline(std::cin, line).good();
        if (line == "end")
            break;
        else if (read_line)
            msg += line + "\n";
    }
    L("%s", msg.c_str());

    Json::Value root;
    Json::Reader reader;
    reader.parse(msg, root);
    L("%s", root.toStyledString().c_str());
    L("isArray: %s", (root.isArray() ? "true" : "false"));
#endif

#if 0
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

    if (argc != 3)
        badusage(argc, argv);

    {
        std::string confarg = std::string(argv[1]);
        auto pos = confarg.find_first_of('=');
        if (pos == std::string::npos)
            badusage(argc, argv);
        std::string sub = confarg.substr(0, pos);
        if (sub != CMD_ARG_CONF)
            badusage(argc, argv);
        sub = confarg.substr(pos + 1);
        if (init_config(sub)) {
            fprintf(stderr, "Error: cannot find config file: %s\n",
                    confarg.c_str());
            exit(-1);
        }
    }

    {
        std::string arg = std::string(argv[2]);
        auto pos = arg.find_first_of('=');
        if ((pos == std::string::npos) && arg == CMD_ARG_SPOUT) {
            GraphSpout spout;
            spout.Run();
        } else if (pos != std::string::npos) {
            std::string sub = arg.substr(0, pos);
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
    }
#endif

    return 0;
}

