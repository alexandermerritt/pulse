/**
 * StormFuncs.cpp
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
#include <memory>

// Local headers
#include "StormFuncs.h"
#include "stitcher.hpp"
#include "Config.hpp"

#include "Objects.pb.h" // generated
#include "cv/decoders.h"

StormFuncs::StormFuncs(void)
: memc(nullptr)
{ }

int StormFuncs::connect(std::string &servers)
{
    memc = memcached(servers.c_str(), servers.length());
    return !memc;
}

int StormFuncs::neighbors(std::string &vertex,
        std::deque<std::string> &others)
{
    if (vertex.length() == 0)
        return 0;
    storm::Vertex vobj;
    if (memc_get(memc, vertex, vobj))
        return -1;
    others.resize(vobj.followers_size());
    for (size_t i = 0; i < others.size(); i++)
        others[i] = vobj.followers(i);
    return 0;
}

int StormFuncs::imagesOf(std::string &vertex,
        std::deque<std::string> &keys)
{
    if (vertex.length() == 0)
        return 0;
    storm::Vertex vobj;
    if (memc_get(memc, vertex, vobj))
        return -1;
    keys.resize(vobj.images_size());
    for (size_t i = 0; i < keys.size(); i++)
        keys[i] = vobj.images(i);
    return 0;
}

int StormFuncs::feature(std::string &image_key)
{
    storm::Image iobj;
    if (memc_get(memc, image_key, iobj))
        return -1;

    void *data;
    size_t len;
    if (memc_get(memc, iobj.key_data(), &data, len))
        return -1;
    if (!data || len == 0)
        return -1;

    cv::Mat mat;
    mat = jpeg::JPEGasMat(data, len);
    if (!mat.data || mat.cols < 1 || mat.rows < 1)
        return -1;

#if 0
    cv::Ptr<cv::detail::FeaturesFinder> finder;
    cv::detail::ImageFeatures features;
    finder = new detail::OrbFeaturesFinder();
    try {
        // may segfart. if so, remove offending images from input
        (*finder)(img, features);
    } catch (Exception &e) {
    }
    finder->collectGarbage();
#endif

    return 0;
}

#if 0
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

int memc_get(memcached_st *memc, const std::string &key, // input params
        void **val, size_t &len) // output params
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

int memc_get(memcached_st *memc, const std::string &key,
        google::protobuf::MessageLite &msg)
{
    size_t len(0);
    void *val(nullptr);

    if (!memc || key.length() == 0)
        return -1;

    if (memc_get(memc, key, &val, len))
        return -1;

    if (!msg.ParseFromArray(val, len)) {
        free(val);
        return -1;
    }

    free(val);
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

int memc_set(memcached_st *memc, const std::string &key,
        google::protobuf::MessageLite &msg)
{
    size_t len(0);
    void *val(nullptr);

    if (!memc || key.length() == 0)
        return -1;

    len = msg.ByteSize();
    if (!(val = malloc(len)))
        return -1;

    if (!msg.SerializeToArray(val, len)) {
        return -1;
    }

    int ret = memc_set(memc, key, val, len);
    free(val);
    return ret;
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
                                << CMD_ARG_BOLT << "=[user|feature|montage]"
        << std::endl;
    exit(-1);
}

#if 0
// We are instantiated by ShellSpout or ShellBolt constructors from Java.
int main(int argc, char *argv[])
{
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
        if (pos != std::string::npos) {
            std::string sub = arg.substr(0, pos);
            if (sub != CMD_ARG_BOLT)
                badusage(argc, argv);
            sub = arg.substr(pos + 1);
            if (sub == CMD_ARG_BFS) {
                BFS bolt;
                bolt.Run();
            } else if (sub == CMD_ARG_MONTAGE) {
                Montage bolt;
                bolt.Run();
#if 0
            } else if (sub == CMD_ARG_REQSTAT) {
                ReqStatBolt bolt;
                bolt.Run();
#endif
            } else {
                badusage(argc, argv);
            }
        } else {
            badusage(argc, argv);
        }
    }

    return 0;
}
#endif

