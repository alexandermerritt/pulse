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
#include "matchers.hpp"

//==--------------------------------------------------------------==//
// Public functions
//==--------------------------------------------------------------==//

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

int StormFuncs::feature(std::string &image_key, int &found)
{
    storm::Image iobj;
    if (memc_get(memc, image_key, iobj))
        return -1;

    // get image
    void *data;
    size_t len;
    if (memc_get(memc, iobj.key_data(), &data, len))
        return -1;
    if (!data || len == 0) {
        free(data);
        return -1;
    }

    // decode
    cv::Mat img;
    img = jpeg::JPEGasMat(data, len);
    if (!img.data || img.cols < 1 || img.rows < 1) {
        free(data);
        return -1;
    }

    free(data);

    // feature detect
    cv::Ptr<cv::detail::FeaturesFinder> finder;
    cv::detail::ImageFeatures features;
    // cv::resize(img, scaled, Size(), 0.4, 0.4); // optional
    // finder = new detail::OrbFeaturesFinder(); // CPU
#define SURF_PARAMS 4000.,1,6
    finder = new detail::SurfFeaturesFinderGpu(SURF_PARAMS);
#undef SURF_PARAMS
    try { (*finder)(img, features); } // may segvomit
    catch (Exception &e) { return -1; }
    finder->collectGarbage();
    found = features.keypoints.size();

    // update image with features key
    std::string key(iobj.key_id() + "::features");
    iobj.set_key_features(key);
    if (memc_set(memc, iobj.key_id(), iobj))
        return -1;

    // serialize and store features
    storm::ImageFeatures fobj;
    marshal(features, fobj, key);
    if (memc_set(memc, fobj.key_id(), fobj))
        return -1;
    const cv::Mat &cvmat = features.descriptors;
    if (cvmat.data) {
        len = cvmat.elemSize() * cvmat.total();
        if (memc_set(memc, fobj.mat().key_data(), cvmat.data, len))
            return -1;
    }

    return 0;
}

int StormFuncs::match(std::deque<std::string> &imgkeys,
        std::deque<cv::detail::MatchesInfo> &matches)
{
    std::deque<cv::detail::ImageFeatures> features;

    // get all the image features
    size_t i = 0;
    for (std::string &key : imgkeys) {
        storm::Image iobj;
        if (memc_get(memc, key, iobj))
            throw std::runtime_error("memc");
        if (iobj.has_key_features()) {
            storm::ImageFeatures fobj;
            if (memc_get(memc, iobj.key_features(), fobj))
                throw std::runtime_error("memc");
            cv::detail::ImageFeatures cvfeat;
            if (unmarshal(cvfeat, fobj))
                throw std::runtime_error("unmarshal");
            cvfeat.img_idx = i++;
            features.push_back(cvfeat);
        }
    }

    return do_match(features, matches);
}

int StormFuncs::montage(std::deque<std::string> &image_keys,
        std::string &montage_key)
{
    if (image_keys.size() < 4)
        return -1;

    // get all images
    std::deque<cv::Mat> images;
    images.resize(image_keys.size());
    for (size_t i = 0; i < images.size(); i++) {
        storm::Image iobj;
        if (memc_get(memc, image_keys[i], iobj))
            return -1;
        void *data; size_t len;
        if (memc_get(memc, iobj.key_data(), &data, len))
            return -1;
        images[i] = jpeg::JPEGasMat(data, len);
        free(data);
    }

    // create canvas and montage within it
    size_t area = 0;
    for (cv::Mat &img : images)
        area += img.rows * img.cols;
    area <<= 1;

    // 16:9 ratio for canvas
    cv::Size size;
    size.height   = (3 * (int)std::sqrt(area)) >> 2;
    size.width    = area / size.height;

    cv::Rect rect;
    rect.height = size.height >> 1;
    rect.width  = size.width >> 1;
    rect.x      = rect.width >> 2;
    rect.y      = rect.height >> 2;
    cv::Mat canvas(cv::Mat::zeros(size.height, size.width,
                        images[0].type()));

    std::random_device rd;
    std::mt19937 gen_rand(rd());
    std::uniform_int_distribution<> dis(0, rect.area());
    for (cv::Mat &img : images) {
        auto _scale = std::log1p(img.rows) * 400 / img.rows;
        cv::Mat scaled;
        cv::resize(img, scaled, cv::Size(), _scale, _scale);
        auto loc = dis(gen_rand);
        int x = (loc % rect.width) + rect.x;
        int y = (loc / rect.width) + rect.y;
        scaled.copyTo(canvas(cv::Rect(x, y, scaled.cols, scaled.rows)));
    }

    cv::Mat montage(canvas(rect));
    void *buf;
    size_t len;
    if (jpeg::MatToJPEG(montage, &buf, len))
        return -1;
    std::stringstream ss;
    for (int i = 0; i < 4; i++)
        ss << dis(gen_rand);
    if (memc_set(memc, ss.str(), buf, len))
        return -1;
    free(buf);
    buf = nullptr;
    montage_key = ss.str();

#if 0
    // test
    if (memc_get(memc, ss.str(), &buf, len))
        return -1;
    cv::Mat image = jpeg::JPEGasMat(buf, len);
    free(buf);
    imwrite("/tmp/montage.jpg", image);
#endif

    return 0;
}

//==--------------------------------------------------------------==//
// Private functions
//==--------------------------------------------------------------==//

int StormFuncs::do_match_on(
        cv::Ptr<cv::detail::FeaturesMatcher> &matcher,
        const cv::detail::ImageFeatures &f1,
        const cv::detail::ImageFeatures &f2,
        cv::detail::MatchesInfo &minfo,
        size_t thresh1, size_t thresh2)
{
    (*matcher)(f1, f2, minfo);

    // Check if it makes sense to find homography
    if (minfo.matches.size() < thresh1)
        throw std::runtime_error(std::string(__func__)
                + " matches < threshold");

    // Construct point-point correspondences for homography estimation
    cv::Mat src_points(1, minfo.matches.size(), CV_32FC2);
    cv::Mat dst_points(1, minfo.matches.size(), CV_32FC2);
    for (size_t i = 0; i < minfo.matches.size(); ++i) {
        const DMatch& m = minfo.matches[i];

        cv::Point2f p = f1.keypoints[m.queryIdx].pt;
        p.x -= f1.img_size.width * 0.5f;
        p.y -= f1.img_size.height * 0.5f;
        src_points.at<cv::Point2f>(0, static_cast<int>(i)) = p;

        p = f2.keypoints[m.trainIdx].pt;
        p.x -= f2.img_size.width * 0.5f;
        p.y -= f2.img_size.height * 0.5f;
        dst_points.at<cv::Point2f>(0, static_cast<int>(i)) = p;
    }

    // Find pair-wise motion
    minfo.H = findHomography(src_points, dst_points,
            minfo.inliers_mask, CV_RANSAC);
    if (std::abs(determinant(minfo.H))
            < numeric_limits<double>::epsilon())
        throw std::runtime_error(std::string(__func__)
                + " determinant too small");

    // Find number of inliers
    minfo.num_inliers = 0;
    for (size_t i = 0; i < minfo.inliers_mask.size(); ++i)
        if (minfo.inliers_mask[i])
            minfo.num_inliers++;

    // These coeffs are from paper M. Brown and D. Lowe. "Automatic
    // Panoramic Image Stitching using Invariant Features"
    minfo.confidence = minfo.num_inliers
        / (8 + 0.3 * minfo.matches.size());

    // Set zero confidence to remove matches between too close images,
    // as they don't provide additional information anyway. The
    // threshold was set experimentally.
    minfo.confidence = minfo.confidence > 3. ? 0. : minfo.confidence;

    // Check if we should try to refine motion
    if (static_cast<size_t>(minfo.num_inliers) < thresh2)
        throw std::runtime_error(std::string(__func__)
                + " num_inliers < threshold");

    // Construct point-point correspondences for inliers only
    src_points.create(1, minfo.num_inliers, CV_32FC2);
    dst_points.create(1, minfo.num_inliers, CV_32FC2);
    int inlier_idx = 0;
    for (size_t i = 0; i < minfo.matches.size(); ++i) {
        if (!minfo.inliers_mask[i])
            continue;

        const DMatch& m = minfo.matches[i];

        cv::Point2f p = f1.keypoints[m.queryIdx].pt;
        p.x -= f1.img_size.width * 0.5f;
        p.y -= f1.img_size.height * 0.5f;
        src_points.at<cv::Point2f>(0, inlier_idx) = p;

        p = f2.keypoints[m.trainIdx].pt;
        p.x -= f2.img_size.width * 0.5f;
        p.y -= f2.img_size.height * 0.5f;
        dst_points.at<cv::Point2f>(0, inlier_idx) = p;

        inlier_idx++;
    }

    // Rerun motion estimation on inliers only
    minfo.H = findHomography(src_points, dst_points, CV_RANSAC);
    return 0;
}

int StormFuncs::do_match(std::deque<cv::detail::ImageFeatures> &features,
        std::deque<cv::detail::MatchesInfo> &matches)
{
    matches.clear();
    cv::Ptr<cv::detail::FeaturesMatcher> matcher;
    const size_t num_images = features.size();

    Mat_<uchar> mask_ = Mat::ones(num_images, num_images, CV_8U);

    std::vector<std::pair<int,int>> near_pairs;
    for (size_t i = 0; i < num_images - 1; ++i)
        for (size_t j = i + 1; j < num_images; ++j)
            if (features[i].keypoints.size() > 0
                    && features[j].keypoints.size() > 0
                    && mask_(i, j))
                near_pairs.push_back(make_pair(i, j));

    matches.resize(num_images * num_images);

#if defined(_OPENMP)
#pragma omp parallel \
    private(matcher) \
    num_threads(num_threads)
#endif  /* _OPENMP */
    {
        matcher = new CpuMatcher(0.2f);
        // matcher = new GpuMatcher(0.2f);
#if defined(_OPENMP)
#pragma omp for
#endif  /* _OPENMP */
        for (size_t i = 0; i < near_pairs.size(); ++i)
        {
            int from = near_pairs[i].first;
            int to = near_pairs[i].second;
            int pair_idx = from*num_images + to;

            if (do_match_on(matcher, features[from], features[to],
                    matches[pair_idx]))
                return -1;

            matches[pair_idx].src_img_idx = from;
            matches[pair_idx].dst_img_idx = to;

            size_t dual_pair_idx = to*num_images + from;

            matches[dual_pair_idx] = matches[pair_idx];
            matches[dual_pair_idx].src_img_idx = to;
            matches[dual_pair_idx].dst_img_idx = from;

            if (!matches[pair_idx].H.empty())
                matches[dual_pair_idx].H = matches[pair_idx].H.inv();

            const size_t num = matches[dual_pair_idx].matches.size();
            for (size_t j = 0; j < num; ++j)
                std::swap(matches[dual_pair_idx].matches[j].queryIdx,
                        matches[dual_pair_idx].matches[j].trainIdx);
        }
    }

    return 0;
}

inline int StormFuncs::unmarshal(cv::detail::ImageFeatures &cv_feat,
        const storm::ImageFeatures &fobj)
{
    cv_feat.img_idx = fobj.img_idx();
    cv_feat.img_size.width = fobj.width();
    cv_feat.img_size.height = fobj.height();

    std::vector<cv::KeyPoint> &kp = cv_feat.keypoints;
    kp.resize(fobj.keypoints_size());
    for (int i = 0; i < fobj.keypoints_size(); i++)
        unmarshal(kp[i], fobj.keypoints(i));

    if (fobj.has_mat()) {
        const storm::Mat &mobj = fobj.mat();
        void *data; size_t len;
        if (memc_get(memc, mobj.key_data(), &data, len))
            return -1;
        cv::Mat &mat = cv_feat.descriptors;
        mat = cv::Mat(mobj.rows(), mobj.cols(),
                mobj.type(), static_cast<unsigned char*>(data));
        mat.flags = mobj.flags();
        mat.dims = mobj.dims();
    }

    return 0;
}

inline void StormFuncs::marshal(cv::detail::ImageFeatures &cv_feat,
        storm::ImageFeatures &fobj, std::string &key)
{
    fobj.Clear();
    fobj.set_key_id(key);
    fobj.set_img_idx(0); // XXX wtf is this used for
    fobj.set_width(cv_feat.img_size.width);
    fobj.set_height(cv_feat.img_size.height);
    for (auto &kp : cv_feat.keypoints)
        marshal(kp, fobj.add_keypoints());

    const cv::Mat &desc = cv_feat.descriptors;
    if (desc.data) {
        storm::Mat *mobj = fobj.mutable_mat();
        mobj->set_flags(desc.flags);
        mobj->set_dims(desc.dims);
        mobj->set_rows(desc.rows);
        mobj->set_cols(desc.cols);
        mobj->set_type(desc.type());
        mobj->set_key_data(key + "::desc_data");
    }
}

inline void StormFuncs::unmarshal(cv::KeyPoint &cv_kp,
        const storm::KeyPoint &kobj)
{
    cv_kp.pt.x = kobj.x();
    cv_kp.pt.y = kobj.y();
    cv_kp.octave = kobj.octave();
    cv_kp.class_id = kobj.class_id();
    cv_kp.size = kobj.size();
    cv_kp.angle = kobj.angle();
    cv_kp.response = kobj.resp();
}

inline void StormFuncs::marshal(cv::KeyPoint &cv_kp,
        storm::KeyPoint *kobj)
{
    kobj->Clear();
    kobj->set_x(cv_kp.pt.x);
    kobj->set_y(cv_kp.pt.y);
    kobj->set_octave(cv_kp.octave);
    kobj->set_class_id(cv_kp.class_id);
    kobj->set_size(cv_kp.size);
    kobj->set_angle(cv_kp.angle);
    kobj->set_resp(cv_kp.response);
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
        const void *val, size_t len)
{
    memcached_return_t mret;
    if (!memc || !val) return -1;
    mret = memcached_set(memc, key.c_str(), key.length(),
            (char*)val, len, 0, 0);
    return !(mret == MEMCACHED_SUCCESS);
}

int memc_set(memcached_st *memc, const std::string &key,
        const google::protobuf::MessageLite &msg)
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

