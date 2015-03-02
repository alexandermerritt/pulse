/**
 * StormFuncs.cpp
 *
 * Refer to load_graph.cpp comment at top of file for JSON formats and structure
 * of data in memcached.
 *
 * Each of these functions may access a key-value store. STL
 * exceptions are used to communicate errors. Keys not found have a
 * special error: memc_notfound and must be caught.
 */

// C headers
#include <string.h>
#include <unistd.h>
#include <libmemcached/memcached.h>

#include <sys/types.h>
#include <sys/socket.h>

// C++ headers
#include <iostream>
#include <opencv2/opencv.hpp>
#include <sstream>
#include <memory>

// Local headers
#include "StormFuncs.h"
#include "stitcher.hpp"
#include "Config.hpp"
#include "Objects.pb.h" // generated
#include "cv/decoders.h"
#include "matchers.hpp"

// FIXME make memc a per-thread variable...

//==--------------------------------------------------------------==//
// Public functions
//==--------------------------------------------------------------==//

StormFuncs::StormFuncs(void)
: memc(nullptr), rd(), gen(rd()), dis(0,1UL<<20)
{

}

int StormFuncs::connect(std::string &servers)
{
    if (memc)
        return 0;
    memc = memcached(servers.c_str(), servers.length());
    return !memc;
}

int StormFuncs::neighbors(std::string &vertex,
        std::deque<std::string> &others)
{
    if (vertex.length() == 0)
        throw runtime_error("vertex zero length");
    storm::Vertex vobj;
    memc_get(memc, vertex, vobj);

    // adjust how many we emit.. otherwise growth is too great
    const size_t ower = vobj.followers_size();
    const size_t ing  = vobj.following_size();
    const size_t total = (ower + ing);
    // XXX dangling vertex? use self
    if (total == 0) {
        std::cout << "zero links" << std::endl;
        others.push_back(vertex);
        return 0;
    }
    const float base = 1.5f;
    size_t num;
    if (ower > ing) {
        num = ower;
        if (ower > 20)
            num = std::log2(ower) / std::log2(base);
        others.resize(num);
        for (size_t i = 0; i < others.size(); i++)
            others[i] = vobj.followers(i);
            //others[i] = vobj.followers(dis(gen) % ower);
    } else {
        num = ing;
        if (ing > 20)
            num = std::log2(ing) / std::log2(base);
        others.resize(num);
        for (size_t i = 0; i < others.size(); i++)
            others[i] = vobj.following(i);
            //others[i] = vobj.following(dis(gen) % ing);
    }

    return 0;
}

int StormFuncs::imagesOf(std::string &vertex,
        std::deque<std::string> &keys)
{
    if (vertex.length() == 0)
        return 0;
    storm::Vertex vobj;
    try {
        memc_get(memc, vertex, vobj);
    } catch (memc_notfound &e) {
        // XXX hard-code some image
        keys.push_back(std::string("15800153247.jpg"));
        return 0;
    }
    size_t num = vobj.images_size();
    if (num == 0) {
        // XXX hard-code some image
        keys.push_back(std::string("15800153247.jpg"));
        return 0;
    }
    keys.resize(vobj.images_size());
    for (size_t i = 0; i < keys.size(); i++)
        keys[i] = vobj.images(i);
    return 0;
}

int StormFuncs::feature(std::string &image_key, int &found)
{
    storm::Image iobj;
    memc_get(memc, image_key, iobj);

    // get image
    void *data; size_t len;
    memc_get(memc, iobj.key_data(), &data, len);

    // decode
    cv::Mat img;
    img = jpeg::JPEGasMat(data, len);
    if (!img.data || img.cols < 1 || img.rows < 1) {
        free(data);
        throw ocv_vomit(std::string(__func__) + ": "
                + "JPEGasMat failed on " + image_key);
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
    catch (Exception &e) {
        throw ocv_vomit(std::string(__func__) + ": "
                + "opencv shat itself on " + image_key
                + ": " + e.what());
    }
    finder->collectGarbage();
    found = features.keypoints.size();

    // update image with features key
    std::string key(iobj.key_id() + "::features");
    iobj.set_key_features(key);
    memc_set(memc, iobj.key_id(), iobj);

    // serialize and store features
    storm::ImageFeatures fobj;
    marshal(features, fobj, key);
    memc_set(memc, fobj.key_id(), fobj);
    const cv::Mat &cvmat = features.descriptors;
    if (cvmat.data) {
        len = cvmat.elemSize() * cvmat.total();
        memc_set(memc, fobj.mat().key_data(), cvmat.data, len);
    }

    return 0;
}

int StormFuncs::match(std::deque<std::string> &imgkeys,
        std::deque<cv::detail::MatchesInfo> &matches)
{
    //std::deque<cv::detail::ImageFeatures> features;

    // get all the image features
    size_t i = 0;
    for (std::string &key : imgkeys) {
        storm::Image iobj;
        try { memc_get(memc, key, iobj); }
        catch (memc_notfound &e)  { continue; }
        if (iobj.has_key_features()) {
            storm::ImageFeatures fobj;
            try { memc_get(memc, iobj.key_features(), fobj); }
            catch (memc_notfound &e)  { continue; }
            cv::detail::ImageFeatures cvfeat;
            if (unmarshal(cvfeat, fobj))
                continue; // ignore..
            cvfeat.img_idx = i++;
            //features.push_back(cvfeat);
        }
    }

    return 0;
    //return do_match(features, matches); // XXX broken
}

int StormFuncs::montage(std::deque<std::string> &image_keys,
        std::string &montage_key)
{
    std::deque<cv::detail::MatchesInfo> matches; // not used

    if (image_keys.size() < 4)
        throw ocv_vomit("image set too small");

    // touch the features
    try { match(image_keys, matches); }
    catch (memc_notfound &e) { ; }

    // get all images
    std::deque<cv::Mat> images;
    images.resize(image_keys.size());
    for (size_t i = 0; i < images.size(); i++) {
        storm::Image iobj;
        memc_get(memc, image_keys[i], iobj);
        void *data; size_t len;
        memc_get(memc, iobj.key_data(), &data, len);
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
        try {
            cv::resize(img, scaled, cv::Size(), _scale, _scale);
            auto loc = dis(gen_rand);
            int x = (loc % rect.width) + rect.x;
            int y = (loc / rect.width) + rect.y;
            scaled.copyTo(canvas(cv::Rect(x, y,
                            scaled.cols, scaled.rows)));
        } catch (cv::Exception e) {
            std::cerr << "Error: caught exception"
                << std::endl;
            return -1;
        }
    }

    cv::Mat montage;
    try {
        montage = canvas(rect);
    } catch (cv::Exception e) {
        throw ocv_vomit("montage: creating canvas: "
                + std::string(e.what()));
    }
    void *buf;
    size_t len;
    if (jpeg::MatToJPEG(montage, &buf, len))
        return -1;
    std::stringstream ss;
    for (int i = 0; i < 4; i++)
        ss << dis(gen_rand);
    ss << ".jpg";
    memc_set(memc, ss.str(), buf, len);
    free(buf);
    buf = nullptr;
    montage_key = ss.str();

#if 0
    // test
    memc_get(memc, ss.str(), &buf, len);
    cv::Mat image = jpeg::JPEGasMat(buf, len);
    free(buf);
    imwrite("/tmp/montage.jpg", image);
#endif

    return 0;
}

void StormFuncs::writeImage(std::string &key, std::string &path)
{
    void *buf;
    size_t len;
    memc_get(memc, key, &buf, len);
    cv::Mat image = jpeg::JPEGasMat(buf, len);
    if (!image.data)
        throw std::runtime_error("JPEGasMat failed");
    try {
        cv::imwrite(path, image);
    } catch (cv::Exception &e) {
        throw std::runtime_error("cv::imwrite failed: "
                + std::string(e.what()));
    }
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
        memc_get(memc, mobj.key_data(), &data, len);
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

// low-level call
int memc_get(memcached_st *memc, const std::string &key,
        void **val, size_t &len)
{
    memcached_return_t mret;
    uint32_t flags;
    if (!memc || !val)
        throw std::runtime_error(std::string(__func__) + ": "
                + "invalid arguments");
    *val = memcached_get(memc, key.c_str(), key.length(),
            &len, &flags, &mret);
    if (!*val || (mret != MEMCACHED_SUCCESS)) {
        std::stringstream ss;
        ss << std::string(__func__) + ": "
                + "failed to fetch " + key;
        ss << ": ";
        ss << memcached_strerror(memc, mret);
        if (MEMCACHED_NOTFOUND == mret)
            throw memc_notfound(std::string(ss.str()));
        else
            throw std::runtime_error(ss.str());
    }
    return 0;
}

int memc_get(memcached_st *memc, const std::string &key,
        google::protobuf::MessageLite &msg)
{
    size_t len(0);
    void *val(nullptr);

    if (!memc)
        throw std::runtime_error(std::string(__func__) + ": "
                + "memc arg is null");
    if (key.length() == 0)
        throw std::runtime_error(std::string(__func__) + ": "
                + "key is empty");

    memc_get(memc, key, &val, len);

    // try to parse buffer.
    // XXX hack. if the buffer is corrupt in some way where just the
    // length is invalid, try to decrease it to a point where a valid
    // protobuf can be extracted. Don't decrease more than a quarter
    // of the original size.
    size_t real = len;
    while (!msg.ParseFromArray(val, real))
        if (real-- <= (len >> 2))
            break;
    if (real <= (len >> 2)) {
        free(val);
        std::stringstream ss;
        ss << std::string(__func__) + ": ";
        ss << "failed to parse object '" + key;
        ss << "' within length ";
        ss << len; ss << "-"; ss << real;
        throw protobuf_parsefail(ss.str());
    }
    if (real != len)
        std::cerr << "Key was reduced in size during parsing: "
            + key << std::endl;

    free(val);
    return 0;
}

int memc_set(memcached_st *memc, const std::string &key,
        const void *val, size_t len)
{
    memcached_return_t mret;
    if (!memc || !val)
        throw std::runtime_error(std::string(__func__) + ": "
                + "invalid args");
    mret = memcached_set(memc, key.c_str(), key.length(),
            (char*)val, len, 0, 0);
    if (!(mret == MEMCACHED_SUCCESS))
        throw std::runtime_error(std::string(__func__) + ": "
                + "failed to fetch " + key);
    return 0;
}

int memc_set(memcached_st *memc, const std::string &key,
        const google::protobuf::MessageLite &msg)
{
    size_t len(0);
    void *val(nullptr);

    if (!memc || key.length() == 0)
        throw std::runtime_error(std::string(__func__) + ": "
                + "invalid args");

    len = msg.ByteSize();
    if (!(val = malloc(len)))
        throw std::runtime_error(std::string(__func__) + ": "
                + "out of memory");

    if (!msg.SerializeToArray(val, len)) {
        throw protobuf_parsefail(std::string(__func__) + ": "
                + "failed to serialize object " + key);
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

