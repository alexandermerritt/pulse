/* file: dicer-app.cpp
 * author: Alexander Merritt, merritt.alex@gatech.edu
 * desc: Creates overlapping subimages from an input image upon which it
 * then performs random transformations. Artificially creating images one would
 * input to an image stitching program, but in reverse.
 *
 * crop, rotate (in and out of the plane), resize
 */

/* C++ system includes */
#include <iostream>
#include <memory>
#include <algorithm>

/* C system includes */
#include <stdio.h>
#include <getopt.h>

/* OpenCV includes */
#include <opencv2/highgui/highgui.hpp>
#include <opencv2/imgproc/imgproc.hpp>

/* Local includes */
#include "types.hpp"
#include "io.hpp"

//===----------------------------------------------------------------------===//
// Definitions
//===----------------------------------------------------------------------===//

struct config
{
    bool help;
    char *out_dir;
};

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

static struct config config;

static const struct option options[] = {
    {"help", no_argument, (int*)&config.help, true},
    {"dir", required_argument, NULL, 'd'},
    {NULL, no_argument, NULL, 0} // terminator
};

//===----------------------------------------------------------------------===//
// Entry
//===----------------------------------------------------------------------===//

static int parse_args(int argc, char *argv[])
{
    int opt, idx;
    const int done = -1; // man getopt_long

    while (done != (opt = getopt_long(argc, argv, "", options, &idx))) {
        if (opt == '?')
            return -EINVAL;
        if (opt == 'd')
            config.out_dir = optarg;
    }

    if (config.help)
        return -EINVAL;

    if (!config.out_dir)
        return -EINVAL;

    return 0;
}

static void usage(const char *name)
{
    fprintf(stderr, "Usage: %s --dir=/output/path/ < image_list\n", name);
}

/* Don't use this to assign to a reference:
 *      cv::Mat &mat = __img(..)
 * as seems to screw with the underlying type. Use it to assign to a
 * non-reference type instead:
 *      cv::Mat mat = __img(..)
 */
static inline cv::Mat &
__img(image_t &img)
{
    return std::get<0>(img);
}
static inline std::string &
__pth(image_t &img)
{
    return std::get<1>(img);
}

static int
get_features(cv::Mat &mat, cv::detail::ImageFeatures &features)
{
#define SURF_PARAMS 4000., 1, 6
    std::unique_ptr< cv::detail::FeaturesFinder >
        finder(new cv::detail::SurfFeaturesFinder(SURF_PARAMS));
        //finder(new cv::detail::OrbFeaturesFinder());
#undef SURF_PARAMS
    if (!finder)
        return -ENOMEM;
    finder->operator()(mat, features);
    return 0;
}

static inline void dump_keypoint(const cv::KeyPoint &kp)
{
    std::cout  << kp.pt.x
        << " " << kp.pt.y
        << " " << kp.size
        << " " << kp.angle
        << " " << kp.response
        << " " << kp.octave
        << " " << kp.class_id
        << std::endl;
}

typedef std::unique_ptr< cv::detail::ImageFeatures > featptr_t;
typedef std::unique_ptr< cv::Mat > matptr_t;

static int do_kmeans(featptr_t &feat, matptr_t &centers, int min_resp = 25000)
{
    matptr_t samples, labels;
    cv::TermCriteria criteria( CV_TERMCRIT_EPS + CV_TERMCRIT_ITER, 10, 1.0 );
    int clusters = 4;
    int attempts = 3, flags = cv::KMEANS_PP_CENTERS;

    samples.reset( new cv::Mat(feat->keypoints.size(), 2, CV_32F) );
    centers.reset( new cv::Mat(clusters, 1, CV_32F) );
    labels.reset(  new cv::Mat() );

    int x = 0;
    for (auto &kp : feat->keypoints) {
        samples->at<float>(x, 0) = kp.pt.x;
        samples->at<float>(x, 1) = kp.pt.y;
    }

    cv::kmeans(*samples, clusters, *labels, criteria,
            attempts, flags, *centers);

    return 0;
}

static int dice_one(image_t &image, images_t &subimages)
{
    cv::Mat mat = __img(image);
    int img_width  = mat.size().width;
    int img_height = mat.size().height;
    std::stringstream ss;

    if (img_width == 0 || img_height == 0)
        return -1;

    featptr_t feat;
    if (getenv("USE_FEATURES")) {
        std::cout << ">> loading features ";
        std::cout.flush();

        feat.reset( new cv::detail::ImageFeatures );
        if (!feat) return -1;

        if (get_features(mat, *feat))
            return -1;
        std::cout << " " << feat->keypoints.size() << " found" << std::endl;

        //if (write_features("/tmp/surf.jpg", mat, *feat))
            //return -1;

        //std::cout << "x y size angle resp oct id" << std::endl;
        //for (auto &kp : feat->keypoints)
            //if (kp.response > 25000)
                //dump_keypoint(kp);

        matptr_t centers;
        if (do_kmeans(feat, centers))
            return -1;

        std::cout << ">> centers: ";
        for (size_t i = 0; i < centers->total(); i++) {
            cv::Point p = centers->at<cv::Point2f>(i);
            std::cout << p << " ";
        }
        std::cout << std::endl;
    }

    subimages.clear();

    std::cout << ">> describing sub-images ";
    std::cout.flush();

    /* control over number of subimages to generate */
    int num_vert = 4, num_horiz = 5;

    /* subimage bounding box */
    float bboverlap = 0.5;
    int bbx, bby, bbw, bbh;
    bbw = std::min( (img_width / num_horiz) * (1 + bboverlap), (float)img_width);
    bbh = std::min( (img_height / num_vert) * (1 + bboverlap), (float)img_height);

    /* subimage */
    int x, y, w, h;
    cv::Rect roi;

    matptr_t boxed( new cv::Mat( mat.clone() ) );
    bby = 0;
    while ((bby + bbh) <= img_height) {
        bbx = 0;
        while ((bbx + bbw) <= img_width) {
            /* subimage is bb */
            x = bbx;
            y = bby;
            w = bbw;
            h = bbh;
            roi = cv::Rect(x, y, w, h);
            subimages.push_back(make_tuple(mat(roi), __pth(image)));
            bbx += ((1. - bboverlap) * bbw);

            cv::rectangle(*boxed, cv::Point(x,y), cv::Point(x+w, y+h),
                    cv::Scalar(0,0,255), 10, 8, 0);
        }
        bby += ((1. - bboverlap) * bbh);
    }

    std::cout << subimages.size() << std::endl;

    std::cout << ">> writing image with regions overlay" << std::endl;
    ss.str( std::string() );
    ss << config.out_dir << "/diced.jpg";
    if (!imwrite(ss.str(), *boxed))
        return -1;

    std::cout << ">> writing sub-images" << std::endl;
    static int subset_num = 0;
    ss.str( std::string() );
    ss << "sub-" << subset_num;
    if (write_images(config.out_dir, subimages, ss.str()))
        return -1;

    return 0;
}

static int dice(paths_t &paths)
{
    image_t image;
    images_t subimages;

    /* load one at a time, else we run out of memory */
    for (path_t &path : paths) {
        if (load_image(image, path)) {
            std::cerr << "!! error loading image" << std::endl;
            return -1;
        }
        if (dice_one(image, subimages)) {
            std::cerr << "!! error dicing image" << std::endl;
            return -1;
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{
    paths_t paths;
    std::vector< std::string > types;

    if (parse_args(argc, argv)) {
        usage(*argv);
        return -1;
    }

    types.push_back(".jpg");
    types.push_back(".png");

    read_stdin(paths);
    prune_paths(paths, types);

    if (dice(paths))
        return -1;

    return 0;
}

