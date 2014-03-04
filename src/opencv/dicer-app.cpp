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

#if 0
static int
get_features(cv::Mat &mat, cv::detail::ImageFeatures &features)
{
    std::unique_ptr< cv::detail::FeaturesFinder >
        finder(new cv::detail::SurfFeaturesFinder(4000., 4, 6));
        //finder(new cv::detail::OrbFeaturesFinder());
    if (!finder)
        return -ENOMEM;
    finder->operator()(mat, features);
    return 0;
}
#endif

static int dice_one(image_t &image, images_t &subimages)
{
    cv::Mat mat = __img(image);
    int img_width  = mat.size().width;
    int img_height = mat.size().height;

    if (img_width == 0 || img_height == 0)
        return -1;

#if 0
    cv::detail::ImageFeatures features;
    std::cout << ">> loading features";
    std::cout.flush();
    if (get_features(mat, features))
        return -1;
    std::cout << " " << features.keypoints.size() << " found" << std::endl;

    if (write_features("/tmp/surf.jpg", mat, features))
        return -1;

    if (write_image("/tmp/rect.jpg", sub))
        return -1;
#endif

    subimages.clear();

    std::cout << ">> describing sub-images ";
    std::cout.flush();

    /* control over number of subimages to generate */
    int num_vert = 1, num_horiz = 5;
    (void)num_vert;

    /* subimage bounding box */
    int bbx, bby, bbw, bbh;
    bbw = img_width / num_horiz;
    bbh = img_height;
    bbx = bby = 0;
    float bboverlap = 0.5; /* used when shifting bb */

    /* subimage */
    int x, y, w, h;
    cv::Rect roi;

    while ((bbx + bbw) < img_width) {
        /* subimage is bb */
        x = bbx; y = bby;
        w = bbw; h = bbh;
        roi = cv::Rect(x, y, w, h);
        subimages.push_back(make_tuple(mat(roi), __pth(image)));
        bbx += ((1. - bboverlap) * bbw);
    }

    std::cout << subimages.size() << std::endl;

    std::cout << ">> writing sub-images ";
    std::cout.flush();

    static int subset_num = 0;
    std::stringstream ss;
    ss << "sub-" << subset_num;
    if (write_images(config.out_dir, subimages, ss.str()))
        return -1;

    std::cout << std::endl;

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

