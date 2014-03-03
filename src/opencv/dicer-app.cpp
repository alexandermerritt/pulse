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
__img(image_t img)
{
    return std::get<0>(img);
}

static int dice_one(image_t &image, images_t &subimages)
{
    cv::Mat mat = __img(image);
    cv::Mat sub;
    size_t width  = mat.size().width;
    size_t height = mat.size().height;

    if (width == 0 || height == 0)
        return -1;

    subimages.clear();
    sub = mat(cv::Rect(0, 0, width / 2, height / 2));

    if (write_image("/tmp/rect.jpg", sub))
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

