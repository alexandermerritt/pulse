/* file: dicer-app.cpp
 * author: Alexander Merritt, merritt.alex@gatech.edu
 * desc: Creates overlapping subimages from an input image upon which it
 * then performs random transformations. Artificially creating images one would
 * input to an image stitching program, but in reverse.
 *
 * crop, rotate (in and out of the plane), resize
 */

#define _USE_MATH_DEFINES

/* C++ system includes */
#include <iostream>
#include <memory>
#include <algorithm>
#include <cstdlib>

/* C system includes */
#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>

/* OpenCV includes */
#include <opencv2/highgui/highgui.hpp>
#include <opencv2/imgproc/imgproc.hpp>

/* Local includes */
#include "types.hpp"
#include "io.hpp"

//===----------------------------------------------------------------------===//
// Definitions
//===----------------------------------------------------------------------===//

const int MIN_SUBIMG_DIM = 640;

struct config
{
    bool help;
    path_t out_dir;
    path_t filep;
};

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

static struct config config;

static const struct option options[] = {
    {"help", no_argument, (int*)&config.help, true},
    {"dir", required_argument, NULL, 'd'},
    {"file", required_argument, NULL, 'f'},
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
            config.out_dir = std::string(optarg);
        if (opt == 'f')
            config.filep = std::string(optarg);
    }

    if (config.help)
        return -EINVAL;

    if (config.out_dir.empty())
        return -EINVAL;

    return 0;
}

static void usage(const char *name)
{
    fprintf(stderr, "Usage: %s --file=/path/to/image --dir=/output/path/\n", name);
}

#define __img(image_t) std::get<0>(image_t)
#define __pth(image_t) std::get<1>(image_t)

typedef std::unique_ptr< cv::detail::ImageFeatures > featptr_t;
typedef std::unique_ptr< cv::Mat > matptr_t;

static int dice_one(image_t &image, rois_t &rois, bool show_boxed = false)
{
    cv::Mat mat = __img(image);
    int img_width  = mat.size().width;
    int img_height = mat.size().height;
    std::stringstream ss;
    matptr_t boxed;

    if (img_width == 0 || img_height == 0)
        return -1;

    rois.clear();

    std::cout << ">> describing sub-images " << std::endl;

    cv::Rect roi;
    cv::RNG rng(time(NULL));
    int num_vert  = rng.uniform(3, 6);
    int num_horiz = rng.uniform(3, 6);
    int width     = img_width / (num_vert + 1);
    int height    = img_height / (num_horiz + 1);

    /* ensure enough overlap */
    int xbounce   = width / 4;
    int ybounce   = height / 4;
    int whbounce  = std::max(width, height) / 4;

    if (show_boxed)
        boxed.reset( new cv::Mat( mat.clone() ) );

    for (int v = 0; v < num_vert; v++) {
        int y = img_height / (num_vert + 1) * v;

        for (int h = 0; h < num_vert; h++) {
            int x = img_width / (num_horiz + 1) * h;

            roi.x = x + rng.uniform( 0, xbounce );
            roi.y = y + rng.uniform( 0, ybounce );
            roi.x = std::max( roi.x, 0 );
            roi.y = std::max( roi.y, 0 );

            roi.width  = width + (whbounce + rng.uniform(0, whbounce));
            roi.height = height + (whbounce + rng.uniform(0, whbounce));
            if (roi.x + roi.width > img_width)
                roi.width = img_width - roi.x;
            if (roi.y + roi.height > img_height)
                roi.height = img_height - roi.y;

            if (roi.width < std::min(MIN_SUBIMG_DIM, (img_width / 10)) ||
                    roi.height < std::min(MIN_SUBIMG_DIM, (img_height / 10)))
                continue; /* image 'too small' */

            rois.push_back(roi);

            if (show_boxed) {
                cv::rectangle(*boxed, cv::Point(roi.x, roi.y),
                        cv::Point(roi.x + roi.width, roi.y + roi.height),
                        cv::Scalar(0,0,255), 10, 8, 0);
                std::cout << "box " << roi << std::endl;
            }
        }
    }

    if (show_boxed) {
        std::cout << ">> writing image with regions overlay" << std::endl;
        ss.str( std::string() );
        ss << config.out_dir << "/boxed.jpg";
        if (!imwrite(ss.str(), *boxed))
            return -1;
    }

    return 0;
}

// see also perspectiveTransform, warpPerspective
static int do_transform(cv::Mat &mat, float shrink_by)
{
    cv::Mat copy;
    cv::RNG rng(time(NULL));
    cv::Rect sub;

    try {
        copy = mat.clone();
    } catch (cv::Exception &e) {
        std::cerr << "!! error setting up image transform" << std::endl;
        return -1;
    }

    // original image dimensions (large letters)
    float W = mat.cols, H = mat.rows;
    cv::Point2f center(W / 2., H / 2.);

    // shrunken subimage dimensions (small letters)
    float w = W * (1 - shrink_by), h = H * (1 - shrink_by);

    // length of diagonal in subimage
    float d = std::sqrt( w * w + h * h );

    // angle underneath large diagonal
    float beta = std::atan(H / W);

    // rotate subimage diagonal, what is angle?
    float delta;
    if (d < W && d < H) {
        // can fully rotate subimage; restrict to 45-deg
        delta = M_PI / 4.;
    } else {
        // subimage diagonal will hit *something* when rotated
        if (W > H)  delta = std::asin(H / d);
        else        delta = std::acos(W / d);
    }

    // how much freedom for rotation we have without losing pixels
    float rad = std::abs(delta - beta);

    // rotate original image by some random adjustment
    float _rad = rng.uniform(-rad, rad);
    try {
        float scale = 1.;
        float deg = _rad * 180. / M_PI;
        cv::Mat rmat = cv::getRotationMatrix2D(center, deg, scale);
        cv::warpAffine(mat, copy, rmat, mat.size());
    } catch (cv::Exception &e) {
        std::cerr << "!! error rotating" << std::endl;
        return -1;
    }

    sub = cv::Rect(center.x - w / 2., center.y - h / 2., w, h);

#if 0
    // draw subimage into copy and write to disk
    try {
        rectangle(copy, sub, cv::Scalar(0,0,255), 2);
    } catch (cv::Exception &e) {
        std::cerr << "!! error drawing rectangle" << std::endl;
        return -1;
    }

    try {
        imwrite("/tmp/dicer/copy.jpg", copy);
    } catch (cv::Exception &e) {
        std::cerr << "!! error writing images" << std::endl;
        return -1;
    }
#endif

    mat = copy(sub);
    return 0;
}

static int dice(path_t &path)
{
    image_t image;
    rois_t rois;

    /* load and dice */
    if (load_image(image, path)) {
        std::cerr << "!! error loading image" << std::endl;
        return -1;
    }
    if (dice_one(image, rois, false)) {
        std::cerr << "!! error dicing image" << std::endl;
        return -1;
    }

    std::cout << ">> applying transformations"
        << " (" << rois.size() << " subimages)"
        << std::endl;

#pragma omp parallel for
    for (size_t subidx = 0; subidx < rois.size(); subidx++) {
        std::stringstream ss;
        cv::Mat mat, sub, clone;

        mat = __img(image);
        sub = mat(rois[subidx]);
        clone = sub.clone();
        do_transform(clone, 0.2);

        ss.str( std::string() );
        ss << config.out_dir
            << "/submg-" << subidx << ".png";
        if (!imwrite(ss.str(), clone)) {
            std::cerr << "!! error writing subimage '"
                << ss.str() << "'" << std::endl;
        }
        std::cout << ss.str() << std::endl;
        clone.release();
    } // for each subimage

    return 0;
}

int main(int argc, char *argv[])
{
    std::vector< std::string > types;

    if (parse_args(argc, argv)) {
        usage(*argv);
        return -1;
    }

    if (dice(config.filep))
        return -1;

    return 0;
}

