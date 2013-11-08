#include <iostream>

/* OpenCV includes */
#include <opencv2/opencv.hpp>
#include <opencv2/gpu/gpu.hpp>
#include <opencv2/core/gpumat.hpp>
#include <opencv2/stitching/stitcher.hpp>

#include <opencv2/core/core.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/stitching/warpers.hpp>
#include <opencv2/stitching/detail/matchers.hpp>
#include <opencv2/stitching/detail/motion_estimators.hpp>
#include <opencv2/stitching/detail/exposure_compensate.hpp>
#include <opencv2/stitching/detail/seam_finders.hpp>
#include <opencv2/stitching/detail/blenders.hpp>
#include <opencv2/stitching/detail/camera.hpp>

#include "stitcher.hpp"
#include "io.hpp"
#include "types.hpp"

int main(void)
{
    PStitcher ps = PStitcher::createDefault();
    std::string dirlist("dirlist");
    images_t images;
    cv::Mat pano;

    if (load_images(images, dirlist))
        return -1;

    ps.estimateTransform(images);
    ps.composePanorama(images, pano);
    cv::imwrite("pano.jpg", pano);

    return 0;
}
