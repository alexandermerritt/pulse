#include <iostream>
#include <iomanip>

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

static void printMatches(const matches_t &matches)
{
    int i = 0;
    std::cout << "---- matches start ----" << std::endl;
    for (auto &m : matches) {
        std::cout << std::setw(2) << i++
            << " (" << m.src_img_idx << "," << m.dst_img_idx << ")"
            << ": " << std::setw(10) << m.confidence
            << std::endl;
    }
    std::cout << "---- matches end ----" << std::endl;
}

static void printFeatures(features_t &features)
{
    int i = 0;
    std::cout << "---- features start ----" << std::endl;
    for (auto &f : features) {
        std::cout << std::setw(2) << i++
            << " img " << f.img_idx
            << " pts " << f.keypoints.size()
            << std::endl;
    }
    std::cout << "---- matches end ----" << std::endl;
}

int main(void)
{
    PStitcher ps = PStitcher::createDefault(false);
    std::string dirlist("dirlist");

    images_t images;
    features_t features;

    features_t feats;
    matches_t  matches;
    cameras_t  cams;
    images_t   pano_images;
    indices_t  indices;

    cv::Mat pano;
    int pano_num = 0, offset;
    stringstream pano_name("");
    float confidence;

    if (load_images(images, dirlist))
        return -1;

    ps.findFeatures(images, features, false, 8);

    while (images.size() > 1) {
        std::cout << ">> PANORAMA " << pano_num << std::endl;

        ps.matchFeatures(features, matches, false, 8);

        // find enough related images
        confidence = 2.0f;
        feats = features;
        ps.findRelated(feats, matches, indices, confidence);
        if (feats.size() < 2) {
            std::cerr << "!! 1 image in panorama,"
                << " try decreasing confidence" << std::endl;
            return -1;
        }

        std::cout << ">> " << feats.size() << " images in panorama" << std::endl;
        ps.estimateCameraParams(feats, matches, cams, confidence);

        // reduce work based on images found in current panorama
        pano_images.clear();
        std::sort(indices.begin(), indices.end()); // XXX okay to do?
        offset = 0;
        for (auto _idx : indices) {
            int idx = _idx - offset++;
            pano_images.push_back(images[idx]);
            images[idx].release();
            images.erase(images.begin() + idx);
            features.erase(features.begin() + idx);
        }

        ps.composePanorama(pano_images, cams, pano);
        pano_name.str(std::string());
        pano_name << "pano-" << pano_num++ << ".jpg";
        std::cout << ">> writing " << pano_name.str() << std::endl;
        cv::imwrite(pano_name.str(), pano);
    }

    return 0;
}
