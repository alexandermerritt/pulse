/*M///////////////////////////////////////////////////////////////////////////////////////
//
//  IMPORTANT: READ BEFORE DOWNLOADING, COPYING, INSTALLING OR USING.
//
//  By downloading, copying, installing or using the software you agree to this license.
//  If you do not agree to this license, do not download, install,
//  copy or use the software.
//
//
//                          License Agreement
//                For Open Source Computer Vision Library
//
// Copyright (C) 2000-2008, Intel Corporation, all rights reserved.
// Copyright (C) 2009, Willow Garage Inc., all rights reserved.
// Third party copyrights are property of their respective owners.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//   * Redistribution's of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//
//   * Redistribution's in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//   * The name of the copyright holders may not be used to endorse or promote products
//     derived from this software without specific prior written permission.
//
// This software is provided by the copyright holders and contributors "as is" and
// any express or implied warranties, including, but not limited to, the implied
// warranties of merchantability and fitness for a particular purpose are disclaimed.
// In no event shall the Intel Corporation or contributors be liable for any direct,
// indirect, incidental, special, exemplary, or consequential damages
// (including, but not limited to, procurement of substitute goods or services;
// loss of use, data, or profits; or business interruption) however caused
// and on any theory of liability, whether in contract, strict liability,
// or tort (including negligence or otherwise) arising in any way out of
// the use of this software, even if advised of the possibility of such damage.
//
//M*/

//#include "precomp.hpp"

#include <opencv2/core/core.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/stitching/warpers.hpp>
#include <opencv2/stitching/detail/matchers.hpp>
#include <opencv2/stitching/detail/motion_estimators.hpp>
#include <opencv2/stitching/detail/exposure_compensate.hpp>
#include <opencv2/stitching/detail/seam_finders.hpp>
#include <opencv2/stitching/detail/blenders.hpp>
#include <opencv2/stitching/detail/camera.hpp>

#include <iostream>
#include "stitcher.hpp"

using namespace std;
using namespace cv;

PStitcher PStitcher::createDefault(bool try_use_gpu)
{
    PStitcher stitcher;
    stitcher.setRegistrationResol(0.6);
    stitcher.setSeamEstimationResol(0.1);
    stitcher.setCompositingResol(ORIG_RESOL);
    stitcher.setPanoConfidenceThresh(1);
    stitcher.setWaveCorrection(true);
    stitcher.setWaveCorrectKind(detail::WAVE_CORRECT_HORIZ);
    stitcher.setFeaturesMatcher(new detail::BestOf2NearestMatcher(try_use_gpu));
    stitcher.setBundleAdjuster(new detail::BundleAdjusterRay());

#ifdef HAVE_OPENCV_GPU
    if (try_use_gpu && gpu::getCudaEnabledDeviceCount() > 0)
    {
#if defined(HAVE_OPENCV_NONFREE)
        stitcher.setFeaturesFinder(new detail::SurfFeaturesFinderGpu());
#else
        stitcher.setFeaturesFinder(new detail::OrbFeaturesFinder());
#endif
        stitcher.setWarper(new SphericalWarperGpu());
        stitcher.setSeamFinder(new detail::GraphCutSeamFinderGpu());
    }
    else
#endif
    {
#ifdef HAVE_OPENCV_NONFREE
        stitcher.setFeaturesFinder(new detail::SurfFeaturesFinder());
#else
        stitcher.setFeaturesFinder(new detail::OrbFeaturesFinder());
#endif
        stitcher.setWarper(new SphericalWarper());
        stitcher.setSeamFinder(new detail::GraphCutSeamFinder(detail::GraphCutSeamFinderBase::COST_COLOR));
    }

    stitcher.setExposureCompensator(new detail::BlocksGainCompensator());
    stitcher.setBlender(new detail::MultiBandBlender(try_use_gpu));

    return stitcher;
}



PStitcher::Status PStitcher::composePanorama(images_t &images, cv::Mat & pano)
{
    LOGLN("Warping images (auxiliary)... ");
    cv::Mat img, full_img;
    images_t seam_est_images;

    // compute seam scales (and recompute work scale)
    double work_scale = 1;
    if (registr_resol_ >= 0)
        work_scale = min(1.0, sqrt(registr_resol_ * 1e6 / images[0].size().area()));
    std::cout << ">>    work_scale " << work_scale << std::endl;
    double seam_scale =
        std::min(1.0, sqrt(seam_est_resol_ * 1e6 / images[0].size().area()));
    double seam_work_aspect = seam_scale / work_scale;
    cout << ">>    seam_scale " << seam_scale << endl;
    cout << ">>    seam_work_aspect " << seam_work_aspect << endl;

    for (auto &image : images) {
        cv::resize(image, img, cv::Size(), seam_scale, seam_scale);
        seam_est_images.push_back(img.clone());
    }
    img.release();

    estimateCameraParams();

#if ENABLE_LOG
    int64 t = getTickCount();
#endif

    vector<Point> corners(images.size());
    vector<Mat> masks_warped(images.size());
    vector<Mat> images_warped(images.size());
    vector<Size> sizes(images.size());
    vector<Mat> masks(images.size());

    // Prepare image masks
    for (size_t i = 0; i < images.size(); ++i)
    {
        masks[i].create(seam_est_images[i].size(), CV_8U);
        masks[i].setTo(Scalar::all(255));
    }

    // Find median focal length and use it as final image scale
    vector<double> focals;
    for (auto &cam : cameras_)
        focals.push_back(cam.focal);

    std::sort(focals.begin(), focals.end());
    size_t fsz = focals.size();
    double warped_image_scale;
    if (fsz % 2 == 1)
        warped_image_scale = static_cast<float>(focals[fsz / 2]);
    else
        warped_image_scale = static_cast<float>(focals[fsz / 2 - 1] + focals[fsz / 2]) * 0.5f;

    // Warp images and their masks
    Ptr<detail::RotationWarper> w = warper_->create(float(warped_image_scale * seam_work_aspect));
    for (size_t i = 0; i < images.size(); ++i)
    {
        Mat_<float> K;
        cameras_[i].K().convertTo(K, CV_32F);
        K(0,0) *= (float)seam_work_aspect;
        K(0,2) *= (float)seam_work_aspect;
        K(1,1) *= (float)seam_work_aspect;
        K(1,2) *= (float)seam_work_aspect;

        corners[i] = w->warp(seam_est_images[i], K, cameras_[i].R, INTER_LINEAR, BORDER_REFLECT, images_warped[i]);
        sizes[i] = images_warped[i].size();

        w->warp(masks[i], K, cameras_[i].R, INTER_NEAREST, BORDER_CONSTANT, masks_warped[i]);
    }

    vector<Mat> images_warped_f(images.size());
    for (size_t i = 0; i < images.size(); ++i)
        images_warped[i].convertTo(images_warped_f[i], CV_32F);

    LOGLN("Warping images, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    // Find seams
    exposure_comp_->feed(corners, images_warped, masks_warped);
    seam_finder_->find(images_warped_f, corners, masks_warped);

    // Release unused memory
    seam_est_images.clear();
    images_warped.clear();
    images_warped_f.clear();
    masks.clear();

    LOGLN("Compositing...");
#if ENABLE_LOG
    t = getTickCount();
#endif

    Mat img_warped, img_warped_s;
    Mat dilated_mask, seam_mask, mask, mask_warped;

    //double compose_seam_aspect = 1;
    double compose_work_aspect = 1;
    bool is_blender_prepared = false;

    double compose_scale = 1;
    bool is_compose_scale_set = false;

    for (size_t img_idx = 0; img_idx < images.size(); ++img_idx)
    {
        LOGLN("Compositing image #" << indices_[img_idx] + 1);

        // Read image and resize it if necessary
        full_img = images[img_idx];
        if (!is_compose_scale_set)
        {
            if (compose_resol_ > 0)
                compose_scale = min(1.0, sqrt(compose_resol_ * 1e6 / full_img.size().area()));
            is_compose_scale_set = true;

            // Compute relative scales
            //compose_seam_aspect = compose_scale / seam_scale;
            compose_work_aspect = compose_scale / work_scale;

            // Update warped image scale
            warped_image_scale *= static_cast<float>(compose_work_aspect);
            w = warper_->create((float)warped_image_scale);

            // Update corners and sizes
            for (size_t i = 0; i < images.size(); ++i)
            {
                // Update intrinsics
                cameras_[i].focal *= compose_work_aspect;
                cameras_[i].ppx *= compose_work_aspect;
                cameras_[i].ppy *= compose_work_aspect;

                // Update corner and size
                Size sz = images[i].size();
                if (std::abs(compose_scale - 1) > 1e-1)
                {
                    sz.width = cvRound(images[i].size().width * compose_scale);
                    sz.height = cvRound(images[i].size().height * compose_scale);
                }

                Mat K;
                cameras_[i].K().convertTo(K, CV_32F);
                Rect roi = w->warpRoi(sz, K, cameras_[i].R);
                corners[i] = roi.tl();
                sizes[i] = roi.size();
            }
        }
        if (std::abs(compose_scale - 1) > 1e-1)
            resize(full_img, img, Size(), compose_scale, compose_scale);
        else
            img = full_img;
        full_img.release();
        Size img_size = img.size();

        Mat K;
        cameras_[img_idx].K().convertTo(K, CV_32F);

        // Warp the current image
        w->warp(img, K, cameras_[img_idx].R, INTER_LINEAR, BORDER_REFLECT, img_warped);

        // Warp the current image mask
        mask.create(img_size, CV_8U);
        mask.setTo(Scalar::all(255));
        w->warp(mask, K, cameras_[img_idx].R, INTER_NEAREST, BORDER_CONSTANT, mask_warped);

        // Compensate exposure
        exposure_comp_->apply((int)img_idx, corners[img_idx], img_warped, mask_warped);

        img_warped.convertTo(img_warped_s, CV_16S);
        img_warped.release();
        img.release();
        mask.release();

        // Make sure seam mask has proper size
        dilate(masks_warped[img_idx], dilated_mask, Mat());
        resize(dilated_mask, seam_mask, mask_warped.size());

        mask_warped = seam_mask & mask_warped;

        if (!is_blender_prepared)
        {
            blender_->prepare(corners, sizes);
            is_blender_prepared = true;
        }

        // Blend the current image
        blender_->feed(img_warped_s, mask_warped, corners[img_idx]);
    }

    Mat result, result_mask;
    blender_->blend(result, result_mask);

    LOGLN("Compositing, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    // Preliminary result is in CV_16SC3 format, but all values are in [0,255] range,
    // so convert it to avoid user confusing
    result.convertTo(pano, CV_8U);

    return OK;
}


// 1. find features for each image
// 2. pair-wise match features for all images
// 2b. determine which belong to same panorama
// OUTPUT feature set, list of images part of panorama
PStitcher::Status PStitcher::matchImages(images_t &images)
{
    if ((int)images.size() < 2)
    {
        LOGLN("Need more images");
        return ERR_NEED_MORE_IMGS;
    }

    Mat full_img, img;
    features_.resize(images.size());

    LOGLN("Finding features...");
#if ENABLE_LOG
    int64 t = getTickCount();
#endif

    double work_scale = 1;
    if (registr_resol_ >= 0)
        work_scale = min(1.0, sqrt(registr_resol_ * 1e6 / images[0].size().area()));
    std::cout << ">>    work_scale " << work_scale << std::endl;

    for (size_t i = 0; i < images.size(); ++i)
    {
        full_img = images[i];

        resize(full_img, img, Size(), work_scale, work_scale);
        (*features_finder_)(img, features_[i]);
        features_[i].img_idx = (int)i;

        LOGLN("Features in image #" << i+1 << ": " << features_[i].keypoints.size());
    }

    // Do it to save memory
    features_finder_->collectGarbage();
    full_img.release();
    img.release();

    LOGLN("Finding features, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    LOG("Pairwise matching");
#if ENABLE_LOG
    t = getTickCount();
#endif
    (*features_matcher_)(features_, pairwise_matches_, matching_mask_);
    features_matcher_->collectGarbage();
    LOGLN("Pairwise matching, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    // Leave only images we are sure are from the same panorama
    indices_ = detail::leaveBiggestComponent(features_, pairwise_matches_, (float)conf_thresh_);

    return OK;
}

// 3. look at camera data for each image
void PStitcher::estimateCameraParams()
{
    detail::HomographyBasedEstimator estimator;
    estimator(features_, pairwise_matches_, cameras_);

    for (size_t i = 0; i < cameras_.size(); ++i)
    {
        Mat R;
        cameras_[i].R.convertTo(R, CV_32F);
        cameras_[i].R = R;
        LOGLN("Initial intrinsic parameters #" << indices_[i] + 1 << ":\n " << cameras_[i].K());
    }

    bundle_adjuster_->setConfThresh(conf_thresh_);
    (*bundle_adjuster_)(features_, pairwise_matches_, cameras_);

    if (do_wave_correct_)
    {
        vector<Mat> rmats;
        for (size_t i = 0; i < cameras_.size(); ++i)
            rmats.push_back(cameras_[i].R);
        detail::waveCorrect(rmats, wave_correct_kind_);
        for (size_t i = 0; i < cameras_.size(); ++i)
            cameras_[i].R = rmats[i];
    }
}

