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

    if (try_use_gpu && gpu::getCudaEnabledDeviceCount() > 0)
    {
        stitcher.setFeaturesFinder(new detail::SurfFeaturesFinderGpu());
        stitcher.setWarper(new SphericalWarperGpu());
        stitcher.setSeamFinder(new detail::GraphCutSeamFinderGpu());
    }
    else
    {
        stitcher.setFeaturesFinder(new detail::SurfFeaturesFinder());
        stitcher.setWarper(new SphericalWarper());
        stitcher.setSeamFinder(new detail::GraphCutSeamFinder(detail::GraphCutSeamFinderBase::COST_COLOR));
    }

    stitcher.setExposureCompensator(new detail::BlocksGainCompensator());
    stitcher.setBlender(new detail::MultiBandBlender(try_use_gpu));

    return stitcher;
}

int PStitcher::findFeatures(const images_t &images, features_t &features)
{
    if ((int)images.size() < 2)
        return -1;

    Mat full_img, img;

    features.clear();
    features.resize(images.size());

    std::cout << ">> feature detection" << std::endl;
#if ENABLE_LOG
    int64 t = getTickCount();
#endif

    double work_scale = 1;
    if (registr_resol >= 0)
        work_scale = min(1.0, sqrt(registr_resol * 1e6 / images[0].size().area()));
    assert(work_scale != 1);
    //std::cout << ">>    work_scale " << work_scale << std::endl;

    for (size_t i = 0; i < images.size(); ++i)
    {
        full_img = images[i];

        resize(full_img, img, Size(), work_scale, work_scale);
        (*features_finder)(img, features[i]);
        //features[i].img_idx = (int)i; // XXX what is this for?

        std::cout << "    " << features[i].keypoints.size() << std::endl;
    }

    // Do it to save memory
    features_finder->collectGarbage();
    full_img.release();
    img.release();

    LOGLN("Finding features, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    return 0;
}

int PStitcher::matchFeatures(const features_t &features, matches_t &matches)
{
    std::cout << ">> pairwise matching" << std::endl;

#if ENABLE_LOG
    int64 t = getTickCount();
#endif

    matches.clear();
    (*features_matcher)(features, matches, matching_mask);
    (*features_matcher).collectGarbage();

    LOGLN("Pairwise matching, time: " << ((getTickCount() - t)
                / getTickFrequency()) << " sec");

    return 0;
}

// Reduce features,matches to those images which are related.
// Caller must then extract images from original vector specified by indices
// before giving to panorama composer.
void PStitcher::findRelated(features_t &features, matches_t &matches,
        indices_t &indices)
{
    indices.clear();
    // Leave only images we are sure are from the same panorama
    indices = detail::leaveBiggestComponent(features, matches, (float)conf_thresh);
}

// 3. look at camera data for each image
void PStitcher::estimateCameraParams(features_t &features,
        matches_t &matches, cameras_t &cameras)
{
    detail::HomographyBasedEstimator estimator;

    std::cout << ">> camera adjustment estimation" << std::endl;

    cameras.clear();

    std::cout << "    estimator" << std::endl;
    estimator(features, matches, cameras);

    std::cout << "    conversion ";
    for (size_t i = 0; i < cameras.size(); ++i)
    {
        Mat R;
        cameras[i].R.convertTo(R, CV_32F);
        cameras[i].R = R;
        std::cout << ".";
        std::cout.flush();
        LOGLN("Initial intrinsic parameters #" << indices[i] + 1 << ":\n " << cameras[i].K());
    }
    std::cout << std::endl;

    std::cout << "    bundle adjustment" << std::endl;
    bundle_adjuster->setConfThresh(conf_thresh);
    (*bundle_adjuster)(features, matches, cameras);

    if (do_wave_correct)
    {
        std::cout << "    wave correction" << std::endl;
        vector<Mat> rmats;
        for (size_t i = 0; i < cameras.size(); ++i)
            rmats.push_back(cameras[i].R);
        detail::waveCorrect(rmats, wave_correct_kind);
        for (size_t i = 0; i < cameras.size(); ++i)
            cameras[i].R = rmats[i];
    }
}

PStitcher::Status PStitcher::composePanorama(images_t &images, cameras_t &cameras, cv::Mat & pano)
{
    cv::Mat img, full_img;
    images_t seam_est_images;

    std::cout << ">> composing panorama" << std::endl;

    // compute seam scales (and recompute work scale)
    double work_scale = 1;
    if (registr_resol >= 0)
        work_scale = min(1.0, sqrt(registr_resol * 1e6 / images[0].size().area()));
    assert(work_scale != 1);
    //std::cout << ">>    work_scale " << work_scale << std::endl;

    double seam_scale =
        std::min(1.0, sqrt(seam_est_resol * 1e6 / images[0].size().area()));
    double seam_work_aspect = seam_scale / work_scale;
    assert(seam_scale != 1);
    assert(seam_work_aspect != 1);
    //cout << "      seam_scale " << seam_scale << endl;
    //cout << "      seam_work_aspect " << seam_work_aspect << endl;

    for (auto &image : images) {
        cv::resize(image, img, cv::Size(), seam_scale, seam_scale);
        seam_est_images.push_back(img.clone());
    }
    img.release();

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
    for (auto &cam : cameras)
        focals.push_back(cam.focal);

    std::sort(focals.begin(), focals.end());
    size_t fsz = focals.size();
    double warped_image_scale;
    if (fsz % 2 == 1)
        warped_image_scale = static_cast<float>(focals[fsz / 2]);
    else
        warped_image_scale = static_cast<float>(focals[fsz / 2 - 1] + focals[fsz / 2]) * 0.5f;

    // Warp images and their masks
    std::cout << "    warping images" << std::endl;
    Ptr<detail::RotationWarper> w = warper->create(float(warped_image_scale * seam_work_aspect));
    for (size_t i = 0; i < images.size(); ++i)
    {
        Mat_<float> K;
        cameras[i].K().convertTo(K, CV_32F);
        K(0,0) *= (float)seam_work_aspect;
        K(0,2) *= (float)seam_work_aspect;
        K(1,1) *= (float)seam_work_aspect;
        K(1,2) *= (float)seam_work_aspect;

        corners[i] = w->warp(seam_est_images[i], K, cameras[i].R, INTER_LINEAR, BORDER_REFLECT, images_warped[i]);
        sizes[i] = images_warped[i].size();

        w->warp(masks[i], K, cameras[i].R, INTER_NEAREST, BORDER_CONSTANT, masks_warped[i]);
    }

    vector<Mat> images_warped_f(images.size());
    for (size_t i = 0; i < images.size(); ++i)
        images_warped[i].convertTo(images_warped_f[i], CV_32F);

    LOGLN("Warping images, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    // Find seams
    exposure_comp->feed(corners, images_warped, masks_warped);
    seam_finder->find(images_warped_f, corners, masks_warped);

    // Release unused memory
    seam_est_images.clear();
    images_warped.clear();
    images_warped_f.clear();
    masks.clear();

    std::cout << "    compositing" << std::endl;
#if ENABLE_LOG
    t = getTickCount();
#endif

    Mat img_warped, img_warped_s;
    Mat dilated_mask, seam_mask, mask, mask_warped;

    //double compose_seam_aspect = 1;
    double compose_work_aspect = 1;
    bool is_blender_prepared = false;

    double compose_scale = 1;

    // prepare for blender loop --------------------------------------
    if (compose_resol > 0)
        compose_scale = min(1.0, sqrt(compose_resol * 1e6 / images[0].size().area()));

    compose_work_aspect = compose_scale / work_scale;
    warped_image_scale *= static_cast<float>(compose_work_aspect);

    w = warper->create((float)warped_image_scale);

    // Update corners and sizes
    for (size_t i = 0; i < images.size(); ++i)
    {
        // Update intrinsics
        cameras[i].focal *= compose_work_aspect;
        cameras[i].ppx *= compose_work_aspect;
        cameras[i].ppy *= compose_work_aspect;

        // Update corner and size
        cv::Size sz = images[i].size();
        if (std::abs(compose_scale - 1) > 1e-1)
        {
            sz.width = cvRound(images[i].size().width * compose_scale);
            sz.height = cvRound(images[i].size().height * compose_scale);
        }

        Mat K;
        cameras[i].K().convertTo(K, CV_32F);
        Rect roi = w->warpRoi(sz, K, cameras[i].R);
        corners[i] = roi.tl();
        sizes[i] = roi.size();
    }

    // blender loop --------------------------------------
    std::cout << "      ";
    for (size_t img_idx = 0; img_idx < images.size(); ++img_idx)
    {
        std::cout << " " << img_idx;
        std::cout.flush();

        // Read image and resize it if necessary
        img = images[img_idx]; // XXX is this dangerous if resize is used later?

        if (std::abs(compose_scale - 1) > 1e-1)
            cv::resize(images[img_idx], img, Size(), compose_scale, compose_scale);

        Size img_size = img.size();

        Mat K;
        cameras[img_idx].K().convertTo(K, CV_32F);

        // Warp the current image
        w->warp(img, K, cameras[img_idx].R, INTER_LINEAR, BORDER_REFLECT, img_warped);

        // Warp the current image mask
        mask.create(img_size, CV_8U);
        mask.setTo(Scalar::all(255));
        w->warp(mask, K, cameras[img_idx].R, INTER_NEAREST, BORDER_CONSTANT, mask_warped);

        // Compensate exposure
        exposure_comp->apply((int)img_idx, corners[img_idx], img_warped, mask_warped);

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
            blender->prepare(corners, sizes);
            is_blender_prepared = true;
        }

        // Blend the current image
        blender->feed(img_warped_s, mask_warped, corners[img_idx]);
    }
    std::cout << std::endl;

    Mat result, result_mask;
    blender->blend(result, result_mask);

    LOGLN("Compositing, time: " << ((getTickCount() - t) / getTickFrequency()) << " sec");

    // Preliminary result is in CV_16SC3 format, but all values are in [0,255] range,
    // so convert it to avoid user confusing
    result.convertTo(pano, CV_8U);

    return OK;
}



