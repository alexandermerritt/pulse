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
#include <opencv2/calib3d/calib3d.hpp> // for CV_RANSAC
#include <opencv2/contrib/contrib.hpp> // for LevMarqSparse

#include <iostream>
#include "stitcher.hpp"
#include "matchers.hpp"
#include "motion_estimators.hpp"
#include "io.hpp"

#include <tuple>
#include <timer.h>
#include "types.hpp"

using namespace std;
using namespace cv;

static inline void print_event(const char *str)
{
    struct timespec sp;
    unsigned long ns;
    clock_gettime(CLOCK_REALTIME, &sp);
    ns = sp.tv_sec * 1e9 + sp.tv_nsec;
    fprintf(stderr, "%lu ocv_%s\n", ns, str);
}

PStitcher PStitcher::createDefault(void)
{
    PStitcher stitcher;
    stitcher.setRegistrationResol(0.6);
    stitcher.setSeamEstimationResol(0.1);
    stitcher.setCompositingResol(ORIG_RESOL);
    stitcher.setWaveCorrection(true);
    stitcher.setWaveCorrectKind(detail::WAVE_CORRECT_HORIZ);

    return stitcher;
}

int PStitcher::findFeatures(const images_t &images, features_t &features,
        bool try_gpu, int num_threads)
{
    Ptr<detail::FeaturesFinder> finder;

    if ((int)images.size() < 2)
        return -1;

    features.clear();
    features.resize(images.size());

    std::cout << ">> feature detection" << std::endl;

    double work_scale = 1;
    if (registr_resol >= 0)
        work_scale = min(1.0,
                sqrt(registr_resol * 1e6 / images[0].size().area()));

    if (try_gpu)
        return -1;

    num_threads = std::min(images.size(), (unsigned long)num_threads);

#pragma omp parallel \
    private(finder) \
    num_threads(num_threads)
    {
        #define SURF_PARAMS 4000., 1, 6
        finder = new detail::SurfFeaturesFinder(SURF_PARAMS);
        #undef SURF_PARAMS

#pragma omp for
        for (size_t i = 0; i < images.size(); ++i) {
            Mat img; // TODO put outside loop?
            resize(images[i], img, Size(), work_scale, work_scale);
            //print_event("find-features-img-start");
            (*finder)(img, features[i]); /* modules/stitching/src/matchers.cpp */
            //print_event("find-features-img-end");
            std::cout << "    " << features[i].keypoints.size() << std::endl;
            //features[i].img_idx = (int)i; // XXX what is this for?
        }

        finder->collectGarbage();
    }

    return 0;
}

// formerly BestOf2NearestMatcher::match(feature1, feature2, matches_info)
void PStitcher::doMatch(cv::Ptr< cv::detail::FeaturesMatcher > &matcher,
        const ImageFeatures &features1, const ImageFeatures &features2,
        MatchesInfo &matches_info, int thresh1, int thresh2)
{
    (*matcher)(features1, features2, matches_info);

    // Check if it makes sense to find homography
    if (matches_info.matches.size() < static_cast<size_t>(thresh1))
        return;

    // Construct point-point correspondences for homography estimation
    Mat src_points(1, static_cast<int>(matches_info.matches.size()), CV_32FC2);
    Mat dst_points(1, static_cast<int>(matches_info.matches.size()), CV_32FC2);
    for (size_t i = 0; i < matches_info.matches.size(); ++i)
    {
        const DMatch& m = matches_info.matches[i];

        Point2f p = features1.keypoints[m.queryIdx].pt;
        p.x -= features1.img_size.width * 0.5f;
        p.y -= features1.img_size.height * 0.5f;
        src_points.at<Point2f>(0, static_cast<int>(i)) = p;

        p = features2.keypoints[m.trainIdx].pt;
        p.x -= features2.img_size.width * 0.5f;
        p.y -= features2.img_size.height * 0.5f;
        dst_points.at<Point2f>(0, static_cast<int>(i)) = p;
    }

    // Find pair-wise motion
    matches_info.H = findHomography(src_points, dst_points, matches_info.inliers_mask, CV_RANSAC);
    if (std::abs(determinant(matches_info.H)) < numeric_limits<double>::epsilon())
        return;

    // Find number of inliers
    matches_info.num_inliers = 0;
    for (size_t i = 0; i < matches_info.inliers_mask.size(); ++i)
        if (matches_info.inliers_mask[i])
            matches_info.num_inliers++;

    // These coeffs are from paper M. Brown and D. Lowe. "Automatic Panoramic Image Stitching
    // using Invariant Features"
    matches_info.confidence = matches_info.num_inliers / (8 + 0.3 * matches_info.matches.size());

    // Set zero confidence to remove matches between too close images, as they don't provide
    // additional information anyway. The threshold was set experimentally.
    matches_info.confidence = matches_info.confidence > 3. ? 0. : matches_info.confidence;

    // Check if we should try to refine motion
    if (matches_info.num_inliers < thresh2)
        return;

    // Construct point-point correspondences for inliers only
    src_points.create(1, matches_info.num_inliers, CV_32FC2);
    dst_points.create(1, matches_info.num_inliers, CV_32FC2);
    int inlier_idx = 0;
    for (size_t i = 0; i < matches_info.matches.size(); ++i)
    {
        if (!matches_info.inliers_mask[i])
            continue;

        const DMatch& m = matches_info.matches[i];

        Point2f p = features1.keypoints[m.queryIdx].pt;
        p.x -= features1.img_size.width * 0.5f;
        p.y -= features1.img_size.height * 0.5f;
        src_points.at<Point2f>(0, inlier_idx) = p;

        p = features2.keypoints[m.trainIdx].pt;
        p.x -= features2.img_size.width * 0.5f;
        p.y -= features2.img_size.height * 0.5f;
        dst_points.at<Point2f>(0, inlier_idx) = p;

        inlier_idx++;
    }

    // Rerun motion estimation on inliers only
    matches_info.H = findHomography(src_points, dst_points, CV_RANSAC);
}

void PStitcher::bestOf2NearestMatcher(const features_t &features,
        matches_t &matches, bool try_gpu, int num_threads, float match_conf)
{
    // --------------------------- BestOf2NearestMatcher construtor
    cv::Ptr< cv::detail::FeaturesMatcher > matcher;

    // ------------------------ PFeaturesMatcher::operator(features, matches)
    const int num_images = static_cast<int>(features.size());

    // removed specifying a mask as input to this
    Mat_<uchar> mask_ = Mat::ones(num_images, num_images, CV_8U);

    vector<pair<int,int> > near_pairs;
    for (int i = 0; i < num_images - 1; ++i)
        for (int j = i + 1; j < num_images; ++j)
            if (features[i].keypoints.size() > 0 && features[j].keypoints.size() > 0 && mask_(i, j))
                near_pairs.push_back(make_pair(i, j));
    std::cout << "    " << near_pairs.size() << " matches to perform" << std::endl;

    matches.resize(num_images * num_images);

    num_threads = std::min(features.size(), (unsigned long)num_threads); // FIXME
    if (try_gpu)
        abort();

    // ---------------------------------------- MatchPairsBody
    // Replaced MatchPairsBody class with its operator() directly
#pragma omp parallel \
    private(matcher) \
    num_threads(num_threads)
    {
        matcher = new CpuMatcher(match_conf);
#pragma omp for
        for (size_t i = 0; i < near_pairs.size(); ++i)
        {
            int from = near_pairs[i].first;
            int to = near_pairs[i].second;
            int pair_idx = from*num_images + to;

            progress_bar(i, near_pairs.size());

            // Calls match() on subclass, which is probably
            // PBestOf2NearestMatcher::match().
            doMatch(matcher, features[from], features[to], matches[pair_idx]);

            matches[pair_idx].src_img_idx = from;
            matches[pair_idx].dst_img_idx = to;

            size_t dual_pair_idx = to*num_images + from;

            matches[dual_pair_idx] = matches[pair_idx];
            matches[dual_pair_idx].src_img_idx = to;
            matches[dual_pair_idx].dst_img_idx = from;

            if (!matches[pair_idx].H.empty())
                matches[dual_pair_idx].H = matches[pair_idx].H.inv();

            for (size_t j = 0; j < matches[dual_pair_idx].matches.size(); ++j)
                std::swap(matches[dual_pair_idx].matches[j].queryIdx,
                        matches[dual_pair_idx].matches[j].trainIdx);
        }
    }
    progress_clear();
    std::cout << std::endl;
}

int PStitcher::matchFeatures(const features_t &features, matches_t &matches,
        bool try_gpu, int num_threads)
{
    //Ptr< PFeaturesMatcher > matcher;
    float match_conf = 0.2f;

    std::cout << ">> pairwise matching" << std::endl;

    //matcher = new PBestOf2NearestMatcher(try_gpu, num_threads);

    if (try_gpu)
        return -1;

    matches.clear();
    bestOf2NearestMatcher(features, matches, try_gpu, num_threads, match_conf);

    return 0;
}

// Reduce features,matches to those images which are related.
// Caller must then extract images from original vector specified by indices
// before giving to panorama composer.
void PStitcher::findRelated(features_t &features, matches_t &matches,
        indices_t &indices, float conf_thresh)
{
    indices.clear();
    // Leave only images we are sure are from the same panorama
    indices = detail::leaveBiggestComponent(features, matches, (float)conf_thresh);
}

// serial CPU-only code
void PStitcher::estimateCameraParams(features_t &features,
        matches_t &matches, cameras_t &cameras, float conf_thresh)
{
#if 1 // alex's modified code.. tried to make faster
    cv::Ptr< PBundleAdjusterBase > adjuster;
    detail::HomographyBasedEstimator estimator;

    std::cout << "    estimator" << std::endl;

    cameras.clear();
    estimator(features, matches, cameras);

    for (size_t i = 0; i < cameras.size(); ++i)
    {
        Mat R;
        cameras[i].R.convertTo(R, CV_32F);
        cameras[i].R = R;
    }

    CvTermCriteria crit;
    crit.type = 3;
    crit.max_iter =10;
    crit.epsilon = 2.e-5;

    adjuster = new PBundleAdjusterRay();
    adjuster->setTermCriteria(crit);
    std::cout << "    adjuster" << std::endl;
    (*adjuster)(features, matches, cameras); // XXX

#else

    //cv::Ptr< cv::detail::BundleAdjusterBase > adjuster;
    cv::Ptr< PBundleAdjusterBase > adjuster;
    struct timer t;

    detail::HomographyBasedEstimator estimator;

    std::cout << ">> camera adjustment estimation" << std::endl;

    cameras.clear();

    std::cout << "    estimator" << std::endl;
    estimator(features, matches, cameras); // very quick operation

    std::cout << "    conversion " << std::endl; // also not slow
    for (size_t i = 0; i < cameras.size(); ++i)
    {
        Mat R;
        cameras[i].R.convertTo(R, CV_32F);
        cameras[i].R = R;
        LOGLN("Initial intrinsic parameters #" << indices[i] + 1 << ":\n " << cameras[i].K());
    }

    unsigned long usec;
    timer_init(CLOCK_REALTIME, &t);
    std::cout << "    bundle adjustment (" << conf_thresh << " match conf)" << std::endl;
    //adjuster = new cv::detail::BundleAdjusterRay();
    //adjuster = new cv::detail::BundleAdjusterReproj();
    adjuster = new PBundleAdjusterRay();
    //adjuster = new PBundleAdjusterReproj();
    (*adjuster).setConfThresh(conf_thresh);
    timer_start(&t);
    (*adjuster)(features, matches, cameras); // XXX
    usec = timer_end(&t, MICROSECONDS);
    std::cout << "    bundle took " << usec / 1000000.0f << " sec" << std::endl;

    if (do_wave_correct)
    {
        vector<Mat> rmats;
        for (size_t i = 0; i < cameras.size(); ++i)
            rmats.push_back(cameras[i].R);
        detail::waveCorrect(rmats, wave_correct_kind);
        for (size_t i = 0; i < cameras.size(); ++i)
            cameras[i].R = rmats[i];
    }
#endif
}

int PStitcher::composePanorama(images_t &images, cameras_t &cameras,
        cv::Mat & pano, bool try_gpu, int num_threads)
{
    cv::Ptr< cv::detail::ExposureCompensator > exposure_comp;
    cv::Ptr< cv::detail::SeamFinder > seam_finder;
    cv::Ptr< cv::detail::Blender > blender;
    cv::Ptr< cv::WarperCreator > warper;
    cv::Ptr< cv::detail::RotationWarper > w;
    cv::Mat img, full_img;
    images_t seam_est_images;

    unsigned long usec;
    struct timer t;
    timer_init(CLOCK_REALTIME, &t);

    std::cout << ">> composing panorama" << std::endl;

    if (try_gpu)
        return -1;

    warper = new cv::SphericalWarper();
    seam_finder = new cv::detail::GraphCutSeamFinder(
            cv::detail::GraphCutSeamFinder::COST_COLOR);

    blender = new detail::MultiBandBlender(try_gpu);

    // no GPGPU counterpart
    exposure_comp = new detail::BlocksGainCompensator();

    // compute seam scales (and recompute work scale)
    double work_scale = 1;
    if (registr_resol >= 0)
        work_scale = min(1.0,
                sqrt(registr_resol * 1e6 / images[0].size().area()));

    double seam_scale = std::min(1.0,
            sqrt(seam_est_resol * 1e6 / images[0].size().area()));
    double seam_work_aspect = seam_scale / work_scale;

    for (auto &image : images) {
        cv::resize(image, img, cv::Size(), seam_scale, seam_scale);
        seam_est_images.push_back(img.clone());
    }
    img.release();

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
    std::cout << "    warping images " << std::endl;
    timer_start(&t);
    w = warper->create(float(warped_image_scale * seam_work_aspect));
    for (size_t i = 0; i < images.size(); ++i)
    {
        Mat_<float> K;
        cameras[i].K().convertTo(K, CV_32F);
        K(0,0) *= (float)seam_work_aspect;
        K(0,2) *= (float)seam_work_aspect;
        K(1,1) *= (float)seam_work_aspect;
        K(1,2) *= (float)seam_work_aspect;

        corners[i] = w->warp(seam_est_images[i], K, cameras[i].R,
                INTER_LINEAR, BORDER_REFLECT, images_warped[i]);
        sizes[i] = images_warped[i].size();
        w->warp(masks[i], K, cameras[i].R, INTER_NEAREST, BORDER_CONSTANT, masks_warped[i]);

    }

    vector<Mat> images_warped_f(images.size());
    for (size_t i = 0; i < images.size(); ++i)
        images_warped[i].convertTo(images_warped_f[i], CV_32F);

    usec = timer_end(&t, MICROSECONDS);
    std::cout << "    warping took " << usec / 1000000.0f << " sec" << std::endl;

    // Find seams
    std::cout << "    finding seams " << std::endl;
    timer_start(&t);
    exposure_comp->feed(corners, images_warped, masks_warped);
    seam_finder->find(images_warped_f, corners, masks_warped);
    usec = timer_end(&t, MICROSECONDS);
    std::cout << "      seam finding took " << usec / 1000000.0f << " sec" << std::endl;

    // Release unused memory
    seam_est_images.clear();
    images_warped.clear();
    images_warped_f.clear();
    masks.clear();

    std::cout << "    compositing" << std::endl;
    timer_start(&t);

    Mat img_warped, img_warped_s;
    Mat dilated_mask, seam_mask, mask, mask_warped;

    //double compose_seam_aspect = 1;
    double compose_work_aspect = 1;
    bool is_blender_prepared = false;

    double compose_scale = 1;

    // prepare for blender loop --------------------------------------
    if (compose_resol > 0)
        compose_scale = min(1.0,
                sqrt(compose_resol * 1e6 / images[0].size().area()));

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
            sz.width  = cvRound(images[i].size().width * compose_scale);
            sz.height = cvRound(images[i].size().height * compose_scale);
        }

        Mat K;
        cameras[i].K().convertTo(K, CV_32F);
        Rect roi = w->warpRoi(sz, K, cameras[i].R);
        corners[i] = roi.tl();
        sizes[i] = roi.size();
    }

    // blender loop --------------------------------------
    for (size_t img_idx = 0; img_idx < images.size(); ++img_idx)
    {
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

    Mat result, result_mask;
    blender->blend(result, result_mask);

    usec = timer_end(&t, MICROSECONDS);
    std::cout << "    compositing took " << usec / 1000000.0f << " sec" << std::endl;

    // Preliminary result is in CV_16SC3 format, but all values are in [0,255] range,
    // so convert it to avoid user confusing
    result.convertTo(pano, CV_8U);

    return OK;
}



