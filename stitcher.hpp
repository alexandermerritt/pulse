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

#ifndef __STITCHER_HPP_INCLUDED__
#define __STITCHER_HPP_INCLUDED__

#include <opencv2/core/core.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/stitching/warpers.hpp>
#include <opencv2/stitching/detail/matchers.hpp>
#include <opencv2/stitching/detail/motion_estimators.hpp>
#include <opencv2/stitching/detail/exposure_compensate.hpp>
#include <opencv2/stitching/detail/seam_finders.hpp>
#include <opencv2/stitching/detail/blenders.hpp>
#include <opencv2/stitching/detail/camera.hpp>

//#include <opencv.hpp>
//#include <gpu/gpu.hpp>
//#include <core/gpumat.hpp>
//#include <stitching/stitcher.hpp>

#include "types.hpp"

using namespace std;
using namespace cv;

class PStitcher
{
public:
    enum { ORIG_RESOL = -1 };
    enum Status { OK, ERR_NEED_MORE_IMGS };

    // Creates stitcher with default parameters
    static PStitcher createDefault(bool try_use_gpu = false);


    // Step 1
    Status matchImages(images_t &images_in,
            features_t &features_out, matches_t &matches_out, indices_t &indices_out);

    // Step 2
    void estimateCameraParams(features_t &features_in, matches_t &matches_in,
            cameras_t &cameras_out);

    // Step 3
    Status composePanorama(images_t &images, cameras_t &cameras, cv::Mat &pano);

    //std::vector<int> component() const { return indices; }
    //cameras_t getCameras() const { return cameras; }

    // Below are various fine-tuning functions

    double registrationResol() const { return registr_resol; }
    void setRegistrationResol(double resol_mpx) { registr_resol = resol_mpx; }

    double seamEstimationResol() const { return seam_est_resol; }
    void setSeamEstimationResol(double resol_mpx) { seam_est_resol = resol_mpx; }

    double compositingResol() const { return compose_resol; }
    void setCompositingResol(double resol_mpx) { compose_resol = resol_mpx; }

    double panoConfidenceThresh() const { return conf_thresh; }
    void setPanoConfidenceThresh(double conf_thresh_) { conf_thresh = conf_thresh_; }

    bool waveCorrection() const { return do_wave_correct; }
    void setWaveCorrection(bool flag) { do_wave_correct = flag; }

    detail::WaveCorrectKind waveCorrectKind() const { return wave_correct_kind; }
    void setWaveCorrectKind(detail::WaveCorrectKind kind) { wave_correct_kind = kind; }

    Ptr<detail::FeaturesFinder> featuresFinder() { return features_finder; }
    const Ptr<detail::FeaturesFinder> featuresFinder() const { return features_finder; }
    void setFeaturesFinder(Ptr<detail::FeaturesFinder> features_finder_)
        { features_finder = features_finder_; }

    Ptr<detail::FeaturesMatcher> featuresMatcher() { return features_matcher; }
    const Ptr<detail::FeaturesMatcher> featuresMatcher() const { return features_matcher; }
    void setFeaturesMatcher(Ptr<detail::FeaturesMatcher> features_matcher_)
        { features_matcher = features_matcher_; }

    const cv::Mat& matchingMask() const { return matching_mask; }
    void setMatchingMask(const cv::Mat &mask)
    {
        CV_Assert(mask.type() == CV_8U && mask.cols == mask.rows);
        matching_mask = mask.clone();
    }

    Ptr<detail::BundleAdjusterBase> bundleAdjuster() { return bundle_adjuster; }
    const Ptr<detail::BundleAdjusterBase> bundleAdjuster() const { return bundle_adjuster; }
    void setBundleAdjuster(Ptr<detail::BundleAdjusterBase> bundle_adjuster_)
        { bundle_adjuster = bundle_adjuster_; }

    Ptr<WarperCreator> getWarper() { return warper; }
    const Ptr<WarperCreator> getWarper() const { return warper; }
    void setWarper(Ptr<WarperCreator> creator) { warper = creator; }

    Ptr<detail::ExposureCompensator> exposureCompensator() { return exposure_comp; }
    const Ptr<detail::ExposureCompensator> exposureCompensator() const { return exposure_comp; }
    void setExposureCompensator(Ptr<detail::ExposureCompensator> exposure_comp_)
        { exposure_comp = exposure_comp_; }

    Ptr<detail::SeamFinder> seamFinder() { return seam_finder; }
    const Ptr<detail::SeamFinder> seamFinder() const { return seam_finder; }
    void setSeamFinder(Ptr<detail::SeamFinder> seam_finder_) { seam_finder = seam_finder_; }

    Ptr<detail::Blender> getBlender() { return blender; }
    const Ptr<detail::Blender> getBlender() const { return blender; }
    void setBlender(Ptr<detail::Blender> b) { blender = b; }

private:
    PStitcher() {}

    double registr_resol;
    double seam_est_resol;
    double compose_resol;
    double conf_thresh;
    Ptr<detail::FeaturesFinder> features_finder;
    Ptr<detail::FeaturesMatcher> features_matcher;
    cv::Mat matching_mask;
    Ptr<detail::BundleAdjusterBase> bundle_adjuster;
    bool do_wave_correct;
    detail::WaveCorrectKind wave_correct_kind;
    Ptr<WarperCreator> warper;
    Ptr<detail::ExposureCompensator> exposure_comp;
    Ptr<detail::SeamFinder> seam_finder;
    Ptr<detail::Blender> blender;
};

#endif // __STITCHER_HPP_INCLUDED__
