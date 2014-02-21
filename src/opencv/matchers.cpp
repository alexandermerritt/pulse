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
#include <opencv2/gpu/gpu.hpp> // BFMatcher_GPU

#include <iostream>

#include "types.hpp"
#include "matchers.hpp"

// TODO remove use of these
using namespace std;
using namespace cv;
using namespace cv::detail;

#ifdef HAVE_OPENCV_GPU
using namespace cv::gpu;
#endif

#ifdef HAVE_OPENCV_NONFREE
#include "opencv2/nonfree/nonfree.hpp"

static bool makeUseOfNonfree = initModule_nonfree();
#endif

//////////////////////////////////////////////////////////////////////////////


void CpuMatcher::match(const ImageFeatures &features1, const ImageFeatures &features2, MatchesInfo& matches_info)
{
    CV_Assert(features1.descriptors.type() == features2.descriptors.type());
    CV_Assert(features2.descriptors.depth() == CV_8U || features2.descriptors.depth() == CV_32F);

#ifdef HAVE_TEGRA_OPTIMIZATION
    if (tegra::match2nearest(features1, features2, matches_info, match_conf_))
        return;
#endif

    matches_info.matches.clear();

    Ptr<flann::IndexParams> indexParams = new flann::KDTreeIndexParams();
    Ptr<flann::SearchParams> searchParams = new flann::SearchParams();

    if (features2.descriptors.depth() == CV_8U)
    {
        indexParams->setAlgorithm(cvflann::FLANN_INDEX_LSH);
        searchParams->setAlgorithm(cvflann::FLANN_INDEX_LSH);
    }

    FlannBasedMatcher matcher(indexParams, searchParams);
    vector< vector<DMatch> > pair_matches;
    MatchesSet matches;

    // Find 1->2 matches
    matcher.knnMatch(features1.descriptors, features2.descriptors, pair_matches, 2);
    for (size_t i = 0; i < pair_matches.size(); ++i)
    {
        if (pair_matches[i].size() < 2)
            continue;
        const DMatch& m0 = pair_matches[i][0];
        const DMatch& m1 = pair_matches[i][1];
        if (m0.distance < (1.f - match_conf_) * m1.distance)
        {
            matches_info.matches.push_back(m0);
            matches.insert(make_pair(m0.queryIdx, m0.trainIdx));
        }
    }
    LOG("\n1->2 matches: " << matches_info.matches.size() << endl);

    // Find 2->1 matches
    pair_matches.clear();
    matcher.knnMatch(features2.descriptors, features1.descriptors, pair_matches, 2);
    for (size_t i = 0; i < pair_matches.size(); ++i)
    {
        if (pair_matches[i].size() < 2)
            continue;
        const DMatch& m0 = pair_matches[i][0];
        const DMatch& m1 = pair_matches[i][1];
        if (m0.distance < (1.f - match_conf_) * m1.distance)
            if (matches.find(make_pair(m0.trainIdx, m0.queryIdx)) == matches.end())
                matches_info.matches.push_back(DMatch(m0.trainIdx, m0.queryIdx, m0.distance));
    }
    LOG("1->2 & 2->1 matches: " << matches_info.matches.size() << endl);
}

#ifdef HAVE_OPENCV_GPU
void GpuMatcher::match(const ImageFeatures &features1, const ImageFeatures &features2, MatchesInfo& matches_info)
{
    matches_info.matches.clear();

    ensureSizeIsEnough(features1.descriptors.size(), features1.descriptors.type(), descriptors1_);
    ensureSizeIsEnough(features2.descriptors.size(), features2.descriptors.type(), descriptors2_);

    descriptors1_.upload(features1.descriptors);
    descriptors2_.upload(features2.descriptors);

    BFMatcher_GPU matcher(NORM_L2);
    MatchesSet matches;

    // Find 1->2 matches
    pair_matches.clear();
    matcher.knnMatchSingle(descriptors1_, descriptors2_, train_idx_, distance_, all_dist_, 2);
    matcher.knnMatchDownload(train_idx_, distance_, pair_matches);
    for (size_t i = 0; i < pair_matches.size(); ++i)
    {
        if (pair_matches[i].size() < 2)
            continue;
        const DMatch& m0 = pair_matches[i][0];
        const DMatch& m1 = pair_matches[i][1];
        if (m0.distance < (1.f - match_conf_) * m1.distance)
        {
            matches_info.matches.push_back(m0);
            matches.insert(make_pair(m0.queryIdx, m0.trainIdx));
        }
    }

    // Find 2->1 matches
    pair_matches.clear();
    matcher.knnMatchSingle(descriptors2_, descriptors1_, train_idx_, distance_, all_dist_, 2);
    matcher.knnMatchDownload(train_idx_, distance_, pair_matches);
    for (size_t i = 0; i < pair_matches.size(); ++i)
    {
        if (pair_matches[i].size() < 2)
            continue;
        const DMatch& m0 = pair_matches[i][0];
        const DMatch& m1 = pair_matches[i][1];
        if (m0.distance < (1.f - match_conf_) * m1.distance)
            if (matches.find(make_pair(m0.trainIdx, m0.queryIdx)) == matches.end())
                matches_info.matches.push_back(DMatch(m0.trainIdx, m0.queryIdx, m0.distance));
    }
}

void GpuMatcher::collectGarbage()
{
    descriptors1_.release();
    descriptors2_.release();
    train_idx_.release();
    distance_.release();
    all_dist_.release();
    vector< vector<DMatch> >().swap(pair_matches);
}
#endif

