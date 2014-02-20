/* file: types.hpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

#ifndef TYPES_HPP_INCLUDED
#define TYPES_HPP_INCLUDED

/* C++ includes */
#include <vector>
#include <set>

/* OpenCV includes */
#include <opencv2/core/core.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/stitching/warpers.hpp>
#include <opencv2/stitching/detail/matchers.hpp>
#include <opencv2/stitching/detail/motion_estimators.hpp>
#include <opencv2/stitching/detail/exposure_compensate.hpp>
#include <opencv2/stitching/detail/seam_finders.hpp>
#include <opencv2/stitching/detail/blenders.hpp>
#include <opencv2/stitching/detail/camera.hpp>

typedef std::list< std::string > paths_t;

typedef std::vector< cv::detail::ImageFeatures > features_t;
typedef std::vector< cv::detail::MatchesInfo > matches_t;

typedef std::vector< cv::detail::CameraParams > cameras_t;
typedef cv::detail::WaveCorrectKind wave_t;

typedef std::vector< int > indices_t;

typedef std::set< std::pair< int, int > > MatchesSet;

/* "image" is its data and path on disk */
typedef std::tuple< cv::Mat, std::string > image_t;
typedef std::vector< image_t > images_t;

/* for sorting an images_t by path */
static inline bool
image_path_compare(const image_t &a, const image_t &b)
{
    return std::get<1>(a) < std::get<1>(b);
}

#endif /* TYPES_HPP_INCLUDED */

