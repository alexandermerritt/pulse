/* file: types.hpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

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

typedef std::vector< cv::Mat > images_t;

typedef std::vector< cv::detail::ImageFeatures > features_t;
typedef std::vector< cv::detail::MatchesInfo > matches_t;

typedef std::vector< cv::detail::CameraParams > cameras_t;
typedef cv::detail::WaveCorrectKind wave_t;

typedef std::vector< int > indices_t;

typedef std::set< std::pair< int, int > > MatchesSet;

