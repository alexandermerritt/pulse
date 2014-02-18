/* file: memcached.cpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 *
 * Integration with memcached.
 */

/* C includes */
#include <libmemcached/memcached.h>
//#include <stdio.h>
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <unistd.h>
//#include <dirent.h>
//#include <errno.h>
//#include <strings.h>
//#include <getopt.h>

/* C++ includes */
//#include <iostream>
//#include <sstream>
//#include <fstream>
//#include <iomanip>

/* OpenCV includes */
//#include <opencv2/opencv.hpp>
//#include <opencv2/gpu/gpu.hpp>
//#include <opencv2/core/gpumat.hpp>
//#include <opencv2/stitching/stitcher.hpp>
//#include <opencv2/core/core.hpp>
//#include <opencv2/features2d/features2d.hpp>
//#include <opencv2/stitching/warpers.hpp>
//#include <opencv2/stitching/detail/matchers.hpp>
//#include <opencv2/stitching/detail/motion_estimators.hpp>
//#include <opencv2/stitching/detail/exposure_compensate.hpp>
//#include <opencv2/stitching/detail/seam_finders.hpp>
//#include <opencv2/stitching/detail/blenders.hpp>
//#include <opencv2/stitching/detail/camera.hpp>

/* Local includes */
#include "io.hpp"
#include "types.hpp"

using namespace std;
using namespace cv;

//===----------------------------------------------------------------------===//
// Definitions
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Private functions
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Public functions
//===----------------------------------------------------------------------===//

int watch_memcached(void)
{
    fprintf(stderr, "%s\n", __func__);
    return -EIO;
}

