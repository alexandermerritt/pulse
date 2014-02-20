/* file: io.cpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

/* C includes */
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <strings.h>
#include <getopt.h>

/* C++ includes */
#include <iostream>
#include <sstream>
#include <fstream>
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

/* Local includes */
#include "io.hpp"
#include "types.hpp"

using namespace std;
using namespace cv;

/* Internal functions */

// checks if path exists, is file, readable by user, etc, basically if opening
// and reading it will cause us to fail
static bool _file_okay(const string &path)
{
    struct stat buf;
    uid_t uid = geteuid();
    gid_t gid = getegid();

    if (stat(path.c_str(), &buf))
        return false;

    // check permissions
    if ((buf.st_uid == uid) && !(buf.st_mode & S_IRUSR))
        return false;
    else if ((buf.st_gid == gid) && !(buf.st_mode & S_IRGRP))
        return false;
    else if (!(buf.st_mode & S_IROTH))
        return false;

    // check file type
    if (!S_ISREG(buf.st_mode))
        return false;

    return true;
}

// return 0 if all okay, else > 0
// table contains T/F indicating which
static int file_okay(const paths_t &paths, vector<bool> &okay)
{
    int ret = 0, item = 0;
    if (paths.empty())
        return -EINVAL;
    okay.resize(paths.size());
    for (const string &path : paths)
            ret += !(okay[item++] = _file_okay(path));
    return ret;
}

/* Public functions */

// expect one path per line as input
void read_stdin(paths_t &paths)
{
    string line;
    paths.clear();
    while (getline(cin, line))
        paths.push_back(line);
}

// you should santize paths before calling this
int load_images(images_t &imgs, const paths_t &_paths)
{
    list<string> paths(_paths);
    vector<bool> okay;

    imgs.clear();

    std::cout << ">> loading " << paths.size() << " images" << std::endl;

    if (file_okay(paths, okay)) {
        size_t item = 0;
        for (const string &path : paths) {
            if (!okay[item++]) {
                cerr << "!! '" << path << "' not readable" << endl;
            }
        }
        return -EINVAL;
    }

    for (string &path : paths) {
        cv::Mat mat;
        std::cout << "."; std::cout.flush();
        mat = cv::imread(path);
        if (!mat.data)
            return -EINVAL;
        imgs.push_back(make_tuple(mat, path));
    }
    std::cout << std::endl;

    return 0;
}

int write_features(string &dirpath,
        vector< Mat > &imgs,
        vector< detail::ImageFeatures > &features)
{
    Mat img;
    /* TODO check dirpath, etc */
    for (size_t i = 0; i < features.size(); i++) {
        stringstream s;
        drawKeypoints(imgs[i], features[i].keypoints, img,
                Scalar::all(-1), DrawMatchesFlags::DEFAULT);
        s << dirpath << "/match-" << i << ".jpg";
        std::cout << "    writing " << s.str() << std::endl;
        if (!imwrite(s.str(), img))
            return -1;
    }
    img.release();
    return 0;
}

int write_image(string &filepath, const cv::Mat &img)
{
    return !imwrite(filepath, img);
}

int write_images(string &dirpath,
        const vector< cv::Mat > &imgs, string prefix)
{
    stringstream s;
    if (imgs.size() < 1)
        return -1;
    /* TODO check dirpath */
    for (size_t i = 0; i < imgs.size(); i++) {
        s.str(std::string()); // reset it
        s << dirpath << "/" << prefix << i << ".jpg";
        if (!imwrite(s.str(), imgs[i]))
            return -1;
    }
    return 0;
}

