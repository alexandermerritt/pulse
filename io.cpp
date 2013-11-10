/* file: io.cpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

/* C includes */
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
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

using namespace std;
using namespace cv;

/* Internal functions */

/* read all images in directory path into memory */
static int read_images(vector<cv::Mat> &imgs, string &dirpath)
{
    int err = 0;
    cv::Mat m;
    DIR *dir;
    struct dirent *ent;
    const char ext[] = ".jpg";
    string filename;
    if ((dir = opendir(dirpath.c_str()))) {
        while ((ent = readdir(dir))) {
            filename = ent->d_name;
            /* skip . and .. and anything hidden .* */
            if (filename[0] == '.')
                continue;
            /* skip if no extension */
            const char *pos = strrchr(filename.c_str(), '.');
            if (!pos)
                continue;
            /* skip not jpg */
            if (0 != strncasecmp(pos, ext, strlen(ext)))
                continue;
            m = cv::imread(dirpath + "/" + filename);
            if (!m.data) {
                cerr << "!! Error reading " << filename << endl;
                continue;
            }
            imgs.push_back(m);
            cout << "    " << filename << endl;
        }
        closedir(dir);
    } else {
        err = -1;
    }
    cout << endl;
    return err;
}

/* Public functions */

int load_images(vector<cv::Mat> &imgs, string &dirlist)
{
    int err = 0;
    ifstream ifs;
    string dirpath;

    cout << ">> loading images" << endl;

    ifs.open(dirlist.c_str());
    if (!ifs.is_open())
        return -1;

    while (!ifs.eof()) {
        ifs >> dirpath;
        if (dirpath[0] == '#')
            continue;
        if (dirpath.empty())
            continue;
        err = read_images(imgs, dirpath);
        if (err)
            break;
        dirpath.clear();
    }

    ifs.close();
    return err;
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

int write_images(string &dirpath, vector< cv::Mat > &imgs)
{
    stringstream s;
    if (imgs.size() < 1)
        return -1;
    /* TODO check dirpath */
    for (size_t i = 0; i < imgs.size(); i++) {
        s << dirpath << "/pano-" << i << ".jpg";
        if (!imwrite(s.str(), imgs[i]))
            return -1;
    }
    return 0;
}

