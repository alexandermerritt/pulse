#include <opencv2/calib3d/calib3d.hpp> // for CV_RANSAC
#include <opencv2/contrib/contrib.hpp> // for LevMarqSparse
#include <opencv2/core/core.hpp>
#include <opencv2/core/gpumat.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/gpu/gpu.hpp>
#include <opencv2/opencv.hpp>
#include <opencv2/stitching/detail/blenders.hpp>
#include <opencv2/stitching/detail/camera.hpp>
#include <opencv2/stitching/detail/exposure_compensate.hpp>
#include <opencv2/stitching/detail/matchers.hpp>
#include <opencv2/stitching/detail/motion_estimators.hpp>
#include <opencv2/stitching/detail/seam_finders.hpp>
#include <opencv2/stitching/stitcher.hpp>
#include <opencv2/stitching/warpers.hpp>

#include <iostream>
#include <memory>
#include <sstream>
#include <fstream>

#include "io.hpp"
#include "timer.h"
#include "types.hpp"

using namespace std;
using namespace cv;

#define expand(p) \
    (p).hess, (p).nocts, (p).nlayers, (p).noctdesc, (p).nlayerdesc

// XXX images which are too small can't have many octaves/layers applied
struct surf_params params = {
    .hess = 300.,
    .nocts = 3, .nlayers = 4,
    .noctdesc = 4, .nlayerdesc = 4,
};

#define ITERS 1

// for simplicity we assume a relative or absolute path to some image file
// with extension
string basename(string &path)
{
    string ret;
    auto i = path.rfind('/');
    auto j = path.rfind('.');
    if (i == string::npos) {
        i = 0;
    } else {
        i++;
        j = j-i;
    }
    return path.substr(i,j);
}

template<typename T>
void summary(vector<T> &v, double &mean, double &median)
{
    double sum = 0.;
    for (T &i : v)
        sum += i;
    mean = sum / (double)v.size();
    sort(v.begin(), v.end());
    if ((v.size() % 2) == 1) {
        median = v[v.size() / 2];
    } else {
        int idx = v.size() / 2.;
        median = (v[idx] + v[idx+1]) / 2.;
    }
}

int hog(dir_t &dir, bool ongpu = true, bool doscale = false)
{
    vector<unsigned long> times(ITERS);
    paths_t imagepaths;
    struct timer t;

    gpu::HOGDescriptor hog;
    gpu::GpuMat gm;
    double hoghit = 0.;

    if (listdir(dir, imagepaths))
        return 1;

    hog.setSVMDetector(gpu::HOGDescriptor::getDefaultPeopleDetector());

    cout << "imgid width height depth "
        "hit " // HOG params
        "ongpu scaled "
        "locs hog_mean hog_med" << endl;

    for (string &path : imagepaths) {
        Mat scaled, gray, img = imread(path);
        if (!img.data)
            return -1;

        cv::cvtColor(img, gray, CV_BGR2BGRA);
        if (doscale) {
            double work_scale = min(1.0, sqrt(1e6 / img.size().area()));
            if (work_scale < 0.95) {
                resize(gray, scaled, Size(), work_scale, work_scale);
            }
        } else {
            scaled = gray;
        }

        gm.upload(scaled);

        timer_init(CLOCK_REALTIME, &t);
        vector<Rect> loc;

        try {
            //hog.detectMultiScale(gm, loc, hoghit); // burn-in
            for (size_t i = 0; i < times.size(); i++) {
                timer_start(&t);
                hog.detectMultiScale(gm, loc, hoghit);
                times[i] = timer_end(&t, MICROSECONDS);
            }
        } catch (cv::Exception &e) {
            return -1;
        }
        double mean, median;
        summary(times, mean, median);

        stringstream ss;
        ss << basename(path) << " ";
        ss << scaled.cols << " " << scaled.rows << " ";
        ss << scaled.elemSize() << " ";
        ss << hoghit << " ";
        ss << (ongpu ? "1" : "0") << " ";
        ss << (doscale ? "1" : "0") << " ";
        ss << loc.size() << " ";
        ss << mean << " " << median << " ";
        cout << ss.str() << endl;
    }

    return 0;
}

int surf(dir_t &dir, bool ongpu = true, bool doscale = false)
{
    vector<unsigned long> times(ITERS);
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    unique_ptr<detail::FeaturesFinder> finder;
    if (ongpu)
        finder.reset(new detail::SurfFeaturesFinderGpu(expand(params)));
    else
        finder.reset(new detail::SurfFeaturesFinder(expand(params)));
    if (!finder)
        return -1;

    cout << "imgid width height depth "
        "hess nocts nlay noctdesc nlayerdesc " // SURF params
        "ongpu scaled "
        "surf_mean surf_med feat_n feat_bytes" << endl;

    for (string &path : imagepaths) {
        Mat scaled, img = imread(path);
        if (!img.data)
            return -1;
        scaled = img;

        if (doscale) {
            double work_scale = min(1.0, sqrt(1e6 / img.size().area()));
            if (work_scale < 0.95) {
                resize(img, scaled, Size(), work_scale, work_scale);
            }
        }

        timer_init(CLOCK_REALTIME, &t);
        detail::ImageFeatures feature;
        try {
            finder->operator()(scaled, feature); // burn-in
            for (size_t i = 0; i < times.size(); i++) {
                timer_start(&t);
                finder->operator()(scaled, feature);
                times[i] = timer_end(&t, MICROSECONDS);
            }
        } catch (cv::Exception &e) {
            return -1;
        }
        double mean, median;
        summary(times, mean, median);

        stringstream ss;
        ss << basename(path) << " ";
        ss << scaled.cols << " " << scaled.rows << " ";
        ss << scaled.elemSize() << " ";
        ss << params.hess << " ";
        ss << params.nocts << " ";
        ss << params.nlayers << " ";
        ss << params.noctdesc << " ";
        ss << params.nlayerdesc << " ";
        ss << (ongpu ? "1" : "0") << " ";
        ss << (doscale ? "1" : "0") << " ";
        ss << mean << " " << median << " ";
        ss << feature.keypoints.size() << " ";
        ss << feature.keypoints.size() * sizeof(detail::ImageFeatures) << " ";
        cout << ss.str() << endl;
    }
    return 0;
}

enum which
{
    INVALID, SURF, HOG,
};

int main(int argc, char *argv[])
{
    if (argc != 3) {
        cerr << "Usage: ./feature alg dir/"
            << endl;
        return 1;
    }

    enum which which;

    string alg(argv[1]);
    dir_t dir(argv[2]);

    if (alg == "surf") {
        which = SURF;
    } else if (alg == "hog") {
        which = HOG;
    } else {
        return 1;
    }

    switch (which) {
        case SURF: {
            if (surf(dir, true, false))
                return 1;
        } break;
        case HOG: {
            if (hog(dir, true, false))
                return 1;
        } break;
        default: return 1;
    }

    return 0;
}