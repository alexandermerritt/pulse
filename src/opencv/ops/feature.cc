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

#include <cuda_runtime_api.h>

#include <iostream>
#include <memory>
#include <sstream>
#include <fstream>

#include "io.hpp"
#include "timer.h"
#include "types.hpp"

using namespace std;
using namespace cv;

#define PREFIX "app "

#define expand(p) \
    (p).hess, (p).nocts, (p).nlayers, (p).noctdesc, (p).nlayerdesc

// XXX images which are too small can't have many octaves/layers applied
struct surf_params params = {
    .hess = 300.,
    .nocts = 3, .nlayers = 4,
    .noctdesc = 4, .nlayerdesc = 4,
};

string gpuname;

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

char * fixname(char *name)
{
    char *c = name;
    while (*c != '\0') {
        if (*c == ' ')
            *c = '_';
        if (*c >= 'A' && *c <= 'Z')
            *c = (*c - 'A') + 'a';
        c++;
    }
    return name;
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
    paths_t imagepaths;
    struct timer t;

    double hoghit = 0.;

    if (listdir(dir, imagepaths))
        return 1;

    cout << "imgid width height depth "
        "hit " // HOG params
        "ongpu scaled "
        "locs hog_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat img = imread(path);
        if (!img.data)
            return -1;

        fprintf(stderr,PREFIX "loop start\n");

        fprintf(stderr,PREFIX "making hog\n");
        gpu::HOGDescriptor hog;
        gpu::GpuMat gm;
        fprintf(stderr,PREFIX "setting hog detector\n");
        hog.setSVMDetector(gpu::HOGDescriptor::getDefaultPeopleDetector());

        fprintf(stderr,PREFIX "uploading img\n");
        gm.upload(img);

        fprintf(stderr,PREFIX "new gpumat 'gray'\n");
        gpu::GpuMat gray;

        fprintf(stderr,PREFIX "convert to grayscale gpu2gpu\n");
        gpu::cvtColor(gm, gray, CV_BGR2BGRA);
#if 0
        if (doscale) {
            double work_scale = min(1.0, sqrt(1e6 / img.size().area()));
            if (work_scale < 0.95) {
                resize(gray, scaled, Size(), work_scale, work_scale);
            }
        } else {
            scaled = gray;
        }
#endif

        timer_init(CLOCK_REALTIME, &t);
        vector<Rect> loc;

        unsigned long hogt = 0UL;
        try {
            fprintf(stderr,PREFIX "hog begin\n");
            timer_start(&t);
            hog.detectMultiScale(gray, loc, hoghit);
            hogt = timer_end(&t, MICROSECONDS);
            fprintf(stderr,PREFIX "hog end\n");
        } catch (cv::Exception &e) {
            return -1;
        }
        //double mean, median;
        //summary(times, mean, median);

        stringstream ss;
        ss << basename(path) << " ";
        ss << gray.cols << " " << gray.rows << " ";
        ss << gray.elemSize() << " ";
        ss << hoghit << " ";
        ss << (ongpu ? "1" : "0") << " ";
        ss << (doscale ? "1" : "0") << " ";
        ss << loc.size() << " ";
        ss << hogt << " ";
        ss << gpuname << " ";
        //ss << mean << " " << median << " ";
        cout << ss.str() << endl;

        fprintf(stderr,PREFIX "loop end\n");
    }

    return 0;
}

// XXX needs updating to match hog
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

// ./feature alg n dir/
// you can omit all the extra info printed by sending stderr to /dev/null
// when preloading lib.so, set iter to 1
int main(int argc, char *argv[])
{
    if (argc != 4) {
        cerr << "Usage: ./feature alg iters dir/"
            << endl;
        return 1;
    }

    enum which which;

    string alg(argv[1]);
    int iters = atoi(argv[2]);
    dir_t dir(argv[3]);

    if (alg == "surf") {
        which = SURF;
    } else if (alg == "hog") {
        which = HOG;
    } else {
        return 1;
    }

    int devID = 1;
    cv::gpu::setDevice(devID);
    cudaDeviceProp prop;
    cudaGetDeviceProperties(&prop, devID);
    gpuname = string(fixname(prop.name));

    switch (which) {
        case SURF: {
            if (surf(dir, true, false))
                return 1;
        } break;
        case HOG: {
            while (iters--)
                if (hog(dir, true, false))
                    return 1;
        } break;
        default: return 1;
    }

    return 0;
}

