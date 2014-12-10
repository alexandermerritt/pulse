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

size_t featureSize(const cv::detail::ImageFeatures &f)
{
    size_t bytes = 0UL;
    bytes += sizeof(cv::detail::ImageFeatures);
    bytes += f.descriptors.total() * f.descriptors.elemSize();
    // estimate - lower bound
    bytes += f.keypoints.capacity() * sizeof(cv::KeyPoint);
    return bytes;
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
        fprintf(stderr,PREFIX "setting hog detector\n");
        hog.setSVMDetector(gpu::HOGDescriptor::getDefaultPeopleDetector());

        fprintf(stderr,PREFIX "uploading img\n");
        gpu::GpuMat gm;
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
        ss << gpuname;
        cout << ss.str() << endl;

        fprintf(stderr,PREFIX "loop end\n");
    }

    return 0;
}

int fast(dir_t &dir)
{
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    int thresh = 1;
    double kpRatio = 0.05;

    cout << "imgid width height depth "
        "thresh kpRatio " // fast params
        "ongpu scaled "
        "feats alg_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat img = imread(path);
        if (!img.data)
            return -1;

        RNG rng(0);

        vector<KeyPoint> kp;
        unsigned long tim = 0UL;
        timer_init(CLOCK_REALTIME, &t);
        try {
            timer_start(&t);
            Mat gray, mask;
            cvtColor(img, gray, CV_BGR2GRAY);
            rng.fill(mask, RNG::UNIFORM, 0, 1);
            gpu::GpuMat image(gray), maskg(mask);
            gpu::FAST_GPU fast(thresh, true, kpRatio);
            fast(image, maskg, kp);
            tim = timer_end(&t, MICROSECONDS);
        } catch (cv::Exception &e) {
            cerr << "Error: " << path << " caused opencv to crash" << endl;
            return -1;
        }

        stringstream ss;

        ss << basename(path) << " ";
        ss << img.cols << " " << img.rows << " ";
        ss << img.elemSize() << " ";
        ss << thresh << " ";
        ss << kpRatio << " ";
        ss << "1 "; // on gpu
        ss << "0 "; // not scaled
        ss << kp.size() << " ";
        ss << tim << " ";
        ss << gpuname;
        cout << ss.str() << endl;
    }

    return 0;
}

int surf(dir_t &dir, bool ongpu = true, bool doscale = false)
{
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    cout << "imgid width height depth "
        "hess nocts nlay noctdesc nlayerdesc " // SURF params
        "ongpu scaled "
        "feats feats_sz surf_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat scaled, img = imread(path);
        if (!img.data)
            return -1;
        scaled = img;

        fprintf(stderr,PREFIX "making finder\n");
        unique_ptr<detail::FeaturesFinder> finder;
        if (ongpu)
            finder.reset(new detail::SurfFeaturesFinderGpu(expand(params)));
        else
            finder.reset(new detail::SurfFeaturesFinder(expand(params)));
        if (!finder)
            return -1;

        fprintf(stderr,PREFIX "loop start\n");

#if 0
        if (doscale) {
            double work_scale = min(1.0, sqrt(1e6 / img.size().area()));
            if (work_scale < 0.95) {
                resize(img, scaled, Size(), work_scale, work_scale);
            }
        }
#endif

        unsigned long surft = 0UL;
        timer_init(CLOCK_REALTIME, &t);
        detail::ImageFeatures feature;
        try {
            timer_start(&t);
            finder->operator()(scaled, feature);
            surft = timer_end(&t, MICROSECONDS);
        } catch (cv::Exception &e) {
            cerr << "Error: " << path << " cause opencv to crash" << endl;
            return -1;
        }
        //double mean, median;
        //summary(times, mean, median);

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
        ss << feature.keypoints.size() << " ";
        ss << featureSize(feature) << " ";
        ss << surft << " ";
        ss << gpuname;
        cout << ss.str() << endl;
    }
    fprintf(stderr,PREFIX "loop end\n");
    return 0;
}

int orb(dir_t &dir, bool doscale = false)
{
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    cout << "imgid width height depth "
        "" // orb params
        "ongpu scaled " // config
        "feats alg_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat img2 = imread(path);
        if (!img2.data)
            return -1;
        Mat img;
        cvtColor(img2, img, CV_BGR2GRAY); // to CV_8UC3

        vector<KeyPoint> kp;
        unsigned long tim = 0UL;
        timer_init(CLOCK_REALTIME, &t);
        try {
            timer_start(&t);
            gpu::GpuMat image(img);
            gpu::GpuMat mask(Mat::zeros(img.rows, img.cols, CV_8UC1));
            gpu::ORB_GPU orb(   1000,   // nFeatures
                                1.2f,   // scaleFactor
                                8,      // nLevels
                                31,     // edgeThreshold
                                0,      // firstLevel
                                2,      // WTA_K
                                0,      // scoreType
                                31      // patchSize
                            );
            orb(image, mask, kp);
            tim = timer_end(&t, MICROSECONDS);
        } catch (cv::Exception &e) {
            cerr << "Error: " << path << " caused opencv to crash" << endl;
            return -1;
        }

        stringstream ss;

        ss << basename(path) << " ";
        ss << img.cols << " " << img.rows << " ";
        ss << img.elemSize() << " ";
        ss << "1 "; // on gpu
        ss << (doscale ? "1" : "0") << " ";
        ss << kp.size() << " ";
        ss << tim << " ";
        ss << gpuname;
        cout << ss.str() << endl;
    }
    return 0;
}

int blur(dir_t &dir, bool doscale = false)
{
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    cout << "imgid width height depth "
        "" // blur params
        "ongpu scaled " // config
        "alg_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat img = imread(path);
        if (!img.data)
            return -1;

        unsigned long tim = 0UL;
        timer_init(CLOCK_REALTIME, &t);
        try {
            timer_start(&t);
            gpu::GpuMat src(img), dest;
            // ksize must be < 32 and an odd value
            gpu::GaussianBlur(src, dest, Size(9,9), 9);
            dest.upload(img);
            tim = timer_end(&t, MICROSECONDS);
        } catch (cv::Exception &e) {
            cerr << "Error: " << path << " caused opencv to crash" << endl;
            return -1;
        }

        stringstream ss;

        ss << basename(path) << " ";
        ss << img.cols << " " << img.rows << " ";
        ss << img.elemSize() << " ";
        ss << "1 "; // on gpu
        ss << (doscale ? "1" : "0") << " ";
        ss << tim << " ";
        ss << gpuname;
        cout << ss.str() << endl;
    }
    return 0;
}

int convolve(dir_t &dir, bool doscale = false)
{
    paths_t imagepaths;
    struct timer t;

    if (listdir(dir, imagepaths))
        return 1;

    cout << "imgid width height depth "
        "" // no convolve params
        "ongpu scaled " // config
        "templW templH resW resH" // other img dims
        "alg_time gpuname" << endl;

    for (string &path : imagepaths) {
        Mat img = imread(path);
        if (!img.data)
            return -1;
        (void)img.channels();

        unsigned long tim = 0UL;
        timer_init(CLOCK_REALTIME, &t);
        try {
            timer_start(&t);
            Mat chn4(img.rows, img.cols, CV_32F);
            cout << img.type() << " cv_32f: " << CV_32F << endl;
            //assert(chn4.depth() == img.depth());
            //int from_to[] = {0,0,1,1,2,2,3,3};
            //cv::mixChannels(&img, 1, &chn4, 1, from_to, 4);
            //if (chn4.type() != CV_32F)
                //return -1;
            cvtColor(img, chn4, CV_BGR2BGRA);
            cout << chn4.type() << " cv_32f: " << CV_32F << endl;

            gpu::GpuMat image(chn4), templ, result;
            gpu::resize(image, templ, Size(), 0.25, 0.25);
            // XXX i dunno what is wrong with this retarded function
            gpu::convolve(image, templ, result);
            tim = timer_end(&t, MICROSECONDS);
        } catch (cv::Exception &e) {
            cerr << "Error: " << path << " caused opencv to crash" << endl;
            return -1;
        }
    }
    return 0;
}

enum which
{
    INVALID, SURF, HOG, CONVOLVE, BLUR, ORBALG, FASTALG,
};

// ./feature alg n dir/
// you can omit all the extra info printed by sending stderr to /dev/null
// when preloading lib.so, set iter to 1
int main(int argc, char *argv[])
{
    if (argc != 5) {
        cerr << "Usage: ./feature alg iters gpuid dir/"
            << endl;
        return 1;
    }

    enum which which;

    string alg(argv[1]);
    int iters = atoi(argv[2]);
    dir_t dir(argv[4]);

    if (alg == "surf") {
        which = SURF;
    } else if (alg == "hog") {
        which = HOG;
    } else if (alg == "convolve") {
        which = CONVOLVE;
    } else if (alg == "blur") {
        which = BLUR;
    } else if (alg == "orb") {
        which = ORBALG;
    } else if (alg == "fast") {
        which = FASTALG;
    } else {
        return 1;
    }

    int devID = atoi(argv[3]);
    if (devID < 0 || devID > 3)
        return 1;
    cv::gpu::setDevice(devID);
    cudaDeviceProp prop;
    cudaGetDeviceProperties(&prop, devID);
    gpuname = string(fixname(prop.name));

    switch (which) {
        case SURF: {
            while (iters--)
                if (surf(dir, true, false))
                    return 1;
        } break;
        case HOG: {
            while (iters--)
                if (hog(dir, true, false))
                    return 1;
        } break;
        case CONVOLVE: {
            while (iters--)
                if (convolve(dir, false))
                    return 1;
        } break;
        case BLUR: {
            while (iters--)
                if (blur(dir, false))
                    return 1;
        } break;
        case ORBALG: {
            while (iters--)
                if (orb(dir, false))
                    return 1;
        } break;
        case FASTALG: {
            while (iters--)
                if (fast(dir))
                    return 1;
        } break;
        default: return 1;
    }

    return 0;
}

