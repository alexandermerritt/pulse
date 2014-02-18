/* file: main.cpp
 * author: Alexander Merritt merritt.alex@gatech.edu
 *
 * -- execute --
 * $ find /path/to/images -type f | ./stitcher
 *
 * -- kill --
 * $ killall -s SIGUSR1 stitcher
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* C++ system includes */
#include <iostream>
#include <iomanip>

/* C system includes */
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>
#include <errno.h>

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
#include "stitcher.hpp"
#include "io.hpp"
#include "types.hpp"
#include "memcached.hpp"

//===----------------------------------------------------------------------===//
// Definitions
//===----------------------------------------------------------------------===//

enum pano_state
{
    PANO_INVALID = 0,
    PANO_LOADED,
    PANO_MATCHED,
};

enum exit_reason
{
    EXIT_NORMAL = 0,
    // system errors
    EXIT_EINVAL,
    EXIT_ENOMEM,
    // stitching errors
    EXIT_UNMATCHED,
    EXIT_COMPOSITION,
    EXIT_INVALID_WORKSTATE,
    EXIT_IMAGE_LOADING,
    EXIT_IMAGE_WRITING
};

struct work_item
{
    enum pano_state state;
    int pano_num;
    int gpu_id;
    float confidence;
    cv::Mat pano;
    images_t images;
    matches_t matches;
    cameras_t cameras;
    features_t features;
};

struct thread
{
    pthread_t tid;
    int gpu;
    bool alive;
    enum exit_reason reason;
    int imgidx; // used for writing panos
};

struct config
{
    int help;
    int use_memcached;
};

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

// program configuration (based on cmd arguments)
static struct config config;

// gpu stuff
static int num_gpus;

// work stuf
static std::list< struct work_item * > work_queue;
static pthread_mutex_t queue_lock;

// thread stuff
static int num_threads;
static thread *threads;

// arguments
static const struct option options[] = {
    {"help", no_argument, (int*)&config.help, true},
    {"memcached", no_argument, (int*)&config.use_memcached, true},
    {NULL, no_argument, NULL, 0} // terminator
};

//===----------------------------------------------------------------------===//
// Private functions
//===----------------------------------------------------------------------===//

static inline void lock_queue(void)
{
    pthread_mutex_lock(&queue_lock);
}

static inline void unlock_queue(void)
{
    pthread_mutex_unlock(&queue_lock);
}

#if 0
static void printMatches(const matches_t &matches)
{
    int i = 0;
    std::cout << "---- matches start ----" << std::endl;
    for (auto &m : matches) {
        std::cout << std::setw(2) << i++
            << " (" << m.src_img_idx << "," << m.dst_img_idx << ")"
            << ": " << std::setw(10) << m.confidence
            << std::endl;
    }
    std::cout << "---- matches end ----" << std::endl;
}

static void printFeatures(features_t &features)
{
    int i = 0;
    std::cout << "---- features start ----" << std::endl;
    for (auto &f : features) {
        std::cout << std::setw(2) << i++
            << " img " << f.img_idx
            << " pts " << f.keypoints.size()
            << std::endl;
    }
    std::cout << "---- matches end ----" << std::endl;
}
#endif

static void gpu_thread_cleanup(void *arg)
{
    struct thread *self = (struct thread*)arg;

    std::stringstream err;
    err << "!! thread for GPU " << self->gpu << ": ";
    if (self->reason == EXIT_NORMAL) {
        // do nothing
    } else if (self->reason == EXIT_EINVAL) {
        err << "bad arguments";
    } else if (self->reason == EXIT_ENOMEM) {
        err << "no memory";
    } else if (self->reason == EXIT_COMPOSITION) {
        err << "could not compose panorama";
    } else if (self->reason == EXIT_INVALID_WORKSTATE) {
        err << "work item has invalid state";
    } else {
        err << "unknown reason";
    }
    err << std::endl;
    std::cerr << err.str();

    cv::gpu::resetDevice();

    self->alive = false;
}

// Searches for features on each image, matches all, then sorts into separate
// panoramas, inserting each as a new work unit into the global queue.  Called
// only by gpu_thread.  Any non-zero exit status causes the calling thread to
// exit.
static enum exit_reason sort_images(struct work_item *work)
{
    PStitcher ps = PStitcher::createDefault();
    matches_t matches;
    features_t features;
    indices_t indices;

    if (!work || work->state != PANO_LOADED)
        return EXIT_EINVAL;

    ps.findFeatures(work->images, features, true, 1);
    //ps.findFeatures(work->images, features, false, 24);
    while (work->images.size() > 1) {
        // use new b/c struct contains classes
        struct work_item *next = new struct work_item;
        if (!next)
            return EXIT_ENOMEM;

        ps.matchFeatures(features, matches, true, 1);
        //ps.matchFeatures(features, matches, false, 24);

        next->features = features;
        next->matches = matches;
        ps.findRelated(next->features, next->matches,
                indices, work->confidence);
        if (next->features.size() < 2) {
            if (work->confidence > 0.5f) {
                work->confidence -= 0.25f;
                continue;
            } else {
                fprintf(stderr, ">> dropping %lu unmatched images\n",
                        work->images.size());
                //return EXIT_UNMATCHED; // XXX hmm..
                break;
            }
        }

        std::sort(indices.begin(), indices.end());
        int offset = 0;
        for (auto _idx : indices) {
            int idx = _idx - offset++;
            next->images.push_back(work->images[idx]);
            work->images.erase(work->images.begin() + idx);
            features.erase(features.begin() + idx);
        }
        next->state = PANO_MATCHED;
        next->confidence = work->confidence;

        lock_queue();
        work_queue.push_back(next);
        unlock_queue();
    }
    return EXIT_NORMAL;
}

// Takes a set of images, features, matching info etc and composites the final
// panorama. Called only by gpu_thread.
static enum exit_reason make_pano(struct work_item *work)
{
    PStitcher ps = PStitcher::createDefault();
    int err = 0;

    if (!work || work->state != PANO_MATCHED)
        return EXIT_EINVAL;

    // this runs only on the CPU for now, single thread
    ps.estimateCameraParams(work->features, work->matches,
            work->cameras, work->confidence);

    err = ps.composePanorama(work->images, work->cameras,
            work->pano, true, 1);
    if (err)
        return EXIT_COMPOSITION;

    return EXIT_NORMAL;
}

static void * gpu_thread(void *arg)
{
    struct thread *self = (struct thread*)arg;
    struct work_item *work;
    PStitcher ps = PStitcher::createDefault();
    stringstream pano_name;
    int oldstate /* not used */;
    enum exit_reason reason;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
    pthread_cleanup_push(gpu_thread_cleanup, self);

    if (!self || self->gpu < 0) {
        self->reason = EXIT_EINVAL;
        pthread_exit(NULL);
    }

    self->reason = EXIT_NORMAL;
    self->alive  = true;

    cv::gpu::setDevice(self->gpu);

    while (true) {
        lock_queue();
        if (work_queue.size() == 0) {
            unlock_queue();
            sleep(1);
            continue;
        }
        work = work_queue.front();
        work_queue.pop_front();
        unlock_queue();

        // TODO set pano_num

        if (work->state == PANO_LOADED) {
            reason = sort_images(work);
            if (reason) {
                self->reason = reason;
                pthread_exit(NULL);
            }
            delete work;
            work = NULL;
        }
        else if (work->state == PANO_MATCHED) {
            reason = make_pano(work);
            if (reason) {
                self->reason = reason;
                pthread_exit(NULL);
            }
            stringstream s;
            s << "/tmp/img_" << self->gpu << "_" << self->imgidx++ << ".jpg";
            if (write_image(s.str().c_str(), work->pano)) {
                self->reason = EXIT_IMAGE_WRITING;
                pthread_exit(NULL);
            }
            // TODO reset GPU device
            delete work;
            work = NULL;
        }
        else {
            self->reason = EXIT_INVALID_WORKSTATE;
            pthread_exit(NULL);
        }
#if 0
        cv::gpu::resetDevice();
        pano_name << "sets/pano-" << work->pano_num << ".jpg";
        std::cout << ">> writing " << pano_name.str() << std::endl;
        if (!cv::imwrite(pano_name.str(), work->pano)) {
            std::cerr << "!! thread " << work->pano_num
                << ": error writing panorama" << std::endl;
        }
#endif

        cv::gpu::resetDevice();

    } // while

    pthread_cleanup_pop(1);
    pthread_exit(NULL);
}

static void sighandler(int sig)
{
    if (sig == SIGUSR1)
        for (int i = 0; i < num_threads; i++)
            pthread_cancel(threads[i].tid);
}

static int add_sigusr1(void)
{
    struct sigaction action;
    printf(">> kill me with 'killall -s SIGUSR1 stitcher' when idling\n");
    memset(&action, 0, sizeof(action));
    action.sa_handler = sighandler;
    sigemptyset(&action.sa_mask);
    if (0 > sigaction(SIGUSR1, &action, NULL))
        return -1;
    return 0;
}

static int spawn_threads(void)
{
    int err;

    if (threads)
        return -1;

    num_threads = num_gpus;
    threads = (struct thread*)calloc(num_threads, sizeof(*threads));
    if (!threads) {
        std::cerr << "!! no memory" << std::endl;
        return -1;
    }

    for (int t = 0; t < num_gpus; t++) {
        threads[t].gpu = t;
        threads[t].alive = false;
        err = pthread_create(&threads[t].tid, NULL, gpu_thread, &threads[t]);
        if (err) {
            std::cerr <<  "!! error spawning thread" << std::endl;
            return -1;
        }
    }
    return 0;
}

static void join_threads(void)
{
    int err;
    for (int t = 0; t < num_gpus; t++) {
        err = pthread_join(threads[t].tid, NULL);
        if (err) {
            std::cerr << "!! error joining thread" << std::endl;
        }
    }
}


static void prune_paths(paths_t &_paths, const string &ext)
{
    paths_t paths;
    size_t pos;

    for (string &s : _paths) {
        if (s.empty())
            continue;
        if (s[0] == '#')
            continue;
        pos = s.find_last_of('.');
        if (pos == string::npos)
            continue;
        if (!ext.empty() &&
                0 != strncasecmp(s.substr(pos).c_str(),
                    ext.c_str(), ext.length()))
            continue;
        paths.push_back(s);
    }

    _paths = paths;
}

static int add_images(images_t &imgs)
{
    struct work_item *work;
    
    work = new struct work_item;
    if (!work)
        return EXIT_ENOMEM;

    work->images = imgs;
    work->state = PANO_LOADED;
    work->confidence = 1.0f;

    lock_queue();
    work_queue.push_back(work);
    unlock_queue();

    return EXIT_NORMAL;
}

static void usage(char *argv[])
{
    fprintf(stderr, "\nUsage: %s [options]\n\n", *argv);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    --help\t\tDisplay this help\n");
    fprintf(stderr, "    --memcached\t\tConnect to memcached for data\n");
}

static int parse_args(int argc, char *argv[])
{
    int opt, idx, ret;
    const int done = -1; // man getopt_long

    while (done != (opt = getopt_long(argc, argv, "", options, &idx)))
        if (opt == '?')
            return -EINVAL;

    if (config.help)
        return -EINVAL;

    if (config.use_memcached)
        if ((ret = watch_memcached()))
            return ret;

    return 0;
}

//===----------------------------------------------------------------------===//
// Entry
//===----------------------------------------------------------------------===//

int main(int argc, char *argv[])
{
    paths_t paths;
    images_t imgs;
    int ret;

    ret = parse_args(argc, argv);
    if (ret < 0) {
        if (ret == -EINVAL)
            usage(argv);
        return -1;
    }

    num_gpus = cv::gpu::getCudaEnabledDeviceCount();
    if (num_gpus < 1) {
        std::cerr << "!! no GPGPUs or "
            << "OpenCV not compiled with CUDA" << std::endl;
        return -1;
    }
    num_gpus = 1;
    printf(">> %d GPGPUs for use\n", num_gpus);

    pthread_mutex_init(&queue_lock, NULL); // do before spawning threads

    if (spawn_threads())
        return -1;

    if (add_sigusr1())
        return -1;

    read_stdin(paths);
    prune_paths(paths, string(".jpg"));

    if (load_images(imgs, paths))
        return -1;

    if (add_images(imgs))
        return -1;

    join_threads();

    free(threads);
    for (struct work_item * w : work_queue)
        delete w;
    work_queue.clear();

    printf(">> done.\n");

    return 0;
}
