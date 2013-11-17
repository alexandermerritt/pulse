/* file: main.cpp
 * author: Alexander Merritt merritt.alex@gatech.edu
 */

#include <iostream>
#include <iomanip>

#include <unistd.h> // sleep

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

#include "stitcher.hpp"
#include "io.hpp"
#include "types.hpp"

//===----------------------------------------------------------------------===//
// Types, defines
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
    EXIT_INVALID_WORKSTATE
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
};

//===----------------------------------------------------------------------===//
// State
//===----------------------------------------------------------------------===//

// gpu stuff
static int num_gpus;

// work stuf
static std::list< struct work_item * > work_queue;
static pthread_mutex_t queue_lock;

// thread stuff
static int num_threads;
static thread *threads;

//===----------------------------------------------------------------------===//
// Utility functions
//===----------------------------------------------------------------------===//

static inline void lock_queue(void)
{
    pthread_mutex_lock(&queue_lock);
}

static inline void unlock_queue(void)
{
    pthread_mutex_unlock(&queue_lock);
}

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
    } else if (self->reason == EXIT_UNMATCHED) {
        err << "could not relate images into a panorama";
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

static void * gpu_thread(void *arg)
{
    struct thread *self = (struct thread*)arg;
    struct work_item *work;
    PStitcher ps = PStitcher::createDefault(false);
    stringstream pano_name;
    int err = 0, oldstate /* not used */;

    self->reason = EXIT_NORMAL;
    self->alive  = true;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
    pthread_cleanup_push(gpu_thread_cleanup, self);

    if (!self || self->gpu < 0) {
        self->reason = EXIT_EINVAL;
        pthread_exit(NULL);
    }

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
            matches_t matches;
            features_t features;
            indices_t indices;

            ps.findFeatures(work->images, features, true, 1);
            while (work->images.size() > 1) {
                struct work_item *next;

                // use new b/c struct contains classes
                next = new struct work_item;
                if (!next) {
                    self->reason = EXIT_ENOMEM;
                    pthread_exit(NULL);
                }

                ps.matchFeatures(features, matches, true, 1);

                next->features = features;
                next->matches = matches;
                ps.findRelated(next->features, next->matches,
                        indices, work->confidence);
                if (next->features.size() < 2) {
                    if (work->confidence > 0.5f) {
                        work->confidence -= 0.25f;
                        continue;
                    } else {
                        self->reason = EXIT_UNMATCHED;
                        pthread_exit(NULL);
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
            delete work;
            work = NULL;
        } else if (work->state == PANO_MATCHED) {
            ps.estimateCameraParams(work->features, work->matches,
                    work->cameras, work->confidence);
            err = ps.composePanorama(work->images, work->cameras,
                    work->pano, true, 1);
            if (err) {
                self->reason = EXIT_COMPOSITION;
                pthread_exit(NULL);
            }
            // TODO write pano to disk
            // TODO reset GPU device
            delete work;
            work = NULL;
        } else {
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

//===----------------------------------------------------------------------===//
// Entry
//===----------------------------------------------------------------------===//

int main(void)
{
    std::string dirlist("dirlist");
    struct work_item *work;
    int err;

    num_gpus = cv::gpu::getCudaEnabledDeviceCount();

    if (num_gpus < 1) {
        std::cerr << "!! no GPGPUs or "
            << "OpenCV not compiled with CUDA" << std::endl;
        return -1;
    }

    pthread_mutex_init(&queue_lock, NULL); // do before spawning threads

    // Spawn the threads
    threads = (struct thread*)calloc(num_gpus, sizeof(*threads));
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

    // Initialize some work
    work = new struct work_item;
    if (!work) {
        std::cerr << "!! error: no memory" << std::endl;
        return -1;
    }

    err = load_images(work->images, dirlist);
    if (err) {
        std::cerr << "!! error loading images" << std::endl;
        return -1;
    }

    work->state = PANO_LOADED;
    work->confidence = 1.0f;

    lock_queue();
    work_queue.push_back(work);
    unlock_queue();

    // Wait
    for (int t = 0; t < num_gpus; t++) {
        err = pthread_join(threads[t].tid, NULL);
        if (err) {
            std::cerr << "!! error joining thread" << std::endl;
        }
    }

    free(threads);
    for (struct work_item * w : work_queue)
        delete w;
    work_queue.clear();

    return 0;
}
