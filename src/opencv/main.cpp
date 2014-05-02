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
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

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

struct work_item
{
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
    bool alive;
    int exit_code;
};

// global application configuration
struct config
{
    int help;
    bool use_memcached;
    bool match_only;
    bool gpu_all;
    bool cpu_all;
    long num_cpus;
    bool verbose;
    const char *input; /* input file with list of images */
};

//===----------------------------------------------------------------------===//
// Global state
//===----------------------------------------------------------------------===//

// program configuration (based on cmd arguments)
static struct config config;

// work stuf
static std::list< struct work_item * > work_queue;
static pthread_mutex_t queue_lock;

static struct thread main_state;

// arguments
static const struct option options[] = {
    {"help", no_argument, (int*)&config.help, true},
    {"memcached", no_argument, (int*)&config.use_memcached, true},
    /* don't stich images, just print which match and which don't */
    {"match-only", no_argument, (int*)&config.match_only, true},
    {"gpu-all", no_argument, (int*)&config.gpu_all, true},
    {"cpu-all", no_argument, (int*)&config.cpu_all, true},
    {"input", required_argument, NULL, 'i'},
    {"verbose", no_argument, (int*)&config.verbose, true},
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

static void gpu_thread_cleanup(void *arg)
{
    struct thread *self = (struct thread*)arg;
    self->alive = false;
    std::cerr << "!! main thread: "
        << strerror(self->exit_code) << std::endl;
}

static int do_sort_images(struct work_item *item, // input
        images_t &unmatched, // output
        bool use_gpu, int num_cpus) // tuning; num_cpus not used if use_gpu=1
{
    PStitcher ps = PStitcher::createDefault();
    matches_t matches, mtmp;
    features_t features, ftmp;
    indices_t indices;
    bool have_matching;
    int offset;
    float confidence;

    if (!item)
        return -EINVAL;

    // We'll return unmatched images to the caller to decide further.
    ps.findFeatures(item->images, features, use_gpu, (use_gpu ? 1 : num_cpus));
    ps.matchFeatures(features, matches, use_gpu, (use_gpu ? 1 : num_cpus));

    // Match images, reducing confidence until we do, else quit
    have_matching = false;
    confidence = item->confidence;
    do {
        // findRelated modifies its input args, so we make copy
        ftmp = features; mtmp = matches;
        ps.findRelated(ftmp, mtmp, indices, confidence);
        if (ftmp.size() > 1) {
            have_matching = true;
            break;
        }
        confidence -= 0.1f;
    } while (confidence > 0.2f);

    // None of the images match with any confidence
    if (!have_matching) {
        std::cout << "    no matching images in set" << std::endl;
        unmatched = item->images;
        item->images.clear();
        return 0;
    }

    // Some did match, so filter out ones which didn't
    std::cout << "    " << indices.size() << " images match" << std::endl;
    std::sort(indices.begin(), indices.end());
    offset = 0;
    images_t newset(0);
    for (int _idx : indices) {
        int idx = _idx - offset++;
        newset.push_back(item->images[idx]);
        item->images.erase(item->images.begin() + idx);
        //features.erase(features.begin() + idx);
    }

    item->features = ftmp;
    item->matches  = mtmp;
    item->images   = newset;

    return 0;
}

static int sort_images(struct work_item *item)
{
    images_t unmatched; // ignored for now
    int err;

    if (!item)
        return -EINVAL;

    err = do_sort_images(item, unmatched,
            config.gpu_all, config.num_cpus);
    if (err)
        return -ENOTRECOVERABLE;

    return 0;
}

// Takes a set of images, features, matching info etc and composites the final
// panorama. Called only by gpu_thread.
static int make_pano(struct work_item *item)
{
    PStitcher ps = PStitcher::createDefault();
    int err = 0;

    if (!item)
        return -EINVAL;

    // Bundle Adjustment - runs only on the CPU for now, single thread
    ps.estimateCameraParams(item->features, item->matches,
            item->cameras, item->confidence);

    err = ps.composePanorama(item->images, item->cameras,
            item->pano, config.gpu_all, config.num_cpus);
    if (err)
        return -ENOTRECOVERABLE;

    return 0;
}

static void* main_thread(void *arg)
{
    struct thread *self = (struct thread*)arg;
    int olddata;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &olddata);
    pthread_cleanup_push(gpu_thread_cleanup, self);

    self->alive  = true;

    struct work_item *i = NULL;
    while (true) {
        if (work_queue.size() == 0) // TODO replace with cond var
            { sleep(1); continue; }

        lock_queue();
        if (work_queue.size() == 0)
            { unlock_queue(); continue; }
        i = work_queue.front();
        work_queue.pop_front();
        unlock_queue();

        if ((self->exit_code = sort_images(i)))
            pthread_exit(NULL);

        if (i->images.size() < 2) // no matches...
            { delete i; continue; }

        if ((self->exit_code = make_pano(i)))
            pthread_exit(NULL);

        if (!(i->pano.data)) {
            std::cerr << "!! pano object has no data" << std::endl;
            continue;
        }

    }

    pthread_cleanup_pop(1);
    pthread_exit(NULL);
}

static const char *signames[] = {
    [SIGUSR1] = "SIGUSR1",
    [SIGINT]  = "SIGINT",
};

static void sighandler(int sig)
{
    if (sig == SIGUSR1 || sig == SIGINT) {
        printf(">> signal %s received\n", signames[sig]);
        pthread_cancel(main_state.tid);
    }
}

// program can be aborted with SIGINT or SIGUSR1
static int add_signals(void)
{
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = sighandler;
    sigemptyset(&action.sa_mask);
    if (0 > sigaction(SIGINT, &action, NULL))
        return -1;
    if (0 > sigaction(SIGUSR1, &action, NULL))
        return -1;
    return 0;
}

static int spawn(void)
{
    int err;

    if (main_state.alive)
        return -1;

    main_state.alive = false;
    err = pthread_create(&main_state.tid, NULL, main_thread, &main_state);
    if (err) {
        std::cerr <<  "!! error spawning thread" << std::endl;
        return -1;
    }
    return 0;
}

static int join(void)
{
    return (0 == pthread_join(main_state.tid, NULL));
}

static int add_images(images_t &imgs)
{
    struct work_item *item;
    
    item = new struct work_item;
    if (!item)
        return -ENOMEM;

    item->images = imgs;
    item->confidence = 2.;

    lock_queue();
    work_queue.push_back(item);
    unlock_queue();

    return 0;
}

static void usage(char *argv[])
{
    fprintf(stderr, "\nUsage: %s [options]\n\n", *argv);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    --help\t\tDisplay this help\n");
    fprintf(stderr, "    --memcached\t\tConnect to memcached for data\n");
    fprintf(stderr, "    --match-only\tStop after matching images. Print paths.\n");
    fprintf(stderr, "    --cpu-all\t\tUse CPU for all operations\n");
    fprintf(stderr, "    --gpu-all\t\tUse GPU for all operations, where implemented\n");
    fprintf(stderr, "    --input  \t\tSpecify file with images to parse, instead of stdin\n");
    fprintf(stderr, "    --verbose\t\tDump text in gobs\n");
}

static int parse_args(int argc, char *argv[])
{
    int opt, idx, ret;
    const int done = -1; // man getopt_long

    while (done != (opt = getopt_long(argc, argv, "i", options, &idx))) {
        if (opt == 'i')
            config.input = strdup(optarg);
        else if (opt == '?')
            return -EINVAL;
    }

    /* use input file in place of stdin */
    if (config.input) {
        int fd = open(config.input, O_RDONLY);
        if (fd < 0) {
            perror("open input file");
            return -1;
        }
        dup2(fd, 0);
        close(fd);
    }

    if (config.help)
        return -EINVAL;

    if (!config.verbose)
        { close(1); close(2); }

    if (config.gpu_all && config.cpu_all)
        return -EINVAL;

    if (config.cpu_all)
        config.num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    else 
        config.num_cpus = 1;

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

    pthread_mutex_init(&queue_lock, NULL); // do before spawning threads

    if (spawn())
        return -1;

    if (add_signals())
        return -1;

    std::vector< std::string > types;
    types.push_back(".jpg");

    read_stdin(paths);
    prune_paths(paths, types);

    if (load_images(imgs, paths))
        return -1;

    if (add_images(imgs))
        return -1;

    if(join())
        return -1;

    for (struct work_item * w : work_queue)
        delete w;
    work_queue.clear();

    return 0;
}
