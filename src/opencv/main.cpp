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
    int exit_code;
    int imgidx; // used for writing panos
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
    const char *input; /* input file with list of images */
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
    /* don't stich images, just print which match and which don't */
    {"match-only", no_argument, (int*)&config.match_only, true},
    {"gpu-all", no_argument, (int*)&config.gpu_all, true},
    {"cpu-all", no_argument, (int*)&config.cpu_all, true},
    {"input", required_argument, NULL, 'i'},
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
    if (self->gpu != 0) {
        err << "!! thread for GPU " << self->gpu << ": ";
        err << strerror(self->exit_code) << std::endl;
        std::cerr << err.str();
    }

    cv::gpu::resetDevice();

    self->alive = false;
}

static void __dump_features(const images_t &images, const features_t &features)
{
    static size_t imgnum = 0UL;
    std::stringstream ss;

    for (auto &feat : features) {
        string path = get<1>(images[imgnum]);
        path = path.substr(path.find_last_of("/") + 1);
        ss << "features____" << path << "____.yaml";
        cv::FileStorage fs(ss.str(), cv::FileStorage::WRITE);
        cv::write(fs, std::string(), feat.keypoints);
        imgnum++;
        ss.str(std::string());
    }
}

static void __print_features(const features_t &features)
{
    size_t imgnum = 0UL, kpnum = 0UL;
    std::cout << "    features:" << std::endl;
    std::cout << "image keypoint coord.x coord.y size angle rsp oct id" << std::endl;
    for (auto &feat : features) {
        kpnum = 0UL;
        for (auto &kp : feat.keypoints) {
            std::cout << imgnum
                << " " << kpnum
                << " " << kp.pt.x << " " << kp.pt.y
                << " " << kp.size
                << " " << kp.angle
                << " " << kp.response
                << " " << kp.octave
                << " " << kp.class_id
                << std::endl;
            kpnum++;
        }
        imgnum++;
    }
}

static inline void dump_features(const images_t &images, const features_t &features)
{
    if (getenv("DUMP_FEATURES"))
        __dump_features(images, features);
}

static inline void print_features(const features_t &features)
{
    if (getenv("PRINT_FEATURES"))
        __print_features(features);
}

static int do_sort_images(struct work_item *work, // input
        std::list< struct work_item * > &sets, images_t &unmatched, // output
        bool use_gpu, int num_cpus) // tuning; num_cpus not used if use_gpu=1
{
    PStitcher ps = PStitcher::createDefault();
    matches_t matches;
    features_t features;
    indices_t indices;
    bool have_matching;
    int offset;
    float confidence;
    struct work_item *next;

    if (!work)
        return -EINVAL;
    if (work->state != PANO_LOADED)
       return -EINVAL;
    if (work->images.size() < 2)
        return -EINVAL;

    ps.findFeatures(work->images, features, use_gpu, (use_gpu ? 1 : num_cpus));

    print_features(features);
    dump_features(work->images, features);

    while (work->images.size() > 1) {

        // use new b/c struct contains classes
        next = new struct work_item;
        if (!next)
            return -ENOMEM;

        ps.matchFeatures(features, matches, use_gpu, (use_gpu ? 1 : num_cpus));

        // match images, reducing confidence until we do, else quit
        have_matching = false;
        confidence = work->confidence;
        do {
            next->features = features;
            next->matches = matches;
            ps.findRelated(next->features, next->matches,
                    indices, confidence);
            if (next->features.size() > 1) {
                have_matching = true;
                break;
            }
            confidence -= 0.1f;
            std::cout << "    reducing confidence to "
                << confidence << std::endl;
        } while (confidence > 0.2f);

        if (!have_matching) {
            for (auto &img : work->images)
                unmatched.push_back(img);
            work->images.clear();
            delete next;
            break;
        }

        // some images did match, create new work item
        std::sort(indices.begin(), indices.end());
        offset = 0;
        for (auto _idx : indices) {
            int idx = _idx - offset++;
            next->images.push_back(work->images[idx]);
            work->images.erase(work->images.begin() + idx);
            features.erase(features.begin() + idx);
        }
        next->state = PANO_MATCHED;
        next->confidence = work->confidence;
        sets.push_back(next);
    }

    return 0;
}

static int sort_images(struct work_item *work)
{
    std::list< struct work_item * > sets;
    images_t unmatched;
    static int num = 0;
    int err;

    if (!work)
        return -EINVAL;

    err = do_sort_images(work, sets, unmatched,
            config.gpu_all, config.num_cpus);
    if (err) {
        std::cerr << "!! error sorting: " << strerror(err) << std::endl;
        return -ENOTRECOVERABLE;
    }

    if (!unmatched.empty())
        for (auto &img : unmatched)
            std::cout << "nomatch " << get<1>(img) << std::endl;

    for (auto item : sets) {
        for (auto &img : item->images)
            std::cout << "match-" << num << " " << get<1>(img) << std::endl;
        num++;
    }

    if (!config.match_only) {
        for (auto item : sets) {
            lock_queue();
            work_queue.push_back(item);
            unlock_queue();
        }
    }

    work->images.clear();

    return 0;
}

// Takes a set of images, features, matching info etc and composites the final
// panorama. Called only by gpu_thread.
static int make_pano(struct work_item *work)
{
    PStitcher ps = PStitcher::createDefault();
    int err = 0;

    if (!work || work->state != PANO_MATCHED)
        return -EINVAL;

    // Bundle Adjustment - runs only on the CPU for now, single thread
    ps.estimateCameraParams(work->features, work->matches,
            work->cameras, work->confidence);

    err = ps.composePanorama(work->images, work->cameras,
            work->pano, config.gpu_all, config.num_cpus);
    if (err)
        return -ENOTRECOVERABLE;

    return 0;
}

static void * gpu_thread(void *arg)
{
    struct thread *self = (struct thread*)arg;
    struct work_item *work;
    PStitcher ps = PStitcher::createDefault();
    stringstream pano_name;
    int oldstate /* not used */;

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
    pthread_cleanup_push(gpu_thread_cleanup, self);

    if (!self || self->gpu < 0) {
        self->exit_code = -EINVAL;
        pthread_exit(NULL);
    }

    self->exit_code = 0;
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
            if ((self->exit_code = sort_images(work)))
                pthread_exit(NULL);
            delete work;
            work = NULL;
        }
        else if (work->state == PANO_MATCHED) {
            if ((self->exit_code = make_pano(work)))
                pthread_exit(NULL);
            if (!work->pano.data) {
                self->exit_code = -ENOTRECOVERABLE;
                pthread_exit(NULL);
            }
            stringstream s;
            s << "/tmp/pano_thread-" << self->gpu << "_img-" << self->imgidx++ << ".jpg";
            std::cout << "    writing pano to " << s.str() << " ...";
            if (write_image(s.str().c_str(), work->pano)) {
                std::cout << std::endl;
                self->exit_code = -ENOTRECOVERABLE;
                pthread_exit(NULL);
            }
            std::cout << " done" << std::endl;
            // TODO reset GPU device
            delete work;
            work = NULL;
        }
        else {
            self->exit_code = -ENOTRECOVERABLE;
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

static const char *signames[] = {
    [SIGUSR1] = "SIGUSR1",
    [SIGINT]  = "SIGINT",
};

static void sighandler(int sig)
{
    if (sig == SIGUSR1 || sig == SIGINT) {
        printf(">> signal %s received\n", signames[sig]);
        for (int i = 0; i < num_threads; i++) {
            pthread_cancel(threads[i].tid);
        }
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

static int add_images(images_t &imgs)
{
    struct work_item *work;
    
    work = new struct work_item;
    if (!work)
        return -ENOMEM;

    work->images = imgs;
    work->state = PANO_LOADED;
    work->confidence = 2.;

    lock_queue();
    work_queue.push_back(work);
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

    join_threads();

    free(threads);
    for (struct work_item * w : work_queue)
        delete w;
    work_queue.clear();

    printf(">> done.\n");

    return 0;
}
