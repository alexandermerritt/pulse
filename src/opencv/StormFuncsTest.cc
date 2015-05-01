// test the opencv stacks
// XXX don't spawn more threads than #gpus, AND, set the gpu compute
// modes to THREAD_EXCLUSIVE so each thread works in isolation
#include <iostream>
#include <unordered_set>
#include <thread>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <exception>
#include "StormFuncs.h"

thread_local StormFuncs *funcs;

static std::string servers_g;
static std::mutex servers_lock;

static std::mutex stdout_lock;

class Lock {
    std::mutex &l;
    public:
    Lock(std::mutex &_l) : l(_l) { l.lock(); }
    ~Lock() { l.unlock(); }
};

static inline void
set_servers(std::string &servers)
{
    Lock lock(servers_lock);
    servers_g = servers;
}

static inline void
construct(void) {
    if (funcs)
        return;
    funcs = new StormFuncs();
    if (!funcs)
        throw std::runtime_error("no memory");
    if (servers_g.size() < 1)
        throw std::runtime_error("servers_g empty");
    if (funcs->connect(servers_g))
        throw std::runtime_error("funcs connect(" + servers_g + ")");
}

void threadfunc(int id)
{
    construct();

    size_t visits = (1 << (22 - id));
    size_t c_memc(0), c_feat(0);

    std::unordered_set<std::string> seen;
    std::unordered_set<std::string> next;
    // start with some key that is known XXX
    next.insert(std::string("100000772955143706751"));

    std::deque<std::string> neighbors;
    while (next.size() > 0) {
        neighbors.clear();
        auto iter = next.begin();
        std::string v(*iter);
        next.erase(iter);
        try {
            funcs->neighbors(v, neighbors);
        } catch (memc_notfound &e) {
            c_memc++;
            continue;
        }
        for (std::string &item : neighbors) {
            auto r = seen.insert(item);
            if (r.second)
                next.insert(item);
        }
        if (--visits == 0)
            break;
    }

    std::unordered_set<std::string> images;

    std::deque<std::string> images_;
    for (std::string v : seen) {
        try {
            funcs->imagesOf(v, images_);
        } catch (memc_notfound &e) {
            c_memc++;
            continue;
        }
        for (std::string &img : images_)
            images.insert(img);
    }

    int found = 0;
    for (std::string image_key : images) {
        try {
            funcs->feature(image_key, found);
        } catch (ocv_vomit &e) {
            c_feat++;
            continue;
        }
    }

    Lock lock(stdout_lock);
    std::cout << "memc misses " << c_memc
        << " feat failures " << c_feat
        << " (out of " << images.size() << ")" << std::endl;
}

int main(void)
{
    std::string servers(
            "--SERVER=10.0.0.1:11211"
            " --SERVER=10.0.0.2:11211"
            " --SERVER=10.0.0.3:11211"
            " --SERVER=10.0.0.4:11211"
            " --SERVER=10.0.0.5:11211"
            " --SERVER=10.0.0.6:11211"
            " --SERVER=10.0.0.7:11211"
            );
    set_servers(servers);

    std::list<std::thread> threads(0);
    for (int i = 0; i < 3; i++)
        threads.push_back(std::thread(threadfunc, i));

    for (auto &t : threads)
        t.join();

#if 0
    std::deque<cv::detail::MatchesInfo> matchinfo;
    if (funcs->match(image_keys, matchinfo))
        return -1;
    std::cout << "There are " << matchinfo.size()
        << " match infos" << std::endl;
    for (auto &m : matchinfo) {
        if (m.confidence <= 0)
            continue;
        std::cout << m.src_img_idx << " matches "
            << m.dst_img_idx << " with " 
            << m.matches.size() << " at conf "
            << m.confidence << std::endl;
    }


    // pick ones with least similarity and create montage
    // pick images above some threshold size
    std::string montage_key;
    size_t num = image_keys.size();
    if (num > 16)
        num = static_cast<size_t>(std::log1p(image_keys.size())) << 2;
    auto comp = [](const cv::detail::MatchesInfo &a,
            const cv::detail::MatchesInfo &b) {
        return !!(a.confidence < b.confidence);
    };
    std::sort(matchinfo.begin(), matchinfo.end(), comp);
    while (matchinfo.size() > num)
        matchinfo.erase(matchinfo.end() - 1);
    std::cout << "montage with " << num << " images" << std::endl;
    if (funcs->montage(image_keys, montage_key)) {
        std::cout << "Error with montage" << std::endl;
    }

    // XXX add montage to original user

#endif
    return 0;
}
