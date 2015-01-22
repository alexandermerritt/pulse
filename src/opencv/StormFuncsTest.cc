#include <iostream>
#include <deque>
#include "StormFuncs.h"

int main(void)
{
    StormFuncs funcs;

    std::string servers("--SERVER=10.0.0.1:11211 "
            "--SERVER=10.0.0.2:11211");
    if (funcs.connect(servers)) {
        std::cerr << "Error: connect" << std::endl;
        return -1;
    }

    std::string vertex = "100000772955143706751";
    std::deque<std::string> neighbors;
    if (funcs.neighbors(vertex, neighbors)) {
        std::cerr << "Error: neighbors" << std::endl;
        return -1;
    }

    vertex = "";
    std::deque<std::string> image_keys;
    for (std::string &v : neighbors) {
        if (funcs.imagesOf(v, image_keys)) {
            std::cerr << "Error: connect" << std::endl;
            return -1;
        }
        if (image_keys.size() > 1) {
            vertex = v;
            break;
        }
    }
    if (vertex.length() < 1) {
        return -1;
    }
    std::cout << "vertex " << vertex 
        << " has " << neighbors.size() << " neighbors"
        << std::endl;

    int found = 0;
    for (std::string &image_key : image_keys) {
        std::cout << "Exec feature on " << image_key << std::endl;
        if (funcs.feature(image_key, found)) {
            std::cerr << "Error: feature on "
                << image_key << std::endl;
            return -1;
        }
        std::cout << "    found " << found
            << " features" << std::endl;
    }

    std::deque<cv::detail::MatchesInfo> matchinfo;
    if (funcs.match(image_keys, matchinfo))
        return -1;
    std::cout << "There are " << matchinfo.size()
        << " match infos" << std::endl;
    for (auto &m : matchinfo) {
        std::cout << m.src_img_idx << " matches "
            << m.dst_img_idx << " with " 
            << m.matches.size() << " at conf "
            << m.confidence << std::endl;
    }

    return 0;
}
