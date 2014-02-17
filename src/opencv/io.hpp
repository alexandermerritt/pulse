/* file: io.hpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

#ifndef _IO_HPP_INCLUDED_
#define _IO_HPP_INCLUDED_

#include "types.hpp"

void read_stdin(paths_t &paths);

/*
 * dirlist  path to file containing list of directories to scan for files
 * mats     container into which to read files found on disk
 */
int load_images(images_t &imgs, const paths_t &_paths);

int write_features(std::string &dirpath,
        std::vector< cv::Mat > &imgs,
        std::vector< cv::detail::ImageFeatures > &features);

static inline int write_features(const char *dirpath,
        std::vector< cv::Mat > &imgs,
        std::vector< cv::detail::ImageFeatures > &features)
{
    std::string s(dirpath);
    return write_features(s, imgs, features);
}

int write_image(std::string &filepath, const cv::Mat &img);

static inline int write_image(const char *filepath, const cv::Mat &img)
{
    std::string s(filepath);
    return write_image(s, img);
}

int write_images(std::string &dirpath,
        const std::vector< cv::Mat > &imgs,
        std::string prefix = std::string(""));

static inline int write_images(const char *dirpath,
        const std::vector< cv::Mat > &imgs,
        std::string prefix = std::string(""))
{
    std::string s(dirpath);
    return write_images(s, imgs, prefix);
}

#endif /* _IO_HPP_INCLUDED_ */
