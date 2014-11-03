/* file: io.hpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

#ifndef _IO_HPP_INCLUDED_
#define _IO_HPP_INCLUDED_

#include <stdio.h>
#include "types.hpp"

void read_stdin(paths_t &paths);

void progress_bar(int i, int max);
void progress_clear(void);

int load_images(images_t &imgs, dir_t &dir, paths_t &outpaths);
int load_images(images_t &imgs, paths_t &_paths);
int listdir(dir_t &dir, paths_t &entries);
int load_image(image_t &img, const path_t &path);

/* XXX this writes many images, so first arg is diretory path */
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

/* XXX this writes one file, so first arg is full filename, not a directory */
int write_features(std::string &filepath,
        cv::Mat &img, cv::detail::ImageFeatures &features);
static inline int write_features(const char *filepath,
        cv::Mat &img, cv::detail::ImageFeatures &features)
{
    std::string s(filepath);
    return write_features(s, img, features);
}

int write_image(std::string &filepath, const cv::Mat &img);

static inline int write_image(const char *filepath, const cv::Mat &img)
{
    std::string s(filepath);
    return write_image(s, img);
}

int write_images(std::string &dirpath,
        const images_t &imgs,
        std::string prefix = std::string(""));

static inline int write_images(const char *dirpath,
        const images_t &imgs,
        std::string prefix = std::string(""))
{
    std::string s(dirpath);
    return write_images(s, imgs, prefix);
}

void prune_paths(paths_t &_paths, const std::vector< std::string > &exts);

#endif /* _IO_HPP_INCLUDED_ */
