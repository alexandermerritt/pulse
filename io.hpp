/* file: io.hpp
 * author: Alexander Merritt <merritt.alex@gatech.edu>
 */

#ifndef _IO_HPP_INCLUDED_
#define _IO_HPP_INCLUDED_

/*
 * dirlist  path to file containing list of directories to scan for files
 * mats     container into which to read files found on disk
 */
int load_images(std::vector< cv::Mat > &mats, std::string &dirlist);

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

int write_images(std::string &dirpath, std::vector< cv::Mat > &imgs);

#endif /* _IO_HPP_INCLUDED_ */
