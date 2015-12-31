#include <exception>
#include <fstream>
#include <iostream>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <Magick++.h>
#include <opencv2/opencv.hpp>
#include <opensift/include/sift.h>
#include <hiredis/hiredis.h>

#define MB (1024. * 1024.)

#define REDIS_UNIX_SOCK "/var/run/redis/redis.sock"
// list to hold newly injected images
#define REDIS_IMG_LIST  "image-list"
// image object key prefix
#define REDIS_IMG_KEY_PREFIX "img-"
#define REDIS_IMG_KEY_DATA_SUFFIX ":data"
// redis object, total images injected
#define REDIS_IMG_COUNTER "num-images"

using namespace std;

typedef enum {
    STAT_VMSIZE = 23,
    STAT_RSS = 24,
} statfield_t;

// getstat(STAT_VMSIZE)
// getstat(STAT_RSS, 12)        because RSS is reported in pages
long getstat(statfield_t t, size_t shift = 0UL, pid_t pid = 0)
{
    long value;
    string line;
    array<char, 512> cline;
    if (pid == 0)
        pid = getpid(); // self
    stringstream input;
    const char *val = NULL;
    input << "/proc/" << pid << "/stat";
    ifstream ifs(input.str());
    getline(ifs, line);
    memcpy(cline.data(), line.c_str(), min(line.size(),cline.size()));
    strtok(cline.data(), " ");
    for (int f = 0; f < t - 1; f++)
        val = strtok(NULL, " ");
    sscanf(val, "%ld", &value);
    return (value<<shift);
}

Magick::Blob* newBlob(const char *path)
{
    struct stat finfo;
    Magick::Blob *blob = nullptr;

    int fd = open(path, O_RDONLY);
    if (fd < 1)
        throw runtime_error(strerror(errno));
    if (stat(path, &finfo))
        throw runtime_error(strerror(errno));

    off_t len = finfo.st_size;
    int flags = MAP_SHARED | MAP_POPULATE;
    void *m = mmap(NULL, len, PROT_READ, flags, fd, 0);
    if (m == MAP_FAILED)
        throw runtime_error(strerror(errno));

    blob = new Magick::Blob(m, len); // makes a copy!

    munmap(m, len);
    close(fd);

    return blob;
}

// TODO create 2-6 smaller-scale images
// "Flickr" or "Etsy" workload
void rescale(const char *path) // FIXME change to key? or blob?
{
    Magick::Blob *blob = newBlob(path);

    Magick::Image img;
    img.read(*blob);
    img.magick("JPG");

    //img.rotate(90);
    img.scale(Magick::Geometry(1024,1024));
    //img.gaussianBlur(2, 3);

    //img.write("/tmp/magick.jpg");

    delete blob;
}

void findSIFT(const char *path) // FIXME change to key? or blob?
{
    struct feature *feat;
    int n;

    // TODO how to load from memory, not disk?
    cv::Mat mat = cv::imread(path);
    cv::Mat smaller;

    cv::resize(mat, smaller, cv::Size(), 0.7, 0.7);
    IplImage img(smaller), *imgp = &img;

    cout << "(SIFT)" << endl;
    n = sift_features(&img, &feat);

    free(feat);
}

void findHOG(const char *path) // FIXME change to key? or blob?
{
    // TODO how to load from memory, not disk?
    cv::Mat mat = cv::imread(path), smaller;
    cv::HOGDescriptor hog;
    hog.setSVMDetector(cv::HOGDescriptor::getDefaultPeopleDetector());
    vector<cv::Point> locs;

    cv::resize(mat, smaller, cv::Size(), 0.7, 0.7);
    hog.detect(smaller, locs);
    //hog.detect(mat, locs);
}

// Need a way to write to Redis without copying the command buffer -
// esp. for larger objects like images. This is similar to
// redisBufferWrite but uses a user-supplied input buffer.
int redisBufferWriteBlob(redisContext *c, int *done,
        void *buf, size_t buflen)
{
    int nwritten = 0;
    uintptr_t bufpos = (uintptr_t)buf;

    if (c->err)
        return REDIS_ERR;

    if ((buflen - nwritten) > 0) {
        nwritten = write(c->fd,(void*)bufpos,buflen-nwritten);
        if (nwritten == -1) {
            if ((errno == EAGAIN && !(c->flags & REDIS_BLOCK)) || (errno == EINTR)) {
                /* Try again later */
            } else {
                // __redisSetError(c,REDIS_ERR_IO,NULL);
                return REDIS_ERR;
            }
        } else if (nwritten > 0) {
            bufpos -= nwritten;
        }
    }
    if (done != NULL) *done = (nwritten == buflen);
    return REDIS_OK;
}

void publish(const string &ipath)
{
    ifstream ilist(ipath);
    size_t imgidx = 1;
    stringstream cmd;
    string path;
    redisReply *reply;
    int ret;

    if (!ilist.is_open())
        throw runtime_error(strerror(errno));

    redisContext *redis;
    redis = redisConnectUnix(REDIS_UNIX_SOCK);
    if (!redis)
        throw runtime_error(strerror(errno));

    // reset counter
    cmd.str(string());
    cmd << "SET " << REDIS_IMG_COUNTER << " 0";
    ret = redisAppendCommand(redis, cmd.str().data());
    if (ret != REDIS_OK)
        throw runtime_error("redis set failed");
    ret = redisGetReply(redis, (void**)&reply);
    if (ret != REDIS_OK)
        throw runtime_error("redis getReply failed");
    if (!reply)
        throw runtime_error("redis reply is null");
    freeReplyObject(reply);

    // add all images
    while (ilist >> path) {
        size_t slashpos = path.find_last_of("/");
        string fname = path.substr(slashpos+1);

        // add the JPG binary: map in image, send to Redis
        struct stat statinfo;
        if (stat(path.data(), &statinfo))
            throw runtime_error(strerror(errno));
        size_t maplen = statinfo.st_size;
        const int flags = MAP_SHARED | MAP_POPULATE;
        int fd = open(path.data(), O_RDONLY);
        if (fd < 0)
            throw runtime_error(strerror(errno));
        void *imgmap = mmap(NULL, maplen, PROT_READ, flags, fd, 0);
        if (imgmap == MAP_FAILED)
            throw runtime_error(strerror(errno));

        ret = redisAppendCommand(redis, "set %s%d%s %b",
                REDIS_IMG_KEY_PREFIX, imgidx,
                REDIS_IMG_KEY_DATA_SUFFIX,
                imgmap, maplen);
        if (ret != REDIS_OK)
            throw runtime_error("redis append failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_STATUS );
        assert( 0 == strncmp(reply->str, "OK", 2) );
        freeReplyObject(reply);
        // --- OR ---
        // redisBufferWriteBlob()

        munmap(imgmap, maplen);
        imgmap = NULL;
        close(fd);
        fd = -1;

        // add the JPG metadata
        cmd.str(string());
        cmd << "HMSET " << REDIS_IMG_KEY_PREFIX << imgidx
            << " name " << fname
            << " datakey " << REDIS_IMG_KEY_PREFIX << imgidx
            << REDIS_IMG_KEY_DATA_SUFFIX
            << " len " << statinfo.st_size;
        cout << cmd.str() << endl;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis append failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_STATUS );
        assert( 0 == strncmp(reply->str, "OK", 2) );
        freeReplyObject(reply);

        // update image counter
        cmd.str(string());
        cmd << "INCR " << REDIS_IMG_COUNTER;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis incr failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        freeReplyObject(reply);

        imgidx++;
    }
    ilist.close();

    // append JPG to list - throttled
    //cmd.str(string());
    //cmd << "LPUSH " << REDIS_IMG_LIST << " " << path;
}

int main(int narg, char *args[])
{
    long vm, rss;

    if (narg != 2)
        return 1;

    Magick::InitializeMagick(*args);

    publish(string(args[1]));

#if 0
    vm = getstat(STAT_VMSIZE, 0, getpid());
    rss = getstat(STAT_RSS, 12, getpid());
    cout << "vm  " << vm/MB << endl;
    cout << "rss " << rss/MB << endl;

    string line;
    while (cin >> line) {
        cout << line << endl;
        //transform(args[1]);
        //findSIFT(line.c_str());
        findHOG(line.c_str());
    }
#endif

    return 0;
}
