#include <exception>
#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>
#include <array>
#include <thread>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <Magick++.h>
#include <opencv2/opencv.hpp>
#include <opensift/include/imgfeatures.h>
#include <opensift/include/sift.h>
#include <hiredis/hiredis.h>

#include "cv/decoders.h"

#define MB (1024. * 1024.)

#define REDIS_UNIX_SOCK "/var/run/redis/redis.sock"
// list to hold newly injected images
#define REDIS_IMG_LIST  "image-list"
// image object key prefix
#define REDIS_IMG_KEY_PREFIX "img-"
#define REDIS_IMG_KEY_DATA_SUFFIX ":data"
#define REDIS_FEAT_KEY_SUFFIX ":feat"
// redis object, total images injected
#define REDIS_IMG_COUNTER "num-images"
#define REDIS_NOTIFY_CHANNEL "channel"

using namespace std;

typedef enum {
    STAT_VMSIZE = 23,
    STAT_RSS = 24,
} statfield_t;

static const vector<int> scales =
    { 75, 150, 240, 320, 640, 1024 };

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

void doConnect(struct redisContext **redis)
{
    if (*redis)
        return;
    *redis = redisConnectUnix(REDIS_UNIX_SOCK);
    if (!*redis)
        throw runtime_error(strerror(errno));
}

static mutex notifyLock;
static list<string> submsgs;

void subscribeThread(void)
{
    redisReply *reply = NULL;
    stringstream cmd;
    int ret;
    struct redisContext *redis = NULL;

    doConnect(&redis);

    cmd << "SUBSCRIBE " << REDIS_NOTIFY_CHANNEL;
    ret = redisAppendCommand(redis, cmd.str().data());
    if (ret != REDIS_OK)
        throw runtime_error("redis subscribe failed");

    while (true) {
        string msg;
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        if (reply->type != REDIS_REPLY_ARRAY) {
            cerr << __func__ << ": reply->type: " << reply->type << endl;
            if (reply->str)
                cerr << reply->str << endl;
            continue;
        }

        // check the reply
        //      subscribe CHANNEL [01]
        //      message   CHANNEL MSG
        if (reply->elements < 3)
            throw runtime_error("subscribe reply: too few elements");
        struct redisReply **elems = reply->element;
        assert( elems[0]->type == REDIS_REPLY_STRING );
        string s(elems[0]->str);
        if (s == "subscribe") {
            s = string(elems[1]->str);
            assert( s == REDIS_NOTIFY_CHANNEL );
            assert( elems[2]->type == REDIS_REPLY_INTEGER );
            assert( elems[2]->integer == 1 );
            freeReplyObject(reply);
            continue;
        }

        if (s != "message")
            throw runtime_error("unknown subscribe response");

        s = string(elems[1]->str);
        assert( s == REDIS_NOTIFY_CHANNEL );
        msg = string( elems[2]->str );

        notifyLock.lock();
        submsgs.push_back(msg);
        notifyLock.unlock();

        freeReplyObject(reply);
    }
}

// "Flickr" or "Etsy" workload
// create 2-6 smaller-scale images
void doScale(void) // FIXME change to key? or blob?
{
    int ret, nimages;
    stringstream cmd;
    struct redisReply *reply = NULL;
    struct redisContext *redis = NULL;

    doConnect(&redis);

    thread t(subscribeThread);
    t.detach();

    while (true) {
        Magick::Blob inblob, outblob;
        string key;

        if (submsgs.size() == 0) {
            sleep(1);
            continue;
        }
        notifyLock.lock();
        key = submsgs.front();
        submsgs.pop_front();
        notifyLock.unlock();

        // pull the image object
        cmd.str(string());
        cmd << "GET " << key << REDIS_IMG_KEY_DATA_SUFFIX;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis get failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        if (reply->type != REDIS_REPLY_STRING ) {
            cerr << __func__ << ": reply->type: " << reply->type << endl;
            if (reply->str)
                cerr << reply->str << endl;
            continue;
        }

        inblob.updateNoCopy(reply->str, reply->len,
                Magick::Blob::MallocAllocator);

        // FIXME blob's destructor will free this (on next loop
        // iteration), as it seems updateNoCopy takes ownership of the
        // pointer directly.
        reply->str = NULL;
        reply->len = 0;

        // scale different sizes
        for (int scale : scales) {
            struct redisReply *rr;
            Magick::Image img;
            string scaleKey = key + "_" + to_string(scale)
                + REDIS_IMG_KEY_DATA_SUFFIX;
            cout << key << " " << scale << endl;

            // check if exists
            cmd.str(string());
            cmd << "EXISTS " << scaleKey;
            ret = redisAppendCommand(redis, cmd.str().data());
            if (ret != REDIS_OK)
                throw runtime_error("redis exists failed");
            ret = redisGetReply(redis, (void**)&rr);
            if (ret != REDIS_OK)
                throw runtime_error("redis getReply failed");
            if (!rr)
                throw runtime_error("redis rr is null");
            assert( rr->type == REDIS_REPLY_INTEGER );
            if (rr->integer == 1)
                continue;
            freeReplyObject(rr);

            // create thumbnail
            img.read(inblob); // prob. makes a copy
            if (img.rows() < scale && img.columns() < scale)
                break;
            img.thumbnail(Magick::Geometry(scale,scale));
            img.magick("JPG");
            img.write(&outblob);

            // push to redis
            ret = redisAppendCommand(redis, "SET %s %b",
                    scaleKey.data(),
                    outblob.data(), outblob.length());
            if (ret != REDIS_OK)
                throw runtime_error("redis SET failed");
            ret = redisGetReply(redis, (void**)&rr);
            if (ret != REDIS_OK)
                throw runtime_error("redis getReply failed");
            if (!rr)
                throw runtime_error("redis rr is null");
            assert( rr->type == REDIS_REPLY_STATUS );
            assert( 0 == strncmp(rr->str, "OK", 2) );
            freeReplyObject(rr);
        }
        freeReplyObject(reply);
    }
}

void doSIFT(int scale = -1)
{
    int ret, nimages;
    stringstream cmd;
    struct redisReply *reply = NULL;
    struct redisContext *redis = NULL;

    if (scale >= (int)scales.size())
        throw runtime_error(string(__func__)
                + string(": scale too large"));

    doConnect(&redis);

    thread t(subscribeThread);
    t.detach();

    while (true) {
        Magick::Blob inblob, outblob;
        string key;

        if (submsgs.size() == 0) {
            continue;
        }
        notifyLock.lock();
        key = submsgs.front();
        submsgs.pop_front();
        notifyLock.unlock();

        // pull the image object
        cmd.str(string());
        cmd << "GET " << key;
        if (scale >= 0 && scale < (int)scales.size())
            cmd << "_" << scales.at(scale);
        cmd << REDIS_IMG_KEY_DATA_SUFFIX;
        cout << cmd.str() << endl;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis get failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        if (reply->type != REDIS_REPLY_STRING ) {
            cerr << __func__ << ": reply->type: " << reply->type << endl;
            if (reply->str)
                cerr << reply->str << endl;
            continue;
        }
        cout << "  image size (enc)  " << reply->len << endl;

        // Construct image object in opencv by decoding buffer.
        // Find the features.
        cv::Mat mat = jpeg::JPEGasMat(reply->str, reply->len);
        IplImage img(mat);
        freeReplyObject(reply); // release encoded image

        cout << "  image size (dec)  "
            << mat.total() * mat.elemSize()
            << endl;

        cout << "  detecting    ...  "; cout.flush();
        struct feature *feat;
        int n = sift_features(&img, &feat);
        cout << n << endl;

        // Send features to redis.  The features array is flat,
        // so shove a single object into redis
        string featKey = key;
        if (scale >= 0 && scale < (int)scales.size())
            featKey += "_" + to_string(scales.at(scale));
        featKey += REDIS_FEAT_KEY_SUFFIX;
        cout << "  sending feat size " << (n*sizeof(*feat)) << endl;
        ret = redisAppendCommand(redis, "SET %s %b",
                featKey.data(), feat, (n*sizeof(*feat)));
        if (ret != REDIS_OK)
            throw runtime_error("redis SET failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_STATUS );
        assert( 0 == strncmp(reply->str, "OK", 2) );
        freeReplyObject(reply);

        // one free seems sufficient -- feat.fwd_match etc. exist but are
        // expected to be NULL, so single free is ok
        free(feat);

        cout << "ok " << key << endl;
    }
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

//
// work generator functions
//

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

    while ((buflen - nwritten) > 0) {
        nwritten = write(c->fd,(void*)bufpos,buflen-nwritten);
        if (nwritten == -1) {
            if ((errno == EAGAIN && !(c->flags & REDIS_BLOCK)) || (errno == EINTR)) {
                /* Try again later */
                goto out;
            } else {
                // __redisSetError(c,REDIS_ERR_IO,NULL);
                return REDIS_ERR;
            }
        } else if (nwritten > 0) {
            bufpos += nwritten;
        }
    }
out:
    if (done != NULL) *done = (nwritten == buflen);
    return REDIS_OK;
}

// returns number of images available
int doUpload(void)
{
    size_t imgidx = 1;
    stringstream cmd;
    string path;
    redisReply *reply = NULL;
    int ret = 0, flags = 0, fd = -1;
    struct stat statinfo;
    size_t maplen = 0;
    void *imgmap = NULL;
    struct redisContext *redis = NULL;

    doConnect(&redis);

    // Connecting to Redis with UNIX domain sockets can trigger
    // SIGPIPE on writes. Ignore them and do error checking manually.
    signal(SIGPIPE, SIG_IGN);

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
    while (cin >> path) {
        size_t slashpos = path.find_last_of("/");
        string fname = path.substr(slashpos+1);

        cout << fname << endl;

        // check if image already loaded
        string key = REDIS_IMG_KEY_PREFIX;
        key += to_string(imgidx);
        key += REDIS_IMG_KEY_DATA_SUFFIX;
        ret = redisAppendCommand(redis, "EXISTS %s", key.data());
        if (ret != REDIS_OK)
            throw runtime_error("redis exists failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_INTEGER );
        if (reply->integer == 1) {
            freeReplyObject(reply);
            goto update;
        }
        freeReplyObject(reply);

        // add the JPG binary: map in image, send to Redis
        if (stat(path.data(), &statinfo))
            throw runtime_error(strerror(errno));
        maplen = statinfo.st_size;
        flags = MAP_SHARED | MAP_POPULATE;
        fd = open(path.data(), O_RDONLY);
        if (fd < 0)
            throw runtime_error(strerror(errno));
        imgmap = mmap(NULL, maplen, PROT_READ, flags, fd, 0);
        if (imgmap == MAP_FAILED)
            throw runtime_error(strerror(errno));

#if 1
        ret = redisAppendCommand(redis, "SET %s%d%s %b",
                REDIS_IMG_KEY_PREFIX, imgidx,
                REDIS_IMG_KEY_DATA_SUFFIX,
                imgmap, maplen);
        if (ret != REDIS_OK)
            throw runtime_error("redis set failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_STATUS );
        assert( 0 == strncmp(reply->str, "OK", 2) );
        freeReplyObject(reply);

#else   // --- OR ---

        // throws SIGPIPEs... I think Redis chops the connection.
        int done;
        assert( REDIS_OK == redisBufferWriteBlob(redis, &done, (void*)"SET alex ", 9) );
        assert( REDIS_OK == redisBufferWriteBlob(redis, &done, imgmap, maplen) );
#endif

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

update:
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

    return --imgidx;
}

void __publish(struct redisContext *redis, int nimages)
{
    redisReply *reply = NULL;
    stringstream cmd;
    int ret;

    for (int i = 1; i < nimages; i++) {
        // push item to list
        cmd.str(string());
        cmd << "LPUSH"
            << " " << REDIS_IMG_LIST
            << " " << REDIS_IMG_KEY_PREFIX << i;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis lpush failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_INTEGER );
        freeReplyObject(reply);

        // publish notification
        cmd.str(string());
        cmd << "PUBLISH"
            << " " << REDIS_NOTIFY_CHANNEL
            << " " << REDIS_IMG_KEY_PREFIX << i;
        ret = redisAppendCommand(redis, cmd.str().data());
        if (ret != REDIS_OK)
            throw runtime_error("redis publish failed");
        ret = redisGetReply(redis, (void**)&reply);
        if (ret != REDIS_OK)
            throw runtime_error("redis getReply failed");
        if (!reply)
            throw runtime_error("redis reply is null");
        assert( reply->type == REDIS_REPLY_INTEGER );
        freeReplyObject(reply);

        cout << i << " "; cout.flush();
        sleep(1);
    }
}

void doPublish(void)
{
    redisReply *reply = NULL;
    stringstream cmd;
    int ret, nimages;
    struct redisContext *redis = NULL;

    doConnect(&redis);

    // read the counter to know how many to publish
    cmd << "GET " << REDIS_IMG_COUNTER;
    cout << cmd.str() << endl;
    ret = redisAppendCommand(redis, cmd.str().data());
    if (ret != REDIS_OK)
        throw runtime_error("redis get failed");
    ret = redisGetReply(redis, (void**)&reply);
    if (ret != REDIS_OK)
        throw runtime_error("redis getReply failed");
    if (!reply)
        throw runtime_error("redis reply is null");
    assert( reply->type == REDIS_REPLY_STRING );
    nimages = strtol(reply->str, NULL, 10);
    freeReplyObject(reply);

    cout << nimages << " images to publish" << endl;
    __publish(redis, nimages);
}

int main(int narg, char *args[])
{
    long vm, rss;

    if (narg != 2)
        return 1;

    Magick::InitializeMagick(*args);

    string arg(args[1]);
    if (arg == "upload") {
        doUpload();
    } else if (arg == "publish") {
        doPublish();
    } else if (arg == "scale") {
        doScale();
    } else if (arg == "sift") {
        doSIFT(scales.size()-1);
    } else {
        cerr << "Command unknown" << endl;
        return 1;
    }
    return 0;
}
