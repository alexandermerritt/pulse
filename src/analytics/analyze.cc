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
#include <opencv2/objdetect/objdetect.hpp>
#include <opensift/include/imgfeatures.h>
#include <opensift/include/sift.h>
#include <hiredis/hiredis.h>

#include "cv/decoders.h"

#define MB (1024. * 1024.)

// milliseconds in a nanosecond
#define MS_nsec (1e3)

#define REDIS_UNIX_SOCK "/var/run/redis/redis.sock"
// list to hold newly injected images
#define REDIS_IMG_LIST  "image-list"
// image object key prefix
#define REDIS_IMG_KEY_PREFIX "img-"
#define REDIS_IMG_KEY_DATA_SUFFIX ":data"
#define REDIS_FEAT_KEY_SUFFIX ":feat"
#define REDIS_HOG_KEY_SUFFIX ":hog"
#define REDIS_FACE_KEY_SUFFIX ":face"
#define REDIS_EYE_KEY_SUFFIX ":eye"
// redis object, total images injected
#define REDIS_IMG_COUNTER "num-images"
#define REDIS_NOTIFY_CHANNEL "channel"

// HAAR training sets
enum {
    HAAR_EYE = 0,
    HAAR_EYES_BIG,
    HAAR_EYES_SM,
    HAAR_FACE_ALT2,
    HAAR_FACE_ALT,
    HAAR_FACE_ALTT,
    HAAR_FACE,
    HAAR_FACE_PROF,
    HAAR_FULL_BODY,
    HAAR_LEFT_EAR,
    HAAR_LEFT_EYE,
    HAAR_LEFT_EYE2,
    HAAR_LOW_BODY,
    HAAR_MOUTH,
    HAAR_NOSE,
    HAAR_RIGHT_EAR,
    HAAR_RIGHT_EYE_2,
    HAAR_RIGHT_EYE,
    HAAR_SMILE,
    HAAR_SPECS,
    HAAR_UP_BODY,
    HAAR_UP_BODY2,
};
#define HAAR_XML_BASEDIR "/usr/share/opencv/haarcascades"
static const char *haar_xml[] = {
    [HAAR_EYE]         = HAAR_XML_BASEDIR "/haarcascade_eye.xml",
    [HAAR_EYES_BIG]    = HAAR_XML_BASEDIR "/haarcascade_mcs_eyepair_big.xml",
    [HAAR_EYES_SM]     = HAAR_XML_BASEDIR "/haarcascade_mcs_eyepair_small.xml",
    [HAAR_FACE_ALT2]   = HAAR_XML_BASEDIR "/haarcascade_frontalface_alt2.xml",
    [HAAR_FACE_ALT]    = HAAR_XML_BASEDIR "/haarcascade_frontalface_alt.xml",
    [HAAR_FACE_ALTT]   = HAAR_XML_BASEDIR "/haarcascade_frontalface_alt_tree.xml",
    [HAAR_FACE]        = HAAR_XML_BASEDIR "/haarcascade_frontalface_default.xml",
    [HAAR_FACE_PROF]   = HAAR_XML_BASEDIR "/haarcascade_profileface.xml",
    [HAAR_FULL_BODY]   = HAAR_XML_BASEDIR "/haarcascade_fullbody.xml",
    [HAAR_LEFT_EAR]    = HAAR_XML_BASEDIR "/haarcascade_mcs_leftear.xml",
    [HAAR_LEFT_EYE]    = HAAR_XML_BASEDIR "/haarcascade_mcs_lefteye.xml",
    [HAAR_LEFT_EYE2]   = HAAR_XML_BASEDIR "/haarcascade_lefteye_2splits.xml",
    [HAAR_LOW_BODY]    = HAAR_XML_BASEDIR "/haarcascade_lowerbody.xml",
    [HAAR_MOUTH]       = HAAR_XML_BASEDIR "/haarcascade_mcs_mouth.xml",
    [HAAR_NOSE]        = HAAR_XML_BASEDIR "/haarcascade_mcs_nose.xml",
    [HAAR_RIGHT_EAR]   = HAAR_XML_BASEDIR "/haarcascade_mcs_rightear.xml",
    [HAAR_RIGHT_EYE_2] = HAAR_XML_BASEDIR "/haarcascade_righteye_2splits.xml",
    [HAAR_RIGHT_EYE]   = HAAR_XML_BASEDIR "/haarcascade_mcs_righteye.xml",
    [HAAR_SMILE]       = HAAR_XML_BASEDIR "/haarcascade_smile.xml",
    [HAAR_SPECS]       = HAAR_XML_BASEDIR "/haarcascade_eye_tree_eyeglasses.xml",
    [HAAR_UP_BODY]     = HAAR_XML_BASEDIR "/haarcascade_mcs_upperbody.xml",
    [HAAR_UP_BODY2]    = HAAR_XML_BASEDIR "/haarcascade_upperbody.xml",
};

using namespace std;

typedef chrono::system_clock::time_point timept;

typedef enum {
    STAT_VMSIZE = 23,
    STAT_RSS = 24,
} statfield_t;

static const vector<unsigned int> scales =
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

    auto scales_(scales);
    sort(scales_.begin(), scales_.end());
    reverse(scales_.begin(), scales_.end());

    while (true) {
        Magick::Blob blob;
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

        timept c2, c1, c0 = chrono::high_resolution_clock::now();
        blob.update(reply->str, reply->len);
        //blob.updateNoCopy(reply->str, reply->len,
                //Magick::Blob::MallocAllocator);
        unsigned int r, c;
        {
            Magick::Image img;
            img.ping(blob); // read header only
            r = img.rows();
            c = img.columns();
        }

        // FIXME blob's destructor will free this (on next loop
        // iteration), as it seems updateNoCopy takes ownership of the
        // pointer directly.
        //reply->str = NULL;
        //reply->len = 0;

        // scale different sizes
        for (unsigned int scale : scales_) {
            struct redisReply *rr;
            Magick::Image img;
            string scaleKey = key + "_" + to_string(scale)
                + REDIS_IMG_KEY_DATA_SUFFIX;
            cout << key << " " << scale
                << " " << c << " x " << r
                << endl;

#if 0
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
#endif

            c1 = chrono::high_resolution_clock::now();
            // create thumbnail
            img.read(blob); // prob. makes a copy
            if (r < scale && c < scale) {
                cout << "  skipping" << endl;
                continue;
            }
            c2 = chrono::high_resolution_clock::now();

            cout << "  decode time "
                << chrono::duration_cast<std::chrono::microseconds>(c2-c1).count()
                << endl;

            c1 = chrono::high_resolution_clock::now();
            img.thumbnail(Magick::Geometry(scale,scale));
            img.magick("JPG");
            img.write(&blob);
            c2 = chrono::high_resolution_clock::now();

            cout << "  scale time " << scaleKey << " "
                << chrono::duration_cast<std::chrono::microseconds>(c2-c1).count()
                << endl;

            // push to redis
            ret = redisAppendCommand(redis, "SET %s %b",
                    scaleKey.data(),
                    blob.data(), blob.length());
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

        c2 = chrono::high_resolution_clock::now();
        cout << "  hold time "
            << chrono::duration_cast<std::chrono::microseconds>(c2-c0).count()
            << endl;
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

        // TODO check exists already

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

        timept c2, c1, c0 = chrono::high_resolution_clock::now();
        // Construct image object in opencv by decoding buffer.
        // Find the features.
        c1 = chrono::high_resolution_clock::now();
        cv::Mat mat = jpeg::JPEGasMat(reply->str, reply->len);
        c2 = chrono::high_resolution_clock::now();
        IplImage img(mat);
        freeReplyObject(reply); // release encoded image

        cout << "  decode time "
            << chrono::duration_cast<std::chrono::microseconds>(c2-c1).count()
            << endl;

        cout << "  image size (dec)  "
            << mat.total() * mat.elemSize()
            << endl;

        cout << "  detecting    ...  "; cout.flush();
        struct feature *feat;
        c1 = chrono::high_resolution_clock::now();
        int n = sift_features(&img, &feat);
        c2 = chrono::high_resolution_clock::now();
        cout << n << endl;

        cout << "  analysis time "
            << chrono::duration_cast<std::chrono::microseconds>(c2-c1).count()
            << endl;

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

        c2 = chrono::high_resolution_clock::now();
        cout << "  hold time "
            << chrono::duration_cast<std::chrono::microseconds>(c2-c0).count()
            << endl;

        cout << "ok " << key << endl;
    }
}

// much a copy/paste of doSIFT due to shared code...
void doCascade(int scale = -1)
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
        string key;

        if (submsgs.size() == 0) {
            continue;
        }
        notifyLock.lock();
        key = submsgs.front();
        submsgs.pop_front();
        notifyLock.unlock();

        // TODO check exists already

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
        cv::Mat mat = jpeg::JPEGasMat(reply->str, reply->len);
        freeReplyObject(reply); // release encoded image

        cout << "  image size (dec)  "
            << mat.total() * mat.elemSize()
            << endl;

        // Find multiple types of objects
        vector<int> objs = {HAAR_FACE, HAAR_EYE, HAAR_MOUTH, HAAR_SPECS};
        vector<string> objSuffix = {":face", ":eye", ":mouth", ":specs"};

        for (size_t o = 0; o < objs.size(); o++) {
            cout << "  detecting    ...  "; cout.flush();
            vector<cv::Rect> rects;
            cv::CascadeClassifier cas;

            cas.load(haar_xml[o]);
            cas.detectMultiScale(mat, rects, 1.1, 2,
                    CV_HAAR_DO_ROUGH_SEARCH, Size(30,30));
            cout << rects.size() << endl;

            if (rects.size() < 1)
                continue;

            // Convert STL container to flat array.
            struct item { int x,y,w,h; } *casbuf;
            size_t caslen = rects.size() * sizeof(*casbuf);
            casbuf = (struct item*)malloc(caslen);
            assert( casbuf );
            for (size_t i = 0; i < rects.size(); i++) {
                casbuf[i].x = rects[i].x;
                casbuf[i].y = rects[i].y;
                casbuf[i].w = rects[i].width;
                casbuf[i].h = rects[i].height;
            }

            // Send points to redis. Use single, flat object.
            string casKey = key;
            if (scale >= 0 && scale < (int)scales.size())
                casKey += "_" + to_string(scales.at(scale));
            casKey += objSuffix[o];
            cout << "  sending size      " << caslen << endl;
            ret = redisAppendCommand(redis, "SET %s %b",
                    casKey.data(), casbuf, caslen);
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

            free(casbuf);
        }

        cout << "ok " << key << endl;
    }
}

// much a copy/paste of doSIFT due to shared code...
void doHOG(int scale = -1)
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

        // TODO check exists already

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
        freeReplyObject(reply); // release encoded image

        cout << "  image size (dec)  "
            << mat.total() * mat.elemSize()
            << endl;

        // TODO HOG detection TODO
        cout << "  detecting    ...  "; cout.flush();
        cv::HOGDescriptor hog;
        hog.setSVMDetector(cv::HOGDescriptor::getDefaultPeopleDetector());
        vector<cv::Rect> rects; // int x, y, width, height
        vector<double> weights;
        hog.detectMultiScale(mat, rects, weights);
        cout << rects.size() << endl;
        assert( weights.size() == rects.size() );

        // Convert STL container to flat array.
        // cv::Point has only int x,y, so can use a flattened buffer.
        struct item { int x,y,w,h; double ww; } *hogbuf;
        size_t hoglen = rects.size() * sizeof(*hogbuf);
        hogbuf = (struct item*)malloc(hoglen);
        assert( hogbuf );
        for (size_t i = 0; i < rects.size(); i++) {
            hogbuf[i].x = rects[i].x;
            hogbuf[i].y = rects[i].y;
            hogbuf[i].w = rects[i].width;
            hogbuf[i].h = rects[i].height;
            hogbuf[i].ww = weights[i];
        }

        // Send points to redis. Use single, flat object.
        string hogKey = key;
        if (scale >= 0 && scale < (int)scales.size())
            hogKey += "_" + to_string(scales.at(scale));
        hogKey += REDIS_HOG_KEY_SUFFIX;
        cout << "  sending hog size  " << hoglen << endl;
        ret = redisAppendCommand(redis, "SET %s %b",
                hogKey.data(), hogbuf, hoglen);
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

        free(hogbuf);

        cout << "ok " << key << endl;
    }
}

//
// work generator functions
//

#if 0
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
#endif

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

    for (int i = 1; i < nimages+1; i++) {
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
        usleep(100*MS_nsec);
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

    if (getenv("QUIET")) {
        int fd = open("/dev/null", O_RDWR);
        dup2(fd, fileno(stdout));
        close(fd);
    }

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
    } else if (arg == "hog") {
        doHOG();
    } else if (arg == "cas") {
        //doCascade(scales.size()-1);
        doCascade();
    } else {
        cerr << "Command unknown" << endl;
        return 1;
    }
    return 0;
}
