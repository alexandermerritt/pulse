// Implementation (in C++) of the JNILinker.java native methods.
// http://www.ibm.com/developerworks/java/tutorials/j-jni/j-jni.html
//
// FIXME Wrap all use of funcs with try/catch; for memc_notfound

#include <iostream>
#include <stdio.h>
#include <mutex>
#include <stdexcept>

#include <jni.h>
#include "JNILinker.h" // generated by javah

#include "StormFuncs.h"

thread_local StormFuncs *funcs;

static std::string servers_g;
static std::mutex servers_lock;

static const std::string prefix("JNI: ");

//==------------------------------------------------------------------
// Utility functions
//==------------------------------------------------------------------

static inline void
jcheck(JNIEnv *env)
{
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        assert("JNI hiccup");
    }
}

// must match Exceptions class
enum jthrow_type {
    JTHROW_PROTOBUF = 1,
    JTHROW_MEMC_NOTFOUND,
    JTHROW_OPENCV,
    JTHROW_NORECOVER, // uncontrollable sharting
};

#define FUNCS_CATCH_BLOCK \
    catch (ocv_vomit &e) { \
        jthrow(env, JTHROW_OPENCV, e.what()); \
        return -1; \
    } catch (protobuf_parsefail &e) { \
        jthrow(env, JTHROW_PROTOBUF, e.what()); \
        return -1; \
    } catch (memc_notfound &e) { \
        jthrow(env, JTHROW_MEMC_NOTFOUND, e.what()); \
        return -1; \
    } catch (std::runtime_error &e) { \
        jthrow(env, JTHROW_NORECOVER, e.what()); \
        return -1; \
    }

static inline void
jthrow(JNIEnv *env, jthrow_type t, const char *msg)
{
    jclass cls = env->FindClass("JNIException");
    jcheck(env);
    if (!cls)
        throw std::runtime_error("jthrow not find class");
    jmethodID constructor = env->GetMethodID(cls,
            "<init>", "(ILjava/lang/String;)V");
    jcheck(env);
    if (!constructor)
        throw std::runtime_error("jthrow not find constructor");
    jstring jmsg = env->NewStringUTF(msg);
    jcheck(env);
    if (!jmsg)
        throw std::runtime_error("jthrow not alloc string");
    jobject jobj = env->NewObject(cls, constructor, t, jmsg);
    if (!jobj)
        throw std::runtime_error("jthrow not alloc exception");
    jthrowable jexception = static_cast<jthrowable>(jobj);
    env->Throw(jexception);
}

static inline void
jthrow(JNIEnv *env, jthrow_type t, std::string &msg)
{
    jthrow(env, t, msg.data());
}

class Lock {
    std::mutex &l;
    public:
    Lock(std::mutex &_l) : l(_l) { l.lock(); }
    ~Lock() { l.unlock(); }
};

static inline std::string
J2C_string(JNIEnv *env, jstring jstr)
{
    const char *cstr = env->GetStringUTFChars(jstr, 0);
    std::string str(cstr);
    env->ReleaseStringUTFChars(jstr, cstr);
    return str;
}

static std::deque<std::string>
J2C_hashset(JNIEnv *env, jobject hashset)
{
    std::deque<std::string> deque(0);

    if (!hashset)
        return deque;

    // HashSet<String>::iterator iter;
    jclass cls = env->GetObjectClass(hashset);
    jcheck(env);
    jmethodID getiter = env->GetMethodID(cls,
            "iterator", "()Ljava/util/Iterator;");
    jcheck(env);
    jobject iter = env->CallObjectMethod(hashset, getiter);
    jcheck(env);
    cls = env->GetObjectClass(iter);
    jcheck(env);

    jmethodID hasNext = env->GetMethodID(cls, "hasNext", "()Z");
    jcheck(env);
    jmethodID next = env->GetMethodID(cls,
            "next", "()Ljava/lang/Object;");
    jcheck(env);

    while (env->CallBooleanMethod(iter, hasNext)) {
        jcheck(env);
        jobject obj = env->CallObjectMethod(iter, next);
        jcheck(env);
        jstring str = static_cast<jstring>(obj);
        jcheck(env);
        deque.push_back(J2C_string(env, str));
    }

    return deque;
}

static void
C2J_hashset(JNIEnv *env, std::deque<std::string> &set, jobject hashset)
{
    jclass cls = env->GetObjectClass(hashset);
    jcheck(env);
    jmethodID clear = env->GetMethodID(cls, "clear", "()V");
    jcheck(env);
    env->CallVoidMethod(hashset, clear);
    jcheck(env);

    jmethodID add = env->GetMethodID(cls,
            "add", "(Ljava/lang/Object;)Z");
    jcheck(env);
    for (std::string &item : set) {
        env->CallBooleanMethod(hashset, add,
                env->NewStringUTF(item.c_str()));
        jcheck(env);
    }
}

// check per-thread funcs ptr is created
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

static inline void
set_servers(std::string &servers)
{
    Lock lock(servers_lock);
    servers_g = servers;
}

//==------------------------------------------------------------------
// JNI implementation
//==------------------------------------------------------------------

// private int setServers(String servers);
JNIEXPORT void JNICALL Java_JNILinker_setServers
  (JNIEnv *env, jobject thisobj, jstring jservers)
{
    std::string servers(J2C_string(env, jservers));
    set_servers(servers);
}

// int neighbors(String vertex, HashSet<String> others);
JNIEXPORT jint JNICALL Java_JNILinker_neighbors
  (JNIEnv *env, jobject thisobj, jstring vertex, jobject hashset)
{
    construct();

    std::string vertex_cpp(J2C_string(env, vertex));
    std::deque<std::string> others;
    try { funcs->neighbors(vertex_cpp, others); } FUNCS_CATCH_BLOCK;

    if (0 == others.size())
        return 0; // nothing to do

    // Move the neighbor vertices to the other object
    auto cls = env->GetObjectClass(hashset);
    if (!cls) return -1;
    // HashSet<E>.add(E e) -- for templated parameters, use Object
    // TODO cache this reference
    auto add = env->GetMethodID(cls, "add", "(Ljava/lang/Object;)Z");
    if (!add) return -1;

    for (std::string &s : others)
        env->CallBooleanMethod(hashset, add,
                env->NewStringUTF(s.c_str()));

    return 0;
}

// int imagesOf(String vertex, HashSet<String> keys);
JNIEXPORT jint JNICALL Java_JNILinker_imagesOf
  (JNIEnv *env, jobject thisobj, jstring vertex, jobject hashset)
{
    construct();

    std::string v(J2C_string(env, vertex));
    std::deque<std::string> keys;
    try { funcs->imagesOf(v, keys); } FUNCS_CATCH_BLOCK;
    if (0 == keys.size())
        return 0; // nothing to do

    auto cls = env->GetObjectClass(hashset);
    if (!cls) return -1;
    auto add = env->GetMethodID(cls, "add", "(Ljava/lang/Object;)Z");
    if (!add) return -1;

    for (std::string &s : keys)
        env->CallBooleanMethod(hashset, add,
                env->NewStringUTF(s.c_str()));

    return 0;
}

// int feature(String image_key);
JNIEXPORT jint JNICALL Java_JNILinker_feature
  (JNIEnv *env, jobject thisobj, jstring image_key)
{
    int ret, num;
    construct();

    std::string key(J2C_string(env, image_key));
    try { ret = funcs->feature(key, num); } FUNCS_CATCH_BLOCK;
    return ret;
}

// int match(HashSet<String> image_keys);
JNIEXPORT jint JNICALL Java_JNILinker_match
  (JNIEnv *env, jobject thisobj, jobject hashset)
{
    construct();

    std::deque<cv::detail::MatchesInfo> matchinfo;
    std::deque<std::string> keys;

    keys = J2C_hashset(env, hashset);
    if (keys.size() == 0)
        return -1;

#if 0 // fucking opencv asserts make me want to die
    if (funcs.match(keys, matchinfo))
        return -1;

    // pick images above some threshold size
    size_t num = keys.size();
    if (num > 16)
        num = static_cast<size_t>(std::log1p(keys.size())) << 2;
    auto comp = [](const cv::detail::MatchesInfo &a,
            const cv::detail::MatchesInfo &b) {
        return !!(a.confidence < b.confidence);
    };
    std::sort(matchinfo.begin(), matchinfo.end(), comp);
    while (matchinfo.size() > num)
        matchinfo.erase(matchinfo.end() - 1);
#else
    size_t num = keys.size();
    if (num > 16)
        num = static_cast<size_t>(std::log1p(keys.size())) << 2;
    while (keys.size() > num)
        keys.erase(keys.end());
#endif

    C2J_hashset(env, keys, hashset);

    return 0;
}

// int montage(HashSet<String> imageKeys, String montage_key);
JNIEXPORT jint JNICALL Java_JNILinker_montage
  (JNIEnv *env, jobject thisobj, jobject hashset, jobject montage_key)
{
    construct();

    std::deque<std::string> keys;
    std::string key;

    keys = J2C_hashset(env, hashset);
    if (keys.size() == 0) {
        jthrow(env, JTHROW_NORECOVER, "converted hashset is empty");
        return -1;
    }

    try { funcs->montage(keys, key); } FUNCS_CATCH_BLOCK;

    // update montage_key
    jclass cls = env->GetObjectClass(montage_key);
    jcheck(env);
    jmethodID append = env->GetMethodID(cls,
            "append", "(Ljava/lang/String;)Ljava/lang/StringBuffer;");
    jcheck(env);
    env->CallObjectMethod(montage_key, append,
            env->NewStringUTF(key.c_str()));
    jcheck(env);

    return 0;
}

// int writeImage(String key, String path);
JNIEXPORT jint JNICALL Java_JNILinker_writeImage
  (JNIEnv *env, jobject thisobj, jstring jkey, jstring jpath)
{
    construct();

    std::string key = J2C_string(env, jkey);
    std::string path = J2C_string(env, jpath);

    try { funcs->writeImage(key, path); } FUNCS_CATCH_BLOCK;

    return 0;
}

