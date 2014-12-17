#include <deque>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <set>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <unistd.h>

#include <opencv2/core/core.hpp>
#include <opencv2/highgui/highgui.hpp>

#include <libmemcached/memcached.h>

#include "Objects.pb.h" // generated
#include "Config.hpp"

using namespace std;

struct EgoID
{
    // entry is [path/somewhere/]egoid
    EgoID(string &entry)
        : path(entry)
    {
        auto idx = entry.rfind("/");
        if (string::npos != idx)
            id = entry.substr(idx + 1);
    }
    string id, path;
};

deque<EgoID> egoids;

vector< string > imagelist;
typedef EgoID egoid_t;
typedef string id;

memcached_st *memc = NULL;

int readfile(string &path, void **buf, size_t *len)
{
    struct stat st;
    uintptr_t ptr;
    int fd = open(path.c_str(), O_RDONLY);

    if (fd < 0)
        return -1;

    stat(path.c_str(), &st);
    size_t rem = *len = st.st_size;
    *buf = malloc(*len);
    if (!*buf)
        goto err;

    ptr = (uintptr_t)*buf;
    while (rem > 0) {
        size_t rd = read(fd, (void*)ptr, rem);
        if (rd == 0)
            goto err;
        rem -= rd;
        ptr += rd;
    }

    close(fd);
    return 0;

err:
    if (fd > 0)
        close(fd);
    if (*buf)
        free(*buf);
    return -1;
}

int memc_exists(memcached_st *memc,
        const string &key)
{
    memcached_return_t mret;

    assert(memc);

    mret = memcached_exist(memc, key.c_str(), key.length());
    if (MEMCACHED_SUCCESS == mret)
        return true;
    if (MEMCACHED_NOTFOUND == mret)
        return false;

    abort();
    return false;
}

// full path to image per line
int load_images(string &path)
{
    memcached_return_t mret;
    string line;
    imagelist.clear();

    printf(">> Loading images...\n");

    ifstream file;
    file.open(path);
    if (!file.is_open()) {
        perror("open image file");
        return -1;
    }
    while (getline(file, line)) {
        if (line[0] == '#')
            continue;
        imagelist.push_back(line);
    }
    file.close();

    for (auto &imgpath : imagelist) {
        auto idx = imgpath.rfind("/");
        string imgname(imgpath);
        if (idx != string::npos)
            imgname = imgpath.substr(idx + 1);
        cout << imgname << endl;

        if (memc_exists(memc, imgname)) {
            cout << "   skipping; already uploaded" << endl;
            continue;
        }

        cv::Mat img;
        img = cv::imread(imgpath, 1);
        if (!img.data) {
            fprintf(stderr, "failed to read %s\n", imgpath.c_str());
            return -1;
        }

        if (img.total() > (20 * 1e6)) { // too big
            cout << "   skipping; too big" << endl;
            continue;
        }

        void *buf;
        size_t len;

        storm::Image image;
        string s; // serialized header
        image.set_key_id(imgname);
        image.set_width(img.cols);
        image.set_height(img.rows);
        image.set_depth(img.elemSize());
        image.set_key_data(imgname + "::data");

        len = image.ByteSize();
        buf = malloc(len);
        if (!buf)
            return -1;
        image.SerializeToArray(buf, len);

        // save the header
        mret = memcached_set(memc, imgname.c_str(), imgname.length(),
                (char*)buf, len, 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }
        free(buf);

        // save the raw data
        if (0 > readfile(imgpath,  &buf, &len))
            return -1;
        mret = memcached_set(memc, image.key_data().c_str(),
                image.key_data().length(), (char*)buf, len, 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }
        free(buf);
    }

    return 0;
}

// appends contents of file to lines
int read_lines(string &fname, deque<string> &lines)
{
    ifstream file;
    file.open(fname);
    if (!file.is_open()) {
        string msg("open file " + fname);
        perror(msg.data());
        return -1;
    }
    string line;
    while (getline(file, line)) {
        lines.push_back(line);
    }
    file.close();

    return 0;
}

set<string> vertices;
set<string> edges;
map<id, storm::Vertex> graph;

int split_string(const string &text, deque<string> &tokens, const string &_delim)
{
    char *copy = strdup(text.c_str());
    char *delim = strdup(_delim.c_str());
    char *save, *tok;

    if (!copy || !delim)
        return -1;

    tokens.clear();
    tok = strtok_r(copy, delim, &save);
    while (tok) {
        string s(tok);
        tokens.push_back(s);
        tok = strtok_r(nullptr, delim, &save);
    }

    free(copy);
    free(delim);
    return 0;
}

// feats is features for a single vertex
int handle_feat(string &feats, deque<string> featidx)
{
    deque<string> tokens;
    if (split_string(feats, tokens, string(" ")))
        return -1;

    const string &id(tokens.at(0));
    storm::Vertex *v(&graph[id]);
    v->set_key(id);
    for (size_t tokidx = 1; tokidx < tokens.size(); tokidx++) {
        size_t idx = tokidx - 1; // first col is vertex ID, not a feature
        if (0 == atoi(tokens.at(idx).c_str())) // feat not used
            continue;
        deque<string> featinfo; // [0] is name [1] is value
        if (split_string(featidx.at(idx), featinfo, string(":")))
            return -1;
        if (featinfo.size() != 2)
            continue;
        const string &item(featinfo.at(0));
        const string  &val(featinfo.at(1));
        if (item == "gender") {
            v->set_gender(atoi(val.c_str()));
        } else if (item == "institution") {
            v->add_inst(val);
        } else if (item == "university") {
            v->add_univ(val);
        } else if (item == "last_name") {
            v->add_name(val);
        } else if (item == "place") {
            v->add_place(val);
        } else if (item == "job_title") {
            v->add_jobtitle(val);
        } else {
            cerr << "unknown feature name: '" << item << "'" << endl;
            return -1;
        }
    }
    //cout << "------------------------------------" << endl;
    //v->PrintDebugString();
    return 0;
}

// once called, egoids, edges are created
// graph has entries for all vertices; now fill in rest
int handle_ego(egoid_t &egoid)
{
    deque<string> feats, featidx, followers;

    string fname(egoid.path + ".featnames");
    if (read_lines(fname, featidx))
        return -1;
    // fix it up.. to only have name:val
    for (string &entry : featidx) {
        entry = entry.substr(entry.find(" ") + 1);
        //cout << "    '" << entry << "'" << endl;
    }

    fname = egoid.path + ".egofeat";
    if (read_lines(fname, feats))
        return -1;
    assert(feats.size() > 0);
    feats.at(0) = egoid.id + " " + feats.at(0); // egofeat doesn't have id in col 1

    // append the rest (they have id in first col)
    fname = egoid.path + ".feat";
    if (read_lines(fname, feats))
        return -1;

    for (string &feat : feats)
        if (handle_feat(feat, featidx))
            return -1;

    fname = egoid.path + ".followers";
    if (read_lines(fname, followers))
        return -1;
    for (auto &f : followers)
        graph.at(egoid.id).add_followers(f);

    return 0;
}

// search for files in egonet (SNAP data)
// file should just be a list of ego IDs
//      e.g. /path/to/files/egoid
int load_graph(string &path)
{
    memcached_return_t mret;
    string line;

    deque<string> entries;
    if (read_lines(path, entries))
        return -1;
    for (auto &entry : entries) {
        EgoID ego(entry);
        egoids.push_back(ego);
    }

    // collect all edges among all files
    cout << "Parsing edges... (" << egoids.size()
        << " egos)" << endl;
    for (EgoID &egoid : egoids) {
        string fname(egoid.path + ".edges");
        cout << "    -> " << egoid.id; cout.flush(); fflush(stdout);
        deque<string> _edges;
        if (read_lines(fname, _edges))
            return -1;
        cout << " " << _edges.size() << " edges" << endl;
        for (string &edge : _edges) {
            edges.insert(edge);
            // egoid also follows all nodes in file
            auto idx = edge.find(" ");
            string v1 = edge.substr(0, idx), v2 = edge.substr(idx + 1);
            string egolink = egoid.id + " " + v1; 
            edges.insert(egolink);
            egolink = egoid.id + " " + v2; 
            edges.insert(egolink);

            vertices.insert(v1);
            vertices.insert(v2);
            vertices.insert(egoid.id);
        }
    }
    cout << "Initializing edges... (" << edges.size()
        << " edges)" << endl;
    for (auto &edge : edges) {
        auto idx = edge.find(" ");
        string n1 = edge.substr(0, idx), n2 = edge.substr(idx + 1);
        //cout << "    " << n1 << " " << n2 << endl;

        storm::Vertex *v1 = &graph[n1], *v2 = &graph[n2];
        v1->set_key(n1);
        v1->add_following(n2);
        v2->set_key(n2);
        v2->add_followers(n1);
    }

    cout << "Parsing remaining..." << endl;
    for (EgoID &egoid : egoids) {
        if (handle_ego(egoid))
            return -1;
    }

    for (string id : vertices) {
        set<string> uniq;
        storm::Vertex *v = &graph.at(id);

        // remove duplicates from following
        for (int f = 0; f < v->following_size(); f++)
            uniq.insert(v->following(f));
        v->clear_following();
        for (auto &f : uniq)
            v->add_following(f);

        // and from followers
        uniq.clear();
        for (int f = 0; f < v->followers_size(); f++)
            uniq.insert(v->followers(f));
        v->clear_followers();
        for (auto &f : uniq)
            v->add_followers(f);
    }

    // inject all into object store
    cout << "Injecting to object store... (" << graph.size()
        << " nodes)" << endl;
    void *buf(nullptr);
    int buflen(0), len(0);
    for (auto &g : graph) {
        storm::Vertex *v = &g.second;
        len = v->ByteSize();
        if (len > buflen) {
            buflen = len;
            buf = realloc(buf, buflen);
            if (!buf)
                return -1;
        }
        v->SerializeToArray(buf, buflen);

        //cout << "    -> " << len << " bytes" << endl;
        mret = memcached_set(memc, v->key().c_str(), v->key().length(),
                (char*)buf, len, 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }
    }

    return 0;
}

int init_memc(void)
{
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc)
        return -1;
    return 0;
}

int
main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s graphlist imagelist config\n", *argv);
        return 1;
    }
    string g(argv[1]);
    string i(argv[2]);
    string c(argv[3]);

    if (init_config(c))
        return 1;

    if (init_memc())
        return 1;

    //if (load_images(i))
        //return 1;

    if (load_graph(g))
        return 1;

    // cout << "vertices: " << vertices.size() << endl;
    // cout << "edges:    " << edges.size() << endl;

    return 0;
}

