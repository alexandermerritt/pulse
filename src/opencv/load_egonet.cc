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
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <opencv2/core/core.hpp>
#include <opencv2/highgui/highgui.hpp>

#include <libmemcached/memcached.h>

#include "Objects.pb.h" // generated
#include "Config.hpp"

#define MP_20   ((unsigned int)(20 * 1e6))
enum {
    IMG_TOO_BIG_THRESH = MP_20,
    MAX_IMGS_PER_VERTEX = 20,
};

using namespace std;
using namespace google::protobuf::io;

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
deque<storm::Image> images;

typedef EgoID egoid_t;
typedef string id;

memcached_st *memc = NULL;

int readfile(const string &path, void **buf, size_t *len)
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
    string line;
    vector< string > imagelist;

    printf(">> Loading images...\n");

    ifstream ifile;
    ifile.open(path);
    if (!ifile.is_open()) {
        perror("open image file");
        return -1;
    }
    while (getline(ifile, line)) {
        if (line[0] == '#')
            continue;
        imagelist.push_back(line);
    }
    ifile.close();

    string pbname("imagelist.pb");
    cout << "Writing image list to " << pbname << endl;

    int fd = open(pbname.data(), O_WRONLY | O_CREAT,  S_IRUSR | S_IWUSR);
    if (fd < 0) return -1;
    unique_ptr<ZeroCopyOutputStream> raw_output(new FileOutputStream(fd));
    CodedOutputStream coded_output(raw_output.get());

    coded_output.WriteVarint32(imagelist.size());

    void *buf(nullptr);
    size_t buflen(0), len(0);
    for (auto &imgpath : imagelist) {
        auto idx = imgpath.rfind("/");
        string imgname(imgpath);
        if (idx != string::npos)
            imgname = imgpath.substr(idx + 1);
        cout << imgname << endl;

        cv::Mat img;
        img = cv::imread(imgpath, 1);
        if (!img.data) {
            fprintf(stderr, "failed to read %s\n", imgpath.c_str());
            return -1;
        }

        if (img.total() > IMG_TOO_BIG_THRESH) {
            cout << "   skipping; too big" << endl;
            continue;
        }

        storm::Image image;
        string s; // serialized header
        image.set_key_id(imgname);
        image.set_width(img.cols);
        image.set_height(img.rows);
        image.set_depth(img.elemSize());
        image.set_key_data(imgname + "::data");
        image.set_path(imgpath);

        images.push_back(image);

        len = image.ByteSize();
        if (len > buflen)
            if (!(buf = realloc(buf, (buflen = len))))
                return -1;
        coded_output.WriteVarint32(len);
        image.SerializeToArray(buf, buflen);
        coded_output.WriteRaw(buf, len);
    }
    free(buf);
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
    v->set_key_id(id);
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
    deque<string> circles, feats, featidx, followers;

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

    fname = egoid.path + ".circles";
    if (read_lines(fname, circles))
        return -1;
    for (auto &line : circles) {
        deque<string> tokens;
        if (split_string(line, tokens, " "))
            return -1;
        string circname(tokens.at(0));
        size_t i = 1;
        for ( ; i < tokens.size(); i++) {
            storm::Vertex *v = &graph.at(tokens.at(i));
            v->add_circles(circname);
        }
    }

    return 0;
}

// search for files in egonet (SNAP data)
// file should just be a list of ego IDs
//      e.g. /path/to/files/egoid
int load_graph(string &path)
{
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
        v1->set_key_id(n1);
        v1->add_following(n2);
        v2->set_key_id(n2);
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

    // choose images for each vertex
    for (auto &g : graph) {
        storm::Vertex *v = &g.second;
        // arbitrarily but deterministically determine count
        size_t n_to_assign = // XXX require >0 images/vertex
            1 + (v->followers_size() % MAX_IMGS_PER_VERTEX);
        // pick n images at this place
        size_t imgidx = (v->followers_size() + v->following_size() +
                v->gender() * 10 + v->circles_size()) %
                (images.size() - n_to_assign);
        //cout << "    images " << v->key() << ": #" << n_to_assign
            //<< " at " << imgidx << endl;
        while (n_to_assign > 0) {
            v->add_images(images.at(imgidx++).key_id());
            n_to_assign--;
        }
    }

    // -----------------------------------------------

    string pbname("graph.pb");
    cout << "Writing graph data to " << pbname << endl;

    int fd = open(pbname.data(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) return -1;
    unique_ptr<ZeroCopyOutputStream> raw_output(new FileOutputStream(fd));

    {
        CodedOutputStream coded_output(raw_output.get());
        coded_output.WriteVarint32(graph.size());
    }

    void *buf(nullptr);
    size_t buflen(0), len(0);
    for (auto &g : graph) {
        // Must destroy and recreate CodedInputStream with each Protobuf...
        // https://groups.google.com/forum/#!topic/protobuf/ZKB5EePJDNU
        CodedOutputStream coded_output(raw_output.get());
        storm::Vertex *v = &g.second;
        len = v->ByteSize();
        if (len > buflen)
            if (!(buf = realloc(buf, (buflen = len))))
                return -1;
        coded_output.WriteVarint32(len);
        v->SerializeToCodedStream(&coded_output);
    }
    free(buf);
    raw_output.release();
    close(fd);

    return 0;
}

// write out the IDs of all vertices so someone can query them directly...
// putting into one protobuf would cause the PB lib to complain the PB is
// 'too large'
void
write_graphids_file(std::string &path, deque<string> &nodes)
{
    //string idlist("graph-ids.txt");
    cout << "Writing graph ids to " << path << endl;
    FILE *fp = fopen(path.data(), "w");
    if (!fp) {
        std::cerr << "Error writing graph-ids file" << std::endl;
        return;
    }
    for (auto &n : nodes)
        fprintf(fp, "%s\n", n.data());
    fclose(fp);

}

int init_memc(void)
{
    memc = memcached(config->memc.servers.c_str(),
            config->memc.servers.length());
    if (!memc)
        return -1;
    return 0;
}

// load all input files into protobuf files
// save them to disk as snapshot
int
make_proto(string &egolist, string &images)
{
    if (load_images(images))
        return 1;

    if (load_graph(egolist))
        return 1;

    return 0;
}

// load previously created protobuf files
// insert them into object store
int
load_proto(string &graph_path, string &imagelist, string &config)
{
    if (init_config(config))
        return 1;

    if (init_memc())
        return 1;

    // -------------------------------------------------
    cout << "Loading graph data into object store..." << endl;

    int fd = open(graph_path.data(), O_RDONLY);
    if (fd < 0) return -1;
    unique_ptr<ZeroCopyInputStream> raw_input(new FileInputStream(fd));

    unsigned int count;
    {
        CodedInputStream coded_input(raw_input.get());
        coded_input.ReadVarint32(&count);
    }

    void *buf(nullptr);
    size_t buflen(0);
    unsigned int len;
    deque<string> nodes; // used for writing graph-ids file

    cout << count << " nodes to read" << endl;

    while (count-- > 0) {
        // Must destroy and recreate CodedInputStream with each Protobuf...
        // https://groups.google.com/forum/#!topic/protobuf/ZKB5EePJDNU
        CodedInputStream coded_input(raw_input.get());

        coded_input.ReadVarint32(&len);
        if (buflen < len)
            if (!(buf = realloc(buf, (buflen = len))))
                return -1;
        // when reading, don't give protobuf the whole stream, as it will assume
        // the entire stream belongs to a single message
        coded_input.ReadRaw(buf, len);
        storm::Vertex vertex;
        if (!vertex.ParseFromArray(buf, len)) {
            // a bug in the file or in the generation code
            std::cerr << "protobuf could not parse object" << std::endl;
        } else {
            nodes.push_back(vertex.key_id().data());

            // avoid duplicates.. could happen with bugs in the snapshot
            // protobuf file
            if (memc_exists(memc, vertex.key_id()))
                continue;

            auto mret = memcached_set(memc, vertex.key_id().c_str(),
                    vertex.key_id().length(), (char*)buf, len, 0, 0);
            if (mret != MEMCACHED_SUCCESS) {
                fprintf(stderr, "memc error: %s",
                        memcached_strerror(memc, mret));
                return -1;
            }
        }
    }
    raw_input.release();
    close(fd);

    string p("graph-ids.txt");
    write_graphids_file(p, nodes);

    // -------------------------------------------------
    cout << "Loading image data into object store..." << endl;

    fd = open(imagelist.data(), O_RDONLY);
    if (fd < 0) return -1;
    raw_input.reset(new FileInputStream(fd));

    {
        CodedInputStream coded_input(raw_input.get());
        coded_input.ReadVarint32(&count);
    }

    // reuse buf and buflen from above

    while (count-- > 0) {
        // Must destroy and recreate CodedInputStream with each Protobuf...
        // https://groups.google.com/forum/#!topic/protobuf/ZKB5EePJDNU
        CodedInputStream coded_input(raw_input.get());

        coded_input.ReadVarint32(&len);
        if (buflen < len) {
            buflen = len;
            if (!(buf = realloc(buf, buflen))) return -1;
        }
        coded_input.ReadRaw(buf, len);
        storm::Image image;
        image.ParseFromArray(buf, len);

        auto mret = memcached_set(memc, image.key_id().c_str(),
                image.key_id().length(), (char*)buf, len, 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }

        // now load the image from disk
        void *imgbuf(nullptr);
        size_t imglen(0);
        if (0 > readfile(image.path(), &imgbuf, &imglen))
            return -1;
        mret = memcached_set(memc, image.key_data().c_str(),
                image.key_data().length(), (char*)imgbuf, imglen, 0, 0);
        if (mret != MEMCACHED_SUCCESS) {
            fprintf(stderr, "memc error: %s", memcached_strerror(memc, mret));
            return -1;
        }
        free(imgbuf);
    }
    close(fd);

    return 0;
}

void usage(void)
{
    cerr << "Usage: cmd opts*" << endl;
    cerr << "       proto egolist imagelist" << endl;
    cerr << "       load graph.pb imagelist.pb conf" << endl;
}

int
main(int argc, char *argv[])
{
    int ret(0);

    if (argc < 2) {
        usage();
        return -1;
    }

    string cmd(argv[1]);
    if (cmd == "proto") {
        if (argc != 4) {
            usage();
            return -1;
        }
        // file with list of path to ego IDs
        // e.g. ../stanford-snap/gplus/23534534514534
        //      ../stanford-snap/gplus/09348573498842
        string egolist(argv[2]);
        // file with list of paths to JPG images
        // e.g. ../images/23534534514534.jpg
        string images(argv[3]);
        ret = make_proto(egolist, images);
    } else if (cmd == "load") {
        if (argc != 5) {
            usage();
            return -1;
        }
        // path to file holding Vertex protobuf objects
        string graph(argv[2]);
        // path to file holding ImageList protobuf object
        string images(argv[3]);
        // path to config file
        string config(argv[4]);
        ret = load_proto(graph, images, config);

        cout << endl << "\tDon't forget to copy the graph ids file " << endl
            << "\t\tinto the storm resources/ directory before" << endl
            << "\t\tcreating the jar" << endl;
    } else {
        usage();
        ret = -1;
    }

    return ret;
}

