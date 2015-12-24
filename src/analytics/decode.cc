#include <iostream>
#include <exception>

#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "jpeglib.h"
//#include <jpeglib.h>
//#include <turbojpeg.h>

using namespace std;

static inline long
diff(struct timespec c1, struct timespec c2)
{
    return (c2.tv_sec * 1e9 + c2.tv_nsec)
        - (c1.tv_sec * 1e9 + c1.tv_nsec);
}

// output dimensions known only after start_decompress
#define get_outlen(cinfo) \
    (cinfo.output_width * cinfo.output_height * cinfo.num_components)
#define get_rowlen(cinfo) \
    (cinfo.output_width * cinfo.num_components)

// reads from stdin
int decode(void)
{
    struct jpeg_decompress_struct cinfo;
    struct jpeg_error_mgr jerr;
    struct timespec c1,c2;
    struct stat statinfo;
    off_t maplen, outlen;
    JSAMPROW rowp[1];

    cout << "file width height pix_len outbuf_len decode_usec" << endl;

    string line;
    while (cin >> line) {
        // size of image
        if (stat(line.data(), &statinfo))
            throw runtime_error(strerror(errno));
        maplen = statinfo.st_size;

        // map image; populate to exclude disk i/o from measurement
        const int flags = MAP_SHARED | MAP_POPULATE;
        int fd = open(line.data(), O_RDONLY);
        if (fd < 0)
            throw runtime_error(strerror(errno));
        void *imgmap = mmap(NULL, maplen, PROT_READ, flags, fd, 0);
        if (imgmap == MAP_FAILED)
            throw runtime_error(strerror(errno));

        cinfo.err = jpeg_std_error(&jerr);

        jpeg_create_decompress(&cinfo);
        jpeg_mem_src(&cinfo, (unsigned char*)imgmap, maplen);
        jpeg_read_header(&cinfo, false);

        jpeg_start_decompress(&cinfo);

        outlen = get_outlen(cinfo);
        if (outlen < 1)
            throw runtime_error("output buffer length is zero");
        rowp[0] = (JSAMPROW)calloc(1, get_rowlen(cinfo));
        if (!rowp[0])
            throw runtime_error("OOM");
        JSAMPARRAY outbuf = (JSAMPARRAY)calloc(1,outlen);
        if (!outbuf)
            throw runtime_error("OOM");

        clock_gettime(CLOCK_MONOTONIC, &c1);
        while (cinfo.output_scanline < cinfo.image_height) {
            jpeg_read_scanlines(&cinfo, rowp, 1);
        }
        jpeg_finish_decompress(&cinfo);
        clock_gettime(CLOCK_MONOTONIC, &c2);

        cout << line.data();
        cout << " " << cinfo.image_width;
        cout << " " << cinfo.image_height;
        cout << " " << cinfo.num_components;
        cout << " " << outlen;
        cout << " " << diff(c1,c2)/1e3 << endl;
        cout.flush();

        jpeg_destroy_decompress(&cinfo);
        free(rowp[0]); rowp[0] = NULL;
        free(outbuf); outbuf = NULL;
        munmap(imgmap, maplen); imgmap = NULL;
        close(fd); fd = -1;
    }

    return 0;
}

int main(int narg, char *args[])
{
    return decode();
}
