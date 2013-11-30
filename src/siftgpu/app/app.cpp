/* file: app.cpp
 * author: Alexander Merritt merritt.alex@gatech.edu
 *
 * ./app -help
 * ./app -cuda DEVID -il IMAGE_FILE -v VERBOSITY
 */

/* C++ includes */
#include <iostream>

/* SiftGPU includes */
#include <SiftGPU/SiftGPU.h>

static int run(int argc, char *argv[])
{
    int err, ret = 0;
    SiftGPU *sift = NULL;

    sift = new SiftGPU;
    if (!sift)
        return -1;

    sift->ParseParam(argc, argv);

    err = sift->CreateContextGL();
    if (err != SiftGPU::SIFTGPU_FULL_SUPPORTED)
        return -1;

    for (int img = 0; img < sift->GetImageCount(); img++)
        sift->RunSIFT(img);

    delete sift;
    return ret;
}

int main(int argc, char *argv[])
{
    int err;

    err = run(argc, argv);
    if (err)
        return -1;

    return 0;
}
