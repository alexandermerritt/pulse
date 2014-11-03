#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include <iostream>
#include <typeinfo>

#include <__cudaFatFormat.h>
#include <driver_types.h>
#include <vector_types.h>
#include "precudart.h"

#include <execinfo.h>

//==----------------------------------------------------------------------------
//  Defines
//==----------------------------------------------------------------------------

#define PREFIX "ctrace "

#define NEXT(f) reinterpret_cast<decltype(f)>(dlsym(RTLD_NEXT, __func__))
#define CUDA(f,name) \
    decltype(name) *f; \
    if (!(f = NEXT(f))) abort()

#define DIM3(d) d.x, d.y, d.z

//==----------------------------------------------------------------------------
//  Globals
//==----------------------------------------------------------------------------

// if false: cudaLaunch parameter is function address
// else: parameter could also be function name (string)
static bool has_registered_funcs = false;

// if false: any symbol params are addresses
// else: parameter could also be symbol name (string)
static bool has_registered_vars = false;

static FILE *tracefp;

//==----------------------------------------------------------------------------
//  Library
//==----------------------------------------------------------------------------

#include <sstream>

static void dump_maps(void)
{
    char line[64];
    std::stringstream ss;

    FILE *fp = fopen("/proc/self/maps", "r");
    if (!fp)
        return;

    ss << "maps." << getpid();
    std::string path(ss.str());
    FILE *fpout = fopen(path.c_str(), "w");
    if (!fpout) {
        fclose(fp);
        return;
    }

    while ((fgets(line, 64, fp)))
        fprintf(fpout, "%s", line);

    fclose(fp);
    fclose(fpout);
}

__attribute__((constructor))
void open_file(void)
{
    tracefp = stdout;
    atexit(dump_maps);
}

__attribute__((destructor))
void close_file(void)
{
    if (tracefp && tracefp != stdout)
        fclose(tracefp);
}

//==----------------------------------------------------------------------------
//  Traception
//==----------------------------------------------------------------------------

#include <map>
static std::map<uintptr_t, size_t> regions;

static size_t memused(void)
{
    size_t mem = 0UL;
    for (auto v : regions)
        mem += v.second;
    return mem;
}

cudaError_t cudaDeviceSynchronize(void)
{
    CUDA(f, cudaDeviceSynchronize);
    fprintf(tracefp, PREFIX "%s()\n", __func__);
    return f();
}

cudaError_t cudaFree(void * devPtr)
{
    CUDA(f,cudaFree);
    fprintf(tracefp, PREFIX "%s(%p)\n", __func__, devPtr);
    regions.erase((uintptr_t)devPtr);
    return f(devPtr);
}

cudaError_t cudaFreeHost(void * ptr)
{
    CUDA(f,cudaFreeHost);
    fprintf(tracefp, PREFIX "%s(%p)\n", __func__, ptr);
    return f(ptr);
}

cudaError_t cudaConfigureCall(dim3 gridDim, dim3 blockDim, size_t sharedMem,
        cudaStream_t stream)
{
    CUDA(f,cudaConfigureCall);
    fprintf(tracefp, PREFIX "%s({%u,%u,%u}, {%u,%u,%u}, %lu, -)\n",
            __func__, DIM3(gridDim), DIM3(blockDim),
            sharedMem);
    return f(gridDim,blockDim,sharedMem,stream);
}

cudaError_t cudaSetupArgument(const void * arg, size_t size, size_t offset)
{
    CUDA(f,cudaSetupArgument);
    fprintf(tracefp, PREFIX "%s(%p,%lu,%lu)\n", __func__, arg, size, offset);
    //char *p = (char*)arg;
    //while (size-- > 0)
        //fprintf(tracefp, " %x", *p++);
    return f(arg, size, offset);
}

cudaError_t cudaLaunch(const void *func)
{
    CUDA(f,cudaLaunch);
    fprintf(tracefp, PREFIX "%s(%p) mem=%lu\n", __func__, func, memused());
    return f(func);
}

cudaError_t cudaMallocPitch(void ** devPtr, size_t * pitch, size_t width,
        size_t height)
{
    CUDA(f,cudaMallocPitch);
    cudaError_t cerr;
    cerr = f(devPtr, pitch, width, height);
    if (cerr == cudaSuccess) {
        fprintf(tracefp, PREFIX "%s(%p[%p], %p[%lu], %lu, %lu)\n",
                __func__, devPtr, *devPtr, pitch, *pitch, width, height);
        regions.insert(std::make_pair((uintptr_t)*devPtr, *pitch * height));
    }
    return cerr;
}

cudaError_t cudaMemcpy2D(void * dst, size_t dpitch, const void * src, size_t
        spitch, size_t width, size_t height, enum cudaMemcpyKind kind)
{
    CUDA(f,cudaMemcpy2D);
    fprintf(tracefp, PREFIX "%s(%p, %lu, %p, %lu, %lu, %lu, %d)\n", __func__,
            dst, dpitch, src, spitch, width, height, kind);
    return f(dst, dpitch, src, spitch, width, height, kind);
}

cudaError_t cudaMemcpy2DAsync(void * dst, size_t dpitch, const void * src,
        size_t spitch, size_t width, size_t height, enum cudaMemcpyKind kind,
        cudaStream_t stream)
{
    CUDA(f,cudaMemcpy2DAsync);
    fprintf(tracefp, PREFIX "%s(%p, %lu, %p, %lu, %lu, %lu, %d, -)\n", __func__,
            dst, dpitch, src, spitch, width, height, kind);
    return f(dst, dpitch, src, spitch, width, height, kind, stream);
}

cudaError_t cudaMemcpy2DToArray(cudaArray_t dst, size_t wOffset, size_t hOffset,
        const void * src, size_t spitch, size_t width, size_t height, enum
        cudaMemcpyKind kind)
{
    CUDA(f,cudaMemcpy2DToArray);
    fprintf(tracefp, PREFIX "%s(-, %lu, %lu, %p, %lu, %lu, %lu, %d)\n", __func__,
            wOffset, hOffset, src, spitch, width, height, kind);
    return f(dst, wOffset, hOffset, src, spitch, width, height, kind);
}

cudaError_t cudaMemcpy2DToArrayAsync(cudaArray_t dst, size_t wOffset, size_t
        hOffset, const void * src, size_t spitch, size_t width, size_t height,
        enum cudaMemcpyKind kind, cudaStream_t stream)
{
    CUDA(f,cudaMemcpy2DToArrayAsync);
    fprintf(tracefp, PREFIX "%s(-, %lu, %lu, %p, %lu, %lu, %lu, %d, -)\n", __func__,
            wOffset, hOffset, src, spitch, width, height, kind);
    return f(dst, wOffset, hOffset, src, spitch, width, height, kind, stream);
}

cudaError_t cudaMemcpy2DFromArray(void * dst, size_t dpitch, cudaArray_const_t
        src, size_t wOffset, size_t hOffset, size_t width, size_t height, enum
        cudaMemcpyKind kind)
{
    CUDA(f,cudaMemcpy2DFromArray);
    fprintf(tracefp, PREFIX "%s(%p, %lu, -, %lu, %lu, %lu, %lu, %d)\n", __func__,
            dst, dpitch, wOffset, hOffset, width, height, kind);
    return f(dst, dpitch, src, wOffset, hOffset, width, height, kind);
}

cudaError_t cudaMemcpy2DFromArrayAsync(void * dst, size_t dpitch,
        cudaArray_const_t src, size_t wOffset, size_t hOffset, size_t width,
        size_t height, enum cudaMemcpyKind kind, cudaStream_t stream)
{
    CUDA(f,cudaMemcpy2DFromArrayAsync);
    fprintf(tracefp, PREFIX "%s(%p, %lu, -, %lu, %lu, %lu, %lu, %d, -)\n", __func__,
            dst, dpitch, wOffset, hOffset, width, height, kind);
    return f(dst, dpitch, src, wOffset, hOffset, width, height, kind, stream);
}

cudaError_t cudaMemcpy2DArrayToArray(cudaArray_t dst, size_t wOffsetDst, size_t
        hOffsetDst, cudaArray_const_t src, size_t wOffsetSrc, size_t hOffsetSrc,
        size_t width, size_t height, enum cudaMemcpyKind kind)
{
    CUDA(f,cudaMemcpy2DArrayToArray);
    fprintf(tracefp, PREFIX "%s(-, %lu, %lu, -, %lu, %lu, %lu, %lu, %d)\n", __func__,
            wOffsetDst, hOffsetDst, wOffsetSrc, hOffsetSrc, width, height, kind);
    return f(dst, wOffsetDst, hOffsetDst, src, wOffsetSrc, hOffsetSrc, width,
            height, kind);
}

cudaError_t cudaMemcpyToSymbol(const void * symbol, const void * src, size_t
        count, size_t offset, enum cudaMemcpyKind kind)
{
    CUDA(f,cudaMemcpyToSymbol);
    fprintf(tracefp, PREFIX "%s(%p, %p, %lu, %lu, %d)\n", __func__,
            symbol, src, count, offset, kind);
    return f(symbol, src, count, offset, kind);
}

cudaError_t cudaMemcpyToSymbolAsync(const void * symbol, const void * src,
        size_t count, size_t offset, enum cudaMemcpyKind kind, cudaStream_t
        stream)
{
    CUDA(f,cudaMemcpyToSymbolAsync);
    fprintf(tracefp, PREFIX "%s(%p, %p, %lu, %lu, %d, -)\n", __func__,
            symbol, src, count, offset, kind);
    return f(symbol, src, count, offset, kind, stream);
}

void __cudaRegisterFunction(void ** fatCubinHandle, const char * hostFun,
        char * deviceFun, const char * deviceName, int thread_limit, uint3 *
        tid, uint3 * bid, dim3 * bDim, dim3 * gDim, int * wSize)
{
    has_registered_funcs = true;
    abort();
}

void __cudaRegisterVar(void ** fatCubinHandle, char * hostVar, char *
        deviceAddress, const char * deviceName, int ext, int size, int constant,
        int global)
{
    has_registered_vars = true;
    abort();
}

#if 0
cudaError_t cudaUnbindTexture(const struct textureReference * texref)
{
}

void __cudaUnregisterFatBinary(void ** fatCubinHandle)
{
}

cudaError_t cudaFreeArray(cudaArray_t array)
{
    CUDA(f,cudaFreeArray);
    return f(array);
}

cudaError_t cudaFreeMipmappedArray(cudaMipmappedArray_t mipmappedArray)
{
    CUDA(f,cudaFreeMipmappedArray);
    return f(mipmappedArray);
}

cudaError_t cudaFuncSetCacheConfig(const void * func, enum cudaFuncCache
        cacheConfig)
{
}

cudaError_t cudaGetLastError(void)
{
}

cudaError_t cudaBindTexture2D(size_t * offset, const struct textureReference *
        texref, const void * devPtr, const struct cudaChannelFormatDesc * desc,
        size_t width, size_t height, size_t pitch)
{
}

struct cudaCnnelFormatDesc cudaCreateChannelDesc(int x, int y, int z, int w,
        enum cudaChannelFormatKind f)
{
}
#endif

