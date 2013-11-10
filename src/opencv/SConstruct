#! /usr/bin/env python

import os

cc = os.environ.get('CC', 'gcc-4.7')
cxx = os.environ.get('CXX', 'g++-4.7')

cuda_root = os.environ.get('CUDA_ROOT', '/usr/local/cuda')

ccflags = ['-Wall', '-Wextra', '-Werror']
ccflags.extend(['-Wno-unused-function', '-Wno-unused-parameter', '-Wno-unused-variable'])
ccflags.extend(['-std=c++11']) #, '-fopenmp'])
cpath = ['/usr/local/include']
libpath = []
libs = ['opencv_core', 'opencv_gpu', 'opencv_stitching', 'gomp']

if ARGUMENTS.get('debug', 0):
    ccflags.extend(['-ggdb', '-O0'])
else:
    ccflags.append('-O3')

if cxx == 'clang++':
    ccflags.append('-fcolor-diagnostics')

env = Environment(CC = cc, CXX = cxx)
env.Append(CCFLAGS = ccflags)
env.Append(CPPPATH = cpath)
env.Append(LIBPATH = libpath)
env.Append(LIBS = libs)

sources = ['io.cpp', 'stitcher.cpp', 'main.cpp']

env.Program('stitcher', sources)

