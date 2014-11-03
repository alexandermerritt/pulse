#! /usr/bin/env python

# LD_PRELOAD=lib.so ./feature <args> > preload.out 2>&1
# pull out the address provided to cudaLaunch and the memory reported
# (they are both on the same line)

import os
import sys

print('func gpumem')

line = sys.stdin.readline().rstrip()
while line:
    if 'cudaLaunch' in line:
        fields = line.split()
        # convert to int so script fails if NaN
        addr = int(fields[1].split('(')[1].rstrip(')'), base=16)
        mem = int(fields[2].split('=')[1])
        print(str(addr) + ' ' + str(mem))
    line = sys.stdin.readline().rstrip()

