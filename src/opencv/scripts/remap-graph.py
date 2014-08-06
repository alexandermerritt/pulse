#! /usr/bin/env python
# Reads in a graph and changes all node IDs to be 0-based and contiguous.
# Input file is two columns representing an edge in the graph: vertexID vertexID
#
# bzcat flickrEdges.txt.gz | ./remap-graph.py > flickr.graph

import sys

d = {}

text = sys.stdin.readlines()

# find all values
for line in text:
    line = line.rstrip()
    if line[0] == "#":
        continue
    (n0, n1) = line.split()
    d[n0] = None
    d[n1] = None

# make reverse map
idx = 0
for k in d.keys():
    d[k] = idx
    idx += 1

for line in text:
    line = line.rstrip()
    if line[0] == "#":
        continue
    (n0,n1) = line.split()
    print(str(d[n0]) + ' ' + str(d[n1]))

