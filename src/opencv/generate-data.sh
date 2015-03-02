#! /usr/bin/env bash
set -e
set -u

[[ 1 -ne $# ]] && echo "Usage: $0 dataset" && exit 1

exec=load_egonet
make Objects.pb.cc
make DEBUG=no $exec

size=$1
gplus=inputs/ego_gplus_paths-$size.in
flickr=inputs/flickr_social_paths-$size.in
[[ ! -e $gplus ]] && \
    echo "Error: file not found: $gplus" && exit 1
[[ ! -e $flickr ]] && \
    echo "Error: file not found: $flickr" && exit 1
./$exec proto ./inputs/ego_gplus_paths-$size.in ./inputs/flickr_social_paths-$size.in
mv -v graph.pb inputs/graph-$size.pb
mv -v imagelist.pb inputs/imagelist-$size.pb

