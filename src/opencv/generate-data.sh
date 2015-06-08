#! /usr/bin/env bash
set -e ; set -u

# Use this script to convert the SNAP graph data into protobuf objects
# for insertion into a KVS. It will generate a large protobuf blob -
# basically a serialized array of protobuf objects.

[[ 1 -ne $# ]] && echo "Usage: $0 dataset-ID" && exit 1

# Program used to do the conversion.
exec=load_egonet

# Build the protobuf files to perform serialization.
make Objects.pb.cc
make DEBUG=no $exec

ID=$1
gplus=inputs/ego_gplus_paths-$ID.in
flickr=inputs/flickr_social_paths-$ID.in
[[ ! -e $gplus ]] && \
    echo "Error: file not found: $gplus" && exit 1
[[ ! -e $flickr ]] && \
    echo "Error: file not found: $flickr" && exit 1
./$exec proto ./inputs/ego_gplus_paths-$ID.in ./inputs/flickr_social_paths-$ID.in
mv -v graph.pb inputs/graph-$ID.pb
mv -v imagelist.pb inputs/imagelist-$ID.pb

