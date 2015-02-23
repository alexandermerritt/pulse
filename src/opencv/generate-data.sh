#! /usr/bin/env bash
set -e
set -u

exec=load_egonet
make DEBUG=no $exec

# small dataset
./$exec proto ./inputs/ego_gplus_paths-small.in ./inputs/flickr_social_paths-small.in
mv -v graph.pb inputs/graph-small.pb
mv -v imagelist.pb inputs/imagelist-small.pb

# TODO other datasets

