#! /usr/bin/env bash
set -e
set -u

exec=./load_egonet

[[ ! -e $exec  ]] && scons -j6

# small dataset
$exec proto ./inputs/ego_gplus_paths-small.in ./inputs/flickr_social_paths-small.in
mv graph.pb inputs/graph-small.pb
mv imagelist.pb inputs/imagelist-small.pb

# TODO other datasets

