#! /usr/bin/env bash
set -e
set -u

CONF=pulse.conf

[[ 1 -ne $# ]] && echo "Usage: $0 dataset" && exit 1

exec=./load_egonet

size=$1
graph=inputs/graph-$size.pb
images=inputs/imagelist-$size.pb
[[ ! -e $graph ]] && \
    echo "Error: file not found: $graph" && exit 1
[[ ! -e $images ]] && \
    echo "Error: file not found: $images" && exit 1
$exec load $graph $images $CONF

