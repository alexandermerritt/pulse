#! /usr/bin/env bash
set -e
set -u

CONF=pulse.conf

[[ 1 -ne $# ]] && echo "Usage: $0 dataset" && exit 1

exec=./load_egonet

ID=$1
graph=inputs/graph-$ID.pb
images=inputs/imagelist-$ID.pb
[[ ! -e $graph ]] && \
    echo "Error: file not found: $graph" && exit 1
[[ ! -e $images ]] && \
    echo "Error: file not found: $images" && exit 1
echo $exec load $graph $images $CONF
$exec load $graph $images $CONF

