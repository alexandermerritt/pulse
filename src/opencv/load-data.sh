#! /usr/bin/env bash
set -e
set -u

CONF=pulse.conf

[[ 1 -ne $# ]] && echo "Usage: $0 dataset" && exit 1

exec=./load_egonet

if [[ "$1" == "small" ]]; then
    $exec load ./inputs/graph-small.pb ./inputs/imagelist-small.pb $CONF
fi

# TODO other datasets

