#! /usr/bin/env bash
# Run locally:
#   ./run_topology.sh
# Run in Storm:
# ./run_topology.sh <topology-name>

set -e
set -u

SOURCE=/usr/local/src/storm/latest

./makejar.sh

JARS="$(find $SOURCE/ -type f | egrep '\.jar$' | tr '\n' ':')"

java -client -Dstorm.options= \
    -Dstorm.home=$SOURCE \
    -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib \
    -Dstorm.conf.file= -cp $JARS:$SOURCE/conf:$SOURCE/bin:search.jar \
    -Dstorm.jar=search.jar \
    SearchTopology \
    ../pulse.conf $@

