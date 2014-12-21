#! /usr/bin/env bash
# Run locally:
#   ./run_topology.sh
# Run in Storm:
# ./run_topology.sh <topology-name>

set -e
set -u

SOURCE=/usr/local/src/storm/latest
STORM=$SOURCE/bin/storm

scons -C ..

./makejar.sh

JARS="$(find $SOURCE/ -type f | egrep '\.jar$' | tr '\n' ':')"

($STORM jar search.jar SearchTopology ../pulse.conf $@ 2>&1 ) | tee storm.log

# java -client -Dstorm.options= \
#     -Dstorm.home=$SOURCE \
#     -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib \
#     -Dstorm.conf.file= -cp $JARS:$SOURCE/conf:$SOURCE/bin:search.jar \
#     -Dstorm.jar=search.jar \
#     SearchTopology \
#     ../pulse.conf $@

grep output storm.log

