#! /usr/bin/env bash
set -u
ulimit -c unlimited

source /etc/profile.d/java.sh

SOURCE=/usr/local/src/storm/latest
STORM=$SOURCE/bin/storm

set -e
make

# enable JNI lib to be loaded from current path
(LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. \
    $STORM jar search.jar SearchTopology 2>&1 ) \
    | tee storm.log

# JARS="$(find $SOURCE/ -type f | egrep '\.jar$' | tr '\n' ':')"
# java -client -Dstorm.options= \
#     -Dstorm.home=$SOURCE \
#     -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib \
#     -Dstorm.conf.file= -cp $JARS:$SOURCE/conf:$SOURCE/bin:search.jar \
#     -Dstorm.jar=search.jar \
#     SearchTopology \
#     ../pulse.conf $@

