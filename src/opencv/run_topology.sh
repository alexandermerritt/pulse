#! /usr/bin/env bash
set -u
ulimit -c unlimited

[[ $# -ne 1 ]] && \
    echo "Specify 'local' or 'storm'" && \
    exit 1

source /etc/profile.d/java.sh

SOURCE=/usr/local/src/storm/latest
STORM=$SOURCE/bin/storm

set -e
make

CLASSNAME="SearchTopology"
#CLASSNAME="TestTopology"

if [[ "$1" == "local" ]]; then
# enable JNI lib to be loaded from current path
(LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. \
    $STORM jar search.jar $CLASSNAME 2>&1 ) \
    | tee storm.log
elif [[ "$1" == "storm" ]]; then
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. \
    $STORM jar search.jar $CLASSNAME search
fi

# JARS="$(find $SOURCE/ -type f | egrep '\.jar$' | tr '\n' ':')"
# java -client -Dstorm.options= \
#     -Dstorm.home=$SOURCE \
#     -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib \
#     -Dstorm.conf.file= -cp $JARS:$SOURCE/conf:$SOURCE/bin:search.jar \
#     -Dstorm.jar=search.jar \
#     SearchTopology \
#     ../pulse.conf $@

