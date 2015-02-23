#! /usr/bin/env bash
set -u
ulimit -c unlimited

[[ $# -ne 1 ]] && \
    echo "Specify 'local' or 'storm'" && \
    exit 1

if [[ -e /etc/profile.d/java.sh ]]; then
    source /etc/profile.d/java.sh
fi

SOURCE=''
HOST=$(hostname | cut -c 1-3)
if [[ kid = $HOST ]]; then
    SOURCE=/opt/ifrit-nfs/cercs-kid/src/storm/latest
else
    SOURCE=/usr/local/src/storm/latest
fi
STORM=$SOURCE/bin/storm
if [[ ! -d $SOURCE ]]; then
    echo "Error: $SOURCE not a directory"
    exit 1
fi
if [[ ! -e $STORM ]]; then
    echo "Error: $STORM not found"
    exit 1
fi

set -e
make

CLASSNAME="SearchTopology"

if [[ "$1" == "local" ]]; then
    # enable JNI lib to be loaded from current path
    (LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. \
        $STORM jar search.jar $CLASSNAME 2>&1 ) \
        | tee storm.log
elif [[ "$1" == "storm" ]]; then
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

