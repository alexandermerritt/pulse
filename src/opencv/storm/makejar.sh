#! /usr/bin/env bash
set -u

source /etc/profile.d/java.sh

SOURCE=/usr/local/src/storm/latest

# add others as necessary, colon-separated
JARS=$(find $SOURCE/ -type f | egrep 'storm-core')

[[ ! -e "$JARS" ]] && echo "storm jar not found" && exit 1

set -e

FILES="SearchTopology.java Logger.java"

echo Compiling ...
javac -cp $JARS $FILES

[[ ! -e ../stormfuncs ]] && \
	echo "Compile stormfuncs first" && exit 1

[[ ! -e ../pulse.conf ]] && \
	echo "../pulse.conf missing" && exit 1

[[ ! -d resources ]] && mkdir -pv resources
cp -v ../stormfuncs resources/
cp -v ../pulse.conf resources/

echo Creating jars ...
jar cvf search.jar Search*.class Logger*.class resources

rm -f *.class
echo Done.

