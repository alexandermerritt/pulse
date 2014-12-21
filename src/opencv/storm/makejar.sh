#! /usr/bin/env bash
set -e
set -u

source /etc/profile.d/java.sh

SOURCE=/usr/local/src/storm/latest

# add others as necessary, colon-separated
JARS=$(find $SOURCE/ -type f | grep 'storm-core')

[[ ! -e "$JARS" ]] && echo "storm jar not found" && exit 1

killall -s SIGHUP stormfuncs || true

echo Compiling ...
javac -cp $JARS SearchTopology.java

[[ ! -e ../stormfuncs ]] && \
	echo "Compile stormfuncs first" && exit 1

[[ ! -e ../pulse.conf ]] && \
	echo "../pulse.conf missing" && exit 1

[[ ! -d resources ]] && mkdir -pv resources
cp -v ../stormfuncs resources/
cp -v ../pulse.conf resources/

echo Creating jars ...
jar cvf search.jar Search*.class resources

rm -f *.class
echo Done.

