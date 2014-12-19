#! /usr/bin/env bash
set -e
set -u

source /etc/profile.d/java.sh

# add others as necessary, colon-separated
JARS=/usr/local/src/storm/latest/lib/storm-core-0.9.3.jar

[[ ! -e "$JARS" ]] && echo "storm jar not found" && exit 1

killall -s 1 stormfuncs || true

echo Compiling ...
javac -cp $JARS SearchTopology.java

[[ ! -e ../stormfuncs ]] && \
	echo "Compile stormfuncs first" && exit 1

[[ ! -e ../pulse.conf ]] && \
	echo "../pulse.conf missing" && exit 1

[[ ! -d resources ]] && mkdir resources

cp ../stormfuncs resources/
cp ../pulse.conf resources/

echo Creating jars ...
jar cf search.jar Search*.class resources/

rm -f *.class
echo Done.

