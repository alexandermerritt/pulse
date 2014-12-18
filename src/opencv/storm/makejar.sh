#! /usr/bin/env bash
set -e
set -u

source /etc/profile.d/java.sh

# add others as necessary, colon-separated
JARS=/usr/local/src/storm/latest/lib/storm-core-0.9.3.jar

[[ ! -e "$JARS" ]] && echo "storm jar not found" && exit 1

killall -s 1 stormstitcher || true

echo Compiling ...
javac -cp $JARS StitcherTopology.java

[[ ! -e ../stormstitcher ]] && \
	echo "Compile stormstitcher first" && exit 1

[[ ! -e ../pulse.conf ]] && \
	echo "../pulse.conf missing" && exit 1

[[ ! -d resources ]] && mkdir resources

cp ../stormstitcher resources/
cp ../pulse.conf resources/

echo Creating jar ...
jar cf stitcher.jar Stitcher*.class resources/

rm -f *.class
echo Done.

