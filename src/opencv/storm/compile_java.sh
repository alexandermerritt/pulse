#! /usr/bin/env bash
set -e
set -u

# add others as necessary, colon-separated
JARS=/home/merrital/local/storm/lib/storm-core-0.9.1-incubating.jar

killall -s 1 stormstitcher || true

echo Compiling ...
javac -cp $JARS StitcherTopology.java

[ ! -e ../stormstitcher ] && \
	echo "Compile stormstitcher first" && exit 1
[ ! -d resources ] && mkdir resources
#cp ../stormstitcher resources/
sudo cp ../stormstitcher /usr/local/bin/

echo Creating jar ...
jar cf stitcher.jar Stitcher*.class resources
rm -f *.class
echo Done.
