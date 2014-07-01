#! /usr/bin/env bash
set -e
set -u
# add others as necessary, colon-separated
JARS=/home/merrital/local/storm/lib/storm-core-0.9.1-incubating.jar
echo Compiling ...
javac -cp $JARS StitcherTopology.java
echo Creating jar ...
jar cf stitcher.jar Stitcher*.class
rm -f *.class
echo Done.
