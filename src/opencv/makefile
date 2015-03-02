all: search.jar load_egonet

allall:	all StormFuncsTest LinkerTest.class JNILinker.h

HOST = $(shell hostname | cut -c 1-3)

NFSDIR = /opt/ifrit-nfs/cercs-kid

ifeq ($(HOST),kid)
CC = gcc-4.8
CXX = g++-4.8
else
CC = clang
CXX = clang++
endif

NV_CPATH = -I/usr/local/cuda/include
NV_LIBS = -L/usr/local/cuda/lib64

OPENCV_LIBS = $(shell pkg-config --libs opencv)
OPENCV_CAPTH = $(shell pkg-config --cflags opencv)

PROTOBUF_LIBS = $(shell pkg-config --libs protobuf)
PROTOBUF_CPATH = $(shell pkg-config --cflags protobuf)

ifeq ($(HOST),kid)
STORM_PATH = $(NFSDIR)/src/storm/latest
JAVA_PATH = $(NFSDIR)/src/java/latest
else
STORM_PATH = /usr/local/src/storm/latest
JAVA_PATH = /opt/java/jdk
endif

JAVA_CPATH = -I$(JAVA_PATH)/include -I$(JAVA_PATH)/include/linux

STORM_JAR := $(shell find $(STORM_PATH)/ -type f | egrep 'storm-core')

CPATH = -I/usr/local/include -I../include -I.
CPATH += $(OPENCV_CAPTH) $(PROTOBUF_CPATH) $(NV_CPATH)
CPATH += $(JAVA_CPATH)

ifeq ($(HOST),kid)
CPATH += -I$(NFSDIR)/local/include
endif

# update LD_LIBRARY_PATH to point to the libdir where this is
JPEG_LIBS = -ljpeg

LIBS = -L$(NFSDIR)/local/lib64 -L$(NFSDIR)/local/lib
LIBS += -lmemcached -lpthread $(JPEG_LIBS)
LIBS += $(OPENCV_LIBS) $(PROTOBUF_LIBS) $(NV_LIBS)

EXTRAFLAGS = -Wall -Wextra 
EXTRAFLAGS += -Wno-unused-function -Wno-unused-parameter -Wno-unused-variable
ifeq ($(CC),clang)
EXTRAFLAGS += -fcolor-diagnostics
endif

DEBUG ?= 1
ifeq ($(DEBUG),1)
EXTRAFLAGS += -O0 -ggdb3 -fno-inline
else
EXTRAFLAGS += -Os -mmmx  -msse  -msse2 -msse3 -msse4.1 -msse4.2
endif

CFLAGS = $(EXTRAFLAGS) $(CPATH)
CXXFLAGS = -std=c++11 $(EXTRAFLAGS) $(CPATH)

#
# Our java sources for use in Storm
#

JAVA_SOURCES = SearchTopology.java Logger.java JNILinker.java JNIException.java

.jar: $(JAVA_SOURCES)
	javac -cp $(STORM_JAR) $^
	touch .jar

graph-ids.txt:	load_egonet
	echo "Run load_egonet using the load-data.sh script"
	echo "to generate the graph-ids.txt file, before"
	echo "we can create the search.jar file"

CLASSES = *.class
search.jar: libjnilinker.so .jar graph-ids.txt
	cp -vf libjnilinker.so resources/
	cp -vf graph-ids.txt resources/
	cp -vf pulse.conf resources/
	jar cvf search.jar $(CLASSES) $< resources/

#
# Protobuf files
#

%.pb.h %.pb.cc: %.proto
	protoc --cpp_out=. $<

%.pb.o:	%.pb.cc %.pb.h
	$(CXX) $(CXXFLAGS) -o $@ -c $<

#
# OpenCV source code integrated directly into our library.
#

CV_SOURCES = $(wildcard cv/*.cpp)
CV_OBJ = $(patsubst %.cpp,%.o,$(CV_SOURCES))

CV_COMMON = cv/decoders.h cv/precomp.hpp

cv/%.o:	cv/%.cpp cv/%.hpp $(CV_COMMON)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

cv/libcv.a:	$(CV_OBJ)
	ar rcus $@ $^

# 
# Shared library for JNI invocation
#

JNILinker.h:	JNILinker.java
	javah -jni JNILinker
	touch $@

LIB_SOURCES = StormFuncs.cpp JNILinker.cc matchers.cpp

libjnilinker.so: cv/libcv.a Objects.pb.cc JNILinker.h $(LIB_SOURCES)
	$(CXX) $(CXXFLAGS) --shared -fPIC $(CPATH) -o $@ \
		Objects.pb.cc $(LIB_SOURCES) $(CV_SOURCES) $(LIBS)

#
# Test codes
#

LinkerTest.class:	LinkerTest.java JNILinker.java JNIException.java
	javac $^

# make this directly if wanted
CWD = $(shell pwd)
LinkerTest:	LinkerTest.class libjnilinker.so cv/libcv.a
	java -Djava.library.path=$(CWD) LinkerTest

StormFuncsTest:	Objects.pb.cc StormFuncsTest.cc StormFuncs.cpp cv/libcv.a matchers.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

#
# Utilities for loading data into object store
#

load_egonet: load_egonet.o Objects.pb.cc Config.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

memctest:	memctest.o Objects.pb.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

#
# Misc
#

read:	read.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

clean:
	rm -fv .jar
	rm -f *.class *.so *.o *.pb.cc *.pb.h JNILinker.h search.jar
	rm -fv cv/*.o cv/*.a
	rm -fv /tmp/*.log
	rm -fv load_egonet StormFuncsTest
	$(shell cd /tmp/; ls | egrep '^[0-9a-f]{8}-' | xargs rm -rf)

.PHONY: all clean

