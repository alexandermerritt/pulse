all: search.jar load_egonet

allall:	all StormFuncsTest LinkerTest.class JNILinker.h

CC = clang
CXX = clang++

NV_CPATH = -I/usr/local/cuda/include
NV_LIBS = -L/usr/local/cuda/lib64

OPENCV_LIBS = $(shell pkg-config --libs opencv)
OPENCV_CAPTH = $(shell pkg-config --cflags opencv)

JSON_LIBS = $(shell pkg-config --libs jsoncpp)
JSON_CPATH = $(shell pkg-config --cflags jsoncpp)

PROTOBUF_LIBS = $(shell pkg-config --libs protobuf)
PROTOBUF_CPATH = $(shell pkg-config --cflags protobuf)

JAVA_PATH = /opt/java/jdk
JAVA_CPATH = -I$(JAVA_PATH)/include -I$(JAVA_PATH)/include/linux

STORM_PATH = /usr/local/src/storm/latest
STORM_JAR := $(shell find $(STORM_PATH)/ -type f | egrep 'storm-core')

CPATH = -I/usr/local/include -I../include -I.
CPATH += $(OPENCV_CAPTH) $(JSON_CPATH) $(PROTOBUF_CPATH) $(NV_CPATH)
CPATH += $(JAVA_CPATH)

LIBS = -lmemcached -ljpeg
LIBS += $(OPENCV_LIBS) $(JSON_LIBS) $(PROTOBUF_LIBS) $(NV_LIBS)

EXTRAFLAGS = -Wall -Werror -Wextra 
EXTRAFLAGS += -Wno-unused-function -Wno-unused-parameter
EXTRAFLAGS += -fcolor-diagnostics

DEBUG ?= 1
ifeq ($(DEBUG),1)
EXTRAFLAGS += -O0 -ggdb3
else
EXTRAFLAGS += -O2
endif

CFLAGS = $(EXTRAFLAGS) $(CPATH)
CXXFLAGS = -std=c++11 $(EXTRAFLAGS) $(CPATH)

#
# Our java sources for use in Storm
#

JAVA_SOURCES = SearchTopology.java Logger.java JNILinker.java

.jar: $(JAVA_SOURCES)
	javac -cp $(STORM_JAR) $^
	touch .jar

CLASSES = *.class
search.jar: libjnilinker.so .jar
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

LIB_SOURCES = StormFuncs.cpp JNILinker.cc

libjnilinker.so: cv/libcv.a Objects.pb.cc JNILinker.h | $(LIB_SOURCES)
	$(CXX) $(CXXFLAGS) --shared -fPIC $(CPATH) -o $@ \
		cv/libcv.a Objects.pb.cc $(LIB_SOURCES) $(LIBS)

#
# Test codes
#

LinkerTest.class:	LinkerTest.java JNILinker.java
	javac $^

# make this directly if wanted
LinkerTest:	LinkerTest.class libjnilinker.so cv/libcv.a
	java LinkerTest

StormFuncsTest:	Objects.pb.cc StormFuncsTest.cc StormFuncs.cpp cv/libcv.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

#
# Utilities for loading data into object store
#

load_egonet: load_egonet.o Objects.pb.cc Config.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

#
# Misc
#

clean:
	rm -fv .jar
	rm -f *.class *.so *.o *.pb.cc *.pb.h JNILinker.h search.jar
	rm -fv cv/*.o cv/*.a
	rm -fv /tmp/*bolt* /tmp/*spout*
	rm -fv load_egonet StormFuncsTest
	$(shell cd /tmp/; ls | egrep '^[0-9a-f]{8}-' | xargs rm -rf)

.PHONY: all clean

