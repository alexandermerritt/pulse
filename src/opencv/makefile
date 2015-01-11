all: | search.jar load_egonet

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

CPATH = -I/usr/local/include -I../include -I
CPATH += $(OPENCV_CAPTH) $(JSON_CPATH) $(PROTOBUF_CPATH) $(NV_CPATH)
CPATH += $(JAVA_CPATH)

LIBS = -lmemcached
LIBS += $(OPENCV_LIBS) $(JSON_LIBS) $(PROTOBUF_LIBS) $(NV_LIBS)

EXTRAFLAGS = -Wno-unused-function -Wno-unused-parameter
EXTRAFLAGS += -fcolor-diagnostics

CFLAGS = -Wall -Werror -Wextra $(EXTRAFLAGS) $(CPATH)
CXXFLAGS = -std=c++11 $(EXTRAFLAGS) $(CPATH)

# TODO update flags and rebuild
# debug: sdfs

JAVA_SOURCES = SearchTopology.java Logger.java JNILinker.java

.jar: $(JAVA_SOURCES)
	javac -cp $(STORM_JAR) $^
	touch .jar

CLASSES = *.class
search.jar: libstormfuncs.so .jar
	jar cvf search.jar $(CLASSES) $< resources/

%.pb.cc: %.proto
	protoc --cpp_out=. $<

JNILinker.h:	JNILinker.java
	javah -jni JNILinker

LIB_SOURCES = StormFuncs.cpp Objects.pb.cc JNILinker.cc

libstormfuncs.so: $(LIB_SOURCES) JNILinker.h
	$(CXX) $(CXXFLAGS) --shared -fPIC $(CPATH) $(LIB_SOURCES) -o $@ $(LIBS)

load_egonet: load_egonet.cc Objects.pb.cc Config.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

clean:
	rm -fv .jar
	rm -f *.class *.so *.o *.pb.cc *.pb.h JNILinker.h search.jar
	rm -fv /tmp/*bolt* /tmp/*spout*
	cd /tmp/
	ls | egrep '^[0-9a-f]{8}-' | xargs rm -rvf

.PHONY: all clean

