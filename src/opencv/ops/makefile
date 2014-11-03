all:	feature lib.so parsemaps
.PHONY:	all

OCV_CFLAGS = $(shell pkg-config --cflags opencv)
OCV_LDFLAGS = $(shell pkg-config --libs-only-L opencv)
OCV_LIBS = $(shell pkg-config --libs-only-l opencv)
OCV_LIBS += $(shell pkg-config --libs-only-other opencv)

NV_CPATH = /usr/local/cuda/include
NV_LDFLAGS = -L/usr/local/cuda/lib64

CLANG_FLAGS = 

CXX = clang++
CFLAGS = -O3 -I$(NV_CPATH)
#CFLAGS = -ggdb -O0 -I$(NV_CPATH)
CXXFLAGS  = -std=c++11 $(CFLAGS) $(OCV_CFLAGS)
LDFLAGS = $(OCV_LDFLAGS) $(NV_LDFLAGS)
LIBS = $(OCV_LIBS)

feature:	feature.o io.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

lib.so:	lib.cc
	$(CXX) $(CXXFLAGS) -shared -fPIC $< -o $@ 

parsemaps:	parsemaps.o
	$(CXX) -o $@ $^

clean:
	rm -f feature *.o
.PHONY:	clean

