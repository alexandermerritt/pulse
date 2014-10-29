all:	feature
.PHONY:	all

OCV_CFLAGS = $(shell pkg-config --cflags opencv)
OCV_LDFLAGS = $(shell pkg-config --libs-only-L opencv)
OCV_LIBS = $(shell pkg-config --libs-only-l opencv)
OCV_LIBS += $(shell pkg-config --libs-only-other opencv)

NV_LDFLAGS = -L/usr/local/cuda/lib64

CLANG_FLAGS = 

CXX = clang++
CXXFLAGS = -std=c++11 -O3 $(OCV_CFLAGS)
#CXXFLAGS = -std=c++11 -g -O0 $(OCV_CFLAGS)
LDFLAGS = $(OCV_LDFLAGS) $(NV_LDFLAGS)
LIBS = $(OCV_LIBS)

feature:	feature.o io.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f feature *.o
.PHONY:	clean

