#
# Makefile for analytics apps.
#

CC := clang-3.7
CXX := clang++-3.7
CFLAGS := -std=c11 -Wall -Wextra -Wno-unused-variable
CXXFLAGS := -std=c++11 -Wall -Wextra -I/usr/include/GraphicsMagick -Ithirdparty -Wno-unused-variable
LDFLAGS :=
GMCCLIBS := $(shell pkg-config --libs GraphicsMagick++)
OCVLIBS := $(shell pkg-config --libs opencv)
LIBS := -ljpeg -pthread -lopencv_highgui -lopencv_core -lhiredis

ifeq ($(DEBUG),0)
	CFLAGS += -O2
	CXXFLAGS += -O2
else
	CFLAGS += -ggdb -O0
	CXXFLAGS += -ggdb -O0
endif

TARGETS := cv/libcv.a analyze decode
OTHERDEPS := makefile

.PHONY:	all
all:	$(TARGETS)

#analyze:	analyze.o $(OTHERDEPS)
#$(CXX) -o $@ $< \

analyze:	analyze.o cv/libcv.a $(OTHERDEPS)
	$(CXX) -o $@ $< cv/libcv.a \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a

analyze_je:	analyze.o cv/libcv.a $(OTHERDEPS)
	$(CXX) -o $@ $< cv/libcv.a \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a \
		-ljemalloc

analyze_tc:	analyze.o cv/libcv.a $(OTHERDEPS)
	$(CXX) -o $@ $< cv/libcv.a \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a \
		-ltcmalloc

#malloc-track.o: malloc-track.c $(OTHERDEPS)
#$(CC) -o $@ $< $(CFLAGS) -c -fPIC

#libmalloctrack.so: malloc-track.o $(OTHERDEPS)
#$(CC) -o $@ $< $(LDFLAGS) -shared

decode:	decode.o $(OTHERDEPS)
	$(CXX) -o $@ $< $(LDFLAGS) $(LIBS)

#
# OpenCV (internal) sources used to decode images from in-memory buffers.
#
CV_SOURCES = $(wildcard cv/*.cpp)
CV_OBJ = $(patsubst %.cpp,%.o,$(CV_SOURCES))

CV_COMMON = cv/decoders.h cv/precomp.hpp

cv/%.o:	cv/%.cpp cv/%.hpp $(CV_COMMON)
		$(CXX) $(CXXFLAGS) -c -o $@ $<

cv/libcv.a:	$(CV_OBJ)
		ar rcus $@ $^

.PHONY: clean
clean:
	rm -f $(TARGETS) cv/*.o *.o *.so

