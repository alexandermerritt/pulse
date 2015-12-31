#
# Makefile for analytics apps.
#

CC := clang-3.7
CXX := clang++-3.7
CFLAGS := -std=c11 -Wall -Wextra
CXXFLAGS := -std=c++11 -Wall -Wextra -I/usr/include/GraphicsMagick -Ithirdparty
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

TARGETS := analyze decode
OTHERDEPS := makefile

.PHONY:	all
all:	$(TARGETS)

#analyze:	analyze.o $(OTHERDEPS)
#$(CXX) -o $@ $< \

analyze:	analyze.o $(OTHERDEPS)
	$(CXX) -o $@ $< \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a

analyze_je:	analyze.o $(OTHERDEPS)
	$(CXX) -o $@ $< \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a \
		-ljemalloc

analyze_tc:	analyze.o $(OTHERDEPS)
	$(CXX) -o $@ $< \
		$(LDFLAGS) $(LIBS) $(GMCCLIBS) -ldl \
		-lgdk-x11-2.0 $(OCVLIBS) ./thirdparty/opensift/lib/libopensift.a \
		-ltcmalloc

malloc-track.o: malloc-track.c $(OTHERDEPS)
	$(CC) -o $@ $< $(CFLAGS) -c -fPIC

libmalloctrack.so: malloc-track.o $(OTHERDEPS)
	$(CC) -o $@ $< $(LDFLAGS) -shared

decode:	decode.o $(OTHERDEPS)
	$(CXX) -o $@ $< $(LDFLAGS) $(LIBS)

.PHONY: clean
clean:
	rm -f $(TARGETS) *.o *.so

