all:	OBmain
.PHONY:	all

CXX = clang++
CXXFLAGS = -g -std=c++11
CPATH = -I.
LDFLAGS = 
LIBS = -ljpeg

# exclude matlabwrapper.cc
sources = detect.cc dt.cc fconv.cc LSVMModel.cc MaxGetSpatialPyramid.cc \
		  MaxGetSpatialPyramid_single.cc myfeatures.cc mymex.cc OBmain.cc \
		  parserUtil.cc resize.cc
objects = $(patsubst %.cc,%.o,$(sources))

OBmain:  $(objects)
	$(CXX) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LIBS)

clean: 
	rm -f OBmain *.o
.PHONY:	clean

