CXX := clang++-3.7
CXXFLAGS := -O3 -g -Wall -Wextra -std=c++11  -fdiagnostics-color=auto -pedantic\
	-Wno-unused-parameter -Wno-unused-function -Wno-unused-variable
LDFLAGS := -L/usr/lib64 -L/usr/lib/x86_64-linux-gnu

DEPS := makefile bitops.h
TARGETS := libctest jetest tctest sutest hoardtest

.PHONY:	all
all:	$(TARGETS)

malloc.o:	malloc.cc $(DEPS)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

libctest:	malloc.o
	$(CXX) -o $@ $< $(LDFLAGS)

jetest:	malloc.o
	$(CXX) -o $@ $< $(LDFLAGS) -ljemalloc

tctest:	malloc.o
	$(CXX) -o $@ $< $(LDFLAGS) -ltcmalloc_minimal

sutest:	malloc.o
	$(CXX) -o $@ $< $(LDFLAGS) -lsupermalloc

hoardtest:	malloc.o
	$(CXX) -o $@ $< $(LDFLAGS) -lhoard

clean:
	rm -f *.o $(TARGETS)

.PHONY:	run
run:
	@./libctest
	@./jetest
	@./tctest
	@./sutest
	@./hoardtest
