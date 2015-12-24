#
# Makefile for analytics apps.
#

CXX := clang++-3.7
CXXFLAGS := -Wall -Wextra -O2 -static
LDFLAGS :=
LIBS := -static -ljpeg

TARGETS := analyze decode
OTHERDEPS := makefile

.PHONY:	all
all:	$(TARGETS)

analyze:	analyze.o $(OTHERDEPS)
	$(CXX) -o $@ $< $(LDFLAGS) $(LIBS)

decode:	decode.o $(OTHERDEPS)
	$(CXX) -o $@ $< $(LDFLAGS) $(LIBS)

.PHONY: clean
clean:
	rm -f $(TARGETS) *.o

