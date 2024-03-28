files = $(wildcard *.cc *.cpp)
OBJS = $(addsuffix .o,$(basename $(files)))

OPT = -O -Wall -Wextra -Wpedantic -fPIC

ROOTINC := $(shell root-config --cflags)
ROOTLIBS := $(shell root-config --glibs)

INC = -I. $(ROOTINC)

LIBS = -L. $(ROOTLIBS)

.cc.o:
	$(CXX) $(OPT) $(INC) -c $*.cc

.cpp.o:
	$(CXX) $(OPT) $(INC) -c $*.cpp

.o.out:
	$(CXX) $(OPT) *.o $(INC) $(LIBS) -o $@

clean:
	rm -f *.o *.so $(OBJS)
