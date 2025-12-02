files = $(wildcard *.cc *.cpp)
OBJS = $(addsuffix .o,$(basename $(files)))

OPT = -O -Wall -fPIC
LCG_PATH = /cvmfs/sft.cern.ch/lcg/views/LCG_108/x86_64-el9-gcc15-opt/

ROOTINC := $(shell root-config --cflags)
ROOTLIBS := $(shell root-config --glibs)

INC = -I. $(ROOTINC) -I$(LCG_PATH)include

LIBS = -L. $(ROOTLIBS) -L$(LCG_PATH)lib

.cc.o:
	$(CXX) $(OPT) $(INC) -c $*.cc

.cpp.o:
	$(CXX) $(OPT) $(INC) -c $*.cpp

.o.out:
	$(CXX) $(OPT) *.o $(INC) $(LIBS) -o $@

clean:
	rm -f *.o *.so $(OBJS)
