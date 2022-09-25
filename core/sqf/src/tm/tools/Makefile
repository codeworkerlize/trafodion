# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@

include ../../../macros.gmk
include ../../macros.gmk
include ../macros.gmk
include macros.gmk


# possible defines
#   DEBUG

DEBUG		=
DEFINES		=  -DxUSE_NEW_PHANDLE -DDEBUG -DxUSE_SCAN_WAIT -DxUSE_PROCESS_COMPLETION -DxUSE_INTERLEAVE

LIBJVM          = -I. -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -L$(JAVA_HOME)/jre/lib/$(JRE_LIB_DIR)/server -ljvm

ifdef SQ_NEW_PHANDLE
DEFINES         += -DUSE_NEW_PHANDLE
endif
 
# temporary
DEFINES		+= -D_MPICC_H
ifeq ($(SQ_MTYPE),64)
DEFINES += -DNA_64BIT
endif

CFILES		= `ls *.c`
ifeq ($(USE_LOGGING),1)
LIBSX		= -L$(LIBEXPDIR) -levlsq -lshbasetmlib
endif

#TESTAROBJS        = adp_read_test.o \

#gcc 4.8 need explicit enable flag to search for dependent shared object
CXXFLAGS       += -Xlinker --copy-dt-needed-entries

ifeq ($(DIST_TYPE),Ubuntu)
PROGS = dtmci 
else ifeq ($(strip $(DIST_TYPE)),Kylin)
PROGS = dtmci
else
PROGS = dtmci \
        tmshutdown 
endif

all:   $(PROGS)

dtmci: $(OUTDIR)/dtmci.o $(OUTDIR)/versdtmci.o $(LIBEXPDIR)/libshbasetmlib.so
	$(CXX) $(CXXFLAGS) -o $@ $(OUTDIR)/dtmci.o $(OUTDIR)/versdtmci.o $(LIBSTM) -lshbasetmlib -L$(LOG4CPLUS_LIB_DIR) -llog4cplus -L$(JAVA_HOME)/jre/lib/$(JRE_LIB_DIR)/server -ljvm
	cp -fup dtmci $(BINEXPDIR)

tmshutdown: $(OUTDIR)/tmshutdown.o $(OUTDIR)/verstmshutdown.o $(LIBEXPDIR)/libshbasetmlib.so
	$(CXX) $(CXXFLAGS) -o $@ $(OUTDIR)/tmshutdown.o $(OUTDIR)/verstmshutdown.o $(LIBSTM) $(LIBJVM) -lshbasetmlib
	cp -fup tmshutdown $(BINEXPDIR)


#ro_rm: ro_rm.o
#	$CXX) -o $@ ro_rm.o $(LIBSTM)

setup:
	@echo OUTDIR = $(OUTDIR)
	@echo LIBEXPDIR = $(LIBEXPDIR)
	@echo LIBSTM = $(LIBSTM)

clean:
	$(RM) $(PROGS) $(OUTDIR)/*.o $(CDEPFILES)
	$(RM) $(BINEXPDIR)/tmshutdown
	$(RM) $(BINEXPDIR)/dtmci

cleanall: clean
	$(RM) -r $(BUILD_PLAT)

cleaner: clean
	$(RM) *~

-include $(CDEP_DIR)/*.dep
_dummy := $(shell if [ ! -d $(CDEP_DIR) ]; then mkdir -p $(CDEP_DIR); fi)
