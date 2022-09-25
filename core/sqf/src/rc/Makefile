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

CHK		= $(OUTDIR) 
PROGS           = $(OUTDIR)/rc_client $(OUTDIR)/rc_server 

PROGSI		= $(OUTDIR)/rc_client $(OUTDIR)/rc_server

###########################################################################

include ../../macros.gmk # top level
include ../macros.gmk # top level
include ./macros.gmk
# overwrite
INCLUDES	= -I$(INCEXPDIR)
COVER_FLGS      =

# possible defines
#   DEBUG
DEBUG		=
DEFINES		= -D_MPICC_H -DDEBUG

# Tests always need assert
CXXFLAGS       += -UNDEBUG
INCLUDESX	= $(INCLUDES)
INCLUDESJNIX	= $(INCLUDES) -I$(OUTDIR) -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
INCLUDESMONX	= $(INCLUDES) -I$(INCMONDIR)
INCLUDESSRCX	= -I../seabed/src $(INCLUDESX)
LIBS		= $(LIBTMSUTIL) \
		  $(LIBTUTIL) \
		  $(LIBTUTILP)
LIBTMSUTIL	= $(LIBEXPDIR)/libsbztmsfsutil.so
LIBTMSUTILX	= -L$(LIBEXPDIR) -lsbztmsfsutil
LIBTUTIL	= $(LIBEXPDIR)/libsbztutil.so
LIBTUTILX	= -L$(LIBEXPDIR) -lsbztutil
LIBTUTILP	= $(LIBEXPDIR)/libsbztutilp.so
LIBTUTILPX	= -L$(LIBEXPDIR) -lsbztutilp
LINKITCXX	= $(CXX) $(LNK_FLGS) $(LINKSYMBOLS)
LINKSYMBOLS	= -g3
OBJTMSUTIL	= $(OUTDIR)/rc_msfsutil.o
OBJTUTIL	= $(OUTDIR)/rc_util.o
OBJTUTILP	= $(OUTDIR)/rc_arg.o 

PURGE		= *.a *.so $(OUTDIR)/*.o $(OUTDIR)/com/hp/traf/*.class $(LIBS) $(PROGS) $(PROGSI) \
                  $(OUTDIR)/*.gcda $(OUTDIR)/*.gcno *.gcno $(OUTDIR)/*.gcov \
		  c core.* dgt* m monitor.* s \
		  test.status $(BINEXPDIR)/rc_server $(BINEXPDIR)/rc_client \
		  zjunk zos zrpc zns
# rule macros
RCXXINCLUDES	= $(CXX) $(CDEPFLAGS) $(CXXFLAGS) $(INCLUDESX) -o $@ -c $<
RCXXINCLUDESSRC	= $(CXX) $(CDEPFLAGS) $(CXXFLAGS) $(INCLUDESSRCX) -o $@ -c $<
RFS		= $(LIBTMSUTILX) $(LIBTUTILX) $(LIBTUTILPX) $(LIBSBFSX) $(LIBSBUTILX) -lpthread $(DEBUG)
RMS		= $(LIBTMSUTILX) $(LIBTUTILX) $(LIBTUTILPX) $(LIBSBMSX) $(LIBSBUTILX) -lpthread $(DEBUG)
RMSEG		= $(LIBSBMSX) $(LIBSBUTILX) -lpthread $(DEBUG)
RMSUTP		= $(LIBSBMSX) $(LIBTUTILPX) $(LIBSBUTILX) -lpthread $(DEBUG)
RMX		= $(LIBTUTILX) $(LIBTUTILPX) $(DEBUG)

# target macros
TFS		= $(LIBTMSUTIL) $(LIBTUTIL) $(LIBTUTILP) $(LIBSBFS)
TMS		= $(LIBTMSUTIL) $(LIBTUTIL) $(LIBTUTILP) $(LIBSBMS)
TMSEG		= $(LIBSBMS)
TMSUTP		= $(LIBSBMS) $(LIBTUTILP)
TMX		= $(LIBTUTIL) $(LIBTUTILP)

all: $(PROGS)

RC_SERVER_OBJS = $(OUTDIR)/rc_server.o \
                 $(OUTDIR)/rc_vers_server.o

RC_CLIENT_OBJS = $(OUTDIR)/rc_client.o \
                 $(OUTDIR)/rc_vers_client.o

$(OUTDIR)/rc_server: $(RC_SERVER_OBJS) $(TMS)
	@echo "link, $@ , $< , outdir: $(OUTDIR), $(BINEXPDIR)"
	$(LINKITCXX) -o $@ $(RC_SERVER_OBJS) $(RMS)
	cp -fup $@ $(BINEXPDIR)

$(OUTDIR)/rc_server.o: rc_server.cpp
	@echo "compile $<"
	$(RCXXINCLUDESSRC)

$(OUTDIR)/rc_client: $(RC_CLIENT_OBJS) $(TMS)
	@echo "link, $@ , $< , outdir: $(OUTDIR)"
	$(LINKITCXX) -o $@ $(RC_CLIENT_OBJS) $(RMS)
	cp -fup $@ $(BINEXPDIR)

$(OUTDIR)/rc_client.o: rc_client.cpp
	@echo "compile $<"
	$(RCXXINCLUDESSRC)

$(LIBEXPDIR)/libsbztmsfsutil.so: $(OBJTMSUTIL)
	@echo "link, $@ outdir: $(OUTDIR)"
	$(CXX) $(OPTIM_FLGS) -shared -o $@ $(OBJTMSUTIL) $(LIBSBMSX)

$(LIBEXPDIR)/libsbztutil.so: $(OBJTUTIL)
	@echo "link, $@ outdir: $(OUTDIR)"
	$(CXX) $(OPTIM_FLGS) -shared -o $@ $(OBJTUTIL)

$(LIBEXPDIR)/libsbztutilp.so: $(OBJTUTILP)
	@echo "link, $@ outdir: $(OUTDIR)"
	$(CXX) $(OPTIM_FLGS) -shared -o $@ $(OBJTUTILP)

trace.cpp: vproc.h
$(OUTDIR)/vproc.o: vproc.h
vproc.h: vproc.h.in
	$(DOVERS) vproc.h.in vproc.h

clean:
	$(RM) $(PURGE) $(CDEPFILES)

cleanall: clean
	$(RM) -fr $(BUILD_PLAT) stdout_* sqconfig.db

cleaner: clean
	$(RM) *~

cleanprogs:
	$(RM) $(PROGS) $(PROGSI)

-include $(CDEP_DIR)/*.dep
_dummy := $(shell if [ ! -d $(CDEP_DIR) ]; then mkdir -p $(CDEP_DIR); fi)
