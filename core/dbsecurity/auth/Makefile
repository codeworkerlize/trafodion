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
#
# File:         Makefile
# Description:  Makefile for building authentication library 
#

include $(TRAF_HOME)/macros.gmk #top level
include ../macros.gmk

#OUTDIR	= .

RM	= /bin/rm
CP	= /bin/cp
DBG_FLAGS	= $(DBG_FLGS)  

.PHONY: all
all: $(LIBEXPDIR)/libsqauth.so $(BINEXPDIR)/ldapconfigcheck $(BINEXPDIR)/ldapcheck $(INCEXPDIR)/dbUserAuth.h $(INCEXPDIR)/auth.h $(INCEXPDIR)/authEvents.h $(BINEXPDIR)/ldapsync

#Source files required to build the library

OBJS	= $(OUTDIR)/dbUserAuth.o \
	  $(OUTDIR)/ldapconfignode.o \
  	  $(OUTDIR)/authEvents.o \
	  $(OUTDIR)/ldapconfigfile.o  \
	  $(OUTDIR)/CommonLogger.o  \
	  $(OUTDIR)/token.o  \
	  $(OUTDIR)/tokenkey.o \
	  $(OUTDIR)/verssqauth.o

OBJS2	= \
	  $(OUTDIR)/sqldapconfigcheck.o \
	  $(OUTDIR)/verssqldapconfigcheck.o \
	  $(OUTDIR)/ldapconfigfile.o  

OBJS3	= \
	  $(OUTDIR)/ldapconfignode.o \
 	  $(OUTDIR)/authEvents.o \
	  $(OUTDIR)/versldapcheck.o \
	  $(OUTDIR)/ldapcheck.o \
	  $(OUTDIR)/CommonLogger.o  \
	  $(OUTDIR)/ldapconfigfile.o  

OBJS4    = \
	  $(OUTDIR)/namefilter.o \
	  $(OUTDIR)/sqlCollector.o \
	  $(OUTDIR)/ldapCollector.o \
	  $(OUTDIR)/ldapconfigfile.o \
	  $(OUTDIR)/ldapconfignode.o \
	  $(OUTDIR)/authEvents.o \
	  $(OUTDIR)/CommonLogger.o \
	  $(OUTDIR)/ldapsync.o

INCLUDES	= -I. -I./inc -I ../shared/inc \
	        -I $(TRAF_HOME)/../sql/cli \
	        -I $(TRAF_HOME)/../sql/common \
	        -I $(TRAF_HOME)/../sql/executor \
	        -I $(TRAF_HOME)/../sql/export \
	        -I $(TRAF_HOME)/../sql/porting_layer \
	        -I $(TRAF_HOME)/export/include \
	        -I $(TRAF_HOME)/export/include/nsk \
	        -I $(TRAF_HOME)/commonLogger \
	        -I $(LOG4CPLUS_INC_DIR) \
                -I $(LOG4CPLUS_INC_DIR)/log4cplus \
	        -I ../../sql/common \
			-I ../../sqf/inc \
         	-I ../../sql/eh \
			-I ../../sql/sqlcomp


LINK_OPTIONS	= -L$(LIBEXPDIR) -L$(LOG4CPLUS_LIB_DIR) -lldap -lssl -llber -llog4cplus -lcrypto
LINK_OPTIONS   += $(LNK_FLGS)

$(LIBEXPDIR)/libsqauth.so:	$(OBJS)
	$(CXX) -fPIC $(DBG_FLAGS) -shared $(GCCMODEXX) -o $@ $(INCLUDES) $(OBJS) $(LINK_OPTIONS)

$(INCEXPDIR)/dbUserAuth.h: ../auth/inc/dbUserAuth.h
	$(CP) -p ../auth/inc/dbUserAuth.h $(INCEXPDIR)

$(INCEXPDIR)/auth.h: ../auth/inc/auth.h
	$(CP) -p ../auth/inc/auth.h $(INCEXPDIR)

$(INCEXPDIR)/authEvents.h: ../auth/inc/authEvents.h
	$(CP) -p ../auth/inc/authEvents.h $(INCEXPDIR)

$(BINEXPDIR)/ldapconfigcheck:	$(OBJS2)
	$(CXX) -fPIC $(DBG_FLAGS) $(GCCMODEXX) -o $@ $(INCLUDES) $(OBJS2) $(LINK_OPTIONS)

$(BINEXPDIR)/ldapcheck:	$(OBJS3)
	$(CXX) -fPIC $(DBG_FLAGS) $(GCCMODEXX) -o $@ $(INCLUDES) $(OBJS3) $(LINK_OPTIONS) 

$(BINEXPDIR)/ldapsync:	$(OBJS4)
	$(CXX) -fPIC $(DBG_FLAGS) $(GCCMODEXX) -o $@ $(INCLUDES) $(OBJS4) $(LINK_OPTIONS) -lodbc -lpthread


clean:
	$(RM) -f  $(OUTDIR)/*.o
	$(RM) -rf $(OUTDIR)
	$(RM) -f  libsqauth.so
	$(RM) -f  $(LIBEXPDIR)/libsqauth.so
	$(RM) -f  $(INCEXPDIR)/dbUserAuth.h 
	$(RM) -f  $(INCEXPDIR)/auth.h 
	$(RM) -f  $(BINEXPDIR)/ldapcheck
	$(RM) -f  $(BINEXPDIR)/ldapconfigcheck
	$(RM) -f  $(BINEXPDIR)/ldapsync
	${RM} -f  $(TRAF_HOME)/export/include/authEvents.h

cleanall: clean
	$(RM) -rf $(BUILD_PLAT)

sinclude depend.mk
