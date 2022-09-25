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
all: $(BINEXPDIR)/decoder encoder copy 

#Source files required to build the library

OBJS	= $(OUTDIR)/encoder.o \
	  $(OUTDIR)/decoder.o

LIBOBJS = $(OUTDIR)/decoderlibc.o 

OBJS1	= $(OUTDIR)/verdecoder.o

INCLUDES	= -I. -I./inc -I ../shared/inc \
                -I $(TRAF_HOME)/export/include \
	        -I ../../sql/common


LINK_OPTIONS	= 
LINK_OPTIONS   += $(LNK_FLGS) 

COMMON_LIBS =

$(LIBEXPDIR)/libdecoder.a : $(LIBOBJS)
	ar rcs $(LIBEXPDIR)/libdecoder.a $(LIBOBJS)

$(BINEXPDIR)/decoder: $(LIBEXPDIR)/libdecoder.a  $(OBJS1)  $(OUTDIR)/decoder.o
	$(CXX) -o $@ $(OUTDIR)/decoder.o $(OBJS1) -lssl -lrt -L$(LIBEXPDIR) -ldecoder -lcrypto

encoder:  $(LIBEXPDIR)/libdecoder.a $(OUTDIR)/encoder.o
	$(CXX) -o $@ $(OUTDIR)/encoder.o -lssl -L$(LIBEXPDIR) -ldecoder -lcrypto

copy:  $(LIBEXPDIR)/libdecoder.a $(BINEXPDIR)/decoder
	cp $(BINEXPDIR)/decoder $(BINEXPDIR)/../../../../install/python-installer/scripts

clean:
	$(RM) -f  $(OUTDIR)/*.o
	$(RM) -rf $(OUTDIR)
	$(RM) -f encoder
	$(RM) -f  $(BINEXPDIR)/decoder
	$(RM) -f $(LIBEXPDIR)/libdecoder.a

cleanall: clean
	$(RM) -rf $(BUILD_PLAT)

sinclude depend.mk
