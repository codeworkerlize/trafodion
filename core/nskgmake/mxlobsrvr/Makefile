#######################################################################
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
#######################################################################

CPPSRC := ExpLOBprocess.cpp 
#ifdef CLOSED_SOURCE
DEP_LIBS :=  \
                tdm_ddol \
                tdm_uofs \
                tdm_dmo \
                tdm_ds \
                tdm_sqlcli \
		cli	\
		comexe	\
		executor	\
		sqlmsg	\
		tdm_arkfsddl \
		tdm_arkfsdml \
		tdm_sqlexp \
		tdm_sqlexport \
		tdm_sqlerrormsg \
		tdm_arkfsinopen \
		tdm_dmo \
		tdm_ddol \
		common \
		sort \
		tdm_sqlmxevents \
		tse_interface \
		tdm_ds \
		tdm_uofs \
		lmcomp \
		tdm_sqlshare \
		arkcmp_dll



DEP_LIBS :=  \
                tdm_sqlcli \
		cli	\
		comexe	\
		executor	\
		sqlmsg	\
		tdm_sqlexp \
		tdm_sqlexport \
		tdm_sqlerrormsg \
		common \
		sort \
		tdm_sqlmxevents \
		lmcomp \
		tdm_sqlshare \
		arkcmp_dll


ifeq ($(FLAVOR),release)
  DEP_DLLS := ZCLIDLL
endif

EXTERN_LIBS := $(XMPIROOT)/libstmlib.so \
                $(XMPIROOT)/libsbms.so \
		$(XMPIROOT)/libsqauth.so

EXTRA_LIBS := $(ODBC_LIB)

LDFLAGS :=      -L$(TNDM_CMPLRS)/lib \
                -L$(TNDM_CMPLRS)/rogue/lib \
                -L$(TOPDIR)/dll \
                -obey nldyos.oby \
                -lcpp2 -lcli

DEFS := -D_LINUX_TARGET

INCLUDE_DIRS := $(TOPDIR)/sql/nsk/inc 

SRCPATH := exp

CPPSRC += vprocsqlci.cpp

TARGET := mxlobsrvr
