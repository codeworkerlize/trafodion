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

CPPSRC := SqlciMain.cpp 

EARLY_DLLS:= -ljsig

EXTERN_LIBS := \
        $(XMPIROOT)/libstmlib.so \
	$(XMPIROOT)/libsqauth.so 

DEP_LIBS += tdm_sqlcli sqlcilib arkcmp_dll tdm_sqlexp tdm_sqlexport cli executor generator ustat sqlcomp tdm_sqllangman tdm_sqlerrormsg qmscommon arkcmplib parser optimizer comexe common

ifdef BUILD_ORC_READER
DEP_LIBS += orc
endif

CPPSRC += vprocsqlci.cpp


SRCPATH := sqlci bin

