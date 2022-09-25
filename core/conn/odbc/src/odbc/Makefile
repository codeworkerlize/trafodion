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

SHELL := /bin/bash

.PHONY: all
all: ndcs bldlnx_drvr

# NDCS targets
.PHONY:	ndcs
ndcs: nsksrvrcore nsksrvr

.PHONY:	nsksrvrcore
nsksrvrcore:  
	cd nsksrvrcore; $(MAKE)

.PHONY:	nsksrvr
nsksrvr: nsksrvrcore 
	cd nsksrvr; $(MAKE)

### Optional - if client driver not available, give warning and continue
.PHONY:	bldlnx_drvr
bldlnx_drvr: PkgTmp
	if [ -d PkgTmp ]; then make pkglnx_drvr; fi

PkgTmp:
	@if [ -f $(TRAF_HOME)/../conn/clients/TRAF_ODBC_Linux_Driver* ];then \
	  tar xzvf $(TRAF_HOME)/../conn/clients/TRAF_ODBC_Linux_Driver* ; \
	 else \
	  echo "WARNING: $(TRAF_HOME)/../conn/clients/TRAF_ODBC_Linux_Driver* not found"; \
	  echo "WARNING: Skipping packaging of ODBC client with server"; \
	 fi
	
.PHONY: pkglnx_drvr
include $(TRAF_HOME)/macros.gmk
pkglnx_drvr: 
	cp -Pf PkgTmp/*.so PkgTmp/*.so.44 $(LIBEXPDIR)/
	cd $(LIBEXPDIR); ln -sf libicudata.so.44 libicudata.so
	cd $(LIBEXPDIR); ln -sf libicuuc.so.44 libicuuc.so;
	cd $(LIBEXPDIR); ln -sf libtrafodbc_l64.so libtrafodbc64.so;
	cd $(LIBEXPDIR); ln -sf libtrafodbc_l64_drvr.so libtrafodbc_drvr64.so
	cp -p PkgTmp/{connect_test.cpp,TRAFDSN,runconnect_test.sh} $(TRAF_HOME)/samples/



# NDCS clean targets
.PHONY:	cleanndcs
cleanndcs: cleannsksrvrcore cleannsksrvr

.PHONY: cleannsksrvrcore
cleannsksrvrcore:
	cd nsksrvrcore; $(MAKE) clean

.PHONY: cleannsksrvr
cleannsksrvr:
	cd nsksrvr; $(MAKE) clean

# NDCS cleanall targets
.PHONY: cleanallndcs
cleanallndcs:  cleanallnsksrvrcore cleanallnsksrvr

.PHONY: cleanallnsksrvrcore
cleanallnsksrvrcore:
	cd nsksrvrcore; $(MAKE) cleanall

.PHONY: cleanallnsksrvr
cleanallnsksrvr:
	cd nsksrvr; $(MAKE) cleanall

.PHONY: clean
clean: cleanndcs
	rm -rf PkgTmp

.PHONY: cleanall
cleanall: cleanallndcs

