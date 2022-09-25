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
# Makefile for packaging client products

include ../macros.gmk #top level
include $(TRAF_HOME)/macros.gmk # pick up BINEXPDIR defn

RM      = /bin/rm

ifeq ($(SQ_BUILD_TYPE),release)
  CLIENT_TAR	?= ../../${DISTRIBUTION_DIR}/QianBase_clients-$(ESGYN_PRODUCT_VER)-${OS_TYPE}${OS_MAJOR}-${P_TYPE}.tar.gz
else
  CLIENT_TAR	?= ../../${DISTRIBUTION_DIR}/QianBase_clients-$(ESGYN_PRODUCT_VER)-${OS_TYPE}${OS_MAJOR}-${P_TYPE}-debug.tar.gz
endif


.PHONY: all pkg_server_odb pkg-clients
all: pkg-clients pkg_server_odb rlwrap

pkg-clients: clients/LICENSE clients/NOTICE clients/LICENSE_Esgyn
	mkdir -p $$(dirname $(CLIENT_TAR))
	tar -zcvf $(CLIENT_TAR) clients

clients/LICENSE: ../../licenses/LICENSE-clients
	cp -f $? $@

clients/LICENSE_Esgyn: ../../licenses/Esgyn
	cp -f $? $@

../../licenses/LICENSE-clients:
	cd $(@D) && $(MAKE) $(@F)

clients/NOTICE: ../../NOTICE
	cp -f $? $@

### Optional - if ODB client not available, give warning and continue
pkg_server_odb: PkgODB
	if [ -d PkgODB ]; then make pkg_odb; fi

PkgODB:
	@if [ -f clients/odb64_linux* ];then \
	   mkdir -p PkgODB ; \
	   cd PkgODB ; \
	   tar xzvf ../clients/odb64_linux* ; \
	 else \
	   echo "WARNING: clients/odb64_linux* not found"; \
	   echo "WARNING: Skipping packaging of ODB client with server"; \
	 fi

pkg_odb: $(BINEXPDIR)/odb64luo

$(BINEXPDIR)/odb64luo: PkgODB/bin/odb64luo
	cp -f $? $@

.PHONY:rlwrap

rlwrap:
	bash ./util/build.sh

clean:	
	$(RM) -rf $(TRAF_HOME)/../conn/clients 
	$(RM) -rf PkgODB
	$(RM) -f $(TRAF_HOME)/../conn/*.mf
	$(RM) -f $(CLIENT_TAR)
