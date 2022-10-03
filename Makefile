## Top-level Makefile for building Trafodion components

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
SRCDIR = $(shell echo $(TRAFODION_VER_PROD) | sed -e 's/ /-/g' | tr 'A-Z' 'a-z')

all: 
	@echo "Building all Trafodion components"
	cd core && $(MAKE) all 

rpmbuild:
ifneq ($(SQ_BUILD_TYPE), release)
	$(error You have to make the release package in 'release' mode)
endif
	@echo "Building RPM package for all Trafodion components"
	cd core && $(MAKE) rpmbuild

debbuild:
ifneq ($(SQ_BUILD_TYPE), release)
	$(error You have to make the release package in 'release' mode)
endif
	@echo "Building DEB package for all Trafodion components"
	cd core && $(MAKE) debbuild

package: 
	@echo "Packaging Trafodion components"
	cd core && $(MAKE) package 

package-all: 
	@echo "Packaging all Trafodion components"
	cd core && $(MAKE) package-all 

package-src: $(SRCDIR)-${TRAFODION_VER}/LICENSE
	@echo "Packaging source for $(TRAFODION_VER_PROD) $(TRAFODION_VER)"
	mkdir -p distribution
	git archive --format tar --prefix $(SRCDIR)-${TRAFODION_VER}/ HEAD > distribution/$(SRCDIR)-${TRAFODION_VER}-src.tar
	tar rf distribution/$(SRCDIR)-${TRAFODION_VER}-src.tar $^
	gzip distribution/$(SRCDIR)-${TRAFODION_VER}-src.tar
	rm -rf $(SRCDIR)-${TRAFODION_VER} LICENSE

$(SRCDIR)-${TRAFODION_VER}/LICENSE:
	cd licenses && $(MAKE) LICENSE-src
	mkdir -p $(@D)
	cp licenses/LICENSE-src $@

eclipse: 
	@echo "Making eclipse projects for Trafodion components"
	cd core && $(MAKE) eclipse 

OWASP_DIRS=  core/common \
             core/conn/jdbcT4 \
	     core/conn/jdbc_type2 \
	     core/conn/trafci \
	     core/dbmgr \
	     core/dbsecurity/sentryauth \
	     core/rest \
	     core/sqf/hbase_utilities \
	     core/sqf/src/seatrans/hbase-trx \
	     core/sqf/src/seatrans/monarch \
	     core/sqf/src/seatrans/tm/hbasetmlib2 \
	     core/sql/lib_mgmt \
	     core/sql/nskgmake \
	     dcs

depcheck: all
	# Dependencies 
	# - seatrans/monarch depcheck needs TRX
	# - tm/hbasetmlib2 depcheck needs monarch
	# - sql/lib_mgmt depcheck needs SQL jar
	# So, we just depend on all to make sure we have dependencies
	top=$$(pwd); for dir in $(OWASP_DIRS) ; do \
	  cd $$top ; cd $$dir; $(MAKE) depcheck || exit 1; done

clean: 
	@echo "Removing Trafodion objects"
	cd core && $(MAKE) clean 
	cd licenses && $(MAKE) clean
	rm -rf ${TRAF_HOME}/trafci/lib/jdbcT4-${TRAFODION_VER}.jar
	rm -rf $(SRCDIR)-${TRAFODION_VER} LICENSE

cleanall: 
	@echo "Removing all Trafodion objects"
	cd core && $(MAKE) cleanall 



trafinstall:
	@echo "Installing Trafodion components"
	cd core && $(MAKE) trafinstall
