## Top-level Makefile with rules for main components

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
## This version number is used by automated build procedures.
## Please don't change the version number unless you know what you are doing.
## Makefile Version: 8  -- SeaMonster
SHELL := /bin/bash
include macros.gmk

# Make Targets
.PHONY: all dbsecurity foundation $(MPI_TARGET) ndcs ci jdbc_jar jdbc_type2_jar sqroot $(SEAMONSTER_TARGET) verhdr dcs rest wms common
.PHONY: package package-all pkg-product pkg-sql-regress pkg-phx-tests pkg-dcs-tests check-copyrights pkg-installer

################
### Main targets
# Server-side only

# Default target (all components)
all: $(MPI_TARGET) dbsecurity foundation jdbc_jar $(SEAMONSTER_TARGET) ndcs ci jdbc_type2_jar dcs rest lib_mgmt wms common

package: pkg-product pkg-client pkg-installer pkg-dep_el7

#############
# Components

mpi: sqroot verhdr
	echo "Building MPI"
	cd mpi && $(MAKE) sq-local 2>&1 | sed -e "s/$$/	##(MPI)/";exit $${PIPESTATUS[0]}

mpistub: sqroot verhdr
	echo "Building MPI stub"
	cd mpistub && $(MAKE) sq-local 2>&1 | sed -e "s/$$/	##(MPISTUB)/";exit $${PIPESTATUS[0]}

seamonster: mpi
	echo "Building SM"
	cd seamonster/src; $(MAKE) all 2>&1 | sed -e "s/$$/	##(SEAMONSTER)/" ; exit $${PIPESTATUS[0]}

smstub: mpistub
	echo "Building SM stub"
	cd smstub/src; $(MAKE) all 2>&1 | sed -e "s/$$/	##(SMSTUB)/" ; exit $${PIPESTATUS[0]}

verhdr:
	cd sqf && $(MAKE) genverhdr

dbsecurity: $(MPI_TARGET)
	cd sqf/src/seabed && $(MAKE) all 2>&1 | sed -e "s/$$/	##(libsbms.so)/";exit $${PIPESTATUS[0]}
	cd dbsecurity && $(MAKE) all 2>&1 | sed -e "s/$$/	##(Security)/";exit $${PIPESTATUS[0]}

foundation: common sqroot dbsecurity $(MPI_TARGET) $(SEAMONSTER_TARGET)
	cd sqf && $(MAKE) all

jdbc_jar: verhdr
	cd conn/jdbcT4 && $(MAKE) 2>&1 | sed -e "s/$$/	##(JDBCT4)/";exit $${PIPESTATUS[0]}

dcs: jdbc_jar jdbc_type2_jar common mds
	cd ../dcs && $(MAKE) 2>&1 | sed -e "s/$$/  ##(DCS)/" ; exit $${PIPESTATUS[0]}

ndcs: jdbc_jar foundation
	cd conn/odbc/src/odbc && $(MAKE) all        2>&1 | sed -e "s/$$/	##(NDCS)/";exit $${PIPESTATUS[0]}

wms: jdbc_jar jdbc_type2_jar
	cd ../wms && $(MAKE) 2>&1 | sed -e "s/$$/  ##(WMS)/" ; exit $${PIPESTATUS[0]}

ci: trafci
trafci: jdbc_jar
	cd conn/trafci && $(MAKE) 2>&1 | sed -e "s/$$/	##(TRAFCI)/" ; exit $${PIPESTATUS[0]}
	$(MAVEN) install:install-file -Dfile=conn/trafci/lib/trafci.jar \
	    -DgroupId=org.trafodion.ci -DartifactId=trafci -Dversion="$$TRAFODION_VER" \
	    -Dpackaging=jar -DgeneratePom=true | sed -e "s/$$/        ##(TRAFCI)/";exit $${PIPESTATUS[0]}

jdbc_type2_jar: verhdr foundation
	cd conn/jdbc_type2 && $(MAKE) 2>&1 | sed -e "s/$$/	##(JDBC_TYPE2)/" ; exit $${PIPESTATUS[0]}

rest: verhdr jdbc_jar common
	cd rest && $(MAKE) 2>&1 | sed -e "s/$$/  ##(REST)/" ; exit $${PIPESTATUS[0]}

dbmgr: trafci common
	cd dbmgr && $(MAKE) 2>&1 | sed -e "s/$$/	##(DBMGR)/" ; exit $${PIPESTATUS[0]}

mds:
	cd conn/mds && make 2>&1 | sed -e "s/$$/	##(MDS)/" ; exit $${PIPESTATUS[0]}

common: sqroot
	cd common && $(MAKE) 2>&1 | sed -e "s/$$/       ##(ESGYNCOMMON)/" ; exit $${PIPESTATUS[0]}

cleantests:
	cd ../dcs/src/test/pytests && $(RM) -r odbc* tox.ini config.ini .tox .testrep* env.sh test_p2.pyc
	cd ../dcs/src/test/jdbc_test && $(RM) -r jdbcprop pom.xml  target
	cd ../tests/phx && $(RM) -r jdbcprop *.log pom.xml target

clean:
	cd $(MPI_TARGET) &&		$(MAKE) clean-local
	cd $(SEAMONSTER_TARGET)/src &&	$(MAKE) clean
	cd dbsecurity &&		$(MAKE) clean
	cd sqf &&			$(MAKE) clean
	cd conn/odbc/src/odbc &&	$(MAKE) clean
	cd conn/jdbcT4        &&	$(MAKE) clean
	cd conn &&			$(MAKE) clean
	cd conn/jdbc_type2 &&		$(MAKE) clean
	cd conn/trafci &&		$(MAKE) clean
	cd rest &&			$(MAKE) clean
	cd ../dcs &&			$(MAKE) clean
	cd ../wms &&			$(MAKE) clean
	cd dbmgr &&			$(MAKE) clean
	cd conn/mds &&			$(MAKE) clean
	cd sql/lib_mgmt &&		$(MAKE) clean
	cd ../install &&                $(MAKE) clean
	cd ../install/cm-installer &&   $(MAKE) clean
	cd common  &&                   $(MAKE) clean
	$(RM) -r ../${DISTRIBUTION_DIR}

cleanall: eclipseclean cleantests
	cd $(MPI_TARGET) &&		$(MAKE) clean-local
	cd dbsecurity &&		$(MAKE) cleanall
	cd sqf &&			$(MAKE) cleanall
	cd conn/odbc/src/odbc &&	$(MAKE) cleanall
	cd conn/jdbcT4        &&	$(MAKE) clean
	cd conn &&			$(MAKE) clean
	cd conn/jdbc_type2    &&        $(MAKE) clean
	cd conn/trafci &&		$(MAKE) clean
	cd rest &&			$(MAKE) clean
	cd ../dcs &&			$(MAKE) clean
	cd ../wms &&			$(MAKE) clean
	cd dbmgr &&			$(MAKE) cleanall
	cd sql/lib_mgmt &&		$(MAKE) cleanall
	cd ../install &&                $(MAKE) cleanall
	cd common  &&                   $(MAKE) clean
	$(RM) -r ../${DISTRIBUTION_DIR}

package-all: package pkg-sql-regress pkg-phx-tests pkg-dcs-tests

pkg-product: all dcs rest wms pkg-client
	cd sqf && $(MAKE) package 2>&1 | sed -e "s/$$/	##(Package)/";exit $${PIPESTATUS[0]}

deb-pkg-product: all dcs rest wms
	cd sqf && $(MAKE) debpackage 2>&1 | sed -e "s/$$/	##(Package)/";exit $${PIPESTATUS[0]}

pkg-client: ci ndcs
	cd conn &&  make all 2>&1 | sed -e "s/$$/	##(Package clients)/" ; exit $${PIPESTATUS[0]}

# Package SQL regression tests (all target produces some regress/tool files so do that first)
pkg-sql-regress: all
	cd sqf && $(MAKE) package-regress 2>&1 | sed -e "s/$$/	##(Package sql regress)/";exit $${PIPESTATUS[0]}

# Package Phoenix test
pkg-phx-tests: all
	cd sqf && $(MAKE) package-phx 2>&1 | sed -e "s/$$/	##(Package phoenix)/";exit $${PIPESTATUS[0]}

# Package dcs tests
pkg-dcs-tests: all
	cd sqf && $(MAKE) package-dcs 2>&1 | sed -e "s/$$/	##(Package dcs tests)/";exit $${PIPESTATUS[0]}

# Package rpm dependent packages
DEP_EL7 := dep_el7
PKG_DEP_EL7 ?= ../${DISTRIBUTION_DIR}/${DEP_EL7}.tar.gz
pkg-dep_el7:
	@echo "Start exporting dependent packages [${DEP_EL7}.tar.gz]"
ifeq ($(SQ_BUILD_TYPE), release)
	@if [ ! -d ../${DISTRIBUTION_DIR} ];then \
		echo "Create directory ${DISTRIBUTION_DIR}"; \
		mkdir -p ../${DISTRIBUTION_DIR};\
	fi;
	@if [ ! -f ${PKG_DEP_EL7} ];then \
		if [ -f ${TOOLSDIR}/${DEP_EL7}.tar.gz ];then \
			echo "Using filebeat binary from [${TOOLSDIR}/${DEP_EL7}.tar.gz], and cache to local directory [${PKG_DEP_EL7}]"; \
			cp -r ${TOOLSDIR}/${DEP_EL7}.tar.gz ../${DISTRIBUTION_DIR}/; \
		else \
			echo "Please confirm whether the dep_el7.tar.gz exists in the directory  [${TOOLSDIR}]"; \
		fi;\
	fi;
endif

# Package installer
pkg-installer:  dbsecurity
	cd ../install && $(MAKE) 2>&1 | sed -e "s/$$/	##(Package installer)/";exit $${PIPESTATUS[0]}

version:
	@cd sqf; unset SQ_VERBOSE; source sqenv.sh ; echo "$${TRAFODION_VER}"

# Check that Environment variables are set correctly and
# create links and files needed as prerequisite for the build
sqroot:
	@echo "ARCH=$(ARCH)"
	./bldenvchk.sh;
	cd sqf && $(MAKE) setupdir 2>&1 | sed -e "s/$$/	##(setupdir)/";exit $${PIPESTATUS[0]}

# Check for absolute filenames used as dynamic linked libraries
find-absolute-dlls:
	sqf/build-scripts/find-abs-dlls

check-copyrights:
	python ./updateCopyrightCheck.py

parcels: pkg-product
	cd ../install/cm-installer && $(MAKE) all | sed -e "s/$$/	##(Package Cldera Parcels)/";exit $${PIPESTATUS[0]}

##############
# RPM defines
PKG_PROD=QianBase

ifeq ($(SQ_BUILD_TYPE),release)
  RELEASE ?= 1
  SERVERTAR="$(PKG_PROD)_server-$(TRAFODION_VER)-${OS_TYPE}${OS_MAJOR}-${P_TYPE}.tar.gz"
else
  RELEASE ?= devel
  SERVERTAR="$(PKG_PROD)_server-$(TRAFODION_VER)-${OS_TYPE}${OS_MAJOR}-${P_TYPE}-debug.tar.gz"
endif

ARCH=$(P_TYPE)

SPECFILE = trafodion.spec

RPMROOT=$(shell cd ..; /bin/pwd)/RPMROOT
RPMDIR=$(RPMROOT)/RPMS
SRPMDIR=$(RPMROOT)/SRPMS
SOURCEDIR=$(RPMROOT)/SOURCES
BUILDDIR=$(RPMROOT)/BUILD
BUILDROOTDIR=$(RPMROOT)/BUILDROOT

rpmpackage: pkg-product
	/bin/rm -rf $(RPMROOT)
	mkdir -p $(RPMDIR)
	mkdir -p $(SOURCEDIR)
	mkdir -p $(BUILDDIR)
	mkdir -p $(BUILDROOTDIR)
	mkdir -p $(SRPMDIR)
	ln -L ../distribution/$(SERVERTAR) $(SOURCEDIR)/$(PKG_PROD)_server-$(TRAFODION_VER).tar.gz

rpmbuild: rpmpackage
	rpmbuild -vv -bb \
                        --define "name $(PKG_PROD)" \
                        --define "version $(TRAFODION_VER)" \
                        --define "release $(RELEASE)" \
                        --define "_builddir $(BUILDDIR)" \
                        --define "_buildrootdir $(BUILDROOTDIR)" \
                        --define "_sourcedir $(SOURCEDIR)" \
                        --define "_rpmdir $(RPMDIR)" \
                        --define "_arch $(ARCH)" \
                        --define "_topdir $(RPMROOT)" \
                        $(SPECFILE)
	mkdir -p ../distribution/${OS_TYPE}${OS_MAJOR}
	mv $(RPMROOT)/RPMS/$(ARCH)/$(PKG_PROD)-${ESGYN_PRODUCT_VER}*.rpm ../distribution/${OS_TYPE}${OS_MAJOR}/

DEB_ROOT_DIR    = $(shell cd ..; /bin/pwd)/DEBROOT
DEB_WORK_DIR    = $(DEB_ROOT_DIR)/esgynDB_server-$(TRAFODION_VER)
DEB_DEBIAN_DIR  = $(DEB_WORK_DIR)/DEBIAN

CONTROL_FILE        = control
CONTROL_FILE-TMPL   = control-tmpl
POSTINST            = postinst
POSTINST-TMPL       = postinst-tmpl

debpackage: deb-pkg-product
	mkdir -p $(DEB_DEBIAN_DIR)                       # to hold file `control`, `postinst`

	# replace text `%TRAFODION_VER_EXT` with version number and unix timestamp,
	# to enable `apt upgrade`
	cat $(CONTROL_FILE-TMPL) | while read line;                                 \
	do                                                                          \
	    line=$${line/\%TRAFODION_VER_EXT/$${TRAFODION_VER}.$$(date +%s)};       \
	    echo $${line} >> $(CONTROL_FILE);                                       \
	done
	mv $(CONTROL_FILE) ${DEB_DEBIAN_DIR}

	# replace text `TRAFODION_VER` in TRAF_HOME path with version number
	cat $(POSTINST-TMPL) | while read line;                                     \
	do                                                                          \
	    line=$${line/\%TRAFODION_VER/$${TRAFODION_VER}};                        \
	    echo $${line} >> $(POSTINST);                                           \
	done
	mv $(POSTINST) ${DEB_DEBIAN_DIR}
	chmod 755 ${DEB_DEBIAN_DIR}/${POSTINST}

	cp -r $(TRAF_HOME)/sysinstall/* $(DEB_WORK_DIR)

debbuild: debpackage
	(cd $(DEB_ROOT_DIR); \
	dpkg-deb --build $(DEB_WORK_DIR))

	# delete the working dir, but just keep the .deb package
	rm -rf $(DEB_WORK_DIR)

eclipse: rest/.project dcs/.project sql/.project sqf/src/seatrans/hbase-trx/.project common/.project .project
	@echo
	@echo
	@echo "  Now you can import 5 projects in Eclipse:"
	@echo "      $^"
	@echo
	@echo "  File->Import->General->Existing Projects into Workspace"
	@echo "  Select root directory $(@D)"
	@echo "  Search for nested projects"
	@echo "  Select all or only those you want"
	@echo

rest/.project:
	cd rest && $(MAVEN) eclipse:eclipse

dcs/.project:
	cd ../dcs && $(MAVEN) eclipse:eclipse

sql/.project: dbsecurity
	cd sql  && $(MAVEN) eclipse:eclipse

sqf/src/seatrans/hbase-trx/.project:
	cd sqf/src/seatrans/hbase-trx && $(MAVEN) -f pom.xml.apache eclipse:eclipse

common/.project:
	cd common  && $(MAVEN) eclipse:eclipse

.project:
	cp sqf/export/limited-support-tools/eclipse.project .project
	cp sqf/export/limited-support-tools/eclipse.cproject .cproject

eclipseclean:
	$(RM) rest/.classpath rest/.project
	$(RM) ../dcs/.classpath ../dcs/.project
	$(RM) sql/.classpath sql/.project
	$(RM) sqf/src/seatrans/hbase-trx/.classpath sqf/src/seatrans/hbase-trx/.project
	$(RM) .cproject .project

trafinstall:
	cd $(TRAF_HOME)/sql/scripts && install_traf_components

lib_mgmt: foundation
	cd sql/lib_mgmt && $(MAKE) all
