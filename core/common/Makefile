# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2017-2018 Esgyn Corporation 
#
# @@@ END COPYRIGHT @@@

.PHONY: all

include ../macros.gmk #top level
DBMGR_TARNAME=dbmgr-$(TRAFODION_VER).tar.gz
MGBLTY_TOOLS_TARNAME=mgblty-tools-$(TRAFODION_VER).tar.gz
CP :=/bin/cp -fp

all: common

common: LICENSE_Esgyn
	$(MAVEN) -DskipTests -P prod,!eclipse install
	${CP} target/esgyn-common-$(TRAFODION_VER).jar $(TRAF_HOME)/export/lib/.

LICENSE_Esgyn: ../../licenses/Esgyn
	cp -f $? $@

depcheck:
	$(MAVEN) org.owasp:dependency-check-maven:check

clean:
	$(MAVEN) clean
	rm -f $(TRAF_HOME)/export/lib/esgyn-common-*.jar

