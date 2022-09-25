# If you already have cm_ext outside of source tree (github.com:cloudera/cm_ext)
# set cm_ext variable in environment or define on make command line
# Otherwise validate targets will cause it to be cloned from github and built locally
SHELL := /bin/bash
cm_ext ?= ./cm_ext

validator = $(cm_ext)/validator/target/validator.jar
make_man = $(cm_ext)/make_manifest/make_manifest.py

# increment on command line to force new version
RELEASE ?= 1

ifdef ESGYN_PRODUCT_VER
VER=$(ESGYN_PRODUCT_VER)-$(RELEASE)
else
include ERROR----ESGYN_PRODUCT_VER-not-defined
endif

osver = $(shell lsb_release -rs | cut -f1 -d.)
ifeq ($(osver),n/a)
   osver = 7
endif
codename = $(shell lsb_release -cs)

ifneq ($(DIST_TYPE),Ubuntu)
   OS =  el$(osver)
else
   OS = $(codename)$(osver)
endif 

ifeq ($(osver),6)
USEPYTHON=/usr/local/bin/python
else 
USEPYTHON=python
endif

#---------------------
# build parcels & service (CSD)
#  all target does not use validator

PRODUCT=QianBase
DIST=../../distribution
STAGE=./CM
PARCELS=$(STAGE)/parcels/QIANBASE_TRX-$(VER)-$(OS).parcel \
        $(STAGE)/parcels/QIANBASE-$(VER)-$(OS).parcel

all: $(DIST)/$(PRODUCT)-CM-$(VER)-$(OS).tgz

test: validate csd-validate

$(DIST)/$(PRODUCT)-CM-$(VER)-$(OS).tgz: $(STAGE)/tools/jq $(STAGE)/tools/cm_settings.sh $(PARCELS) $(make_man)
$(DIST)/$(PRODUCT)-CM-$(VER)-$(OS).tgz: $(STAGE)/csd/QIANBASE-$(VER).jar
	$(USEPYTHON) $(make_man) $(STAGE)/parcels
	mkdir -p $(@D)
	cd $(STAGE); tar czvf ../$@ *

csd-validate: $(validator) csd-esgyndb/descriptor/service.sdl
	java -jar $(validator) -s csd-esgyndb/descriptor/service.sdl

csd: $(STAGE)/csd/QIANBASE-$(VER).jar

# aux is reserved word on windows, so we rename it at build time
$(STAGE)/csd/QIANBASE-$(VER).jar: csd/descriptor/service.sdl csd-esgyndb/*/*
	rm -rf csd/aux csd/images csd/scripts
	mkdir -p $(@D) csd/scripts
	cp -r csd-esgyndb/aux_src csd/aux
	cp -r csd-esgyndb/images csd/
	cd csd-esgyndb ; find scripts -type f | while read f ; do \
	   sed -e "s/_TRAF_VERSION_/$(TRAFODION_VER)/" $$f > ../csd/$$f ; done
	cp ../../core/sqf/conf/keepalived.conf csd/aux/
	rm -f $(STAGE)/csd/QIANBASE-*.jar
	cd csd ; jar -cvf ../$@ descriptor images scripts aux

$(STAGE)/tools/cm_settings.sh: cm_settings.sh
	mkdir -p $(@D)
	cp $? $@

# get statically linked version so works with RH6,RH7
$(STAGE)/tools/jq:
	mkdir -p $(@D)
#	wget -O $@ https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64
	wget -O $@ http://downloads.esgyn.cn/dev-tools/jq
	chmod +x $@

csd/descriptor/service.sdl: csd-esgyndb/service.template.sdl
	mkdir -p $(@D)
	sed -e "s/_CSD_VERSION_/$(VER)/" \
	    -e "s/_CSD_NAME_/QIANBASE/" -e "s/_CSD_LABEL_/EsgynDB/" \
	    -e "s/_CSD_DESC_/EsgynDB, based on Apache Trafodion, combining the power of transactional SQL and Apache HBase with the elastic scalability of Hadoop/"  \
	    -e "s/_CSD_INSTANCE_/1/" $? > $@

validate: $(validator)
	java -jar $(validator) -f $(STAGE)/parcels/QIANBASE_TRX-$(VER)-$(OS).parcel
	java -jar $(validator) -f $(STAGE)/parcels/QIANBASE-$(VER)-$(OS).parcel

$(make_man) $(validator):
	git clone git@github.com:cloudera/cm_ext
	cd cm_ext/validator; mvn install

$(STAGE)/parcels/QIANBASE_TRX-$(VER)-$(OS).parcel: QIANBASE_TRX-$(VER)/lib esgyndb_trx/meta/*
	rm -rf QIANBASE_TRX-$(VER)/meta/
	mkdir -p QIANBASE_TRX-$(VER)/meta/
	cd esgyndb_trx ; find . -type f | while read f ; do \
	   sed -e "s/_PARCEL_VERSION_/$(VER)/" \
	       -e "s/_TRAF_VERSION_/$(TRAFODION_VER)/" $$f > ../QIANBASE_TRX-$(VER)/$$f ; done
	mkdir -p $(STAGE)/parcels
	rm -f $(STAGE)/parcels/QIANBASE_TRX-*.parcel
	tar zcf $@ QIANBASE_TRX-$(VER) --owner=root --group=root --mode=o+rX

$(STAGE)/parcels/QIANBASE-$(VER)-$(OS).parcel: QIANBASE-$(VER)/traf_home esgyndb/meta/*
	rm -rf QIANBASE-$(VER)/meta/
	mkdir -p QIANBASE-$(VER)/meta/
	cd esgyndb ; find . -type f | while read f ; do \
	   sed -e "s/_PARCEL_VERSION_/$(VER)/" \
	       -e "s/_TRAF_VERSION_/$(TRAFODION_VER)/" $$f > ../QIANBASE-$(VER)/$$f ; done
	mkdir -p $(STAGE)/parcels
	rm -f $(STAGE)/parcels/QIANBASE-*.parcel
	tar zcf $@ QIANBASE-$(VER) --owner=root --group=root --mode=o+rX

QIANBASE-$(VER)/traf_home: $(DIST)/$(PRODUCT)_server-$(ESGYN_PRODUCT_VER)-$(OS_TYPE)$(OS_MAJOR)-$(ARCH)*.tar.gz
	rm -rf QIANBASE-$(VER)/traf_home
	mkdir -p QIANBASE-$(VER)/traf_home
	cd QIANBASE-$(VER)/traf_home; tar -xzf ../../$?

QIANBASE_TRX-$(VER)/lib: ../../core/sqf/export/lib/hbase-trx-cdh*
	rm -rf QIANBASE_TRX-$(VER)/lib
	mkdir -p QIANBASE_TRX-$(VER)/lib/{5.4,5.5,5.7}
	cp ../../core/sqf/export/lib/trafodion-utility* QIANBASE_TRX-$(VER)/lib/
	cp ../../core/sqf/export/lib/hbase-trx-cdh5_4* QIANBASE_TRX-$(VER)/lib/5.4/
	cp ../../core/sqf/export/lib/hbase-trx-cdh5_5* QIANBASE_TRX-$(VER)/lib/5.5/
	cp ../../core/sqf/export/lib/hbase-trx-cdh5_7* QIANBASE_TRX-$(VER)/lib/5.7/

clean:
	rm -rf QIANBASE_TRX-*
	rm -rf QIANBASE-*
	rm -rf $(STAGE) csd
	rm -rf csd-esgyndb/descriptor/service.sdl csd-esgyndb/aux
