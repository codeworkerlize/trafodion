# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2020 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

.PHONY: all mgblty

TOOLSDIR := /opt/home/tools
SHELL := /bin/bash
DBMGR_VER := $(shell grep dbmgr ../../version_matrix |awk -F: '{print $$2}' |sed 's/^[ ]*//g')
DBMGR_TARNAME=dbmgr-${DBMGR_VER}.tar.gz
DBMGR_WARNAME=dbmgr-${DBMGR_VER}.war
OM_RELEASE_URL=http://downloads.esgyn.cn/OM-Release/dbmanager/

# Location of manageability folder
MGBLTY_DEST_DIR=./mgblty

# Location of temporary download folder
DOWNLOAD_DIR=./mgblty_download
# the source file from https://artifacts.elastic.co/downloads/beats/filebeat/
FILEBEAT_PACKAGE=filebeat-7.8.0-linux-${ARCH}.tar.gz

EXIST=$(shell wget --no-check-certificate --spider ${OM_RELEASE_URL}/${DBMGR_TARNAME} 2>&1 |grep -c '200 OK')

all:
	mkdir -p ${DOWNLOAD_DIR}
	@if [ -f ${DOWNLOAD_DIR}/${DBMGR_TARNAME} ]; then \
        echo "Using local cached dbmgr binary from [${DOWNLOAD_DIR}]"; \
	elif [ ${EXIST} -eq 1 ]; then \
        echo "Copying DB Manager binary from OM releases location ..."; \
        wget ${OM_RELEASE_URL}/${DBMGR_TARNAME} -O ${DOWNLOAD_DIR}/${DBMGR_TARNAME}; \
    else \
        echo "Cannot get DB Manager binary from OM releases location: [${OM_RELEASE_URL}/${DBMGR_TARNAME}], please check 'wget' command is installed or binary exists"; \
        exit 1; \
    fi
	mkdir -p target
	tar xf ${DOWNLOAD_DIR}/${DBMGR_TARNAME} -C target

	#replace jdbc jar
	@if [ -f ${TRAF_HOME}/export/lib/jdbcT4-${TRAFODION_VER}.jar ]; then \
        echo "Replacing jdbcT4 jar file from [${TRAF_HOME}/export/lib/]" ;\
        mkdir -p target/tmp ;\
        cd target/tmp && jar -xvf ../dbmgr-${DBMGR_VER}/lib/${DBMGR_WARNAME} ;\
        ${RM} -f WEB-INF/lib/jdbcT4-*.jar ;\
        cp ${TRAF_HOME}/export/lib/jdbcT4-${TRAFODION_VER}.jar WEB-INF/lib/ ;\
        jar -cvfm ${DBMGR_WARNAME} META-INF/MANIFEST.MF ./ && cd ../../ && mv target/tmp/${DBMGR_WARNAME} target/dbmgr-${DBMGR_VER}/lib/ ;\
        chmod 770 target/dbmgr-${DBMGR_VER}/lib/${DBMGR_WARNAME} ;\
    else \
        echo "WARNING****** jdbcT4 jar in dbmgr is not the latest." ;\
	fi

mgblty:
	mkdir -p ${DOWNLOAD_DIR}
	@if [ -f ${DOWNLOAD_DIR}/${FILEBEAT_PACKAGE} ]; then \
        echo "Using local cached filebeat binary from [${DOWNLOAD_DIR}]"; \
    elif [ -f ${TOOLSDIR}/om/${FILEBEAT_PACKAGE} ]; then \
        echo "Using filebeat binary from [${TOOLSDIR}/om], and cache to local directory [${DOWNLOAD_DIR}]"; \
        cp -r ${TOOLSDIR}/om/${FILEBEAT_PACKAGE} ${DOWNLOAD_DIR}/; \
    else \
		echo "Downloading filebeat tar file from internet"; \
		wget http://downloads.esgyn.cn/OM-Release/filebeat/${FILEBEAT_PACKAGE} -O ${DOWNLOAD_DIR}/${FILEBEAT_PACKAGE}; \
    fi
	echo "Decompression filebeat and setup"
	@mkdir -p ${DOWNLOAD_DIR}/filebeat
	@tar xfz ${DOWNLOAD_DIR}/${FILEBEAT_PACKAGE} -C ${DOWNLOAD_DIR}/filebeat --strip-components=1
	@mkdir -p ${MGBLTY_DEST_DIR}/filebeat
	@cp -rp ${DOWNLOAD_DIR}/filebeat/* ${MGBLTY_DEST_DIR}/filebeat
	@cp scripts/filebeat/filebeat.yml ${MGBLTY_DEST_DIR}/filebeat/

clean:
	$(RM) -r target ${MGBLTY_DEST_DIR}

cleanall: clean
	$(RM) -r ${DOWNLOAD_DIR}
