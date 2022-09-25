
# @@@ START COPYRIGHT @@@
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# @@@ END COPYRIGHT @@@
SHELL := /bin/bash
RELEASE_VER ?= ${ESGYN_PRODUCT_VER}
RELEASE_TYPE = QianBase

PYINSTALLER_TARNAME = $(shell echo ${RELEASE_TYPE}_pyinstaller-${RELEASE_VER}-${OS_TYPE}${OS_MAJOR}.tar.gz)
PYINSTALLER_VER = $(shell ../core/sqf/build-scripts/build.branch)-${RELEASE_TYPE}-${RELEASE_VER}-${OS_TYPE}${OS_MAJOR}

TOOLSDIR := /opt/home/tools
DOWNLOAD_DIR=./installer_download
JAR_URL="http://downloads.esgyn.cn/dev-tools/cdh/ha_jars/"
JAR_EXIST=$(shell wget --no-check-certificate --spider ${JAR_URL} 2>&1 |grep -c '200 OK')
JAR_DOWNLOAD_DIR=${DOWNLOAD_DIR}/ha_jars
JAR_TARGET_DIR=./python-installer/ha_jars

EXPORTER_DOWNLOAD_DIR=${DOWNLOAD_DIR}/exporter
EXPORTER_RELEASE_URL="http://downloads.esgyn.cn/OM-Release/exporter/"
NODE_EXPORTER_EXIST=$(shell wget --no-check-certificate --spider ${EXPORTER_RELEASE_URL}/${ARCH}/node_exporter 2>&1 |grep -c '200 OK')
NODE_EXPORTER_TARGET_DIR=./python-installer/omclient/node_exporter

ESGYN_EXPORTER_EXIST=$(shell wget --no-check-certificate --spider ${EXPORTER_RELEASE_URL}/${ARCH}/esgyn_exporter 2>&1 |grep -c '200 OK')
ESGYN_EXPORTER_TARGET_DIR=./python-installer/omclient/esgyn_exporter

# the source file from https://artifacts.elastic.co/downloads/beats/filebeat/
FILEBEAT_PACKAGE=filebeat-7.8.0-linux-${ARCH}.tar.gz
FILEBEAT_TARGET_DIR=./python-installer/omclient/filebeat

all: pkg-pyinstaller

pre-pkg: create-dir python-installer/LICENSE python-installer/NOTICE create-verfile python-installer/scripts/constants_release.py

pkg-ambari:
	cd ambari-installer && $(MAKE) all

create-dir:
	mkdir -p ../distribution

create-verfile:
	./traf_install_genver.sh "${PYINSTALLER_VER}"

pkg-pyinstaller: pre-pkg downloads-jars om-client
	rm -f python-installer/templates/demo.license
	tar czf ${PYINSTALLER_TARNAME} python-installer
	mv -f ${PYINSTALLER_TARNAME} ../distribution

../licenses/LICENSE-pyinstall:
	cd $(@D) && $(MAKE) $(@F)

python-installer/scripts/constants_release.py:
	echo "DEF_TRAFODION_VER = '$(TRAFODION_VER)'" > $@

python-installer/LICENSE: ../licenses/LICENSE-pyinstall
	cp -f $? $@

python-installer/NOTICE: ../NOTICE
	cp -f $? $@

version:
	@echo "$(RELEASE_VER)"

downloads-jars:
	@if [ $(shell echo `ls ${JAR_DOWNLOAD_DIR}|wc -w`) -eq 4 ]; then \
        echo "Using local cached HA jars from [${JAR_DOWNLOAD_DIR}], this may not be the latest version, use of your own risk"; \
	elif [ ${JAR_EXIST} -eq 1 ]; then \
        echo "Copying HA jars from downloads.esgyn.cn ..."; \
		mkdir -p ${JAR_DOWNLOAD_DIR}; \
        wget -r -np -nd ${JAR_URL} -P ${JAR_DOWNLOAD_DIR}; \
		rm -f ${JAR_DOWNLOAD_DIR}/index.html; \
    else \
        echo "Cannot find HA jars from downloads.esgyn.cn: [${JAR_URL}], please check 'wget' command is installed or binary exists"; \
        exit 1; \
    fi
	mkdir -p ${JAR_TARGET_DIR}
	cp -r ${JAR_DOWNLOAD_DIR}/* ${JAR_TARGET_DIR}

om-client: node_exporter esgyn_exporter filebeat

node_exporter:
	mkdir -p ${NODE_EXPORTER_TARGET_DIR} ${EXPORTER_DOWNLOAD_DIR}
	@if [ -f ${EXPORTER_DOWNLOAD_DIR}/node_exporter ]; then \
        echo "Using local cached node_exporter binary from [${EXPORTER_DOWNLOAD_DIR}]"; \
	elif [ ${NODE_EXPORTER_EXIST} -eq 1 ]; then \
        echo "Copying node_exporter binary from OM releases location ..."; \
        wget ${EXPORTER_RELEASE_URL}/${ARCH}/node_exporter -O ${EXPORTER_DOWNLOAD_DIR}/node_exporter; \
    else \
        echo "Cannot find node_exporter binary from OM releases location: [${EXPORTER_RELEASE_URL}], please check 'wget' command is installed or binary exists"; \
        exit 1; \
    fi
	@chmod +x ${EXPORTER_DOWNLOAD_DIR}/node_exporter
	@cp -r ${EXPORTER_DOWNLOAD_DIR}/node_exporter ${NODE_EXPORTER_TARGET_DIR}/

esgyn_exporter:
	mkdir -p ${ESGYN_EXPORTER_TARGET_DIR} ${EXPORTER_DOWNLOAD_DIR}
	@if [ -f ${EXPORTER_DOWNLOAD_DIR}/esgyn_exporter ]; then \
        echo "Using local cached esgyn_exporter binary from [${EXPORTER_DOWNLOAD_DIR}], this may not be the latest version, use of your own risk"; \
	elif [ ${ESGYN_EXPORTER_EXIST} -eq 1 ]; then \
        echo "Copying esgyn_exporter binary from OM releases location ..."; \
        wget ${EXPORTER_RELEASE_URL}/${ARCH}/esgyn_exporter -O ${EXPORTER_DOWNLOAD_DIR}/esgyn_exporter; \
    else \
        echo "Cannot find esgyn_exporter binary from OM releases location: [${EXPORTER_RELEASE_URL}], please check 'wget' command is installed or binary exists"; \
        exit 1; \
    fi
	@chmod +x ${EXPORTER_DOWNLOAD_DIR}/esgyn_exporter
	@cp -r ${EXPORTER_DOWNLOAD_DIR}/esgyn_exporter ${ESGYN_EXPORTER_TARGET_DIR}/

filebeat:
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
	@mkdir -p ${FILEBEAT_TARGET_DIR}
	@cp -rp ${DOWNLOAD_DIR}/filebeat/* ${FILEBEAT_TARGET_DIR}/
	@cp ./python-installer/templates/filebeat-template.yml ${FILEBEAT_TARGET_DIR}/filebeat.yml

clean:
	rm -f ${INSTALLER_TARNAME} installer/decoder
	rm -f ${PYINSTALLER_TARNAME}
	rm -rf ${JAR_TARGET_DIR} ./python-installer/omclient
	cd ambari-installer && $(MAKE) clean

cleanall: clean
	rm -rf ${DOWNLOAD_DIR}
