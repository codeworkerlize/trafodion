TOOLSDIR := /opt/home/tools

PROMETHEUS_VER=2.10.0
PROMETHEUS_NAME=prometheus-${PROMETHEUS_VER}.linux-amd64
PROMETHEUS_TAR_NAME=${PROMETHEUS_NAME}.tar.gz
PROMETHEUS_TAR_FILE=$(TOOLSDIR)/om/${PROMETHEUS_TAR_NAME}

GRAFANA_VER=6.2.5
GRAFANA_RPM_NAME=grafana-${GRAFANA_VER}-1.x86_64.rpm
GRAFANA_RPM_FILE=$(TOOLSDIR)/om/${GRAFANA_RPM_NAME}

GRAFANA_PLUGIN_DIRNAME=grafana-plugins
GRAFANA_PLUGIN_DIR=$(TOOLSDIR)/om/${GRAFANA_PLUGIN_DIRNAME}

PHPLDAPADMIN_VER=1.2.3
PHPLDAPADMIN_ZIP_NAME=phpldapadmin-${PHPLDAPADMIN_VER}.zip
PHPLDAPADMIN_ZIP_FILE=$(TOOLSDIR)/om/${PHPLDAPADMIN_ZIP_NAME}

ELASTICSEARCH_VER=7.1.1
ELASTICSEARCH_RPM_NAME=elasticsearch-${ELASTICSEARCH_VER}-x86_64.rpm
ELASTICSEARCH_RPM_FILE=$(TOOLSDIR)/om/${ELASTICSEARCH_RPM_NAME}

LOGSTASH_VER=7.1.1
LOGSTASH_RPM_NAME=logstash-${LOGSTASH_VER}.rpm
LOGSTASH_RPM_FILE=$(TOOLSDIR)/om/${LOGSTASH_RPM_NAME}

XLWT_VER = 1.3.0
XLWT_DIR_NAME = xlwt-${XLWT_VER}
XLWT_TAR_NAME = ${XLWT_DIR_NAME}.tar.gz
XLWT_TAR_FILE = $(TOOLSDIR)/om/${XLWT_TAR_NAME}

CRONTAB_VER = 2.3.8
CRONTAB_DIR_NAME = python-crontab-${CRONTAB_VER}
CRONTAB_TAR_NAME = ${CRONTAB_DIR_NAME}.tar.gz
CRONTAB_TAR_FILE = $(TOOLSDIR)/om/${CRONTAB_TAR_NAME}
CRONTAB_RPM = ${CRONTAB_DIR_NAME}-1.noarch.rpm

OM_PKG_DIRNAME=om_pkgs
DBMGR_DIRNAME=dbmgr
OM_TARNAME=esgyn_operational_management_tool-${ESGYN_PRODUCT_VER}.tar.gz

all: package

create-dir:
	mkdir -p ../../distribution ${OM_PKG_DIRNAME}

package: create-dir pkg-dbmgr pkg-prometheus rpm-grafana grafana-plugin pkg-phpldapadmin rpm-elasticsearch rpm-logstash pkg-xlwt pkg-crontab
	tar czf ../../distribution/${OM_TARNAME} ../om-installer --exclude Makefile --exclude ${XLWT_TAR_NAME} --exclude ${CRONTAB_DIR_NAME} --exclude ${CRONTAB_TAR_NAME}

build-dbmgr:
	@if [ ! -f ../../core/dbmgr/target/dbmgr-${TRAFODION_VER}.war ]; then \
	    cd ../../core/dbmgr; make ;\
    else \
        echo "DBMgr war file exists, skip building dbmgr" ;\
    fi

pkg-dbmgr: build-dbmgr
	mkdir -p ${DBMGR_DIRNAME}/bin ${DBMGR_DIRNAME}/conf/dbmgr ${DBMGR_DIRNAME}/lib ${DBMGR_DIRNAME}/logs/dbmgr
	cp ../../core/dbmgr/bin/dbmgr.sh ${DBMGR_DIRNAME}/bin/
	cp ../../core/dbmgr/conf/logback.xml ${DBMGR_DIRNAME}/conf/dbmgr
	cp ../../core/dbmgr/src/main/resources/warning_level.json ${DBMGR_DIRNAME}/conf/dbmgr
	cp ../../core/dbmgr/src/main/resources/config_template.xml ${DBMGR_DIRNAME}/conf/dbmgr
	cp ../../core/dbmgr/target/dist/dbmgr-${TRAFODION_VER}/dbmgr-${TRAFODION_VER}/lib/* ${DBMGR_DIRNAME}/lib/

pkg-prometheus:
	@if [ -f ${OM_PKG_DIRNAME}/${PROMETHEUS_TAR_NAME} ]; then \
        echo "Local prometheus tar exists, skip downloading" ;\
    elif [ -f ${PROMETHEUS_TAR_FILE} ]; then \
        echo "Copying prometheus tar from TOOLSDIR" ;\
        cp -f ${PROMETHEUS_TAR_FILE} ${OM_PKG_DIRNAME} ;\
    else \
        curl -L https://github.com/prometheus/prometheus/releases/download/v${OS_MAJOR}/${PROMETHEUS_TAR_NAME} -o ${OM_PKG_DIRNAME}/${PROMETHEUS_TAR_NAME};\
    fi

rpm-grafana:
	@if [ -f ${OM_PKG_DIRNAME}/${GRAFANA_RPM_NAME} ]; then \
        echo "Local Grafana RPM exists, skip downloading" ;\
    elif [ -f ${GRAFANA_RPM_FILE} ]; then \
        echo "Copying Grafana RPM from TOOLSDIR" ;\
        cp -f ${GRAFANA_RPM_FILE} ${OM_PKG_DIRNAME}/ ;\
    else \
        curl -L https://dl.grafana.com/oss/release/${GRAFANA_RPM_NAME} -o ${OM_PKG_DIRNAME}/${GRAFANA_RPM_NAME} ;\
    fi

pkg-phpldapadmin:
	@if [ -f ${OM_PKG_DIRNAME}/${PHPLDAPADMIN_ZIP_NAME} ]; then \
        echo "Local phpldapadmin zip exists, skip downloading" ;\
    elif [ -f ${PHPLDAPADMIN_ZIP_FILE} ]; then \
        echo "Copying phpldapadmin zip from TOOLSDIR" ;\
        cp -f ${PHPLDAPADMIN_ZIP_FILE} ${OM_PKG_DIRNAME} ;\
    else \
        curl -L https://sourceforge.net/projects/phpldapadmin/files/phpldapadmin-php5/${PHPLDAPADMIN_VER}/${PHPLDAPADMIN_ZIP_NAME}/download -o ${OM_PKG_DIRNAME}/${PHPLDAPADMIN_ZIP_NAME};\
    fi

rpm-elasticsearch:
	@if [ -f ${OM_PKG_DIRNAME}/${ELASTICSEARCH_RPM_NAME} ]; then \
        echo "Local Elasticsearch RPM exists, skip downloading" ;\
    elif [ -f ${ELASTICSEARCH_RPM_FILE} ]; then \
        echo "Copying Elasticsearch RPM from TOOLSDIR" ;\
        cp -f ${ELASTICSEARCH_RPM_FILE} ${OM_PKG_DIRNAME}/ ;\
    else \
        curl https://artifacts.elastic.co/downloads/elasticsearch/${ELASTICSEARCH_RPM_NAME} -o ${OM_PKG_DIRNAME}/${ELASTICSEARCH_RPM_NAME} ;\
    fi

rpm-logstash:
	@if [ -f ${OM_PKG_DIRNAME}/${LOGSTASH_RPM_NAME} ]; then \
        echo "Local Logstash RPM exists, skip downloading" ;\
    elif [ -f ${LOGSTASH_RPM_FILE} ]; then \
        echo "Copying Logstash RPM from TOOLSDIR" ;\
        cp -f ${LOGSTASH_RPM_FILE} ${OM_PKG_DIRNAME}/ ;\
    else \
        curl https://artifacts.elastic.co/downloads/logstash/${LOGSTASH_RPM_NAME} -o ${OM_PKG_DIRNAME}/${LOGSTASH_RPM_NAME} ;\
    fi

grafana-plugin:
	@if [ -d ${GRAFANA_PLUGIN_DIRNAME} ]; then \
        echo "Local Grafana plugins folder exists, skip downloading" ;\
    elif [ -d ${GRAFANA_PLUGIN_DIR} ]; then \
        echo "Copying Grafana plugins from TOOLSDIR" ;\
        mkdir -p ${GRAFANA_PLUGIN_DIRNAME}; \
        cp -rf ${GRAFANA_PLUGIN_DIR}/* ${GRAFANA_PLUGIN_DIRNAME}/ ;\
    else \
        echo "Downloading Grafana plugins from github" ;\
        mkdir -p ${GRAFANA_PLUGIN_DIRNAME}; \
        cd ${GRAFANA_PLUGIN_DIRNAME} && git clone https://github.com/foursquare/datasource-plugin-clouderamanager.git; \
        cd ${GRAFANA_PLUGIN_DIRNAME} && git clone https://github.com/grafana/piechart-panel.git; \
    fi

pkg-xlwt:
	@if [ -f ${XLWT_TAR_NAME} ]; then \
        echo "Local xlwt package exists, skip downloading" ;\
    elif [ -d ${XLWT_TAR_FILE} ]; then \
        echo "Copying xlwt package from TOOLSDIR" ;\
        cp -rf ${XLWT_TAR_FILE} . ;\
    else \
        echo "Downloading xlwt package" ;\
        curl https://files.pythonhosted.org/packages/06/97/56a6f56ce44578a69343449aa5a0d98eefe04085d69da539f3034e2cd5c1/${XLWT_TAR_NAME} -o ${XLWT_TAR_NAME} ;\
    fi
	tar -xf ${XLWT_TAR_NAME} && cp -rf ${XLWT_DIR_NAME}/xlwt scripts/ && rm -rf ${XLWT_DIR_NAME}

pkg-crontab:
	@if [ -f ${CRONTAB_TAR_NAME} ]; then \
        echo "Local python-crontab package exists, skip downloading" ;\
    elif [ -d ${CRONTAB_TAR_FILE} ]; then \
        echo "Copying python-crontab package from TOOLSDIR" ;\
        cp -rf ${CRONTAB_TAR_FILE} . ;\
    else \
        echo "Downloading python-crontab package" ;\
        curl https://files.pythonhosted.org/packages/a9/f1/96b7d9ad7b591908a5c9b02e4c7c6cb82425da7e2cd0a831815ca196046b/${CRONTAB_TAR_NAME} -o ${CRONTAB_TAR_NAME} ;\
    fi
	tar -xf ${CRONTAB_TAR_NAME} && cd ${CRONTAB_DIR_NAME} && ./setup.py bdist_rpm
	cp -f ${CRONTAB_DIR_NAME}/dist/${CRONTAB_RPM} ${OM_PKG_DIRNAME}

clean:
	rm -rf ${OM_PKG_DIRNAME} ${OM_TARNAME} ${GRAFANA_PLUGIN_DIRNAME} ${DBMGR_DIRNAME}
	rm -rf scripts/xlwt ${XLWT_DIR_NAME} ${XLWT_TAR_NAME} ${CRONTAB_TAR_NAME} ${CRONTAB_DIR_NAME}
