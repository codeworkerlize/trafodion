#!/usr/bin/env python

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

### The common constants ###

import os
import re
import logging

### agent configs ###
CMD_TIMEOUT_SEC = 300 # seconds
AGENT_CFG_SECTION = 'agent'
DEF_SERVER_HOST = 'localhost'
DEF_SERVER_PORT = '4203'
DEF_LOG_LEVEL = logging.DEBUG
REMOTE_SERVER_PORT = '4205'

# command status
STAT_IN_PROGRESS = 'IN_PROGRESS'
STAT_SUCCESS = 'SUCCESS'
STAT_ERROR = 'ERROR'
STAT_CANCELED = 'CANCELED'
STAT_UNKNOWN = 'UNKNOWN'

NA = 'N/A' # not available
OK = 'ok'
WARN = 'warn'
ERR = 'error'
UNKNOWN = 'unknown'

### installer configs ###
DEF_LOG_LEVEL = logging.INFO

OS_SUSE = 'SUSE'
OS_UBUNTU = 'UBUNTU'
OS_CENTOS = 'CENTOS'
OS_REDHAT = 'REDHAT'
CENTOS6_MAJOR_VER = '6'
CENTOS7_MAJOR_VER = '7'
REDHAT6_MAJOR_VER = '6'
REDHAT7_MAJOR_VER = '7'

INSTALLER_LOC = re.search('(.*)/\w+', os.path.dirname(os.path.abspath(__file__))).groups()[0]

CONFIG_DIR = INSTALLER_LOC + '/configs'
SCRIPTS_DIR = INSTALLER_LOC + '/scripts'
TEMPLATES_DIR = INSTALLER_LOC + '/templates'

TEMPLATE_XML = TEMPLATES_DIR + '/templates.xml'
USER_PROMPT_FILE = CONFIG_DIR + '/prompt.json'
SCRCFG_FILE = CONFIG_DIR + '/script.json'
CENTRAL_CFG_FILE = CONFIG_DIR + '/central_config.json'

DBCFG_FILE = INSTALLER_LOC + '/db_config'
DBCFG_TMP_FILE = INSTALLER_LOC + '/.db_config_temp'
STATUS_FILE = INSTALLER_LOC + '/install.status'

DEF_LOG_DIR = '/var/log/trafodion'

SSH_CFG_FILE = '/etc/ssh/sshd_config'
SYSCTL_CONF_FILE = '/etc/sysctl.conf'

TMP_SSHKEY_FILE = '/tmp/id_rsa'
TMP_TRAF_PKG_FILE = '/tmp/traf_bin.tar.gz'
TMP_CFG_DIR = '/tmp/conf'
TMP_CFG_FILE = '/tmp/trafodion_config'
TMP_DIR = '/tmp/.trafodion_install_temp'
TMP_LIC_FILE = '/tmp/esgyndb_license'
TMP_ID_FILE = '/tmp/esgyndb_id'

DEF_TRAF_HOME_DIR = '/opt/trafodion'
DEF_TRAF_HOME = '/opt/trafodion/esgyndb'
DEF_HADOOP_HOME = '/usr'
DEF_HBASE_HOME = '/usr'
DEF_HIVE_HOME = '/usr'
DEF_INSTALL_HADOOP_HOME = '/opt/hadoop'
DEF_INSTALL_HBASE_HOME = '/opt/hbase'
DEF_INSTALL_HIVE_HOME = '/opt/hive'
DEF_HBASE_SITE_XML = '/etc/hbase/conf/hbase-site.xml'
DEF_CORE_SITE_XML = '/etc/hadoop/conf/core-site.xml'
DEF_HIVE_SITE_XML = '/etc/hive/conf/hive-site.xml'
DEF_HDFS_BIN = '/usr/bin/hdfs'
DEF_HBASE_LIB = '/usr/lib/hbase/lib'
HDP_HBASE_LIB = '/usr/hdp/current/hbase-regionserver/lib'
PARCEL_DIR = '/opt/cloudera/parcels'
PARCEL_HBASE_LIB = PARCEL_DIR + '/CDH/lib/hbase/lib'
PARCEL_HDFS_BIN = PARCEL_DIR + '/CDH/bin/hdfs'

TRAF_HSPERFDATA_FILE = '/tmp/hsperfdata_trafodion'
TRAF_SUDOER_FILE = '/etc/sudoers.d/trafodion'
TRAF_SUDOER_DIR = '/etc/sudoers.d'
TRAF_CFG_DIR = '/etc/trafodion/conf/'
TRAF_CFG_FILE = '/etc/trafodion/trafodion_config'
TRAF_USER = 'trafodion'

### Prometheus and Grafana config
OMINSTALL_CFG_FILE = INSTALLER_LOC + '/ominstall_config'
OMINSTALL_TMP_FILE = INSTALLER_LOC + '/.ominstall_config_temp'
OMINSTALL_STATUS_FILE = INSTALLER_LOC + '/ominstall.status'
NODE_EXPORTER = 'node_exporter'
ESGYN_EXPORTER = 'esgyn_exporter'
EXPORTER_DIR = INSTALLER_LOC + '/exporter/'
NODE_EXPORTER_FILE = EXPORTER_DIR + NODE_EXPORTER
ESGYN_EXPORTER_FILE = EXPORTER_DIR + ESGYN_EXPORTER
OM_PKG_DIRNAME = 'om_pkgs'
OM_DIR = INSTALLER_LOC + '/' + OM_PKG_DIRNAME

REPO_DIR = '/etc/yum.repos.d/'
PRO_REPO_FILE = REPO_DIR + 'om_tools.repo'
TMP_REPO_FILE = '/tmp/om_tools.repo'
LOCAL_REPO = """
[prometheus]
name=prometheus
baseurl=%s
enable=1
gpgcheck=0
"""

PRO_CFG_DIR = '/etc/prometheus'
GRAFANA_PLUGS = INSTALLER_LOC + '/grafana-plugins'
TMP_GRAFANA_PLUGS = '/tmp/grafana-plugins'

### LDAP
LDAP_DB_CONFIG_EXAMPLE = '/usr/share/openldap-servers/DB_CONFIG.example'
LDAP_DB_CONFIG_DIR = '/var/lib/ldap/'
LDAP_DB_CONFIG = LDAP_DB_CONFIG_DIR + 'DB_CONFIG'
LDAP_CONFIG_DIR = '/etc/openldap'

### Off Instance DB Manager
DBMGR_PKGS = INSTALLER_LOC + '/dbmgr'
TMP_DBMGR_PKGS = '/tmp/dbmgr'
DEF_DBMGR_DIR = '/opt/dbmgr'
