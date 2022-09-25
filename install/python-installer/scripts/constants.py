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
DEF_SERVER_PORT = '4205'
DEF_LOG_LEVEL = logging.DEBUG

AES_KEY_FILE = '/etc/dbmgr-server/config.xml'
AGENT_CFG_FILE = '/etc/dbmgr-agent/agent.ini'
DEF_LOG_FILE = '/var/log/dbmgr-agent/agent.log'
DEF_PID_FILE = '/var/run/dbmgr-agent.pid'

# heartbeat interval
DEF_HEARTBEAT_INTERVAL = 5
HEARTBEAT_INTERVAL_MIN = 1
HEARTBEAT_INTERVAL_MAX = 30

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

# discover items when agent register to server
CAT_REG = [
    'get_cpu_cores',
    'get_disk_nums',
    'get_user_disk_free',
    'get_install_disk_free',
    'get_mem_total',
    'get_mem_free',
    'get_swap_pct',
    'get_ext_interface',
    'get_net_interfaces',
    'get_net_bw',
]

# discover items when do installation
CAT_INSTALL = [
    'get_default_java',
    'get_linux_distro',
    'get_firewall_status',
    'get_traf_status',
    'get_license_status',
    'get_aws_ec2',
    'get_kernel_ver',
    'get_hbase_ver',
    'get_home_dir',
    'get_hadoop_auth',
    'get_hadoop_group_mapping'
]


### installer configs ###
DEF_LOG_LEVEL = logging.INFO

LOCAL_REPO_PTR = """
[traflocal]
name=trafodion local repo
baseurl=%s/
enabled=1
gpgcheck=0
"""

REPO_NAME = 'traflocal'
REPO_FILE = '/etc/yum.repos.d/%s.repo' % REPO_NAME
EPEL_FILE = '/etc/yum.repos.d/epel.repo'

OS_SUSE = 'SUSE'
OS_UBUNTU = 'UBUNTU'
OS_KYLIN = 'KYLIN'
OS_CENTOS = 'CENTOS'
OS_REDHAT = 'REDHAT'
CENTOS6_MAJOR_VER = '6'
CENTOS7_MAJOR_VER = '7'
REDHAT6_MAJOR_VER = '6'
REDHAT7_MAJOR_VER = '7'
KYLIN10_MAJOR_VER = '10'

INSTALLER_LOC = re.search('(.*)/\w+', os.path.dirname(os.path.abspath(__file__))).groups()[0]

CONFIG_DIR = INSTALLER_LOC + '/configs'
SCRIPTS_DIR = INSTALLER_LOC + '/scripts'
TEMPLATES_DIR = INSTALLER_LOC + '/templates'

TEMPLATE_XML = TEMPLATES_DIR + '/templates.xml'
DEMO_LICENSE = TEMPLATES_DIR + '/demo.license'
HBASE_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/hbase-site-template.xml'
HDFS_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/hdfs-site-template.xml'
YARN_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/yarn-site-template.xml'
YARN_HA_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/yarn-ha-site-template.xml'
MAPRED_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/mapred-site-template.xml'
HIVE_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/hive-site-template.xml'
ZOO_CFG_TEMPLATE_XML = TEMPLATES_DIR + '/zoo-cfg-template.xml'
CORE_SITE_TEMPLATE_XML = TEMPLATES_DIR + '/core-site-template.xml'
MY_CFG_TEMPLATE_INI = TEMPLATES_DIR + '/my-cnf-template.ini'

USER_PROMPT_FILE = CONFIG_DIR + '/prompt.json'
SCRCFG_FILE = CONFIG_DIR + '/script.json'
CENTRAL_CFG_FILE = CONFIG_DIR + '/central_config.json'
MODCFG_FILE = CONFIG_DIR + '/mod_cfgs.json'
#CUSTOM_MODCFG_FILE = CONFIG_DIR + '/custom_mod_cfgs.json'
CUSTOM_MODCFG_FILE_AP = CONFIG_DIR + '/custom_mod_ap_cfgs.json'
CUSTOM_MODCFG_FILE_TP = CONFIG_DIR + '/custom_mod_tp_cfgs.json'
CUSTOM_MOD_HA_CFG_FILE = CONFIG_DIR + '/custom_mod_ha_cfgs.json'

DEF_PORT_FILE = CONFIG_DIR + '/default_ports.ini'
CHECK_PORTS = CONFIG_DIR + '/check_ports_list.ini'
#MSENV_CUSTOM_FILE = CONFIG_DIR + '/custom_ms_env.ini'
MSENV_CUSTOM_FILE_AP = CONFIG_DIR + '/custom_ms_ap_env.ini'
MSENV_CUSTOM_FILE_TP = CONFIG_DIR + '/custom_ms_tp_env.ini'
#CUSTOM_CQD_FILE = CONFIG_DIR + '/custom_cqd.sql'
CUSTOM_CQD_FILE_AP = CONFIG_DIR + '/custom_ap_cqd.sql'
CUSTOM_CQD_FILE_TP = CONFIG_DIR + '/custom_tp_cqd.sql'

DBCFG_FILE = INSTALLER_LOC + '/db_config'
DBCFG_TMP_FILE = INSTALLER_LOC + '/.db_config_temp'
STATUS_FILE = INSTALLER_LOC + '/install.status'
ALLSTATUS_FILE = INSTALLER_LOC + '/allinstall.status'

DEF_LOG_DIR = '/var/log/trafodion/install'

SSH_CFG_FILE = '/etc/ssh/sshd_config'
SYSCTL_CONF_FILE = '/etc/sysctl.conf'

TMP_SSHKEY_FILE = '/tmp/id_rsa'
TMP_TRAF_PKG_FILE = '/tmp/traf_bin.tar.gz'
TMP_CFG_DIR = '/tmp/conf'
TMP_MSENV_FILE = '/tmp/ms.env'
TMP_CFG_FILE = '/tmp/trafodion_config'
TMP_DIR = '/tmp/.trafodion_install_temp'
TMP_LIC_FILE = '/tmp/esgyndb_license'
TMP_ID_FILE = '/tmp/esgyndb_id'
TMP_HADOOP_DIR = '/tmp/hadoop_pkg'
TMP_OMCLIENT_DIR = '/tmp/omclient'

DEF_TRAF_HOME_DIR = '/opt/trafodion'
DEF_TRAF_HOME = '/opt/trafodion/esgyndb'
DEF_HADOOP_HOME = '/usr'
DEF_HBASE_HOME = '/usr'
DEF_HIVE_HOME = '/usr'
DEF_INSTALL_ZK_HOME = '/opt/zookeeper'
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

HADOOP_HDFS = 'hadoop-hdfs-%s.jar'
HBASE_SERVER = 'hbase-server-%s.jar'
HBASE_COMMON = 'hbase-common-%s.jar'
HBASE_CLIENT = 'hbase-client-%s.jar'

TRAF_HSPERFDATA_FILE = '/tmp/hsperfdata_trafodion'
TRAF_SUDOER_FILE = '/etc/sudoers.d/trafodion'
SUDOER_FILE = '/etc/sudoers'
TRAF_SUDOER_DIR = '/etc/sudoers.d'
TRAF_CFG_DIR = '/etc/trafodion/conf/'
TRAF_CFG_FILE = '/etc/trafodion/trafodion_config'
TRAF_USER = 'trafodion'

EDB_LIC_DIR = '/etc/trafodion/'
EDB_LIC_FILE = EDB_LIC_DIR + 'esgyndb_license'
EDB_ID_FILE = EDB_LIC_DIR + 'esgyndb_id'
EDB_CGROUP_NAME = 'Esgyn'
OVERFLOW_DIR_NAME = 'overflow'
DFS_DIR_NAME = 'dfs'

CGRULES_CONF_FILE = '/etc/cgrules.conf'
ULIMITS_DIR = '/etc/security/limits.d'

### pre-install config ###
CM_PORT = 7180

HOST_FILE = '/etc/hosts'
SELINUX_FILE = '/etc/selinux/config'
NTP_CONF_FILE = '/etc/ntp.conf'
CHRONY_CONF_FILE = '/etc/chrony.conf'

PREINSTALL_CFG_FILE = INSTALLER_LOC + '/preinstall_config'
PREINSTALL_TMP_FILE = INSTALLER_LOC + '/.preinstall_config_temp'
PREINSTALL_STATUS_FILE = INSTALLER_LOC + '/preinstall.status'

DBCFG_FILE_DEF = CONFIG_DIR + '/db_config_default.ini'
SCALING_GOVERNOR = '/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'


CDH_AGENT_CFG_FILE = '/etc/cloudera-scm-agent/config.ini'
CDH_REPO_FILE = '/etc/yum.repos.d/cmlocal.repo'
CDH_CFG_FILE = CONFIG_DIR + '/cdh_config.ini'
CDH_LOCAL_REPO_PTR = """
[cmlocal]
name=cloudera manager local repo
baseurl=%s/
enabled=1
gpgcheck=0
"""
BASE_REPO_PTR = """
[baselocal]
name=base local repo
baseurl=%s/
enabled=1
gpgcheck=0
"""

### security setup configs
SECURECFG_FILE = INSTALLER_LOC + '/secure_config'
SECURECFG_TMP_FILE = INSTALLER_LOC + '/.secure_config_temp'

### LDAP
LDAP_DB_CONFIG_EXAMPLE = '/usr/share/openldap-servers/DB_CONFIG.example'
LDAP_DB_CONFIG_DIR = '/var/lib/ldap/'
LDAP_DB_CONFIG = LDAP_DB_CONFIG_DIR + 'DB_CONFIG'
LDAP_CONFIG_DIR = '/etc/openldap'

### SSH
SSH_CFG_ITEMS = 'StrictHostKeyChecking=no\nNoHostAuthenticationForLocalhost=yes\n'
TMP_SSH_DIR = '/tmp/.ssh'
SSH_CFG = TMP_SSH_DIR + '/config'
PRIVATE_KEY = TMP_SSH_DIR + '/id_rsa'
PUB_KEY = TMP_SSH_DIR + '/id_rsa.pub'
AUTH_KEY = TMP_SSH_DIR + '/authorized_keys'


