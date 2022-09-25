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

### installer configs ###
DEF_LOG_LEVEL = logging.INFO

INSTALLER_LOC = re.search('(.*)/\w+', os.path.dirname(os.path.abspath(__file__))).groups()[0]

CONFIG_DIR = INSTALLER_LOC + '/configs'
SCRIPTS_DIR = INSTALLER_LOC + '/scripts'

USER_PROMPT_FILE = CONFIG_DIR + '/prompt.json'
SCRCFG_FILE = CONFIG_DIR + '/script.json'
CENTRAL_CFG_FILE = CONFIG_DIR + '/central_config.json'
MODCFG_FILE = CONFIG_DIR + '/mod_cfgs.json'

WATCHDOG = '/watchdog-5.13-12.el7.x86_64.rpm'
TMP_WATCHDOG_RPM = '/tmp' + WATCHDOG
WATCHDOG_RPM = INSTALLER_LOC + WATCHDOG
HADOOP_HDFS = 'hadoop-hdfs-%s.jar'
HBASE_SERVER = 'hbase-server-%s.jar'
HBASE_CLIENT = 'hbase-client-%s.jar'

ADDON_CFG_FILE = INSTALLER_LOC + '/add_on_config'
ADDON_CFG_TMP_FILE = INSTALLER_LOC + '/.add_on_config_temp'
LOCKSETUP_CFG_FILE = INSTALLER_LOC + '/lock_setup_config'
LOCKSETUP_CFG_TMP_FILE = INSTALLER_LOC + '/.lock_setup_config_temp'
STATUS_FILE = INSTALLER_LOC + '/install.status'

DEF_LOG_DIR = '/var/log/trafodion/install'

TMP_TRAF_PKG_FILE = '/tmp/traf_bin.tar.gz'
TMP_CFG_DIR = '/tmp/conf'
TMP_DIR = '/tmp/.trafodion_install_temp'

PARCEL_DIR = '/opt/cloudera/parcels'

TRAF_SUDOER_FILE = '/etc/sudoers.d/trafodion'
TRAF_SUDOER_DIR = '/etc/sudoers.d'
TRAF_CFG_DIR = '/etc/trafodion/conf/'
TRAF_CFG_FILE = '/etc/trafodion/trafodion_config'

EDB_LIC_DIR = '/etc/trafodion/'
EDB_LIC_FILE = EDB_LIC_DIR + 'esgyndb_license'
EDB_ID_FILE = EDB_LIC_DIR + 'esgyndb_id'

IPMI_HOST_MAP = TRAF_CFG_DIR + 'ipmi_host_map'