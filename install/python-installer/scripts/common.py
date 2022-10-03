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

### The common functions ###

import os
import pty
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
import re
import fnmatch
import time
import base64
import subprocess
import logging
import socket
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from ConfigParser import ConfigParser
from collections import defaultdict
from constants import *
try:
    from Crypto.Cipher import AES
    from binascii import b2a_hex, a2b_hex
    ENCRP_MODULE = True
except ImportError:
    ENCRP_MODULE = False

def ok(msg):
    print '\n\33[32m***[OK]: %s \33[0m' % msg

def info(msg):
    print '\n\33[33m***[INFO]: %s \33[0m' % msg

def warn(msg):
    print '\n\33[35m***[WARN]: %s \33[0m' % msg

def err_m(msg):
    """ used by main script """
    sys.stderr.write('\n\33[31m***[ERROR]: %s \33[0m\n' % msg)
    sys.exit(1)

def err(msg):
    """ used by sub script """
    logger = logging.getLogger()
    logger.error(msg)
    sys.exit(1)

def set_stream_logger(logger, log_level=DEF_LOG_LEVEL):
    logger.setLevel(log_level)

    if log_level == logging.DEBUG:
        #formatter = logging.Formatter('[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] - %(message)s')
        formatter = logging.Formatter('[%(levelname)s %(filename)s:%(lineno)d] - %(message)s')
    else:
        #formatter = logging.Formatter('[%(asctime)s %(levelname)s] - %(message)s')
        formatter = logging.Formatter('[%(levelname)s] - %(message)s')

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)

    logger.addHandler(sh)

def set_logger(logger, log_file, log_level=DEF_LOG_LEVEL):
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        #os.mkdir(log_dir)
        run_cmd('%s mkdir -p %s' % (get_sudo_prefix(), log_dir))
        run_cmd('%s chmod 777 %s' % (get_sudo_prefix(), log_dir))

    logger.setLevel(log_level)

    formatter = logging.Formatter('[%(asctime)s %(levelname)s]: %(message)s')

    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)

    logger.addHandler(fh)

def run_cmd(cmd, timeout=-1, logger_output=True):
    """ check command return value and return stdout """
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    start_t = time.time()

    def _cmd():
        stdout, stderr = p.communicate()
        debug_output = 'Execute command [%s], Output: ' % cmd
        if stdout:
            debug_output += '[%s]' % stdout.strip()
        else:
            debug_output += '[]'
        if logger_output:
            logger = logging.getLogger()
            logger.info(debug_output)

        if p.returncode != 0:
            msg = stderr if stderr else stdout
            if logger_output:
                err('Failed to run command %s: %s' % (cmd, msg))
            else:
                err_m('Failed to run command %s: %s' % (cmd, msg))
        return stdout

    if timeout > 0:
        while True:
            if p.poll() is not None:
                stdout = _cmd()
                break

            duration_t = time.time() - start_t
            if duration_t > timeout:
                p.kill()
                err('Timeout (%d)s exit on cmd [%s]' % (timeout, cmd))
                break
            time.sleep(0.1)
    else:
        stdout = _cmd()

    return stdout.strip()

def run_cmd2(cmd):
    """ return command return code """
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    p.communicate()
    return p.returncode

def run_cmd3(cmd):
    """ return command return code """
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    stdout, stderr = p.communicate()
    return p.returncode, stdout + stderr

def run_cmd_as_user(user, cmd, timeout=-1, logger_output=True):
    return run_cmd('%s su - %s -c \'%s\'' % (get_sudo_prefix(), user, cmd), timeout=timeout, logger_output=logger_output)

def cmd_output_as_user(user, cmd):
    return cmd_output('%s su - %s -c \'%s\'' % (get_sudo_prefix(), user, cmd))

def cmd_output(cmd, oneLine=True):
    """ return command output but not check return value """
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    stdout, stderr = p.communicate()
    if stdout:
    #    debug_output = 'Command output [%s], Output: [%s] ' % (cmd, stdout.strip())
    #    logger = logging.getLogger()
    #    logger.debug(debug_output)
        if oneLine == True:
            return stdout.strip()
        else:
            return stdout
    else:
        return stderr

def get_default_home():
    return cmd_output('%s cat /etc/default/useradd |grep HOME |cut -d "=" -f 2' % get_sudo_prefix())

def get_sudo_prefix():
    """ donnot use sudo prefix if user is root """
    uid = os.getuid()
    if uid == 0:
        return ''
    else:
        return 'sudo -n'

def get_timezone():
    cmd = """
if [ -f /etc/timezone ]; then
  TIMEZONE=`cat /etc/timezone`
elif [ -h /etc/localtime ]; then
  TIMEZONE=`readlink /etc/localtime | sed "/posix/d;s/.*\/usr\/share\/zoneinfo\///"`
else
  checksum=`md5sum /etc/localtime | cut -d' ' -f1`
  TIMEZONE=`find /usr/share/zoneinfo/ -maxdepth 1 -type f -exec md5sum {} \; | grep "^$checksum" | sed "/posix/d;s/.*\/usr\/share\/zoneinfo\///"`
fi
echo $TIMEZONE | tr " " "\n" |tail -1
"""
    timezone = cmd_output(cmd)
    if not timezone: timezone = 'UTC'
    return timezone

def gen_template_file(template, dest_file, change_items, doublebrace=True):
    """ input: template string
        output: file
    """
    for key, value in change_items.iteritems():
        if type(value) == dict: continue
        if doublebrace:
            if not value: value = ''
            template = re.sub('{{\s*%s\s*}}' % key, value, template)
        else:
            template = template.replace(key, value)

    # replace the ones not defined in change_items
    if doublebrace:
        template = re.sub('{{\s*\w+\s*}}', '', template)

    with open(dest_file, 'w') as f:
        f.write(template)
    run_cmd('chmod a+r %s' % dest_file) # umask

def mod_file(template_file, change_items):
    """
        @params: change_items: a dict includes:
        {regular_expression : replace_string}
    """
    try:
        with open(template_file, 'r') as f:
            lines = f.read()
    except IOError:
        err('Failed to open file %s to modify' % template_file)

    for regexp, replace in change_items.iteritems():
        lines = re.sub(regexp, replace, lines)

    with open(template_file, 'w') as f:
        f.write(lines)

def replace_file(template_file, change_items):
    """
        @params: change_items: a dict includes:
        {regular_expression : replace_string}
    """
    try:
        with open(template_file, 'r') as f:
            lines = f.read()
    except IOError:
        err('Failed to open file %s to modify' % template_file)

    for regexp, replace in change_items.iteritems():
        #FIXME: marge flags into mod_file
        lines = re.sub(regexp, replace, lines, flags=re.MULTILINE)

    with open(template_file, 'w') as f:
        f.write(lines)

def append_file(template_file, string, position='', upper=False):
    try:
        with open(template_file, 'r') as f:
            lines = f.readlines()
        pos = 0
        offset = 0 if upper else 1
        if position:
            for index, line in enumerate(lines):
                if position in line:
                    pos = index + offset

        if pos == 0: pos = len(lines)
        string_lf = string + '\n'
        newlines = lines[:pos] + [string_lf] + lines[pos:]
        different_string = set(string_lf.split('\n')).difference(set([line.replace('\n','') for line in lines]))
        different_string = different_string.difference(set(['']))
        if different_string:
            with open(template_file, 'w') as f:
                f.writelines(newlines)
    except IOError:
        err('Failed to open file %s to append' % template_file)

def write_file(template_file, string):
    try:
        with open(template_file, 'w') as f:
            f.write(string)
    except IOError:
        err('Failed to open file %s to write' % template_file)

class AESCipher():
    def __init__(self, key=''):
        if ENCRP_MODULE:
            # key length: 16(AES-128), 24(AES-192), 32(AES-256) Bytes
            self.__key = self._get_key() if not key else key
            self.mode = AES.MODE_ECB
            # PKCS5Padding
            BS = 16
            self.pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
            self.unpad = lambda s : s[0:-ord(s[-1])]

    def _get_key(self):
        entry = 'aesPassword'
        key_string = cmd_output('grep %s %s' % (entry, AES_KEY_FILE))
        try:
            key = re.search(r'"%s">(.*)</entry>' % entry, key_string).groups()[0]
        except AttributeError:
            err('Failed to get AES encryption key from dbmgr config file')
        return key

    def _create_key(self):
        if not os.path.exists(AES_KEY_FILE):
            with open(AES_KEY_FILE, 'w') as f:
                key = b2a_hex(os.urandom(8))
                f.write(key)
        else:
            with open(AES_KEY_FILE, 'r') as f:
                key = f.read().strip()
        return key

    # text should be multiple of 16
    def encrypt(self, text):
        if ENCRP_MODULE:
            cryptor = AES.new(self.__key, self.mode)
            cipher_text = cryptor.encrypt(self.pad(text))
            return b2a_hex(cipher_text)
        else:
            return text

    def decrypt(self, text):
        if not text: return ''
        if ENCRP_MODULE:
            try:
                cryptor = AES.new(self.__key, self.mode)
                plain_text = cryptor.decrypt(a2b_hex(text))
                return self.unpad(plain_text)
            except:
                return text
        else:
            return text

class License(object):
    def __init__(self, license_str):
        self.ispath = False
        self.nolic = False
        license_str = license_str.strip()
        if os.path.exists(license_str):
            self.param = '-f %s' % license_str
            self.ispath = True
        elif (len(license_str) == 64 or len(license_str) == 128) and not '/' in license_str:
            self.param = '-s %s' % license_str
        elif license_str == 'NONE' or (license_str == EDB_LIC_FILE and not os.path.exists(license_str) and os.path.exists(EDB_ID_FILE)):
        #elif license_str == 'NONE':
            self.nolic = True
        else:
            raise StandardError('Invalid license file or license string')

        self.decoder = '%s/decoder' % SCRIPTS_DIR
        if not os.path.exists(self.decoder):
            raise StandardError('Missing decoder program')

        if 'Invalid License' in self.get_edition():
            raise StandardError('Invalid license')

    def gen_id(self, path):
        run_cmd('%s -g > %s' % (self.decoder, path), logger_output=False)

    def _lic_cmd(self, check_item):
        if self.nolic: return '0'
        return cmd_output('%s -%s %s' % (self.decoder, check_item, self.param))

    def is_multi_tenancy_enabled(self):
        return int(self._lic_cmd('x')) & 4 != 0

    def get_license_type(self):
        return self._lic_cmd('t')

    def get_license_ver(self):
        return int(self._lic_cmd('v'))

    def get_supported_node_nums(self):
        return int(self._lic_cmd('n'))

    def get_edition(self):
        return self._lic_cmd('p')

    def get_expired_day(self):
        return int(self._lic_cmd('e'))

    def check_license(self, node_nums):
        if self.nolic: return
        if self.get_license_type() != 'INTERNAL':
            #check support node number
            supported_node_nums = self.get_supported_node_nums()
            if int(node_nums) > supported_node_nums:
                raise StandardError('License invalid: Current number of nodes(%d) does not match allowed number of nodes(%d)' % (node_nums, supported_node_nums))

        #check expired day
        expired_day = self.get_expired_day()
        current_day = int(time.time() / 86400)
        days_left = expired_day - current_day
        if days_left <= 0:
            raise StandardError('License expired!')
        elif days_left < 30:
            warn('Days left of license %s' % days_left)

class HadoopDiscover(object):
    """ discover for hadoop related info """
    def __init__(self, user, pwd, url, cluster_name=''):
        self.cluster_nodes = []
        self.rsnodes = []
        self.users = {}
        self.hg = ParseHttp(user, pwd)
        self.url = url
        self.v1_url = '%s/api/v1/clusters' % self.url
        self.v6_url = '%s/api/v6/clusters' % self.url
        self.v1_info = self.hg.get(self.v1_url)

        if not cluster_name:
            self.cluster_name = self.get_def_cluster_name()
        else:
            self.cluster_name = cluster_name
        self.cluster_url = '%s/%s' % (self.v1_url, self.cluster_name.replace(' ', '%20'))
        self.v6_cluster_url = '%s/%s' % (self.v6_url, self.cluster_name.replace(' ', '%20'))
        self._get_distro()
        self._check_version()
        if 'CDH' in self.distro:
            self.cm = self.hg.get('%s/api/v6/cm/deployment' % self.url)

    def get_cdh_cluster_names(self):
        # loop all managed clusters
        try:
            cluster_names = []
            for cluster in self.v1_info['items']:
                cluster_names.append(cluster['name'])
            return cluster_names
        except (IndexError, KeyError):
            return []

    def get_def_cluster_name(self):
        try:
            cluster_name = self.v1_info['items'][0]['name']
        except (IndexError, KeyError):
            try:
                cluster_name = self.v1_info['items'][0]['Clusters']['cluster_name']
            except (IndexError, KeyError):
                err_m('Failed to get cluster info from management url')
        return cluster_name

    def _get_distro(self):
        content = self.v1_info
        if content['items'][0].has_key('name'):
            # use v6 rest api for CDH to get fullversion
            content = self.hg.get(self.v6_url)

        # loop all managed clusters
        for cluster in content['items']:
            try:
                # HDP
                self.distro = cluster['Clusters']['version']
            except KeyError:
                # CDH
                try:
                    if self.cluster_name == cluster['displayName']:
                        self.distro = 'CDH' + cluster['fullVersion']
                        break
                except KeyError:
                    err_m('Failed to get hadoop distribution info from management url')

    def get_hive_authorization(self):
        hive_authorzation = 'NONE'
        hive_srvname = self.get_hive_srvname()
        if 'CDH' in self.distro and hive_srvname:
            cfg = self.hg.get('%s/services/%s/config' % (self.cluster_url, hive_srvname))
            if cfg.has_key('items'):
                for item in cfg['items']:
                    if item['name'] == 'sentry_service':
                        hive_authorzation = item['value']
        elif 'HDP' in self.distro:
            # will support HDP in the future if edb supports ranger
            pass
        return hive_authorzation

    def get_hiveserver2_url(self):
        hiveSrv2_hostid = ''
        hiveSrv2_hostname = 'localhost'
        hiveSrv2_port = '10000'
        hiveSrv2_rolename = ''
        hive_srvname = self.get_hive_srvname()

        if 'CDH' in self.distro and hive_srvname:

            allHiveRoles = self.hg.get('%s/services/%s/roles' % (self.cluster_url, hive_srvname))
            if allHiveRoles.has_key('items'):
                for item in allHiveRoles['items']:
                    if item['type'] == 'HIVESERVER2':
                        hiveSrv2_rolename = item['name']
                        hiveSrv2_hostid = item['hostRef']['hostId']
                        break

            hiveSrv2_host = self.hg.get('%s/api/v6/hosts/%s' % (self.url, hiveSrv2_hostid))
            hiveSrv2_hostname = hiveSrv2_host['hostname']

            hiveSiteXml = self.hg.get('%s/services/%s/roles/%s/process/configFiles/hive-site.xml' % (self.v6_cluster_url, hive_srvname, hiveSrv2_rolename), 'xml')
            hsx = ParseXML(hiveSiteXml, 'tree')
            hiveSrv2_port = hsx.get_property('hive.server2.thrift.port')

        elif 'HDP' in self.distro and hive_srvname:

            hiveSrv2_host = self.hg.get('%s/services/HIVE/components/HIVE_SERVER' % (self.cluster_url))
            if hiveSrv2_host.has_key('host_components'):
                for item in hiveSrv2_host['host_components']:
                    if item['HostRoles']['component_name'] == 'HIVE_SERVER':
                        hiveSrv2_hostname = item['HostRoles']['host_name']
                        break

            hsx = ParseXML(DEF_HIVE_SITE_XML)
            hiveSrv2_port = hsx.get_property('hive.server2.thrift.port')

        return hiveSrv2_hostname + ':' + hiveSrv2_port

    def get_hive_principal(self):
        hive_principal = ''
        hive_srvname = self.get_hive_srvname()

        if 'CDH' in self.distro and hive_srvname:

            hiveSrv2_rolename = ''
            allHiveRoles = self.hg.get('%s/services/%s/roles' % (self.v6_cluster_url, hive_srvname))
            if allHiveRoles.has_key('items'):
                for item in allHiveRoles['items']:
                    if item['type'] == 'HIVESERVER2':
                        hiveSrv2_rolename = item['name']
                        break

            hiveSiteXml = self.hg.get('%s/services/%s/roles/%s/process/configFiles/hive-site.xml' % (self.v6_cluster_url, hive_srvname, hiveSrv2_rolename), 'xml')
            hsx = ParseXML(hiveSiteXml, 'tree')
            hive_principal = hsx.get_property('hive.server2.authentication.kerberos.principal')

        elif 'HDP' in self.distro and hive_srvname:

            hsx = ParseXML(DEF_HIVE_SITE_XML)
            hive_principal = hsx.get_property('hive.server2.authentication.kerberos.principal')

        return hive_principal

    def get_hdfs_srvname(self):
        return self._get_service_name('HDFS')

    def get_hbase_srvname(self):
        return self._get_service_name('HBASE')

    def get_hive_srvname(self):
        return self._get_service_name('HIVE')

    def get_zookeeper_srvname(self):
        return self._get_service_name('ZOOKEEPER')

    def get_yarn_srvname(self):
        return self._get_service_name('YARN')

    def _get_service_name(self, service):
        services_cfgs = self.hg.get(self.cluster_url +'/services')
        service_name = ''
        # CDH uses different service names in multiple clusters
        if 'CDH' in self.distro:
            for item in services_cfgs['items']:
                if item['type'] == service:
                    service_name = item['name']
        elif 'HDP' in self.distro:
            for item in services_cfgs['items']:
                if item['ServiceInfo']['service_name'] == service:
                    service_name = service
        return service_name

    def _check_version(self):
        ccfg = CentralConfig()
        if 'CDH' in self.distro: version_list = ccfg.get('cdh_ver')
        if 'HDP' in self.distro: version_list = ccfg.get('hdp_ver')

        has_version = 0
        for ver in version_list:
            if ver in self.distro: has_version = 1

        if not has_version:
            err_m('Sorry, currently Trafodion doesn\'t support %s version' % self.distro)

    def get_hbase_ver(self):
        hbase_ver = cmd_output('/usr/bin/hbase version | head -n1')

        try:
            hbase_ver = re.search(r'HBase (\d\.\d\.\d-.*)', hbase_ver).groups()[0]
        except AttributeError:
            err_m('Failed to get HBase version')
        return hbase_ver

    def get_hdfs_ver(self):
        hdfs_ver = cmd_output('/usr/bin/hdfs version | head -n1')

        try:
            hdfs_ver = re.search(r'Hadoop (\d\.\d\.\d-.*)', hdfs_ver).groups()[0]
        except AttributeError:
            err_m('Failed to get Hadoop Hdfs version')
        return hdfs_ver

    def get_hadoop_users(self):
        if 'CDH' in self.distro:
            self._get_cdh_users()
        elif 'HDP' in self.distro or 'BigInsights' in self.distro:
            self._get_hdp_users()
        return self.users

    def _get_hdp_users(self):
        desired_cfg = self.hg.get('%s/?fields=Clusters/desired_configs' % (self.cluster_url))
        config_type = {'hbase-env':'hbase_user', 'hadoop-env':'hdfs_user', 'zookeeper-env':'zk_user'}
        for key, value in config_type.items():
            desired_tag = desired_cfg['Clusters']['desired_configs'][key]['tag']
            current_cfg = self.hg.get('%s/configurations?type=%s&tag=%s' % (self.cluster_url, key, desired_tag))
            self.users[value] = current_cfg['items'][0]['properties'][value]

    def _get_cdh_users(self):
        def _get_username(service_name, hadoop_type):
            cfg = self.hg.get('%s/services/%s/config' % (self.cluster_url, service_name))
            if cfg.has_key('items'):
                for item in cfg['items']:
                    if item['name'] == 'process_username':
                        return item['value']
            return hadoop_type

        hdfs_user = _get_username(self.get_hdfs_srvname(), 'hdfs')
        hbase_user = _get_username(self.get_hbase_srvname(), 'hbase')
        zookeeper_user = _get_username(self.get_zookeeper_srvname(), 'zookeeper')

        self.users = {'hbase_user':hbase_user, 'hdfs_user':hdfs_user, 'zk_user':zookeeper_user}

    def get_cluster_nodes(self):
        """ get list of nodes in cluster """
        hostids = []
        cluster_hostids = self.hg.get('%s/hosts' % self.v6_cluster_url)
        for i in cluster_hostids['items']:
            hostids.append(i['hostId'])

        for i in hostids:
            for h in self.cm['hosts']:
                if i == h['hostId']: self.cluster_nodes.append(h['hostname'])
        
        self.cluster_nodes.sort()
        return self.cluster_nodes

    def get_rsnodes(self):
        if 'CDH' in self.distro:
            self._get_rsnodes_cdh()
        elif 'HDP' in self.distro or 'BigInsights' in self.distro:
            self._get_rsnodes_hdp()

        self.rsnodes.sort()
        ## use short hostname
        #try:
        #    self.rsnodes = [re.match(r'([\w\-]+).*', node).group(1) for node in self.rsnodes]
        #except AttributeError:
        #    pass
        return self.rsnodes

    def _get_rsnodes_cdh(self):
        """ get list of HBase RegionServer nodes in CDH """
        hostids = []
        for c in self.cm['clusters']:
            if c['displayName'] == self.cluster_name:
                for s in c['services']:
                    if s['type'] == 'HBASE':
                        for r in s['roles']:
                            if r['type'] == 'REGIONSERVER': hostids.append(r['hostRef']['hostId'])
        for i in hostids:
            for h in self.cm['hosts']:
                if i == h['hostId']: self.rsnodes.append(h['hostname'])

    def _get_rsnodes_hdp(self):
        """ get list of HBase RegionServer nodes in HDP """
        hdp = self.hg.get('%s/services/HBASE/components/HBASE_REGIONSERVER' % self.cluster_url)
        self.rsnodes = [c['HostRoles']['host_name'] for c in hdp['host_components']]

    def get_hbase_lib_path(self):
        if 'CDH' in self.distro:
            for c in self.cm['clusters']:
                if c['displayName'] == self.cluster_name:
                    parcels = c['parcels']

            cdh_parcel_enabled = False
            for parcel in parcels:
                if parcel['product'] == 'CDH':
                    cdh_parcel_enabled = True
                    break

            if cdh_parcel_enabled:
                parcel_config = self.hg.get('%s/api/v6/cm/allHosts/config' % self.url)
                # custom parcel dir exists
                if parcel_config['items']:
                    for item in parcel_config['items']:
                        if item['name'] == 'parcels_directory':
                            hbase_lib_path = item['value'] + '/CDH/lib/hbase/lib'
                        else:
                            hbase_lib_path = PARCEL_HBASE_LIB
                else:
                    hbase_lib_path = PARCEL_HBASE_LIB
            else:
                hbase_lib_path = DEF_HBASE_LIB
        elif 'HDP' in self.distro:
            hbase_lib_path = HDP_HBASE_LIB

        return hbase_lib_path

    def get_parcel_dir(self):
        if 'CDH' in self.distro:
            parcel_config = self.hg.get('%s/api/v6/cm/allHosts/config' % self.url)
            # custom parcel dir exists
            if parcel_config['items'] and parcel_config['items'][0]['name'] == 'parcels_directory':
                parcel_dir = parcel_config['items'][0]['value'] + '/CDH'
            else:
                parcel_dir = PARCEL_DIR + '/CDH'
        else:
            parcel_dir = ""
        return parcel_dir

class CentralConfig(object):
    def __init__(self):
        self.cfg = ParseJson(CENTRAL_CFG_FILE).load()

    def get(self, component):
        # return empty str if component not exist
        return self.cfg[component]

class Remote(object):
    """
        copy files to/fetch files from remote host using ssh
        can also use paramiko, but it's not a build-in module
    """

    def __init__(self, host, user='', pwd='', port=''):
        self.host = host
        self.user = user
        self.rc = 0
        self.pwd = pwd
        self.port = port
        self.sshpass = self._sshpass_available()
        self._connection_test()

    @staticmethod
    def _sshpass_available():
        sshpass_available = True
        try:
            p = subprocess.Popen(['sshpass'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p.communicate()
        except OSError:
            sshpass_available = False

        return sshpass_available

    def _commands(self, method):
        cmd = []
        if self.sshpass and self.pwd: cmd = ['sshpass', '-p', self.pwd]
        cmd += [method]
        cmd += ['-oStrictHostKeyChecking=no', '-oNoHostAuthenticationForLocalhost=yes', '-oConnectTimeout=5']
        if not (self.sshpass and self.pwd): cmd += ['-oPasswordAuthentication=no']
        return cmd

    def _execute(self, cmd, verbose=False, shell=False, timeout=-1):
        try:
            logger = logging.getLogger()

            if verbose: print 'cmd:', cmd

            master, slave = pty.openpty()
            if shell:
                p = subprocess.Popen(cmd, stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
            else:
                p = subprocess.Popen(cmd, stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            start_t = time.time()
            self.stdout = self.stderr = ''
            if timeout > 0:
                while True:
                    if p.poll() is not None:
                        self.rc = p.returncode
                        self.stdout, self.stderr = p.communicate()
                        break

                    duration_t = time.time() - start_t
                    if duration_t > timeout:
                        self.rc = 128
                        p.kill()
                        self.stderr = 'Timeout (%d)s exit' % timeout
                        break
                    time.sleep(0.1)
            else:
                #self.stdout, self.stderr = p.communicate()
                #self.rc = p.returncode
                while p.poll() is None:
                    stdout = p.stdout.readline()
                    #stdout = stdout.strip()
                    if stdout.strip():
                        logger.info('Host [%s] %s' % (self.host, stdout.strip()))
                        self.stdout += stdout
                self.rc = p.returncode
                if self.rc != 0:
                    if not self.rc: self.rc = 1
                    _, stderr = p.communicate()
                    if stderr:
                        self.stderr += stderr

            logger.debug('Remote execute: Host [%s] Command [%s]' % (self.host, cmd))
        except Exception as e:
            err_m('Failed to run commands on remote host: %s' % e)

    def _connection_test(self):
        self.execute('echo -n', chkerr=False)
        if self.rc != 0:
            msg = 'Host [%s]: Failed to connect using ssh. Be sure:\n' % self.host
            msg += '1. Remote host\'s name and IP is configured correctly in /etc/hosts.\n'
            msg += '2. Remote host\'s sshd service is running.\n'
            msg += '3. Passwordless SSH is set if not using \'enable-pwd\' option.\n'
            msg += '4. \'sshpass\' tool is installed and ssh password is correct if using \'enable-pwd\' option.\n'
            err_m(msg)

    def execute(self, user_cmd, timeout=-1, verbose=False, shell=False, executable='/bin/bash', chkerr=True):
        """ @params: user_cmd should be a string """
        cmd = self._commands('ssh')
        if self.port: cmd += ['-p', self.port]
        cmd += ['-tt'] # force tty allocation
        if self.user:
            cmd += ['%s@%s' % (self.user, self.host)]
        else:
            cmd += [self.host]

        # if shell=True, cmd should be a string not list
        if shell:
            cmd = ' '.join(cmd) + ' '
            cmd += user_cmd
        else:
            cmd += user_cmd.split()

        self._execute(cmd, verbose=verbose, shell=shell, timeout=timeout)

        if chkerr and self.rc != 0:
            err_m('Failed to execute command on remote host [%s]: "%s", reason: "%s:%s"' % (self.host, user_cmd, self.stdout, self.stderr))
        elif self.rc != 0 and 'Connection to ' not in self.stderr: # not show 'Connection to nodexxx closed' error
            sys.stderr.write('\n[ERROR]: %s' % self.stderr)
            logger = logging.getLogger()
            logger.error('host [%s]: %s' % (self.host, self.stderr))

    def copy(self, files, remote_folder='.', verbose=False):
        """ copy file to user's home folder """
        for f in files:
            if not os.path.exists(f):
                self.rc = 1
                err_m('[%s] Copy file error: %s doesn\'t exist' % (self.host, f))

        cmd = self._commands('scp')
        if self.port: cmd += ['-P', self.port]
        cmd += ['-r']
        cmd += files # files should be full path
        if self.user:
            cmd += ['%s@%s:%s/' % (self.user, self.host, remote_folder)]
        else:
            cmd += ['%s:%s/' % (self.host, remote_folder)]

        self._execute(cmd, verbose=verbose)
        if self.rc != 0: err_m('Failed to copy files to remote node [%s]: %s' % (self.host, self.stderr))

    def fetch(self, files, local_folder='.', verbose=False):
        """ fetch file from user's home folder """
        cmd = self._commands('scp')
        cmd += ['-r']
        if self.user:
            cmd += ['%s@%s:~/{%s}' % (self.user, self.host, ','.join(files))]
        else:
            cmd += ['%s:~/{%s}' % (self.host, ','.join(files))]
        cmd += [local_folder]

        self._execute(cmd, verbose=verbose)
        if self.rc != 0: err('Failed to fetch files from remote nodes')


class ParseHttp(object):
    def __init__(self, user, passwd, json_type=True):
        # httplib2 is not installed by default
        try:
            import httplib2
        except ImportError:
            err_m('Python module httplib2 is not found. Install python-httplib2 first.')

        self.user = user
        self.passwd = passwd
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.h.add_credentials(self.user, self.passwd)
        self.headers = {}
        self.headers['X-Requested-By'] = 'trafodion'
        if json_type:
            self.headers['Content-Type'] = 'application/json'
        self.headers['Authorization'] = 'Basic %s' % (base64.b64encode('%s:%s' % (self.user, self.passwd)))

    def _request(self, url, method, body=None):
        try:
            resp, content = self.h.request(url, method, headers=self.headers, body=body)
            # return code is not 2xx and 409(for ambari blueprint)
            if not (resp.status == 409 or 200 <= resp.status < 300):
                err_m('Error return code {0} when {1}ting configs: {2}'.format(resp.status, method.lower(), content))
            return content
        except Exception as exc:
            err_m('Error with {0}ting configs using URL {1}. Reason: {2}'.format(method.lower(), url, exc))

    def get(self, url, content_type='json'):
        try:
            if content_type == 'xml':
                result_str = self._request(url, 'GET')
                return ET.ElementTree(ET.fromstring(result_str))

            return defaultdict(str, json.loads(self._request(url, 'GET')))
        except ValueError as e:
            err_m(str(e))
            err_m('Failed to get data from URL, check password if URL requires authentication')

    def put(self, url, config):
        if not isinstance(config, dict): err_m('Wrong HTTP PUT parameter, should be a dict')
        result = self._request(url, 'PUT', body=json.dumps(config))
        if result: return defaultdict(str, json.loads(result))

    def post(self, url, config=None):
        try:
            if config:
                if not isinstance(config, dict): err_m('Wrong HTTP POST parameter, should be a dict')
                body = json.dumps(config)
            else:
                body = None

            result = self._request(url, 'POST', body=body)
            if result: return defaultdict(str, json.loads(result))

        except ValueError as ve:
            err_m('Failed to send command to URL: %s' % ve)

    def delete(self, url):
        result = self._request(url, 'DELETE')
        if result: return defaultdict(str, json.loads(result))


class ParseXML(object):
    """ handle *-site.xml with format
        <property><name></name><value></value></proerty>
    """
    def __init__(self, xml_src, src_type='file'):
        if src_type == 'file':
            self.__xml_file = xml_src
            if not os.path.exists(self.__xml_file): err_m('Cannot find xml file %s' % self.__xml_file)
            try:
                self._tree = ET.parse(self.__xml_file)
            except Exception as e:
                err_m('failed to parse xml: %s' % e)

        if src_type == 'string':
            self.__xml_string = xml_src
            try:
                self._tree = ET.ElementTree(ET.fromstring(self.__xml_string))
            except Exception as e:
                err_m('failed to parse xml: %s' % e)

        if src_type == 'tree':
            self._tree = xml_src

        self._root = self._tree.getroot()
        self._properties = self._root.findall('property')
        # name, value list
        self._nvlist = [[elem.text for elem in p] for p in self._properties]

    def __indent(self, elem):
        """Return a pretty-printed XML string for the Element."""
        if len(elem):
            if not elem.text: elem.text = '\n' + '  '
            if not elem.tail: elem.tail = '\n'
            for subelem in elem:
                self.__indent(subelem)
        else:
            if not elem.tail: elem.tail = '\n' + '  '

    def get_property(self, name):
        try:
            return [x[1] for x in self._nvlist if x[0] == name][0]
        except:
            return ''

    def rm_property(self, name):
        for p in self._properties:
            if p[0].text == name:
                self._root.remove(p)

    def _update_property(self, name, value):
        for elem_p in self._root.getchildren():
            elem_n = elem_p.getchildren()[0]
            elem_v = elem_p.getchildren()[1]
            if elem_n.text == name: elem_v.text = value

    def add_property(self, name, value):
        if self.get_property(name):
            self._update_property(name, value)
        else:
            elem_p = ET.Element('property')
            elem_name = ET.Element('name')
            elem_value = ET.Element('value')

            elem_name.text = name
            elem_value.text = value
            elem_p.append(elem_name)
            elem_p.append(elem_value)

            self._nvlist.append([name, value])
            self._root.append(elem_p)

    def write_xml_to_string(self):
        return ET.tostring(self._root)

    def write_xml(self):
        self.__indent(self._root)
        self._tree.write(self.__xml_file)

    def output_xml(self):
        return self._nvlist

class ParseJson(object):
    def __init__(self, js_file):
        self.__js_file = js_file

    def load(self):
        """ load json file to a dict """
        if not os.path.exists(self.__js_file): err_m('Cannot find json file %s' % self.__js_file)
        with open(self.__js_file, 'r') as f:
            tmparray = f.readlines()
        content = ''
        for t in tmparray:
            content += t

        try:
            return defaultdict(str, json.loads(content))
        except ValueError:
            err_m('No json format found in config file %s' % self.__js_file)

    def save(self, dic):
        """ save dict to json file with pretty format """
        with open(self.__js_file, 'w') as f:
            f.write(json.dumps(dic, indent=4))
        return 0


class MyConfigParser(ConfigParser):
    """
    set ConfigParser options for case sensitive.
    """

    def __init__(self, defaults=None):
        ConfigParser.__init__(self, defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


class ParseInI(object):
    def __init__(self, ini_file, section):
        self.__ini_file = ini_file
        self.section = section

    def load(self):
        """ load content from ini file and return a dict """
        if not os.path.exists(self.__ini_file):
            err_m('Cannot find ini file %s' % self.__ini_file)

        cfgs = {}
        cf = MyConfigParser()
        cf.read(self.__ini_file)

        if not cf.has_section(self.section):
            return {}

        for cfg in cf.items(self.section):
            cfgs[cfg[0]] = cfg[1]

        return defaultdict(str, cfgs)

    def save(self, dic):
        """ save a dict as an ini file """
        cf = ConfigParser()
        cf.add_section(self.section)
        for key, value in dic.iteritems():
            cf.set(self.section, key, value)

        with open(self.__ini_file, 'w') as f:
            cf.write(f)

    def rewrite(self, dic):
        """  """
        cf = ConfigParser()
        cf.read(self.__ini_file)
        cf.remove_section(self.section)
        cf.add_section(self.section)
        for key, value in dic.iteritems():
            cf.set(self.section, key, value)

        with open(self.__ini_file, 'w') as f:
            cf.write(f)

def http_start(repo_dir, repo_port):
    info('Starting temporary python http server')
    os.system("cd %s; python -m SimpleHTTPServer %s > /dev/null 2>&1 &" % (repo_dir, repo_port))

def http_stop():
    #info('Stopping temporary python http server')
    os.system("ps -ef|grep SimpleHTTPServer |grep -v grep | awk '{print $2}' |xargs kill -9 >/dev/null 2>&1")

def format_output(text):
    num = len(text) + 4
    print '*' * num
    print '  ' + text
    print '*' * num

def expNumRe(text):
    """
    expand numeric regular expression to list
    e.g. 'n[01-03],n1[0-1]': ['n01','n02','n03','n10','n11']
    e.g. 'n[09-11].com': ['n09.com','n10.com','n11.com']
    """
    explist = []
    for regex in text.split(','):
        regex = regex.strip()
        r = re.match(r'(.*)\[(\d+)-(\d+)\](.*)', regex)
        if r:
            h = r.group(1)
            d1 = r.group(2)
            d2 = r.group(3)
            t = r.group(4)

            convert = lambda d: str(('%0' + str(min(len(d1), len(d2))) + 'd') % d)
            if int(d1) > int(d2): d1, d2 = d2, d1
            explist.extend([h + convert(c) + t for c in range(int(d1), int(d2)+1)])

        else:
            # keep original value if not matched
            explist.append(regex)

    return explist

def time_elapse(func):
    """ time elapse decorator """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        output = func(*args, **kwargs)
        end_time = time.time()
        seconds = end_time - start_time
        hours = seconds / 3600
        seconds = seconds % 3600
        minutes = seconds / 60
        seconds = seconds % 60
        print '\nTime Cost: %d hour(s) %d minute(s) %d second(s)' % (hours, minutes, seconds)
        return output
    return wrapper

def retry(func, maxcnt, interval, msg):
    """ retry timeout function """
    retry_cnt = 0
    rc = False
    try:
        while not rc:
            retry_cnt += 1
            rc = func()
            flush_str = '.' * retry_cnt
            print '\rCheck %s status (timeout: %d secs) %s' % (msg, maxcnt * interval, flush_str),
            sys.stdout.flush()
            time.sleep(interval)
            if retry_cnt == maxcnt:
                err_m('Timeout exit')
    except KeyboardInterrupt:
        err_m('user quit')
    ok('%s successfully!' % msg)

def encode_pwd(s):
    def _to36(value):
        if value == 0:
            return '0'
        if value < 0:
            sign = '-'
            value = -value
        else:
            sign = ''
        result = []
        while value:
            value, mod = divmod(value, 36)
            result.append('0123456789abcdefghijklmnopqrstuvwxyz'[mod])
        return sign + ''.join(reversed(result))

    OBF_PREFIX = 'OBF:'
    o = OBF_PREFIX
    if isinstance(s, bytes):
        s = s.decode('utf-8')
    b = bytearray(s, 'utf-8')
    l = len(b)
    for i in range(0, l):
        b1, b2 = b[i], b[l - (i + 1)]
        if b1 < 0 or b2 < 0:
            i0 = (0xff & b1) * 256 + (0xff & b2)
            o += 'U0000'[0:5 - len(x)] + x
        else:
            i1, i2 = 127 + b1 + b2, 127 + b1 - b2
            i0 = i1 * 256 + i2
            x = _to36(i0)
            j0 = int(x, 36)
            j1, j2 = i0 / 256, i0 % 256
            o += '000'[0:4 - len(x)] + x
    return o

def get_host_ip(socket):
    """return: ip"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def decode_password_policy(Password_Policy_String):
    def parse_config(str):
        '''
        00    - must use,but no care about numbers of this char
        ee    - must not use
        ff    - no check
        '''
        num = int(str, 16)
        if num == 0xff:
            num = -2
        elif num == 0xee:
            num = -1
        return num

    dict = {}
    dict["pwd_min_len"] = parse_config(Password_Policy_String[0:2])
    dict["pwd_max_len"] = parse_config(Password_Policy_String[2:4])
    dict["upper_char_num"] = parse_config(Password_Policy_String[4:6])
    dict["lower_char_num"] = parse_config(Password_Policy_String[6:8])
    dict["num_char_num"] = parse_config(Password_Policy_String[8:10])
    dict["sep_char_num"] = parse_config(Password_Policy_String[10:])
    return dict

def show_password_policy(Password_Policy_String):
    dict = decode_password_policy(Password_Policy_String)
    pwd_min_len = dict["pwd_min_len"]
    pwd_max_len = dict["pwd_max_len"]
    upper_char_num = dict["upper_char_num"]
    lower_char_num = dict["lower_char_num"]
    num_char_num = dict["num_char_num"]
    sep_char_num = dict["sep_char_num"]
    #print information to user
    print("Password check policy for User")
    print("==============================")
    print("\nVaild characters in password:")
    print("* Digit chars    [0-9]")
    print("* English chars  [a-z] or [A-Z]")
    print("* Symbol chars   [-_@./]")
    print("Length for password: %d - %d chars" % (pwd_min_len, pwd_max_len))
    if upper_char_num >= 0 or lower_char_num >= 0 or num_char_num >= 0 or sep_char_num >= 0:
        print ("Password must contents character:")
        if upper_char_num >= 0:
            print ("* Upper chars")
        if lower_char_num >= 0:
            print ("* Lower chars")
        if num_char_num >= 0:
            print ("* Digital")
        if sep_char_num >= 0:
            print ("* Symbol chars")
    if upper_char_num == -1 or lower_char_num == -1 or num_char_num == -1 or sep_char_num == -1:
        print ("Password must not contents character:")
        if upper_char_num == -1:
            print ("* Upper chars")
        if lower_char_num == -1:
            print ("* Lower chars")
        if num_char_num == -1:
            print ("* Digital")
        if sep_char_num == -1:
            print ("* Symbol chars")
    if upper_char_num >= 0 or lower_char_num >= 0 or num_char_num >= 0 or sep_char_num >= 0:
        print ("Password complexity rules:")
        if upper_char_num >= 0:
            str1 = ""
            if upper_char_num == 0:
                str1 = "ANY"
            else:
                str1 = str(upper_char_num)
            print ("* Contains %s at least upper chars" % str1)
        if lower_char_num >= 0:
            str1 = ""
            if lower_char_num == 0:
                str1 = "ANY"
            else:
                str1 = str(lower_char_num) + " at least"
            print ("* Contains %s at least lower chars" % str1)
        if num_char_num >= 0:
            str1 = ""
            if num_char_num == 0:
                str1 = "ANY"
            else:
                str1 = str(num_char_num) + " at least"
            print ("* Contains %s at least digitals" % str1)
        if sep_char_num >= 0:
            str1 = ""
            if sep_char_num == 0:
                str1 = "ANY"
            else:
                str1 = str(sep_char_num) + " at least"
            print ("* Contains %s at least symbol chars" % str1)
    if upper_char_num == -2 and lower_char_num == -2 and num_char_num == -2 and sep_char_num == -2:
        print ("Password complexity rules:")
        print ("* Contains at least three or more of them: upper character, lower character, digit and special symbols.")
    print("\n==============================")
    print("\nTRAFODION_PASSWORD_CHECK_SYNTAX=%s" % Password_Policy_String)
    print("\n==============================")

def Generate_Password_Policy(use_default_password_policy):

    isOk = False
    pwd_max_len = 128
    pwd_min_len = 6
    upper_char_num = 0
    lower_char_num = 0
    num_char_num = 0
    sep_char_num = 0
    Password_Policy_String = ""

    def GenItemValueStr(item):
        itemStr = ""
        if item == -1:
            itemStr = "EE"
        elif item == -2:
            itemStr = "FF"
        else:
            itemStr = "{:02X}".format(item)
        return itemStr

    def CountItemValue(item):
        item_num = 0
        #EE
        if item == -1:
            item_num = 0
        #FF
        elif item == -2:
            item_num = 0
        #00
        elif item == 0:
            item_num = 1
        else:
            item_num = item
        return item_num

    def CheckItemValue(item):
        if item < 0 or item > pwd_max_len:
            print("warn: The number of characters should be in 1 ~ %d" % pwd_max_len)
            return False
        return True

    def GetUserInput(item_name):
        item_val = 0
        isOk = False
        while isOk == False:
            print("Want to use the %s characters in password? or don't care about this characters(Y/N/I)" % item_name)
            use_char_flag = sys.stdin.readline().strip()
            if use_char_flag.upper() == 'N':
                #EE
                item_val = -1
                isOk = True
            elif use_char_flag.upper() == 'Y':
                item_val = int(input(
                    "Enter the minimum number of the %s characters in password. If you don't care about numbers, enter 0\n" % item_name))
                if CheckItemValue(item_val) == False:
                    continue
                isOk = True
            elif use_char_flag.upper() == 'I':
                #FF
                item_val = -2
                isOk = True
            else:
                print ("warn: Please type Y or N or I")
                continue
        return item_val

    while isOk == False:
        try:
            if use_default_password_policy.upper() != 'Y':
                pwd_max_len = int(input("Enter the maximum length for password\n"))
                if pwd_max_len <= 0 or pwd_max_len > 128:
                    print("warn: The length for password should be in 1 ~ 128\n")
                    continue
                pwd_min_len = int(input("Enter the minimum length for password\n"))
                if pwd_min_len <= 0 or pwd_min_len > pwd_max_len:
                    print("warn: The length for password should be in 1 ~ %d\n" % pwd_max_len)
                    continue
                upper_char_num = GetUserInput("uppercase english")
                lower_char_num = GetUserInput("lowercase english")
                num_char_num = GetUserInput("number")
                sep_char_num = GetUserInput("special")
                sum_char_num = 0
                sum_char_num += CountItemValue(sep_char_num)
                sum_char_num += CountItemValue(num_char_num)
                sum_char_num += CountItemValue(lower_char_num)
                sum_char_num += CountItemValue(upper_char_num)
                if sep_char_num == -1 and num_char_num == -1 and lower_char_num == -1 and upper_char_num == -1 :
                    #no char can input
                    print ("warn: No type of chars can use\n")
                    continue
                elif sep_char_num == -2 and num_char_num == -2 and lower_char_num == -2 and upper_char_num == -2 :
                    #ok!
                    info ("Setup password check policy with 'Need to contains 3 different type of character at last'.\n")
                    #reserves 3 chars
                    sum_char_num = 3
                if sum_char_num > pwd_max_len:
                    print ("warn: The characters that make up for password should be in %d ~ %d\n" % (pwd_min_len, pwd_max_len))
                    continue
                if sum_char_num > pwd_min_len:
                    pwd_min_len = sum_char_num
                Password_Policy_String = ""
                #min len
                Password_Policy_String += GenItemValueStr(pwd_min_len)
                #max len
                Password_Policy_String += GenItemValueStr(pwd_max_len)
                #upper
                Password_Policy_String += GenItemValueStr(upper_char_num)
                #lower
                Password_Policy_String += GenItemValueStr(lower_char_num)
                #lower
                Password_Policy_String += GenItemValueStr(num_char_num)
                #special characters
                Password_Policy_String += GenItemValueStr(sep_char_num)
            else:
                Password_Policy_String = "0680FFFFFFFF"
            show_password_policy(Password_Policy_String)
            print("\nReady to use this password policy? (Y/N)")
            ready = sys.stdin.readline().strip()
            if ready.upper() == 'Y':
                isOk = True
            else:
                use_default_password_policy = 'N'
        except StandardError:
            print("warn: Input error")
    return Password_Policy_String

def password_check(password, Password_Policy_String):

    def is_number(s):
        try:
            int(s)
            return True
        except ValueError:
            pass

    def check_for_item(count,check_num):
        is_ok = True
        if check_num != 0xff:
            if check_num == 0xee and count != 0:
                is_ok = False
            elif check_num != 0xee:
                if check_num == 0 and count == 0:
                    is_ok = False
                elif check_num != 0 and count < check_num:
                    is_ok = False
        return is_ok

    def has_this_char_type(count, flag):
        if count > 0:
            return flag + 1
        return flag

    #decode password policy
    '''
    [0-1] minimum password length
    [2-3] maximum password length
    [4-5] number of uppercase alphabet
    [6-7] number of lowercase alphabet
    [8-9] number of digital alphabet
    [10-11] number of symbol alphabet
    number of chars
    00    - must use,but no care about numbers of this char
    ee    - must not use
    ff    - no check
    other - numbers of char
    number of password length
    max len - 0xff skip check
    max len - 0    not set
    '''
    is_default_policy = False
    if Password_Policy_String == "0680FFFFFFFF":
        is_default_policy = True
        pwd_min_len = 6
        pwd_max_len = 128
        upper_char_num = -2
        lower_char_num = -2
        num_char_num = -2
        sep_char_num = -2
    else:
        dict = decode_password_policy(Password_Policy_String)
        pwd_min_len = dict["pwd_min_len"]
        pwd_max_len = dict["pwd_max_len"]
        upper_char_num = dict["upper_char_num"]
        lower_char_num = dict["lower_char_num"]
        num_char_num = dict["num_char_num"]
        sep_char_num = dict["sep_char_num"]

    pwd_len = len(password)
    valid_chars = "-_@./"

    if pwd_len < pwd_min_len or pwd_len > pwd_max_len:
        return False
    upper_counter = 0
    lower_counter = 0
    digit_counter = 0
    symbol_counter = 0
    for i in range(pwd_len):
        #number or alphabet
        if password[i].isalnum():
            if is_number(password[i]):
                digit_counter += 1
            elif password[i].isupper():
                upper_counter += 1
            else:
                lower_counter += 1
        elif valid_chars.find(password[i]) != -1:
            symbol_counter += 1
        else:
            return False

    #the minimum password quality check
    if is_default_policy:
        pwdCharType = 0
        pwdCharType = has_this_char_type(upper_counter,pwdCharType)
        pwdCharType = has_this_char_type(lower_counter,pwdCharType)
        pwdCharType = has_this_char_type(digit_counter,pwdCharType)
        pwdCharType = has_this_char_type(symbol_counter,pwdCharType)
        if pwdCharType >= 3:
            return True
        else:
            return False
    #the complex password quality check
    else:
        return check_for_item(upper_counter, upper_char_num) and check_for_item(lower_counter, lower_char_num) and check_for_item(digit_counter, num_char_num) and check_for_item(symbol_counter, sep_char_num)

if __name__ == '__main__':
    exit(0)