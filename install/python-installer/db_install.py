#!/usr/bin/env python
# -*- coding: utf8 -*-

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

import os
import re
import gc
import socket
import json
import getpass
import time
import sys
import readline
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from glob import glob
from collections import defaultdict
try:
    from prettytable import PrettyTable
except ImportError:
    print 'Python module prettytable is not found. Install python-prettytable first.'
    exit(1)
from scripts import wrapper
from scripts.constants import *
from scripts.constants_release import *
from scripts.common import get_sudo_prefix, HadoopDiscover, Remote, License, ParseHttp, ParseInI, ParseJson, run_cmd, info, \
                           warn, http_start, http_stop, format_output, err_m, expNumRe, cmd_output, Generate_Password_Policy, \
                           show_password_policy, password_check
from scripts.hadoop_mods import CDHMod, HDPMod

os.environ['LANG'] = 'en_US.UTF8'

# init global cfgs for user input
cfgs = defaultdict(str)

class UserInput(object):
    def __init__(self, options, pwd, dbcfgs):
        self.in_data = ParseJson(USER_PROMPT_FILE).load()
        self.pwd = pwd
        self.cfgs = dbcfgs

    def _basic_check(self, name, answer, optional):
        isYN = self.in_data[name].has_key('isYN')
        isdigit = self.in_data[name].has_key('isdigit')
        isexist = self.in_data[name].has_key('isexist')
        isfile = self.in_data[name].has_key('isfile')
        isremote_exist = self.in_data[name].has_key('isremote_exist')
        isIP = self.in_data[name].has_key('isIP')
        isuser = self.in_data[name].has_key('isuser')

        # check answer value basicly
        if answer:
            if isYN:
                answer = answer.upper()
                if answer != 'Y' and answer != 'N':
                    log_err('Invalid parameter for %s, should be \'Y|y|N|n\'' % name)
            elif isdigit:
                if not answer.isdigit():
                    log_err('Invalid parameter for %s, should be a number' % name)
            elif isexist:
                if not os.path.exists(answer):
                    log_err('%s path \'%s\' doesn\'t exist' % (name, answer))
            elif isfile:
                if not os.path.isfile(answer):
                    log_err('%s file \'%s\' doesn\'t exist' % (name, answer))
            elif isremote_exist:
                hosts = self.cfgs['node_list'].split(',')
                check_remote_exists(hosts, self.pwd, answer)
            elif isIP:
                try:
                    socket.inet_pton(socket.AF_INET, answer)
                except:
                    log_err('Invalid IP address \'%s\'' % answer)
            elif isuser:
                if re.match(r'\w+', answer).group() != answer:
                    log_err('Invalid user name \'%s\'' % answer)

        elif not optional:
            log_err('Empty value for \'%s\'' % name)

    def _handle_prompt(self, name, user_defined, optional):
        prompt = self.in_data[name]['prompt']
        default = user_defined

        if (not default) and self.in_data[name].has_key('default'):
            default = self.in_data[name]['default']

        ispasswd = self.in_data[name].has_key('ispasswd')
        isYN = self.in_data[name].has_key('isYN')

        # no default value for password
        if ispasswd: default = ''

        if isYN:
            prompt += ' (Y/N) '

        if optional:
            prompt += ' (optional)'

        if default:
            prompt += ' [' + default + ']: '
        else:
            prompt += ': '

        # no default value for password
        if ispasswd:
            orig = getpass.getpass(prompt)
            confirm = getpass.getpass('Confirm ' + prompt)
            if orig == confirm:
                answer = confirm
            else:
                log_err('Password mismatch')
        else:
            try:
                answer = raw_input(prompt)
            except UnicodeEncodeError:
                log_err('Character Encode error, check user input')
            if not answer and default: answer = default

        return answer.strip()

    def get_input(self, name, user_defined='', prompt_mode=True, optional=False):
        if self.in_data.has_key(name):
            if prompt_mode:
                # save configs to global dict
                self.cfgs[name] = self._handle_prompt(name, user_defined, optional)

            # check basic values from global configs
            self._basic_check(name, self.cfgs[name], optional)
            return self.cfgs[name]
        else:
            # should not go to here, just in case
            log_err('Invalid prompt')

    def notify_user(self, conf_file, status_file):
        """ show the final configs to user """
        format_output('Final Configs')
        title = ['config type', 'value']
        pt = PrettyTable(title)
        for item in title:
            pt.align[item] = 'l'

        for key, value in sorted(self.cfgs.items()):
            # only notify user input value
            if self.in_data.has_key(key) and value:
                if self.in_data[key].has_key('ispasswd'): continue
                if key in ['confirm_result', 'cdh_nodes', 'host_ip']: continue
                pt.add_row([key, value])
        print pt
        confirm = self.get_input('confirm_result')
        if confirm.upper() != 'Y':
            if os.path.exists(conf_file):
                os.remove(conf_file)
            run_cmd('rm -rf %s/*.status' % INSTALLER_LOC)
            log_err('User quit')

        if os.path.exists(status_file):
            confirm = self.get_input('confirm_status')
            if confirm.upper() != 'Y':
                run_cmd('rm -rf %s' % status_file)

def log_err(errtext):
    # save tmp config files
    tp = ParseInI(DBCFG_TMP_FILE, 'dbconfigs')
    tp.save(cfgs)
    err_m(errtext)

def check_remote_exists(hosts, pwd, path, check=True):
    remotes = [Remote(host, pwd=pwd) for host in hosts]

    nodes = ''
    for remote in remotes:
        # check if directory exists on remote host
        remote.execute(get_sudo_prefix() + ' ls %s 2>&1 >/dev/null' % path, chkerr=False)
        if remote.rc != 0:
            nodes += ' ' + remote.host
    if nodes and check:
        log_err('Path \'%s\' doesn\'t exist on node(s) \'%s\'' % (path, nodes))
    return nodes # directory not exist

def user_input(options, prompt_mode=True, pwd=''):
    """ get user's input and check input value """
    global cfgs, pyinstaller_type, pyinstaller_major

    apache = True if hasattr(options, 'apache') and options.apache else False
    offline = True if hasattr(options, 'offline') and options.offline else False
    silent = True if hasattr(options, 'silent') and options.silent else False
    build = True if hasattr(options, 'build') and options.build else False
    if hasattr(options, 'customized'):
        if options.customized is None:
            customized = "CUSTOM_NO"
        elif options.customized.upper() == "AP" :
            customized = "CUSTOM_AP"
        else:
            customized = "CUSTOM_TP"
    else:
        customized = "CUSTOM_NO"

    enhance_ha = False if hasattr(options, 'not_enhance_ha') and options.not_enhance_ha else True
    ssh_port = options.port if hasattr(options, 'port') and options.port else ''

    # load from temp config file if in prompt mode
    if os.path.exists(DBCFG_TMP_FILE) and prompt_mode == True:
        tp = ParseInI(DBCFG_TMP_FILE, 'dbconfigs')
        cfgs = tp.load()
        if not cfgs:
            # set cfgs to defaultdict again
            cfgs = defaultdict(str)

    u = UserInput(options, pwd, cfgs)
    g = lambda n: u.get_input(n, cfgs[n], prompt_mode=prompt_mode)
    o = lambda n: u.get_input(n, cfgs[n], prompt_mode=prompt_mode, optional=True)

    # list1 ['node1.local1', 'node2.local2', 'node3.local3']
    # list2 ['node1', 'node3']
    # return: ['node1.local1', 'node3.local3']
    verify_nodes = lambda list1, list2: [n1 for n1 in list1 for n2 in list2 if n2 in n1]

    ### begin user input ###
    if apache:
        g('rsnodes')
        rsnodes = expNumRe(cfgs['rsnodes'])

        # check if node list is expanded successfully
        if len([1 for node in rsnodes if '[' in node]):
            log_err('Failed to expand node list, please check your input.')

        cfgs['rsnodes'] = ','.join(rsnodes) # convert to back to comma separated list
        g('use_rs_node')
        if cfgs['use_rs_node'].upper() == 'Y':
            cfgs['node_list'] = ','.join(rsnodes) # convert to back to comma separated list
        else:
            g('node_list')
            node_lists = expNumRe(cfgs['node_list'])

            node_lists = verify_nodes(rsnodes, node_lists)
            if not node_lists:
                log_err('Incorrect node list, should be part of RegionServer nodes')
            cfgs['node_list'] = ','.join(node_lists) # convert to back to comma separated list

        # a switch to determine if using existing Hadoop or install a new Hadoop env
        if cfgs['hadoop_exists'].upper() == 'N':
            cfgs['hadoop_home'] = DEF_INSTALL_HADOOP_HOME
            cfgs['hbase_home'] = DEF_INSTALL_HBASE_HOME
            cfgs['hive_home'] = DEF_INSTALL_HIVE_HOME
            cfgs['hdfs_user'] = TRAF_USER
            cfgs['hbase_user'] = TRAF_USER
            cfgs['hive_user'] = TRAF_USER
            g('hadoop_package')
            g('hbase_package')
            g('hive_package')
        else:
            g('hadoop_home')
            g('hbase_home')
            g('hive_home')
            g('hdfs_user')
            g('hbase_user')
        cfgs['distro'] = 'APACHE'
        cfgs['hbase_lib_path'] = cfgs['hbase_home'] + '/lib'
    else:
        g('mgr_url')
        if not ('http:' in cfgs['mgr_url'] or 'https:' in cfgs['mgr_url']):
            cfgs['mgr_url'] = 'http://' + cfgs['mgr_url']

        # set cloudera default port 7180 if not provided by user
        if not re.search(r':\d+', cfgs['mgr_url']):
            cfgs['mgr_url'] += ':7180'

        g('mgr_user')
        g('mgr_pwd')

        cfgs['enhance_ha'] = 'Y' if enhance_ha else 'N'
        hadoop_discover = HadoopDiscover(cfgs['mgr_user'], cfgs['mgr_pwd'], cfgs['mgr_url'])
        cluster_names = hadoop_discover.get_cdh_cluster_names()
        if len(cluster_names) > 1: # only CDH support multiple clusters
            for index, name in enumerate(cluster_names):
                print str(index + 1) + '. ' + name
            g('cluster_no')
            c_index = int(cfgs['cluster_no']) - 1
            if c_index < 0 or c_index >= len(cluster_names):
                log_err('Incorrect number')
            cluster_name = cluster_names[int(c_index)]
            hadoop_discover = HadoopDiscover(cfgs['mgr_user'], cfgs['mgr_pwd'], cfgs['mgr_url'], cluster_name)
            cfgs['cluster_name'] = cluster_name
        else:
            cfgs['cluster_name'] = hadoop_discover.get_def_cluster_name()

        rsnodes = [rsnode.lower() for rsnode in hadoop_discover.get_rsnodes()]
        if cfgs['enhance_ha'] == 'Y':
            cdhnodes = [cdhnode.lower() for cdhnode in hadoop_discover.get_cluster_nodes()]
            cfgs['cdhnodes'] = ','.join(cdhnodes)
        hadoop_users = hadoop_discover.get_hadoop_users()

        cfgs['rsnodes'] = ','.join(rsnodes)
        print '** HBase RegionServer node list: [%s]' % cfgs['rsnodes']
        g('use_rs_node')
        if cfgs['use_rs_node'].upper() == 'Y':
            cfgs['node_list'] = cfgs['rsnodes']
        else:
            g('node_list')
            node_lists = expNumRe(cfgs['node_list'].lower())

            node_lists = verify_nodes(rsnodes, node_lists)
            if not node_lists:
                log_err('Incorrect node list, should be part of RegionServer nodes')
            cfgs['node_list'] = ','.join(node_lists)
        cfgs['exporter_list'] = cfgs['node_list']

        cfgs['distro'] = hadoop_discover.distro
        cfgs['hive_authorization'] = hadoop_discover.get_hive_authorization()
        cfgs['hbase_lib_path'] = hadoop_discover.get_hbase_lib_path()

        cfgs['hdfs_user'] = hadoop_users['hdfs_user']
        cfgs['hbase_user'] = hadoop_users['hbase_user']
        cfgs['zookeeper_user'] = hadoop_users['zk_user']
        cfgs['hive_principal'] = hadoop_discover.get_hive_principal()
        cfgs['hiveserver2_url'] = hadoop_discover.get_hiveserver2_url()
        cfgs['parcel_path'] = hadoop_discover.get_parcel_dir()
        if cfgs['enhance_ha'] == 'Y':
            cfgs['hdfs_version'] = hadoop_discover.get_hdfs_ver()
            cfgs['hbase_version'] = hadoop_discover.get_hbase_ver()
 
    ### set Cluster ID
    g('traf_cluster_id')

    ### set DCS master node
    if cfgs['use_rs_node'].upper() == 'Y':
        print '** Available list of DcsMaster nodes : [%s]' % cfgs['rsnodes']
    else:
        print '** Available list of DcsMaster nodes : [%s]' % cfgs['node_list']
    g('dcs_master_nodes')
    cfgs['dcs_master_nodes'] = cfgs['dcs_master_nodes'].lower()
    # check dcs master nodes should exist in node list
    if sorted(list(set((cfgs['dcs_master_nodes'].strip() + ',' + cfgs['node_list']).split(',')))) != sorted(cfgs['node_list'].split(',')):
        log_err('Invalid DCS master nodes, please pick up nodes from DCS master node list')

    # set some system default configs
    cfgs['config_created_date'] = time.strftime('%Y/%m/%d %H:%M %Z')

    if not cfgs['traf_user']:
        cfgs['traf_user'] = TRAF_USER

    if apache:
        cfgs['hbase_xml_file'] = cfgs['hbase_home'] + '/conf/hbase-site.xml'
        cfgs['hdfs_xml_file'] = cfgs['hadoop_home'] + '/etc/hadoop/hdfs-site.xml'
        cfgs['core_site_xml_file'] = cfgs['hadoop_home'] + '/etc/hadoop/core-site.xml'
    else:
        cfgs['hbase_xml_file'] = DEF_HBASE_SITE_XML

    ##### Discover Start #####
    ### discover system settings, return a dict
    cfgs['ssh_port'] = ssh_port
    system_discover = wrapper.run(cfgs, options, mode='discover', param='install', pwd=pwd)

    # check discover results, return error if fails on any single node
    node_count = len(cfgs['node_list'].split(','))
    need_license = need_java_home = has_home_dir = 0
    kernel_vers = []
    java_homes = []
    mount_disks_all = []
    for result in system_discover:
        os_major = re.search(r'\D+(\d+)', result['linux_distro']['value']).group(1)
        java_home = result['default_java']['value']
        java_homes.append(java_home)
        if len(list(set(java_homes))) > 1 or java_home == 'N/A':
            need_java_home += 1
        if result['ssh_pam']['status'] == ERR:
            log_err('SSH PAM should be enabled in %s' % SSH_CFG_FILE)
        if result['linux_distro']['status'] == ERR:
            log_err('Unsupported Linux version')
        if OS_SUSE in result['linux_distro']['value'].upper():
            if not silent:
                confirm = g('confirm_suse')
                if confirm.upper() != 'Y':
                    log_err('User quit')
        if pyinstaller_major != os_major:
            log_err('Version of PyInstaller package is [%s%s], cannot match the server package version' \
                     % (pyinstaller_type, pyinstaller_major))

        if result['firewall_status']['status'] == WARN:
            warn('Firewall is running, please make sure the ports used by Trafodion are open')
        if result['traf_status']['status'] == WARN and not build:
            log_err('Trafodion process is found, please stop it first')
        if result['hbase_ver']['status'] == ERR:
            log_err('HBase check error: HBase version [%s]' % result['hbase_ver']['value'])
        else:
            cfgs['hbase_ver'] = result['hbase_ver']['value']
        if result['home_dir']['value']: # trafodion user exists
            has_home_dir += 1
            cfgs['home_dir'] = result['home_dir']['value']
        if result['hadoop_auth']['value'] == 'kerberos':
            cfgs['secure_hadoop'] = 'Y'
        else:
            cfgs['secure_hadoop'] = 'N'

        license_status = result['license_status']['value']
        if result['license_status']['status'] == ERR:
            need_license += 1

        # we only use the disks which have the same mount point on all nodes
        mount_disks = result['mount_disks']['value'].split(',')
        mount_disks_all.append(mount_disks)

        kernel_ver = result['kernel_ver']['value']
        kernel_vers.append(kernel_ver)

        cfgs['hadoop_group_mapping'] = result['hadoop_group_mapping']['value']
        cfgs['aws_ec2'] = result['aws_ec2']['value']

    common_disks = set(mount_disks_all[0])
    for disk in mount_disks_all:
        common_disks = common_disks.intersection(disk)

    scratch_locs = ','.join(['%s/%s' % (disk, OVERFLOW_DIR_NAME) for disk in common_disks if disk])

    if len(list(set(kernel_vers))) > 1:
        warn('Linux kernel version is not consistent on all nodes, please check.')
    ##### Discover End #####

    if offline:
        g('local_repo_dir')
        if os.path.exists(cfgs['local_repo_dir']):
            if not glob('%s/repodata' % cfgs['local_repo_dir']):
                log_err('repodata directory not found, this is not a valid repository directory')
            cfgs['repo_ip'] = socket.gethostbyname(socket.gethostname())
            ports = ParseInI(DEF_PORT_FILE, 'ports').load()
            cfgs['repo_http_port'] = ports['repo_http_port']
        elif '404 Not Found' not in cmd_output('curl %s' % cfgs['local_repo_dir']):
            cfgs['repo_url'] = cfgs['local_repo_dir']
        else:
            log_err('local repository folder or repository URL not exist')
        cfgs['offline_mode'] = 'Y'

    pkg_list = ['apache-trafodion_server', 'esgynDB_server','QianBase_server']
    # find tar in installer folder, if more than one found, use the first one
    for pkg in pkg_list:
        tar_loc = glob('%s/*%s*.tar.gz' % (INSTALLER_LOC, pkg))
        if tar_loc:
            cfgs['traf_package'] = tar_loc[0]
            break

    g('traf_package')

    # constant generated at build time
    cfgs['traf_version'] = DEF_TRAFODION_VER
    # get basename from tar filename
    try:
        pattern = '|'.join(pkg_list)
        cfgs['traf_basename'] = re.search(r'.*(%s).*-\d+\.\d+\.\d+.*' % pattern, cfgs['traf_package']).groups()[0]
    except:
        log_err('Invalid package tar file')
    server_type, server_major = re.search(r'.*-(\D+)(\d+)-.*', cfgs['traf_package']).groups()
    for result in system_discover:
        os_major = re.search(r'\D+(\d+)', result['linux_distro']['value']).group(1)
        if server_major != os_major:
            log_err('Version of server package is [%s%s], cannot match system major version [%s]' \
                    % (server_type, server_major, os_major))

    def input_cgroup():
        numchk = lambda n: 1 <= n <= 100
        g('cgroups_cpu_pct')
        if not numchk(int(cfgs['cgroups_cpu_pct'])):
            log_err('Invalid number, should be between 1 to 100.')
        g('cgroups_mem_pct')
        if not numchk(int(cfgs['cgroups_mem_pct'])):
            log_err('Invalid number, should be between 1 to 100.')

    cfgs['req_license'] = 'N'
    if not need_license:
        g('req_license')

    if not cfgs['license_used']:
        cfgs['license_used'] = 'NONE'
    if need_license or cfgs['req_license'].upper() == 'Y':
        cfgs['req_license'] = 'Y'
        if os.path.exists(DEMO_LICENSE):
          g('license_demo')
          if cfgs['license_demo'].upper() == 'DEMO':
            cfgs['license_used'] = DEMO_LICENSE
          else:
            cfgs['license_used'] =  cfgs['license_demo']
        else:
          g('license_used')
        try:
            lic = License(cfgs['license_used'])
            # multi tenancy
            if lic.is_multi_tenancy_enabled():
                input_cgroup()
        except StandardError as e:
            log_err(str(e))
    elif not need_license and license_status == 'MTOK':
        input_cgroup()

    if has_home_dir != node_count:
        g('home_dir')
        g('traf_pwd')

    # set traf_home location
    if not cfgs['traf_dirname']:
        cfgs['traf_dirname'] = '%s-%s' % (cfgs['traf_basename'], cfgs['traf_version'])
    g('traf_dirname')
    if cfgs['traf_dirname'].startswith('/'):
        if cfgs['traf_dirname'].count('/') == 1:
            log_err('cannot set directory under root folder \'/\'')
        else:
            hosts = cfgs['node_list'].split(',')
            traf_folder = cfgs['traf_dirname'][:cfgs['traf_dirname'].rfind('/')]
            check_remote_exists(hosts, pwd, traf_folder)
        cfgs['traf_abspath'] = cfgs['traf_dirname']
    else:
        cfgs['traf_abspath'] = '%s/%s/%s' % (cfgs['home_dir'], cfgs['traf_user'], cfgs['traf_dirname'])
    # check traf_abspath exist
    hosts = cfgs['node_list'].split(',')
    not_exists = check_remote_exists(hosts, pwd, cfgs['traf_abspath'], False)
    exists = set(hosts).difference(set(not_exists.strip().split()))
    if exists:
        result = raw_input('Path \'%s\' has existed on node(s) \'%s\', it will fail to start Trafodion. Continue(Y/N): '
                           % (cfgs['traf_abspath'], ' '.join(exists)))
        if result.strip().upper() == 'N':
            sys.exit(0)

    # aws env needs to set up aws cli tools for trafodion user
    if cfgs['aws_ec2'] == 'EC2':
        print '** AWS Cloud environment detected'
        hosts = cfgs['node_list'].split(',')
        nodes = check_remote_exists(hosts, pwd, cfgs['home_dir'] + '/' + cfgs['traf_user'] + '/.aws', check=False)
        if not nodes:
            g('aws_overwrite')
        if not cfgs['aws_overwrite'] or cfgs['aws_overwrite'].upper() == 'Y':
            g('aws_access_key_id')
            g('aws_secret_access_key')
            g('aws_region_name')
            g('aws_output_format')

    g('dcs_cnt_per_node')
    # set overflow folder to data disk if detected
    if scratch_locs:
        cfgs['scratch_locs'] = scratch_locs
    g('scratch_locs')
    if not cfgs['traf_log']: cfgs['traf_log'] = '/var/log/trafodion'
    g('traf_log')
    if not cfgs['traf_var']: cfgs['traf_var'] = '/var/lib/trafodion'
    g('traf_var')
    if not cfgs['core_loc']: cfgs['core_loc'] = DEF_TRAF_HOME
    g('core_loc')
    g('traf_start')

    # kerberos
    if cfgs['secure_hadoop'] == 'Y':
        g('kdc_server')
        g('admin_principal')
        g('kdcadmin_pwd')

    # ldap security
    g('ldap_security')
    if cfgs['ldap_security'].upper() == 'Y':
        info("Set LDAP as type of trafodion authentication")
        g('db_root_user')
        g('db_admin_user')
        g('db_admin_pwd')
        g('ldap_hosts')
        g('ldap_port')
        g('ldap_identifiers')
        g('ldap_encrypt')
        if cfgs['ldap_encrypt'] == '1' or cfgs['ldap_encrypt'] == '2':
            g('ldap_certpath')
        elif cfgs['ldap_encrypt'] == '0':
            cfgs['ldap_certpath'] = ''
        else:
            log_err('Invalid ldap encryption level')

        o('ldap_userinfo')
        if cfgs['ldap_userinfo'].upper() == 'Y':
            g('ldap_user')
            g('ldap_pwd')

        o('ldap_srch_grp_info')
        if cfgs['ldap_srch_grp_info'].upper() == 'Y':
            g('ldap_srch_grp_base')
            g('ldap_srch_grp_obj_class')
            g('ldap_srch_grp_mem_attr')
            g('ldap_srch_grp_name_attr')
        cfgs["TRAFODION_AUTHENTICATION_TYPE"] = "LDAP"
    else:
        info("Set LOCAL as type of trafodion authentication")
        cfgs["TRAFODION_PASSWORD_CHECK_SYNTAX"] = "0680FFFFFFFF"
        g('db_root_user')
        g('db_admin_user')
        g("use_default_admin_password")
        if cfgs["use_default_admin_password"].upper() == "N":
            show_password_policy(cfgs["TRAFODION_PASSWORD_CHECK_SYNTAX"])
            while True:
                g("db_admin_pwd")
                if password_check(cfgs["db_admin_pwd"], cfgs["TRAFODION_PASSWORD_CHECK_SYNTAX"]) == True:
                    break
                print "Input password is not meeting the password rules."
        else:
            #not write into prompt.json
            cfgs["db_admin_pwd"] = "esgynDB123"
            info("The default password for admin user is %s" %
                 cfgs["db_admin_pwd"])
        # if not using LDAP,use local authentication with no password login in default
        g("local_security_with_password")
        if cfgs["local_security_with_password"].upper() == 'N':
            cfgs["TRAFODION_AUTHENTICATION_TYPE"] = "SKIP_AUTHENTICATION"
            info("Will be no check password when login. Be careful to stay secure.")
        else:
            cfgs["TRAFODION_AUTHENTICATION_TYPE"] = "LOCAL"

    # Enable/Disable SSL for DB Manager and REST
    g('enable_https')
    if cfgs['enable_https'].upper() == 'Y':
        g('keystore_pwd')
        if len(cfgs['keystore_pwd']) < 6:
            log_err('Keystore password must be at least 6 characters')

    # Use floating IP 
    g('dcs_ha')
    cfgs['enable_ha'] = 'false'
    cfgs['dcs_ha_keepalived'] = 'false'

    if cfgs['dcs_ha'].upper() == 'Y':
        g('dcs_floating_ip')
        g('dcs_interface')
        cfgs['enable_ha'] = 'true'
        g('dcs_ha_keepalive')
        if cfgs['dcs_ha_keepalive'].upper() == 'Y':
            cfgs['dcs_ha_keepalived'] = 'true'

    # Enable/Disable mds
    cfgs['dcs_mds'] == 'Y'
    g('dcs_mds')

    # OM HA architecture
    cfgs['om_ha_arch'] == 'N'
    g('om_ha_arch')
    if cfgs['om_ha_arch'].upper() == 'Y':
        g('es_virtual_ip')

    if need_java_home:
        g('java_home')
    else:
        # don't overwrite user input java home
        if not cfgs['java_home']:
            cfgs['java_home'] = java_home
    
    if customized == 'CUSTOM_AP':
        cfgs['customized'] = 'CUSTOM_AP'
    elif customized == 'CUSTOM_TP':
        cfgs['customized'] = 'CUSTOM_TP'
    else :
        cfgs['customized']='CUSTOM_NO'

    # lock
    g("use_lock")
    hbase_env_opt = ""
    if cfgs['use_lock'].upper() == 'Y':
        hbase_env_opt = "ENABLE_ROW_LEVEL_LOCK=1\n"
        hbase_env_opt += "LOCK_ENABLE_DEADLOCK_DETECT=1\n"
        # https://abc.locl,abc2.local:3333
        rest_uri = ""
        ports = ParseInI(DEF_PORT_FILE, 'ports').load()
        # HTTPS ?
        if cfgs['enable_https'].upper() == 'Y':
            rest_uri += "https://"
            rest_port = ports['rest_https_port']
        else:
            rest_uri += "http://"
            rest_port = ports['rest_http_port']
        # get rest servers
        for dcs_master_node in cfgs['dcs_master_nodes'].split(','):
            rest_uri += dcs_master_node + ","
        # drop last of ,
        rest_uri = rest_uri[:-1]
        rest_uri += ":" + rest_port
        hbase_env_opt += ("REST_SERVER_URI=\"" + rest_uri + "\"")
    else:
        hbase_env_opt = "ENABLE_ROW_LEVEL_LOCK=0"
    #read from mod_cfgs.json
    #mod_cfgs_json_file = sys.path[0] + "/configs/mod_cfgs.json"
    if customized == "CUSTOM_AP":
        modcfg_file = CUSTOM_MODCFG_FILE_AP
    elif customized == "CUSTOM_TP":
        modcfg_file = CUSTOM_MODCFG_FILE_TP
    else:
        modcfg_file = MODCFG_FILE
    hadoop_conf_dic = ParseJson(modcfg_file).load()
    lines = ""
    #get current config on hbase
    if 'CDH' in cfgs['distro']:
        hadoop_mod = CDHMod(cfgs['mgr_user'], cfgs['mgr_pwd'],
                            cfgs['mgr_url'], cfgs['cluster_name'])
        # current_HBase_env_string = hadoop_mod.get_hbase_env_opt()
        # tmp = current_HBase_env_string.split("\n")
        # for line in tmp:
        #     if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
        #         if re.search(r"^LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
        #             if re.search(r"^REST_SERVER_URI\s*=.*", line) == None:
        #                 if line != "":
        #                     lines += (line + "\n")
        # lines += hbase_env_opt
        for item in hadoop_conf_dic["ENV_CONFIG"]:
            if item["serviceName"].lower() == "hbase":
                has_config = False
                for sub_item in item["config"]["items"]:
                    if sub_item["name"] == "hbase_service_env_safety_valve":
                        for line in sub_item["value"].split('\n'):
                            if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                                if re.search(r"^LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
                                    if re.search(r"^REST_SERVER_URI\s*=.*", line) == None:
                                        if line != "":
                                            lines += (line + "\n")
                        sub_item["value"] = lines + hbase_env_opt
                        has_config = True
                        break
                if not has_config:
                    item["config"]["items"].append(
                        {"name": "hbase_service_env_safety_valve", "value": hbase_env_opt})
                    break
    elif 'HDP' in cfgs['distro']:
        hadoop_mod = HDPMod(cfgs['mgr_user'], cfgs['mgr_pwd'],
                            cfgs['mgr_url'], cfgs['cluster_name'])
        current_HBase_env_option = hadoop_mod.get_hbase_env_opt()
        tmp = current_HBase_env_option["content"].split("\n")
        for line in tmp:
            if re.search(r"^export ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                if re.search(r"^export LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
                    if re.search(r"^export REST_SERVER_URI\s*=.*", line) == None:
                        lines += (line + "\n")
        tmp = hbase_env_opt.split("\n")
        for line in tmp:
            lines += ("export %s\n" % line)
        lines = lines[:-1]
        current_HBase_env_option.update({"content": lines})
        if hadoop_conf_dic["MOD_CFGS"].has_key("hbase-env"):
            hadoop_conf_dic["MOD_CFGS"].update(
                {"hbase-env": current_HBase_env_option})
        else:
            hadoop_conf_dic["MOD_CFGS"]["hbase-env"] = current_HBase_env_option
    #write to mod_cfgs.json
    ParseJson(modcfg_file).save(hadoop_conf_dic)

    g('enable_binlog')
    if cfgs['enable_binlog'].upper() == 'Y':
        g('rebuild_binlog')

    if not silent:
        u.notify_user(DBCFG_FILE, STATUS_FILE)

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion install main script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--config-file", dest="cfgfile", metavar="FILE",
                      help="Trafodion config file. If provided, all install prompts \
                            will be taken from this file and not prompted for.")
    parser.add_option("-l", "--log-file", dest="logfile", metavar="FILE",
                      help="Specify the absolute path name of log file.")
    parser.add_option("-s", "--stat-file", dest="statfile", metavar="FILE",
                      help="Specify the absolute path name of status.")
    parser.add_option("-u", "--remote-user", dest="user", metavar="USER",
                      help="Specify ssh login user for remote server, \
                            if not provided, use current login user as default.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Verbose mode, will print commands.")
    parser.add_option("--silent", action="store_true", dest="silent", default=False,
                      help="Do not ask user to confirm configuration result")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("--build", action="store_true", dest="build", default=False,
                      help="Build the config file in guided mode only.")
    parser.add_option("-p", "--password", dest="password", metavar="PASSWD",
                      help="Specify ssh login password for remote server.")
    parser.add_option("--reinstall", action="store_true", dest="reinstall", default=False,
                      help="Reinstall Trafodion without restarting Hadoop.")
    parser.add_option("--apache-hadoop", action="store_true", dest="apache", default=False,
                      help="Install Trafodion on top of Apache Hadoop.")
    parser.add_option("--offline", action="store_true", dest="offline", default=False,
                      help="Enable local repository for offline installing Trafodion.")
    parser.add_option("--version", action="store_true", dest="version", default=False,
                      help="Pyinstaller package version.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    parser.add_option("--customized-config", dest="customized", metavar="CUSTOMIZED-CONFIG",
                      help="Customized config file. If enabled, it will set CQD, ms_env and CDH by configs/custom_cqd.sql,\
                            configs/custom_ms_env.ini and configs/custom_mod_cfgs.json.")
    parser.add_option("--not-enhance-ha", action="store_true", dest="not_enhance_ha", default=False,
                      help="Disable faster HA recovery for Trafodion.")

    (options, args) = parser.parse_args()
    return options

def main():
    """ db_installer main loop """
    global cfgs, pyinstaller_type, pyinstaller_major
    if not os.path.exists(INSTALLER_LOC+'/PyInstallerVer'):
        log_err('PyInstallerVer file does not exist')
    else:
        with open(INSTALLER_LOC+'/PyInstallerVer') as f:
            pyinstaller_info = f.read()
            pyinstaller_type, pyinstaller_major = re.search(r'.*-(\D+)(\d+)', pyinstaller_info).groups()

    # handle parser option
    options = get_options()

    if options.version:
        print pyinstaller_info
        sys.exit(0)

    format_output('Trafodion Installation ToolKit')

    if options.build and options.cfgfile:
        log_err('Wrong parameter, cannot specify both --build and --config-file')

    if options.build and options.offline:
        log_err('Wrong parameter, cannot specify both --build and --offline')

    if options.cfgfile:
        if not os.path.exists(options.cfgfile):
            log_err('Cannot find config file \'%s\'' % options.cfgfile)
        config_file = options.cfgfile
    else:
        config_file = DBCFG_FILE

    if options.pwd:
        if options.password:
            pwd = options.password
        else:
            pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    # not specified config file and default config file doesn't exist either
    p = ParseInI(config_file, 'dbconfigs')
    if options.build or (not os.path.exists(config_file)):
        if options.build: format_output('DryRun Start')
        user_input(options, prompt_mode=True, pwd=pwd)

        # save config file as json format
        print '\n** Generating config file [%s] to save configs ... \n' % config_file
        p.save(cfgs)
    # config file exists
    else:
        print '\n** Loading configs from config file [%s] ... \n' % config_file
        cfgs = p.load()
        if options.offline and cfgs['offline_mode'] != 'Y':
            log_err('To enable offline mode, must set "offline_mode = Y" in config file')
        user_input(options, prompt_mode=False, pwd=pwd)

    if options.reinstall:
        cfgs['reinstall'] = 'Y'

    if options.offline:
        if not cfgs['repo_url']:
            http_start(cfgs['local_repo_dir'], cfgs['repo_http_port'])
    else:
        cfgs['offline_mode'] = 'N'

    if not options.build:
        format_output('Installation Start')

        # force gc to clean up RemoteRun instance
        gc.collect()

        ### perform actual installation ###
        if options.logfile and options.statfile:
            wrapper.run(cfgs, options, pwd=pwd, log_file=options.logfile, stat_file=options.statfile)
        elif options.logfile:
            wrapper.run(cfgs, options, pwd=pwd, log_file=options.logfile)
        elif options.statfile:
            wrapper.run(cfgs, options, pwd=pwd, stat_file=options.statfile)
        else:
            wrapper.run(cfgs, options, pwd=pwd)

        format_output('Installation Complete')

        if options.offline: http_stop()

        # rename default config file when successfully installed
        # so next time user can input new variables for a new install
        # or specify the backup config file to install again
        try:
            # only rename default config file
            ts = time.strftime('%y%m%d_%H%M')
            if config_file == DBCFG_FILE and os.path.exists(config_file):
                os.rename(config_file, config_file + '.bak' + ts)
        except OSError:
            log_err('Cannot rename config file')
    else:
        format_output('DryRun Complete')

    # remove temp config file
    if os.path.exists(DBCFG_TMP_FILE): os.remove(DBCFG_TMP_FILE)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        tp = ParseInI(DBCFG_TMP_FILE, 'dbconfigs')
        tp.save(cfgs)
        http_stop()
        print '\nAborted...'
