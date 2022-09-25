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
from scripts.yaml import YAML
from scripts import wrapper
from scripts.constants import *
from scripts.constants_release import *
from scripts.common import get_sudo_prefix, HadoopDiscover, Remote, License, ParseHttp, ParseInI, ParseJson, run_cmd, \
    info, warn, http_start, http_stop, format_output, err_m, expNumRe, cmd_output, Generate_Password_Policy, \
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


def auto_allocate(hosts):
    """enable mgmt node if node count is larger than mgmt_th"""
    mgmt_th = 6

    if type(hosts) != list: err_m('hosts parameter should be a list')
    host_num = len(hosts)
    # node<=3, ZK=1 ,node>3, ZK=3
    zk_num = 1 if host_num <= 3 else 3
    jn_num = 3

    if cfgs['hadoop_ha'] == 'Y':
        cfgs['nn_host'] = cfgs['hm_host'] = cfgs['rm_host'] = ','.join(hosts[:2])
    elif cfgs['hadoop_ha'] == 'N':
        cfgs['nn_host'] = cfgs['snn_host'] = cfgs['hm_host'] = cfgs['rm_host'] = hosts[0]
    cfgs['hms_host'] = cfgs['jhs_host'] = hosts[0]
    # with mgmt node
    if host_num >= mgmt_th:
        cfgs['dn_hosts'] = cfgs['rs_hosts'] = cfgs['nm_hosts'] = ','.join(hosts[1:])
    # without mgmt node
    else:
        cfgs['dn_hosts'] = cfgs['rs_hosts'] = cfgs['nm_hosts'] = ','.join(hosts)

    cfgs['zk_hosts'] = ','.join(hosts[-zk_num:])
    cfgs['hbase_hosts'] = cfgs['hadoop_hosts'] = ','.join(hosts)

    if cfgs['hadoop_ha'] == 'Y':
        cfgs['jn_hosts'] = ','.join(hosts[-jn_num:])


def get_host_allocate(roles):
    valid_roles = ['DN', 'RS', 'ZK', 'HM', 'NN', 'SNN', 'NM', 'RM', 'JHS', 'JN', 'QB']
    role_host = defaultdict(list)
    hadoop_list = []

    # roles is a dict
    items = [[h, r.split(',')] for h, r in roles.iteritems()]
    for item in items:
        hadoop_list.append(item[0].lower())
        for role in item[1]:
            role = role.strip()
            if role not in valid_roles: err_m('Incorrect role config')
            role_host[role].append(item[0].lower())

    for val in role_host.values():
        val.sort()

    cfgs['nn_host'] = role_host['NN'][0]
    cfgs['dn_hosts'] = ','.join(role_host['DN'])
    cfgs['snn_host'] = role_host['SNN'][0] if role_host['SNN'] else cfgs['nn_host']
    cfgs['hm_host'] = ','.join(role_host['HM'])
    cfgs['rs_hosts'] = ','.join(role_host['RS'])
    cfgs['rm_host'] = ','.join(role_host['RM'])
    cfgs['nm_hosts'] = ','.join(role_host['NM'])
    cfgs['jhs_host'] = ','.join(role_host['JHS'])
    cfgs['zk_hosts'] = ','.join(role_host['ZK'])
    hadoop_list.sort()
    cfgs['hadoop_list'] = cfgs['hbase_hosts'] = cfgs['hadoop_hosts'] = ','.join(hadoop_list)


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
    return nodes  # directory not exist


def user_input(options, prompt_mode=True, pwd=''):
    """ get user's input and check input value """
    global cfgs, pyinstaller_type, pyinstaller_major

    offline = True if hasattr(options, 'offline') and options.offline else False
    silent = True if hasattr(options, 'silent') and options.silent else False
    build = True if hasattr(options, 'build') and options.build else False
    customized = True if hasattr(options, 'customized') and options.customized else False
    enhance_ha = False if hasattr(options, 'not_enhance_ha') and options.not_enhance_ha else True
    hadoop_ha = False
    install_qian = True if hasattr(options, 'install_qian') and options.install_qian else False
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

    cfgs['enhance_ha'] = 'Y' if enhance_ha else 'N'
    cfgs['hadoop_ha'] = 'Y' if hadoop_ha else 'N'
    cfgs['install_qian'] = 'Y' if install_qian else 'N'
    ### begin user input ###
    if os.path.exists('%s/omclient/allocate.yaml' % DEF_TRAF_HOME_DIR):
        yaml = YAML()
        with open('%s/omclient/allocate.yaml' % DEF_TRAF_HOME_DIR, 'r') as f:
            roles = yaml.load(f)
            roles = {item['host']: item['role'] for item in roles['hosts']}
    else:
        roles = ParseInI('%s/hadoop_roles.ini' % CONFIG_DIR, 'roles').load()

    if not roles:
        g('hadoop_list')
        hadoop_list = expNumRe(cfgs['hadoop_list'])
        # switch alis hostname to hostname
        hadoop_list = [socket.gethostbyaddr(node)[0].lower() for node in hadoop_list]
        if len(hadoop_list) < 3 and hadoop_ha:
            log_err('If want to enable Hadoop and HBase HA, need three nodes at least')
        auto_allocate(hadoop_list)
        cfgs['hadoop_list'] = ','.join(hadoop_list)
    else:
        get_host_allocate(roles)
    cfgs['cdhnodes'] = cfgs['exporter_list'] = cfgs['hadoop_list']

    # Mysql
    # g('mysql_hosts')
    # mysql_list = expNumRe(cfgs['mysql_hosts'])
    # # switch alis hostname to hostname
    # mysql_list = [socket.gethostbyaddr(node)[0] for node in mysql_list]
    #
    # if len(mysql_list) != 2: log_err('Incorrect node list, need two nodes to install Mysql')
    # mysql_list = verify_nodes(hadoop_list, mysql_list)
    # if not mysql_list:
    #     log_err('Incorrect Mysql list, should be part of Hadoop nodes')
    #
    # cfgs['mysql_hosts'] = ','.join(mysql_list)
    # cfgs['first_node'] = mysql_list[0]
    # g('mysql_package')
    # cfgs['mysql_dir'] = os.path.basename(cfgs['mysql_package'])
    # cfgs['mysql_ha'] = json.dumps(ParseInI(MY_CFG_TEMPLATE_INI, 'mysqld').load())
    # cfgs['root_passwd'] = cfgs['repl_passwd'] = 'traf123'

    #Postgres
    # cfgs['hive_pwd'] = 'hive'
    # g('pg_host')
    # cfgs['pg_host'] = socket.gethostbyaddr(cfgs['pg_host'])[0]

    rsnodes = cfgs['rs_hosts'].split(',')
    cfgs['rsnodes'] = cfgs['rs_hosts']  # convert to back to comma separated list
    g('use_rs_node')
    if cfgs['use_rs_node'].upper() == 'Y':
        cfgs['node_list'] = cfgs['rs_hosts']  # convert to back to comma separated list
    else:
        g('node_list')
        node_lists = expNumRe(cfgs['node_list'])

        node_lists = verify_nodes(rsnodes, node_lists)
        if not node_lists:
            log_err('Incorrect node list, should be part of RegionServer nodes')
        cfgs['node_list'] = ','.join(node_lists)  # convert to back to comma separated list

    cfgs['hadoop_home'] = DEF_INSTALL_HADOOP_HOME
    cfgs['hbase_home'] = DEF_INSTALL_HBASE_HOME
    cfgs['hive_home'] = DEF_INSTALL_HIVE_HOME
    cfgs['zookeeper_home'] = DEF_INSTALL_ZK_HOME
    cfgs['hdfs_user'] = cfgs['hbase_user'] = cfgs['hive_user'] = cfgs['zookeeper_user'] = 'hadoop'
    cfgs['distro'] = 'CDH_APACHE'
    cfgs['hbase_lib_path'] = cfgs['hbase_home'] + '/lib'
    cfgs['hbase_ver'] = '1.2'

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

    cfgs['hbase_xml_file'] = cfgs['hbase_home'] + '/conf/hbase-site.xml'
    cfgs['hbase_env_file'] = cfgs['hbase_home'] + '/conf/hbase-env.sh'
    cfgs['hdfs_xml_file'] = cfgs['hadoop_home'] + '/etc/hadoop/hdfs-site.xml'
    cfgs['hadoop_env_file'] = cfgs['hadoop_home'] + '/etc/hadoop/hadoop-env.sh'
    cfgs['core_site_xml_file'] = cfgs['hadoop_home'] + '/etc/hadoop/core-site.xml'
    cfgs['zoo_cfg_file'] = cfgs['zookeeper_home'] + '/conf/zoo.cfg'

    ##### Discover Start #####
    ### discover system settings, return a dict
    cfgs['ssh_port'] = ssh_port
    system_discover = wrapper.run(cfgs, options, mode='alldiscover', param='allinstall', pwd=pwd)

    # check discover results, return error if fails on any single node
    node_count = len(cfgs['node_list'].split(','))
    need_license = has_home_dir = 0
    kernel_vers = []
    mount_disks_all = []
    for result in system_discover:
        os_major = re.search(r'\D+(\d+)', result['linux_distro']['value']).group(1)
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
        if result['home_dir']['value']:  # trafodion user exists
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
    dfs_locs = ','.join(['%s/%s' % (disk, DFS_DIR_NAME) for disk in common_disks if disk])

    if len(list(set(kernel_vers))) > 1:
        warn('Linux kernel version is not consistent on all nodes, please check.')
    ##### Discover End #####

    if not install_qian:
        g('hadoop_tar')
        if dfs_locs:
            cfgs['dfs_locs'] = dfs_locs
        g('dfs_locs')
        name_dirs = ' '.join(['%s/nn/current' % dfs_loc for dfs_loc in cfgs['dfs_locs'].split(',')])
        name_dir_not_exists = check_remote_exists(cfgs['nn_host'].split(','), pwd, name_dirs, False)
        name_dir_exists = set(cfgs['nn_host'].split(',')).difference(set(name_dir_not_exists.strip().split(' ')))
        if name_dir_exists:
            print '** \'%s\' has existed on nodes : [%s]' % (cfgs['dfs_locs'], ' '.join(name_dir_exists))
            g('format_nn')
        else:
            cfgs['format_nn'] = 'Y'

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

    pkg_list = ['apache-trafodion_server', 'esgynDB_server', 'QianBase_server']
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
                cfgs['license_used'] = cfgs['license_demo']
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

    cfgs['java_home'] = "/usr/lib/jvm/java-1.8.0-openjdk"

    cfgs['customized'] = 'Y' if customized else 'N'

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
    # read from mod_cfgs.json
    # mod_cfgs_json_file = sys.path[0] + "/configs/mod_cfgs.json"
    hadoop_conf_dic = ParseJson(MODCFG_FILE).load()
    lines = ""
    # get current config on hbase
    if cfgs['distro'] == 'CDH':
        hadoop_mod = CDHMod(cfgs['mgr_user'], cfgs['mgr_pwd'],
                            cfgs['mgr_url'], cfgs['cluster_name'])
        current_HBase_env_string = hadoop_mod.get_hbase_env_opt()
        tmp = current_HBase_env_string.split("\n")
        for line in tmp:
            if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                if re.search(r"^LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
                    if re.search(r"^REST_SERVER_URI\s*=.*", line) == None:
                        if line != "":
                            lines += (line + "\n")
        lines += hbase_env_opt
        for item in hadoop_conf_dic["ENV_CONFIG"]:
            if item["serviceName"].lower() == "hbase":
                has_config = False
                for sub_item in item["config"]["items"]:
                    if sub_item["name"] == "hbase_service_env_safety_valve":
                        sub_item["value"] = lines
                        has_config = True
                        break;
                if not has_config:
                    item["config"]["items"].append(
                        {"name": "hbase_service_env_safety_valve", "value": lines})
                    break;
    elif cfgs['distro'] == 'HDP':
        hadoop_mod = HDPMod(cfgs['mgr_user'], cfgs['mgr_pwd'], cfgs['mgr_url'], cfgs['cluster_name'])
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
            hadoop_conf_dic["MOD_CFGS"].update({"hbase-env": current_HBase_env_option})
        else:
            hadoop_conf_dic["MOD_CFGS"]["hbase-env"] = current_HBase_env_option
    else:
        hbase_env = {}
        for line in hbase_env_opt.split('\n'):
            key, value = line.split('=')
            hbase_env[key] = value
        hadoop_conf_dic["MOD_CFGS"]["hbase-env"] = hbase_env
    # write to mod_cfgs.json
    ParseJson(MODCFG_FILE).save(hadoop_conf_dic)

    if not silent:
        u.notify_user(DBCFG_FILE, ALLSTATUS_FILE)


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion and Hadoop install main script.'
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
    parser.add_option("--offline", action="store_true", dest="offline", default=False,
                      help="Enable local repository for offline installing Trafodion.")
    parser.add_option("--version", action="store_true", dest="version", default=False,
                      help="Pyinstaller package version.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    parser.add_option("--customized-config", action="store_true", dest="customized", default=False,
                      help="Customized config file. If enabled, it will set CQD, ms_env and CDH by configs/custom_cqd.sql,\
                            configs/custom_ms_env.ini and configs/custom_mod_cfgs.json.")
    parser.add_option("--not-enhance-ha", action="store_true", dest="not_enhance_ha", default=False,
                      help="Enable faster HA recovery for Trafodion.")
    parser.add_option("--install-qianbase-only", action="store_true", dest="install_qian", default=False,
                      help="Only install QianBase.")

    (options, args) = parser.parse_args()
    return options


def main():
    """ db_installer main loop """
    global cfgs, pyinstaller_type, pyinstaller_major
    if not os.path.exists(INSTALLER_LOC + '/PyInstallerVer'):
        log_err('PyInstallerVer file does not exist')
    else:
        with open(INSTALLER_LOC + '/PyInstallerVer') as f:
            pyinstaller_info = f.read()
            pyinstaller_type, pyinstaller_major = re.search(r'.*-(\D+)(\d+)', pyinstaller_info).groups()

    # handle parser option
    options = get_options()

    if options.version:
        print pyinstaller_info
        sys.exit(0)

    format_output('Trafodion and Hadoop Installation ToolKit')

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
            wrapper.run(cfgs, options, 'allinstall', pwd=pwd, log_file=options.logfile, stat_file=options.statfile)
        elif options.logfile:
            wrapper.run(cfgs, options, 'allinstall', pwd=pwd, log_file=options.logfile)
        elif options.statfile:
            wrapper.run(cfgs, options, 'allinstall', pwd=pwd, stat_file=options.statfile)
        else:
            wrapper.run(cfgs, options, 'allinstall', pwd=pwd)

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
