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
import socket
import json
import getpass
import logging
import time
import sys
reload(sys)
import gc
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from glob import glob
from collections import defaultdict
from threading import Thread
from scripts import wrapper
from db_install import UserInput
from scripts.constants import *
from scripts.common import License, ParseInI, get_sudo_prefix, Remote, run_cmd, run_cmd_as_user, info, ok, warn, \
                           format_output, err_m, expNumRe, cmd_output, cmd_output_as_user, append_file, http_start, http_stop

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion add nodes main script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-n", "--nodes", dest="nodes", metavar="NODES",
                      help="Specify the node names you want to add, separated by comma if more than one, \
                            support numeric RE, e.g. node[1-3].com")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Verbose mode, will print commands.")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("--offline", action="store_true", dest="offline", default=False,
                      help="Enable local repository to add Trafodion nodes offline.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")

    (options, args) = parser.parse_args()
    return options

def main():
    """ add_nodes main loop """
    cfgs = defaultdict(str)

    # handle parser option
    options = get_options()
    if not options.nodes:
        err_m('Must specifiy the node names using \'--nodes\' option')

    # get node list from user input
    nodes = expNumRe(options.nodes)
    if not nodes:
        err_m('Incorrect format')

    def get_fully_domain_name():
        node_pat = re.compile(r'.*?\..*')
        for node in nodes:
            if node_pat.match(node):
                hostname = node
            else:
                hostname = cmd_output('cat /etc/hosts | grep %s$' % node).split()[1]
            yield hostname.lower()
    new_nodes = list(get_fully_domain_name())
    wnew_nodes = ' '.join(['-w %s' % node for node in new_nodes])  # -w node_name1 -w node_name2

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    ssh_port = options.port if hasattr(options, 'port') and options.port else ''

    u = UserInput(options, pwd, cfgs)

    ### read configs from current trafodion_config and save it to cfgs
    if os.path.exists(TRAF_CFG_FILE):
        with open(TRAF_CFG_FILE, 'r') as f:
            traf_cfgs = f.readlines()
        for traf_cfg in traf_cfgs:
            if not traf_cfg.strip(): continue
            if traf_cfg.startswith('#'): continue
            key, value = traf_cfg.replace('export ', '').split('=')
            value = value.replace('"','')
            value = value.replace('\n','')
            cfgs[key.lower()] = value
    else:
        err_m('Cannot find %s, be sure to run this script on one of trafodion nodes' % TRAF_CFG_FILE)

    ### config check
    if not cfgs['hbase_lib_path'] or not cfgs['traf_version']:
        err_m('Missing parameters in Trafodion config file')

    traf_abspath = cfgs['traf_abspath']
    if not traf_abspath or not cmd_output('%s ls %s' % (get_sudo_prefix(), traf_abspath)):
        err_m('Cannot find trafodion binary folder')

    ### get trafodion user from traf_home path
    cfgs['traf_user'] = traf_abspath.split('/')[-2]
    if not cfgs['traf_user']:
        err_m('Cannot detect trafodion user')

    ### get trafodion home directory
    cfgs['home_dir'] = traf_abspath[:traf_abspath.rfind('/'+cfgs['traf_user'])]

    ### parse trafodion user's password
    cfgs['traf_shadow'] = cmd_output("%s grep %s /etc/shadow |awk -F: '{print $2}'" % (get_sudo_prefix(), cfgs['traf_user']))

    ### node/license check
    current_nodes = cmd_output_as_user(cfgs['traf_user'], 'trafconf -name 2>/dev/null').split()
    all_nodes = current_nodes + new_nodes

    exist_nodes = list(set(new_nodes).intersection(current_nodes))
    if exist_nodes:
        err_m('Node(s) %s already exist in the cluster' % exist_nodes)

    cfgs['ssh_port'] = ssh_port

    if not os.path.exists(EDB_LIC_FILE):
        err_m('Cannot find esgyndb license file %s' % EDB_LIC_FILE)
    else:
        try:
            lic = License(EDB_LIC_FILE)
            lic.check_license(len(all_nodes))
        except StandardError as e:
            err_m(str(e))

    def copy_files():
        # package trafodion binary into a tar file
        run_cmd('rm -f TMP_TRAF_PKG_FILE')
        info('Creating trafodion packages of %s, this will take a while ...' % traf_abspath)
        excludes = '--exclude=*.log --exclude *.log.* --exclude *.out --exclude *.out.* --exclude core[-.]* --exclude *.pid --exclude *.trace.* --exclude monitor.map.* --exclude monitor.port.*'
        run_cmd_as_user(cfgs['traf_user'], 'cd %s; tar czf %s %s ./*' % (traf_abspath, TMP_TRAF_PKG_FILE, excludes), logger_output=False)

        info('Copying trafodion files to new nodes, this will take a while ...')
        run_cmd('%s chmod a+r %s' % (get_sudo_prefix(), TMP_TRAF_PKG_FILE))
        run_cmd('%s rm -rf %s{,.pub}' % (get_sudo_prefix(), TMP_SSHKEY_FILE))
        run_cmd('%s cp -rf %s/../.ssh/id_rsa{,.pub} /tmp' % (get_sudo_prefix(), traf_abspath))
        run_cmd('%s chmod 777 %s{,.pub}' % (get_sudo_prefix(), TMP_SSHKEY_FILE))

        hbase_trx_file = cmd_output('ls %s/hbase-trx-*' % cfgs['hbase_lib_path'])
        trafodion_utility_file = cmd_output('ls %s/trafodion-utility-*' % cfgs['hbase_lib_path'])
        esgyn_common_file = cmd_output('ls %s/esgyn-common-*' % cfgs['hbase_lib_path'])
        msenv_file = '%s/ms.env' % cfgs['traf_var']

        files = [EDB_LIC_FILE, TRAF_CFG_FILE, TRAF_CFG_DIR, TMP_TRAF_PKG_FILE, TMP_SSHKEY_FILE, TMP_SSHKEY_FILE+'.pub', \
                 hbase_trx_file, trafodion_utility_file, esgyn_common_file, msenv_file]

        if os.path.exists(EDB_ID_FILE):
            files += [EDB_ID_FILE]

        remote_insts = [Remote(h, pwd=pwd, port=ssh_port) for h in new_nodes]
        threads = [Thread(target=r.copy, args=(files, '/tmp')) for r in remote_insts]
        for thread in threads: thread.start()
        for thread in threads: thread.join()

        for r in remote_insts:
            if r.rc != 0: err_m('Failed to copy files to %s' % r.host)

        # clean up immediately after copy finished
        run_cmd('%s rm -rf %s{,.pub}' % (get_sudo_prefix(), TMP_SSHKEY_FILE))

    format_output('Trafodion Elastic Add Nodes Script')

    ### copy trafodion_config/trafodion-package/hbase-trx to the new nodes
    copy_files()

    ### set parameters ###
    def g(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True)

    if cfgs['enable_ha'].upper() == 'TRUE':
        g('dcs_floating_ip')
        g('dcs_interface')

    if cfgs['secure_hadoop'].upper() == 'Y':
        g('kdc_server')
        g('admin_principal')
        g('kdcadmin_pwd')

    # offline support
    if options.offline:
        cfgs['offline_mode'] = 'Y'
        g('local_repo_dir')
        if not glob('%s/repodata' % cfgs['local_repo_dir']):
            err_m('repodata directory not found, this is not a valid repository directory')
        cfgs['repo_ip'] = socket.gethostbyname(socket.gethostname())
        ports = ParseInI(DEF_PORT_FILE, 'ports').load()
        cfgs['repo_http_port'] = ports['repo_http_port']
        http_start(cfgs['local_repo_dir'], cfgs['repo_http_port'])
    else:
        cfgs['offline_mode'] = 'N'

    format_output('AddNode sub scripts Start')
    ### run addNode script on new nodes ###
    cfgs['node_list'] = ','.join(new_nodes)
    cfgs['traf_package'] = TMP_TRAF_PKG_FILE
    info('Running add node setup on new node(s) [%s] ...' % cfgs['node_list'])
    wrapper.run(cfgs, options, mode='addnodes', pwd=pwd)

    # force gc to clean up RemoteRun instance
    gc.collect()

    ### run client config on all nodes ###
    cfgs['node_list'] = ','.join(all_nodes)
    info('Running dcs server setup on all node(s) [%s] ...' % cfgs['node_list'])
    wrapper.run(cfgs, options, mode='config', param='dcsserver', pwd=pwd)

    ### add nodes ###
    # check nodes which are running trafodion
    mon_up_nodes = cmd_output_as_user(cfgs['traf_user'], 'cstat|grep -vE "grep|mpirun"|grep "monitor COLD"|cut -d" " -f1').replace(':','')
    # instance is up, add nodes in sqshell
    if mon_up_nodes:
        info('Trafodion instance is up, adding node in sqshell ...')

        remote = ''
        mon_up_nodes = mon_up_nodes.split('\n')
        result = cmd_output_as_user(cfgs['traf_user'], 'sqshell -c node info')
        if not 'Up' in result:
            remote = Remote(mon_up_nodes[0], pwd=pwd, port=ssh_port)
            active_dcsmaster = cmd_output_as_user(cfgs['traf_user'], "hbase zkcli ls /%s/%s/dcs/master 2>/dev/null | grep ^\\\[ | cut -d '[' -f 2 | cut -d ':' -f 1"
                                                  % (cfgs['traf_user'], cfgs['traf_instance_id']))
            current_node = cmd_output_as_user(cfgs['traf_user'], 'trafconf -myname')
            dcsmaster_pid = cmd_output_as_user(cfgs['traf_user'], "jps | grep DcsMaster").split()[0]
            if active_dcsmaster != ']':
                if active_dcsmaster == current_node:
                    run_cmd_as_user(cfgs['traf_user'], 'kill %s' % dcsmaster_pid)
            else:
                err_m('No activate dcsmaster')

        # cores=0-1;processors=2;roles=connection,aggregation,storage
        sqconfig_ptr = cmd_output_as_user(cfgs['traf_user'], 'trafconf -node|sed -n 2p|cut -d";" -f3-5')
        for node in new_nodes:
            info('adding node [%s] in sqshell ...' % node)

            # add node to sqshell
            node_add_cmd = 'echo "node add {node-name %s,%s}" | sqshell -a' % (node, sqconfig_ptr)
            node_add_cmd = node_add_cmd.replace(';',',') # semicolon is not available in sqshell
            if remote:
                remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], node_add_cmd))
                result = remote.stdout
            else:
                result = cmd_output_as_user(cfgs['traf_user'], node_add_cmd)
            if 'Invalid' in result:
                err_m('Failed to add node [%s]' %  node)

            # up node in sqshell
            node_up_cmd = 'echo "node up %s" | sqshell -a' % node
            if remote:
                remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], node_up_cmd))
                result = remote.stdout
            else:
                result = cmd_output_as_user(cfgs['traf_user'], node_up_cmd)
            if not 'is UP' in result:
                err_m('Failed to up node [%s]' %  node)

            ok('Node [%s] added!' % node)

        if remote:
            info('Copy sqcert ...')
            remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], 'edb_pdsh -a mkdir -p $HOME/sqcert'))
            remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], 'pdcp -r %s $HOME/sqcert $HOME/' % wnew_nodes))

            info('Copy ms.env ...')
            remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], 'pdcp -r $(trafconf -wname) %s/ms.env %s' % (cfgs['traf_var'], cfgs['traf_var'])))

            info('Starting DCS on new nodes ...')
            remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], 'dcsstart'))

            info('Run sqregen ...')
            remote.execute('%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], 'sqregen -config %s/sqconfig' % TRAF_CFG_DIR))
        else:
            info('Copy sqcert ...')
            run_cmd_as_user(cfgs['traf_user'], 'edb_pdsh -a mkdir -p $HOME/sqcert', logger_output=False)
            run_cmd_as_user(cfgs['traf_user'], 'pdcp -r %s $HOME/sqcert $HOME/' % wnew_nodes, logger_output=False)

            info('Copy ms.env ...')
            run_cmd_as_user(cfgs['traf_user'], 'pdcp -r $(trafconf -wname) %s/ms.env %s' % (cfgs['traf_var'], cfgs['traf_var']), logger_output=False)

            info('Starting DCS on new nodes ...')
            run_cmd_as_user(cfgs['traf_user'], 'dcsstart', logger_output=False)

            info('Run sqregen ...')
            run_cmd_as_user(cfgs['traf_user'], 'sqregen -config %s/sqconfig' % TRAF_CFG_DIR, logger_output=False)
    # instance is not up, add node in sqconfig only
    else:
        info('Trafodion instance is stopped, add node in sqconfig ...')
        wrapper.run(cfgs, options, mode='config', param='sqconfig', pwd=pwd)

        info('Do sqgen ...')
        run_cmd_as_user(cfgs['traf_user'], 'rm %s/sqconfig.db' % cfgs['traf_var'], logger_output=False)
        run_cmd_as_user(cfgs['traf_user'], 'sqgen', logger_output=False)
        ok('Setup completed. You need to start trafodion manually')

    ### clean up ###
    run_cmd('%s rm -rf %s %s' % (get_sudo_prefix(), TMP_TRAF_PKG_FILE, TMP_CFG_DIR), logger_output=False)

    format_output('AddNode Complete')
    warn('You need to manually restart RegionServer on newly added nodes to take effect')
    if options.offline: http_stop()

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        http_stop()
        print '\nAborted...'
