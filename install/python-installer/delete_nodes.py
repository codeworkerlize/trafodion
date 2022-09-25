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
import time
import sys
import logging
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from glob import glob
from collections import defaultdict
from threading import Thread
from scripts import wrapper
from db_install import UserInput
from scripts.constants import TRAF_CFG_FILE, TRAF_CFG_DIR
from scripts.common import get_sudo_prefix, run_cmd_as_user, cmd_output_as_user, info, ok, warn, \
                           format_output, err, err_m, expNumRe, run_cmd, cmd_output, Remote

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion offline delete nodes script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-n", "--nodes", dest="nodes", metavar="NODES",
                      help="Specify the node names you want to delete, separated by comma if more than one, \
                            support numeric Regular Expression, e.g. node[1-3].com")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Verbose mode, will print commands.")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
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

    ssh_port = options.port if hasattr(options, 'port') and options.port else ''

    # get node list from user input
    deleted_nodes = expNumRe(options.nodes.lower())
    if not deleted_nodes:
        err_m('Incorrect format')

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    u = UserInput(options, pwd, cfgs)

    format_output('Trafodion Delete Nodes Script')

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

    traf_abspath = cfgs['traf_abspath']
    ### config check
    if not traf_abspath or not cmd_output('%s ls %s' % (get_sudo_prefix(), traf_abspath)):
        err_m('Cannot find trafodion binary folder')

    ### get trafodion user from traf_home path
    cfgs['traf_user'] = traf_abspath.split('/')[-2]
    if not cfgs['traf_user']:
        err_m('Cannot detect trafodion user')

    ### set parameters
    def g(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True)

    # get current trafodion node list
    current_nodes = cmd_output_as_user(cfgs['traf_user'], 'trafconf -name 2>/dev/null').split()
    remaining_nodes = list(set(current_nodes).difference(set(deleted_nodes)))

    if not (set(current_nodes).intersection(deleted_nodes)):
        err_m('Node(s) %s you are trying to delete are not in current node list %s' % (deleted_nodes, current_nodes))

    if not (set(cfgs['dcs_master_nodes'].split(',')).difference(set(deleted_nodes))):
        delete_dcsmaster_node = cfgs['dcs_master_nodes'].split(',')
        warn("Will delete all Dcs master nodes [%s]" % delete_dcsmaster_node)
        cfgs['master_node'] = remaining_nodes[0]  # default master node
        g('master_node')
        master_nodes = expNumRe(cfgs['master_node'])
        for master in master_nodes:
            if (master not in remaining_nodes) and (master in delete_dcsmaster_node):
                err_m('Master node must be set on current available nodes: %s' % remaining_nodes)
        cfgs['dcs_master_nodes'] = ','.join(master_nodes)
    elif set(cfgs['dcs_master_nodes'].split(',')).difference(set(deleted_nodes)) != set(cfgs['dcs_master_nodes'].split(',')):
        remaining_dcsmaster_nodes = list(set(cfgs['dcs_master_nodes'].split(',')).difference(set(deleted_nodes)))
        delete_dcsmaster_node = list(set(cfgs['dcs_master_nodes'].split(',')).intersection(set(deleted_nodes)))
        cfgs['dcs_master_nodes'] = ','.join(remaining_dcsmaster_nodes)
        warn("Will delete a Dcs master node [%s]" % ','.join(delete_dcsmaster_node))
        g('confirm_del_dcsmaster')
        if cfgs['confirm_del_dcsmaster'].upper() == 'N':
            print '\nAborted...'
            sys.exit(0)
    else:
        delete_dcsmaster_node = []

    cfgs['node_list'] = ','.join(remaining_nodes)

    if cfgs['enable_ha'].upper() == 'TRUE':
        g('dcs_floating_ip')
        cfgs['dcs_ha_keepalived'] = 'false'
        g('dcs_interface')
        g('dcs_ha_keepalive')
        if cfgs['dcs_ha_keepalive'].upper() == 'Y':
          cfgs['dcs_ha_keepalived'] = 'true'
        cfgs['dcs_ha'] = 'Y'
    else:
        cfgs['dcs_ha'] = 'N'

    cfgs['ssh_port'] = ssh_port

    ### delete nodes ###
    # check nodes which are running trafodion
    mon_up_nodes = cmd_output_as_user(cfgs['traf_user'], 'cstat|grep -vE "grep|mpirun"|grep "monitor COLD"')
    # instance is up, delete nodes in sqshell
    if mon_up_nodes:
        info('Trafodion instance is running, please stop it first before deleting nodes.')
        exit(0)
    else:
        info('Trafodion instance is stopped, delete node in sqconfig ...')
        wrapper.run(cfgs, options, mode='config', param='sqconfig', pwd=pwd)

        info('Do sqgen ...')
        sqgen_cmd = 'rm -rf %s/sqconfig.db;export TRAF_AGENT=CM;sqgen' % cfgs['traf_var']
        do_sqgen_cmd = '%s su - %s -c \'%s\'' % (get_sudo_prefix(), cfgs['traf_user'], sqgen_cmd)
        remote_insts = [Remote(h, pwd=pwd, port=ssh_port) for h in remaining_nodes]
        threads = [Thread(target=r.execute, args=(do_sqgen_cmd, )) for r in remote_insts]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for r in remote_insts:
            if r.rc != 0: err('Failed to run sqgen to %s' % r.host)
        ok('Offline delete nodes from sqconfig completed.')

    ### run dcs setup script on all nodes ###
    info('Updating dcs servers on remaining node(s) [%s] ...' % cfgs['node_list'])
    wrapper.run(cfgs, options, mode='config', param='dcsserver', pwd=pwd)

    if cfgs['enable_ha'].upper() == 'TRUE':
        info('Updating dcs HA on remaining node(s) [%s] ...' % cfgs['node_list'])
        wrapper.run(cfgs, options, mode='config', param='dcs_ha', pwd=pwd)

    if delete_dcsmaster_node:
        info('Updating dcs masters on remaining node(s) [%s] ...' % cfgs['node_list'])
        wrapper.run(cfgs, options, mode='config', param='dcsmaster', pwd=pwd)
        info('Updating trafodion config file on remaining node(s) [%s] ...' % cfgs['node_list'])
        wrapper.run(cfgs, options, mode='config', param='dcs_trafconf', pwd=pwd)

    format_output('DeleteNode Complete')

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print '\nAborted...'
