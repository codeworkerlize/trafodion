#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import re
import copy
import socket
import json
import getpass
import logging
import gc
import time
import math
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from collections import defaultdict
from threading import Thread
from scripts import wrapper
from scripts.constants import *
from scripts.common import get_sudo_prefix, Remote, run_cmd, run_cmd_as_user, info, ok, warn, format_output, err_m, \
                           cmd_output_as_user

# TODO put it into common
def read_trafcfg():
    # read configs from current trafodion_config and save it to cfgs
    cfgs = defaultdict(str)
    if os.path.exists(TRAF_CFG_FILE):
        with open(TRAF_CFG_FILE, 'r') as f:
            traf_cfgs = f.readlines()
        for traf_cfg in traf_cfgs:
            if not traf_cfg.strip(): continue
            if traf_cfg.startswith('#'): continue
            key, value = traf_cfg.replace('export ', '').split('=')
            value = value.replace('"', '')
            value = value.replace('\n', '')
            cfgs[key.lower()] = value
    else:
        err_m('Cannot find %s, be sure to run this script on one of trafodion nodes' % TRAF_CFG_FILE)

    return cfgs


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  EsgynDB nodes rolling upgrade main script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-f", "--package", dest="package", metavar="PACKAGE",
                      help="Specify new EsgynDB package file location")
    parser.add_option("-i", "--install", dest="install", action="store_true", default=False,
                      help="Install new EsgynDB version")
    parser.add_option("-n", "--nodes", dest="nodes", metavar="NODES",
                      help="Specify the node names you want to upgrade or restart at a time, \
                            separated by comma if more than one.")
    parser.add_option("-u", "--upgrade", dest="upgrade", action="store_true", default=False,
                      help="Upgrade to new EsgynDB version")
    parser.add_option("-r", "--restart", dest="restart", action="store_true", default=False,
                      help="Restart EsgynDB Cluster")
    parser.add_option("-d", "--dir", dest="dir", metavar="DIR",
                      help="Specify the new EsgynDB directory name")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("--ratio", dest="ratio", type="string",
                      help="EITHER represents a percentage of total number of nodes to restart and upgrade at a time \
                            (if < 1) OR, it is the number of node to restart and upgrade once. Default is 1.")

    (options, args) = parser.parse_args()
    return options


def main():
    """ main loop """
    cfgs = defaultdict(str)

    # handle parser option
    options = get_options()
    if not options.upgrade and not options.install and not options.restart:
        err_m('Must specify --install, --upgrade or --restart')

    if options.install and options.upgrade:
        err_m('Cannot specify both --install and --upgrade')

    if options.install and not options.package and not options.dir:
        err_m('Must specify --package and --dir')

    if options.install and not os.path.exists(options.package):
        err_m('Package file doesn\'t exist')

    if not options.dir and not options.restart:
        err_m('Must specify --dir')

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    trafcfgs = read_trafcfg()

    ### node check
    # should get from sqconfig file
    try:
        sqconfig_outputs = cmd_output_as_user(trafcfgs['traf_user'], 'cat $TRAF_CONF/sqconfig |grep "node-name" 2>/dev/null').split('\n')
        current_nodes = [re.search('node-name=(.*?);', line).groups()[0] for line in sqconfig_outputs]
    except AttributeError:
        err_m('Cannot get EsgynDB nodes from current sqconfig file')

    # should start up trafodion
    mon_up_nodes = cmd_output_as_user(trafcfgs['traf_user'], 'cstat|grep -vE "grep|mpirun"|grep "monitor COLD"|cut -d" " -f1').replace(':','')
    if not mon_up_nodes:
        err_m('The Trafodion environment is down')
    mon_up_nodes = mon_up_nodes.split('\n')

    master_nodes = trafcfgs['dcs_master_nodes'].split(',')
    if len(master_nodes) == 1:
        err_m('DCS Master need to enable HA')
    active_master = cmd_output_as_user(trafcfgs['traf_user'], 'dcscheck -active').split()
    backup_master = list(set(master_nodes).difference(set(active_master)))

    format_output('Rolling upgrade sub script Start')

    if not options.restart:
        home_dir = trafcfgs['traf_home'][:trafcfgs['traf_home'].rfind('/' + trafcfgs['traf_user'])]
        cfgs['traf_home'] = home_dir + '/' + trafcfgs['traf_user'] + '/' + options.dir

    cfgs['traf_user'] = trafcfgs['traf_user']

    if options.install:
        # set log
        log_file = '%s/%s_%s.log' % (DEF_LOG_DIR, "roll_upgrade_install", time.strftime('%Y%m%d_%H%M%S'))

        cfgs['traf_package'] = options.package
        answer = raw_input('Install new esgynDB package on node(s) %s, continue (Y/N) [Y]: ' % current_nodes)
        if not answer: answer = 'Y'
        if answer.lower() != 'y': exit(1)
        info('Copying new EsgynDB packages to all nodes, this will take a while ...')

        files = [options.package]

        remote_insts = [Remote(h, pwd=pwd) for h in current_nodes]
        threads = [Thread(target=r.copy, args=(files, '/tmp')) for r in remote_insts]
        for thread in threads: thread.start()
        for thread in threads: thread.join()

        for r in remote_insts:
            if r.rc != 0: err_m('Failed to copy files to %s' % r.host)

        cfgs['node_list'] = ','.join(current_nodes)
        info('Install new Trafodion on node(s) [%s] ...' % cfgs['node_list'])
        # this sub script will install new esgyndb on remote nodes and copy configs from current one
        wrapper.run(cfgs, options, mode='upgrade_p1', pwd=pwd, log_file=log_file)
        ok('Install new package and copy configurations successfully, you need to run \'upgrade_nodes.py --upgrade\' to do rolling upgrade')
    elif options.upgrade or options.restart:
        cfgs['restart'] = options.restart
        script_type = "restart" if options.restart else "upgrade"
        # set log
        log_file = '%s/%s_%s.log' % (DEF_LOG_DIR, "roll_"+script_type, time.strftime('%Y%m%d_%H%M%S'))

        if options.upgrade:
            traf_version = cmd_output_as_user(trafcfgs['traf_user'], 'cat %s/sqenvcom.sh | grep -E "TRAFODION_VER_MAJOR=|TRAFODION_VER_MINOR=|TRAFODION_VER_UPDATE=" | cut -d= -f2' % cfgs['traf_home'])
            cfgs['traf_version'] = '.'.join(traf_version.split('\n'))

        localhost = socket.gethostname().lower()
        node_list = [socket.gethostbyaddr(node)[0].lower() for node in options.nodes.split(',')] if hasattr(options, 'nodes') and options.nodes else cmd_output_as_user(trafcfgs['traf_user'], 'trafconf -name').split()
        node_count = len(node_list)
        valid_active_master = [node for node in active_master if node in node_list]
        valid_backup_master = [node for node in backup_master if node in node_list]
        server_nodes = [node for node in node_list if node not in valid_active_master and node not in valid_backup_master]

        ratio = float(options.ratio) if hasattr(options, 'ratio') and options.ratio else 1
        if math.fabs(ratio) < 1e-6 or ratio < 0:
            err_m("Ratio needs to greater than zero")

        if math.fabs(ratio - node_count) < 1e-6 or ratio > node_count:
            err_m("Ratio cannot greater than or equal to the total number of nodes")

        if 0 < ratio < 1:
            rnode_count = node_count * ratio
            rnode_count = math.ceil(rnode_count) if rnode_count <= 1 else math.floor(rnode_count)
        else:
            rnode_count = int(ratio)

        # idtmsrv
        # idtm_info = 'nid,pid\nnid,pid\n
        idtm_infos = cmd_output_as_user(trafcfgs['traf_user'], 'pdsh -w %s sqps | grep idtmsrv | cut -d" " -f3' % mon_up_nodes[0]).strip('\n')
        idtm_nid = idtm_pid = ''
        for nid, pid in [idtm_info.split(',') for idtm_info in idtm_infos.split('\n')]:
            idtm_nid = nid.lstrip('0') if nid.lstrip('0') else '0'
            idtm_pid = idtm_pid + ' ' + pid
        idtm_node = cmd_output_as_user(trafcfgs['traf_user'], 'trafconf -node | grep "node-id=%s" | cut -d";" -f2 | cut -d"=" -f2' % idtm_nid)
        idtm_node = idtm_node if idtm_node in node_list else ''

        if idtm_node in valid_active_master or idtm_node in valid_backup_master:
            order_list = copy.deepcopy(node_list)
            order_list.remove(idtm_node)
        else:
            if idtm_node:
                server_nodes.remove(idtm_node)
            order_list = valid_active_master + server_nodes + valid_backup_master
            if rnode_count == (node_count - 1) and valid_active_master and idtm_node:
                rnode_count = node_count - 2

        while True:
            restart_nodes = list()

            if order_list:
                for i in range(int(rnode_count)):
                    try:
                        restart_nodes.append(order_list.pop())
                    except IndexError:
                        continue
            elif idtm_node:
                restart_nodes.append(idtm_node)

            answer = raw_input('The script will %s nodes [%s], continue (Y/N) [N]: ' % (script_type, ', '.join(restart_nodes)))
            if answer.upper() != 'Y':
                warn("Nodes [%s] has not been %s yet. Quit script ..." % (', '.join(set(restart_nodes + order_list + [idtm_node])), script_type))
                exit(1)

            cfgs['node_list'] = ','.join(restart_nodes)

            if options.upgrade:
                info('Upgrade Trafodion on node(s) [%s] ...' % cfgs['node_list'])
            else:
                info('Stop node(s) [%s] ...' % cfgs['node_list'])

            # disable mxosrv on upgrade nodes
            server_index = [cmd_output_as_user(trafcfgs['traf_user'], 'grep -n %s $TRAF_CONF/dcs/servers | cut -d":" -f1' % node) for node in restart_nodes]
            run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs disable n=%s 2>&1' % ','.join(server_index), logger_output=False)

            conn_output = cmd_output_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs getnum n=%s|grep -E "^%s"' % (','.join(server_index), '|'.join(restart_nodes)))
            conn_cnt = 0
            try:
                for node_conn in conn_output.split('\n'):
                    if int(node_conn.split(':')[1]) > 0:
                        conn_cnt += 1
                if conn_cnt > 0:
                    answer = raw_input(
                        'Please migrate the connected mxosrvs to other nodes manually, continue (Y/N) [N]: ')
                    if answer.upper() != 'Y':
                        run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs restore 2>&1')
                        run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs enable n=%s 2>&1' % ','.join(server_index))
                        run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs restart n=%s 2>&1' % ','.join(server_index))
                        exit(0)
            except ValueError:
                answer = raw_input('Please confirm the connected mxosrvs has been migrated, press any key to continue: ')

            # this sub script will stop all esgyndb processes on remote nodes
            if len(restart_nodes) == 1 and idtm_node in restart_nodes:
                remote = Remote(idtm_node, pwd=pwd)
                remote.execute('kill -9 %s' % idtm_pid)
                idtm_node = ''
            # stop trafodion process
            wrapper.run(cfgs, options, mode='upgrade_p2', pwd=pwd, log_file=log_file)

            info('Start node(s) [%s] ...' % cfgs['node_list'])
            # locally run sqshell node up command to start monitor process on remote nodes
            run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs enable n=%s 2>&1' % ','.join(server_index), logger_output=False)

            available_nodes = list(set(mon_up_nodes).difference(set(restart_nodes)))
            if not available_nodes:
                err_m('Can not get available node to execute node up command')

            info('node up [%s] ...' % cfgs['node_list'])
            node_up_cmd = '%s su - %s -c \'%s\'' % (get_sudo_prefix(), trafcfgs['traf_user'], 'sqshell -c node up %s')
            if localhost in restart_nodes:
                remote = Remote(available_nodes[0], pwd=pwd)
                threads = [Thread(target=remote.execute, args=(node_up_cmd % node,)) for node in restart_nodes]
            else:
                threads = [Thread(target=run_cmd, args=(node_up_cmd % node,)) for node in restart_nodes]
            for thread in threads: thread.start()
            for thread in threads: thread.join()

            # force gc to clean up RemoteRun instance
            gc.collect()

            # this sub script will start dcs/rest/mgblty processes on remote nodes
            wrapper.run(cfgs, options, mode='upgrade_p3', pwd=pwd, log_file=log_file)

            if not order_list and not idtm_node:
                run_cmd_as_user(trafcfgs['traf_user'], '${DCS_INSTALL_DIR}/bin/dcs rebalance 2>&1')
                break

            ok('Rolling %s complete on nodes [%s]' % (script_type, ', '.join(restart_nodes)))

        format_output('Rolling %s Complete' % script_type)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print '\nAborted...'
