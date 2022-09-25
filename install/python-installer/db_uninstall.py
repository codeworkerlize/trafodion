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

import sys
import os
import re
import getpass
from optparse import OptionParser
from scripts.constants import *
from scripts.common import run_cmd, run_cmd_as_user, cmd_output, format_output, err_m, \
                           expNumRe, ParseInI, Remote, info, get_sudo_prefix


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion uninstall script. It will remove \n\
  trafodion user and home folder.'
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--config-file", dest="cfgfile", metavar="FILE",
                      help="Trafodion config file. If provided, all prompts \
                            will be taken from this file and not prompted for.")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("--silent", action="store_true", dest="silent", default=False,
                      help="Do not ask user to confirm.")
    parser.add_option("-p", "--prompt", action="store_true", dest="prompt", default=False,
                      help="Prompt for user to input the node list.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    parser.add_option("--incomplete-uninstall", action="store_true", dest="incomplete", default=False,
                      help="It will retain trafodion user and the omclient directory.")
    (options, args) = parser.parse_args()
    return options


def get_uninstall_nodes(prompt):
    node_lists = raw_input(prompt)
    if not node_lists: err_m('Empty value')
    return expNumRe(node_lists)


def main():
    """ db_uninstaller main loop """

    # handle parser option
    options = get_options()

    notify = lambda n: raw_input('Uninstall Trafodion on [%s], it will kill all trafodion processes \
and remove all files in trafodion user, do you really want to continue (Y/N) [N]: ' % n)

    format_output('Trafodion Uninstall Start')

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    ssh_port = options.port if hasattr(options, 'port') and options.port else ''
    comp_uninstall = False if hasattr(options, 'incomplete') and options.incomplete else True

    # parse node list from trafodion_config
    traf_home = '/opt/trafodion/esgyndb'
    traf_var = '/var/lib/trafodion'
    traf_log = '/var/log/trafodion'
    if options.prompt:
        node_list = get_uninstall_nodes('Enter Trafodion node list to uninstall(separated by comma): ')
        traf_user = raw_input('Enter Trafodion user name: ')
        if not traf_user: err_m('Empty value')
    elif options.cfgfile:
        # parse node list from installation config file
        if not os.path.exists(options.cfgfile):
            err_m('Cannot find config file \'%s\'' % options.cfgfile)
        config_file = options.cfgfile
        p = ParseInI(config_file, 'dbconfigs')
        cfgs = p.load()
        traf_user = cfgs['traf_user']
        traf_home = '/opt/trafodion/esgyndb'
        traf_var = cfgs['traf_var']
        traf_log = cfgs['traf_log']
        node_list = cfgs['node_list'].split(',')
    else:
        try:
            traf_user = run_cmd_as_user(TRAF_USER, "echo $TRAF_USER")
            traf_home = run_cmd_as_user(TRAF_USER, "echo $TRAF_ABSPATH")
            traf_var = run_cmd_as_user(TRAF_USER, "echo $TRAF_VAR")
            traf_log = run_cmd_as_user(TRAF_USER, "echo $TRAF_LOG")
            node_list = run_cmd_as_user(TRAF_USER, "trafconf -name").split()
        except:
            node_list = get_uninstall_nodes('Enter Trafodion node list to uninstall(separated by comma): ')
            traf_user = raw_input('Enter Trafodion user name: ')
            if not traf_user: err_m('Empty value')

    if not options.silent:
        rc = notify(' '.join(node_list))
        if rc.lower() != 'y': sys.exit(1)

    remotes = [Remote(node, pwd=pwd, port=ssh_port) for node in node_list]
    sudo_prefix = get_sudo_prefix()

    # remove trafodion userid and group on all trafodion nodes, together with folders
    for remote in remotes:
        info('Remove Trafodion on node [%s] ...' % remote.host)
        remote.execute('%s su - %s -c \'%s/sql/scripts/sqipcrm %s\'' % (sudo_prefix, traf_user, traf_home, remote.host), chkerr=False)
        remote.execute('ps -f -u %s|awk \'{print $2}\'|xargs %s kill -9' % (traf_user, sudo_prefix), chkerr=False)
        remote.execute('cgstatus=`/bin/lscgroup | grep ".*cpu.*Esgyn"`; if [[ -n $cgstatus ]]; then %s /bin/cgdelete -r cpu:%s; fi' % (sudo_prefix, EDB_CGROUP_NAME))
        remote.execute('cgstatus=`/bin/lscgroup | grep ".*cpu.*Esgyn"`; if [[ -n $cgstatus ]]; then %s /bin/cgdelete -r cpuacct:%s; fi' % (sudo_prefix, EDB_CGROUP_NAME))
        remote.execute('cgstatus=`/bin/lscgroup | grep "memory.*Esgyn"`; if [[ -n $cgstatus ]]; then %s /bin/cgdelete -r memory:%s; fi' % (sudo_prefix, EDB_CGROUP_NAME))
        remote.execute('trafid=`getent passwd %s|awk -F: \'{print $3}\'`; if [[ -n $trafid ]]; then ps -f -u $trafid|awk \'{print $2}\'|xargs %s kill -9; fi' % (traf_user, sudo_prefix), chkerr=False)
        remote.execute('%s rm -rf %s' % (sudo_prefix, DEF_TRAF_HOME))
        remote.execute('traf_home=`getent passwd %s | cut -d: -f6`; if [[ -n $traf_home ]]; then find $traf_home/* -maxdepth 0 ! -name "omclient" | xargs %s rm -rf; fi' % (traf_user, sudo_prefix))
        if comp_uninstall:
            remote.execute('%s traf_group=`id -ng %s`;%s /usr/sbin/userdel -rf %s;%s /usr/sbin/groupdel $traf_group' % (sudo_prefix, traf_user, sudo_prefix, traf_user, sudo_prefix), chkerr=False)
            remote.execute('%s rm -rf %s' % (sudo_prefix, DEF_TRAF_HOME_DIR))
        remote.execute('%s rm -rf /etc/security/limits.d/%s.conf %s %s %s /tmp/hsperfdata_%s /dev/shm/sem* 2>/dev/null' % (sudo_prefix, traf_user, TRAF_CFG_DIR, EDB_LIC_DIR, traf_var, traf_user), chkerr=False)
        if traf_log: remote.execute('find %s/* -maxdepth 0 ! -name "install" | xargs %s rm -rf' % (traf_log, sudo_prefix))

    run_cmd('rm -f %s/*.status %s' % (INSTALLER_LOC, DBCFG_FILE))
    format_output('Trafodion Uninstall Completed')


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print '\nAborted...'
