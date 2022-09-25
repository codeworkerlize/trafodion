#!/usr/bin/env python
# -*- coding: utf8 -*-
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import re
import gc
import time
import socket
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from db_install import UserInput
from inspector import overview, pretty_detail_view
from collections import defaultdict
try:
    from prettytable import PrettyTable
except ImportError:
    print 'Python module prettytable is not found. Install python-prettytable first.'
    exit(1)
from scripts import wrapper
from scripts.constants import *
from scripts.common import ParseInI, ParseJson, run_cmd, http_stop, format_output, err_m, expNumRe, cmd_output, get_host_ip

os.environ['LANG'] = 'en_US.UTF8'

STAT_ERR  = ' x '
STAT_WARN = ' w '
STAT_OK   = ' o '
C_STAT_ERR  = '\33[31m x \33[0m'
C_STAT_WARN = '\33[33m w \33[0m'
C_STAT_OK   = '\33[32m o \33[0m'

CAT_CFG = ['hostname', 'fqdn', 'linux_distro', 'firewall_status', 'selinux_status', 'systime', 'timezone', 'transparent_hugepages']
ALL_CATS = {'Configs':CAT_CFG}

# init global cfgs for user input
cfgs = defaultdict(str)


def log_err(errtext):
    # save tmp config files
    tp = ParseInI(PREINSTALL_TMP_FILE, 'preinstallconf')
    tp.save(cfgs)
    err_m(errtext)

def expIpRe(text):
    explist = []
    for regex in text.split(','):
        regex = regex.strip()
        r = re.match(r'(.*)\[(\d+)-(\d+)\]', regex)
        if r:
            h = r.group(1)
            d1 = r.group(2)
            d2 = r.group(3)

            if d1 > d2: d1, d2 = d2, d1
            explist.extend([h + str(c) for c in range(int(d1), int(d2) + 1)])

        else:
            # keep original value if not matched
            explist.append(regex)

    return explist

def user_input(options, prompt_mode=True, pwd=''):
    """ get user's input and check input value """
    global cfgs

    ldap_only = True if hasattr(options, 'ldap') and options.ldap else False

    # load from temp config file if in prompt mode
    if os.path.exists(PREINSTALL_TMP_FILE) and prompt_mode == True:
        tp = ParseInI(PREINSTALL_TMP_FILE, 'preinstallconf')
        cfgs = tp.load()
        if not cfgs:
            # set cfgs to defaultdict again
            cfgs = defaultdict(str)

    u = UserInput(options, pwd, cfgs)
    g = lambda n: u.get_input(n, cfgs[n], prompt_mode=prompt_mode)
    # remote user
    cfgs["ssh_username"] = options.user if options.user else '' if ldap_only else g('ssh_username')
    cfgs["ssh_passwd"] = options.password if options.password else '' if ldap_only else g('ssh_passwd')
    cfgs["ssh_port"] = options.port if options.port else ''

    with open(HOST_FILE, 'r') as f:
        lines = f.readlines()
    g("cdh_nodes")
    if not cfgs["cdh_nodes"]: log_err('Empty value')
    cdh_nodes = expNumRe(cfgs["cdh_nodes"])
    if re.search(r'\d+\.\d+\.\d+\.\d+', cdh_nodes[0]):
        p = re.compile(r"^((?:(2[0-4]\d)|(25[0-5])|([01]?\d\d?))\.){3}(?:(2[0-4]\d)|(255[0-5])|([01]?\d\d?))$")
        try:
            [p.match(h).group() for h in cdh_nodes]
        except:
            err_m('Invalid Ip address')
        cfgs["node_list"] = ','.join(cdh_nodes)
    else:
        cfgs["node_list"] = ','.join([[l.split()[0] for l in lines if h in l][0] for h in cdh_nodes])

    if not ldap_only:
        try:
            cfgs["hosts"] = ','.join([[l for l in lines if h in l][0] for h in cfgs["node_list"].split(',')])
        except IndexError:
            log_err('hosts mismatch, please check the hosts in config.ini are set in /etc/hosts.')

        # local repo url
        g("iso_file")
        if not cfgs['iso_file'].endswith('iso') and not os.path.exists(cfgs['iso_file'] + '/repodata'):
            err_m('Repodata directory not found, this is not a valid repository directory')
        cfgs["iso_repo_ip"] = get_host_ip(socket)

        # ntpd
        g("enable_ntp")
        if cfgs["enable_ntp"].upper() == 'Y':
            CAT_CFG.append('ntp_status')
            g("ntp_server")
            if not cfgs["ntp_gateway"] and prompt_mode == True:
                cfgs["ntp_gateway"] = [i for i in cmd_output("route -n | grep '0.0.0.0' | awk '{print $2}'").split('\n') if i !='0.0.0.0'][0]
            g("ntp_gateway")
            if not cfgs["ntp_gateway"]: log_err('Empty value')

            if not cfgs["ntp_mask"] and prompt_mode == True:
                cfgs["ntp_mask"] = cmd_output("ifconfig | grep %s | awk -F'[mM]ask[ :]' '/[mM]ask[ :]/{print $2}' \
                                              | cut -d' ' -f1" % cfgs["iso_repo_ip"])
            g("ntp_mask")
            if not cfgs["ntp_mask"]: log_err('Empty value')
        # deploy LDAP
        g('deploy_ldap')

    # LDAP
    g('ldap_security')
    if cfgs['ldap_security'].upper() == 'Y':
        print '** Available list of nodes : [%s]' % ','.join(cdh_nodes)
        g('ldap_node')
        if len(cfgs['ldap_node'].split(',')) != 1:
            log_err('Too many input nodes')
        elif sorted(list(set((','.join(cdh_nodes) + ',' + cfgs['ldap_node'].strip()).split(',')))) != sorted(cdh_nodes):
            log_err('Invalid node, please pick up node from available node list')
        remain_nodes = cdh_nodes
        remain_nodes.remove(cfgs['ldap_node'])
        cfgs['ldap_node_ip'] = ','.join([l.split()[0] for l in lines if cfgs['ldap_node'] in l])
        cfgs['ldap_nodes'] = cfgs['ldap_node_ip']

        g('enable_ldap_ha')
        if cfgs['enable_ldap_ha'].upper() == 'Y':
            print '** Available list of nodes : [%s]' % ','.join(remain_nodes)
            g('ldap_ha_node')
            if len(cfgs['ldap_ha_node'].split(',')) != 1:
                log_err('Too many input nodes')
            elif sorted(list(set((','.join(remain_nodes) + ',' + cfgs['ldap_ha_node'].strip()).split(',')))) != sorted(remain_nodes):
                log_err('Invalid node, please pick up node from available node list')
            cfgs['ldap_ha_node_ip'] = ','.join([l.split()[0] for l in lines if cfgs['ldap_ha_node'] in l])
            cfgs['ldap_nodes'] = cfgs['ldap_node_ip'] + ',' + cfgs['ldap_ha_node_ip']

        g('ldap_rootpw')
        g('db_root_pwd')
        g('db_admin_pwd')

    u.notify_user(PREINSTALL_CFG_FILE, PREINSTALL_STATUS_FILE)

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += 'Pre-install script. It will stop the iptables, selinux service, modify the \n\
    /etc/hosts file and start ntp service. Please edit /etc/hosts in this node before run.'
    parser = OptionParser(usage=usage)
    parser.add_option("-l", "--log-file", dest="logfile", metavar="FILE",
                      help="Specify the absolute path name of log file.")
    parser.add_option("-s", "--stat-file", dest="statfile", metavar="FILE",
                      help="Specify the absolute path name of status.")
    parser.add_option("-u", "--remote-user", dest="user", metavar="USER",
                      help="Specify ssh login user for remote server, \
                      if not provided, use current login user as default.")
    parser.add_option("-p", "--password", dest="password", metavar="PASSWD",
                      help="Specify ssh login password for remote server.")
    parser.add_option("-a", "--all", action="store_true", dest="all", default=False,
                      help="Display all scan results.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    parser.add_option("--ldap-only", action="store_true", dest="ldap", default=False,
                      help="Only install openLDAP.")

    (options, args) = parser.parse_args()
    return options

def main():
    """ pre_installer main loop """
    global cfgs

    # handle parser option
    options = get_options()
    format_output('Trafodion Pre-Installation ToolKit')

    config_file = PREINSTALL_CFG_FILE
    # not specified config file and default config file doesn't exist either
    p = ParseInI(config_file, 'preinstallconf')
    if not os.path.exists(config_file):
        user_input(options, prompt_mode=True)
        # save config file as json format
        print '\n** Generating config file [%s] to save configs ... \n' % config_file
        p.save(cfgs)
    # config file exists
    else:
        print '\n** Loading configs from config file [%s] ... \n' % config_file
        cfgs = p.load()
        user_input(options, prompt_mode=False)

    format_output('Pre-Installation Start')
    user = cfgs["ssh_username"]
    passwd = cfgs["ssh_passwd"]

    # force gc to clean up RemoteRun instance
    gc.collect()

    ### perform actual installation ###
    if options.logfile and options.statfile:
        wrapper.run(cfgs, options, mode='preinstall', user=user, pwd=passwd, log_file=options.logfile, stat_file=options.statfile)
    elif options.logfile:
        wrapper.run(cfgs, options, mode='preinstall', user=user, pwd=passwd, log_file=options.logfile)
    elif options.statfile:
        wrapper.run(cfgs, options, mode='preinstall', user=user, pwd=passwd, stat_file=options.statfile)
    else:
        wrapper.run(cfgs, options, mode='preinstall', user=user, pwd=passwd)

    format_output('Pre-Installation Complete')

    if not options.ldap:
        if options.logfile:
            results = wrapper.run(cfgs, options, mode='discover', param='preinstall', user=user, pwd=passwd, log_file=options.logfile)
        else:
            results = wrapper.run(cfgs, options, mode='discover', param='preinstall', user=user, pwd=passwd)

        format_output('Pre-Install results')

        hosts = 'Hosts: ' + ','.join([result['hostname']['value'] for result in results])
        output = hosts + '\n' + overview(results, CAT_CFG) + '\n'
        if options.all: output += pretty_detail_view(results, ALL_CATS)

        with open('%s/preinstall_result' % INSTALLER_LOC, 'w') as f:
            f.write(output)

        output = output.replace(STAT_OK, C_STAT_OK)
        output = output.replace(STAT_WARN, C_STAT_WARN)
        output = output.replace(STAT_ERR, C_STAT_ERR)
        print output

    # rename default config file when successfully installed
    # so next time user can input new variables for a new install
    # or specify the backup config file to install again
    try:
        # only rename default config file
        ts = time.strftime('%y%m%d_%H%M')
        if os.path.exists(config_file):
            os.rename(config_file, config_file + '.bak' + ts)
    except OSError:
        log_err('Cannot rename config file')

    # remove temp config file
    if os.path.exists(PREINSTALL_TMP_FILE): os.remove(PREINSTALL_TMP_FILE)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        tp = ParseInI(PREINSTALL_TMP_FILE, 'preinstallconf')
        tp.save(cfgs)
        http_stop()
        print '\nAborted...'
