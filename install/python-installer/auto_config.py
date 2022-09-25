#!/usr/bin/env python
# -*- coding: utf8 -*-
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import json
import getpass
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from collections import defaultdict
from scripts import wrapper
from db_install import UserInput
from secure_setup import TrafMethod
from scripts.SQLCmd import SQLCmd
from scripts.constants import TRAF_CFG_FILE, EDB_LIC_FILE, TRAF_CFG_DIR
from scripts.common import get_sudo_prefix, run_cmd_as_user, info, ok, warn, format_output, err_m, \
                           cmd_output, License, ParseXML


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Trafodion security setup script.'
    parser = OptionParser(usage=usage)
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("--ldap", action="store_true", dest="ldap", default=False,
                      help="Switch the configuration to LDAP.")
    parser.add_option("--local", action="store_true", dest="local", default=False,
                      help="Switch the configuration to LOCAL.")
    parser.add_option("--dcs", action="store_true", dest="dcs", default=False,
                      help="Modify the configuration of DCS and enable DCS HA.")
    parser.add_option("--rest", action="store_true", dest="rest", default=False,
                      help="Enable SSL for REST and DB Manager.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    (options, args) = parser.parse_args()
    return options

def main():
    """ main loop """
    cfgs = defaultdict(str)

    # handle parser option
    options = get_options()

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    ssh_port = options.port if hasattr(options, 'port') and options.port else ''

    u = UserInput(options, pwd, cfgs)

    format_output('Trafodion Automatic Configuration Tool')

    ### read configs from current trafodion_config and save it to cfgs
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

    traf_abspath = cfgs['traf_abspath']
    ### config check
    if not traf_abspath or not cmd_output('%s ls %s' % (get_sudo_prefix(), traf_abspath)):
        err_m('Cannot find trafodion binary folder')

    if not cfgs['traf_user']: err_m('Cannot detect trafodion user')

    ### get trafodion home directory
    cfgs['home_dir'] = traf_abspath[:traf_abspath.rfind('/'+cfgs['traf_user'])]

    ### discover security related settings
    cfgs['node_list'] = cmd_output('%s su - %s -c "trafconf -name 2>/dev/null"' % (get_sudo_prefix(), cfgs['traf_user'])).replace(' ', ',')

    cfgs['ssh_port'] = ssh_port

    def g(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True)

    def o(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True, optional=True)

    ### rest
    def prompt_rest():
        g('enable_https')
        if cfgs['enable_https'] == 'Y':
            g('keystore_pwd')
            if len(cfgs['keystore_pwd']) < 6:
                err_m('Keystore password must be at least 6 characters')

    def modify_rest():
        if cfgs['enable_https'] == 'Y':
            info('Modifying rest configuration file on all node(s) [%s] ...' % cfgs['node_list'])
            wrapper.run(cfgs, options, mode='config', param='rest', pwd=pwd)

            if traf_method.is_db_running():
                info('Init SSL for DB Manager and REST')
                info('Restarting Rest to take effect ...')
                run_cmd_as_user(cfgs['traf_user'], 'reststop && reststart', logger_output=False)
                traf_method.dbmgr_restart()
            else:
                traf_method.traf_start(logger_output=False)
                info('Init SSL for DB Manager and REST')
                traf_method.conn_start(logger_output=False)

    ### dcs
    def prompt_dcs():
        print '** DcsMaster node list: [%s]' % cfgs['dcs_master_nodes']
        g('dcs_master_nodes')
        g('dcs_cnt_per_node')

        # Use floating IP
        if cfgs['enable_ha'] == 'false':
            g('dcs_ha')
            cfgs['dcs_ha_keepalived'] = 'false'

            if cfgs['dcs_ha'].upper() == 'Y':
                g('dcs_floating_ip')
                g('dcs_interface')
                cfgs['enable_ha'] = 'true'
                g('dcs_ha_keepalive')
                if cfgs['dcs_ha_keepalive'].upper() == 'Y':
                    cfgs['dcs_ha_keepalived'] = 'true'
        elif cfgs['enable_ha'] == 'true':
            info('DCS HA had already been enabled on this cluster')

    def modify_dcs():
        info('Modifying Dcs configuration file on all node(s) [%s] ...' % cfgs['node_list'])
        wrapper.run(cfgs, options, mode='config', param='dcs', pwd=pwd)

        if traf_method.is_db_running():
            info('Init DCS HA')
            traf_method.dcs_restart(logger_output=False)
            traf_method.dbmgr_restart()
        else:
            traf_method.traf_start(logger_output=False)
            info('Init DCS HA')
            traf_method.conn_start(logger_output=False)

    ### ldap
    def prompt_ldap():
        try:
            lic = License(EDB_LIC_FILE)
            prod_edition = lic.get_edition()
        except StandardError as e:
            err_m(str(e))

        g('ldap_modification')
        if cfgs['ldap_modification'].upper() == 'Y':
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
                err_m('Invalid ldap encryption level')

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

    def modify_ldap():
        if cfgs['ldap_modification'].upper() == 'Y':
            ### run LDAP setup script on all nodes ###
            info('Running LDAP modification on all node(s) [%s] ...' % cfgs['node_list'])
            wrapper.run(cfgs, options, mode='config', param='ldap', pwd=pwd)

            sqlcmd = SQLCmd(cfgs['traf_user'])
            if traf_method.is_db_running():
                info('Init LDAP authentication')
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.dcs_restart(logger_output=False)
                traf_method.dbmgr_restart()
            else:
                traf_method.traf_start(logger_output=False)
                info('Init LDAP authentication')
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.conn_start(logger_output=False)

    def disable_ldap():
            ### run LDAP setup script on all nodes ###
            info('Disable LDAP on all node(s) [%s] ...' % cfgs['node_list'])
            cfgs['security_selection'] = 'local'
            cfgs['ldap_security'] = 'N'
            wrapper.run(cfgs, options, mode='config', param='ldap', pwd=pwd)

            if traf_method.is_db_running():
                traf_method.dcs_restart(logger_output=False)
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.dbmgr_restart()
            else:
                traf_method.traf_sqstart()
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])

    ### main
    traf_method = TrafMethod(cfgs, pwd)

    val = 0
    if options.ldap: val += 1
    if options.dcs: val += 1
    if options.rest: val += 1
    if options.local: val += 1

    if val != 1:
        err_m('Must specify only one operation: <ldap,dcs,rest>')

    ldap = True if hasattr(options, 'ldap') and options.ldap else False
    dcs = True if hasattr(options, 'dcs') and options.dcs else False
    rest = True if hasattr(options, 'rest') and options.rest else False
    local = True if hasattr(options, 'local') and options.local else False

    if ldap:
        prompt_ldap()
        modify_ldap()

    if dcs:
        prompt_dcs()
        modify_dcs()

    if rest:
        prompt_rest()
        modify_rest()

    if local:
        disable_ldap()


    format_output('Auto-Config-Tool Complete')


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print '\nAborted...'