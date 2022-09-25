#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import getpass
import sys
import random
import string
reload(sys)
sys.setdefaultencoding("utf-8")
from optparse import OptionParser
from collections import defaultdict
from scripts import wrapper
from db_install import UserInput
from scripts.constants import TRAF_CFG_FILE, EDB_LIC_FILE, SECURECFG_TMP_FILE, SECURECFG_FILE
from scripts.SQLCmd import SQLCmd
from scripts.common import get_sudo_prefix, Remote, cmd_output_as_user, run_cmd2, run_cmd_as_user, info, ok, warn, \
                           format_output, err_m, cmd_output, License, ParseInI, show_password_policy, Generate_Password_Policy


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
    parser.add_option("--kerberos", action="store_true", dest="kerberos", default=False,
                      help="Prompt configuration of kerberos.")
    parser.add_option("--ssh-port", dest="port", metavar="PORT",
                      help="Specify ssh port, if not provided, use default port 22.")
    (options, args) = parser.parse_args()
    return options

class TrafMethod(object):
    def __init__(self, cfgs, pwd):
        self.traf_user = cfgs['traf_user']
        self.traf_abspath = cfgs['traf_abspath']
        self.ssh_port = cfgs['ssh_port']
        self.pwd = pwd

    def traf_start(self, logger_output=True):
        info('Starting core DB services, it will take a while ...')
        run_cmd_as_user(self.traf_user, 'source ~/.bashrc; trafstart', logger_output=logger_output)

    def traf_sqstart(self, logger_output=True):
        info('Starting Trafodion, it will take a while ...')
        run_cmd_as_user(self.traf_user, 'source ~/.bashrc; sqstart', logger_output=logger_output)

    def traf_restart(self, logger_output=True):
        info('Restarting Trafodion, it will take a while ...')
        run_cmd_as_user(self.traf_user, 'source ~/.bashrc; sqstop abrupt && sqstart', logger_output=logger_output)

    def conn_start(self, logger_output=True):
        info('Starting connectivity services ...')
        run_cmd_as_user(self.traf_user, 'connstart', logger_output=logger_output)

    def dcs_restart(self, logger_output=True):
        info('Restarting DCS to take effect ...')
        run_cmd_as_user(self.traf_user, "source ~/.bashrc; dcsstop && dcsstart", logger_output=logger_output)

    def dbmgr_restart(self):
        info('Restarting DB Manager to take effect ...')
        dcs_master_nodes = cmd_output_as_user(self.traf_user, 'cat $TRAF_CONF/dcs/masters')
        for dcs_master_node in dcs_master_nodes.split():
            remote = Remote(dcs_master_node, pwd=self.pwd, port=self.ssh_port)
            dbmgr_script_loc = '%s/dbmgr/bin/dbmgr.sh' % self.traf_abspath
            stop_cmd = '%s stop' % dbmgr_script_loc
            start_cmd = '%s start' % dbmgr_script_loc

            remote.execute('%s su - %s -c "%s"' % (get_sudo_prefix(), self.traf_user, stop_cmd))
            remote.execute('%s su - %s -c "%s"' % (get_sudo_prefix(), self.traf_user, start_cmd))

    def is_db_running(self):
        rc = run_cmd2('%s su - %s -c sqcheck' % (get_sudo_prefix(), self.traf_user))
        if rc == 0 or rc == 1:
            info('Trafodion is running or partially running.')
            return True
        else:
            info('Trafodion is not running.')
            return False

def user_input(options, prompt_mode=True, pwd=''):
    """ get user's input and check input value """
    global cfgs

    enable_ldap = True if hasattr(options, 'ldap') and options.ldap else False
    enable_local= True if hasattr(options, 'local') and options.local else False
    enable_kerberos = True if hasattr(options, 'kerberos') and options.kerberos else False
    ssh_port = options.port if hasattr(options, 'port') and options.port else ''

    # load from temp config file if in prompt mode
    if os.path.exists(SECURECFG_TMP_FILE) and prompt_mode == True:
        tp = ParseInI(SECURECFG_TMP_FILE, 'secureconfigs')
        dbcfgs = tp.load()
        cfgs = defaultdict(str, dbcfgs, **cfgs)

    u = UserInput(options, pwd, cfgs)

    def g(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True)

    def o(n): cfgs[n] = u.get_input(n, cfgs[n], prompt_mode=True, optional=True)

    cfgs['ssh_port'] = ssh_port

    # get environment variable on local
    cfgs['trafodion_authentication_type'] = run_cmd_as_user(cfgs['traf_user'], "echo $TRAFODION_AUTHENTICATION_TYPE")
    info("current type for trafodion authentication: " + cfgs['trafodion_authentication_type'])

    if not enable_ldap and not enable_kerberos and not enable_local:
        g('security_selection')

    if enable_ldap or cfgs['security_selection'] == 'ldap':
        # user prompt for ldap
        if cfgs['TRAFODION_AUTHENTICATION_TYPE'].upper() == 'LDAP' and not os.path.exists(SECURECFG_FILE):
            info('LDAP had already been enabled on this cluster')
            sys.exit(0)
        else:
            try:
                lic = License(EDB_LIC_FILE)
                prod_edition = lic.get_edition()
            except StandardError as e:
                log_err(str(e))

            g('ldap_security')
            if cfgs['ldap_security'].upper() == 'Y':
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
                cfgs['TRAFODION_AUTHENTICATION_TYPE'] = "LDAP"
            else:
                sys.exit(0)

    elif enable_local or cfgs['security_selection'] == 'local':
        try:
            lic = License(EDB_LIC_FILE)
            prod_edition = lic.get_edition()
        except StandardError as e:
            log_err(str(e))

        g('local_security')
        if cfgs['local_security'].upper() == 'N':
            sys.exit(0)
        g('local_security_with_password')
        if cfgs['local_security_with_password'].upper() == 'N':
            if cfgs['TRAFODION_AUTHENTICATION_TYPE'].upper() == 'SKIP_AUTHENTICATION' and not os.path.exists(SECURECFG_FILE):
                info('SKIP_AUTHENTICATION had already been enabled on this cluster')
            else:
                ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 5))
                while True:
                    print("\nPlease enter '%s' (without single quotes) to confirm no password login\n" % ran_str)
                    user_input_string = sys.stdin.readline().strip()
                    if user_input_string == ran_str:
                        break
            cfgs['TRAFODION_AUTHENTICATION_TYPE'] = "SKIP_AUTHENTICATION"
        else:
            cfgs['TRAFODION_AUTHENTICATION_TYPE'] = "LOCAL"
        g("use_default_password_policy")
        cfgs['TRAFODION_PASSWORD_CHECK_SYNTAX'] = Generate_Password_Policy(cfgs['use_default_password_policy'])
        cfgs['ldap_security'] = 'N'

    elif enable_kerberos or cfgs['security_selection'] == 'kerberos':
        system_discover = wrapper.run(cfgs, options, mode='discover', param='install', pwd=pwd)

        for result in system_discover:
            if result['hadoop_auth']['value'] == 'kerberos':
                cfgs['secure_hadoop_kerberos'] = 'Y'  # actual kerberos status
            else:
                cfgs['secure_hadoop_kerberos'] = 'N'
            cfgs['hadoop_group_mapping'] = result['hadoop_group_mapping']['value']

        # user prompt for kerberos
        if cfgs['secure_hadoop'] == 'Y' and not os.path.exists(SECURECFG_FILE):  # trafodion kerberos status
            info('Kerberos had already been enabled on this cluster')
            sys.exit(0)
        else:
            if cfgs['secure_hadoop_kerberos'] == 'Y':
                g('kdc_server')
                g('admin_principal')
                g('kdcadmin_pwd')

                # get hadoop users from user input
                g('hdfs_user')
                g('hbase_user')
            else:
                info('Kerberos is not enabled in Hadoop, skipped')

def log_err(errtext):
    # save tmp config files
    tp = ParseInI(SECURECFG_TMP_FILE, 'secureconfigs')
    tp.save(cfgs)
    err_m(errtext)

def main():
    """ main loop """
    global cfgs
    cfgs = defaultdict(str)

    # handle parser option
    options = get_options()

    if options.pwd:
        pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    format_output('Trafodion Security Setup Script')

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
        log_err('Cannot find %s, be sure to run this script on one of trafodion nodes' % TRAF_CFG_FILE)

    traf_abspath = cfgs['traf_abspath']
    ### config check
    if not traf_abspath or not cmd_output('%s ls %s' % (get_sudo_prefix(), traf_abspath)):
        log_err('Cannot find trafodion binary folder')

    if not cfgs['traf_user']: log_err('Cannot detect trafodion user')

    ### get trafodion home directory
    cfgs['home_dir'] = traf_abspath[:traf_abspath.rfind('/'+cfgs['traf_user'])]

    ### discover security related settings
    cfgs['node_list'] = cmd_output('%s su - %s -c "trafconf -name 2>/dev/null"' % (get_sudo_prefix(), cfgs['traf_user'])).replace(' ', ',')

    def set_ldap():
        if cfgs['ldap_security'].upper() == 'Y':
            ### run LDAP setup script on all nodes ###
            info('Running LDAP setup on all node(s) [%s] ...' % cfgs['node_list'])
            wrapper.run(cfgs, options, mode='config', param='ldap', pwd=pwd)

            sqlcmd = SQLCmd(cfgs['traf_user'])
            if traf_method.is_db_running():
                info('Init LDAP authentication')
                traf_method.dcs_restart()
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.dbmgr_restart()
            else:
                traf_method.traf_start()
                info('Init LDAP authentication')
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.conn_start()

    def set_local():
        if cfgs['ldap_security'].upper() == 'N':
            ### run Local setup script on all nodes ###
            info('Enbale Local authentication on all node(s) [%s] ...' % cfgs['node_list'])
            #The operation logic is same as LDAP
            wrapper.run(cfgs, options, mode='config', param='ldap', pwd=pwd)

            sqlcmd = SQLCmd(cfgs['traf_user'])
            if traf_method.is_db_running():
                traf_method.dcs_restart()
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
                traf_method.dbmgr_restart()
            else:
                traf_method.traf_sqstart()
                sqlcmd.init_auth(cfgs['db_root_user'], cfgs['db_admin_user'])
            sqlcmd.check_auth()

    def set_kerberos():
        if cfgs['secure_hadoop_kerberos'] == 'Y':
            ### run kerberos setup script on all nodes ###
            info('Running kerberos setup on all node(s) [%s] ...' % cfgs['node_list'])
            wrapper.run(cfgs, options, mode='kerberos', pwd=pwd)

            if traf_method.is_db_running():
                # g('krb_restart_all')
                # if cfgs['krb_restart_all'].upper() == 'Y':
                #     traf_restart()
                # else:
                #     warn('You must make sure DB is already restarted, otherwise it will not be fully functional')
                warn('You must manually restart EsgynDB, otherwise it will not be fully functional!')

    traf_method = TrafMethod(cfgs, pwd)
    config_file = SECURECFG_FILE

    p = ParseInI(config_file, 'secureconfigs')
    if not os.path.exists(config_file):
        user_input(options, prompt_mode=True, pwd=pwd)

        # save config file as json format
        print '\n** Generating config file [%s] to save configs ... \n' % config_file
        p.save(cfgs)
    # config file exists
    else:
        print '\n** Loading configs from config file [%s] ... \n' % config_file
        cfgs = p.load()
        user_input(options, prompt_mode=False, pwd=pwd)

    if options.kerberos or cfgs['security_selection'] == 'kerberos':
        # set up kerberos
        set_kerberos()
        format_output('Setup kerberos Completed')
    elif options.ldap or cfgs['security_selection'] == 'ldap':
        # set up ldap
        set_ldap()
        format_output('Setup ldap Completed')
    elif options.local or cfgs['security_selection'] == 'local':
        set_local()
        format_output('Setup local Completed')

    # remove config file
    if os.path.exists(config_file): os.remove(config_file)

    # remove temp config file
    if os.path.exists(SECURECFG_TMP_FILE): os.remove(SECURECFG_TMP_FILE)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        tp = ParseInI(SECURECFG_TMP_FILE, 'secureconfigs')
        tp.save(cfgs)
        print '\nAborted...'
