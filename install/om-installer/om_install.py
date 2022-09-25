#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import gc
import glob
import socket
import getpass
import time
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
try:
    from prettytable import PrettyTable
except ImportError:
    print 'Python module prettytable is not found. Install python-prettytable first.'
    exit(1)
from optparse import OptionParser
from collections import defaultdict
from scripts import wrapper
from scripts.constants import *
from scripts.common import HadoopDiscover, ParseInI, ParseJson, run_cmd, http_stop, format_output, \
                           err_m, expNumRe, run_cmd_as_user, cmd_output

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
    tp = ParseInI(OMINSTALL_TMP_FILE, 'dbconfigs')
    tp.save(cfgs)
    err_m(errtext)

def user_input(options, prompt_mode=True, pwd=''):
    """ get user's input and check input value """
    global cfgs

    # load from temp config file if in prompt mode
    if os.path.exists(OMINSTALL_TMP_FILE) and prompt_mode == True:
        tp = ParseInI(OMINSTALL_TMP_FILE, 'dbconfigs')
        cfgs = tp.load()
        if not cfgs:
            # set cfgs to defaultdict again
            cfgs = defaultdict(str)

    u = UserInput(options, pwd, cfgs)
    g = lambda n: u.get_input(n, cfgs[n], prompt_mode=prompt_mode)

    verify_nodes = lambda list1, list2: [n1 for n1 in list1 for n2 in list2 if n2 in n1]

    ### Prometheus
    try:
        rsnodes = run_cmd_as_user(TRAF_USER, 'trafconf -name')
        rsnodes = rsnodes.split()
        cfgs['rsnodes'] = ','.join(rsnodes)
        cfgs['node_list'] = cfgs['rsnodes']
        print '** Trafodion node list: [%s]' % cfgs['rsnodes']
    except:
        g('mgr_url')
        if not ('http:' in cfgs['mgr_url'] or 'https:' in cfgs['mgr_url']):
            cfgs['mgr_url'] = 'http://' + cfgs['mgr_url']

        # set cloudera default port 7180 if not provided by user
        if not re.search(r':\d+', cfgs['mgr_url']):
            cfgs['mgr_url'] += ':7180'

        g('mgr_user')
        g('mgr_pwd')

        hadoop_discover = HadoopDiscover(cfgs['mgr_user'], cfgs['mgr_pwd'], cfgs['mgr_url'])
        cluster_names = hadoop_discover.get_cdh_cluster_names()
        cfgs['cluster_name'] = hadoop_discover.get_def_cluster_name()

        rsnodes = hadoop_discover.get_rsnodes()

        cfgs['rsnodes'] = ','.join(rsnodes)
        cfgs['node_list'] = cfgs['rsnodes']
    #    print '** HBase RegionServer node list: [%s]' % cfgs['rsnodes']

    # g('use_rs_node_prometheus')
    # if cfgs['use_rs_node_prometheus'].upper() == 'Y':
    #     cfgs['node_list'] = cfgs['rsnodes']

    # else:
    #     g('exporter_list')
    #     exporter_lists = expNumRe(cfgs['exporter_list'])
    #
    #     exporter_lists = verify_nodes(rsnodes, exporter_lists)
    #     if not exporter_lists:
    #         log_err('Incorrect node list, should be part of RegionServer nodes')
    #     cfgs['node_list'] = ','.join(exporter_lists)

    # used to configure openTSDB data source
    cfgs['first_node'] = run_cmd("cat /etc/hosts | grep '%s\s\|%s$' | awk '{print $1}'" % (rsnodes[0], rsnodes[0]))

    g('management_node')
    cfgs['management_node'] = run_cmd("cat /etc/hosts | grep '%s\s\|%s$' | awk '{print $1}'" % (cfgs['management_node'], cfgs['management_node']))
    if not cfgs['management_node']: log_err('Incorrect node')

    ### Prometheus
    g('prometheus_data_dir')
    try:
        cfgs['prometheus_tar'] = glob.glob('%s/prometheus*' % OM_DIR)[0]
    except IndexError:
        err_m('No Prometheus package in %s' % OM_DIR)

    ### Off Instance DB Manager
    cfgs['dbmgr_dir'] = DEF_DBMGR_DIR
    try:
        cfgs['traf_cluster_id'] = run_cmd_as_user(TRAF_USER, 'echo $TRAF_INSTANCE_ID')
    except:
        g('traf_cluster_id')

    ### Grafana
    cfgs['grafana_admin_user'] = 'admin'
    cfgs['grafana_admin_pwd'] = 'admin'

    if cmd_output('rpm -qa | grep grafana'):
        if not cfgs['reinstall_grafana'] and not prompt_mode:
            cfgs['reinstall_grafana'] = 'Y'
        else:
            g('reinstall_grafana')

        if cfgs['reinstall_grafana'].upper() == 'N':
            g('grafana_admin_user')
            g('grafana_admin_pwd')
    else:
        cfgs['reinstall_grafana'] = ''

    g('modify_admin_passwd')
    if cfgs['modify_admin_passwd'].upper() == 'Y':
        g('grafana_admin_new_pwd')
        if len(cfgs['grafana_admin_new_pwd']) < 6:
            log_err('Admin password must be at least 6 characters')

    # g('create_grafana_user')
    # if cfgs['create_grafana_user'].upper() == 'Y':
    #     g('grafana_user_name')
    #     g('grafana_user_passwd')
    #     g('grafana_user_email')

    g('alarm_method')
    if cfgs['alarm_method'] == 'smtp':
        g('receive_mailbox')
        g('smtp_email')
        g('smtp_server')
        g('smtp_passwd')

    if not cfgs['mgr_user']: g('mgr_user')
    if not cfgs['mgr_pwd']: g('mgr_pwd')

    ### PHPLdapadmin
    try:
        cfgs['phpldapadmin_zip'] = glob.glob('%s/phpldapadmin*' % OM_DIR)[0]
    except IndexError:
        err_m('No phpLDAPadmin package in %s' % OM_DIR)

    ### LDAP
    g('enable_ldap_ha')
    if cfgs['enable_ldap_ha'].upper() == 'Y':
        if not cfgs['ldap_ha_node'] and prompt_mode:
            cfgs['ldap_ha_node'] = cfgs['node_list'].split(',')[0]
        g('ldap_ha_node')
        cfgs['ldap_ha_node'] = run_cmd("cat /etc/hosts | grep %s | awk '{print $1}'" % cfgs['ldap_ha_node'])
    g('ldap_rootpw')
    g('db_root_pwd')
    g('db_admin_pwd')

    u.notify_user(OMINSTALL_CFG_FILE, OMINSTALL_STATUS_FILE)


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  EsgynDB Operational Management Server Component installation tool.'
    parser = OptionParser(usage=usage)
    parser.add_option("-l", "--log-file", dest="logfile", metavar="FILE",
                      help="Specify the absolute path name of log file.")
    parser.add_option("-s", "--stat-file", dest="statfile", metavar="FILE",
                      help="Specify the absolute path name of status.")
    parser.add_option("--enable-pwd", action="store_true", dest="pwd", default=False,
                      help="Prompt SSH login password for remote hosts. \
                            If set, \'sshpass\' tool is required.")
    parser.add_option("-p", "--password", dest="password", metavar="PASSWD",
                      help="Specify ssh login password for remote server.")
    (options, args) = parser.parse_args()
    return options

def main():
    """ prometheus_install main loop """
    global cfgs

    # handle parser option
    options = get_options()

    format_output('Operational Management Installation ToolKit')

    config_file = OMINSTALL_CFG_FILE

    if options.pwd:
        if options.password:
            pwd = options.password
        else:
            pwd = getpass.getpass('Input remote host SSH Password: ')
    else:
        pwd = ''

    # not specified config file and default config file doesn't exist either
    p = ParseInI(config_file, 'dbconfigs')
    if not os.path.exists(config_file):
        user_input(options, prompt_mode=True, pwd=pwd)

        # save config file as json format
        print '\n** Generating config file [%s] to save configs ... \n' % config_file
        p.save(cfgs)
    # config file exists
    else:
        print '\n** Loading configs from config file [%s] ... \n' % config_file
        cfgs = p.load()
        user_input(options, prompt_mode=False)

    format_output('Installation Start')

    # force gc to clean up RemoteRun instance
    gc.collect()

    ### perform actual installation ###
    if options.logfile and options.statfile:
        wrapper.run(cfgs, options, mode='ominstall', pwd=pwd, log_file=options.logfile, stat_file=options.statfile)
    elif options.logfile:
        wrapper.run(cfgs, options, mode='ominstall', pwd=pwd, log_file=options.logfile)
    elif options.statfile:
        wrapper.run(cfgs, options, mode='ominstall', pwd=pwd, stat_file=options.statfile)
    else:
        wrapper.run(cfgs, options, mode='ominstall', pwd=pwd)

    format_output('Installation Complete')

    http_stop()

    # rename default config file when successfully installed
    # so next time user can input new variables for a new install
    # or specify the backup config file to install again
    try:
        # only rename default config file
        ts = time.strftime('%y%m%d_%H%M')
        if config_file == OMINSTALL_CFG_FILE and os.path.exists(config_file):
            os.rename(config_file, config_file + '.bak' + ts)
    except OSError:
        log_err('Cannot rename config file')

    # remove temp config file
    if os.path.exists(OMINSTALL_TMP_FILE): os.remove(OMINSTALL_TMP_FILE)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        tp = ParseInI(OMINSTALL_TMP_FILE, 'dbconfigs')
        tp.save(cfgs)
        http_stop()
        print '\nAborted...'
