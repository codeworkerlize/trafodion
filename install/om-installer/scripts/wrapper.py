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

import os
import time
import json
import subprocess
import base64
import logging
from glob import glob
from threading import Thread
from constants import *
from common import err_m, cmd_output, time_elapse, set_logger, get_sudo_prefix, Remote, ParseJson, run_cmd2

logger = logging.getLogger()

class RemoteRun(Remote):
    """ run commands or scripts remotely using ssh """

    def __init__(self, host, user='', pwd='', verbose=False, quiet=False):
        self.verbose = verbose
        self.sudo_prefix = ''
        self.host = host
        self.user = user
        self.pwd = pwd
        self.quiet = quiet # no output

        if not self.user:
            self.sudo_prefix = get_sudo_prefix()
        elif self.user != 'root':
            self.sudo_prefix = 'sudo -n'

    def initialize(self):
        super(RemoteRun, self).__init__(self.host, self.user, self.pwd)
        # remove temp folder anyway
        self.execute('%s rm -rf %s' % (self.sudo_prefix, TMP_DIR), verbose=self.verbose, chkerr=False)
        # create tmp folder
        self.execute('mkdir -p %s' % TMP_DIR, verbose=self.verbose)

        # copy all needed files to remote host
        all_files = [CONFIG_DIR, SCRIPTS_DIR, TEMPLATES_DIR]

        self.copy(all_files, remote_folder=TMP_DIR, verbose=self.verbose)

        # set permission
        self.execute('chmod -R a+rx %s' % TMP_DIR, verbose=self.verbose)

    def __del__(self):
        # clean up
        self.execute('%s rm -rf %s' % (self.sudo_prefix, TMP_DIR), verbose=self.verbose, chkerr=False)

    def run_script(self, script, run_user, json_string, param=''):
        """ @param run_user: run the script with this user """

        msg_format = 'Host [%s]: Script [%s]' % (self.host, script)
        logger.info(msg_format + ' start!')

        # serialize the json string to avoid format issue
        encoded_string = base64.b64encode(json_string)
        if run_user:
            # this command only works with shell=True
            script_cmd = '"%s su - %s -c \'%s/scripts/%s %s %s\'"' % (self.sudo_prefix, run_user, TMP_DIR, script, encoded_string, param)
            self.execute(script_cmd, verbose=self.verbose, shell=True, executable='/bin/bash', chkerr=False)
        else:
            script_cmd = '%s %s/scripts/%s \'%s\' \'%s\'' % (self.sudo_prefix, TMP_DIR, script, encoded_string, param)
            self.execute(script_cmd, verbose=self.verbose, chkerr=False)

        # format1 = 'Host [%s]: Script [%s]:\n%s' % (self.host, script, self.stdout)
        # format2 = 'Host [%s]: Script [%s]' % (self.host, script)

        if self.rc == 0:
            if not self.quiet: state_ok(msg_format)
            logger.info(msg_format + ' ran successfully!')
        else:
            if not self.quiet: state_fail(msg_format)
            # only output error messages to screen
            try:
                stdouts = self.stdout.split('\n')
                error_msgs = [stdouts[index:] for index, line in enumerate(stdouts) if 'ERROR' in line][0]
                print '\n' + '\n'.join(error_msgs)
            except IndexError:
                print '\n' + self.stdout
            exit(1)


def state_ok(msg):
    state(32, ' OK ', msg)

def state_fail(msg):
    state(31, 'FAIL', msg)

def state_skip(msg):
    state(33, 'SKIP', msg)

def state(color, result, msg):
    WIDTH = 80
    print '\n\33[%dm%s %s [ %s ]\33[0m\n' % (color, msg, (WIDTH - len(msg))*'.', result)

class Status(object):
    def __init__(self, stat_file, name):
        self.stat_file = stat_file
        self.name = name

    def get_status(self):
        if not os.path.exists(self.stat_file): os.mknod(self.stat_file)
        with open(self.stat_file, 'r') as f:
            st = f.readlines()
        for s in st:
            try:
                if s.split()[0] == self.name: return True
            except IndexError:
                return False
        return False

    def set_status(self):
        # set status only if both scripts are passed
        COPY_FILE_SCRIPT = 'copy_files.py'
        ENV_SETUP_SCRIPT = 'env_setup.py'
        if self.name == COPY_FILE_SCRIPT:
            return
        if self.name == ENV_SETUP_SCRIPT:
            with open(self.stat_file, 'a+') as f:
                f.write('%s OK\n%s OK\n' % (COPY_FILE_SCRIPT, ENV_SETUP_SCRIPT))
        else:
            with open(self.stat_file, 'a+') as f:
                f.write('%s OK\n' % self.name)

@time_elapse
def run(dbcfgs, options, mode='install', param='', user='', pwd='', log_file='', stat_file=''):
    """ main entry
        mode: install/discover
    """
    if not stat_file:
        stat_file = '%s/%s.status' % (INSTALLER_LOC, mode)
    if not log_file:
        log_file = '%s/%s_%s.log' % (DEF_LOG_DIR, mode, time.strftime('%Y%m%d_%H%M%S'))

    set_logger(logger, log_file)

    if mode in ['install', 'preinstall', 'ominstall']:
        print '** Log file location: [%s]' % log_file

    verbose = True if hasattr(options, 'verbose') and options.verbose else False
    reinstall = True if hasattr(options, 'reinstall') and options.reinstall else False
    if not user:
        user = options.user if hasattr(options, 'user') and options.user else ''
    threshold = options.fork if hasattr(options, 'fork') and options.fork else 10

    script_output = []  # script output array
    conf = ParseJson(SCRCFG_FILE).load()
    script_cfgs = conf[mode]

    dbcfgs_json = json.dumps(dbcfgs)
    trafnodes = dbcfgs['node_list'].split(',')
    rsnodes = dbcfgs['rsnodes'].split(',')
    if dbcfgs['ldap_ha_node']:
        mgrnodes = [dbcfgs['management_node'], dbcfgs['ldap_ha_node']]
    else:
        mgrnodes = [dbcfgs['management_node']]

    # handle skipped scripts, skip them if no need to run
    skipped_scripts = []

    if reinstall:
        skipped_scripts += ['hadoop_ops', 'hadoop_mods', 'apache_mods', 'traf_dep', 'traf_kerberos']

    if mode == 'kerberos':
        if dbcfgs['secure_hadoop_kerberos'].upper() == 'N':
            skipped_scripts += ['traf_kerberos']
    else:
        if dbcfgs['secure_hadoop'].upper() == 'N':
            skipped_scripts += ['traf_kerberos']

    if dbcfgs['traf_start'].upper() == 'N':
        skipped_scripts += ['traf_start']

    if 'APACHE' in dbcfgs['distro']:
        skipped_scripts += ['hadoop_mods']
    else:
        skipped_scripts += ['apache_mods']

    def run_local_script(script, json_string, req_pwd):
        cmd = '%s/%s \'%s\'' % (SCRIPTS_DIR, script, base64.b64encode(json_string))
        # pass the ssh password to sub scripts which need SSH password
        if req_pwd: cmd += ' ' + pwd + ' ' + user

        if verbose: print cmd

        MSG_PATTERN = 'Host [localhost]: Script [%s]'
        logger.info(MSG_PATTERN % script + ' start!')
        # stdout on screen
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()

        rc = p.returncode
        if rc != 0:
            msg = 'Failed to run \'%s\'' % script
            if stderr:
                msg += ': ' + stderr
                print stderr
            logger.error(msg)
            state_fail(MSG_PATTERN % script)
            exit(rc)
        else:
            state_ok(MSG_PATTERN % script)
            logger.info(MSG_PATTERN % script + ' ran successfully!')

        return stdout

    # run sub scripts
    try:
        if mode == 'install':
            # 'install' mode needs to be run on all rsnodes
            remote_rs_instances = [RemoteRun(host, user=user, pwd=pwd, verbose=verbose) for host in rsnodes]
            # trafnodes should be part of rsnodes
            remote_traf_instances = [instance for instance in remote_rs_instances if instance.host in trafnodes]
            # do init on all rsnodes
            threads = [Thread(target=r.initialize) for r in remote_rs_instances]
        elif mode == 'preinstall':
            remote_traf_instances = [RemoteRun(host, user=user, pwd=pwd, verbose=verbose) for host in trafnodes]
            # do init in threads to improve performance
            threads = [Thread(target=r.initialize) for r in remote_traf_instances]
            status = Status(stat_file, 'gen_sshkey.py')
            if status.get_status():
                msg = 'Script [gen_sshkey.py] had already been executed'
                state_skip(msg)
                logger.info(msg)
            else:
                print '\nTASK: %s %s' % ('Passwordless SSH Set Up', (83 - len('Passwordless SSH Set Up')) * '*')
                run_local_script('gen_sshkey.py', dbcfgs_json, req_pwd=True)
            status.set_status()
        elif mode == 'cloudera':
            remote_traf_instances = [RemoteRun(host, user=user, pwd=pwd, verbose=verbose) for host in trafnodes]
            # do init in threads to improve performance
            threads = [Thread(target=r.initialize) for r in remote_traf_instances]
        elif mode == 'ominstall':
            remote_traf_instances = [RemoteRun(host, user=user, pwd=pwd, verbose=verbose) for host in mgrnodes]
            # do init in threads to improve performance
            threads = [Thread(target=r.initialize) for r in remote_traf_instances]
        else:
            remote_traf_instances = [RemoteRun(host, user=user, pwd=pwd, verbose=verbose, quiet=True) for host in trafnodes]
            # do init in threads to improve performance
            threads = [Thread(target=r.initialize) for r in remote_traf_instances]

        for t in threads: t.start()
        for t in threads: t.join()

        first_instance = remote_traf_instances[0]

        logger.info(' ***** %s Start *****' % mode)
        for cfg in script_cfgs:
            script = cfg['script']
            node = cfg['node']
            desc = cfg['desc']
            run_user = ''
            if not 'run_as_traf' in cfg.keys():
                pass
            elif cfg['run_as_traf'] == 'yes':
                run_user = dbcfgs['traf_user']

            if not 'req_pwd' in cfg.keys():
                req_pwd = False
            elif cfg['req_pwd'] == 'yes':
                req_pwd = True

            status = Status(stat_file, script)
            if status.get_status():
                msg = 'Script [%s] had already been executed' % script
                state_skip(msg)
                logger.info(msg)
                continue

            if script.split('.')[0] in skipped_scripts:
                continue
            else:
                print '\nTASK: %s %s' % (desc, (83 - len(desc))*'*')

            #TODO: timeout exit
            if node == 'local':
                run_local_script(script, dbcfgs_json, req_pwd)
            elif node == 'first':
                first_instance.run_script(script, run_user, dbcfgs_json, param)
            elif node == 'rsnodes' or node == 'trafnodes' or node == 'mgrnodes':
                if node == 'rsnodes':
                    instances = remote_rs_instances
                elif node == 'trafnodes' or node == 'mgrnodes':
                    instances = remote_traf_instances

                l = len(instances)
                if l > threshold:
                    piece = (l - (l % threshold)) / threshold
                    parted_remote_instances = [instances[threshold*i:threshold*(i+1)] for i in range(piece)]
                    parted_remote_instances.append(instances[threshold*piece:])
                else:
                    parted_remote_instances = [instances]

                for parted_remote_inst in parted_remote_instances:
                    threads = [Thread(target=r.run_script, args=(script, run_user, dbcfgs_json, param)) for r in parted_remote_inst]
                    for t in threads: t.start()
                    for t in threads: t.join()

                    if sum([r.rc for r in parted_remote_inst]) != 0:
                        err_m('Script failed to run on one or more nodes, exiting ...\nCheck log file %s for details.' % log_file)

                    #script_output += [{r.host:r.stdout.strip()} for r in parted_remote_inst]
                    for r in parted_remote_inst:
                        try:
                            dic = json.loads(r.stdout)
                        except ValueError:
                            dic = r.stdout.strip()
                        script_output.append(dic)

            else:
                # should not go to here
                err_m('Invalid configuration for %s' % SCRCFG_FILE)

            status.set_status()

        logger.info(' ***** %s End *****' % mode)
    except KeyboardInterrupt:
        err_m('User quit')

    # remove status file if all scripts run successfully
    os.remove(stat_file)

    # clear logger handlers to avoid duplicate logging handlers in another wrapper.run
    logger.handlers = []

    # remove ^M dos format in log file
    with open(log_file, 'r') as f:
        lines = f.readlines()
    with open(log_file, 'w') as f:
        for line in lines:
            f.write(line.rstrip('\r\n') + '\n')

    return script_output

if __name__ == '__main__':
    exit(0)
