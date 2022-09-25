#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import sys
import pty
import json
import socket
import base64
import logging
import subprocess
from collections import defaultdict
from constants import *
from common import info, err, run_cmd, set_stream_logger, get_host_ip, Remote

logger = logging.getLogger()

no_pexpect = 0
try:
    import pexpect
except ImportError:
    no_pexpect = 1

no_sshpass = 0
try:
    p = subprocess.Popen(['sshpass'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.communicate()
except OSError:
    no_sshpass = 1


class PexpectRemote(object):
    def __init__(self, host, user, pwd, port=''):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port

    def copy(self, local_folder, remote_folder='.'):
        scp_cmd = '-P %s' % self.port if self.port else ''
        cmd = 'scp -oStrictHostKeyChecking=no %s -r %s %s@%s:%s' % (scp_cmd, local_folder, self.user, self.host, remote_folder)
        p = pexpect.spawn(cmd, timeout=3)
        try:
            rc = p.expect([pexpect.TIMEOUT, 'password: '])
            has_err = 0
            if rc == 0:
                has_err = 1
            else:
                rc = p.sendline(self.pwd)
                p.expect([pexpect.TIMEOUT, pexpect.EOF])
                if 'Permission denied' in p.before: has_err = 1
                if rc == 0: has_err = 1
        except pexpect.EOF:
            return

        p.close()
        if has_err:
            err('Failed to copy files to host [%s] using pexpect, check your password' % self.host)

    def execute(self, user_cmd):
        ssh_cmd = '-p %s' % self.port if self.port else ''
        cmd = 'ssh -oStrictHostKeyChecking=no -oNoHostAuthenticationForLocalhost=yes %s %s@%s \'%s\'' % (ssh_cmd, self.user, self.host, user_cmd)
        p = pexpect.spawn(cmd, timeout=3)
        try:
            rc = p.expect([pexpect.TIMEOUT, 'password: ', pexpect.EOF])
            has_err = 0
            if rc == 0:
                has_err = 1
            elif rc == 1:
                p.sendline(self.pwd)
                rc = p.expect([pexpect.TIMEOUT, pexpect.EOF])
                if rc == 0: has_err = 1
            if 'Permission denied' in p.before: has_err = 1
        except pexpect.EOF:
            return

        p.close()
        if has_err:
            err('Failed to run command[%s] in host [%s]' % (user_cmd, self.host))


def gen_key_file(logger_output=True):
    run_cmd('mkdir -p %s' % TMP_SSH_DIR, logger_output=logger_output)
    run_cmd('umask 0022; echo -e "y" | ssh-keygen -t rsa -N "" -f %s' % PRIVATE_KEY, logger_output=logger_output)
    run_cmd('cp -f %s %s' % (PUB_KEY, AUTH_KEY), logger_output=logger_output)

    with open(SSH_CFG, 'w') as f:
        f.write(SSH_CFG_ITEMS)
    run_cmd('chmod 600 %s %s; chmod 700 %s' % (SSH_CFG, AUTH_KEY, TMP_SSH_DIR), logger_output=logger_output)

def del_key_file():
    run_cmd('rm -rf %s' % TMP_SSH_DIR)

def run(dbcfgs, user, pwd):
    nodes = dbcfgs["node_list"].split(',')
    ssh_port = dbcfgs['ssh_port']
    local_host = get_host_ip(socket)
    remote_folder = '/root' if user == 'root' else '/home/' + user
    if local_host not in nodes:
        nodes.append(local_host)

    print 'Setting up passwordless SSH across nodes [%s] for user [%s]' % (','.join(nodes), user)
    if no_sshpass and no_pexpect:
        remotes = [Remote(node, user=user, pwd='', port=ssh_port) for node in nodes]
    elif no_sshpass and not no_pexpect:
        remotes = [PexpectRemote(node, user=user, pwd=pwd, port=ssh_port) for node in nodes]
    else:
        remotes = [Remote(node, user=user, pwd=pwd, port=ssh_port) for node in nodes]

    gen_key_file()
    for remote in remotes:
        info('Setting up ssh on host [%s]' % remote.host)
        remote.copy(TMP_SSH_DIR, remote_folder)

    del_key_file()

if __name__ == '__main__':
    try:
        set_stream_logger(logger, log_level=logging.ERROR)
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')
    try:
        pwd = sys.argv[2]
    except IndexError:
        user = pwd = ''

    try:
        user = sys.argv[3]
    except IndexError:
        user = ''

    run(dbcfgs, user, pwd)
