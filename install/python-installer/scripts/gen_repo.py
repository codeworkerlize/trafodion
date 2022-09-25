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
import glob
import base64
import logging
import subprocess
from collections import defaultdict
from gen_sshkey import PexpectRemote
from common import info, err, run_cmd, set_stream_logger, Remote, append_file, cmd_output, run_cmd3, get_sudo_prefix

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

REPO_DIR = '/etc/yum.repos.d/'
ISO_REPO_FILE = REPO_DIR + 'iso.repo'
TMP_REPO_FILE = '/tmp/iso.repo'
ISO_FSTAB = '%s    /media/cdrom    iso9660    defaults    0 0'
ISO_LOCAL_REPO = """
[iso]
name=iso
baseurl=%s
enable=1
gpgcheck=0
"""

def gen_repo(iso_file):
    run_cmd('mkdir -p /data/old_yum')
    if glob.glob('/etc/yum.repos.d/*.repo'):
        run_cmd('mv /etc/yum.repos.d/*.repo /data/old_yum')

    run_cmd('mkdir -p /media/cdrom')
    run_cmd('chmod 755 /media/cdrom')
    if iso_file.endswith('iso'):
        if iso_file not in cmd_output('mount'):
            run_cmd('mount -o loop %s /media/cdrom' % iso_file)
        append_file('/etc/fstab', ISO_FSTAB % iso_file)
    else:
        run_cmd('\cp -rfp %s/* /media/cdrom/' % iso_file)

    repo_content = ISO_LOCAL_REPO % 'file:///media/cdrom'
    with open(ISO_REPO_FILE, 'w') as f:
        f.write(repo_content)

    # install httpd
    if not cmd_output('rpm -qa | grep httpd-'):
        logger.info('Installing httpd...')
        rc, stderr = run_cmd3('yum install -y httpd')
        if rc:
            err('Failed to install httpd: %s' % stderr)
    else:
        logger.info('Package httpd had already been installed')

    if os.path.exists('/var/www/html/cdrom'):
        run_cmd('%s rm -rf /var/www/html/cdrom' % get_sudo_prefix())
    run_cmd('ln -s /media/cdrom /var/www/html/cdrom')

    run_cmd('systemctl start httpd')
    run_cmd('systemctl enable httpd')

def gen_tmp_repofile(ip):
    iso_url = 'http://%s/cdrom' % ip
    repo_content = ISO_LOCAL_REPO % iso_url
    with open(TMP_REPO_FILE, 'w') as f:
        f.write(repo_content)

def del_repo_file():
    run_cmd('rm -rf %s' % TMP_REPO_FILE)

def run(dbcfgs):
    user = dbcfgs["ssh_username"]
    pwd = dbcfgs["ssh_passwd"]
    iso_file = dbcfgs["iso_file"]
    host_ip = dbcfgs["iso_repo_ip"]
    nodes = dbcfgs["node_list"].split(',')
    ssh_port = dbcfgs['ssh_port']
    subs_manager = "/etc/yum/pluginconf.d/subscription-manager.conf"

    gen_repo(iso_file)
    gen_tmp_repofile(host_ip)

    print 'Setting up linux repo on nodes [%s]' % ','.join(nodes)
    if no_sshpass and no_pexpect:
        remotes = [Remote(node, user=user, pwd='', port=ssh_port) for node in nodes]
    elif not no_pexpect:
        remotes = [PexpectRemote(node, user=user, pwd=pwd, port=ssh_port) for node in nodes]
    else:
        remotes = [Remote(node, user=user, pwd=pwd, port=ssh_port) for node in nodes]

    for remote in remotes:
        info('Setting up linux repo on host [%s]' % remote.host)
        remote.execute('repo_file=`ls %s*.repo 2>/dev/null`; if [[ -n $repo_file ]]; then echo $repo_file | xargs -I {} mv "{}" "{}.bak";fi' % REPO_DIR)
        remote.copy(TMP_REPO_FILE, REPO_DIR)
        # disable subscription-manager
        remote.execute('if [ -f %s ]; then sed -i s/enabled=.*/enabled=0/g %s; fi' % (subs_manager, subs_manager))
        remote.execute('yum clean all')

    del_repo_file()

if __name__ == '__main__':
    try:
        set_stream_logger(logger, log_level=logging.ERROR)
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
