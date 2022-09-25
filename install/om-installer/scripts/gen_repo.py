#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import sys
import json
import socket
import base64
import logging
from collections import defaultdict
from constants import OM_DIR, OM_PKG_DIRNAME, TMP_REPO_FILE, LOCAL_REPO, GRAFANA_PLUGS, DBMGR_PKGS, REPO_DIR
from common import err, run_cmd, set_stream_logger, Remote, cmd_output, run_cmd3, http_start

logger = logging.getLogger()


def gen_repo(host_ip):
    # install createrepo
    if not cmd_output('rpm -qa | grep createrepo'):
        logger.info('Installing createrepo ...')
        rc, stderr = run_cmd3('yum install -y createrepo')
        if rc:
            err('Failed to install createrepo: %s' % stderr)
    else:
        logger.info('Package createrepo had already been installed')

    # create repo
    run_cmd('cp -r %s /opt/' % OM_DIR)
    run_cmd('createrepo /opt/%s' % OM_PKG_DIRNAME)
    http_start('/opt/%s' % OM_PKG_DIRNAME, '9902')

    repo_url = 'http://%s:9902' % host_ip
    repo_content = LOCAL_REPO % repo_url
    with open(TMP_REPO_FILE, 'w') as f:
        f.write(repo_content)


def run(user, pwd):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    host_name = socket.getfqdn()
    host_ip = socket.gethostbyname(host_name)
    management_node = dbcfgs['management_node']
    ldap_ha = dbcfgs['enable_ldap_ha'].upper()
    ldapha_node = dbcfgs['ldap_ha_node']
    prometheus_tar = dbcfgs['prometheus_tar']
    phpldapadmin_zip = dbcfgs['phpldapadmin_zip']
    node_list = dbcfgs['rsnodes'].split(',')

    gen_repo(host_ip)

    def copy_file(host, files):
        remote = Remote(host, user=user, pwd=pwd)
        remote.copy(files, '/tmp')
        if remote.rc != 0: err('Failed to copy files to %s' % host)

    print 'Setting up operational management repo on node [%s]' % management_node
    files = [TMP_REPO_FILE, DBMGR_PKGS, GRAFANA_PLUGS, prometheus_tar, phpldapadmin_zip]
    copy_file(management_node, files)

    if ldap_ha == 'Y':
        print 'Copy phpLDAPadmin to node [%s]' % ldapha_node
        copy_file(ldapha_node, [phpldapadmin_zip])

    print 'Install python-crontab on node [%s]' % ','.join(node_list)
    files = [TMP_REPO_FILE]
    remotes = [Remote(node, user=user, pwd=pwd) for node in node_list]
    for remote in remotes:
        remote.copy(files, REPO_DIR)
        remote.execute('yum install -y python-crontab && rm -f %s/om_tools.repo' % REPO_DIR)


if __name__ == '__main__':
    set_stream_logger(logger, log_level=logging.ERROR)
    try:
        dbcfgs_json = sys.argv[1]
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

    run(user, pwd)
