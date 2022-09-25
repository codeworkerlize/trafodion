#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import sys
import json
import socket
import base64
import logging
from collections import defaultdict
from constants import TMP_REPO_FILE, REPO_DIR, PRO_REPO_FILE
from common import run_cmd, cmd_output, err, set_stream_logger, get_host_ip

logger = logging.getLogger()

def run(dbcfgs_json):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    host_ip = get_host_ip(socket)
    mgr_node = dbcfgs['management_node']

    logger.info('Installing Operational Management dependencies ...')
    all_pkg_list = cmd_output('rpm -qa')
    install_pkg_list = []

    logger.info('Installing OM dependence ...')
    if mgr_node == host_ip:
        #delete grafana
        if dbcfgs['reinstall_grafana'].upper() == 'Y':
            run_cmd('service grafana-server stop')
            run_cmd('yum remove -y grafana')
            run_cmd('rm -rf /var/lib/grafana /etc/grafana')

        run_cmd('cp %s %s' % (TMP_REPO_FILE, REPO_DIR))
        req_pkg_list = ['grafana.x86_64', 'openldap-2.4.44', 'openldap-servers', 'openldap-clients', 'libzip', \
                        'httpd', 'elasticsearch', 'logstash', 'numpy', 'php', 'php-cli', 'php-common', 'php-ldap', 'unzip']
    else:
        req_pkg_list = ['openldap-2.4.44', 'openldap-servers', 'openldap-clients', 'httpd', 'libzip', 'php', 'php-cli',\
                        'php-common', 'php-ldap', 'unzip']

    for pkg in req_pkg_list:
        if pkg + '-' in all_pkg_list:
            logger.info('Package %s had already been installed' % pkg)
        else:
            install_pkg_list.append(pkg)

    if install_pkg_list:
        run_cmd('yum install -y %s' % ' '.join(install_pkg_list))

    # clean up
    run_cmd('rm -f %s %s' % (TMP_REPO_FILE, PRO_REPO_FILE))


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)
