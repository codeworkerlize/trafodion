#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

## This script should be run on all nodes with sudo user ##

import os
import sys
import json
import base64
import socket
import logging
from collections import defaultdict
from constants import *
from common import run_cmd, cmd_output, err, mod_file, CentralConfig, set_stream_logger, append_file

logger = logging.getLogger()
JAVA_DIR = '/usr/share/java/'

def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    nodes = dbcfgs['node_list'].split(',')
    first_node = nodes[0]
    local_host = socket.getfqdn()

    ### setup cmlocal repo file
    if dbcfgs['repo_url']:
        repo_content = CDH_LOCAL_REPO_PTR % dbcfgs['repo_url']
    elif not dbcfgs["base_repo_url"]:
        repo_content = CDH_LOCAL_REPO_PTR % dbcfgs['cm_repo_url']
    else:
        cm_repo_content = CDH_LOCAL_REPO_PTR % dbcfgs['cm_repo_url']
        base_repo_content = BASE_REPO_PTR % dbcfgs['base_repo_url']
        repo_content = cm_repo_content + base_repo_content
    with open(CDH_REPO_FILE, 'w') as f:
        f.write(repo_content)

    ### remove already installed java1.8
    logger.info('Removing already installed java1.8 ...')
    java_installed = cmd_output('rpm -qa | grep jdk | grep 1.8.0').split()
    for java in java_installed:
        run_cmd('rpm -e %s --nodeps' % java)

    ### install CM packages
    logger.info('Installing CM packages...')
    cm_packages = CentralConfig().get('packages_cm')
    all_pkg_list = cmd_output('rpm -qa')
    install_pkg_list = []
    if first_node in local_host or local_host in first_node:
        # install CM server on first node
        req_pkg_list = cm_packages
    else:
        # install cloudera agents
        req_pkg_list = [cm_packages[i] for i in range(2)]
    for pkg in req_pkg_list:
        if pkg + '-' in all_pkg_list:
            logger.info('Package %s had already been installed' % pkg)
        else:
            install_pkg_list.append(pkg)
    if install_pkg_list:
        run_cmd('yum install -y %s' % ' '.join(install_pkg_list))

    ### clean up local repo file
    os.remove(CDH_REPO_FILE)

    ### deploy java
    logger.info('Setting java environment variable ...')
    java_ver = cmd_output('ls /usr/java | grep jdk1.8.0')
    env_config = '''export JAVA_HOME=/usr/java/%s
export PATH=$JAVA_HOME/bin:$PATH
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
    ''' % java_ver
    append_file('/root/.bash_profile', env_config, 'export PATH', upper=True)

    ### copy mysql jdbc
    jdbc_file_name = dbcfgs['mysql_jdbc']
    if os.path.exists('/tmp/%s' % jdbc_file_name) and (not os.path.exists(JAVA_DIR + 'mysql-connector-java.jar')):
        logger.info('Copying mysql jdbc ...')
        run_cmd('mkdir -p %s' % JAVA_DIR)
        run_cmd('cp %s %s' % ('/tmp/%s' % jdbc_file_name, JAVA_DIR))
        run_cmd('ln -s %s %s' % (JAVA_DIR + jdbc_file_name, JAVA_DIR + 'mysql-connector-java.jar'))

    if first_node in local_host or local_host in first_node:
        ### configuring an external database for CDH
        logger.info('Configuring external database for CDH...')
        init_cmd = '/usr/share/cmf/schema/scm_prepare_database.sh -h "%s" mysql cm cm "%s"' \
                   % (dbcfgs['first_node'], dbcfgs['root_passwd'])
        run_cmd('%s' % init_cmd)

        ### fix permission for parcel repo folder
        run_cmd('chown cloudera-scm:cloudera-scm /opt/cloudera/parcel-repo/')

        logger.info('Stopping Cloudera manager server...')
        run_cmd('service cloudera-scm-server stop')

        logger.info('Starting Cloudera manager server...')
        run_cmd('service cloudera-scm-server start')

    ### modify cloudera agent settings
    mod_file(CDH_AGENT_CFG_FILE, {'server_host=.*':'server_host=%s' % first_node})

    ### start cloudera agent
    logger.info('Starting Cloudera manager agent...')
    run_cmd('service cloudera-scm-agent restart')

    ### delete temporary file
    if local_host in dbcfgs['mysql_hosts']:
        run_cmd('rm -rf /tmp/%s /tmp/%s' % (dbcfgs['mysql_jdbc'], dbcfgs['mysql_dir']))


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
