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

### this script should be run on local node ###

import sys
import json
import base64
import glob
import logging
from threading import Thread
from gen_sshkey import gen_key_file
from constants import TMP_SSHKEY_FILE, INSTALLER_LOC, TMP_HADOOP_DIR, TMP_SSH_DIR
from common import Remote, run_cmd, err, get_sudo_prefix, set_stream_logger

logger = logging.getLogger()

def run(user, pwd):
    """ gen ssh key on local and copy to all nodes
        copy traf package file from local to all nodes
    """
    def copy_file(hosts, files):
        remote_insts = [Remote(h, user=user, pwd=pwd, port=ssh_port) for h in hosts]
        threads = [Thread(target=r.copy, args=(files, '/tmp')) for r in remote_insts]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for r in remote_insts:
            if r.rc != 0: err('Failed to copy files to %s' % r.host)

    dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    traf_nodes = dbcfgs['node_list'].split(',')
    distro = dbcfgs['distro']
    if dbcfgs['enhance_ha'] == 'Y' or distro == 'CDH_APACHE':
        rsnodes = dbcfgs['cdhnodes'].split(',')
    else:
        rsnodes = dbcfgs['rsnodes'].split(',')

    traf_package = dbcfgs['traf_package']
    ssh_port = dbcfgs['ssh_port']
    sudo_prefix = get_sudo_prefix()

    # untar hbase trx files
    run_cmd('tar xf %s --wildcards -C /tmp export/lib/*.jar' % traf_package, logger_output=False)

    # copy hbase trx files to region server nodes' /tmp folder
    trx_files = glob.glob('/tmp/export/lib/hbase-trx-*')
    # for license check
    trx_files.extend(glob.glob('/tmp/export/lib/esgyn-common-*'))
    print 'Copy hbase-trx files to all RS nodes ...'
    copy_file(rsnodes, trx_files)

    # clean up hbase trx files on local node
    run_cmd('%s rm -rf /tmp/export' % sudo_prefix, logger_output=False)
    # clean up old ssh key files
    run_cmd('%s rm -rf %s*' % (sudo_prefix, TMP_SSHKEY_FILE), logger_output=False)
    # generate new ssh key files
    run_cmd('%s echo -e "y" | ssh-keygen -t rsa -N "" -f %s' % (sudo_prefix, TMP_SSHKEY_FILE), logger_output=False)

    files = [TMP_SSHKEY_FILE, TMP_SSHKEY_FILE+'.pub', traf_package]
    print 'Copy ssh key and DB package to all DB nodes ...'
    copy_file(traf_nodes, files)

    if dbcfgs['enhance_ha'] == 'Y' and distro != 'CDH_APACHE':
        jar_files = [INSTALLER_LOC + '/ha_jars']
        print 'Copy hbase jar and hadoop jar to all nodes ...'
        copy_file(rsnodes, jar_files)

    if dbcfgs['distro'] == 'CDH_APACHE':
        files = [INSTALLER_LOC + '/omclient', TMP_SSHKEY_FILE, TMP_SSHKEY_FILE + '.pub']
        if dbcfgs['install_qian'] == 'N':
            run_cmd('mkdir -p %s' % TMP_HADOOP_DIR, logger_output=False)
            run_cmd('tar xf %s -C %s --strip-components 1' % (dbcfgs['hadoop_tar'], TMP_HADOOP_DIR), logger_output=False)
            files.append(TMP_HADOOP_DIR)
            gen_key_file(logger_output=False)
            files.append(TMP_SSH_DIR)
        print 'Copy Hadoop package and om-client files to all Hadoop nodes ...'
        copy_file(rsnodes, files)
    else:
        files = [INSTALLER_LOC + '/omclient']
        print 'Copy om-client files to all DB nodes ...'
        copy_file(traf_nodes, files)

# main
if __name__ == '__main__':
    set_stream_logger(logger)
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
