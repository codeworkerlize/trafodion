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
import glob
import base64
import logging
from threading import Thread
from common import Remote, run_cmd, err, get_sudo_prefix, set_stream_logger
from constants import INSTALLER_LOC

logger = logging.getLogger()

def run(user, pwd):
    """ copy  from local to all nodes """
    def copy_file(hosts, files):
        remote_insts = [Remote(h, user=user, pwd=pwd) for h in hosts]
        threads = [Thread(target=r.copy, args=(files, '/tmp')) for r in remote_insts]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for r in remote_insts:
            if r.rc != 0: err('Failed to copy files to %s' % r.host)

    dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    traf_nodes = dbcfgs['node_list'].split(',')
    cluster_nodes = dbcfgs['cdhnodes'].split(',')

    # copy hbase-server.jar and hadoop-hdfs.jar to region server nodes' /tmp folder
    print 'Copy hbase-server.jar and hadoop-hdfs.jar to all cluster nodes ...'
    files = glob.glob('%s/*.jar' % INSTALLER_LOC)
    copy_file(cluster_nodes, files)

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
