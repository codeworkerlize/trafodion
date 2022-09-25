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

### this script should be run on first node with trafodion user ###

import sys
import base64
import json
import logging
from common import run_cmd_as_user, err, set_stream_logger, HadoopDiscover, retry, run_cmd2, get_sudo_prefix, retry
from hadoop_mods import CDHMod, HDPMod

logger = logging.getLogger()

def run(dbcfgs):
    """ start trafodion, start Cloudera cluster """
    traf_user = dbcfgs['traf_user']

    if 'CDH' in dbcfgs['distro']:
        hadoop_mod = CDHMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'],
                            dbcfgs['mgr_url'], dbcfgs['cluster_name'])
        # restart Hbase cluster
        logger.info('Restarting Hbase services ...')
        hadoop_mod.restart_hbase()
        retry(hadoop_mod.get_status, 60, 10, 'Restart Hbase cluster')
        logger.info('Restart Hbase cluster successfully.')

        # deploy CDH client configs
        logger.info('Deploy CDH client configs ...')
        hadoop_mod.deploy_cfg()
        retry(hadoop_mod.get_status, 60, 10, 'Deploy CDH client configs')
        logger.info('Deploy CDH client configs successfully.')
    elif 'HDP' in dbcfgs['distro']:
        hadoop_mod = HDPMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'],
                            dbcfgs['mgr_url'], dbcfgs['cluster_name'])
        # restart Hbase cluster
        hadoop_mod.restart_hbase()
        retry(hadoop_mod.get_status, 60, 10, 'Run Hbase cluster')

    # stop trafodion
    def is_db_running():
        rc = run_cmd2('%s su - %s -c sqcheck' % (get_sudo_prefix(), traf_user))
        if rc == 0 or rc == 1:
            return True
        else:
            return False
    if is_db_running():
        logger.info('Stop trafodion ...')
        run_cmd_as_user(traf_user, 'sqstop abrupt', logger_output=False)
        logger.info('Stop trafodion successfully.')

    # start trafodion
    logger.info('Start trafodion ...')
    run_cmd_as_user(traf_user, 'sqstart', logger_output=False)
    logger.info('Start trafodion successfully.')

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
