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
from common import run_cmd_as_user, err, set_stream_logger, HadoopDiscover, retry

logger = logging.getLogger()

def run(dbcfgs):
    """ start trafodion, start Cloudera cluster """
    traf_user = dbcfgs['traf_user']
    cms = HadoopDiscover(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])

    # start Cloudera cluster
    logger.info('Start Cloudera cluster ...')
    cms.restart_cluster()
    retry(cms.check_cluster_status, 20, 10, 'Start Cloudera cluster')
    logger.info('Start Cloudera cluster successfully.')

    # deploy CDH client configs
    logger.info('Deploy CDH client configs ...')
    cms.deploy_cfg()
    retry(cms.check_cluster_status, 20, 10, 'Deploy CDH client configs')
    logger.info('Deploy CDH client configs successfully.')

    # start trafodion
    logger.info('Start trafodion ...')
    run_cmd_as_user(traf_user, 'sqstart')
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
