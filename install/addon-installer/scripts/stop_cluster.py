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
from common import run_cmd_as_user, run_cmd2, err, set_stream_logger, get_sudo_prefix, HadoopDiscover, retry

logger = logging.getLogger()

def run(dbcfgs):
    """ stop trafodion, stop Cloudera cluster """
    traf_user = dbcfgs['traf_user']
    cms = HadoopDiscover(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])

    # stop trafodion
    def is_db_running():
        rc = run_cmd2('%s su - %s -c sqcheck' % (get_sudo_prefix(), traf_user))
        if rc == 0 or rc == 1:
            return True
        else:
            return False
    if is_db_running():
        logger.info('Stop trafodion ...')
        run_cmd_as_user(traf_user, 'sqstop abrupt')
        logger.info('Stop trafodion successfully.')

    # stop Cloudera cluster
    logger.info('Stop Cloudera cluster ...')
    cms.stop_cluster()
    retry(cms.check_cluster_status, 20, 10, 'Stop Cloudera cluster')
    logger.info('Stop Cloudera cluster successfully.')

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
