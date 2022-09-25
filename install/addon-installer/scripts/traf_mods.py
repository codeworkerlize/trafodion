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

import re
import sys
import base64
import json
import logging
from constants import TRAF_CFG_DIR, SYSCTL_CONF_FILE
from common import err, set_stream_logger, ParseXML, cmd_output, append_file, mod_file

logger = logging.getLogger()
TRAF_SITE_INFOS = [('hbase.status.published', 'true'), ('hbase.status.listener.class', 'org.apache.hadoop.hbase.client.ClusterStatusListener$ZKClientListener')]
MS_INFO = """SHARED_CACHE_SEG_SIZE=200
TMCLIENT_RETRY_ATTEMPTS=1
TM_TRANSACTIONAL_TABLE_RETRY=2
TM_JAVA_THREAD_POOL_SIZE=384"""


def run(dbcfgs):
    """Modify configs for trafodion"""
    # trafodion-site.xml
    logger.info('Modify %s/trafodion-site.xml' % TRAF_CFG_DIR)
    traf_site = ParseXML('%s/trafodion-site.xml' % TRAF_CFG_DIR)
    for k, v in TRAF_SITE_INFOS:
        traf_site.add_property(k, v)
    traf_site.write_xml()

    # ms.env
    ms_file = TRAF_CFG_DIR + 'ms.env.custom'
    logger.info('Modify %sms.env.custom' % TRAF_CFG_DIR)
    with open(ms_file, 'r') as f:
        lines = f.read()
    mod_dicts = {}
    for item in MS_INFO.split('\n'):
        item_name = item.split('=')[0] + '.*'
        mod_dicts[item_name] = item
        lines = re.sub(item_name, item, lines)
    mod_file(ms_file, mod_dicts)

    different_string = set(MS_INFO.split('\n')).difference(set([line.replace('\n', '') for line in lines.split('\n')]))
    different_string = '\n'.join(list(different_string))
    append_file(ms_file, different_string)

    # monitor.env
    append_file('%s/monitor.env' % TRAF_CFG_DIR, 'SQ_MON_EPOLL_WAIT_TIMEOUT=2')

    # tmstart
    append_file('%s/sql/scripts/tmstart' % dbcfgs['traf_home'], 'set DTM_TRANS_HUNG_RETRY_INTERVAL=20000', 'set TRAF_TM_LOCKED')


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
