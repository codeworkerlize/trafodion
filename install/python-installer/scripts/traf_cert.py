#!/usr/bin/env python
### this script should be run on all nodes with trafodion user ###

import os
import sys
import json
import base64
import logging
from collections import defaultdict
from common import err, run_cmd, set_stream_logger

logger = logging.getLogger()

def run(dbcfgs):

    traf_home = dbcfgs['traf_abspath']
    traf_version = dbcfgs['traf_version']
    traf_var = dbcfgs['traf_var']
    user_home = os.environ['HOME']

    ### run sqgen ###
    logger.info('Run sqgen ...')

    # delete sqconfig.db to avoid sqgen error
    run_cmd('rm -rf %s/sqconfig.db' % traf_var)

    # here sqgen will be run on all nodes, so bypass pdsh cmds in sqgen using TRAF_AGENT=CM
    if len(dbcfgs['node_list'].split(',')) == 1:
        run_cmd('%s/sql/scripts/sqgen' % traf_home)
    else:
        run_cmd('export TRAF_AGENT=CM; %s/sql/scripts/sqgen' % traf_home)

    if dbcfgs['enable_https'].upper() == 'Y':
        ### Generate keystore file for REST and DBMGR ###
        logger.info('Generating SSL keystore...')
        run_cmd('export TRAF_AGENT=CM; %s/sql/scripts/sqcertgen gen_keystore %s' % (traf_home, dbcfgs['keystore_pwd']))

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
