#!/usr/bin/env python
### this script should be run on all nodes with trafodion user ###

import os
import socket
import sys
import json
import base64
import logging
from collections import defaultdict
from common import err, err_m, set_stream_logger, run_cmd, mod_file, append_file, cmd_output
from constants import *

logger = logging.getLogger()
set_stream_logger(logger)


def run(dbcfgs_json):
    # TODO put it into common
    def read_trafcfg():
        # read configs from current trafodion_config and save it to cfgs
        cfgs = defaultdict(str)
        if os.path.exists(TRAF_CFG_FILE):
            with open(TRAF_CFG_FILE, 'r') as f:
                traf_cfgs = f.readlines()
            for traf_cfg in traf_cfgs:
                if not traf_cfg.strip(): continue
                if traf_cfg.startswith('#'): continue
                key, value = traf_cfg.replace('export ', '').split('=')
                value = value.replace('"','')
                value = value.replace('\n','')
                cfgs[key.lower()] = value
        else:
            err_m('Cannot find %s, be sure to run this script on one of trafodion nodes' % TRAF_CFG_FILE)

        return cfgs

    new_cfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    traf_cfgs = read_trafcfg()

    tmp_traf_package_file = '/tmp/' + new_cfgs['traf_package'].split('/')[-1]
    old_traf_home = traf_cfgs['traf_home']
    new_traf_home = new_cfgs['traf_home']

    if old_traf_home == new_traf_home:
        err('Cannot upgrade new package to the same TRAF_HOME as current one')

    # extract new package
    logger.info('Extracting new package to %s ...' % new_traf_home)
    run_cmd('mkdir -p %s' % new_traf_home)
    run_cmd('tar xf %s --wildcards -C %s' % (tmp_traf_package_file, new_traf_home))
    run_cmd('chmod 700 %s' % new_traf_home)


# main
try:
    dbcfgs_json = sys.argv[1]
except IndexError:
    err('No db config found')

run(dbcfgs_json)
