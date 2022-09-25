#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import sys
import json
import base64
import logging
from collections import defaultdict
from traf_mods import MS_INFO, TRAF_SITE_INFOS
from common import err, set_stream_logger, run_cmd, get_sudo_prefix, run_cmd_as_user, ParseXML, write_file, cmd_output
from constants import *

logger = logging.getLogger()

def run(dbcfgs_json):
    """Reset configs for trafodion"""
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    # restore trafodion-site.xml
    logger.info('Modify %s/trafodion-site.xml' % TRAF_CFG_DIR)
    traf_site = ParseXML('%s/trafodion-site.xml' % TRAF_CFG_DIR)
    for k, _ in TRAF_SITE_INFOS:
        traf_site.rm_property(k)
    traf_site.write_xml()

    # restore ms.env
    ms_file = TRAF_CFG_DIR + '/ms.env.custom'
    logger.info('Restore %s' % ms_file)
    with open(ms_file, 'r') as f:
        lines = f.read()
    for item in MS_INFO.split('\n'):
        item_name = item.split('=')[0] + '.*\n'
        lines = re.sub(item_name, '', lines)
    write_file(ms_file, lines)

    rm_cmd = 'sed -i \'/%s/d\' %s'
    # restore monitor.env
    run_cmd(rm_cmd % ('SQ_MON_EPOLL_WAIT_TIMEOUT', '%s/monitor.env' % TRAF_CFG_DIR))

    # restore tmstart
    run_cmd(rm_cmd % ('set DTM_TRANS_HUNG_RETRY_INTERVAL', '%s/sql/scripts/tmstart' % dbcfgs['traf_home']))


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)
