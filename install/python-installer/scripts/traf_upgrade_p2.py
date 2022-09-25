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

DEF_TRAF_HOME = '/opt/trafodion/esgyndb'

def run(dbcfgs_json):
    def modify_configs(traf_home, traf_version):
    #def modify_configs(traf_home):
        run_cmd('rm -f %s' % DEF_TRAF_HOME)
        run_cmd('ln -s %s %s' % (traf_home, DEF_TRAF_HOME))

        mod_file(TRAF_CFG_FILE, {'export TRAF_VERSION=.*': 'export TRAF_VERSION=%s' % traf_version,
                                 'export TRAF_ABSPATH=.*': 'export TRAF_ABSPATH=%s' % traf_home})

        # update ms.env
        run_cmd('export TRAF_AGENT=CM; sqgen')

    def stop_instance():
        """ stop instance on single node """
        node = socket.gethostname().lower()

        # stop monitor
        run_cmd('sqshell -c node down %s; exit 0' % node)

        # wait for monitor down
        run_cmd('sqcheckmon -s down -i 40 -d 3; sleep 5')

        # clean up ipcrm
        run_cmd('sqipcrm -local; exit 0')

        # dcs server / mxo servers
        dcs_cnt = cmd_output('grep -n %s $TRAF_CONF/dcs/servers | cut -d":" -f1' % node)
        run_cmd('${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --config ${DCS_CONF_DIR} stop master')
        run_cmd('${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --config ${DCS_CONF_DIR} stop mds %s' % dcs_cnt)
        run_cmd('${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --config ${DCS_CONF_DIR} stop server %s' % dcs_cnt)

        # krb check
        run_cmd('krb5service stop')

        # dbmgr
        run_cmd('$DBMGR_INSTALL_DIR/bin/dbmgr.sh stop')

        # rest
        run_cmd('${REST_INSTALL_DIR}/bin/rest-daemon.sh stop rest')

    new_cfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    new_traf_home = new_cfgs['traf_home']
    new_ver = new_cfgs['traf_version']
    restart = new_cfgs['restart']

    logger.info('Stopping old instance ...')
    stop_instance()

    if not restart:
        logger.info('Create new symbol link for TRAF_HOME and modify configs ...')
        modify_configs(new_traf_home, new_ver)


# main
try:
    dbcfgs_json = sys.argv[1]
except IndexError:
    err('No db config found')

run(dbcfgs_json)
