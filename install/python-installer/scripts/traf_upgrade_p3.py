#!/usr/bin/env python
### this script should be run on all nodes with trafodion user ###

import os
import socket
import sys
import json
import base64
import logging
from collections import defaultdict
from common import err, err_m, set_stream_logger, run_cmd, mod_file, append_file, cmd_output, retry
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
                value = value.replace('"', '')
                value = value.replace('\n', '')
                cfgs[key.lower()] = value
        else:
            err_m('Cannot find %s, be sure to run this script on one of trafodion nodes' % TRAF_CFG_FILE)

        return cfgs

    def get_my_hostname():
        return socket.gethostname().lower()

    def get_backup_master_nodes():
        active_master = cmd_output('dcscheck -active')
        backup_masters = [node for node in get_master_nodes() if active_master not in node]
        return backup_masters

    def get_master_nodes():
        masters = cmd_output('cat $TRAF_CONF/dcs/masters').split('\n')
        return masters

    def is_bkmaster_node():
        if get_my_hostname() in get_backup_master_nodes():
            return True
        else:
            return False

    def is_master_node():
        if get_my_hostname() in get_master_nodes():
            return True
        else:
            return False

    def check_mxosrv_status():
        dcs_cnt = cmd_output('grep -n %s $TRAF_CONF/dcs/servers | cut -d" " -f2' % get_my_hostname())
        server_index = cmd_output('grep -n %s $TRAF_CONF/dcs/servers | cut -d":" -f1' % get_my_hostname())
        started_mxosrv = cmd_output('${DCS_INSTALL_DIR}/bin/dcs getstartednum n=%s | grep -E "^%s:.+"' % (server_index, server_index))
        started_mxosrv = started_mxosrv.split(':')[1]
        return float(started_mxosrv)/float(dcs_cnt) > 0.8

    def start_instance():
        """ start conn services on single node """

        logger.info('Starting new instance ...')
        dcs_instance = cmd_output('grep -n %s $TRAF_CONF/dcs/servers | cut -d":" -f1' % get_my_hostname())
        dcs_cnt = cmd_output('grep -n %s $TRAF_CONF/dcs/servers | cut -d" " -f2' % get_my_hostname())

        run_cmd('. ${DCS_INSTALL_DIR}/bin/dcs-config.sh')

        # start dcs server
        logger.info('Starting dcs server ...')
        run_cmd('${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --config ${DCS_CONF_DIR} start server %s %s' % (dcs_instance, dcs_cnt))

        # start mds
        logger.info('Starting mds ...')
        run_cmd('${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --config ${DCS_CONF_DIR} start mds %s' % dcs_instance)

        # krb check
        if traf_cfgs['secure_hadoop'].upper() == 'Y':
            logger.info('Starting krb5service ...')
            run_cmd('krb5service start')

        if is_master_node():
            logger.info('Starting DBMgr ...')
            run_cmd('${DBMGR_INSTALL_DIR}/bin/dbmgr.sh start')

        # wait for mxosrv run
        retry(check_mxosrv_status, 40, 5, 'Mxosrv')

    traf_cfgs = read_trafcfg()
    start_instance()


# main
try:
    dbcfgs_json = sys.argv[1]
except IndexError:
    err('No db config found')

run(dbcfgs_json)
