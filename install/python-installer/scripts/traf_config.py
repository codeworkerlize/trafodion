#!/usr/bin/env python
### this script should be run on all nodes with trafodion user ###

import sys
import json
import base64
import logging
from collections import defaultdict
from ClientConfig import ClientConfig
from common import err, set_stream_logger

logger = logging.getLogger()
set_stream_logger(logger)


def run(cfg_type='all'):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    logger.info('Deploying all client configs ...')
    client_config = ClientConfig(dbcfgs)

    if cfg_type == 'all':
        client_config.deploy_all()
        if dbcfgs['enhance_ha'] == 'Y':
            client_config.deploy_ha_recovery()
    if cfg_type == 'ldap':
        client_config.deploy_ldap()
        client_config.deploy_dbmgr_admin_info()
        client_config.deploy_auth_type()
    if cfg_type == 'dcsserver':
        client_config.deploy_dcsserver()
    if cfg_type == 'sqconfig':
        client_config.deploy_sqconfig()
    if cfg_type == 'dcsmaster':
        client_config.deploy_dcsmaster()
    if cfg_type == 'dcs_trafconf':
        client_config.deploy_dcsmaster_trafconf()
    if cfg_type == 'dcs_ha':
        client_config.deploy_dbmgr_ha()
        client_config.deploy_ha_trafconf()
        client_config.deploy_dcssite_ha()
    # auto config
    if cfg_type == 'rest':
        client_config.deploy_rest()
        client_config.deploy_dbmgr_https()
    if cfg_type == 'dcs':
        client_config.deploy_dcsmaster()
        client_config.deploy_dcsserver()
        client_config.deploy_dcsmaster_trafconf()
        client_config.deploy_dcscount_trafconf()
        client_config.deploy_dcssite()
        if dbcfgs['dcs_ha'].upper() == 'Y':
            client_config.deploy_dbmgr_ha()
            client_config.deploy_ha_trafconf()
            client_config.deploy_dcssite_ha()

    logger.info('All client configs are deployed successfully')


# main
try:
    dbcfgs_json = sys.argv[1]
except IndexError:
    err('No db config found')
try:
    cfg_type = sys.argv[2]
    if not cfg_type: cfg_type = 'all'
except IndexError:
    cfg_type = 'all'

run(cfg_type)
