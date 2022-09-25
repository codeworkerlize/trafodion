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
import os
import json
import logging
from constants import CUSTOM_CQD_FILE_AP,CUSTOM_CQD_FILE_TP
from common import cmd_output, run_cmd, err, set_stream_logger
from SQLCmd import SQLCmd, SQLException

TIMEOUT = 600
logger = logging.getLogger()

def run(dbcfgs):
    """ start trafodion instance """

    traf_home = os.environ['TRAF_HOME']

    # start OpsMgmt exporters
    logger.info('Starting Operational Management Components: esgyn_exporter, node_exporter and filebeat ...')
    run_cmd('om-client start')

    # Upgrade pre-2.3 meta-data in hbase, if needed
    logger.info('Migrating meta-data, if needed')
    run_cmd('%s/sql/scripts/cleanat_upgrade' % traf_home)

    # kill any left over krb5check - normally no-op
    run_cmd('trafkerberos stop')

    logger.info('Starting trafodion foundation services, timeout [%s]s ...' % TIMEOUT)
    run_cmd('trafstart', timeout=TIMEOUT)

    try:
        sqlcmd = SQLCmd()

        logger.info('Initialize trafodion ...')
        sqlcmd.init_traf()

        logger.info('Initialize trafodion, create xdc metadata ...')
        sqlcmd.create_xdc()

        logger.info('Initialize trafodion, add tenant usage ...')
        sqlcmd.add_tenant()

        logger.info('Initialize trafodion, upgrade library management ...')
        sqlcmd.upgrade_libmgmt()

        logger.info('Initialize trafodion, create lob metadata ...')
        sqlcmd.create_lob_metadata()

        logger.info('Initialize trafodion, create partition tables ...')
        sqlcmd.create_partition_tables()

        logger.info('Create alias name for admin user')
        sqlcmd.init_auth(dbcfgs['db_root_user'], dbcfgs['db_admin_user'])


        if dbcfgs['customized'] == 'CUSTOM_AP':
            if os.path.exists(CUSTOM_CQD_FILE_AP):
                logger.info('Import CQD from default_cqd.sql of AP ...')
                sqlcmd.obey_cqd(CUSTOM_CQD_FILE_AP)
        if dbcfgs['customized'] == 'CUSTOM_TP':
            if os.path.exists(CUSTOM_CQD_FILE_TP):
                logger.info('Import CQD from default_cqd.sql of TP...')
                sqlcmd.obey_cqd(CUSTOM_CQD_FILE_TP)


        logger.info('Initialize authentication ...')
        sqlcmd.check_auth()

        if dbcfgs['ldap_security'].upper() == 'N':
            logger.info('Setup password for admin user')
            sqlcmd.change_admin_pwd(dbcfgs["db_admin_pwd"])

        logger.info('Clean up user query cache ...')
        sqlcmd.cleanup_user_querycache()

    except SQLException as e:
        err(e)

    logger.info('Starting trafodion connectivity/managebility services, timeout [%s]s ...' % TIMEOUT)
    run_cmd('connstart', timeout=TIMEOUT)

    logger.info('Starting to enable binlog.')
    # enable binlog
    if dbcfgs['enable_binlog'].upper() == 'Y':
        run_cmd('echo -e "%s" | %s/sql/scripts/atrxdc -init' % (dbcfgs['rebuild_binlog'], dbcfgs['traf_abspath']))
        # run_cmd_as_user(dbcfgs['traf_user'], '%s/sql/scripts/atrxdc -set mode 1' % dbcfgs['traf_abspath'])
        run_cmd('%s/sql/scripts/atrxdc -set bwsz 4194304' % dbcfgs['traf_abspath'])
    elif dbcfgs['enable_binlog'].upper() == 'N':
        run_cmd('%s/sql/scripts/atrxdc -disable' % dbcfgs['traf_abspath'])

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
