#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

## This script should be run on all nodes with sudo user ##

import sys
import json
import base64
import socket
import logging
from collections import defaultdict
from constants import *
from common import run_cmd, cmd_output, err, set_stream_logger

logger = logging.getLogger()
MYSQL_DIR = '/usr/local/mysql'
MYSQL_DATADIR = '/usr/local/mysql/data'
MY_CNF_FILE = '/etc/my.cnf'

def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    local_host = socket.getfqdn()
    mysql_hosts = dbcfgs['mysql_hosts'].split(',')
    root_passwd = dbcfgs['root_passwd']
    root_exec_cmd = '%s/bin/mysql -uroot -p%s -e' % (MYSQL_DIR, root_passwd)

    def create_cdh_database(user, db):
        logger.info('Creating %s database and user ...' % user)
        database_cmd = 'create database ' + user + ' default character set utf8'
        user_cmd = 'create user "%s"@"%%" identified by "%s"' % (user, root_passwd)
        grant_cmd = 'grant all privileges on %s.* to "%s"@"%%"' % (db, user)
        grant_mysql_cmds = ['grant all on *.* to %s@"%s" identified by "%s"' % (user, host, root_passwd) for host in mysql_hosts]

        cmd_output('%s "%s"' % (root_exec_cmd, database_cmd))
        cmd_output('%s \'%s\'' % (root_exec_cmd, user_cmd))
        run_cmd('%s \'%s\'' % (root_exec_cmd, grant_cmd))
        for grant_mysql_cmd in grant_mysql_cmds:
            run_cmd('%s \'%s\'' % (root_exec_cmd, grant_mysql_cmd))

    if local_host in dbcfgs['first_node'] or dbcfgs['first_node'] in local_host:
        if dbcfgs['distro'] == 'CDH_APACHE':
            create_cdh_database('hive', 'hive')
            create_cdh_database('hive', 'metastore')
        else:
            create_cdh_database('hive', 'hive')
            create_cdh_database('amon', 'amon')
            create_cdh_database('rman', 'rman')
            create_cdh_database('cm', 'cm')

        run_cmd('%s "flush privileges"' % root_exec_cmd)
        logger.info('Created Hive database in mysql successfully!')


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
