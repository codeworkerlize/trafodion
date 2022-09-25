#!/usr/bin/env python
### this script should be run on first node with sudo user ###

import os
import sys
import json
import base64
import logging
from constants import DEF_HDFS_BIN, PARCEL_HBASE_LIB, PARCEL_HDFS_BIN
from common import err, run_cmd, run_cmd_as_user, set_stream_logger, cmd_output

logger = logging.getLogger()

def run(dbcfgs):
    distro = dbcfgs['distro']

    hdfs_bin = DEF_HDFS_BIN
    if 'CDH' == distro:
        parcel_lib = PARCEL_HBASE_LIB
        if os.path.exists(parcel_lib): hdfs_bin = PARCEL_HDFS_BIN
    elif 'APACHE' in distro:
        hdfs_bin = dbcfgs['hadoop_home'] + '/bin/hdfs'

    ### set hdfs folder permissions
    logger.info('Setting up hdfs folder permissions for trafodion user ...')
    traf_loc = '/user/trafodion'
    traf_user = dbcfgs['traf_user']
    traf_group = cmd_output('id -ng %s' % traf_user)
    hdfs_user = dbcfgs['hdfs_user']
    hbase_user = dbcfgs['hbase_user']
    hbase_group = run_cmd_as_user(hdfs_user, '%s groups %s | cut -d" " -f3' % (hdfs_bin, hbase_user))

    run_cmd_as_user(hdfs_user, '%s dfsadmin -safemode wait' % hdfs_bin)
    run_cmd_as_user(hdfs_user, '%s dfs -chmod 755 /user' % hdfs_bin)
    run_cmd_as_user(hdfs_user, '%s dfs -mkdir -p %s/{bulkload,lobs,PIT,binlogcache,backups,backupsys} /hbase/archive /user/hbase' % (hdfs_bin, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chown -R %s:%s /hbase/archive' % (hdfs_bin, hbase_user, hbase_user))
    run_cmd_as_user(hdfs_user, '%s dfs -chown %s:%s %s %s/{bulkload,PIT,binlogcache,backups,backupsys}' % (hdfs_bin, traf_user, traf_group, traf_loc, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chown -R %s:%s %s/lobs' % (hdfs_bin, traf_user, traf_group, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chmod 0755 %s' % (hdfs_bin, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chmod 0750 %s/{bulkload,lobs}' % (hdfs_bin, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chmod 0770 %s/{PIT,binlogcache,backups,backupsys}' % (hdfs_bin, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -chgrp %s %s/{PIT,binlogcache,backups,bulkload,backupsys}' % (hdfs_bin, hbase_group, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -setfacl -R -m default:user:%s:rwx "{%s/PIT,%s/binlogcache,/hbase/archive}"' % (hdfs_bin, traf_user, traf_loc, traf_loc))
    run_cmd_as_user(hdfs_user, '%s dfs -setfacl -R -m user:%s:rwx /hbase/archive' % (hdfs_bin, traf_user))
    run_cmd_as_user(hdfs_user, '%s dfs -setfacl -R -m mask::rwx /hbase/archive' % hdfs_bin)
    run_cmd_as_user(hdfs_user, '%s dfs -chown %s:%s /user/hbase' % (hdfs_bin, hbase_user, hbase_user))

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
