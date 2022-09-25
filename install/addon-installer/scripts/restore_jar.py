#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import os
import sys
import json
import glob
import base64
import logging
from collections import defaultdict
from common import err, set_stream_logger, run_cmd, get_sudo_prefix, cmd_output
from constants import *

logger = logging.getLogger()

def run():
    """ replace jar and kernel setting """
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    sudo_prefix = get_sudo_prefix()
    jars_dir = dbcfgs['parcels_dir'] + '/jars/'
    cdh_hbase_sever = jars_dir + HBASE_SERVER % dbcfgs['hbase_ver']
    cdh_hbase_client = jars_dir + HBASE_CLIENT % dbcfgs['hbase_ver']
    cdh_hbase_common = jars_dir + HBASE_COMMON % dbcfgs['hbase_ver']
    cdh_hadoop_hdfs = jars_dir + HADOOP_HDFS % dbcfgs['hdfs_ver']

    def restore_jar(dest_jar):
        if os.path.exists('%s.source.bak' % dest_jar):
            run_cmd('%s rm -f %s' % (sudo_prefix, dest_jar))
            run_cmd('%s mv -f %s.source.bak %s' % (sudo_prefix, dest_jar, dest_jar))

    logger.info('Restore hbase-server.jar, hbase-client.jar, hbase-common.jar and hadoop-hdfs.jar ...')
    restore_jar(cdh_hbase_sever)
    restore_jar(cdh_hbase_client)
    restore_jar(cdh_hbase_common)
    restore_jar(cdh_hadoop_hdfs)
    run_cmd('%s chmod 644 %s %s %s %s' % (sudo_prefix, cdh_hbase_sever, cdh_hbase_client, cdh_hbase_common, cdh_hadoop_hdfs))

    # restore kernel setting
    rm_cmd = 'sed -i \'/%s/d\' %s'
    run_cmd(rm_cmd % ('net.ipv4.tcp_retries2', SYSCTL_CONF_FILE))
    run_cmd(rm_cmd % ('net.ipv4.tcp_syn_retries', SYSCTL_CONF_FILE))
    cmd_output('/sbin/sysctl -e -p %s 2>&1 > /dev/null' % SYSCTL_CONF_FILE)


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
