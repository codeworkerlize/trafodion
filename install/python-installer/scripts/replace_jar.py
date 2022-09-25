#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import os
import sys
import json
import glob
import base64
import logging
from Setup import Setup
from collections import defaultdict
from common import err, set_stream_logger, run_cmd, get_sudo_prefix, mod_file, append_file, cmd_output
from constants import *

logger = logging.getLogger()

def run():
    """ replace jar and set kernel setting """
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    sudo_prefix = get_sudo_prefix()
    jars_dir = dbcfgs['parcel_path'] + '/jars/'
    cdh_hbase_sever = jars_dir + HBASE_SERVER % dbcfgs['hbase_version']
    cdh_hbase_client = jars_dir + HBASE_CLIENT % dbcfgs['hbase_version']
    cdh_hbase_common = jars_dir + HBASE_COMMON % dbcfgs['hbase_version']
    cdh_hadoop_hdfs = jars_dir + HADOOP_HDFS % dbcfgs['hdfs_version']
    tmp_hbase_server = glob.glob('/tmp/ha_jars/hbase-server*.jar')[0]
    tmp_hbase_client = glob.glob('/tmp/ha_jars/hbase-client*.jar')[0]
    tmp_hbase_common = glob.glob('/tmp/ha_jars/hbase-common*.jar')[0]
    tmp_hadoop_hdfs = glob.glob('/tmp/ha_jars/hadoop-hdfs*.jar')[0]

    def replace_jar(source_jar, dest_jar):
        if not os.path.exists('%s.source.bak' % dest_jar):
            run_cmd('%s mv %s %s.source.bak' % (sudo_prefix, dest_jar, dest_jar))
        run_cmd('%s cp -f %s %s' % (sudo_prefix, source_jar, dest_jar))

    logger.info('Replace hbase-server.jar, hbase-client.jar, hbase-common.jar and hadoop-hdfs.jar ...')
    replace_jar(tmp_hbase_server, cdh_hbase_sever)
    replace_jar(tmp_hbase_client, cdh_hbase_client)
    replace_jar(tmp_hbase_common, cdh_hbase_common)
    replace_jar(tmp_hadoop_hdfs, cdh_hadoop_hdfs)
    run_cmd('%s chmod 644 %s %s %s %s' % (sudo_prefix, cdh_hbase_sever, cdh_hbase_client, cdh_hbase_common, cdh_hadoop_hdfs))

    logger.info('Clean up temporary hbase-server.jar, hbase-client.jar, hbase-common.jar and hadoop-hdfs.jar ...')
    run_cmd('%s rm -rf /tmp/ha_jars' % sudo_prefix)

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
