#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import os
import sys
import json
import glob
import base64
import logging
from collections import defaultdict
from common import err, set_stream_logger, run_cmd, get_sudo_prefix, mod_file, append_file, cmd_output, write_file
from constants import *

logger = logging.getLogger()
SYSCTl_ITEMS = {'net.ipv4.tcp_retries2': 'net.ipv4.tcp_retries2=3',
                'net.ipv4.tcp_syn_retries': 'net.ipv4.tcp_syn_retries=1'}

def run():
    """ replace jar and set kernel setting """
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    sudo_prefix = get_sudo_prefix()
    jars_dir = dbcfgs['parcels_dir'] + '/jars/'
    cdh_hbase_sever = jars_dir + HBASE_SERVER % dbcfgs['hbase_ver']
    cdh_hbase_client = jars_dir + HBASE_CLIENT % dbcfgs['hbase_ver']
    cdh_hbase_common = jars_dir + HBASE_COMMON % dbcfgs['hbase_ver']
    cdh_hadoop_hdfs = jars_dir + HADOOP_HDFS % dbcfgs['hdfs_ver']
    tmp_hbase_server = glob.glob('/tmp/hbase-server*.jar')[0]
    tmp_hbase_client = glob.glob('/tmp/hbase-client*.jar')[0]
    tmp_hbase_common = glob.glob('/tmp/hbase-common*.jar')[0]
    tmp_hadoop_hdfs = glob.glob('/tmp/hadoop-hdfs*.jar')[0]

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
    run_cmd('%s rm -f %s %s %s %s' % (sudo_prefix, tmp_hbase_server, tmp_hbase_client, tmp_hbase_common, tmp_hadoop_hdfs))

    # kernel settings
    logger.info('Setting up sysctl configs ...')
    with open(SYSCTL_CONF_FILE, 'r') as f:
        lines = f.read()
    existed_items = [item for item in SYSCTl_ITEMS.keys() if item in lines]
    new_items = set(SYSCTl_ITEMS.keys()).difference(set(existed_items))
    for item in existed_items:
        mod_file(SYSCTL_CONF_FILE, {item + '=.*': SYSCTl_ITEMS[item]})
    for item in new_items:
        append_file(SYSCTL_CONF_FILE, SYSCTl_ITEMS[item])
    cmd_output('/sbin/sysctl -e -p /etc/sysctl.conf 2>&1 > /dev/null')

    # shutdown_HA.service
    logger.info('Creating shutdown_HA.service ...')
    net_interface = cmd_output('/sbin/ip route |grep default|awk \'{print $5}\'')
    shutdown_ha_script = '/var/local/shutdown_HA.sh'
    script_content = '''#!/bin/bash
ifconfig %s down
''' % net_interface
    write_file(shutdown_ha_script, script_content)
    run_cmd('chmod a+x %s' % shutdown_ha_script)

    shutdown_ha_srv = '/usr/lib/systemd/system/shutdown_HA.services'
    srv_content = '''[Unit]
Description=poweroff cust
After=getty@tty1.service display-manager.service plymouth-start.service
Before=systemd-poweroff.service systemd-reboot.service systemd-halt.service
DefaultDependencies=no

[Service]
ExecStart=%s
Type=forking

[Install]
WantedBy=poweroff.target
WantedBy=reboot.target
WantedBy=halt.target
''' % shutdown_ha_script
    write_file(shutdown_ha_srv, srv_content)

    run_cmd('ln -s %s /usr/lib/systemd/system/poweroff.target.wants/' % shutdown_ha_srv)
    run_cmd('ln -s %s /usr/lib/systemd/system/halt.target.wants/' % shutdown_ha_srv)
    run_cmd('ln -s %s /usr/lib/systemd/system/reboot.target.wants/' % shutdown_ha_srv)

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
