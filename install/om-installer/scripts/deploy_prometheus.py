#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import re
import sys
import json
import socket
import base64
import logging
from collections import defaultdict
from constants import PRO_CFG_DIR, CONFIG_DIR, SCRIPTS_DIR
from common import run_cmd, cmd_output, err, get_sudo_prefix, set_stream_logger, write_file, get_host_ip

logger = logging.getLogger()

def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    sudo_prefix = get_sudo_prefix()
    host_ip = get_host_ip(socket)

    prometheus_data_dir = dbcfgs['prometheus_data_dir']
    node_list = dbcfgs['node_list'].split(',')
    prometheus_tar = dbcfgs['prometheus_tar'].split('/')[-1]
    prometheus_dir = re.match(r'(.*)\.tar\.gz', prometheus_tar).group(1)

    if host_ip in dbcfgs['management_node']:
        run_cmd('%s/prometheus-server.sh stop' % SCRIPTS_DIR)

        ### install prometheus
        logger.info('Installing Prometheus ...')
        run_cmd('tar xvf /tmp/%s -C /tmp' % prometheus_tar)
        run_cmd('cp -f /tmp/%s/prometheus /usr/bin' % prometheus_dir)
        run_cmd('%s mkdir -p %s' % (sudo_prefix, PRO_CFG_DIR))

        ### config prometheus
        logger.info('Deploy Prometheus config file ...')
        prometheus_opts = '--config.file=%s/prometheus.yml --storage.tsdb.path=%s' % (PRO_CFG_DIR, prometheus_data_dir)
        write_file('%s/options' % PRO_CFG_DIR, prometheus_opts)

        first_node = [{"targets": [node_list[0] + ':23300']}]
        first_node = json.dumps(first_node)
        write_file('%s/first_node.json' % PRO_CFG_DIR, first_node)

        all_nodes = [{"targets": [node + ':23300' for node in node_list]}]
        all_nodes = json.dumps(all_nodes)
        write_file('%s/all_nodes.json' % PRO_CFG_DIR, all_nodes)

        exporter_nodes = [{"targets": [node + ':23301' for node in node_list]}]
        exporter_nodes = json.dumps(exporter_nodes)
        write_file('%s/exporter_nodes.json' % PRO_CFG_DIR, exporter_nodes)

        mds_nodes = [{"targets": [node + ':8989' for node in node_list]}]
        mds_nodes = json.dumps(mds_nodes)
        write_file('%s/mds_nodes.json' % PRO_CFG_DIR, mds_nodes)

        run_cmd('%s cp %s/prometheus.yml %s' % (sudo_prefix, CONFIG_DIR, PRO_CFG_DIR))

        ### start prometheus
        logger.info('Starting Prometheus server...')
        run_cmd('%s/prometheus-server.sh start' % SCRIPTS_DIR)

        # clean up
        run_cmd('rm -rf /tmp/%s /tmp/%s' % (prometheus_tar, prometheus_dir))

        ### copy om-server
        run_cmd('cp -rf %s/om-server /usr/bin/' % SCRIPTS_DIR)

# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
