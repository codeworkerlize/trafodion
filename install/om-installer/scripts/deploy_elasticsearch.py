#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import sys
import json
import socket
import base64
import logging
import telnetlib
from collections import defaultdict
from constants import TEMPLATE_XML
from common import run_cmd, err, set_stream_logger, get_host_ip, mod_file, retry, ParseXML, gen_template_file, \
                   append_file, cmd_output

logger = logging.getLogger()
ES_CONFIG_FILE = '/etc/elasticsearch/elasticsearch.yml'
LOGSTASH_CONFIG_DIR = '/etc/logstash'
LOGSTASH_CONFIG_FILE = '%s/traf-logstash.conf' % LOGSTASH_CONFIG_DIR
LOGSTASH_LOGDIR = '/var/log/logstash'

def run(dbcfgs_json):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    logstash_configs = defaultdict(str)
    template = ParseXML(TEMPLATE_XML)
    host_ip = get_host_ip(socket)
    mgr_node = dbcfgs['management_node']

    if mgr_node == host_ip:
        # config elasticsearch
        logger.debug('Modify Elasticsearch config file: [%s]' % ES_CONFIG_FILE)
        mod_file(ES_CONFIG_FILE,
                 {'#network.host: .*': 'network.host: %s' % mgr_node})
        append_file(ES_CONFIG_FILE, 'discovery.type: single-node')

        # start elasticsearch
        logger.info('Starting elasticsearch ...')
        run_cmd('service elasticsearch start && chkconfig elasticsearch on')

        def es_start():
            try:
                telnetlib.Telnet(mgr_node, 9200)
                return True
            except:
                return False
        retry(es_start, 20, 5, 'Elasticsearch')

        # config logstash
        logstash_configs['management_node'] = mgr_node
        logstash_configs['traf_cluster_id'] = dbcfgs['traf_cluster_id']
        logger.debug('Deploy Logstash config file: [%s]' % LOGSTASH_CONFIG_FILE)
        gen_template_file(template.get_property('logstash_config'), LOGSTASH_CONFIG_FILE, logstash_configs)

        # start logstash
        logger.info('Starting Logstash ...')
        run_cmd('nohup /usr/share/logstash/bin/logstash -f %s --path.settings %s >/dev/null 2>%s/logstash-err.log &' % (LOGSTASH_CONFIG_FILE, LOGSTASH_CONFIG_DIR, LOGSTASH_LOGDIR))
        if not cmd_output('ps -aux | grep logstash | grep -v grep'):
            err('Logstash failed to start')


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)
