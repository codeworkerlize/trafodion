#!/usr/bin/env python
import os
import re
import sys
import json
import socket
import base64
import logging
from collections import defaultdict
from constants import TEMPLATE_XML, TMP_OMCLIENT_DIR, DEF_TRAF_HOME_DIR, DEF_PORT_FILE
from common import err, set_stream_logger, append_file, run_cmd, ParseXML, write_file, cmd_output, mod_file,\
                   ParseInI, gen_template_file, cmd_output_as_user

logger = logging.getLogger()


class DeployOmClient(object):
    def __init__(self, dbcfgs):
        self.dbcfgs = dbcfgs
        self.traf_user = self.dbcfgs['traf_user']
        self.traf_group = cmd_output('id -ng %s' % self.traf_user)

        self.om_client_dir = DEF_TRAF_HOME_DIR + '/omclient'
        self.om_client = self.om_client_dir + '/om-client'
        self.hadoop_manager = self.om_client_dir + '/hadoop-manage.sh'
        self.esgyn_exporter_config = '%s/esgyn_exporter/esgyn_exporter.yaml' % self.om_client_dir
        self.filebeat_config = '%s/filebeat/filebeat.yml' % self.om_client_dir
        self.template = ParseXML(TEMPLATE_XML)

        try:
            self.mgmt_node = re.search(".*://(.*):.*", self.dbcfgs['mgr_url']).groups()[0]
        except AttributeError:
            self.mgmt_node = 'localhost'

        if self.dbcfgs['om_ha_arch'].upper() == 'N':
            self.dbcfgs['es_virtual_ip'] = self.mgmt_node

        ports = ParseInI(DEF_PORT_FILE, 'ports').load()
        if self.dbcfgs['om_ha_arch'].upper() == 'N':
            self.logstash_port = ports['logstash_port']
            self.elasticsearch_port = ports['elasticsearch_port']
        elif self.dbcfgs['om_ha_arch'].upper() == 'Y':
            self.logstash_port = ports['ha_logstash_port']
            self.elasticsearch_port = ports['ha_elasticsearch_port']

    def _pre_set(self):
        run_cmd('mkdir -p %s' % self.om_client_dir)
        cmd_output_as_user(self.traf_user, '%s stop' % self.om_client)
        run_cmd('cp -rf %s/* %s' % (TMP_OMCLIENT_DIR, self.om_client_dir))

        # generate om-client
        write_file(self.om_client, self.template.get_property('om_client_template'))
        # generate clean-hadoop.sh
        write_file(self.hadoop_manager, self.template.get_property('hadoop_manage_template'))
        run_cmd('chmod 750 %s %s' % (self.om_client, self.hadoop_manager))

    def gen_alloc_yaml(self):
        host_roles = defaultdict(list)
        hadoop_list = self.dbcfgs['hadoop_list'].split(',')
        alloc_yaml = '%s/allocate.yaml' % self.om_client_dir
        roles = {
            'NN': self.dbcfgs['nn_host'].split(','),
            'SNN': self.dbcfgs['snn_host'].split(','),
            'DN': self.dbcfgs['dn_hosts'].split(','),
            'JN': self.dbcfgs['jn_hosts'].split(','),
            'RM': self.dbcfgs['rm_host'].split(','),
            'NM': self.dbcfgs['nm_hosts'].split(','),
            'JHS': self.dbcfgs['jhs_host'].split(','),
            'HM': self.dbcfgs['hm_host'].split(','),
            'RS': self.dbcfgs['rs_hosts'].split(','),
            'ZK': self.dbcfgs['zk_hosts'].split(','),
            'QB': self.dbcfgs['node_list'].split(',')
        }

        for node in hadoop_list:
            for role, role_list in roles.items():
                if node in role_list:
                    host_roles[node].append(role)

        content = 'hosts:\n'
        for host, roles in host_roles.items():
            node_ip = socket.gethostbyaddr(host)[2][0]
            content += '- host: %s\n  ip: %s\n  role: %s\n' % (host, node_ip, ','.join(roles))
        write_file(alloc_yaml, content)
        run_cmd('chmod 600 %s/allocate.yaml' % self.om_client_dir)

    def deploy_exporter(self):
        gen_template_file(self.template.get_property('esgyn_exporter_yaml_template'), self.esgyn_exporter_config,
                          {'es_url': 'http://%s:%s' % (self.dbcfgs['es_virtual_ip'], self.elasticsearch_port),
                           'dbm_ip': '' if self.dbcfgs['om_ha_arch'].upper() == 'N' else self.dbcfgs['es_virtual_ip']})
        run_cmd('chmod 640 %s' % self.esgyn_exporter_config)

    def deploy_filebeat(self):
        mod_file(self.filebeat_config, {'"LOGSTASH_HOST:LOGSTASH_PORT"]': '"%s:%s"]' % (self.dbcfgs['es_virtual_ip'], self.logstash_port)})
        append_file(self.filebeat_config, '''  - add_labels:
     labels:
       traf_cluster_id: %s''' % self.dbcfgs['traf_cluster_id'], 'add_cloud_metadata')

    def _setup_permission(self):
        run_cmd('chown -R %s:%s %s' % (self.traf_user, self.traf_group, DEF_TRAF_HOME_DIR))
        run_cmd('chmod 700 %s' % self.om_client_dir)

    @staticmethod
    def _cleanup():
        run_cmd('rm -rf %s' % TMP_OMCLIENT_DIR)

    def run(self):
        self._pre_set()
        if self.dbcfgs['distro'] == 'CDH_APACHE' and self.dbcfgs['install_qian'] == 'N':
            self.gen_alloc_yaml()
        self.deploy_exporter()
        self.deploy_filebeat()
        self._setup_permission()
        self._cleanup()


def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    om_client = DeployOmClient(dbcfgs)
    om_client.run()


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')

    run()
