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

import time
import sys
import json
import base64
import copy
import logging
import subprocess
from constants import MODCFG_FILE, DEF_MODCFG_FILE
from common import HadoopDiscover, ParseXML, ParseHttp, ParseJson, err, info, set_stream_logger, get_sudo_prefix

logger = logging.getLogger()

CLUSTER_URL_PTR = '%s/api/v1/clusters/%s'
RESTART_URL_PTR = CLUSTER_URL_PTR + '/commands/restart'
RESTART_SRV_URL_PTR = CLUSTER_URL_PTR + '/services/%s/commands/restart'
SRVCFG_URL_PTR = CLUSTER_URL_PTR + '/services/%s/config'
RCGRP_URL_PTR = '%s/api/v6/clusters/%s/services/%s/roleConfigGroups'

class CDHMod(object):
    """ Modify CDH configs for trafodion """
    def __init__(self, user, passwd, url, cluster_name):  # cluster_name contains space
        self.url = url
        self.cluster_name = cluster_name.replace(' ', '%20')
        self.p = ParseHttp(user, passwd)

        hadoop_discover = HadoopDiscover(user, passwd, url, cluster_name)
        self.hdfs_service = hadoop_discover.get_hdfs_srvname()
        self.hbase_service = hadoop_discover.get_hbase_srvname()
        self.zk_service = hadoop_discover.get_zookeeper_srvname()

    def _get_cfgs(self, mod_cfgs):
        self.hbase_cfg = mod_cfgs['HBASE_CONFIG']
        self.hbase_client_cfg = mod_cfgs['HBASE_GATEWAY_CONFIG']
        self.hbase_master_cfg = mod_cfgs['HBASE_MASTER_CONFIG']
        self.hbase_rs_cfg = mod_cfgs['HBASE_RS_CONFIG']
        self.hdfs_cfg = mod_cfgs['HDFS_CONFIG']
        self.hdfs_client_cfg = mod_cfgs['HDFS_GATEWAY_CONFIG']
        self.hdfs_nn_cfg = mod_cfgs['HDFS_NN_CONFIG']
        self.zk_cfg = mod_cfgs['ZK_CONFIG']

    def mod(self, xml_func, mod_cfgs):
        self._get_cfgs(mod_cfgs)

        info('Modifying HDFS/HBASE service configs ...')
        for srv, cfg, item_name in ((self.hdfs_service, self.hdfs_cfg, 'hdfs_service_config_safety_valve'), (self.hbase_service, self.hbase_cfg, 'hbase_service_config_safety_valve')):
            srvcfg_url = SRVCFG_URL_PTR % (self.url, self.cluster_name, srv)
            srvcfg_info = self.p.get(srvcfg_url)
            fixed_cfg_xml = [d['value'] for d in cfg['items'] if d['name'] == item_name][0]
            found = 0
            for item in srvcfg_info['items']:
                if item['name'] == item_name:
                    found = 1
                    current_cfg_xml = item['value']
                    final_cfg_xml = xml_func(current_cfg_xml, fixed_cfg_xml)
                    # update configs
                    cfg_copy = copy.deepcopy(cfg)
                    cfg_copy['items'].pop()
                    cfg_copy['items'].append({'name': item_name, 'value': final_cfg_xml})
                    self.p.put(srvcfg_url, cfg_copy)

            if not found:
                self.p.put(srvcfg_url, cfg)

        # HBase Master/RegionServer advanced configuration snippet contains multiple configs
        # in a single xml string, need to combine existing configs with ours
        role_infos = ((self.hbase_service, 'GATEWAY', 'hbase_client_config_safety_valve', self.hbase_client_cfg),
                      (self.hbase_service, 'REGIONSERVER', '', self.hbase_rs_cfg),
                      (self.hbase_service, 'MASTER', '', self.hbase_master_cfg),
                      (self.hdfs_service, 'GATEWAY', 'hdfs_client_config_safety_valve', self.hdfs_client_cfg),
                      (self.hdfs_service, 'NAMENODE', '', self.hdfs_nn_cfg),
                      (self.zk_service, 'SERVER', '', self.zk_cfg))

        for srv, role_type, role_name, cfg_dict in role_infos:
            rcgrp_url = RCGRP_URL_PTR % (self.url, self.cluster_name, srv)
            rcgrp_cfg = self.p.get(rcgrp_url)
            for r in rcgrp_cfg['items']:
                if r['roleType'] == role_type:
                    url = '%s/%s/config' % (rcgrp_url, r['name'])
                    if role_name:
                        fixed_cfg_xml = [d['value'] for d in cfg_dict['items'] if d['name'] == role_name][0]

                    found = 0
                    for c in r['config']['items']:
                        if c['name'] == role_name:
                            found = 1
                            current_cfg_xml = c['value']
                            final_cfg_xml = xml_func(current_cfg_xml, fixed_cfg_xml)
                            # update configs
                            cfg_dict_copy = copy.deepcopy(cfg_dict)
                            cfg_dict_copy['items'].pop()
                            cfg_dict_copy['items'].append({'name': role_name, 'value': final_cfg_xml})
                            info('Modifying %s configs for group [%s] ...' % (srv, r['name']))
                            self.p.put(url, cfg_dict_copy)
                    if not found:
                        info('Modifying %s configs for group [%s] with default settings ...' % (srv, r['name']))
                        self.p.put(url, cfg_dict)

    def save_cfgs(self, def_mod_cfgs):
        self._get_cfgs(def_mod_cfgs)

        if def_mod_cfgs['FLAG'] == 0:
            info('Save HDFS/HBASE service configs ...')
            for srv, cfgs in ((self.hdfs_service, self.hdfs_cfg), (self.hbase_service, self.hbase_cfg)):
                srvcfg_url = SRVCFG_URL_PTR % (self.url, self.cluster_name, srv)
                srvcfg_info = self.p.get(srvcfg_url)
                for item in srvcfg_info['items']:
                    for cfg in cfgs['items']:
                        if item['name'] == cfg['name']:
                            cfg['value'] = item['value']

            role_infos = ((self.hbase_service, 'GATEWAY', self.hbase_client_cfg),
                          (self.hbase_service, 'REGIONSERVER', self.hbase_rs_cfg),
                          (self.hbase_service, 'MASTER', self.hbase_master_cfg),
                          (self.hdfs_service, 'GATEWAY', self.hdfs_client_cfg),
                          (self.hdfs_service, 'NAMENODE', self.hdfs_nn_cfg),
                          (self.zk_service, 'SERVER', self.zk_cfg))

            for srv, role_type, cfgs in role_infos:
                rcgrp_url = RCGRP_URL_PTR % (self.url, self.cluster_name, srv)
                rcgrp_cfg = self.p.get(rcgrp_url)
                for r in rcgrp_cfg['items']:
                    if r['roleType'] == role_type:
                        for c in r['config']['items']:
                            for cfg in cfgs['items']:
                                if cfg['name'] == c['name']:
                                    cfg['value'] = c['value']

            def_mod_cfgs['FLAG'] = 1
            ParseJson(DEF_MODCFG_FILE).save(def_mod_cfgs)
        elif def_mod_cfgs['FLAG'] == 1:
            info('Configs have been changed in configs/def_mod_cfgs.json. Skip ...')


def combine(xmlstr1, xmlstr2):
    add_label = lambda s: '<configuration>' + s + '</configuration>'
    xmlstr1 = add_label(xmlstr1)
    xmlstr2 = add_label(xmlstr2)

    p1 = ParseXML(xmlstr1, 'string')
    p2 = ParseXML(xmlstr2, 'string')
    for kv in p2.output_xml():
        k, v = kv
        p1.add_property(k, v)

    rm_label = lambda s: s.replace('<configuration>', '').replace('</configuration>', '')
    return rm_label(p1.write_xml_to_string())


def run(dbcfgs):
    mod_cfgs = ParseJson(MODCFG_FILE).load()
    def_mod_cfgs = ParseJson(DEF_MODCFG_FILE).load()
    hadoop_mod = CDHMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])
    hadoop_mod.save_cfgs(def_mod_cfgs)
    hadoop_mod.mod(combine, mod_cfgs)


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
