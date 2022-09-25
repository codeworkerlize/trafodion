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
from constants import MODCFG_FILE, CUSTOM_MODCFG_FILE_AP,CUSTOM_MODCFG_FILE_TP, CUSTOM_MOD_HA_CFG_FILE
from common import HadoopDiscover, ParseXML, ParseHttp, ParseJson, err, info, retry, run_cmd_as_user, set_stream_logger

logger = logging.getLogger()

CLUSTER_URL_PTR = '%s/api/v1/clusters/%s'
RESTART_URL_PTR = CLUSTER_URL_PTR + '/commands/restart'
RESTART_SRV_URL_PTR = CLUSTER_URL_PTR + '/services/%s/commands/restart'
SRVCFG_URL_PTR = CLUSTER_URL_PTR + '/services/%s/config'
RCGRP_URL_PTR = '%s/api/v6/clusters/%s/services/%s/roleConfigGroups'
DEPLOY_CFG_URL_PTR = '%s/api/v6/clusters/%s/commands/deployClientConfig'
CMD_STAT_URL_PTR = '%s/api/v1/commands/%s'

HBASE_HREGION_PROPERTY = 'hbase.hregion.impl'
HBASE_CONFIG_GROUP_TAG = 'HBASE'

class CDHMod(object):
    """ Modify CDH configs for trafodion and restart CDH services """
    def __init__(self, user, passwd, url, cluster_name):  # cluster_name contains space
        self.url = url
        self.cluster_name = cluster_name.replace(' ', '%20')
        self.p = ParseHttp(user, passwd)
        hadoop_discover = HadoopDiscover(user, passwd, url, cluster_name)
        hdfs_service = hadoop_discover.get_hdfs_srvname()
        hbase_service = hadoop_discover.get_hbase_srvname()
        zk_service = hadoop_discover.get_zookeeper_srvname()
        yarn_service = hadoop_discover.get_yarn_srvname()
        self.mod_items = {'hbase': hbase_service, 'hdfs': hdfs_service, 'zookeeper': zk_service, 'yarn': yarn_service}

    def _mod_service(self, cfg_dict, combine_func):
        srv = self.mod_items[cfg_dict['serviceName']]
        cfg = cfg_dict['config']
        info('Modifying %s service configs ...' % srv)
        srv_cfg_url = SRVCFG_URL_PTR % (self.url, self.cluster_name, srv)
        self.p.put(srv_cfg_url, cfg)

    def _mod_service_site_file(self, cfg_dict, combine_func):
        srv = self.mod_items[cfg_dict['serviceName']]
        cfgs = cfg_dict['config']
        info('Modifying %s service configs ...' % srv)
        srv_cfg_url = SRVCFG_URL_PTR % (self.url, self.cluster_name, srv)
        srv_cfg = self.p.get(srv_cfg_url)
        for item in srv_cfg['items']:
            for cfg in cfgs['items']:
                if cfg['name'] == item['name']:
                    cfg['value'] = combine_func(item['value'], cfg['value'])

        self.p.put(srv_cfg_url, cfgs)

    def _mod_roll_cfg_group(self, cfg_dict, combine_func):
        srv = self.mod_items[cfg_dict['serviceName']]
        role_type = cfg_dict['roleType']
        cfgs = cfg_dict['config']
        rcgrp_url = RCGRP_URL_PTR % (self.url, self.cluster_name, srv)
        rcgrp_cfg = self.p.get(rcgrp_url)
        for r in rcgrp_cfg['items']:
            if r['roleType'] == role_type:
                url = '%s/%s/config' % (rcgrp_url, r['name'])
                self.p.put(url, cfgs)

    def _mod_roll_cfg_group_site_file(self, cfg_dict, combine_func):
        srv = self.mod_items[cfg_dict['serviceName']]
        role_type = cfg_dict['roleType']
        cfgs = cfg_dict['config']
        rcgrp_url = RCGRP_URL_PTR % (self.url, self.cluster_name, srv)
        rcgrp_cfg = self.p.get(rcgrp_url)
        for r in rcgrp_cfg['items']:
            if r['roleType'] == role_type:
                url = '%s/%s/config' % (rcgrp_url, r['name'])
                cfgs_copy = copy.deepcopy(cfgs)
                for cfg in cfgs_copy['items']:
                    for c in r['config']['items']:
                        if c['name'] == cfg['name']:
                            cfg['value'] = combine_func(c['value'], cfg['value'])
                self.p.put(url, cfgs_copy)

    def _mod(self, config_items, mod_func, combine_func=None):
        if not len(config_items):
            return

        for cfg_dict in config_items:
            if not self.mod_items[cfg_dict['serviceName']]:
                continue
            mod_func(cfg_dict, combine_func)

    def mod(self, mod_cfgs):
        srv_config = mod_cfgs['SERVER_CONFIG']
        srv_config_site = mod_cfgs['SERVICE_CONFIG_SITE']
        role_config_group = mod_cfgs['ROLE_CONFIG_GROUP']
        role_config_group_site = mod_cfgs['ROLE_CONFIG_GROUP_SITE']
        env_config = mod_cfgs['ENV_CONFIG']

        self._mod(srv_config, self._mod_service)
        self._mod(srv_config_site, self._mod_service_site_file, combine_xml)
        self._mod(role_config_group, self._mod_roll_cfg_group)
        self._mod(role_config_group_site, self._mod_roll_cfg_group_site_file, combine_xml)
        self._mod(env_config, self._mod_service_site_file, combine_dict)

    def restart(self):
        restart_url = RESTART_URL_PTR % (self.url, self.cluster_name)
        deploy_cfg_url = DEPLOY_CFG_URL_PTR % (self.url, self.cluster_name)

        def __retry(url, maxcnt, interval, msg):
            rc = self.p.post(url)
            stat_url = CMD_STAT_URL_PTR % (self.url, rc['id'])
            get_stat = lambda: self.p.get(stat_url)['success'] is True and self.p.get(stat_url)['active'] is False
            retry(get_stat, maxcnt, interval, msg)

        info('Restarting CDH services ...')
        __retry(restart_url, 50, 15, 'CDH services restart')

        info('Deploying CDH client configs ...')
        __retry(deploy_cfg_url, 30, 10, 'CDH services deploy')

    def get_hbase_env_opt(self):
        currhBaseEnvOption = ""
        srvcfg_url = SRVCFG_URL_PTR % (
            self.url, self.cluster_name, self.mod_items['hbase'])
        srvcfg_info = self.p.get(srvcfg_url)
        for item in srvcfg_info["items"]:
            if item["name"] == "hbase_service_env_safety_valve":
                currhBaseEnvOption = item["value"]
                break
        return currhBaseEnvOption

class HDPMod(object):
    """ Modify HDP configs for trafodion and restart HDP services """
    def __init__(self, user, passwd, url, cluster_name):
        self.url = url
        self.cluster_name = cluster_name
        self.p = ParseHttp(user, passwd, json_type=False)

    def mod(self, mod_cfgs):
        cluster_url = CLUSTER_URL_PTR % (self.url, self.cluster_name)
        desired_cfg_url = cluster_url + '?fields=Clusters/desired_configs'
        cfg_url = cluster_url + '/configurations?type={0}&tag={1}'
        desired_cfg = self.p.get(desired_cfg_url)
        mod_cfgs = mod_cfgs['MOD_CFGS']

        hdp = self.p.get('%s/services/HBASE/components/HBASE_REGIONSERVER' % cluster_url)
        rsnodes = [c['HostRoles']['host_name'] for c in hdp['host_components']]

        # delete HBase config group created by pyinstaller if exists
        config_groups = self.p.get('%s/config_groups' % cluster_url)
        for item in config_groups['items']:
            id = item['ConfigGroup']['id']
            id_url = '%s/config_groups/%s' % (cluster_url, id)
            config_group_info = self.p.get(id_url)
            if HBASE_CONFIG_GROUP_TAG == config_group_info['ConfigGroup']['tag']:
                self.p.delete(id_url)

        hbase_config_group = {
            'ConfigGroup': {
                'cluster_name': self.cluster_name,
                'group_name': 'hbase-regionserver',
                'tag': HBASE_CONFIG_GROUP_TAG,
                'description': 'HBase Regionserver configs for Trafodion',
                'hosts': [{'host_name': host} for host in rsnodes],
                'desired_configs': [
                    {
                        'type': 'hbase-site',
                        'tag': 'traf_cg',
                        'properties': {HBASE_HREGION_PROPERTY: mod_cfgs['hbase-site'].pop(HBASE_HREGION_PROPERTY)}
                    }
                ]
            }
        }
        self.p.post('%s/config_groups' % cluster_url, hbase_config_group)

        for config_type in mod_cfgs.keys():
            desired_tag = desired_cfg['Clusters']['desired_configs'][config_type]['tag']
            current_cfg = self.p.get(cfg_url.format(config_type, desired_tag))
            tag = 'version' + str(int(time.time() * 1000000))
            new_properties = current_cfg['items'][0]['properties']
            new_properties.update(mod_cfgs[config_type])
            config = {
                'Clusters': {
                    'desired_config': {
                        'type': config_type,
                        'tag': tag,
                        'properties': new_properties
                    }
                }
            }
            self.p.put(cluster_url, config)

    def restart(self):
        info('Restarting HDP services ...')
        def startstop(srvs, action, count, interval):
            srv_baseurl = CLUSTER_URL_PTR % (self.url, self.cluster_name) + '/services/'
            state = 'INSTALLED' if action == 'stop' else 'STARTED'
            for srv in srvs:
                srv_url = srv_baseurl + srv
                config = {'RequestInfo': {'context': '%s %s services' % (action, srv)}, 'ServiceInfo': {'state': state}}
                rc = self.p.put(srv_url, config)

                # check startstop status
                if rc:
                    get_stat = lambda: self.p.get(srv_url)['ServiceInfo']['state'] == state
                    retry(get_stat, count, interval, 'HDP service %s %s' % (srv, action))
                else:
                    if action == 'stop': action += 'p'
                    info('HDP service %s had already been %sed' % (srv, action))

        srvs = ['HBASE', 'HDFS', 'ZOOKEEPER']
        startstop(srvs, 'stop', 30, 10)
        time.sleep(10)
        srvs.reverse()
        startstop(srvs, 'start', 60, 10)

    def get_hbase_env_opt(self):
        cluster_url = CLUSTER_URL_PTR % (self.url, self.cluster_name)
        desired_cfg_url = cluster_url + '?fields=Clusters/desired_configs'
        cfg_url = cluster_url + '/configurations?type=hbase-env&tag=%s'
        desired_cfg = self.p.get(desired_cfg_url)
        curr_version = desired_cfg["Clusters"]["desired_configs"]["hbase-env"]["tag"]
        if curr_version == None:
            curr_version = "version1"
        hbase_env_opt = self.p.get(cfg_url % curr_version)
        if hbase_env_opt != None and hbase_env_opt["items"][0] != None and hbase_env_opt["items"][0]["properties"] != None:
            return hbase_env_opt["items"][0]["properties"]
        else:
            return {"content": ""}


def combine_xml(xmlstr1, xmlstr2):
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


def combine_dict(dict1, dict2):
    dict1 = dict([(item.strip().split('=')) for item in dict1.strip('\n').split('\n')]) if dict1 else {}
    dict2 = dict([(item.strip().split('=')) for item in dict2.strip('\n').split('\n')])
    dict1.update(dict2)
    return '\n'.join(['='.join(items) for items in dict1.items()])


def run(dbcfgs):
    # set cluster ID before restarting HBase
    cmd = "%s/sql/scripts/xdc -setmyid %s" % (dbcfgs['traf_abspath'], dbcfgs['traf_cluster_id'])
    run_cmd_as_user(dbcfgs['traf_user'], cmd)

    # set hbase balancer before restarting HBase
    run_cmd_as_user(dbcfgs['traf_user'], 'echo "balance_switch false" | hbase shell')

    if dbcfgs['customized'] == 'CUSTOM_AP':
        modcfg_file = CUSTOM_MODCFG_FILE_AP
    elif dbcfgs['customized'] == 'CUSTOM_TP':
        modcfg_file = CUSTOM_MODCFG_FILE_TP
    else:
        modcfg_file = MODCFG_FILE
    mod_cfgs = ParseJson(modcfg_file).load()
    mod_ha_cfgs = ParseJson(CUSTOM_MOD_HA_CFG_FILE).load()


    if 'CDH' in dbcfgs['distro']:
        hadoop_mod = CDHMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])
    elif 'HDP' in dbcfgs['distro']:
        hadoop_mod = HDPMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])

    if dbcfgs['enhance_ha'] == 'Y':
        hadoop_mod.mod(mod_ha_cfgs)

    hadoop_mod.mod(mod_cfgs)
    hadoop_mod.restart()


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
