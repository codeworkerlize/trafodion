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
import re
from common import HadoopDiscover, ParseHttp, err, retry
from gen_lock_config import generateCDHConfig

CLUSTER_URL_PTR = '%s/api/v1/clusters/%s'
SRVCFG_URL_PTR = CLUSTER_URL_PTR + '/services/%s/config'


class CDHMod(object):
    """ Modify CDH configs for trafodion """

    def __init__(self, user, passwd, url, cluster_name):  # cluster_name contains space
        self.cmd_id = -1
        self.url = url
        self.v1_check_url = '%s/api/v1/commands' % self.url
        self.cluster_name = cluster_name.replace(' ', '%20')
        self.cluster_url = CLUSTER_URL_PTR % (url, self.cluster_name)
        self.p = ParseHttp(user, passwd)
        self.hadoop_discover = HadoopDiscover(user, passwd, url, cluster_name)
        self.srvcfg_url = SRVCFG_URL_PTR % (
            self.url, self.cluster_name, self.hadoop_discover.get_hbase_srvname())

    def restart_hbase(self):
        hbase_restart_url = self.cluster_url + "/services/hbase/commands/restart"
        cmd = self.p.post(hbase_restart_url)
        if cmd['active']:
            self.cmd_id = cmd['id']

    def deploy_cfg(self):
        deploy_cfg_url = "%s/api/v6/clusters/%s/commands/deployClientConfig" % (
            self.url, self.cluster_name)
        cmd = self.p.post(deploy_cfg_url)
        if cmd['active']:
            self.cmd_id = cmd['id']

    def get_status(self):
        try:
            if self.cmd_id != -1:
                cmd = self.p.get('%s/%s' % (self.v1_check_url, self.cmd_id))
                return cmd['success']
            elif self.cmd_id == -1:
                return True
        except:
            err('Failed to get %s status info from CDH' % self.cluster_name)

    def get_hbase_env_opt(self):
        currhBaseEnvOption = ""
        srvcfg_info = self.p.get(self.srvcfg_url)
        for item in srvcfg_info["items"]:
            if item["name"] == "hbase_service_env_safety_valve":
                currhBaseEnvOption = item["value"]
                break
        return currhBaseEnvOption

    def set_hbase_env_opt(self, hBaseEnvOption):
        cfg = {"items": []}
        cfg["items"].append(
            {"name": "hbase_service_env_safety_valve", "value": hBaseEnvOption})
        self.p.put(self.srvcfg_url, cfg)


class HDPMod(object):
    """ Modify HDP configs for trafodion and restart HDP services """

    def __init__(self, user, passwd, url, cluster_name):
        self.p = ParseHttp(user, passwd, json_type=False)
        self.cluster_name = cluster_name
        self.cluster_url = CLUSTER_URL_PTR % (url, cluster_name)
        self.desired_cfg_url = self.cluster_url + '?fields=Clusters/desired_configs'
        self.cfg_url = self.cluster_url + '/configurations?type={0}&tag={1}'

    def restart_hbase(self):

        def startstop(srvs, action, count, interval):
            srv_baseurl = self.cluster_url + "/services/"
            state = 'INSTALLED' if action == 'stop' else 'STARTED'
            for srv in srvs:
                srv_url = srv_baseurl + srv
                config = {'RequestInfo': {'context': '%s %s services' % (
                    action, srv)}, 'ServiceInfo': {'state': state}}
                rc = self.p.put(srv_url, config)

                # check startstop status
                if rc:
                    def get_stat(): return self.p.get(srv_url)[
                        'ServiceInfo']['state'] == state
                    retry(get_stat, count, interval,
                          'HDP service %s %s' % (srv, action))
                else:
                    if action == 'stop':
                        action += 'p'
        startstop(["HBASE"], 'stop', 30, 10)
        time.sleep(10)
        startstop(["HBASE"], 'start', 60, 10)

    def get_status(self):
        status = self.p.get(self.cluster_url + "/services/HBASE")
        if status["ServiceInfo"]["state"] != None:
            if status["ServiceInfo"]["state"] == "STARTED":
                return True
        else:
            err('Failed to get %s status info from HDP' % self.cluster_name)
        return False

    def get_hbase_env_opt(self):
        desired_cfg = self.p.get(self.desired_cfg_url)
        curr_version = desired_cfg["Clusters"]["desired_configs"]["hbase-env"]["tag"]
        if curr_version == None:
            curr_version = "version1"
        hbase_env_opt = self.p.get(
            self.cfg_url.format("hbase-env", curr_version))
        if hbase_env_opt != None and hbase_env_opt["items"][0] != None and hbase_env_opt["items"][0]["properties"] != None:
            return hbase_env_opt["items"][0]["properties"]
        else:
            return {"content": ""}

    def set_hbase_env_opt(self, hBaseEnvOption):
        desired_cfg = self.p.get(self.desired_cfg_url)
        curr_version = desired_cfg["Clusters"]["desired_configs"]["hbase-env"]["version"]
        if curr_version == None:
            curr_version = 2
        else:
            num = int(curr_version)
            curr_version = "version%d" % (num + 1)
        cfg = {
            'Clusters':
            {'desired_config':
             {
                 'type': "hbase-env",
                 'tag': curr_version,
                 'properties': hBaseEnvOption
             }
             }
        }
        self.p.put(self.cluster_url, cfg)


def run(dbcfgs):
    useLock = 1
    lines = ""
    if dbcfgs["use_lock"].upper() != "Y":
        useLock = 0
    if 'CDH' in dbcfgs['distro']:
        newHbaseEnvOption = ""
        hBaseEnvOption = ""
        hadoop_mod = CDHMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'],
                            dbcfgs['mgr_url'], dbcfgs['cluster_name'])
        hBaseEnvOption = hadoop_mod.get_hbase_env_opt()
        tmp = hBaseEnvOption.split("\n")
        for line in tmp:
            if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                if re.search(r"^LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
                    if re.search(r"^REST_SERVER_URI\s*=.*", line) == None:
                        if lines != "":
                            lines += (line + "\n")
        newHbaseEnvOption = lines
        newHbaseEnvOption += ("ENABLE_ROW_LEVEL_LOCK=%d\n" % useLock)
        if useLock == 1:
            newHbaseEnvOption += "LOCK_ENABLE_DEADLOCK_DETECT=1\n"
            #generate for REST_SERVER_URI
            gen = generateCDHConfig(dbcfgs['traf_user'])
            newHbaseEnvOption += gen.generateHbaseEnvString()
        if newHbaseEnvOption != "":
            newHbaseEnvOption = newHbaseEnvOption[:-1]
        print("current option for hbase environment is:")
        print(hBaseEnvOption)
        print("---------------------------------------")
        print("new option for hbase environment is:")
        print(newHbaseEnvOption)
        hadoop_mod.set_hbase_env_opt(newHbaseEnvOption)
    elif 'HDP' in dbcfgs['distro']:
        hadoop_mod = HDPMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'],
                            dbcfgs['mgr_url'], dbcfgs['cluster_name'])
        current_HBase_env_option = hadoop_mod.get_hbase_env_opt()
        tmp = current_HBase_env_option["content"].split("\n")
        for line in tmp:
            if re.search(r"^export ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                if re.search(r"^export LOCK_ENABLE_DEADLOCK_DETECT\s*=.*", line) == None:
                    if re.search(r"^export REST_SERVER_URI\s*=.*", line) == None:
                        lines += (line + "\n")
        lines += ("export ENABLE_ROW_LEVEL_LOCK=%d\n" % useLock)
        if useLock == 1:
            lines += "export LOCK_ENABLE_DEADLOCK_DETECT=1\n"
            gen = generateCDHConfig(dbcfgs['traf_user'])
            lines += "export %s\n" % gen.generateHbaseEnvString()
        lines = lines[:-1]
        current_HBase_env_option.update({"content": lines})
        hadoop_mod.set_hbase_env_opt(current_HBase_env_option)
    else:
        err("Sorry, currently Trafodion doesn\'t support %s version" %
            dbcfgs['distro'])


# main
if __name__ == '__main__':
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
