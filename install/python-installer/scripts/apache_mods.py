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

### this script should be run on all rsnodes with sudo user ###

import os
import re
import sys
import json
import socket
import time
import base64
import Hadoop
import logging
from collections import defaultdict
from constants import MODCFG_FILE, CUSTOM_MOD_HA_CFG_FILE,CUSTOM_MODCFG_FILE_AP,CUSTOM_MODCFG_FILE_TP
from common import ParseJson, ParseXML, err, set_stream_logger, mod_file, run_cmd

logger = logging.getLogger()


def modify_heap_size(opts_value, heap_size):
    xmx_opts = re.search(r'-Xmx\d+.?\s?', opts_value)
    if xmx_opts:
        opts_value = re.sub(r'%s' % xmx_opts.group().strip(), '-Xmx%s' % heap_size, opts_value)
    else:
        opts_value += ' -Xmx%s' % heap_size

    xms_opts = re.search(r'-Xms\d+.?\s?', opts_value)
    if xms_opts:
        opts_value = re.sub(r'%s' % xms_opts.group().strip(), '-Xms%s' % heap_size, opts_value)
    else:
        opts_value += ' -Xms%s' % heap_size
    return opts_value


def mod_env_file(env_file, mod_cfgs):
    with open(env_file, 'r') as f:
        content = f.read()

    for key, value in mod_cfgs.items():
        if key.endswith('OPTS') or key.endswith('JVMFLAGS'):
            heap_key = key[:key.rfind('_')] + '_HEAPSIZE'
            if mod_cfgs.has_key(heap_key):
                value = modify_heap_size(value, mod_cfgs[heap_key])

            env = re.search(r'\nexport %s\s*=.*' % key, content)
            if not env:
                content += 'export %s=\"$%s %s\"\n' % (key, key, value)
            else:
                env = env.group()
                opts_key, _ = env.split('=', 1)
                opts_value = '$' + key.strip() + ' ' + value
                content = re.sub(r'%s\s*=.*' % opts_key, '%s=\"%s\"' % (opts_key, opts_value), content)
        elif key.endswith('HEAPSIZE'):
            if os.path.basename(env_file) == 'zookeeper-env.sh':
                key = key.rstrip('HEAPSIZE') + 'JVMFLAGS'
            else:
                key = key.rstrip('HEAPSIZE') + 'OPTS'

            env = re.search(r'\nexport %s\s*=.*' % key, content)
            if not env:
                value = '-Xmx%s -Xms%s' % (value, value)
                content += 'export %s=\"$%s %s\"\n' % (key, key, value)
            else:
                env = env.group()
                opts_key, opts_value = env.split('=', 1)
                opts_value = modify_heap_size(opts_value.strip('"'), value)
                content = re.sub(r'%s\s*=.*' % opts_key, '%s=\"%s\"' % (opts_key, opts_value), content)
        else:
            env = re.search(r'\nexport %s\s*=.*' % key, content)
            if not env:
                content += 'export %s=%s\n' % (key, value)
            else:
                env = env.group()
                content = re.sub(r'%s' % env, '\nexport %s=%s' % (key, value), content)

    with open(env_file, 'w') as f:
        f.write(content)


def mod_xml_file(xml_file, *mod_cfgs_list):
    for mod_cfgs in mod_cfgs_list:
        for key, value in mod_cfgs.items():
            xml_file.add_property(key, value)
    xml_file.write_xml()


def run(dbcfgs):
    if 'APACHE' in dbcfgs['distro']:
        logger.info('Modify HDFS/HBase configs ...')
        modcfgs = ParseJson(MODCFG_FILE).load()
        MOD_CFGS = modcfgs['MOD_CFGS']

        hdfs_xml_file = dbcfgs['hdfs_xml_file']
        hadoop_env_file = dbcfgs['hadoop_env_file']
        # HBase RegionServer config
        hbase_xml_file = dbcfgs['hbase_xml_file']
        hbase_env_file = dbcfgs['hbase_env_file']
        zoo_cfg_file = dbcfgs['zoo_cfg_file']
        zoo_env_file = '%s/conf/zookeeper-env.sh' % dbcfgs['zookeeper_home']

        hbasexml = ParseXML(hbase_xml_file)
        if dbcfgs['distro'] == 'CDH_APACHE':
            master_hbasexml = ParseXML('%s/hm-conf/hbase-site.xml' % dbcfgs['hbase_home'])
        hdfsxml = ParseXML(hdfs_xml_file)

        mod_xml_file(hbasexml, MOD_CFGS['hbase-site'])
        if dbcfgs['distro'] == 'CDH_APACHE': mod_xml_file(master_hbasexml, MOD_CFGS['hbase-site'])
        mod_xml_file(hdfsxml, MOD_CFGS['hdfs-site'])
        mod_env_file(hbase_env_file, MOD_CFGS['hbase-env'])

        if dbcfgs['enhance_ha'] == 'Y':
            ha_modcfgs = ParseJson(CUSTOM_MOD_HA_CFG_FILE).load()
            HA_MOD_CFGS = ha_modcfgs['MOD_CFGS']

            mod_xml_file(hbasexml, HA_MOD_CFGS['hbase-site'])
            mod_xml_file(hdfsxml, HA_MOD_CFGS['hdfs-site'])
            if socket.gethostname() in dbcfgs['hm_host']:
                mod_xml_file(master_hbasexml, HA_MOD_CFGS['hbase-site-master'])

            if os.path.exists(zoo_cfg_file):
                change_items = {}
                for key, value in HA_MOD_CFGS['zoo.cfg'].items():
                    change_items['%s=.*' % key] = key + '=' + value
                mod_file(zoo_cfg_file, change_items)


        if dbcfgs['customized'] == 'CUSTOM_AP':
            cus_mofcfgs = ParseJson(CUSTOM_MODCFG_FILE_AP).load()
            CUS_MOD_CFGS = cus_mofcfgs['MOD_CFGS']
            mod_xml_file(hbasexml, CUS_MOD_CFGS['hbase-site'])
            if dbcfgs['distro'] == 'CDH_APACHE':
                mod_xml_file(master_hbasexml, CUS_MOD_CFGS['hbase-site'], CUS_MOD_CFGS['hbase-master-site'])
                mod_xml_file(hbasexml, CUS_MOD_CFGS['hbase-regionserver-site'])
                if CUS_MOD_CFGS['hbase-regionserver-site']['hbase.bucketcache.size']:
                    direct_mem = int(CUS_MOD_CFGS['hbase-regionserver-site']['hbase.bucketcache.size']) / 1024 + 1
                    CUS_MOD_CFGS['hbase-env']['HBASE_REGIONSERVER_OPTS'] += ' -XX:MaxDirectMemorySize=%dg' % direct_mem
            mod_xml_file(hdfsxml, CUS_MOD_CFGS['hdfs-site'])
            mod_env_file(hbase_env_file, CUS_MOD_CFGS['hbase-env'])
            mod_env_file(hadoop_env_file, CUS_MOD_CFGS['hadoop-env'])
            if os.path.exists(zoo_env_file):
                mod_env_file(zoo_env_file, CUS_MOD_CFGS['zookeeper-env'])
                run_cmd('chmod 755 %s' % zoo_env_file)

            if os.path.exists(zoo_cfg_file):
                change_items = {}
                for key, value in CUS_MOD_CFGS['zoo.cfg'].items():
                    change_items['%s=.*' % key] = key + '=' + value
                mod_file(zoo_cfg_file, change_items)
        elif dbcfgs['customized'] == 'CUSTOM_TP':
            cus_mofcfgs = ParseJson(CUSTOM_MODCFG_FILE_TP).load()
            CUS_MOD_CFGS = cus_mofcfgs['MOD_CFGS']
            mod_xml_file(hbasexml, CUS_MOD_CFGS['hbase-site'])
            if dbcfgs['distro'] == 'CDH_APACHE':
                mod_xml_file(master_hbasexml, CUS_MOD_CFGS['hbase-site'], CUS_MOD_CFGS['hbase-master-site'])
                mod_xml_file(hbasexml, CUS_MOD_CFGS['hbase-regionserver-site'])
                if CUS_MOD_CFGS['hbase-regionserver-site']['hbase.bucketcache.size']:
                    direct_mem = int(CUS_MOD_CFGS['hbase-regionserver-site']['hbase.bucketcache.size']) / 1024 + 1
                    CUS_MOD_CFGS['hbase-env']['HBASE_REGIONSERVER_OPTS'] += ' -XX:MaxDirectMemorySize=%dg' % direct_mem
            mod_xml_file(hdfsxml, CUS_MOD_CFGS['hdfs-site'])
            mod_env_file(hbase_env_file, CUS_MOD_CFGS['hbase-env'])
            mod_env_file(hadoop_env_file, CUS_MOD_CFGS['hadoop-env'])
            if os.path.exists(zoo_env_file):
                mod_env_file(zoo_env_file, CUS_MOD_CFGS['zookeeper-env'])
                run_cmd('chmod 755 %s' % zoo_env_file)

            if os.path.exists(zoo_cfg_file):
                change_items = {}
                for key, value in CUS_MOD_CFGS['zoo.cfg'].items():
                    change_items['%s=.*' % key] = key + '=' + value
                mod_file(zoo_cfg_file, change_items) 




        services = ['hbase', 'yarn', 'hadoop', 'zookeeper']
        hadoop_factory = [Hadoop.install_cdh(srv, dbcfgs) for srv in services]
        if dbcfgs['install_qian'] == 'Y':
            for h in hadoop_factory: h.stop()
        for h in hadoop_factory[::-1]: h.start()
        logger.info('Apache Hadoop start completed')
    else:
        logger.info('no Apache distribution found, skipping')


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')

    run(dbcfgs)
