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
from collections import defaultdict
from common import run_cmd, err, set_stream_logger, get_host_ip, write_file, gen_template_file, ParseXML
from constants import *

logger = logging.getLogger()

def run(dbcfgs_json):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    host_ip = get_host_ip(socket)
    mgr_node = dbcfgs['management_node']

    template = ParseXML(TEMPLATE_XML)
    dbmgr_dir = dbcfgs['dbmgr_dir']
    dbmgr_config_file = '%s/conf/dbmgr/config.xml' % dbmgr_dir
    dbmgr_remote_instance_file = '%s/conf/dbmgr/remote_instances.json' % dbmgr_dir
    dbmgr_env_file = '%s/bin/env.sh' % dbmgr_dir
    ldap_config_file = '%s/conf/.traf_authentication_config' % dbmgr_dir

    if mgr_node == host_ip:
        # fixed configs for core banking off instance dbmgr
        dbmgr_configs = '''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">

<properties>
    <entry key="sessionTimeoutMinutes">120</entry>
    <entry key="timeZoneName">Asia/Shanghai</entry>
    <entry key="httpReadTimeOutSeconds">120</entry>
    <entry key="qwbMaxRows">100000</entry>
    <entry key="enableHTTPS">False</entry>
    <entry key="httpPort">%s</entry>
    <entry key="requestHeaderSize">98304</entry>
</properties>
''' % DEF_SERVER_PORT
        dbmgr_env = '''
export TRAF_LOG=%s/logs
export DBMGR_CONF=%s/conf
export TRAF_CONF=%s/conf
export DBMGR_VAR=%s
export DBMGR_STD_ALONE=1
export TRAFODION_ENABLE_AUTHENTICATION=YES
''' % (DEF_DBMGR_DIR, DEF_DBMGR_DIR, DEF_DBMGR_DIR, DEF_DBMGR_DIR)

        ### copy files
        run_cmd('cp -rf /tmp/dbmgr /opt')

        ### deploy config.xml
        logger.debug('Deploy dbmgr config file: [%s]' % dbmgr_config_file)
        write_file(dbmgr_config_file, dbmgr_configs)

        ### deploy env.sh
        write_file(dbmgr_env_file, dbmgr_env)

        ### deploy remote_instances.json
        json_dict = [{"clusterID": int(dbcfgs['traf_cluster_id']),
                    "clusterName": "Cluster 1",
                    "instanceID": 1,
                    "instanceName": "ESGYNDB",
                    "url": "http://%s:%s" % (dbcfgs['first_node'], REMOTE_SERVER_PORT),
                    "grafanaDashboardUrl": "http://%s:3000/d/esgyndb/dashboard?orgId=1" % dbcfgs['management_node'], # url name should match the grafana dashboard configuration
                    "elasticSearchUrls": ["http://%s:9200" % dbcfgs['management_node']]
        }]
        write_file(dbmgr_remote_instance_file, json.dumps(json_dict))

        ### deploy DB Manager LDAP client config: .traf_authentication_config
        logger.debug('Deploy LDAP config file: [%s]' % ldap_config_file)

        ldap_configs = defaultdict(str)

        ldap_hostname = 'LDAPHostName:%s\n' % dbcfgs['management_node']
        if dbcfgs['ldap_ha_node']:
            ldap_hostname += 'LDAPHostName:%s\n' % dbcfgs['ldap_ha_node']

        ldap_configs['ldap_hosts'] = ldap_hostname

        ldap_configs['ldap_identifiers'] = 'UniqueIdentifier:uid=,ou=Users,dc=esgyn,dc=local'
        ldap_configs['ldap_port'] = '389'
        ldap_configs['ldap_encrypt'] = '0'

        if not ldap_configs['ldap_domain']:
            ldap_configs['ldap_domain'] = 'local'
        gen_template_file(template.get_property('ldap_config_template'), ldap_config_file, ldap_configs)

        ### start dbmgr
        run_cmd('chmod 755 %s/bin/env.sh %s/bin/dbmgr.sh' % (dbmgr_dir, dbmgr_dir))
        run_cmd('source %s/bin/env.sh && %s/bin/dbmgr.sh start' % (dbmgr_dir, dbmgr_dir))


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)
