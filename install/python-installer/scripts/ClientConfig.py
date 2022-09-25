#!/usr/bin/env python

import os
import re
import sys
import json
import socket
import logging
from constants import *
from collections import defaultdict
from common import License, append_file, write_file, mod_file, gen_template_file, cmd_output, \
                   ParseInI, ParseXML, err, run_cmd, get_timezone, encode_pwd, replace_file

logger = logging.getLogger()

class HDFSClientConfig(object):
    """ get HDFS client configurations """
    def __init__(self, hadoop_home=''):
        if hadoop_home:  # apache distro
            core_site_xml = '%s/etc/hadoop/core-site.xml' % hadoop_home
        else:
            core_site_xml = DEF_CORE_SITE_XML
        self.p = ParseXML(core_site_xml)

    def get_hadoop_auth(self):
        """Hadoop authentication"""
        return self.p.get_property('hadoop.security.authentication')

    def get_hadoop_group_mapping(self):
        """Hadoop security group mapping"""
        mapping = self.p.get_property('hadoop.security.group.mapping')
        if 'ShellBasedUnixGroupsMapping' in mapping:
            return 'SHELL'
        elif 'LdapGroupsMapping' in mapping:
            return 'LDAP'
        else:
            return 'NONE'

class HBaseClientConfig(object):
    """ get HBase client configurations """
    def __init__(self, hbase_home=''):
        if hbase_home:  # apache distro
            self.hbase_xml_file = '%s/conf/hbase-site.xml' % hbase_home
        else:
            self.hbase_xml_file = DEF_HBASE_SITE_XML
        self.p = ParseXML(self.hbase_xml_file)

    def _get_property(self, prop):
        val = self.p.get_property(prop)
        if not val:
            err('HBase property \'%s\' is empty, please check HBase config file \'%s\' is correct' % (prop, self.hbase_xml_file))
        else:
            return val

    def get_zk_nodes(self):
        return self._get_property('hbase.zookeeper.quorum')

    def get_zk_port(self):
        return self._get_property('hbase.zookeeper.property.clientPort')

    def get_znode_parent(self):
        return self._get_property('zookeeper.znode.parent')

    def get_hbase_rootdir(self):
        return self._get_property('hbase.rootdir')

    def get_hbase_rpc_protection(self):
        return self._get_property('hbase.rpc.protection')

    def get_hbase_master_info_port(self):
        return self._get_property('hbase.master.info.port')

    def get_hbase_regionserver_info_port(self):
        return self._get_property('hbase.regionserver.info.port')

class ClientConfig(object):
    """ deploy client configs for all EsgynDB components """
    def __init__(self, client_configs):
        self.cfgs = client_configs  # defaultdict
        self.template = ParseXML(TEMPLATE_XML)
        self.nodes = self.cfgs['node_list'].split(',')
        self.timezone = get_timezone()
        self.localhost = socket.gethostname()

        if 'APACHE' in self.cfgs['distro']:
            self.hc = HBaseClientConfig(self.cfgs['hbase_home'])
        else:
            self.hc = HBaseClientConfig()

        self.zk_nodes = self.hc.get_zk_nodes()
        self.zk_port = self.hc.get_zk_port()

        traf_home = self.cfgs['traf_abspath']
        traf_version = self.cfgs['traf_version']

        try:
            self.mgmt_node = re.search(".*://(.*):.*", self.cfgs['mgr_url']).groups()[0]
        except AttributeError:
            self.mgmt_node = 'localhost'

        if self.cfgs['om_ha_arch'].upper() == 'N':
            self.cfgs['es_virtual_ip'] = self.mgmt_node

        # set default values for multi-instance support
        self.cfgs['traf_instance_id'] = '1'
        self.cfgs['traf_instance_name'] = 'ESGYNDB'

        # dcs config files
        dcs_conf_dir = '%sdcs' % (TRAF_CFG_DIR)
        self.dcs_srv_file = dcs_conf_dir + '/servers'
        self.dcs_master_file = dcs_conf_dir + '/masters'
        self.dcs_mds_file = dcs_conf_dir + '/mdsconf.yaml'
        self.dcs_site_file = dcs_conf_dir + '/dcs-site.xml'
        self.dcs_znode_parent = '/%s/%s' % (self.cfgs['traf_user'], self.cfgs['traf_instance_id'])
        # rest config file
        self.rest_site_file = '%s/rest/rest-site.xml' % (TRAF_CFG_DIR)
        # keystore file
        self.keystore_file = '%s/%s/sqcert/server.keystore' % (self.cfgs['home_dir'], self.cfgs['traf_user'])
        # ldap config file
        self.ldap_config_file = '%s/.traf_authentication_config' % TRAF_CFG_DIR
        # ms.env config file
        self.ms_env_file = '%s/tmp/ms.env' % traf_home
        # ms.env config file
        self.ms_env_custom_file = '%s/ms.env.custom' % TRAF_CFG_DIR
        # genms config file
        self.genms_file = '%s/sql/scripts/genms' % traf_home
        # mgblty config files
        self.esgyn_exporter_config = '%s/mgblty/esgyn_exporter/esgyn_exporter.yaml' % TRAF_CFG_DIR
        self.filebeat_config = '%s/mgblty/filebeat/filebeat.yml' % TRAF_CFG_DIR
        # sqconfig
        self.sqconfig_file = TRAF_CFG_DIR + '/sqconfig'
        # dbmgr config
        self.dbmgr_template_file = '%s/dbmgr/config_template.xml' % TRAF_CFG_DIR
        self.dbmgr_config_file = '%s/dbmgr/config.xml' % TRAF_CFG_DIR
        # hplsql config example
        self.hplsql_site_file = '%s/hplsql-site.xml.example' % TRAF_CFG_DIR
        # core pattern
        self.core_pattern = '%s/cores' % self.cfgs['core_loc']

        # ports
        ports = ParseInI(DEF_PORT_FILE, 'ports').load()
        self.rest_http_port = ports['rest_http_port']
        self.rest_https_port = ports['rest_https_port']
        self.dm_http_port = ports['dm_http_port']
        self.dm_https_port = ports['dm_https_port']
        self.dcs_master_port = ports['dcs_master_port']
        self.dcs_info_port = ports['dcs_master_info_port']
        self.monitor_comm_port = ports['monitor_comm_port']
        if self.cfgs['om_ha_arch'].upper() == 'N':
            self.logstash_port = ports['logstash_port']
            self.elasticsearch_port = ports['elasticsearch_port']
        elif self.cfgs['om_ha_arch'].upper() == 'Y':
            self.logstash_port = ports['ha_logstash_port']
            self.elasticsearch_port = ports['ha_elasticsearch_port']

    def deploy_all(self):
        self.deploy_sqconfig()
        self.deploy_trafcfg()
        self.deploy_traf_site()
        # self.deploy_mgblty()
        self.deploy_dbmgr()
        self.deploy_ldap()
        self.deploy_auth_type()
        # self.deploy_msenv()
        # self.deploy_license()
        self.deploy_dcsmaster()
        self.deploy_dcsserver()
        self.deploy_dcsmds()
        self.deploy_dcssite()
        self.deploy_rest()
        self.deploy_hplsql()
        self.deploy_msenv_custom()
        self.deploy_lock()
        self.deploy_sqenvcom()

    def deploy_sqconfig(self):
        logger.debug('Deploy sqconfig file: [%s]' % self.sqconfig_file)
        try:
            core, processor = cmd_output("lscpu|grep -E '(^CPU\(s\)|^Socket\(s\))'|awk '{print $2}'").split('\n')[:2]
        except ValueError:
            # no actual usage for these two values
            # so if error occurs, we give them a hardcoded one
            core, processor = 4, 4
        core = int(core)-1 if int(core) <= 256 else 255
        if core <= 0: core = 1

        lines = ['begin node\n']
        for node_id, node in enumerate(self.nodes):
            line = 'node-id=%s;node-name=%s;cores=0-%d;processors=%s;roles=connection,aggregation,storage\n' % (node_id, node, core, processor)
            lines.append(line)

        lines.append('end node\n')
        lines.append('\n')
        lines.append('begin overflow\n')

        scratch_locs = self.cfgs['scratch_locs'].split(',')
        for scratch_loc in scratch_locs:
            line = 'hdd %s\n' % scratch_loc
            lines.append(line)

        lines.append('end overflow\n')

        # write out the node section
        with open(self.sqconfig_file, 'w') as f:
            f.writelines(lines)

    def deploy_trafcfg(self):
        """ setup trafodion_config file """
        logger.debug('Deploy trafodion_config file: [%s]' % TRAF_CFG_FILE)
        self.cfgs['zk_nodes'] = self.zk_nodes
        self.cfgs['zk_port'] = self.zk_port

        # cgroups setting
        try:
            lic = License(EDB_LIC_FILE)
        except StandardError as e:
            err(str(e))
        if lic.is_multi_tenancy_enabled():
            cpu_mnt = cmd_output("lssubsys -m cpu | cut -f2 -d' '")
            cpuacct_mnt = cmd_output("lssubsys -m cpuacct | cut -f2 -d' '")
            mem_mnt = cmd_output("lssubsys -m memory | cut -f2 -d' '")

            self.cfgs['esgyn_cg_cpu'] = 'cpu:/%s' % EDB_CGROUP_NAME
            self.cfgs['esgyn_cg_cpuacct'] = 'cpuacct:/%s' % EDB_CGROUP_NAME
            self.cfgs['esgyn_cg_mem'] = 'memory:/%s' % EDB_CGROUP_NAME
            self.cfgs['esgyn_cgp_cpu'] = '%s/%s' % (cpu_mnt, EDB_CGROUP_NAME)
            self.cfgs['esgyn_cgp_cpuacct'] = '%s/%s' % (cpuacct_mnt, EDB_CGROUP_NAME)
            self.cfgs['esgyn_cgp_mem'] = '%s/%s' % (mem_mnt, EDB_CGROUP_NAME)

        distro = self.cfgs['distro']
        if 'CDH_APACHE' in distro:
            self.cfgs['hadoop_type'] = 'cdh'
        elif 'CDH' in distro:
            self.cfgs['hadoop_type'] = 'cloudera'
        elif 'HDP' in distro:
            self.cfgs['hadoop_type'] = 'hortonworks'
        elif 'APACHE' in distro:
            self.cfgs['hadoop_type'] = 'apache'

        if not 'APACHE' in distro:
            self.cfgs['hadoop_home'] = ''
            self.cfgs['hbase_home'] = ''
            self.cfgs['hive_home'] = ''

        self.cfgs['node_count'] = str(len(self.nodes))
        self.cfgs['clustername'] = 'cluster'

        if self.cfgs['hive_authorization'] == 'sentry' and lic.get_edition() == 'ADV':
            self.cfgs['sentry_for_hive'] = 'TRUE'

        # $ENABLE_HA used in dcscheck/dcsstop/dcsbind/dcsunbind script
        if self.cfgs['dcs_ha'].upper() == 'Y':
            self.cfgs['enable_ha'] = 'true'
        else:
            self.cfgs['enable_ha'] = 'false'

        # set default traf home as a symbol link
        self.cfgs['default_traf_home'] = DEF_TRAF_HOME
        # self.cfgs['monitor_comm_port'] = self.monitor_comm_port

        self.cfgs['core_loc'] = self.core_pattern

        gen_template_file(self.template.get_property('trafodion_config_template'), TRAF_CFG_FILE, self.cfgs)

    def deploy_dcsmaster_trafconf(self):
        logger.debug('Modify Dcs master nodes in trafodion config file: [%s]' % TRAF_CFG_FILE)
        change_items = {'DCS_MASTER_NODES.*': 'DCS_MASTER_NODES=%s' % self.cfgs['dcs_master_nodes']}
        mod_file(TRAF_CFG_FILE, change_items)

    def deploy_dcscount_trafconf(self):
        logger.debug(
            'Modify the number of DCS client connections per node in trafodion config file: [%s]' % TRAF_CFG_FILE)
        change_items = {'DCS_CNT_PER_NODE.*': 'DCS_CNT_PER_NODE=%s' % self.cfgs['dcs_cnt_per_node']}
        mod_file(TRAF_CFG_FILE, change_items)

    def deploy_ha_trafconf(self):
        logger.debug('Modify Dcs HA info in trafodion config file: [%s]' % TRAF_CFG_FILE)
        change_items = {'ENABLE_HA.*': 'ENABLE_HA=%s' % self.cfgs['enable_ha'],
                        'DCS_MASTER_FLOATING_IP.*': 'DCS_MASTER_FLOATING_IP=%s' % self.cfgs['dcs_floating_ip'],
                        'KEEPALIVED.*': 'KEEPALIVED=%s' % self.cfgs['dcs_ha_keepalived']
                        }
        mod_file(TRAF_CFG_FILE, change_items)

    def deploy_dbmgr_admin_info(self):
        """ only deploy dbmgr admin user/password in config.xml file """
        logger.debug('Modify admin user and password in DBMgr config file: [%s]' % self.dbmgr_config_file)
        change_items = {
            '"adminUserID">.*': '"adminUserID">%s</entry>' % self.cfgs['db_admin_user'],
            '"adminPassword">.*': '"adminPassword">%s</entry>' % encode_pwd(self.cfgs['db_admin_pwd'])
        }
        mod_file(self.dbmgr_config_file, change_items)

    def deploy_dbmgr_https(self):
        """ only deploy SSL in DBMgr's config.xml file """
        logger.debug('Modify https in DBMgr config file: [%s]' % self.dbmgr_config_file)
        change_items = {
            '"enableHTTPS">.*': '"enableHTTPS">True</entry>',
            '"keyStoreFile">.*': '"keyStoreFile">%s</entry>' % self.keystore_file,
            '"securePassword">.*': '"securePassword">%s</entry>' % encode_pwd(self.cfgs['keystore_pwd']),
            '"trafodionRestServerUri">.*': '"trafodionRestServerUri">%s</entry>' % 'https://%s:%s' % (
            self.localhost, self.rest_https_port)
        }
        mod_file(self.dbmgr_config_file, change_items)

        ### Generate keystore file for REST and DBMGR ###
        logger.info('Generating SSL keystore...')
        run_cmd('export TRAF_AGENT=CM; %s/sql/scripts/sqcertgen gen_keystore %s' % (
        self.cfgs['traf_abspath'], self.cfgs['keystore_pwd']))

    def deploy_dbmgr_ha(self):
        """ only deploy Dcs ha in DBMgr's config.xml file """
        logger.debug('Modify Dcs HA in DBMgr config file: [%s]' % self.dbmgr_config_file)
        dcs_master_host = self.cfgs['dcs_floating_ip'] + ":" + self.dcs_master_port
        change_items = {
            '"jdbcUrl">jdbc:t4jdbc://.*': '"jdbcUrl">jdbc:t4jdbc://%s/:</entry>' % dcs_master_host
        }
        mod_file(self.dbmgr_config_file, change_items)

    def deploy_dbmgr(self):
        """ deploy dbmgr config.xml file """
        dcs_master_host = ''
        logger.debug('Deploy DBMgr config file: [%s]' % self.dbmgr_config_file)

        if self.cfgs['dcs_ha'].upper() == 'Y':
            dcs_master_host = self.cfgs['dcs_floating_ip'] + ":" + self.dcs_master_port
        else:
            dcs_master_node_cnt = len(self.cfgs['dcs_master_nodes'].split(','))
            arr = self.cfgs['dcs_master_nodes'].split(',')
            for index in range(len(arr)):
                if index < dcs_master_node_cnt - 1:
                    dcs_master_host += arr[index] + ":" + self.dcs_master_port + ","
                else:
                    dcs_master_host += arr[index] + ":" + self.dcs_master_port

        with open(self.dbmgr_template_file, 'r') as f:
            template = f.read()

        change_items = {
            'DCS_HOST:DCS_PORT': '%s' % dcs_master_host,
            'DCS_INFO_PORT': self.dcs_info_port,
            'HTTP_PORT': self.dm_http_port,
            'HTTPS_PORT': self.dm_https_port,
            'TIMEZONE_NAME': self.timezone,
        }
        change_items['ADMIN_USERID'] = self.cfgs['db_admin_user']
        change_items['ADMIN_PASSWORD'] = encode_pwd(self.cfgs['db_admin_pwd'])

        if self.cfgs['enable_https'].upper() == 'Y':
            change_items['ENABLE_HTTPS'] = 'True'
            change_items['KEYSTORE_FILE'] = self.keystore_file
            change_items['SECURE_PASSWORD'] = encode_pwd(self.cfgs['keystore_pwd'])
            change_items['REST_HOST'] = 'https://' + self.localhost
            change_items['REST_PORT'] = self.rest_https_port
        else:
            change_items['ENABLE_HTTPS'] = 'False'
            change_items['REST_HOST'] = 'http://' + self.localhost
            change_items['REST_PORT'] = self.rest_http_port

        gen_template_file(template, self.dbmgr_config_file, change_items, doublebrace=False)

    def deploy_mgblty(self):
        """ deploy mgblty related config file """
        logger.debug('Deploy mgblty related config files')
        # edit filebeat.yml
        mod_file(self.filebeat_config, {'"LOGSTASH_HOST:LOGSTASH_PORT"]': '"%s:%s"]' % (self.cfgs['es_virtual_ip'], self.logstash_port)})
        # edit esgyn_exporter.yaml
        mod_file(self.esgyn_exporter_config, {'elasticsearch.url:.*': 'elasticsearch.url: http://%s:%s' % (self.cfgs['es_virtual_ip'], self.elasticsearch_port)})
        append_file(self.filebeat_config, '''  - add_labels:
     labels:
       traf_cluster_id: %s''' % self.cfgs['traf_cluster_id'], 'add_cloud_metadata')
        if self.cfgs['secure_hadoop'].upper() == 'Y':
            realm = re.match('.*@(.*)', self.cfgs['admin_principal']).groups()[0]

    def deploy_ldap(self):
        if (self.cfgs['ldap_security'].upper() == 'Y') or (self.cfgs['ldap_modification'].upper() == 'Y'):
            logger.debug('Deploy LDAP config file: [%s]' % self.ldap_config_file)
            ldap_configs = defaultdict(str)
            for key, value in self.cfgs.iteritems():
                if 'ldap_' in key:
                    ldap_configs[key] = value

            ldap_hostname = ''
            for host in ldap_configs['ldap_hosts'].split(','):
                ldap_hostname += 'LDAPHostName:%s\n' % host

            ldap_configs['ldap_hosts'] = ldap_hostname

            unique_identifier = ''
            for identifier in ldap_configs['ldap_identifiers'].split(';'):
                unique_identifier += 'UniqueIdentifier:%s\n' % identifier

            ldap_configs['ldap_identifiers'] = unique_identifier
            if not ldap_configs['ldap_domain']:
                ldap_configs['ldap_domain'] = 'local'
            gen_template_file(self.template.get_property('ldap_config_template'), self.ldap_config_file, ldap_configs)
        else:
            logger.debug('Remove LDAP config file: [%s]' % self.ldap_config_file)
            run_cmd('rm -f %s' % self.ldap_config_file)

    def deploy_msenv(self):
        logger.debug('Deploy ms.env file: [%s] and genms file: [%s]' % (self.ms_env_file, self.genms_file))
        msenv_configs = self.cfgs['msenv']
        if not msenv_configs: return
        for k, v in msenv_configs.iteritems():
            k = k.upper()  # ms.env settings are using uppercase
            line = '%s=%s' % (k, v)
            if os.path.exists(self.ms_env_file):
                if not append_file(self.ms_env_file, line):
                    mod_file(self.ms_env_file, {'%s=.*' % k: line})
            # ms.env file may not exist before running sqgen, so modify genms file directly
            # and use this file to generate ms.env
            line = 'echo "%s=%s"' % (k, v)
            if not append_file(self.genms_file, line):
                mod_file(self.genms_file, {'echo "%s=.*"' % k: line})

    def deploy_license(self):
        logger.debug('Deploy license file: [%s]' % EDB_LIC_FILE)
        if self.cfgs['license_used'] and self.cfgs['license_used'] != 'NONE':
            write_file(EDB_LIC_FILE, self.cfgs['license_used'])

    def deploy_rest(self):
        logger.debug('Deploy rest site xml: [%s]' % self.rest_site_file)
        p = ParseXML(self.rest_site_file)
        p.add_property('rest.zookeeper.property.clientPort', self.zk_port)
        p.add_property('rest.zookeeper.quorum', self.zk_nodes)
        p.add_property('zookeeper.znode.parent', self.dcs_znode_parent)
        p.add_property('rest.port', self.rest_http_port)

        if self.cfgs['enable_https'].upper() == 'Y':
            p.add_property('rest.https.port', self.rest_https_port)
            p.add_property('rest.keystore', self.keystore_file)
            p.add_property('rest.ssl.password', encode_pwd(self.cfgs['keystore_pwd']))

        # apply user settings to overwrite default ones
        if self.cfgs['restsite']:
            for k, v in self.cfgs['restsite'].iteritems():
                if v == 'default': continue  # will use system detected value
                p.add_property(k, v)
            if self.cfgs['enable_https'].upper() == 'N':
                # clean up rest.ssl.password to disable rest https(TrafodionRest.java)
                p.add_property('rest.ssl.password', '')
        p.write_xml()

    def deploy_dcsmaster(self):
        logger.debug('Deploy dcs master file: [%s]' % self.dcs_master_file)
        dcs_masters = ''
        # modify masters file with additional master nodes
        for dcs_master_node in self.cfgs['dcs_master_nodes'].split(','):
            dcs_masters += '%s\n' % dcs_master_node

        write_file(self.dcs_master_file, dcs_masters)

    def deploy_dcsserver(self):
        logger.debug('Deploy dcs server file: [%s]' % self.dcs_srv_file)
        dcs_servers = ''
        for node in self.nodes:
            dcs_servers += '%s %s\n' % (node.lower(), self.cfgs['dcs_cnt_per_node'])

        write_file(self.dcs_srv_file, dcs_servers)

    def deploy_dcsmds(self):
        logger.debug('Deploy mds file: [%s]' % self.dcs_mds_file)
        esurl = 'esurl: http://%s:%s' % (self.cfgs['es_virtual_ip'], self.elasticsearch_port)

        write_file(self.dcs_mds_file, esurl)

    def deploy_dcssite_ha(self):
        p = ParseXML(self.dcs_site_file)

        p.add_property('dcs.master.floating.ip', 'true')
        p.add_property('dcs.master.floating.ip.external.interface', self.cfgs['dcs_interface'])
        p.add_property('dcs.master.floating.ip.external.ip.address', self.cfgs['dcs_floating_ip'])
        p.add_property('dcs.master.keepalived', self.cfgs['dcs_ha_keepalived'])
        p.rm_property('dcs.dns.interface')
        p.write_xml()

    def deploy_dcssite(self):
        logger.debug('Deploy dcs site xml: [%s]' % self.dcs_site_file)
        if not os.path.exists(self.dcs_site_file):
            with open(self.dcs_site_file, 'w') as f:
                f.write('<configuration></configuration>')

        p = ParseXML(self.dcs_site_file)
        p.add_property('dcs.zookeeper.quorum', self.zk_nodes)
        p.add_property('dcs.zookeeper.property.clientPort', self.zk_port)
        p.add_property('zookeeper.znode.parent', self.dcs_znode_parent)

        net_interface = cmd_output('/sbin/ip route |grep default|awk \'{print $5}\'')
        if not p.get_property('dcs.cloud.command'):
            p.add_property('dcs.dns.interface', net_interface)

        p.add_property('dcs.master.port', self.dcs_master_port)

        if self.cfgs['dcs_ha'].upper() == 'Y':
            p.add_property('dcs.master.floating.ip', 'true')
            p.add_property('dcs.master.floating.ip.external.interface', self.cfgs['dcs_interface'])
            p.add_property('dcs.master.floating.ip.external.ip.address', self.cfgs['dcs_floating_ip'])
            p.add_property('dcs.master.keepalived', self.cfgs['dcs_ha_keepalived'])
            p.rm_property('dcs.dns.interface')

            # set DCS_MASTER_FLOATING_IP ENV for trafci
            dcs_floating_ip_cfg = 'export DCS_MASTER_FLOATING_IP=%s' % self.cfgs['dcs_floating_ip']
            append_file(TRAF_CFG_FILE, dcs_floating_ip_cfg)
        elif self.cfgs['enable_ha'] != 'true':
            p.add_property('dcs.master.floating.ip', 'false')

        if self.cfgs['dcs_mds'].upper() == 'Y':
            p.add_property('dcs.server.user.program.mds.enabled', 'true')
        else:
            p.add_property('dcs.server.user.program.mds.enabled', 'false')

        # set max count of mxosrv
        mxosrv_cnt = int(self.cfgs['dcs_cnt_per_node'])
        try:
            memory_total = cmd_output('cat /proc/meminfo').split('\n')[0].split(':')[1].strip().split()[0]
            memory_total_GB = int(round(float(memory_total) / (1024 * 1024)))
            port_range = memory_total_GB if memory_total_GB >= mxosrv_cnt else mxosrv_cnt
        except IndexError:
            logger.info('Cannot get memory info, set dcs.master.port.range by $DCS_CNT_PER_NODE')
            port_range = mxosrv_cnt
        if port_range > 98:
            p.add_property('dcs.master.port.range', str(port_range + 2))

        # apply user settings to overwrite default ones
        # do nothing if dict is empty
        if self.cfgs['dcssite']:
            for k, v in self.cfgs['dcssite'].iteritems():
                if v == 'default': continue  # will use system detected value
                p.add_property(k, v)
        p.write_xml()

    def deploy_hplsql(self):
        logger.debug('Deploy hplsql site xml: [%s]' % self.hplsql_site_file)
        if not os.path.exists(self.hplsql_site_file):
            with open(self.hplsql_site_file, 'w') as f:
                f.write('<configuration></configuration>')

        dcs_masters = ''
        for dcs_master_node in self.cfgs['dcs_master_nodes'].split(','):
            dcs_masters += '%s:%s,' % (dcs_master_node, self.dcs_master_port)

        # remove the last ','
        dcs_masters = dcs_masters[:dcs_masters.rfind(',')]

        p = ParseXML(self.hplsql_site_file)
        p.add_property('hplsql.conn.init.default', '\nCQD DEFAULT_CHARSET \'UTF8\';\nCQD TRAF_DEFAULT_COL_CHARSET \'UTF8\';\nCQD SP_DEFAULT_NO_TRANSACTION_REQUIRED \'ON\';\n')
        p.add_property('hplsql.use.only.one.connection', 'true')
        p.add_property('hplsql.transaction.compatible', 'true')

        p.write_xml()

    def deploy_msenv_custom(self):
        ms_env = dict()
        if self.cfgs['customized'] == 'CUSTOM_AP':
            ms_env = ParseInI(MSENV_CUSTOM_FILE_AP, 'ms_env_custom').load()
        if self.cfgs['customized'] == 'CUSTOM_TP':
            ms_env = ParseInI(MSENV_CUSTOM_FILE_TP, 'ms_env_custom').load()
            
        if self.cfgs['enhance_ha'] == 'Y':
            ha_ms_env = ParseInI(MSENV_CUSTOM_FILE_AP, 'ha_ms_env_custom').load()
            ms_env.update(ha_ms_env)


        ms_env['STFS_HDD_LOCATION'] = ':'.join(self.cfgs['scratch_locs'].split(','))
        with open('%s/ms.env.custom' % TRAF_CFG_DIR, 'r') as f:
            lines = f.read()

        for key, value in ms_env.items():
            if not re.match(r'^([A-Z]+)(?:_[A-Z0-9]+)+$', key):
                continue

            if key in lines:
                lines = re.sub('%s=.*' % key, '%s=%s' % (key, value), lines)
                continue

            lines += '\n%s=%s' % (key, value)

        with open('%s/ms.env.custom' % TRAF_CFG_DIR, 'w') as f:
            f.write(lines)

    def deploy_auth_type(self):
        logger.info("Deploy type of authentication")
        logger.info("type of trafodion authentication: is " + self.cfgs["TRAFODION_AUTHENTICATION_TYPE"])
        #don't use append_file,will be lost "\n" in head of string
        lines = ""
        password_policy_string = ""
        if self.cfgs.has_key("TRAFODION_PASSWORD_CHECK_SYNTAX"):
            password_policy_string = self.cfgs["TRAFODION_PASSWORD_CHECK_SYNTAX"]
        #update genms
        with open(self.genms_file, 'r') as f:
            for line in f:
                if re.search(r"^echo \"TRAFODION_AUTHENTICATION_TYPE.*\"$", line) == None:
                    if password_policy_string != "":
                        if re.search(r"^echo \"TRAFODION_PASSWORD_CHECK_SYNTAX.*\"$", line) == None:
                            lines += line
                    else:
                        lines += line
            if lines[-1] != '\n':
                #new line
                lines += '\n'
            lines += "echo \"TRAFODION_AUTHENTICATION_TYPE=%s\"\n" % self.cfgs["TRAFODION_AUTHENTICATION_TYPE"]
            if password_policy_string != "":
                lines += "echo \"TRAFODION_PASSWORD_CHECK_SYNTAX=%s\"\n" % password_policy_string
            write_file(self.genms_file, lines)
        #update ms.env
        lines = ""
        msenv = "%s/ms.env" % self.cfgs["traf_var"]
        if os.path.exists(msenv):
            with open(msenv, 'r') as f:
               for line in f:
                   if re.search(r"^TRAFODION_AUTHENTICATION_TYPE\s*=.*", line) == None:
                       if password_policy_string != "":
                           if re.search(r"^TRAFODION_PASSWORD_CHECK_SYNTAX\s*=.*", line) == None:
                               lines += line
                       else:
                           lines += line
               if lines[-1] != '\n':
                   #new line
                   lines += '\n'
               lines += "TRAFODION_AUTHENTICATION_TYPE=%s\n" % self.cfgs["TRAFODION_AUTHENTICATION_TYPE"]
               if password_policy_string != "":
                   lines += "TRAFODION_PASSWORD_CHECK_SYNTAX=%s\n" % password_policy_string
            write_file(msenv, lines)
        mod_file(TRAF_CFG_FILE, {r"export TRAFODION_AUTHENTICATION_TYPE=.*": "export TRAFODION_AUTHENTICATION_TYPE=%s" %
                                 self.cfgs["TRAFODION_AUTHENTICATION_TYPE"]})
        if password_policy_string != "":
            mod_file(TRAF_CFG_FILE, {r"export TRAFODION_PASSWORD_CHECK_SYNTAX=.*": "export TRAFODION_PASSWORD_CHECK_SYNTAX=%s" %
                                     password_policy_string})

    def deploy_lock(self):
        logger.info('Deploy row lock related config files')
        # mod ms.env.custom
        lock_switch = 0
        if self.cfgs["use_lock"].upper() == 'Y':
            lock_switch = 1
        else:
            lock_switch = 0
        new_config_line = "ENABLE_ROW_LEVEL_LOCK=%d\n" % lock_switch
        # don't use append_file,will be lost "\n" in head of string
        lines = ""
        with open(self.ms_env_custom_file, 'r') as f:
            for line in f:
                if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                    lines += line
            if lines[-1] != "\n":
                #new line
                lines += "\n"
            lines += new_config_line
        write_file(self.ms_env_custom_file, lines)

    def deploy_ha_recovery(self):
         # trafodion-site.xml
        logger.info('Modify %s/trafodion-site.xml' % TRAF_CFG_DIR)
        traf_site = ParseXML('%s/trafodion-site.xml' % TRAF_CFG_DIR)
        traf_site_infos = {'hbase.status.published': 'true', 
                           'hbase.status.listener.class': 'org.apache.hadoop.hbase.client.ClusterStatusListener$ZKClientListener'}
        for k, v in traf_site_infos.items():
            traf_site.add_property(k, v)
        traf_site.write_xml()

        # monitor.env
        # append_file('%s/monitor.env' % TRAF_CFG_DIR, 'SQ_MON_EPOLL_WAIT_TIMEOUT=2')

        # tmstart
        append_file('%s/sql/scripts/tmstart' % self.cfgs['traf_abspath'], 'set DTM_TRANS_HUNG_RETRY_INTERVAL=20000', 'set TRAF_TM_LOCKED')

    @staticmethod
    def deploy_traf_site():
        traf_site = ParseXML('%s/trafodion-site.xml' % TRAF_CFG_DIR)
        traf_site.add_property('backup.retention.period', '1')
        traf_site.write_xml()

    def deploy_sqenvcom(self):
        sqenvcom = '%s/sqenvcom.sh' % self.cfgs['traf_abspath']
        if self.cfgs['enhance_ha'] == 'Y':
            mod_file(sqenvcom, {'export SQ_MON_ZCLIENT_SESSION_TIMEOUT=.*': 'export SQ_MON_ZCLIENT_SESSION_TIMEOUT=39',
                                'export SQ_MON_ZCLIENT_MY_ZNODE_PING=.*': 'export SQ_MON_ZCLIENT_MY_ZNODE_PING=2',
                                'export SQ_MON_ZCLIENT_MY_ZNODE_DELAY=.*': 'export SQ_MON_ZCLIENT_MY_ZNODE_DELAY=7'})

        append_file(sqenvcom, 'export PATH=${TRAF_HOME}/../omclient:${PATH}')
