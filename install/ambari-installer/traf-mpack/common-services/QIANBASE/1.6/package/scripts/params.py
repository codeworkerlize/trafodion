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
from resource_management import *
from tempfile import TemporaryFile

# config object that holds the configurations declared in the config xml file
config = Script.get_config()

java_home = config['hostLevelParams']['java_home']
java_version = int(config['hostLevelParams']['java_version'])

cluster_name = str(config['clusterName'])
chost = config["hostname"]
monitor_comm_port = config['configurations']['trafodion-env']['traf.monitor.comm.port']

dcs_servers = config['configurations']['dcs-env']['dcs.servers']
dcs_master_port = config['configurations']['dcs-site']['dcs.master.port']
dcs_info_port = config['configurations']['dcs-site']['dcs.master.info.port']
dcs_enable_ha = str(config['configurations']['dcs-site']['dcs.master.floating.ip']).lower()
dcs_keepalived = str(config['configurations']['dcs-site']['dcs.master.keepalived']).lower()
dcs_enable_mds = str(config['configurations']['dcs-site']['dcs.server.user.program.mds.enabled']).lower()
dcs_floating_ip = config['configurations']['dcs-site']['dcs.master.floating.ip.external.ip.address']
dcs_floating_interface = config['configurations']['dcs-site']['dcs.master.floating.ip.external.interface']
dcs_mast_node_list = default("/clusterHostInfo/qianbase_dcs_hosts", '')
dcs_env_template = config['configurations']['dcs-env']['content']
dcs_log4j_template = config['configurations']['dcs-log4j']['content']
rest_port = config['configurations']['rest-site']['rest.port']
rest_https_port = config['configurations']['rest-site']['rest.https.port']
if dcs_enable_ha == 'true':
  restnode = dcs_floating_ip
  dcsquorum = dcs_floating_ip + ":" + str(dcs_master_port)
else:
  restnode = dcs_mast_node_list[0]
  dcsquorum=""
  for dhost in dcs_mast_node_list:
    dcsquorum = dcsquorum + dhost + ":" + str(dcs_master_port) + ","
  dcsquorum=dcsquorum.rstrip(',')
dcs_master_nodes=""
for dhost in dcs_mast_node_list:
  dcs_master_nodes = dcs_master_nodes + dhost + ","
dcs_master_nodes=dcs_master_nodes.rstrip(',')
dcs_aws_access_id = config['configurations']['dcs-env']['dcs.aws.access.id']
dcs_aws_secret_key = config['configurations']['dcs-env']['dcs.aws.secret.key']
dcs_aws_region = config['configurations']['dcs-env']['dcs.aws.region']
dcs_aws_cred_template = config['configurations']['dcs-env']['aws_cred_content']
dcs_aws_config_template = config['configurations']['dcs-env']['aws_config_content']


zookeeper_quorum_hosts = ",".join(config['clusterHostInfo']['zookeeper_hosts'])
if 'zoo.cfg' in config['configurations'] and 'clientPort' in config['configurations']['zoo.cfg']:
  zookeeper_clientPort = config['configurations']['zoo.cfg']['clientPort']
else:
  zookeeper_clientPort = '2181'

traf_user = 'trafodion'
traf_group = 'trafodion'
traf_user_home = '/home/' + traf_user
# source first time as trafodion user, in case of TRAF_VAR creation
Execute("source ~/.bashrc",user=traf_user)

# second time as root to get output, without extraneous output
cmd = "source ~" + traf_user + "/.bashrc 2>/dev/null; echo $TRAF_HOME; echo $SQ_MBTYPE"
ofile = TemporaryFile()
Execute(cmd,stdout=ofile,
       environment={'JAVA_HOME': java_home,'HADOOP_TYPE': "hortonworks"})
ofile.seek(0) # read from beginning
lines = ofile.read().split('\n')
traf_home = lines[0]
sq_mbtype = lines[1]
ofile.close()

esgyndb_license_key = config['configurations']['trafodion-env']['esgyndb_license_key']
traf_db_admin = config['configurations']['trafodion-env']['traf.db.admin']
traf_db_root = config['configurations']['trafodion-env']['traf.db.root']
traf_instance_id = '1'
traf_cluster_id = config['configurations']['trafodion-env']['traf.cluster.id']

traf_cpu_limit = config['configurations']['trafodion-env']['traf.limit.cpu']
traf_mem_limit = config['configurations']['trafodion-env']['traf.limit.mem']

om_ha_enable = str(config['configurations']['trafodion-env']['om.ha.enabled']).lower()
om_virtual_ip = config['configurations']['trafodion-env']['om.virtual.ip']
if om_ha_enable == 'true':
    elasticsearch_url = om_virtual_ip.strip() + ':30002'
    logstash_url = om_virtual_ip.strip() + ':30003'
else:
    elasticsearch_url = config['configurations']['trafodion-env']['elasticsearch.url']
    logstash_url = config['configurations']['trafodion-env']['logstash.url']

traf_conf_dir = '/etc/trafodion/conf' # path is hard-coded in /etc/trafodion/trafodion_config
traf_env_template = config['configurations']['trafodion-env']['content']
traf_clust_template = config['configurations']['traf-cluster-env']['content']
traf_client_template = config['configurations']['clientconf']['content']
traf_oinst_template = config['configurations']['clientodbc']['content']
traf_site_conf = config['configurations']['trafodion-site']

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
user_group = config['configurations']['cluster-env']['user_group']
hbase_user = config['configurations']['hbase-env']['hbase_user']
hbase_staging = config['configurations']['hbase-site']['hbase.bulkload.staging.dir']
hbase_hbmi_port = config['configurations']['hbase-site']['hbase.master.info.port']
hbase_hbri_port = config['configurations']['hbase-site']['hbase.regionserver.info.port']


traf_node_list = default("/clusterHostInfo/qianbase_node_hosts", '')
traf_node_list.sort()
traf_scaling_factor = config['configurations']['trafodion-env']['traf.scaling.factor']

traf_scratch = config['configurations']['trafodion-env']['traf.node.dir']
traf_logdir = config['configurations']['trafodion-env']['traf.log.dir']
traf_vardir = config['configurations']['trafodion-env']['traf.var.dir']

traf_ldap_template = config['configurations']['trafodion-env']['ldap_content']
if traf_ldap_template.count('SECTION:') - traf_ldap_template.count('#SECTION:') > 2:
  ldapvers='v2'
else:
  ldapvers='v1'
if config['configurations']['trafodion-env']['traf.ldap.enabled']:
    traf_ldap_enabled = 'YES'
else:
    traf_ldap_enabled = 'NO'
traf_db_admin_pwd = config['configurations']['trafodion-env']['traf.db.admin.pwd']
ldap_domain = config['configurations']['trafodion-env']['traf.ldap.domain']
ldap_hosts = ''
for host in config['configurations']['trafodion-env']['traf.ldap.hosts'].split(','):
  ldap_hosts += '  LDAPHostName: %s\n' % host
ldap_port = config['configurations']['trafodion-env']['traf.ldap.port']
ldap_identifiers = ''
for identifier in config['configurations']['trafodion-env']['traf.ldap.identifiers'].split(';'):
  ldap_identifiers += '  UniqueIdentifier: %s\n' % identifier
ldap_search_id = config['configurations']['trafodion-env']['traf.ldap.search.id']
ldap_pwd = config['configurations']['trafodion-env']['traf.ldap.pwd']
ldap_search_group_base = config['configurations']['trafodion-env']['traf.ldap.search.group.base']
ldap_search_group_object_class = config['configurations']['trafodion-env']['traf.ldap.search.group.object.class']
ldap_search_group_member_attribute = config['configurations']['trafodion-env']['traf.ldap.search.group.member.attribute']
ldap_search_group_name_attribute = config['configurations']['trafodion-env']['traf.ldap.search.group.name.attribute']
ldap_encrypt = config['configurations']['trafodion-env']['traf.ldap.encrypt']
ldap_certpath = config['configurations']['trafodion-env']['traf.ldap.certpath']



# DBMgr
traf_dbm_admpw = config['configurations']['dbm-env']['esgyndb_admin_password']
traf_dbm_http_port = config['configurations']['dbm-env']['dbmgr.http.port']
traf_dbm_https_port = config['configurations']['dbm-env']['dbmgr.https.port']
traf_dbm_keystore_pwd = config['configurations']['dbm-env']['keystore_pwd']
traf_dbm_enable_https = config['configurations']['dbm-env']['enable_https']
 
#HDFS Dir creation
hostname = config["hostname"]
hadoop_conf_dir = "/etc/hadoop/conf"
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
hdfs_principal = config['configurations']['hadoop-env']['hdfs_principal_name']
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = functions.get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
hbase_rpc_protection = config['configurations']['hbase-site']['hbase.rpc.protection']
if security_enabled:
  hbase_rs_principal = config['configurations']['hbase-site']['hbase.regionserver.kerberos.principal']
  hive_principal = config['configurations']['hive-site']['hive.server2.authentication.kerberos.principal']
else:
  hbase_rs_principal = "None"
  hive_principal = "None"

# calculate Hive URL as hive params does
hive_server_hosts = default("/clusterHostInfo/hive_server_host", [])
hive_server_host = hive_server_hosts[0] if len(hive_server_hosts) > 0 else None
hive_transport_mode = config['configurations']['hive-site']['hive.server2.transport.mode']
if hive_transport_mode.lower() == "http":
  hive_server_port = config['configurations']['hive-site']['hive.server2.thrift.http.port']
else:
  hive_server_port = default('/configurations/hive-site/hive.server2.thrift.port',"10000")
hive_url = format("{hive_server_host}:{hive_server_port}")
hive_ssl = config['configurations']['trafodion-env']['traf.hive.ssl']

traf_security_enabled=config['configurations']['trafodion-env']['traf.security.enabled']
traf_principal=config['configurations']['trafodion-env']['traf.principal'].replace('_HOST', chost.lower())
traf_keytab=config['configurations']['trafodion-env']['traf.keytab']
kinit_cmd = format("{kinit_path_local} -kt {traf_keytab} {traf_principal};")

from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")

import functools
#create partial functions with common arguments for every HdfsDirectory call
#to create hdfs directory we need to call params.HdfsDirectory in code
HdfsDirectory = functools.partial(
  HdfsResource,
  type="directory",
  hadoop_conf_dir=hadoop_conf_dir,
  user=hdfs_user,
  hdfs_site=hdfs_site,
  default_fs=default_fs,
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  principal_name = hdfs_principal,
  kinit_path_local = kinit_path_local
)
HdfsFile = functools.partial(
  HdfsResource,
  type="file",
  hadoop_conf_dir=hadoop_conf_dir,
  user=traf_user,
  hdfs_site=hdfs_site,
  default_fs=default_fs,
  security_enabled = security_enabled,
  keytab = traf_keytab,
  principal_name = traf_principal,
  kinit_path_local = kinit_path_local
)
