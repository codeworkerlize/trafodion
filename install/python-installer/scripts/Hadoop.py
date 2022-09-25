#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import time
import socket
import logging
from constants import *
from common import ParseXML, run_cmd, get_sudo_prefix, cmd_output, gen_template_file, write_file, mod_file, \
                   run_cmd_as_user, append_file

logger = logging.getLogger()


def install_cdh(service, dbcfgs):
    service_map = {
        'zookeeper': DeployZK(dbcfgs, service),
        'hadoop': DeployHadoop(dbcfgs, service),
        'yarn': DeployYarn(dbcfgs, service),
        'hbase': DeployHBase(dbcfgs, service),
        'hive': DeployHive(dbcfgs, service)
    }
    return service_map.get(service)


def create_hadoop_user(hadoop_user, home_dir):
    hadoop_group = hadoop_user
    user_exist = cmd_output('getent passwd %s' % hadoop_user)

    if user_exist:
        logger.info('Hadoop user already exists, skip creating Hadoop user')
    else:
        logger.info('Creating Hadoop user and group ...')
        if not cmd_output('getent group %s' % hadoop_group):
            run_cmd('/usr/sbin/groupadd %s' % hadoop_group)

        useradd_cmd_ptr = '/usr/sbin/useradd --shell /bin/bash -r -m %s -g %s --home %s --password ' % (hadoop_user, hadoop_group, home_dir)
        run_cmd('%s "$(openssl passwd %s)"' % (useradd_cmd_ptr, hadoop_user))


class Roles(object):
    def __init__(self, host_list):
        self.host_list = host_list
        self.hostname = socket.gethostbyaddr(socket.gethostname())[0]

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            if self.hostname in self.host_list:
                func(*args, **kwargs)
        return wrapper


class Deploy(object):
    def __init__(self, client_configs, service):
        self.cfgs = client_configs
        self.hadoop_ha = self.cfgs['hadoop_ha']
        self.hostname = socket.gethostbyaddr(socket.gethostname())[0].lower()
        self.service = service
        self.home = '/opt/' + self.service
        self.user = self.group = 'hadoop'
        self.sudo = get_sudo_prefix()
        self.user_exist = cmd_output('getent passwd %s' % self.user)
        self.log_dir = '/var/log/' + self.service

    # add a user and group
    def _add_user(self):
        if self.user_exist:
            logger.info('%s user already exists, skip creating %s user' % (self.user, self.user))
        else:
            logger.info('Creating %s user and group ...' % self.user)
            if not cmd_output('getent group %s' % self.group):
                run_cmd('/usr/sbin/groupadd %s' % self.group)

            shell = '/bin/bash' if self.user == 'hadoop' else '/bin/false'
            useradd_cmd_ptr = '/usr/sbin/useradd --shell %s -r -m %s -g %s --home %s' % (shell, self.user, self.group, self.home)
            run_cmd(useradd_cmd_ptr)

        if not os.path.exists(self.home):
            run_cmd('mkdir -p %s' % self.home)
        run_cmd('chmod 755 %s' % self.home)

    def _untar_files(self):
        tar_name = glob.glob('%s/%s-?.?.?-*' % (TMP_HADOOP_DIR, self.service))[0]
        if not os.path.exists(self.home):
            run_cmd('mkdir -p %s' % self.home)
        run_cmd('chmod 755 %s' % self.home)
        run_cmd('chown %s:%s %s' % (self.user, self.group, self.home))

        logger.info('Extracting %s binary package ...' % self.service)
        tar_cmd = 'tar xzf %s -C %s --strip-components 1' % (tar_name, self.home)
        run_cmd(tar_cmd)

    def _config_profile(self, env_name, env_value):
        logger.info("Set env value for %s in /etc/profile ..." % self.service)
        result = cmd_output("nl -b a /etc/profile | grep '%s' | awk '{print $1}'" % ('export ' + env_name))
        if not result:
            run_cmd("sed -i '$i export %s=%s' /etc/profile" % (env_name, env_value))
            run_cmd("sed -i '$i export PATH=$%s/bin:$PATH' /etc/profile" % env_name)
            run_cmd("source /etc/profile")

    def _create_log_dir(self):
        run_cmd('mkdir -p %s' % self.log_dir)
        run_cmd('chown -R %s:%s %s' % (self.user, self.group, self.log_dir))

    @staticmethod
    def _set_property(cfg_file, prop, value):
        result = cmd_output("nl -b a %s | grep '%s' | awk '{print $1}'" % (cfg_file, prop))
        if not result:
            run_cmd("sed -i '$i %s' %s" % (prop + value, cfg_file))
        else:
            num = int(result)
            run_cmd("sed -i '%dc %s' %s" % (num, prop + value, cfg_file))

    def _setup_permission(self, cfg_dir):
        run_cmd('chown -R %s:%s %s' % (self.user, self.group, self.home))
        run_cmd('chmod -R g+w %s' % cfg_dir)


class DeployZK(Deploy):
    def __init__(self, client_configs, service):
        super(DeployZK, self).__init__(client_configs, service)
        self.zk_hosts = self.cfgs['zk_hosts'].split(',')

    def __config(self):
        cfg_dir = '%s/conf' % self.home
        zoo_cfg = '%s/zoo.cfg' % cfg_dir
        zoo_env = '%s/zookeeper-env.sh' % cfg_dir
        data_dir = '/var/lib/zookeeper'

        # generate zoo.cfg
        zoo_cfg_temp = ParseXML(ZOO_CFG_TEMPLATE_XML).get_property('zoo_cfg_template')
        write_file(zoo_cfg, zoo_cfg_temp)
        count = 0
        for host in self.zk_hosts:
            count = count + 1
            self._set_property('%s/zoo.cfg' % cfg_dir, 'server.%d=' % count, '%s:2888:3888' % host)
        run_cmd('chmod 644 %s' % zoo_cfg)

        if self.hostname in self.zk_hosts:
            rs = cmd_output('grep \'%s\' %s' % (self.hostname + ':2888:3888', zoo_cfg))
            myid = int(rs.split('=')[0].split('.')[1])

            run_cmd('rm -rf %s' % data_dir)
            run_cmd('mkdir -p %s' % data_dir)
            run_cmd('echo %d > %s/myid' % (myid, data_dir))
            run_cmd('chown -R %s:%s %s' % (self.user, self.group, data_dir))

            # create log dir
            run_cmd('echo "export ZOO_LOG_DIR=%s" > %s' % (self.log_dir, zoo_env))
            run_cmd('chmod 755 %s' % zoo_env)
            self._create_log_dir()

        self._setup_permission(cfg_dir)

    def install(self):
        self._untar_files()
        self._config_profile('ZOOKEEPER_HOME', self.home)
        self.__config()

    def start(self):
        if self.hostname in self.zk_hosts:
            run_cmd_as_user(self.user, '%s/bin/zkServer.sh start' % self.home)

    def stop(self):
        if self.hostname in self.zk_hosts:
            run_cmd_as_user(self.user, '%s/bin/zkServer.sh stop' % self.home)


class DeployHadoop(Deploy):
    def __init__(self, client_configs, service):
        super(DeployHadoop, self).__init__(client_configs, service)
        self.hadoop_hosts = self.cfgs['hadoop_hosts'].split(',')
        self.hadoop_master = self.cfgs['nn_host'].split(',')
        self.hadoop_snn = self.cfgs['snn_host'].split(',')
        self.hadoop_dn = self.cfgs['dn_hosts'].split(',')
        self.hadoop_jn = self.cfgs['jn_hosts'].split(',')
        self.zk_hosts = self.cfgs['zk_hosts'].split(',')

        self.format_nn = self.cfgs['format_nn'].upper()
        self.dfs_locs = self.cfgs['dfs_locs'].split(',')

    def __config(self):
        java_home = self.cfgs['java_home']
        cfg_dir = '%s/etc/hadoop' % self.home
        core_site = cfg_dir + '/core-site.xml'
        hdfs_site = cfg_dir + '/hdfs-site.xml'
        hadoop_env = cfg_dir + '/hadoop-env.sh'
        slaves_cfg = cfg_dir + '/slaves'

        # hadoop-env.sh
        self._set_property(hadoop_env, 'export JAVA_HOME=', java_home)
        self._set_property(hadoop_env, 'export HADOOP_CONF_DIR=', cfg_dir)
        self._set_property(hadoop_env, 'export HADOOP_LOG_DIR=', self.log_dir)

        # core-site.xml
        core_site_temp = ParseXML(core_site)
        if self.hadoop_ha == 'Y':
            core_site_temp.add_property('fs.defaultFS', 'hdfs://esgyn')
            core_site_temp.add_property('dfs.journalnode.edits.dir', ','.join(['%s/jn' % dfs_loc for dfs_loc in self.dfs_locs]))
        elif self.hadoop_ha == 'N':
            core_site_temp.add_property('fs.defaultFS', 'hdfs://%s:8020' % self.cfgs['nn_host'])
        core_site_temp.add_property('hadoop.security.authentication', 'simple')
        core_site_temp.add_property('hadoop.security.authorization', 'false')
        core_site_temp.add_property('hadoop.rpc.protection', 'authentication')
        core_site_temp.add_property('hadoop.ssl.enabled', 'false')
        core_site_temp.add_property('hadoop.ssl.require.client.cert', 'false')
        core_site_temp.add_property('hadoop.http.logs.enabled', 'true')
        core_site_temp.write_xml()

        # hdfs-site.xml
        hdfs_site_temp = ParseXML(HDFS_SITE_TEMPLATE_XML)
        if self.hadoop_ha == 'Y':
            hdfs_site_temp.add_property('dfs.nameservices', 'esgyn')
            hdfs_site_temp.add_property('dfs.ha.namenodes.esgyn', 'nn0,nn1')
            hdfs_site_temp.add_property('ha.zookeeper.quorum', ','.join(['%s:2181' % zk for zk in self.zk_hosts]))
            hdfs_site_temp.add_property('dfs.namenode.rpc-address.esgyn.nn0', '%s:8020' % self.hadoop_master[0])
            hdfs_site_temp.add_property('dfs.namenode.rpc-address.esgyn.nn1', '%s:8020' % self.hadoop_master[1])
            hdfs_site_temp.add_property('dfs.namenode.http-address.esgyn.nn0', '%s:50070' % self.hadoop_master[0])
            hdfs_site_temp.add_property('dfs.namenode.http-address.esgyn.nn1', '%s:50070' % self.hadoop_master[1])
            hdfs_site_temp.add_property('dfs.namenode.shared.edits.dir', 'qjournal://%s/esgyn' % ';'.join(['%s:8485' % jn for jn in self.hadoop_jn]))
            hdfs_site_temp.add_property('dfs.ha.fencing.methods', 'shell(/bin/true)')
            hdfs_site_temp.add_property('dfs.journalnode.edits.dir', ','.join(['%s/jn' % dfs_loc for dfs_loc in self.dfs_locs]))
            hdfs_site_temp.add_property('dfs.client.failover.proxy.provider.esgyn', 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider')
            hdfs_site_temp.add_property('dfs.ha.automatic-failover.enabled.esgyn', 'true')
            hdfs_site_temp.add_property('ipc.client.rpc-timeout.ms', '60000')
        elif self.hadoop_ha == 'N':
            hdfs_site_temp.add_property('dfs.namenode.servicerpc-address', '%s:8022' % self.hadoop_master[0])
            hdfs_site_temp.add_property('dfs.namenode.http-address', '%s:50070' % self.hadoop_master[0])
            hdfs_site_temp.add_property('dfs.namenode.secondary.http-address', '%s:50090' % self.hadoop_snn[0])
            hdfs_site_temp.add_property('dfs.namenode.secondary.https-address', '%s:50495' % self.hadoop_snn[0])
            hdfs_site_temp.add_property('dfs.secondary.https.port', '50495')
        hdfs_site_temp.add_property('dfs.namenode.name.dir', ','.join(['%s/nn' % dfs_loc for dfs_loc in self.dfs_locs]))
        hdfs_site_temp.add_property('dfs.datanode.data.dir', ','.join(['%s/dn' % dfs_loc for dfs_loc in self.dfs_locs]))
        dfs_replication = '3' if len(self.hadoop_dn) >= 3 else '1'
        hdfs_site_temp.add_property('dfs.replication', dfs_replication)

        hdfs_site_temp.write_xml()
        run_cmd('rm -f %s && cp %s %s' % (hdfs_site, HDFS_SITE_TEMPLATE_XML, hdfs_site))

        # slavs
        slaves = '\n'.join(self.cfgs['dn_hosts'].split(','))
        write_file(slaves_cfg, slaves)

        write_file('%s/hadoop-topology.sh' % cfg_dir, ParseXML(TEMPLATE_XML).get_property('hadoop_topology_template'))
        run_cmd('chmod 750 %s/hadoop-topology.sh' % cfg_dir)
        topology_content = ''
        for node in self.hadoop_hosts:
            host_list = socket.gethostbyaddr(node)
            topology_content += '%s %s /default-rack\n' % (host_list[2][0], host_list[0].lower())
        write_file('%s/hadoop-topology' % cfg_dir, topology_content)
        run_cmd('chmod 644 %s/hadoop-topology' % cfg_dir)

        # create log dir
        self._create_log_dir()
        self._setup_permission(cfg_dir)

        # create dn, nn and jn dir
        if self.hostname in self.hadoop_master:
            for dfs_loc in self.dfs_locs:
                run_cmd('mkdir -p %s/nn' % dfs_loc)
                run_cmd('chmod 700 %s/nn' % dfs_loc)
                run_cmd('chown %s:%s %s/nn' % (self.user, self.group, dfs_loc))

            # format NameNode
            if self.hadoop_ha == 'Y' and self.format_nn == 'Y':
                if self.hostname in self.hadoop_master[0]:
                    run_cmd_as_user(self.user, '%s/bin/hdfs namenode -format -force -nonInteractive esgyn' % self.home)
                    run_cmd_as_user(self.user, '%s/bin/hdfs zkfc -formatZK -force -nonInteractive' % self.home)
                else:
                    run_cmd_as_user(self.user, '%s/bin/hdfs namenode -bootstrapStandby -nonInteractive' % self.home)
            elif self.hadoop_ha == 'N' and self.format_nn == 'Y':
                run_cmd_as_user(self.user, '%s/bin/hdfs namenode -format -force -nonInteractive' % self.home)

        if self.hostname in self.hadoop_dn:
            for dfs_loc in self.dfs_locs:
                if self.format_nn == 'Y' and os.path.exists(dfs_loc + '/dn'):
                    run_cmd('rm -rf %s/dn' % dfs_loc)
                run_cmd('mkdir -p %s/dn' % dfs_loc)
                run_cmd('chmod 700 %s/dn' % dfs_loc)
                run_cmd('chown %s:%s %s/dn' % (self.user, self.group, dfs_loc))

            run_cmd('mkdir -p /var/run/hdfs-sockets')
            run_cmd('chown %s:%s /var/run/hdfs-sockets' % (self.user, self.group))

        if self.hostname in self.hadoop_jn:
            for dfs_loc in self.dfs_locs:
                run_cmd('mkdir -p %s/jn' % dfs_loc)
                run_cmd('chown %s:%s %s/jn' % (self.user, self.group, dfs_loc))

    def install(self):
        if self.hostname in self.hadoop_hosts:
            self._untar_files()
            self._config_profile('HADOOP_HOME', self.home)
            self.__config()

    def start(self):
        if self.hostname in self.hadoop_master[0]:
            hdfs_bin = self.home + '/bin/hdfs'
            run_cmd_as_user(self.user, '%s/sbin/start-dfs.sh; sleep 3' % self.home)
            run_cmd_as_user(self.user, '%s dfsadmin -safemode wait' % hdfs_bin)
            run_cmd_as_user(self.user, '%s dfs -mkdir -p /user/history' % hdfs_bin)
            run_cmd_as_user(self.user, '%s dfs -chown -R %s:%s /user/history' % (hdfs_bin, self.user, self.group))
            run_cmd_as_user(self.user, '%s dfs -chmod 0777 /user/history' % hdfs_bin)

    def stop(self):
        if self.hostname in self.hadoop_master[0]:
            run_cmd_as_user(self.user, '%s/sbin/stop-dfs.sh' % self.home)
        else:
            time.sleep(15)


class DeployYarn(Deploy):
    def __init__(self, client_configs, service):
        super(DeployYarn, self).__init__(client_configs, service)
        self.home = self.cfgs['hadoop_home']
        self.rm_host = self.cfgs['rm_host'].split(',')
        self.nm_hosts = self.cfgs['nm_hosts'].split(',')
        self.jhs_host = self.cfgs['jhs_host'].split(',')
        self.hadoop_hosts = self.cfgs['hadoop_hosts'].split(',')
        self.zk_hosts = self.cfgs['zk_hosts'].split(',')
        self.yarn_ha = True if len(self.rm_host) > 1 else False

    def __config(self):
        java_home = self.cfgs['java_home']
        cfg_dir = '%s/etc/hadoop' % self.home
        yarn_env = '%s/yarn-env.sh' % cfg_dir
        yarn_site = '%s/yarn-site.xml' % cfg_dir
        mapred_env = '%s/mapred-env.sh' % cfg_dir
        mapred_site = '%s/mapred-site.xml' % cfg_dir

        # yarn-env.sh
        self._set_property(yarn_env, 'export JAVA_HOME=', java_home)
        self._set_property(yarn_env, 'export HADOOP_YARN_USER=', self.user)
        self._set_property(yarn_env, 'export YARN_CONF_DIR=', cfg_dir)
        append_file(yarn_env, 'export YARN_LOG_DIR=%s' % self.log_dir, '# default log directory & file')

        # yarn-site.xml
        if self.yarn_ha:
            yarn_ha_site_temp = ParseXML(YARN_HA_SITE_TEMPLATE_XML)
            yarn_ha_site_temp.add_property('yarn.resourcemanager.zk-address', ','.join(['%s:2181' % zk for zk in self.zk_hosts]))
            yarn_ha_site_temp.add_property('yarn.resourcemanager.scheduler.address.rm1', '%s:8030' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.scheduler.address.rm2', '%s:8030' % self.rm_host[1])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.resource-tracker.address.rm1', '%s:8031' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.resource-tracker.address.rm2', '%s:8031' % self.rm_host[1])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.address.rm1', '%s:8032' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.address.rm2', '%s:8032' % self.rm_host[1])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.admin.address.rm1', '%s:8033' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.admin.address.rm2', '%s:8033' % self.rm_host[1])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.webapp.address.rm1', '%s:8088' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.webapp.address.rm2', '%s:8088' % self.rm_host[1])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.webapp.https.address.rm1', '%s:8090' % self.rm_host[0])
            yarn_ha_site_temp.add_property('yarn.resourcemanager.webapp.https.address.rm2', '%s:8090' % self.rm_host[1])
            yarn_ha_site_temp.add_property('ipc.client.rpc-timeout.ms','60000')
            yarn_ha_site_temp.write_xml()
            run_cmd('rm -f %s && cp %s %s' % (yarn_site, YARN_HA_SITE_TEMPLATE_XML, yarn_site))
        else:
            yarn_site_temp = ParseXML(YARN_SITE_TEMPLATE_XML)
            yarn_site_temp.add_property('yarn.resourcemanager.address', '%s:8032' % self.rm_host[0])
            yarn_site_temp.add_property('yarn.resourcemanager.admin.address', '%s:8033' % self.rm_host[0])
            yarn_site_temp.add_property('yarn.resourcemanager.scheduler.address', '%s:8030' % self.rm_host[0])
            yarn_site_temp.add_property('yarn.resourcemanager.resource-tracker.address', '%s:8031' % self.rm_host[0])
            yarn_site_temp.add_property('yarn.resourcemanager.webapp.address', '%s:8088' % self.rm_host[0])
            yarn_site_temp.add_property('yarn.resourcemanager.webapp.https.address', '%s:8090' % self.rm_host[0])
            yarn_site_temp.write_xml()
            run_cmd('rm -f %s && cp %s %s' % (yarn_site, YARN_SITE_TEMPLATE_XML, yarn_site))

        # mapred-env.sh
        self._set_property(mapred_env, 'export JAVA_HOME=', java_home)
        self._set_property(mapred_env, 'export HADOOP_MAPRED_LOG_DIR=', self.log_dir)

        # mapred-site.xml
        mapred_site_temp = ParseXML(MAPRED_SITE_TEMPLATE_XML)
        mapred_site_temp.add_property('mapreduce.jobhistory.address', '%s:10020' % self.jhs_host[0])
        mapred_site_temp.add_property('mapreduce.jobhistory.webapp.address', '%s:19888' % self.jhs_host[0])
        mapred_site_temp.add_property('mapreduce.jobhistory.webapp.https.address', '%s:19890' % self.jhs_host[0])
        mapred_site_temp.add_property('mapreduce.jobhistory.admin.address', '%s:10033' % self.jhs_host[0])
        mapred_site_temp.write_xml()
        run_cmd('rm -f %s && cp %s %s' % (mapred_site, MAPRED_SITE_TEMPLATE_XML, mapred_site))

        self._setup_permission(cfg_dir)
        self._create_log_dir()

    def install(self):
        if self.hostname in self.hadoop_hosts:
            self._config_profile('HADOOP_YARN_HOME', self.home)
            self._config_profile('HADOOP_MAPRED_HOME', self.home)
            self.__config()

    def start(self):
        if self.hostname in self.rm_host:
            time.sleep(10)
            run_cmd_as_user(self.user, '%s/sbin/start-yarn.sh' % self.home)

        if self.hostname in self.jhs_host[0]:
            run_cmd_as_user(self.user, '%s/sbin/mr-jobhistory-daemon.sh --config /opt/hadoop/etc/hadoop start historyserver' % self.home)

    def stop(self):
        if self.hostname in self.jhs_host[0]:
            run_cmd_as_user(self.user, '%s/sbin/mr-jobhistory-daemon.sh --config /opt/hadoop/etc/hadoop stop historyserver' % self.home)

        if self.hostname in self.rm_host:
            run_cmd_as_user(self.user, '%s/sbin/stop-yarn.sh' % self.home)
        else:
            time.sleep(10)


class DeployHBase(Deploy):
    def __init__(self, client_configs, service):
        super(DeployHBase, self).__init__(client_configs, service)
        self.hbase_hosts = self.cfgs['hbase_hosts'].split(',')
        self.hbase_master = self.cfgs['hm_host'].split(',')
        self.hbase_rs = self.cfgs['rs_hosts'].split(',')
        self.hbase_ha = True if len(self.hbase_master) > 1 else False

    def __config(self):
        java_home = self.cfgs['java_home']
        cfg_dir = '%s/conf' % self.home
        master_cfg_dir = '%s/hm-conf' % self.home
        hbase_site = cfg_dir + '/hbase-site.xml'
        master_hbase_site = master_cfg_dir + '/hbase-site.xml'
        hbase_env = cfg_dir + '/hbase-env.sh'
        rs_cfg = cfg_dir + '/regionservers'
        hadoop_cfg = '/opt/hadoop/etc/hadoop'

        run_cmd('mkdir -p %s' % master_cfg_dir)
        run_cmd('cd %s; ln -sf ../conf/* .' % master_cfg_dir)
        run_cmd('rm -f %s' % master_hbase_site)

        # hbase-env.sh
        mod_file(hbase_env, {'export HBASE_OPTS=.*': '# export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"',
                             'export HBASE_MASTER_OPTS=.*': 'export HBASE_MASTER_OPTS=\"$HBASE_MASTER_OPTS  -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=256m\"',
                             'export HBASE_REGIONSERVER_OPTS=.*': 'export HBASE_REGIONSERVER_OPTS=\"$HBASE_REGIONSERVER_OPTS -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:ReservedCodeCacheSize=256m\"'})
        self._set_property(hbase_env, 'export JAVA_HOME=', java_home)
        self._set_property(hbase_env, 'export HBASE_MANAGES_ZK=', 'false')
        self._set_property(hbase_env, 'export HBASE_LOG_DIR=', self.log_dir)

        # hbase-site.xml
        change_items = {'ZOOKEEPER_QUORUM': '%s' % self.cfgs['zk_hosts']}
        hbase_site_temp = ParseXML(HBASE_SITE_TEMPLATE_XML).write_xml_to_string()
        if self.hadoop_ha == 'Y':
            change_items['HBASE_ROOTDIR'] = 'hdfs://esgyn/hbase'
        elif self.hadoop_ha == 'N':
            change_items['HBASE_ROOTDIR'] = 'hdfs://%s:8020/hbase' % self.cfgs['nn_host']
        gen_template_file(hbase_site_temp, hbase_site, change_items)
        gen_template_file(hbase_site_temp, master_hbase_site, change_items)

        # regionservers
        rs = '\n'.join(self.cfgs['rs_hosts'].split(','))
        write_file(rs_cfg, rs)

        # backup-masters
        # if self.hbase_ha:
        #    write_file(cfg_dir + '/backup-masters', self.hbase_master[1])

        # link core-site.xml and hdfs-site.xml to hbase config dir
        if os.path.exists('%s/core-site.xml' % cfg_dir):
            run_cmd('rm -f %s/core-site.xml' % cfg_dir)
        run_cmd('ln -s %s/core-site.xml %s/core-site.xml' % (hadoop_cfg, cfg_dir))
        if os.path.exists('%s/hdfs-site.xml' % cfg_dir):
            run_cmd('rm -f %s/hdfs-site.xml' % cfg_dir)
        run_cmd('ln -s %s/hdfs-site.xml %s/hdfs-site.xml' % (hadoop_cfg, cfg_dir))

        # add HADOOP_HOME in /opt/hbase/bin/hbase
        hbase_bin = '%s/bin/hbase' % self.home
        hadoop_home = 'export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}\nexport JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native'
        append_file(hbase_bin, hadoop_home, 'bin=`dirname "$0"`', True)
        run_cmd('chmod 755 %s' % hbase_bin)

        # create log dir
        self._create_log_dir()
        self._setup_permission(cfg_dir)
        self._setup_permission(master_cfg_dir)

    def install(self):
        if self.hostname in self.hbase_hosts:
            self._untar_files()
            self._config_profile('HBASE_HOME', self.home)
            self.__config()

    def start(self):
        if self.hostname in self.hbase_master:
            run_cmd_as_user(self.user, '%s/bin/hbase-daemon.sh --config "%s/hm-conf" start master' % (self.home, self.home))

        if self.hostname in self.hbase_rs:
            run_cmd_as_user(self.user, '%s/bin/hbase-daemon.sh --config "%s/conf" start regionserver; sleep 3' % (self.home, self.home))

    def stop(self):
        if self.hostname in self.hbase_master[0]:
            run_cmd_as_user(self.user, '%s/bin/stop-hbase.sh' % self.home)
        else:
            time.sleep(15)


class DeployHive(Deploy):
    def __init__(self, client_configs, service):
        super(DeployHive, self).__init__(client_configs, service)
        self.hive_hosts = self.cfgs['hms_host']

    def __config(self):
        cfg_dir = '%s/conf' % self.home
        hive_site = '%s/hive-site.xml' % cfg_dir
        hive_env = '%s/hive-env.sh' % cfg_dir
        hive_env_tmp = '%s/hive-env.sh.template' % cfg_dir
        # mysql_jdbc_tar = glob.glob('%s/mysql-*' % TMP_HADDOP_DIR)[0]  # mysql-connector-java-5.1.47.tar.gz
        # mysql_jdbc_dir = re.match(r'(.*)\.tar\.gz', mysql_jdbc_tar).groups()[0]

        # hive-env.sh
        run_cmd('cp %s %s' % (hive_env_tmp, hive_env))
        self._set_property(hive_env, 'HADOOP_HOME=', self.cfgs['hadoop_home'])

        # hive-site.xml
        hive_site_temp = ParseXML(HIVE_SITE_TEMPLATE_XML).write_xml_to_string()
        change_items = {'METASTORE_URIS': 'thrift://%s:9083' % self.cfgs['hms_host'],
                        'METASTORE_WAREHOUSE': 'hdfs://%s:9000/hive/warehouse' % self.cfgs['nn_host'],
                        'CONN_URL': 'jdbc:postgresql://%s:5432/hive?createDatabaseIfNotExist=true' % self.cfgs['pg_host'],
                        'CONN_PWD': self.cfgs['hive_pwd']}
        gen_template_file(hive_site_temp, hive_site, change_items)

        # deploy mysql jdbc
        # run_cmd('tar xf %s -C %s' % (mysql_jdbc_tar, TMP_HADDOP_DIR))
        # mysql_jdbc_bin = glob.glob('%s/*-bin.jar' % mysql_jdbc_dir)[0]
        # run_cmd('mv %s %s/lib' % (mysql_jdbc_bin, self.home))
        # run_cmd('rm -rf %s' % mysql_jdbc_dir)

        # deploy pg jdbc
        # run_cmd('yum install -y postgresql-jdbc')
        # if os.path.exists('%s/lib/postgresql-jdbc.jar' % self.home):
        #    run_cmd('rm -rf %s/lib/postgresql-jdbc.jar' % self.home)
        # run_cmd('ln -s /usr/share/java/postgresql-jdbc.jar %s/lib/postgresql-jdbc.jar' % self.home)

        self._setup_permission(cfg_dir)

    def install(self):
        self._untar_files()
        self._config_profile('HIVE_HOME', self.home)
        # TODO
        self._setup_permission('%s/conf' % self.home)
        if self.hostname in self.hive_hosts:
            self.__config()

    def start(self):
        if self.hostname in self.hive_hosts:
            # run_cmd_as_user(self.user, '%s/bin/hive --service metastore &' % self.home)
            pass
