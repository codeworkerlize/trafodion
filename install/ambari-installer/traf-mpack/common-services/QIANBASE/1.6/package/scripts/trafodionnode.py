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
import sys, os, pwd, signal, time
from resource_management import *
from tempfile import TemporaryFile

class Node(Script):
  def install(self, env):
  
    # Install packages listed in metainfo.xml
    self.install_packages(env)
  
    import params
    Directory("/etc/trafodion", 
              mode=0755, 
              owner = params.traf_user, 
              group = params.traf_group)
    Directory(params.traf_conf_dir, 
              mode=0755, 
              owner = params.traf_user, 
              group = params.traf_group, 
              create_parents = True)
    cmd = "source /etc/trafodion/trafodion_config 2>/dev/null; cp -rf $TRAF_HOME/conf/* %s/"  % params.traf_conf_dir
    Execute(cmd,user=params.traf_user)
    self.configure(env)

  def configure(self, env):
    import params
    from license import License, encode_pwd

    trafhome = os.path.expanduser("~" + params.traf_user)
    traf_sqc=os.path.expanduser('~trafodion/sqcert')
    keystore_file=os.path.join(traf_sqc,"server.keystore")
    pkcs_cert_file=os.path.join(traf_sqc,"server.pkcs12")
    server_key_file=os.path.join(traf_sqc,"server.key")
    server_crt_file=os.path.join(traf_sqc,"server.crt")


    # env files use java_home, be sure we are on 1.8 
    # might be better to check this earlier (in service_advisor.py)
    if params.java_version < 8:
      print "Error: Java 1.8 required for EsgynDB and HBase"
      print "       Use 'ambari-server setup' to change JDK and restart HBase before continuing"
      exit(1)

    # log and tmp dirs
    Directory(params.traf_logdir,
              mode=0755,
              owner = params.traf_user,
              group = params.traf_group,
              create_parents = True)
    Directory(params.traf_vardir,
              mode=0755,
              owner = params.traf_user,
              group = params.traf_group,
              create_parents = True)

    ##################
    # create env files
    env.set_params(params)
    traf_conf_path = os.path.join(params.traf_conf_dir, "trafodion-env.sh")
    File(traf_conf_path,
         owner = params.traf_user, 
         group = params.traf_group, 
         content=InlineTemplate(params.traf_env_template,trim_blocks=False),
         mode=0644)

    # write cluster-env in trafodion home dir
    #params.traf_node_list = [node.split('.')[0] for node in params.traf_node_list] # convert to short hostname
    traf_nodes = ' '.join(params.traf_node_list)
    traf_w_nodes = '-w ' + ' -w '.join(params.traf_node_list)
    traf_node_count = len(params.traf_node_list)

    traf_conf_path = os.path.join(params.traf_conf_dir, "traf-cluster-env.sh")
    File(traf_conf_path,
         owner = params.traf_user, 
         group = params.traf_group, 
         content=InlineTemplate(params.traf_clust_template,
                                traf_nodes=traf_nodes,
                                traf_w_nodes=traf_w_nodes,
                                traf_node_count=traf_node_count,
                                cluster_name=params.cluster_name),
         mode=0644)


    # initialize & verify env
    cmd = "source ~/.bashrc"
    Execute(cmd,user=params.traf_user)

    # trafodion-site
    XmlConfig("trafodion-site.xml",
              conf_dir=params.traf_conf_dir,
              configurations=params.traf_site_conf,
              owner=params.traf_user,
              mode=0644)
    cmd = format("cp -f {params.traf_conf_dir}/trafodion-site.xml {params.traf_home}/conf/trafodion-site.xml")
    Execute(cmd,user=params.traf_user)

    ##################
    # license file
    if params.esgyndb_license_key == '':
      print 'No license key -- a short grace period allowed to acquire a license.'
      print 'Contact Esgyn and send ID file: /etc/trafodion/esgyndb_id on node %s.' % params.traf_node_list[0]
    else:
      File("/etc/trafodion/esgyndb_license",
           owner = params.traf_user, 
           group = params.traf_group, 
           content=params.esgyndb_license_key,
           mode=0644)
      lic = License("/etc/trafodion/esgyndb_license")
      if lic.type() == 'INTERNAL':
        print "Internal license -- skipping node check"
      else:
        if traf_node_count > lic.nodes():
          print 'Current number of nodes does not match allowed number of nodes'
          exit(1)
      if lic.expired():
        print 'License expired!'
        exit(1)

    ##################
    # set up cgroups
    if params.esgyndb_license_key != '' and lic.mt_enabled():
      # append cgroup environment settings
      ofile = TemporaryFile()
      Execute("uname -r",stdout=ofile,logoutput=True)
      ofile.seek(0) # read from beginning
      kernel_ver = ofile.read().rstrip()
      ofile.close()
      major, minor = kernel_ver.split('-')
      # RH6, config cgroups, but only if cgroups kernel patch installed
      if major == '2.6.32': # centos6 kernel major version
        minors = [m for m in minor.split('.') if m.isdigit()]
        if int(minors[0]) < 573 or (int(minors[0]) == 573 and len(minors) > 1 and int(minors[1]) < 7):
          raise Fail("Error: Multitenancy feature requires linux kernel at least 2.6.32-573.7.1")
        Execute("/sbin/chkconfig cgconfig on")
        Execute("/sbin/chkconfig cgred on")

      # RH7, cgroups should be enabled by default

      # make sure cgroups enabled
      Execute("/sbin/service cgconfig start")
      # make sure Esgyn group created
      cmd = "source ~" + params.traf_user + "/.bashrc ; $TRAF_HOME/sql/scripts/edb_cgroup_cmd " + \
            "--add -u %s -g %s --pcgrp Esgyn " % (params.traf_user,params.traf_group) + \
            "--cpu-pct %s --mem-pct %s" % (params.traf_cpu_limit,params.traf_mem_limit)
      Execute(cmd,logoutput=True)
      # write cgroup rule
      rfile = open("/etc/cgrules.conf", "r")
      rfound = 'Esgyn' in rfile.read()
      rfile.close()
      if not rfound:
        rfile = open("/etc/cgrules.conf","a")
        rfile.write('%s   cpu,cpuacct,memory   Esgyn/\n' % params.traf_user)
        rfile.close()
      # restart cgred to re-read rules
      Execute("/sbin/service cgred restart")

    # add cgroup config, even if we are not using cgroups
    File("/tmp/cgroup_env.sh",
         owner = params.traf_user, 
         group = params.traf_group, 
         content=StaticFile("cgroup_env.sh"),
         mode=0744)
    cmd = "source ~/.bashrc ; /tmp/cgroup_env.sh >> %s" % traf_conf_path
    Execute(cmd,user=params.traf_user)

    ##################
    # Make sure our JVM files are owned by us
    if os.path.exists("/tmp/hsperfdata_%s" % params.traf_user):
      cmd = "chown -R %s:%s /tmp/hsperfdata_%s" % (params.traf_user, params.traf_group, params.traf_user)
      Execute(cmd)


    ##################
    # LDAP config
    if params.traf_ldap_enabled == 'YES':
      File(os.path.join(trafhome,".traf_authentication_config"),
           owner = params.traf_user, 
           group = params.traf_group, 
           content = InlineTemplate(params.traf_ldap_template),
           mode=0750)
      cmd = format("mv -f ~/.traf_authentication_config {params.traf_conf_dir}/")
      Execute(cmd,user=params.traf_user)
      cmd = "source ~/.bashrc ; ldapconfigcheck -file $TRAF_CONF/.traf_authentication_config"
      Execute(cmd,user=params.traf_user)
      cmd = 'source ~/.bashrc ; ldapcheck --verbose --username=%s' % params.traf_db_admin
      Execute(cmd,user=params.traf_user)

    ##################
    # All EsgynDB Nodes need DCS config files
    # In future, should move DCS conf to traf_conf_dir
    File(os.path.join(trafhome,"dcs-env.sh"),
         owner = params.traf_user, 
         group = params.traf_group, 
         content = InlineTemplate(params.dcs_env_template),
         mode=0644)
    File(os.path.join(trafhome,"log4j.properties"),
         owner = params.traf_user, 
         group = params.traf_group, 
         content = InlineTemplate(params.dcs_log4j_template),
         mode=0644)

    serverlist = '\n'.join(params.dcs_mast_node_list) + '\n'
    File(os.path.join(trafhome,"masters"),
         owner = params.traf_user, 
         group = params.traf_group, 
         content = serverlist,
         mode=0644)

    traf_conf_path = os.path.join(params.traf_conf_dir, "client_config")
    Directory(params.traf_conf_dir,
         create_parents = True,
         owner = params.traf_user,
         group = params.traf_group,
         mode=0755)
    File(traf_conf_path,
         owner = params.traf_user,
         group = params.traf_group,
         content=InlineTemplate(params.traf_client_template,trim_blocks=False),
         mode=0644)
    Directory(os.path.join(params.traf_conf_dir, "dcs"),
         create_parents = True,
         owner = params.traf_user,
         group = params.traf_group,
         mode=0755)
    Directory(os.path.join(params.traf_conf_dir, "rest"),
         create_parents = True,
         owner = params.traf_user,
         group = params.traf_group,
         mode=0755)

    serverlist = ''
    per_node = int(params.dcs_servers) // traf_node_count
    extra = int(params.dcs_servers) % traf_node_count
    for nnum, node in enumerate(params.traf_node_list, start=0):
      if nnum < extra:
         serverlist += '%s %s\n' % (node, per_node + 1)
      else:
         serverlist += '%s %s\n' % (node, per_node)
    File(os.path.join(trafhome,"servers"),
         owner = params.traf_user, 
         group = params.traf_group, 
         content = serverlist,
         mode=0644)

    cmd = 'route | awk \'$1=="default" {print $NF;}\''
    ofile = TemporaryFile()
    Execute(cmd,user=params.traf_user,stdout=ofile,stderr=ofile,logoutput=True)
    ofile.seek(0) # read from beginning
    def_interface = ofile.read().rstrip()
    ofile.close()
    site_conf = params.config['configurations']['dcs-site'].copy()
    if site_conf['dcs.dns.interface'] == "default":
      site_conf['dcs.dns.interface'] = def_interface
    if site_conf['dcs.master.floating.ip.external.interface'] == "default":
      site_conf['dcs.master.floating.ip.external.interface'] = def_interface
    site_conf['zookeeper.znode.parent'] = "/%s/%s" % (params.traf_user, params.traf_instance_id)

    XmlConfig("dcs-site.xml",
              conf_dir=trafhome,
              configurations=site_conf,
              owner=params.traf_user,
              mode=0644)
    # install DCS conf files
    cmd = "source ~/.bashrc ; mv -f ~/dcs-env.sh ~/log4j.properties ~/dcs-site.xml ~/masters ~/servers $TRAF_CONF/dcs/"
    Execute(cmd,user=params.traf_user)

    site_conf = params.config['configurations']['rest-site'].copy()
    if site_conf['rest.dns.interface'] == "default":
      site_conf['rest.dns.interface'] = def_interface
    site_conf['rest.keystore'] = keystore_file
    site_conf['zookeeper.znode.parent'] = "/%s/%s" % (params.traf_user, params.traf_instance_id)

    if params.traf_dbm_enable_https:
        site_conf['rest.ssl.password'] = encode_pwd(params.traf_dbm_keystore_pwd)

    XmlConfig("rest-site.xml",
              conf_dir=trafhome,
              configurations=site_conf,
              owner=params.traf_user,
              mode=0644)
    # install REST conf files
    cmd = "source ~/.bashrc ; mv -f ~/rest-site.xml $TRAF_CONF/rest/"
    Execute(cmd,user=params.traf_user)

    # mds config
    cmd = "sed -i -e 's#esurl:.*#esurl: http://%s#'" % params.elasticsearch_url + \
          " %s/dcs/mdsconf.yaml" % params.traf_conf_dir
    Execute(cmd, user=params.traf_user)

    ##################
    # mgblty config
    cmd = "sed -i -e 's/LOGSTASH_HOST:LOGSTASH_PORT/%s/'" % params.logstash_url + \
                " -e '/add_cloud_metadata/a\\  - add_labels:\\n    labels:\\n      traf_cluster_id: %s'" % params.traf_cluster_id + \
                " %s/mgblty/filebeat/filebeat.yml" % params.traf_conf_dir
    Execute(cmd, user=params.traf_user)

    cmd = "sed -i -e 's#elasticsearch.url:.*#elasticsearch.url: http://%s#'" % params.elasticsearch_url + \
          " %s/mgblty/esgyn_exporter/esgyn_exporter.yaml" % params.traf_conf_dir
    Execute(cmd, user=params.traf_user)

    ##################
    # create trafodion scratch dirs
    for sdir in params.traf_scratch.split(','):
      Directory(sdir,
                mode=0777,
                owner = params.traf_user,
                group = params.traf_group,
                create_parents = True)

    # generate sqconfig file
    cmd = "lscpu|grep -E '(^CPU\(s\)|^Socket\(s\))'|awk '{print $2}'"
    ofile = TemporaryFile()
    Execute(cmd,stdout=ofile)
    ofile.seek(0) # read from beginning
    core, processor = ofile.read().split('\n')[:2]
    ofile.close()

    core = int(core)-1 if int(core) <= 256 else 255

    lines = ['begin node\n']
    for node_id, node in enumerate(params.traf_node_list):
      line = 'node-id=%s;node-name=%s;cores=0-%d;processors=%s;roles=connection,aggregation,storage\n' \
               % (node_id, node, core, processor)
      lines.append(line)

    lines.append('end node\n')
    lines.append('\n')
    lines.append('begin overflow\n')
    for scratch_loc in params.traf_scratch.split(','):
        line = 'hdd %s\n' % scratch_loc
        lines.append(line)
    lines.append('end overflow\n')

    # write sqconfig in trafodion home dir
    File(os.path.join(trafhome,"sqconfig"),
         owner = params.traf_user,
         group = params.traf_user,
         content=''.join(lines),
         mode=0644)

    # install sqconfig
    Execute(format('mv -f ~/sqconfig {params.traf_conf_dir}/'),user=params.traf_user)

    # Execute SQ gen
    Execute('source ~/.bashrc ; rm -f $TRAF_VAR/ms.env $TRAF_VAR/sqconfig.db; sqgen',user=params.traf_user)

    # security certificate and keystore
    Directory(traf_sqc, 
              mode=0700, 
              owner = params.traf_user, 
              group = params.traf_group)

    #if os.path.exists(server_crt_file):
    #  print "Found sqcert"
    #else:
    Execute('source ~/.bashrc ; CLUSTERNAME=HDP $TRAF_HOME/sql/scripts/sqcertgen ss server.key server.crt',
              user=params.traf_user)

    #if os.path.exists(keystore_file):
    #  print "Found keystore"
    #else:
    if params.traf_dbm_enable_https:
       cmd = "source ~/.bashrc ; CLUSTERNAME=HDP $TRAF_HOME/sql/scripts/" + \
             format("sqcertgen gen_keystore {params.traf_dbm_keystore_pwd!p}")
       Execute(cmd, user=params.traf_user)


  def stop(self, env):
    import params

    # dcs server / mxo servers
    # monitor may not shutdown if mxosrvrs not gone
    cmd = 'source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop server ' + \
          format('$( grep -n {params.chost} $TRAF_CONF/dcs/servers | cut -d: -f1) ')
    Execute(cmd,user=params.traf_user,logoutput=True)

    # mds stop
    cmd = 'source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop mds ' + \
          format('$( grep -n {params.chost} $TRAF_CONF/dcs/servers | cut -d: -f1)')
    Execute(cmd, user=params.traf_user, logoutput=True)

    # monitor stop
    node_name = params.chost.split('.')[0]
    sdir = os.path.dirname(os.path.abspath(__file__))

    cmd = format('/bin/bash {sdir}/stopnode.sh {node_name}')
    Execute(cmd,user=params.traf_user,logoutput=True)

    # wait for monitor down
    Execute('source ~/.bashrc ; sqcheckmon -s down -f',
            user=params.traf_user,tries=40,try_sleep=3)

    # krb check
    Execute('source ~/.bashrc ; krb5service stop',
            user=params.traf_user,logoutput=True)
    # littlejetty
    Execute('source ~/.bashrc ; jps | grep LittleJetty | cut -d " " -f 1 | xargs kill -9 || exit 0',
            user=params.traf_user,logoutput=True)

  def start(self, env):
    import params
    from license import License
    self.configure(env)

    # Verify HBase has TRX installed
    sdir = os.path.dirname(os.path.abspath(__file__))
    cmd = format('/bin/bash {sdir}/trxcheck.sh')
    Execute(cmd,user=params.traf_user,logoutput=True)


    coordinating_host = params.traf_node_list[0]

    lic = License("/etc/trafodion/esgyndb_license")

    # Normally needed only on very first start
    if params.chost == coordinating_host:
      if os.path.exists("/etc/trafodion/esgyndb_id"):
        print "Found esgyndb_id"
      else:
        File("/etc/trafodion/esgyndb_id",
             owner = params.traf_user, 
             group = params.traf_group, 
             content=lic.gen_id(),
             mode=0644)
        if params.esgyndb_license_key != '' and lic.vers() > 1 and lic.type() == 'PRODUCT':
           print 'Error: License key not valid for new cluster ID.'
           print 'Contact Esgyn and send ID file: /etc/trafodion/esgyndb_id on node %s.' % params.chost
           exit(1)

      params.HdfsDirectory("/user/trafodion",
                           action="create_on_execute",
                           owner=params.traf_user,
                           group=params.traf_group,
                           mode=0755,
                          )
      params.HdfsFile("/user/trafodion/cluster_id",
                           action="create_on_execute",
                           source="/etc/trafodion/esgyndb_id",
                           owner=params.traf_user,
                           group=params.traf_group,
                           mode=0600,
                          )
      params.HdfsDirectory(None, action="execute")
      # set cluster ID
      Execute('source ~/.bashrc ; xdc -setmyid %s' % params.traf_cluster_id, user=params.traf_user)

    # copy to local node from HDFS
    if params.security_enabled:
      Execute(params.kinit_cmd,user=params.traf_user)
    Execute('rm -f /etc/trafodion/esgyndb_id',user=params.traf_user)
    test_cmd = "fs -copyToLocal /user/trafodion/cluster_id /etc/trafodion/esgyndb_id"
    ExecuteHadoop( test_cmd,
                   tries     = 30,
                   try_sleep = 5,
                   user      = params.traf_user,
                   conf_dir  = params.hadoop_conf_dir,
                   bin_dir   = params.hadoop_bin_dir
                  )
    if params.esgyndb_license_key != '' and lic.type() == 'PRODUCT' \
        and not lic.idmatch('/etc/trafodion/esgyndb_id') :
       print 'Error: License key not valid for this cluster.'
       exit(1)

    # cleanup environment
    Execute('source ~/.bashrc ; sqipcrm -local', user=params.traf_user,logoutput=True)
    Execute('source ~/.bashrc ; cleanZKmonZNode %s' % params.chost, user=params.traf_user)
    Execute('source ~/.bashrc ; mgblty_stop -m',
            user=params.traf_user,logoutput=True)

    # Create tenant level cgroup on current node
    Execute('source ~/.bashrc ; $JAVA_HOME/bin/java com.esgyn.common.CGroupHelper initZK',user=params.traf_user)

    # start monitor
    print "Starting monitor"
    
    traf_node_count = len(params.traf_node_list)
    Execute('source ~/.bashrc ; monitor COLD_AGENT',
          user=params.traf_user,wait_for_finish=False)
    # wait for local node monitor (sqcheckmon internally tries 10 times w/ 5 sec sleep)
    Execute('source ~/.bashrc ; sqcheckmon',
            user=params.traf_user,tries=3,try_sleep=10)
    # wait for monitors to pick a leader
    for wait in [5, 5, 10, 10, 20, 20, 30, 30]:
      cmd = 'hbase zkcli ls /%s/%s/monitor/master 2>/dev/null | grep "^\[" || exit 0' % (params.traf_user, params.traf_instance_id)
      ofile = TemporaryFile()
      Execute(cmd,user=params.traf_user,stdout=ofile,stderr=ofile,logoutput=True)
      ofile.seek(0) # read from beginning
      output = ofile.read().rstrip()
      ofile.close()
      if output == '' or output == '[]':
         print "Waiting %s seconds for monitor leader" % wait
         time.sleep(wait)
      else:
         break
    if output == '[' + params.chost.split('.')[0] + ']':
      master_monitor_node = True
    else:
      master_monitor_node = False

    # cluster start
    if master_monitor_node:
      # start foundation
      Execute('source ~/.bashrc ; trstart',
              user=params.traf_user,logoutput=True)

    # allow time for other nodes to integrate, scaled to cluster size
    wtime = traf_node_count * float(params.traf_scaling_factor)
    print "Allowing %s seconds for node integration" % wtime
    time.sleep(wtime)

    # start om-client
    Execute('source ~/.bashrc ; om-client start',
            user=params.traf_user, logoutput=True)

    # starting littlejetty
    Execute('source ~/.bashrc ; node_littlejettystart',
              user=params.traf_user,logoutput=True)    

    # start connection servers
    cmd = 'source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh start server ' + \
          format('$( grep -n {params.chost} $TRAF_CONF/dcs/servers | cut -d: -f1) ') + \
          format('$( grep -n {params.chost} $TRAF_CONF/dcs/servers | cut -d" " -f2) ')
    Execute(cmd,user=params.traf_user,logoutput=True)

    # start mds
    cmd = 'source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh start mds ' + \
          format('$( grep -n {params.chost} $TRAF_CONF/dcs/servers | cut -d: -f1)')
    Execute(cmd, user=params.traf_user, logoutput=True)

    # krb check
    if params.security_enabled:
      Execute('source ~/.bashrc ; krb5service restart',
              user=params.traf_user,logoutput=True)

    if master_monitor_node:
      # alter db__root & db__admin users
      if params.traf_ldap_enabled == 'YES':
        cmd = "source ~/.bashrc ; echo 'initialize authorization; " + \
              " alter user DB__ADMIN set external name \"%s\"; " % params.traf_db_admin + \
              " alter user DB__ROOT set external name \"%s\"; " % params.traf_db_root + \
              " ' | sqlci"
        ofile = TemporaryFile()
        Execute(cmd,user=params.traf_user,stdout=ofile,stderr=ofile,logoutput=True)
        ofile.seek(0) # read from beginning
        output = ofile.read()
        ofile.close()
        if (output.find('1393') >= 0):
          print output + '\n'
          print "Warning: Meta-Data not initialized. Run Service Action 'Initialize'."
        elif (output.find('ERROR') >= 0):
          print output + '\n'
          raise Fail("Failed to initialize DB Admin and DB Root users.")
    else:
      # wait for rms before dclare node-up is finished
      Execute('source ~/.bashrc ; sqcheck -c rms -f',
              user=params.traf_user,tries=20,try_sleep=7)


  def status(self, env):
    import status_params
    node_name = status_params.chost.split('.')[0]
    cmd = "source ~/.bashrc ; sqshell -c node info | " + \
	      format("grep {node_name} | grep -q Up")
    try:
      Execute(cmd, user=status_params.traf_user)
    except:
      raise ComponentIsNotRunning()

  def upgrade_qianbase_node(self, env):
    import params
    print 'Upgrading QianBase package ...'
    Execute('yum clean all --disablerepo="*" --enablerepo="QianBase-1.x*"',logoutput=True)
    Execute('yum -y upgrade QianBase',logoutput=True)
    cmd = "source /etc/trafodion/trafodion_config 2>/dev/null; cp -rf $TRAF_HOME/conf/* %s/"  % params.traf_conf_dir
    Execute(cmd,user=params.traf_user)


if __name__ == "__main__":
  Node().execute()
