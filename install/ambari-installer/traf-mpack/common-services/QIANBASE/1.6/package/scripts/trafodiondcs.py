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
import subprocess,os
from resource_management import *
from tempfile import TemporaryFile

class DCS(Script):
  def install(self, env):
  
    # Install packages listed in metainfo.xml
    self.install_packages(env)

  def configure(self, env):
    import params
    my_instance = params.dcs_mast_node_list.index(params.chost)
    if my_instance == 1:
       STATE="MASTER"
    else:
       STATE="BACKUP"
    

    if params.dcs_floating_interface == 'default':
      cmd = 'route | awk \'$1=="default" {print $NF;}\''
      ofile = TemporaryFile()
      Execute(cmd,user=params.traf_user,stdout=ofile,stderr=ofile,logoutput=True)
      ofile.seek(0) # read from beginning
      NET_INTERFACE = ofile.read().rstrip()
      ofile.close()
    else:
      NET_INTERFACE = params.dcs_floating_interface
    File("/etc/keepalived/trafodion.conf",
         group=params.traf_group,
         owner=params.traf_user,
         content=Template("keepalived.conf",
                          DCS_PORT=params.dcs_master_port,
                          ROLE="%s" % STATE,
                          PRIORITY=str(101 - int(my_instance)),
                          FLOATING_IP=params.dcs_floating_ip,
                          NET_INTERFACE=NET_INTERFACE)
        )
    if params.dcs_aws_access_id != '':
      # installing awscli only needed for RH6. rpm dependency for RH7
      if not os.path.exists("/usr/bin/aws"):
        print('Downloading awscli-bundle.zip from aws website...')
        awscli_file = '/tmp/awscli-bundle.zip'
        cmd = 'curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o %s' % awscli_file
        print('Installing awscli-bundle...')
        Execute(cmd,user=params.traf_user)
        cmd = 'unzip -d /tmp -o %s' % awscli_file
        Execute(cmd,user=params.traf_user)
        cmd = '/tmp/awscli-bundle/install -i /usr/local/aws -b /usr/bin/aws'
        Execute(cmd)
        cmd = 'rm -rf /tmp/awscli-bundle'
        Execute(cmd)

      trafhome = os.path.expanduser("~" + params.traf_user)
      aws_conf_path = os.path.join(trafhome, ".aws")
      Directory(aws_conf_path,
                owner=params.traf_user,
                group=params.traf_group,
                mode=0700)
      env.set_params(params)
      File(os.path.join(aws_conf_path,"credentials"),
           owner=params.traf_user,
           group=params.traf_group,
           content=InlineTemplate(params.dcs_aws_cred_template),
           mode=0600)
      File(os.path.join(aws_conf_path,"config"),
           owner=params.traf_user,
           group=params.traf_group,
           content=InlineTemplate(params.dcs_aws_config_template),
           mode=0600)


  def stop(self, env):
    import params
    my_instance = params.dcs_mast_node_list.index(params.chost)
    Execute('source ~/.bashrc ; ${REST_INSTALL_DIR}/bin/rest-daemon.sh stop rest',
            user=params.traf_user,logoutput=True)
    Execute('source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop master %s' % my_instance,
            user=params.traf_user,logoutput=True)
    Execute('pkill -f DCSMASTERS" " || exit 0', user=params.traf_user)

  # REST should run on all DCS backup and master nodes
  def start(self, env):
    import params
    self.configure(env)

    if params.security_enabled:
      Execute(params.kinit_cmd,user=params.traf_user)

    my_instance = params.dcs_mast_node_list.index(params.chost)
    Execute('source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh start master %s' % my_instance,user=params.traf_user)

  # Check master pidfile
  def status(self, env):
    import status_params
    cmd = "source ~%s/.bashrc >/dev/null 2>&1; ls $TRAF_VAR/dcs-trafodion-master*.pid" % status_params.traf_user
    ofile = TemporaryFile()
    try:
      Execute(cmd,stdout=ofile,user=status_params.traf_user)
    except:
      ofile.close()
      raise ComponentIsNotRunning()
    ofile.seek(0) # read from beginning
    pidfile = ofile.read().rstrip()
    ofile.close()
    check_process_status(pidfile)

    
  def initialize(self, env):
    import params

    params.HdfsDirectory("/user/trafodion/backups",
                         action="create_on_execute",
                         owner=params.traf_user,
                         group=params.user_group,
                         mode=0770,
                        )
    params.HdfsDirectory("/user/trafodion/backupsys",
                         action="create_on_execute",
                         owner=params.traf_user,
                         group=params.user_group,
                         mode=0770,
                        )
    params.HdfsDirectory("/user/trafodion/PIT",
                         action="create_on_execute",
                         owner=params.traf_user,
                         group=params.user_group,
                         mode=0770,
                        )
    params.HdfsDirectory("/user/trafodion/bulkload",
                         action="create_on_execute",
                         owner=params.traf_user,
                         group=params.user_group,
                         mode=0750,
                        )
    params.HdfsDirectory("/user/trafodion/lobs",
                         action="create_on_execute",
                         owner=params.traf_user,
                         group=params.traf_group,
                         mode=0750,
                        )
    params.HdfsDirectory(None, action="execute")

    if params.security_enabled:
      Execute(params.kinit_cmd,user=params.traf_user)

    cmd = "source ~/.bashrc ; echo 'initialize trafodion;' | sqlci 2>&1 | tee /tmp/Esgyn_init.out"
    Execute(cmd,user=params.traf_user,logoutput=True)
    ofile = open("/tmp/Esgyn_init.out")
    output = ofile.read()
    ofile.close()

    if (output.find('1395') >= 0 or output.find('1392') >= 0):
      print "Re-trying initialize as upgrade\n"
      cmd = "source ~/.bashrc ; echo 'initialize trafodion, upgrade;' | sqlci 2>&1 | tee /tmp/Esgyn_init_upgrade.out"
      Execute(cmd,user=params.traf_user,logoutput=True)
      ofile = open("/tmp/Esgyn_init_upgrade.out")
      output = ofile.read()
      ofile.close()

    if (output.find('ERROR') >= 0):
      raise Fail("Failed meta-data initialization/upgrade")

    cmd = "source ~/.bashrc ; echo 'initialize trafodion, add tenant usage;' | sqlci 2>&1 | tee /tmp/Esgyn_init_tenant.out"
    Execute(cmd,user=params.traf_user,logoutput=True)
    ofile = open("/tmp/Esgyn_init_tenant.out")
    output = ofile.read()
    ofile.close()

    if (output.find('ERROR') >= 0):
      raise Fail("Failed tenant initialization")

    cmd = "source ~/.bashrc ; echo 'initialize trafodion, upgrade library management;' | sqlci 2>&1 | tee /tmp/Esgyn_init_lib.out"
    Execute(cmd,user=params.traf_user,logoutput=True)
    ofile = open("/tmp/Esgyn_init_lib.out")
    output = ofile.read()
    ofile.close()

    if (output.find('ERROR') >= 0):
      raise Fail("Failed library initialization")

    if params.traf_ldap_enabled == 'YES':
      cmd = "source ~/.bashrc ; echo 'initialize authorization; " + \
            " alter user DB__ADMIN set external name \"%s\"; " % params.traf_db_admin + \
            " alter user DB__ROOT set external name \"%s\"; " % params.traf_db_root + \
            " ' | sqlci sqlci 2>&1 | tee /tmp/Esgyn_init_auth.out"
      Execute(cmd,user=params.traf_user,logoutput=True)
      ofile = open("/tmp/Esgyn_init_auth.out")
      output = ofile.read()
      ofile.close()

      if (output.find('ERROR') >= 0):
        raise Fail("Failed authorization initialization")

    print "Restarting Connections\n"
    self.connections_restart(env)

  def connections_restart(self, env):
    import params
    my_instance = params.dcs_mast_node_list.index(params.chost)
    Execute('source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/stop-dcs.sh', user=params.traf_user,logoutput=True)
    Execute('source ~/.bashrc ; edb_pdsh -a pkill -9 -u $(id -u) mxosrvr', user=params.traf_user,logoutput=True)
    Execute('source ~/.bashrc ; ${DCS_INSTALL_DIR}/bin/start-dcs.sh', user=params.traf_user,logoutput=True)

  def upgrade_qianbase_dcs(self, env):
    print 'Upgrading QianBase package ...'
    Execute('yum clean all --disablerepo="*" --enablerepo="QianBase-1.x*"',logoutput=True)
    Execute('yum -y upgrade QianBase',logoutput=True)

if __name__ == "__main__":
  DCS().execute()
