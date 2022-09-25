import subprocess, os
from resource_management import *
from tempfile import TemporaryFile

class DBMGR(Script):
  def install(self, env):
  
    # Install packages listed in metainfo.xml
    self.install_packages(env)

  def configure(self, env):
    import params
    from license import encode_pwd
    Directory(os.path.join(params.traf_conf_dir, "dbmgr"),
              create_parents = True,
              owner = params.traf_user,
              group = params.traf_group,
              mode=0755)
    cmd = "cat %s/dbmgr/config_template.xml" % params.traf_conf_dir
    ofile = TemporaryFile()
    Execute(cmd, stdout=ofile, user=params.traf_user)
    ofile.seek(0)  # read from beginning
    output = ofile.read()
    ofile.close()
    File(os.path.join(params.traf_conf_dir, "dbmgr/config.xml"),
         owner=params.traf_user,
         group=params.traf_group,
         content=output,
         mode=0664)

    cmd = format('{params.traf_home}/tools/gettimezone.sh')
    ofile = TemporaryFile()
    Execute(cmd,stdout=ofile,user=params.traf_user)
    ofile.seek(0) # read from beginning
    output = ofile.read().split('\n')[0]
    ofile.close()
    if output:
      timezone = output
    else:
      timezone = 'UTC'

    if params.dcs_enable_ha == 'true':
        dcs_master_host = params.dcs_floating_ip + ':' + params.dcs_master_port
    else:
        dcs_master_host = [dcs_master + ':' + params.dcs_master_port for dcs_master in params.dcs_mast_node_list]
        dcs_master_host = ','.join(dcs_master_host)

    cmd = "sed -i -e 's/DCS_HOST:DCS_PORT/%s/'" % dcs_master_host + \
                " -e 's/HTTP_PORT/%s/'" % params.traf_dbm_http_port + \
                " -e 's/HTTPS_PORT/%s/'" % params.traf_dbm_https_port + \
                " -e 's#TIMEZONE_NAME#%s#'" % timezone + \
                " -e 's/ADMIN_USERID/%s/'" % params.traf_db_admin

    if params.traf_ldap_enabled == 'YES':
        cmd += " -e 's/ADMIN_PASSWORD/%s/'" % encode_pwd(params.traf_db_admin_pwd)
    else:
        cmd += " -e 's/ADMIN_PASSWORD/DB__ADMIN/'"

    if params.traf_dbm_enable_https:
        cmd += " -e 's/ENABLE_HTTPS/True/'" + \
               " -e 's#KEYSTORE_FILE#%s/sqcert/server.keystore#'" % params.traf_user_home + \
               " -e 's/SECURE_PASSWORD/%s/'" % encode_pwd(params.traf_dbm_keystore_pwd)
    else:
        cmd += " -e 's/ENABLE_HTTPS/False/'"
    cmd += " %s/dbmgr/config.xml" % params.traf_conf_dir
    Execute(cmd, user=params.traf_user, logoutput=True)

  def stop(self, env):
    import params

    cmd = 'source ~/.bashrc ; ${DBMGR_INSTALL_DIR}/bin/dbmgr.sh stop'
    Execute(cmd,user=params.traf_user,logoutput=True)

  def start(self, env):
    import params
    self.configure(env)

    cmd = 'source ~/.bashrc ; ${DBMGR_INSTALL_DIR}/bin/dbmgr.sh start'
    Execute(cmd,user=params.traf_user,logoutput=True)
	
  def status(self, env):
    import status_params
    cmd = 'source ~/.bashrc ; ${DBMGR_INSTALL_DIR}/bin/dbmgr.sh status'
    try:
      Execute(cmd,user=status_params.traf_user)
    except:
      raise ComponentIsNotRunning()

  def upgrade_qianbase_dbmgr(self, env):
    print 'Upgrading QianBase package ...'
    Execute('yum clean all --disablerepo="*" --enablerepo="QianBase-1.x*"',logoutput=True)
    Execute('yum -y upgrade QianBase',logoutput=True)

    
if __name__ == "__main__":
  DBMGR().execute()
