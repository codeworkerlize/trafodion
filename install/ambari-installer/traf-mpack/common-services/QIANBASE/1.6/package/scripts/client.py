import os
from resource_management import *

class EDBCLIENT(Script):
  def install(self, env):
  
    # Install packages listed in metainfo.xml
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params

    env.set_params(params)

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

    traf_conf_path = os.path.join(params.traf_conf_dir, "odbcinst.ini")
    File(traf_conf_path,
         owner = params.traf_user,
         group = params.traf_group,
         content=InlineTemplate(params.traf_oinst_template,trim_blocks=False),
         mode=0644)

  def status(self, env):
    raise ClientComponentHasNoStatus()

  def upgrade_qianbase_client(self, env):
    print 'Upgrading QianBase package ...'
    Execute('yum clean all --disablerepo="*" --enablerepo="QianBase-1.x*"',logoutput=True)
    Execute('yum -y upgrade QianBase',logoutput=True)
    
if __name__ == "__main__":
  EDBCLIENT().execute()
