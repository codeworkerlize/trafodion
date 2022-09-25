import json
from tempfile import TemporaryFile
from resource_management import *

class Service(Script):

  def service_check(self, env):
    import params
    cmd=format('source ~/.bashrc ; http_trafcheck.py -i 20 -d 3; (( $? < 2 )) && exit 0')
    Execute(cmd,user=params.traf_user,logoutput=True)

    
if __name__ == "__main__":
  Service().execute()
