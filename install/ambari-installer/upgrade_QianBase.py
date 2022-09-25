#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import sys
reload(sys)
import json
import base64
import getpass

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from collections import defaultdict

class AmbariHelper(object):
    def __init__(self, user, pwd, url, cluster_name=''):
        self.hg = ParseHttp(user, pwd, False)
        self.url = url
        self.v1_url = '%s/api/v1/clusters' % self.url
        try:
            self.v1_info = self.hg.get(self.v1_url)
        except Exception as e:
            print ('Error connecting to ambari server %s' % str(e))
            exit(-1)
        

        if not cluster_name:
           try:
               self.cluster_name = self.v1_info['items'][0]['name']
               self.stackver = self.v1_info['items'][0]['version']
           except (Exception):
               try:
                   self.cluster_name = self.v1_info['items'][0]['Clusters']['cluster_name']
                   self.stackver = self.v1_info['items'][0]['Clusters']['version']
               except Exception as e:
                   print('Failed to get cluster info from management url: %s'  % str(e))
                   exit(-1)
           self.cluster_url = '%s/%s' % (self.v1_url, self.cluster_name.replace(' ', '%20'))
        else:
           self.cluster_name = cluster_name
           self.cluster_url = '%s/%s' % (self.v1_url, self.cluster_name.replace(' ', '%20'))
           try:
               self.v1_info = self.hg.get('%s?fields=Clusters/version' % self.cluster_url)
           except Exception as e:
               print ('Error connecting to ambari server %s' % str(e))
               exit(-1)
           self.stackver = self.v1_info['items'][0]['Clusters']['version']


    def upgrade_esgyn_configs(self):
        # New config types causes Ambari database check to fail.
        # Start ambari using flag: --skip-database-check
        # Then use this script to populate the missing configs
        if self.stackver.startswith('HDP'):
            stack_url = self.url + "/api/v1/stacks/HDP/versions/" + self.stackver[4:]
            #print stack_url + '\n'
        else:
            print 'Hadoop Stack (%s) is not HDP' % (self.stackver)
            exit(-1)


        # New config types added in 2.4.x
        # initialize new configs (clientodbc & trafodion-site)
        for configtype in ['clientodbc','trafodion-site']:
          try:
            curconf = self.hg.get('%s/configurations?type=%s' % (self.cluster_url,configtype))
            if curconf['items'] != []:
                print "Found %s config, no update needed" % (configtype)
                continue
            else:
                print "Updating %s..." % (configtype)
          except (IndexError, KeyError):
            print 'Failed to get stack configuration data from Ambari.'
            print 'Install current esgyndb_ambari package and restart Ambari.'
            exit(-1)

          try:
            stconf = self.hg.get('%s/services?configurations/StackConfigurations/type=%s.xml&fields=configurations/StackConfigurations/property_value' % (stack_url,configtype))
            PropList={}
            for config in stconf['items'][0]["configurations"]:
              pname = config["StackConfigurations"]["property_name"]
              pvalue =  config["StackConfigurations"]["property_value"]
              PropList[pname] = pvalue
          except (IndexError, KeyError):
            print 'Failed to get stack configuration data from Ambari'
            exit(-1)

          data = {
            'Clusters': {
              'desired_configs': {
                'type': "%s" % (configtype),
                'tag': "initial2.4",
                'properties' : PropList
              }
            }
          }
          self.hg.put(self.cluster_url, data)

        # New properties added in 2.4.x - 2.7.0
        # initialize new properties
        NewProps = {'dcs-env': ['dcs.aws.access.id',
                               'dcs.aws.secret.key',
                               'dcs.aws.region',
                               'aws_cred_content',
                               'aws_config_content'],
                    'dbm-env': ['enable_https',
                               'keystore_pwd'],
                    'rest-site': ['rest.keystore',
                               'rest.ssl.password'],
                    'clientconf': ['content'],
                    'trafodion-env': ['traf.limit.cpu',
                               'traf.limit.mem',
                               'traf.log.dir',
                               'traf.var.dir',
                               'traf.cluster.id',
                               'traf.ldap.search.group.base',
                               'traf.ldap.search.group.object.class',
                               'traf.ldap.search.group.member.attribute',
                               'traf.ldap.search.group.name.attribute',
                               'traf.security.enabled',
                               'traf.principal',
                               'traf.keytab',
                               'content',
                               'ldap_content']}
        # over-write existing config with new values for these templates
        UpdateProps = {'dcs-env': [],
                       'dbm-env': [],
                       'rest-site': [],
                       'clientconf': [],
                       'trafodion-env': ['content']}

        # Find tags of current configs
        cltags = self.hg.get('%s?fields=Clusters/desired_configs' % (self.cluster_url))

        for configtype in ['dcs-env','dbm-env','rest-site','trafodion-env']:
          # start with current config
          tag = cltags["Clusters"]["desired_configs"][configtype]["tag"]
          curconf = self.hg.get('%s/configurations?type=%s&tag=%s' % (self.cluster_url,configtype,tag))
          PropList = curconf['items'][0]['properties']

          # find stack definition for new properties
          stconf = self.hg.get('%s/services?configurations/StackConfigurations/type=%s.xml&fields=configurations/StackConfigurations/property_value' % (stack_url,configtype))
          StackProp = {}
          for config in stconf['items'][0]["configurations"]:
            pname = config["StackConfigurations"]["property_name"]
            pvalue =  config["StackConfigurations"]["property_value"]
            StackProp[pname] = pvalue

          print "Updating %s..." % (configtype)
          for prop in NewProps[configtype]:
            if prop not in PropList or prop in UpdateProps[configtype]:
              print "  %s" % prop
              PropList[prop] = StackProp[prop]

          data = {
            'Clusters': {
              'desired_configs': {
                'type': "%s" % (configtype),
                'tag': "initial2.7",
                'properties' : PropList
              }
            }
          }
          self.hg.put(self.cluster_url, data)

        return

    def upgrade_esgyn(self):
        # execute node actions
        hosts = []
        comps = ['QIANBASE_NODE', 'QIANBASE_DCS', 'QIANBASE_DBMGR', 'QIANBASE_CLIENT']
        prevComp = ''
        try:
            for comp in comps:
                compcfg = self.hg.get('%s/services/QIANBASE/components/%s' % (self.cluster_url, comp))
                if compcfg.has_key('host_components'):
                    for hc in compcfg['host_components']:
                        h = hc['HostRoles']['host_name']
                        if h not in hosts:
                            hosts.append(h)
                            if prevComp != comp:
                                prevComp = comp
                                print 'Sending upgrade request to all QianBase hosts for component %s'  % (comp)
                                data = {
                                  'RequestInfo': {
                                    'command': 'Upgrade_%s' % (comp),
                                    'context': 'Upgrade QianBase component %s' % (comp),
                                    'operation_level': {
                                      'level': 'HOST_COMPONENT',
                                      'cluster_name': 'HDP',
                                      'hostcomponent_name': '%s' % (comp)
                                    }
                                  },
                                  'Requests/resource_filters': [
                                    {
                                      'service_name': 'ESGYNDB',
                                      'hosts_predicate': 'HostRoles/component_name=%s' % (comp),
                                      'component_name': '%s' % (comp)
                                    }
                                  ]
                                }
                                #print data
                                cmdurl = '%s/requests' % (self.cluster_url)
                                self.hg.post(cmdurl, data)
                                
        except (IndexError, KeyError):
            print 'Error'
        
class ParseHttp(object):
    def __init__(self, user, passwd, json_type=True):
        # httplib2 is not installed by default
        try:
            import httplib2
        except ImportError:
            print('Python module httplib2 is not found. Install python-httplib2 first.')
            exit(-1)

        self.user = user
        self.passwd = passwd
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.h.add_credentials(self.user, self.passwd)
        self.headers = {}
        self.headers['X-Requested-By'] = 'ambari'
        if json_type:
            self.headers['Content-Type'] = 'application/json'
        self.headers['Authorization'] = 'Basic %s' % (base64.b64encode('%s:%s' % (self.user, self.passwd)))

    def _request(self, url, method, body=None):
        try:
            resp, content = self.h.request(url, method, headers=self.headers, body=body)
            # return code is not 2xx and 409(for ambari blueprint)
            if not (resp.status == 409 or 200 <= resp.status < 300):
                print('Error return code {0} when {1} to URL {2} response: {3}'.format(resp.status, method.lower(), url, content))
                exit(-1)
            return content
        except Exception as exc:
            print('Error with {0} using URL {1}. Reason: {2}'.format(method.lower(), url, exc))
            raise exc

    def get(self, url, content_type='json'):
        try:
            if content_type == 'xml':
                result_str = self._request(url, 'GET')
                return ET.ElementTree(ET.fromstring(result_str))

            return defaultdict(str, json.loads(self._request(url, 'GET')))
        except ValueError as e :
            print(str(e))
            print('Failed to get data from URL, check password if URL requires authentication')

    def put(self, url, config):
        if not isinstance(config, dict): print('Wrong HTTP PUT parameter, should be a dict')
        result = self._request(url, 'PUT', body=json.dumps(config))
        if result: return defaultdict(str, json.loads(result))

    def post(self, url, config=None):
        try:
            if config:
                if not isinstance(config, dict): print('Wrong HTTP POST parameter, should be a dict')
                body = json.dumps(config)
            else:
                body = None

            result = self._request(url, 'POST', body=body)
            if result: return defaultdict(str, json.loads(result))

        except ValueError as ve:
            print('Failed to send command to URL: %s' % ve)


def get_user_input(prompt, isPasswd=False): 
    while True:
        try:
            if isPasswd:
                val = getpass.getpass(prompt)
            else:
                val = raw_input(prompt)             
            
            if val:
                return val
            
        except Exception as e:
            print ('Error reading input %s' % e )
        

def main():
    url = get_user_input("Enter the ambari url. (include http or https) : ")
    if not ('http:' in url or 'https:' in url):
        url = 'http://' + url
    
    admin_user = get_user_input("Enter the ambari admin user name : ")
    admin_pwd = get_user_input('Enter the admin password : ', True)
    ah = AmbariHelper(admin_user, admin_pwd, url)
    ah.upgrade_esgyn_configs()
    ah.upgrade_esgyn()
    print 'Check QianBase upgrade results in Ambari console...'
   
    exit(0)
        
if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print '\nAborted...'

