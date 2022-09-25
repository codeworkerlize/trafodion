#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018-2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os,sys,subprocess,json,shlex,time
from xml.dom import minidom
from optparse import OptionParser
import ssl

class RESTConfig(object):
    def __init__(self):
        restconfig_dir = os.environ.get('REST_CONF_DIR')
        self.props = ''
        if not restconfig_dir:
            name = os.environ.get('TRAF_CONF')
            if name:
                restconfig_dir=name+"/rest"
        if restconfig_dir:
            doc = minidom.parse(restconfig_dir+"/rest-site.xml")
            self.props = doc.getElementsByTagName("property")
        else:
            print 'Cannot find REST configuration file'
            sys.exit(1)

    def get_val(self, propName):
        if self.props:
            for prop in self.props:
                tagName = prop.getElementsByTagName("name")[0]
                pname=tagName.childNodes[0].data
                if propName == pname:
                    tagValue = prop.getElementsByTagName("value")[0]
                    if len(tagValue.childNodes) > 0:
                      pvalue = tagValue.childNodes[0].data
                    else:
                      pvalue = ""
                    #print("%s:%s" % (pname,pvalue))       
                    return pvalue
        return ''

def get_options():
    usage = 'usage: %prog [-c <component name>]\n'
    usage += '  Script to check Trafodion status using RestServer.'
    parser = OptionParser(usage=usage)
    parser.add_option("-c", dest="comp_name",  metavar="COMPNAME", default="all", 
                      help="A component name to check status for [ all | fndn | dtm | dcs | rms | rest | mgblty ]")
    parser.add_option("-i", dest="max_checks",  type="int", metavar="ITERS", default=2, 
                      help=" Number of times the check for Trafodion processes will be done")
    parser.add_option("-d", dest="sleep_delay",  type="int", metavar="DELAYT", default=1, 
                      help="Duration of sleep (in seconds) between each check")

    (options, args) = parser.parse_args()
    return options

def getDCSMasterNodes():
    nodes=[]
    try:
        nodes = getDcsMasterNodesFromMastersFile()
        if len(nodes) == 0:
            nodes = getDCSMasterNodesFromClientConfig()
        if len(nodes) == 0:
            nodes = ['localhost']
    except:
        nodes = ['localhost']
    
    return nodes
        
def getDcsMasterNodesFromMastersFile():    
    nodes=[]
    try:
        dcsMasterFile = os.environ.get('TRAF_CONF') + '/dcs/masters'
        if os.path.isfile(dcsMasterFile):
            with open(dcsMasterFile, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    nodes.append(line)
    except:
        nodes = []
    
    return nodes

def getDCSMasterNodesFromClientConfig():
    rNodes = []
    try:
        #REST Server runs on all DCS Master nodes
        #Get list of DCS Master nodes from the client_config
        command = "env -i bash -c 'source /etc/trafodion/conf/client_config && env'"
        stdout = _run_cmd(command, True)
        stdout = stdout.splitlines()
        for line in stdout:
            (key, _, value) = line.partition("=")
            #print key
            if key == 'DCS_MASTER_QUORUM':
                rNodes=[]
                dcsNodes = value.split(",")
                for n in dcsNodes:
                    nName = n.split(':')
                    rNodes.append(nName[0])  
    except:
        rNodes = []
    return rNodes  
                
def _run_cmd(cmd, useShell=True):
    p = subprocess.Popen(cmd, executable='/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=useShell)
    stdout,stderr = p.communicate()
    if p.returncode != 0:
        raise StandardError('Failed to execute command %s' % cmd)
    return stdout.strip()    

def main():
    options = get_options()
    if options.comp_name:
        if options.comp_name not in ['dtm', 'rms', 'dcs', 'all', 'rest', 'mgblty', 'fndn']:
            print 'Invalid component name ' + options.comp_name
            print 'Please specify one of [ all | fndn | dtm | dcs | rms | rest | mgblty]'
            sys.exit(1)
        
    try:
        # For Python 3.0 and later
        from urllib.request import urlopen
    except ImportError:
        # Fall back to Python 2's urllib2
        from urllib2 import urlopen
        
        
    #Component names for display purposes
    compNames = { "DTM": "DTM", "RMS": "RMS", "DCS": "DCS", "DCSSERVER": "DcsServer", "DCSMASTER": "DcsMaster",
                 "RESTSERVER": "RestServer", "REST": "RestServer", "DBMGR": "DB Manager", "MXOSRVR": "mxosrvr",
                 "HREGIONSERVER": "HRegionServer" }
    #Get list of REST Server nodes.
    restServerNodes = []
    try:
        restServerNodes = getDCSMasterNodes()
   
    except Exception as e:
        print 'Cannot load client_config. Using localhost'
    
    #Find REST Server port and if it is using https or http
    restConfig = RESTConfig()
    sslEnabled = restConfig.get_val("rest.ssl.password")
    restPort = 4200
    protocol = "http://"
    context = None
    
    if sslEnabled:
        restPort = restConfig.get_val("rest.https.port")
        protocol = "https://"
        try:
           context = ssl._create_unverified_context()
        except:
           context = None
    else:
        restPort = restConfig.get_val("rest.port")
    
    print ''
    if options.comp_name == 'all':
        print '*** Checking Trafodion Environment ***'
    else:
        print '*** Checking ' + compNames[options.comp_name.upper()] + ' status ***'
        
    iter_count = 0
    while iter_count < options.max_checks:
        sys.stdout.flush()
        if iter_count > 0:
            time.sleep(options.sleep_delay)
            
        iter_count += 1 
        print 'Checking attempt: %s, max attempts: %s' % (iter_count, options.max_checks)
        
        #Iterate through REST Server nodes until you see a node on which monitor is up       
        errorMessage = ''
        useNode = False
        for node in restServerNodes:
            url = '%s%s:%s/v1/servers/node-health' % (protocol, node, restPort)
            try:

                if context:
                   response = urlopen(url, context=context)
                else:
                   response = urlopen(url)

                jr = json.loads(response.read())
                useNode = False
                if "PROCESSES" in jr:
                    for proc in jr["PROCESSES"]:
                        #If DTM is up, we know monitor is up
                        if proc["PROCESS"] == "DTM" and proc["ACTUAL"] > 0:
                            useNode = True;
                            break
                
                #If monitor is up on RESTServer node, proceed to get Trafodion status
                if useNode:
                    comp_name = '' if options.comp_name == 'all' else '/'+options.comp_name
                    url = '%s%s:%s/v1/servers%s' % (protocol, node, restPort, comp_name)
                    try:

                        if context:
                           response = urlopen(url, context=context)
                        else:
                           response = urlopen(url)

                        jr = json.loads(response.read())
                        
                        #If state is not up, repeat iteration
                        if jr['SUBSTATE'] != 'OPERATIONAL' and iter_count != options.max_checks:
                            break
                        
                        print ''
                        if options.comp_name == 'all':
                            print 'Trafodion environment is ' + (jr['STATE']).lower() + ' and ' + jr['SUBSTATE'].lower() + '!'
                        print ''
    
                        print '{0:<24} {1:<12} {2:<10} {3:<}'.format('Component','Configured','Actual', 'Down')
                        print '{0:<24} {1:<12} {2:<10} {3:<}'.format('---------','----------','------', '----')
                        for row in jr['PROCESSES']:
                            print "{0:<24} {1:<12} {2:<10} {3:<}".format(compNames[row['PROCESS']], row['CONFIGURED'], row['ACTUAL'], row['DOWN'])
                        
                        if jr['STATE'] == 'UP':
                           sys.exit(0)
                        elif jr['STATE'] == 'PARTIALLY UP' and jr['SUBSTATE'] == 'OPERATIONAL':
                           sys.exit(1)
                        elif jr['STATE'] == 'PARTIALLY UP' and jr['SUBSTATE'] == 'NOT OPERATIONAL':
                           sys.exit(2)
                        else:
                           sys.exit(255)

                        return
                                 
                    except Exception as e:
                        errorMessage = 'Error getting trafodion status from node %s : %s ' % (node, e)
                    break
                        
            except Exception as e:
                errorMessage = 'Error getting trafodion status from node %s : %s ' % (node, e)
        
    if options.comp_name == 'all' and not useNode:
       print '\n*** ERROR *** Unable to obtain service status.'
       print errorMessage
       print 'Trafodion monitor seems to be down in the connection master nodes.'
       print 'Please run the check from another node.'
       print ''
       sys.exit(3)
    else:    
       print '\n%s' % errorMessage

    sys.exit(3)
    
if __name__ == '__main__':
    main()
