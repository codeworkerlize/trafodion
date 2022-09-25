#!/usr/bin/env ambari-python-wrap
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import imp
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"

class ESGYNDB23ServiceAdvisor(service_advisor.DefaultStackAdvisor):
 
  def colocateService(self, hostsComponentsMap, serviceComponents):
    # Co-locate ESGYNDB_NODE with HBase REGIONSERVERS, if no hosts allocated
    traf_node = [component for component in serviceComponents if component["StackServiceComponents"]["component_name"] == "QIANBASE_NODE"][0]
    if not self.isComponentHostsPopulated(traf_node):
      for hostName in hostsComponentsMap.keys():
        hostComponents = hostsComponentsMap[hostName]
        if {"name": "HBASE_REGIONSERVER"} in hostComponents and {"name": "QIANBASE_NODE"} not in hostComponents:
          hostsComponentsMap[hostName].append( { "name": "QIANBASE_NODE" } )
        if {"name": "HBASE_REGIONSERVER"} not in hostComponents and {"name": "QIANBASE_NODE"} in hostComponents:
          hostComponents.remove({"name": "QIANBASE_NODE"})

    # Place ESGYNDB_DCS on ZOOKEEPER_SERVER nodes by default
    for hostName in hostsComponentsMap.keys():
      hostComponents = hostsComponentsMap[hostName]
      if {"name": "ZOOKEEPER_SERVER"} in hostComponents:
        if {"name": "QIANBASE_DCS"} not in hostComponents:
           hostsComponentsMap[hostName].append( { "name": "QIANBASE_DCS" } )
      else:
        if {"name": "QIANBASE_DCS"} in hostComponents:
           hostComponents.remove({"name": "QIANBASE_DCS"})

    # Place ESGYNDB_DBMGR on ESGYNDB_DCS nodes
    for hostName in hostsComponentsMap.keys():
      hostComponents = hostsComponentsMap[hostName]
      if {"name": "QIANBASE_DCS"} in hostComponents:
        if {"name": "QIANBASE_DBMGR"} not in hostComponents:
           hostsComponentsMap[hostName].append( { "name": "QIANBASE_DBMGR" } )
      else:
        if {"name": "QIANBASE_DBMGR"} in hostComponents:
           hostComponents.remove({"name": "QIANBASE_DBMGR"})

  def getServiceComponentLayoutValidations(self, services, hosts):
    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]

    trafNodeHosts = self.getHosts(componentsList, "QIANBASE_NODE")
    trafConnHosts = self.getHosts(componentsList, "QIANBASE_DCS")
    trafDBMHosts = self.getHosts(componentsList, "QIANBASE_DBMGR")
    trafClientHosts = self.getHosts(componentsList, "QIANBASE_CLIENT")
    regionHosts = self.getHosts(componentsList, "HBASE_REGIONSERVER")
    hmasterHosts = self.getHosts(componentsList, "HBASE_MASTER")
    hiveHosts = self.getHosts(componentsList, "HIVE_CLIENT")
    dataHosts = self.getHosts(componentsList, "DATANODE")

    items = []

    # Generate ERROR if any ESGYNDB_DBMGR is not colocated with DCS
    mismatchHosts = sorted(set(trafConnHosts).symmetric_difference(set(trafDBMHosts)))
    if len(mismatchHosts) > 0:
      hostsString = ', '.join(mismatchHosts)
      message = "EsgynDB DataBase Manager should be co-located with EsgynDB Connection Servers. " \
                "{0} host(s) do not satisfy the colocation requirement: {1}".format(len(mismatchHosts), hostsString)
      items.append( { "type": 'host-component', "level": 'ERROR', "message": message, "component-name": 'QIANBASE_DBMGR' } )

    # Generate WARNING if any ESGYNDB_NODE is not colocated with a DATANODE
    mismatchHosts = sorted(set(trafNodeHosts).symmetric_difference(set(dataHosts)))
    if len(mismatchHosts) > 0:
      hostsString = ', '.join(mismatchHosts)
      message = "EsgynDB Nodes should be co-located with HDFS Data Nodes. " \
                "{0} host(s) do not satisfy the colocation recommendation: {1}".format(len(mismatchHosts), hostsString)
      items.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'QIANBASE_NODE' } )

    # Generate WARNING if any ESGYNDB_NODE is not colocated with a HBASE_REGIONSERVER
    mismatchHosts = sorted(set(trafNodeHosts).symmetric_difference(set(regionHosts)))
    if len(mismatchHosts) > 0:
      hostsString = ', '.join(mismatchHosts)
      message = "EsgynDB Nodes should be co-located with HBase Region Servers. " \
                "{0} host(s) do not satisfy the colocation recommendation: {1}".format(len(mismatchHosts), hostsString)
      items.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'QIANBASE_NODE' } )

    # Generate WARNING if any ESGYNDB_NODE is not colocated with a HIVE_CLIENT
    mismatchHosts = sorted(set(trafNodeHosts).difference(set(hiveHosts)))
    if len(mismatchHosts) > 0:
      hostsString = ', '.join(mismatchHosts)
      message = "Hive Client should be installed on all EsgynDB Nodes. " \
                "{0} host(s) do not satisfy recommendation: {1}".format(len(mismatchHosts), hostsString)
      items.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'QIANBASE_NODE' } )

    # Generate WARNING if any HBase Master is not colocated with a ESGYNDB_CLIENT
    mismatchHosts = sorted(set(hmasterHosts).difference(set(trafClientHosts)))
    if len(mismatchHosts) > 0:
      hostsString = ', '.join(mismatchHosts)
      message = "EsgynDB Client should be installed on HBase Master Nodes. " \
                "{0} host(s) do not satisfy recommendation: {1}".format(len(mismatchHosts), hostsString)
      items.append( { "type": 'host-component', "level": 'WARN', "message": message, "component-name": 'QIANBASE_CLIENT' } )

    return items
 
  def getServiceConfigurationRecommendations(self, configurations, clusterSummary, services, hosts):

    # Update HBASE properties in hbase-site
    if "hbase-site" in services["configurations"]:
      hbase_site = services["configurations"]["hbase-site"]["properties"]
      putHbaseSiteProperty = self.putProperty(configurations, "hbase-site", services)

      for property, desired_value in self.getHbaseSiteDesiredValues().iteritems():
        if property not in hbase_site:
          putHbaseSiteProperty(property, desired_value)
        elif hbase_site[property] != desired_value:
          if property == "hbase.bulkload.staging.dir":
             # don't modify unless it is empty
             if hbase_site[property] == '':
                putHbaseSiteProperty(property, desired_value)
          # for any other property, modify regardless
          else:
             putHbaseSiteProperty(property, desired_value)

   # Update HDFS properties in hdfs-site
   # if "hdfs-site" in services["configurations"]:
   #   hdfs_site = services["configurations"]["hdfs-site"]["properties"]
   #   putHdfsSiteProperty = self.putProperty(configurations, "hdfs-site", services)

   #   for property, desired_value in self.getHdfsSiteDesiredValues().iteritems():
   #     if property not in hdfs_site or hdfs_site[property] != desired_value:
   #       putHdfsSiteProperty(property, desired_value)

    # Update ZOOKEEPER properties in zoo.cfg
    if "zoo.cfg" in services["configurations"]:
      zoo_cfg = services["configurations"]["zoo.cfg"]["properties"]
      putZooCfgProperty = self.putProperty(configurations, "zoo.cfg", services)

      for property, desired_value in self.getZooCfgDesiredValues().iteritems():
        if property not in zoo_cfg or zoo_cfg[property] != desired_value:
          putZooCfgProperty(property, desired_value)

 
  def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
    items = []

    if "dcs-site" in services["configurations"]:
      val_items = []
      cfg = configurations["dcs-site"]["properties"]
      if cfg["dcs.master.floating.ip"] == "true" and cfg["dcs.master.floating.ip.external.ip.address"] == "":
            message = "DCS High Availability requires a Floating IP address"
            val_items.append({"config-name": "dcs.master.floating.ip.external.ip.address", "item": self.getErrorItem(message)})
            items.extend(self.toConfigurationValidationProblems(val_items, "dcs-site"))

    if "trafodion-env" in services["configurations"]:
      val_items = []
      cfg = configurations["trafodion-env"]["properties"]
      if cfg["traf.ldap.enabled"] == "YES" and cfg["traf.ldap.hosts"] == "":
            message = "LDAP authentication requires one or more LDAP servers"
            val_items.append({"config-name": "traf.ldap.hosts", "item": self.getErrorItem(message)})
      if cfg["traf.ldap.encrypt"] != "0" and cfg["traf.ldap.certpath"] == "":
            message = "LDAP encryption may require a certificate file"
            val_items.append({"config-name": "traf.ldap.certpath", "item": self.getWarnItem(message)})
      items.extend(self.toConfigurationValidationProblems(val_items, "trafodion-env"))

    if "hbase-site" in services["configurations"]:
      val_items = []
      cfg = configurations["hbase-site"]["properties"]
      for property, desired_value in self.getHbaseSiteDesiredValues().iteritems():
         if property not in cfg:
            message = "EsgynDB recommends value of " + desired_value
            val_items.append({"config-name": property, "item": self.getWarnItem(message)})
         # use any staging dir, but complain if there is none
         elif property == "hbase.bulkload.staging.dir":
            if cfg[property] == '':
               message = "EsgynDB recommends value of " + desired_value
               val_items.append({"config-name": property, "item": self.getWarnItem(message)})
         elif property == "hbase.superuser":
            if cfg[property].find("trafodion") == -1:
               message = "EsgynDB requires addition 'trafodion' be listed in HBase superuser list."
               val_items.append({"config-name": property, "item": self.getErrorItem(message)})
         elif cfg[property] != desired_value:
            message = "EsgynDB recommends value of " + desired_value
            val_items.append({"config-name": property, "item": self.getWarnItem(message)})
      items.extend(self.toConfigurationValidationProblems(val_items, "hbase-site"))

    #if "hdfs-site" in services["configurations"]:
    #  val_items = []
    #  cfg = configurations["hdfs-site"]["properties"]
    #  for property, desired_value in self.getHdfsSiteDesiredValues().iteritems():
    #     if property not in cfg or cfg[property] != desired_value:
    #        message = "EsgynDB recommends value of " + desired_value
    #        val_items.append({"config-name": property, "item": self.getWarnItem(message)})
    #  items.extend(self.toConfigurationValidationProblems(val_items, "hdfs-site"))

    if "zoo.cfg" in services["configurations"]:
      val_items = []
      cfg = configurations["zoo.cfg"]["properties"]
      for property, desired_value in self.getZooCfgDesiredValues().iteritems():
         if property not in cfg or cfg[property] != desired_value:
            message = "EsgynDB recommends value of " + desired_value
            val_items.append({"config-name": property, "item": self.getWarnItem(message)})
      items.extend(self.toConfigurationValidationProblems(val_items, "zoo.cfg"))

    return items

  ##### Desired values in other service configs
  def getHbaseSiteDesiredValues(self):
    desired = {
        "hbase.snapshot.master.timeoutMillis": "600000",
        "hbase.hregion.impl": "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion",
        "hbase.regionserver.region.split.policy": "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy",
        "hbase.snapshot.enabled": "true",
        "hbase.bulkload.staging.dir": "/apps/hbase/staging",
        "hbase.regionserver.region.transactional.tlog": "true",
        "hbase.snapshot.region.timeout": "600000",
        "hbase.regionserver.lease.period": "3600000",
        "hbase.hstore.blockingStoreFiles": "200",
        "hbase.superuser": "hbase,trafodion",
        "hbase.rootdir.perms": "750",
        "hbase.coprocessor.regionserver.classes": "org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionServerObserver"
        # These tuning values conflict with HDP defaults, preventing a smooth install
        #"hbase.hregion.memstore.flush.size": "268435456",
        #"hbase.hregion.memstore.block.multiplier": "8",
    }
    return desired

  #def getHdfsSiteDesiredValues(self):
  #  desired = {
  #      "dfs.namenode.acls.enabled": "true"
  #  }
  #  return desired

  def getZooCfgDesiredValues(self):
    desired = {
        "maxClientCnxns": "0"
    }
    return desired
