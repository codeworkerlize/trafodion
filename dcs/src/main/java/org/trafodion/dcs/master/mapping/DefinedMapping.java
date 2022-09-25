/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master.mapping;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConfigReader;
import org.trafodion.dcs.master.listener.ConnectionContext;
import org.trafodion.dcs.master.listener.ListenerConstants;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.util.LicenseHolder;
import org.trafodion.dcs.zookeeper.ZkClient;
import com.esgyn.common.ASNodes;
import com.esgyn.common.CGroupHelper;
import com.esgyn.common.TenantNodes;

public class DefinedMapping  {
    private static  final Logger LOG = LoggerFactory.getLogger(DefinedMapping.class);
    private static final String YES = "yes";
    private static final String NO = "no";
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> tenantsMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> slasMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> profilesMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> whitelistMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());

    //fair lock
    private static final java.util.concurrent.locks.Lock lock = new ReentrantLock(true);

    private ZkClient zkc = null;
    private String parentWmsZnode = "";
    
    public DefinedMapping(ConfigReader configReader) throws Exception{
        if (LOG.isInfoEnabled()) {
            LOG.info("Do init for DefinedMapping.");
        }

        zkc = configReader.getZkc();
        parentWmsZnode = configReader.getParentWmsZnode();
        initZkMappings();
        if(LicenseHolder.isMultiTenancyEnabled()) {
            initZkTenants();
        }
        initZkSlas();
        initZkProfiles();
        //Whitelist
        initZKWhitelist();
    }
    class MappingWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    //Set<String> keyset = new HashSet<>(mappingsMap.keySet());

                    List<String> children = zkc.getChildren(znode,new MappingWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i=i+2){
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                mappingsMap.put(child, attributes);
                            }
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("MappingWatcher trigger NodeChildrenChanged. mappingsMap {}.",
                                mappingsMap.keySet());
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }
    class MappingDataWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new MappingDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("MappingDataWatcher EventType.NodeDataChanged <{}>.", child);
                    }
                    mappingsMap.put(child, attributes);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("MappingDataWatcher Data Watcher NodeDeleted <{}>.", child);
                }
                mappingsMap.remove(child);
            }
        }
    }
    private void initZkMappings() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZkMappings <{}>",
                    parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
        }
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS;
        List<String> children = null;
        
        mappingsMap.clear();
          
        children = zkc.getChildren(znode,new MappingWatcher());
        if( ! children.isEmpty()){ 
            for(String child : children) {
                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null) {
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i=i+2){
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    mappingsMap.put(child,attributes);;
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Init mappingsMap {}.", mappingsMap.keySet());
        }
    }

    class TenantWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode, new TenantWatcher());
                    if (!children.isEmpty()) {
                        for (String child : children) {
                            stat = zkc.exists(znode + "/" + child, false);
                            if (stat != null) {
                                LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new TenantDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i = i + 2) {
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                tenantsMap.put(child, attributes);
                            }
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("TenantWatcher trigger NodeChildrenChanged. TenantsMap {}.",
                                tenantsMap.keySet());
                    }
                    //Refresh the CGroupHelper's in memory cache
                    try {
                        CGroupHelper.refresh();
    		        } catch (Exception e) {
                        LOG.error("CGroupHelper.refresh() failed: <{}>.", e.getMessage(), e);
    	            }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class TenantDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new TenantDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    LOG.debug("TenantDataWatcher EventType.NodeDataChanged <{}>.", child);
                    tenantsMap.put(child, attributes);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                //Refresh the CGroupHelper's in memory cache
                try {
		          CGroupHelper.refresh();
		        } catch (Exception e) {
	                LOG.error("CGroupHelper.refresh() failed: <{}>.", e.getMessage(), e);
	            }

            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("TenantDataWatcher NodeDeleted <{}>.", child);
                }
                tenantsMap.remove(child);
            }
        }
    }

    private void initZkTenants() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZkTenants <{}>.",
                    parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS);
        }

        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS;
        List<String> children = null;

        tenantsMap.clear();

        children = zkc.getChildren(znode, new TenantWatcher());
        if (!children.isEmpty()) {
            for (String child : children) {
                stat = zkc.exists(znode + "/" + child, false);
                if (stat != null) {
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode + "/" + child, new TenantDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    tenantsMap.put(child, attributes);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Init tenantsMap {}.", tenantsMap.keySet());
        }
    }

    class SlaWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode, new SlaWatcher());
                    if (!children.isEmpty()) {
                        for (String child : children) {
                            stat = zkc.exists(znode + "/" + child, false);
                            if (stat != null) {
                                LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new SlaDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i = i + 2) {
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                slasMap.put(child, attributes);
                            }
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SlaWatcher trigger NodeChildrenChanged. slasMap {}.",
                                slasMap.keySet());
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class SlaDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new SlaDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SlaDataWatcher EventType.NodeDataChanged <{}>.", child);
                    }
                    slasMap.put(child, attributes);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("SlaDataWatcher NodeDeleted <{}>.", child);
                }
                slasMap.remove(child);
            }
        }
    }

    private void initZkSlas() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZkSlas <{}>.",
                    parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS);
        }

        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS;
        List<String> children = null;

        slasMap.clear();

        children = zkc.getChildren(znode, new SlaWatcher());
        if (!children.isEmpty()) {
            for (String child : children) {
                stat = zkc.exists(znode + "/" + child, false);
                if (stat != null) {
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode + "/" + child, new SlaDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    //For migration from R2.2.x to R2.3.x and higher releases 
                    //isActive flag is a required property as of R2.3.x
                    if(!attributes.containsKey(Constants.IS_ACTIVE)) {
                        attributes.put(Constants.IS_ACTIVE, YES);
                    }
                    slasMap.put(child, attributes);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Init slasMap {}.", slasMap.keySet());
        }
    }


    class ProfileWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode, new ProfileWatcher());
                    if (!children.isEmpty()) {
                        for (String child : children) {
                            stat = zkc.exists(znode + "/" + child, false);
                            if (stat != null) {
                                LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new ProfileDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i = i + 2) {
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                profilesMap.put(child, attributes);
                            }
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ProfileWatcher trigger NodeChildrenChanged. profilesMap {}.",
                                profilesMap.keySet());
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class ProfileDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new ProfileDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ProfileDataWatcher EventType.NodeDataChanged <{}>.", child);
                    }
                    profilesMap.put(child, attributes);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("ProfileDataWatcher NodeDeleted <{}>", child);
                }
                profilesMap.remove(child);
            }
        }
    }

    private void initZkProfiles() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZkProfiles <{}>",
                    parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES);
        }

        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES;
        List<String> children = null;

        profilesMap.clear();

        children = zkc.getChildren(znode, new ProfileWatcher());
        if (!children.isEmpty()) {
            for (String child : children) {
                stat = zkc.exists(znode + "/" + child, false);
                if (stat != null) {
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode + "/" + child, new ProfileDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    profilesMap.put(child, attributes);
                }
            }
        }
        if(LOG.isDebugEnabled()){
            LOG.debug("Init profilesMap {}.", profilesMap.keySet());
        }
    }

    class WhitelistWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();

                    List<String> children = zkc.getChildren(znode, new WhitelistWatcher());
                    //Currently there will only be one child node under the ipwhitelist node
                    if (!children.isEmpty()) {
                        for (String child : children) {
                            stat = zkc.exists(znode + "/" + child, false);
                            if (stat != null) {
                                LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new WhitelistDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i = i + 2) {
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                whitelistMap.put(child, attributes);
                            }
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("WhitelistWatcher trigger NodeChildrenChanged. whitelistMap {}.",
                                whitelistMap.keySet());
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class WhitelistDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new WhitelistDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i = i + 2) {
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("WhitelistDataWatcher EventType.NodeDataChanged <{}>.", child);
                    }
                    whitelistMap.put(child, attributes);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znode = event.getPath();
                String child = znode.substring(znode.lastIndexOf('/') + 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("WhitelistDataWatcher NodeDeleted <{}>", child);
                }
                whitelistMap.remove(child);
            }
        }
    }

    private void initZKWhitelist() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("initZKWhitelist <{}>",
                    parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_IPWHITELIST);
        }

        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_IPWHITELIST;
        List<String> children = null;
        whitelistMap.clear();
        children = zkc.getChildren(znode,new WhitelistWatcher());
        if(!children.isEmpty()){
            for(String child : children){
                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null){
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode + "/" + child,new WhitelistDataWatcher(),stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for(int i = 0; i < tokens.length; i = i + 2){
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    whitelistMap.put(child, attributes);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Init whitelistMap {}.", whitelistMap.keySet());
        }
    }

    private static void sortByValues(Map<String, LinkedHashMap<String, String>> map) {
        List<Map.Entry> list = new LinkedList<Map.Entry>(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                Map<String, String> m1 = ((LinkedHashMap<String, String>) ((Map.Entry) (o1))
                        .getValue());
                String orderNumber1 = m1.get(Constants.ORDER_NUMBER);
                Map<String, String> m2 = ((LinkedHashMap<String, String>) ((Map.Entry) (o2))
                        .getValue());
                String orderNumber2 = m2.get(Constants.ORDER_NUMBER);
                return Integer.valueOf(orderNumber1) - Integer.valueOf(orderNumber2);
            }
        });
        map.clear();
        for (Iterator<Map.Entry> it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            map.put((String) entry.getKey(), (LinkedHashMap<String, String>) entry.getValue());
        }
    }
    public void findProfile(ConnectionContext cc)  throws IOException, KeeperException, InterruptedException  {

        // Mapping
        String sla = "";
        String tenantName = "";
        // Sla
        String cprofile = "";
        String dprofile = "";
        String priority = "";
        String limit = "";
        String sessionLimit = "";
        String throughput = "";
        // Profile
        String hostList = "";
        String hostSelectionMode = Constants.PREFERRED;
        String lastUpdate = "";
        // Tenant
        String tenantSessionLimit = "";

        HashMap<String, String> cc_attributes = cc.getAttributes();
        LinkedHashMap<String,String> map_attributes = null;
        String cc_value = "";
        String map_value = "";
        String userName = cc_attributes.get(Constants.USER_NAME);
        String defaultTenantName = getDefaultTenant(userName);

        if (cc_attributes.isEmpty()){
            int ret = checkTenant(cc.isMultiTenancy(), tenantName, defaultTenantName);
            if (ret == -1){
                cc.setException_nr(ListenerConstants.DcsMasterInvalidTenant_exn);
                throw new IOException("tenantName = " + tenantName +", does not exist, please make sure give the correct value. ");
            } else if (ret == -2){
                cc.setException_nr(ListenerConstants.DcsMasterInvalidTenant_exn);
                throw new IOException("No tenant specified. Please add it in connection string. ");
            } else if (ret == 2) {
                tenantName = defaultTenantName;
            } else if (ret == 3) {
                if (Constants.SYSTEM_WMS_TENANT_NAME.equalsIgnoreCase(cc_attributes.get(Constants.TENANT_NAME))) {
                    tenantName = Constants.SYSTEM_WMS_TENANT_NAME;
                }
            }

            //tenantName = defaultTenantName;
            sla = Constants.DEFAULT_WMS_SLA_NAME;
            priority = "";
            limit = "";
            sessionLimit = "";
            throughput = "";
            cprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            dprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            lastUpdate = "1";
            hostList = "";
            tenantSessionLimit = "";
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Connection attributes are empty : sla :<{}> cprofile :<{}> dprofile :<{}>",
                        sla, cprofile, dprofile);
            }
        } else {
            // if pass check tenant will be a exist tenant or default tenant
            int ret = checkTenant(cc.isMultiTenancy(), cc_attributes.get(Constants.TENANT_NAME), defaultTenantName);
            if (ret == 2) {
                tenantName = defaultTenantName;
                cc_attributes.put(Constants.TENANT_NAME, defaultTenantName);
            } else if (ret == 3) {
                if (Constants.SYSTEM_WMS_TENANT_NAME.equalsIgnoreCase(cc_attributes.get(Constants.TENANT_NAME))) {
                    tenantName = Constants.SYSTEM_WMS_TENANT_NAME;
                } else {
                    tenantName = cc_attributes.get(Constants.TENANT_NAME);
                }
            } else if (ret == -1) {
                cc.setException_nr(ListenerConstants.DcsMasterInvalidTenant_exn);
                throw new IOException("tenantName = " + cc_attributes.get(Constants.TENANT_NAME)
                        + ", does not exist, please make sure give the correct value. ");
            } else if (ret == -2) {
                cc.setException_nr(ListenerConstants.DcsMasterInvalidTenant_exn);
                throw new IOException("No tenant specified. Please add it in connection string. ");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connection context attributes :{}, isMultiTenancy :<{}>", cc_attributes,
                        cc.isMultiTenancy());
            }

            //add lock
            lock.lock();
            boolean bFound;
            try {
                if (!mappingsMap.isEmpty()) {
                    sortByValues(mappingsMap);
                }

                Set<String> maps = new HashSet<>(mappingsMap.keySet());

                bFound = false;
                boolean bNotEqual = false;
                for (String map : maps) {
                    bNotEqual = false;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Checking Mapping <{}>", map);
                    }

                    map_attributes = mappingsMap.get(map);
                    if (map_attributes == null) {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Mapping [{}] value is null", map);
                        }
                        continue;
                    }
                    map_value = map_attributes.get(Constants.IS_ACTIVE);
                    map_value = map_value == null || map_value.length() == 0 || map_value.equals(NO)
                            ? NO : YES;

                    if (map_value.equals(NO)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Mapping <{}> is not active and we go to next map", map);
                        }
                        continue;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Mapping <{}> is active. OrderNumber <{}>", map,
                                map_attributes.get(Constants.ORDER_NUMBER));
                    }

                    Set<String> keys = map_attributes.keySet();
                    for (String key : keys) {
                        cc_value = cc_attributes.get(key);
                        map_value = map_attributes.get(key);

                        bNotEqual = false;
                        switch (key) {
                            case Constants.USER_NAME:
                            case Constants.APPLICATION_NAME:
                            case Constants.SESSION_NAME:
                            case Constants.ROLE_NAME:
                            case Constants.CLIENT_IP_ADDRESS:
                            case Constants.CLIENT_HOST_NAME:
                            case Constants.TENANT_NAME:
                                if (!cc.isMultiTenancy() && Constants.TENANT_NAME.equals(key)) {
                                    break;
                                }
                                if (map_value == null || map_value.length() == 0) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "Mapping <{}>, no value for key=<{}> any value is accepted - we go to next key",
                                                map, key);
                                    }
                                    continue;
                                }

                                if (cc_value == null || cc_value.length() == 0) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "Connection context, no value for key=<{}> match failed - we go to next mapping",
                                                key);
                                    }
                                    bNotEqual = true;
                                    break;
                                }

                                if (!map_value.equalsIgnoreCase(cc_value)) {
                                    if (Constants.TENANT_NAME.equals(key) && (
                                            "NONE".equalsIgnoreCase(map_value) || "NULL"
                                                    .equalsIgnoreCase(map_value))) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug(
                                                    "Mapping <{}> key <{}> value <{}> has no tenant, any value is accepted - we go to next key",
                                                    map, key, map_value);
                                        }
                                    } else {
                                        bNotEqual = true;
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug(
                                                    "Mapping <{}> key <{}> value <{}> not equal connection context value <{}>, match failed - we go to next mapping",
                                                    map, key, map_value, cc_value);
                                        }
                                    }
                                } else {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "Mapping <{}> key <{}> value <{}> equals connection context value <{}>, match",
                                                map, key, map_value, cc_value);
                                    }
                                }
                                break;
                        } // end of switch
                        if (bNotEqual) //go to next mapping
                        {
                            break;
                        }
                    } // end of for
                    sla = map_attributes.get(Constants.SLA);
                    if (!bNotEqual && checkSlaActive(sla)) {
                        bFound = true;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Using mapping <{}> with attributes {} selected SLA=<{}>",
                                    map,
                                    map_attributes, sla);
                        }
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
            if (!bFound) {
                sla = Constants.DEFAULT_WMS_SLA_NAME;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Select SLA <{}>, TENANT <{}>.", sla, tenantName);
            }

            Map<String, String> slaAttr = slasMap.get(sla);
            cprofile = slaAttr.get(Constants.ON_CONNECT_PROFILE);
            if ("".equals(cprofile)) {
                cprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            }
            dprofile = slaAttr.get(Constants.ON_DISCONNECT_PROFILE);
            if ("".equals(dprofile)) {
                dprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            }
            priority = slaAttr.get(Constants.PRIORITY);
            limit = slaAttr.get(Constants.LIMIT);
            sessionLimit = slaAttr.get(Constants.SESSION_LIMIT);
            throughput = slaAttr.get(Constants.THROUGHPUT);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Using Profile <{}> to get hostList & hostSelectionMode, profilesMap = {}",
                        cprofile, profilesMap);
            }
            Map<String, String> profileAttr = profilesMap.get(cprofile);
            lastUpdate = profileAttr.get(Constants.LAST_UPDATE);
            String tkn = profileAttr.get(Constants.HOST_LIST);
            if (null != tkn && tkn.length() > 0)
                hostList = tkn;
            tkn = profileAttr.get(Constants.HOST_SELECTION_MODE);
            if (null != tkn && tkn.length() > 0)
                hostSelectionMode = tkn;
        }

        if (cc.isMultiTenancy()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Tenant name <{}>, tenants map {}", tenantName, tenantsMap);
            }

            LinkedHashMap<String, String> tenantValues = tenantsMap.get(tenantName);

            cc.setTenantName(tenantName);

            TenantNodes tenantNodes = null;
            String serializedNodes = tenantValues.get(Constants.NODES);

            if (serializedNodes != null && serializedNodes.length() > 0)
                // new model
                tenantNodes = TenantNodes.fromSerializedForm(serializedNodes);
            else {
                // old model
                String computeUnitStr = tenantValues.get(Constants.COMPUTE_UNITS);
                String maxNodesStr = tenantValues.get(Constants.MAX_NODES);
                String affinityStr = tenantValues.get(Constants.AFFINITY);
                int computeUnits = 1;
                int clusterSize = 1;
                int affinity = 0;

                if (computeUnitStr.length() > 0)
                    computeUnits = Integer.parseInt(computeUnitStr);
                if (maxNodesStr.length() > 0)
                    clusterSize = Integer.parseInt(maxNodesStr);
                if (affinityStr.length() > 0)
                    affinity = Integer.parseInt(affinityStr);

                if (tenantName.equals(Constants.SYSTEM_WMS_TENANT_NAME)) {
                    // the default tenant stretches across the entire
                    // cluster, ignore the actual values in the metadata
                    computeUnits = -1;
                    clusterSize = -1;
                    affinity = -1;
                }

                tenantNodes = new ASNodes(computeUnits,
                                          clusterSize,
                                          affinity);
            }
            cc.setTenantNodes(tenantNodes);
            cc.setTenantSessionLimit(tenantValues.get(Constants.SESSION_LIMIT));

            if (tenantNodes != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "computeUnits = <{}>, serializedNodes = <{}>, tenantSessionLimit = <{}>.",
                            tenantNodes.getSize(), tenantNodes.toSerializedForm(),
                            cc.getTenantSessionLimit());
                }
            }
        }
        cc.setSla(sla);
        cc.setPriority(priority);
        cc.setLimit(limit);
        cc.setSessionLimit(sessionLimit);
        cc.setThroughput(throughput);
        cc.setConnectProfile(cprofile);
        cc.setDisconnectProfile(dprofile);
        cc.setLastUpdate(lastUpdate);
        cc.setHostList(hostList);
        cc.setProfHostSelectionMode(hostSelectionMode);
        if (LOG.isInfoEnabled()) {
            LOG.info(
                "sla : <{}>, priority : <{}>, limit : <{}>, sessionlimit : <{}>, throughput : <{}>, connect profile : <{}>, disconnect profile : <{}>, hostList : <{}>, lastUpdate : <{}>",
                sla, priority, limit, sessionLimit, throughput, cprofile, dprofile, hostList,
                lastUpdate);
        }
    }

    private boolean checkSlaActive(String sla) {
        if (Constants.DEFAULT_WMS_SLA_NAME.equals(sla)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("selected default sla : <{}>", sla);
            }
            return true;
        }
        String isActive = YES;
        if(slasMap.containsKey(sla) && slasMap.get(sla).containsKey(Constants.IS_ACTIVE)) {
             isActive = slasMap.get(sla).get(Constants.IS_ACTIVE).trim().toLowerCase();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("selected sla : <{}>, isActive : <{}>", sla, isActive);
        }
        return YES.equals(isActive);
    }

    /**
     * check whether tenant valid, if tenantName is empty use the default one, if the default one is empty throw exception.
     * Or check the whether tenantName exists, if not throw exception
     * @param isMultiTenancy
     * @param tenantName
     * @param defaultTenantName
     * @return 1 means pass
     *         2 means use the default tenant instead of current tenant
     *         3 means tenant passed check
     *         -2 means no tenant
     *         -1 means tenantName not exist
     * @throws IOException
     */
    private int checkTenant(boolean isMultiTenancy, String tenantName, String defaultTenantName) throws IOException {
        if (! isMultiTenancy) {
            return 1;
        }

        if (null == tenantName || "".equals(tenantName)){
       	    if (Constants.SYSTEM_WMS_TENANT_NAME.equals(defaultTenantName.toUpperCase())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Tenant name is null. Use default tenant");
                }
                return 2;
            } else {
                LOG.error("Tenant name is null. Reject.");
            	return -2;
            }
        	//tenantName = defaultTenantName;
            //return 2;
            //if ("".equals(tenantName)){
            //    throw new IOException("no default tenant, please do configure first. ");
            //} else {
            //    return 2;
            //}
            //throw new IOException("no tenant, please add it in connection string. ");
            //return -2;
        }
        if (Constants.SYSTEM_WMS_TENANT_NAME.equals(tenantName.toUpperCase())) {
            return 3;
        }
        if (tenantsMap.containsKey(tenantName)) {
            return 3;
        } else {
            //throw new IOException("tenantName = " + tenantName +", does not exist, please make sure give the correct value. ");
            LOG.error("TenantName <{}>, does not exist, please make sure give the correct value.", tenantName);
            return -1;
        }

    }

    private String getDefaultTenant(String userName) {
	String defTenant = "";
       
        //If no tenants have been created yet, use system tenant as default tenant
    	if(tenantsMap.isEmpty())
    	    defTenant = Constants.SYSTEM_WMS_TENANT_NAME;
    	
        //If no user tenants have been created yet, use system tenant as default tenant
    	if(tenantsMap.size() == 1) {
    		for (Entry<String, LinkedHashMap<String, String>> entry : tenantsMap.entrySet()) {
    			if(entry.getKey().equalsIgnoreCase(Constants.SYSTEM_WMS_TENANT_NAME))
    				defTenant = Constants.SYSTEM_WMS_TENANT_NAME;
    		}
    	}

        //If user is elevated user, use system tenant as default tenant
	String dbAdminUserName = GetJavaProperty.getEnv("DB_ADMIN_USER");
	String dbRootUserName = GetJavaProperty.getEnv("DB_ROOT_USER");
	if(userName != null) {
            if(dbRootUserName != null && userName.equalsIgnoreCase(dbRootUserName))
 		defTenant = Constants.SYSTEM_WMS_TENANT_NAME;

            if(dbAdminUserName != null && userName.equalsIgnoreCase(dbAdminUserName))
 		defTenant = Constants.SYSTEM_WMS_TENANT_NAME;
        }

        //If there is at least 1 user created tenant exists, then return empty string as default tenant
        //User has to explictly specify tenant if atleast 1 user created tenant exists. Cannot use default tenant.
        if (LOG.isDebugEnabled()) {
            LOG.debug("Default tenant = <{}>", defTenant);
        }

        return defTenant;
    }

    //ip whitelist check
    public void checkWhitelist(ConnectionContext cc) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking Mapping......");
        }
        if (whitelistMap.isEmpty()) {
            LOG.warn("Ipwhitelist node data is empty......");
            return;
        }
        //Currently there will only be one child node under the ipwhitelist node
        LinkedHashMap<String, String> whitelistHashMap = whitelistMap
                .get(Constants.DEFAULT_WMS_WHITELIST_NAME);
        String isopen = whitelistHashMap.get(Constants.IS_OPEN);
        if (isopen == null || isopen.isEmpty()) {
            LOG.warn("Whether the ip whitelist is turned on indicates abnormal......");
            return;
        }
        //ip whitelist is open
        if (isopen.equalsIgnoreCase("yes")) {
            String whitelist = whitelistHashMap.get(Constants.IP_WHITELIST);
            if (whitelist == null || whitelist.isEmpty()) {
                LOG.warn("Ip whitelist is empty");
                return;
            }
            HashMap<String, String> cc_attributes = cc.getAttributes();
            String clientIpAddress = cc_attributes.get(Constants.CLIENT_IP_ADDRESS);
            if (clientIpAddress.isEmpty() || !isIp(clientIpAddress)) {
                LOG.error("ClientIpAddress information is abnormal. clientIpAddress : <{}>",
                        clientIpAddress);
                throw new SQLException("ClientIpAddress information is abnormal. clientIpAddress : "
                        + clientIpAddress);
            }
            //whitelist
            //ip1;ip2;ip3;ip4......
            String ipList[] = whitelist.split(";");
            for (int i = 0; i < ipList.length; i++) {
                if (isIp(ipList[i]) && clientIpAddress.equalsIgnoreCase(ipList[i])) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<{}> In the whitelist...... and list ip <{}>", clientIpAddress,
                                ipList[i]);
                    }
                    return;
                }
            }
            LOG.error("Illegal IP, IP is not in the whitelist. clientIpAddress : <{}>",
                    clientIpAddress);
            throw new SQLException("Illegal IP, IP is not in the whitelist. clientIpAddress : "
                    + clientIpAddress);
        } else {//ip whitelist is close
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ip whitelist is close.");
            }
        }
    }

    private static boolean isIp(String ipAddress) {
        String ip = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }
}
