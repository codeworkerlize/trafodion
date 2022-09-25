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
package org.trafodion.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.ws.rs.core.Response;

import org.trafodion.rest.Constants;
import org.trafodion.rest.RestConstants;
import org.trafodion.rest.zookeeper.ZkClient;

import com.esgyn.common.LicenseHelper;

import org.trafodion.rest.script.ScriptManager;
import org.trafodion.rest.script.ScriptContext;
import org.trafodion.rest.util.RestConfiguration;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet implements RestConstants {
	private static final Log LOG = LogFactory.getLog(RESTServlet.class);
	private static RESTServlet INSTANCE;
	private Configuration conf = null;
	private static ScriptManager scriptManager;
	private String parentDcsZnode = null;
	private String parentWmsZnode = null;
	private ZkClient zkc = null;
	private final ArrayList<String> runningServers = new ArrayList<String>();
	private final ArrayList<String> registeredServers = new ArrayList<String>();
        private String trafInstanceId = null;
	
    private final Map<String, LinkedHashMap<String,String>> slasMap = Collections.synchronizedMap(new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> profilesMap = Collections.synchronizedMap(new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = Collections.synchronizedMap(new LinkedHashMap<String, LinkedHashMap<String,String>>());
    private final Map<String, LinkedHashMap<String,String>> tenantsMap = Collections.synchronizedMap(new LinkedHashMap<String, LinkedHashMap<String,String>>());

    private final HashSet<String> orderNumbers = new HashSet<String>();
    private static boolean cacheInitialized = false;
	/**
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance() 
	throws IOException {
		assert(INSTANCE != null);
		return INSTANCE;
	}

	/**
	 * @param conf Existing configuration to use in rest servlet
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance(Configuration conf)
	throws IOException {
		if (INSTANCE == null) {
			INSTANCE = new RESTServlet(conf);
		}
		return INSTANCE;
	}
	

	public ScriptManager getScriptManager(){
		return this.scriptManager.getInstance();
	}

	public synchronized static void stop() {
		if (INSTANCE != null)  INSTANCE = null;
	}

	/**
	 * Constructor with existing configuration
	 * @param conf existing configuration
	 * @throws IOException.
	 */
	RESTServlet(Configuration conf) throws IOException {
		this.conf = conf;
		this.parentDcsZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
                this.trafInstanceId = System.getenv("TRAF_INSTANCE_ID");
                if (this.trafInstanceId != null) {
		   this.parentWmsZnode = this.parentDcsZnode.substring(0,this.parentDcsZnode.lastIndexOf("/"));
                } else {
		   this.parentWmsZnode = this.parentDcsZnode;
                }
		try {
		    zkc = new ZkClient();
		    zkc.connect();
		    LOG.info("Connected to ZooKeeper");
		} catch (Exception e) {
            e.printStackTrace();
		    LOG.error("Error connecting to zookeeper : " + e);
		    System.exit(1);
		}
		initializeCache();

	}
	
	public boolean initializeCache() {
		if(!cacheInitialized) {
			try {
			    getZkRunning();
			    getZkRegistered();
			    initZkSlas();
			    initZkProfiles();
			    initZkMappings();
			    initZkTenants();
			    cacheInitialized = true;
			} catch (Exception e) {
			    LOG.error("Error initializing cache : " + e);
			}	
		}
		return cacheInitialized;
	}

	Configuration getConfiguration() {
		return conf;
	}

	/**
	 * Helper method to determine if server should
	 * only respond to GET HTTP method requests.
	 * @return boolean for server read-only state
	 */
	boolean isReadOnly() {
		return getConfiguration().getBoolean("trafodion.rest.readonly", false);
	}
	class RunningWatcher implements Watcher {
	    public void process(WatchedEvent event) {
	        if(event.getType() == Event.EventType.NodeChildrenChanged) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Running children changed [" + event.getPath() + "]");
	            try {
	                getZkRunning();
	            } catch (Exception e) {
	                e.printStackTrace();
	                if(LOG.isErrorEnabled())
	                    LOG.error(e);
	            }
	        }
	    }
	}
	class RegisteredWatcher implements Watcher {
	    public void process(WatchedEvent event) {
	        if(event.getType() == Event.EventType.NodeChildrenChanged) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Registered children changed [" + event.getPath() + "]");
	            try {
	                getZkRegistered();
	            } catch (Exception e) {
	                e.printStackTrace();
	                if(LOG.isErrorEnabled())
	                    LOG.error(e);
	            }
	        }
	    }
	}
    class SlaWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Sla children changed [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    
                    Set<String> keyset = new HashSet<>(slasMap.keySet());
                    
                    List<String> children = zkc.getChildren(znode,new SlaWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                if (keyset.contains(child)){
                                    keyset.remove(child);
                                    continue;
                                }
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new SlaDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                                if((tokens.length % 2) != 0){
                                    LOG.error("SlaWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                                }
                                else {
                                    for (int i = 0; i < tokens.length; i=i+2){
                                        attributes.put(tokens[i], tokens[i + 1]);
                                    }
                                    synchronized(slasMap){
                                        slasMap.put(child, attributes);
                                    }
                                }
                            }
                        }
                        for (String child : keyset) {
                            synchronized(slasMap){
                                slasMap.remove(child);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class SlaDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new SlaDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                    if((tokens.length % 2) != 0){
                        LOG.error("SlaDataWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                    }
                    else {
                        for (int i = 0; i < tokens.length; i=i+2){
                            attributes.put(tokens[i], tokens[i + 1]);
                        }
                        synchronized (slasMap){
                            slasMap.put(child,attributes);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class ProfileWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Profile children changed [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    
                    Set<String> keyset = new HashSet<>(profilesMap.keySet());
                    
                    List<String> children = zkc.getChildren(znode,new ProfileWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                if (keyset.contains(child)){
                                    keyset.remove(child);
                                    continue;
                                }
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new ProfileDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                                if((tokens.length % 2) != 0){
                                    LOG.error("ProfileWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                                }
                                else {
                                    for (int i = 0; i < tokens.length; i=i+2){
                                        attributes.put(tokens[i], tokens[i + 1]);
                                    }
                                    synchronized (profilesMap){
                                        profilesMap.put(child, attributes);
                                    }
                                }
                            }
                        }
                        for (String child : keyset) {
                           synchronized (profilesMap){
                               profilesMap.remove(child);
                           }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
   class ProfileDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new ProfileDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                    if((tokens.length % 2) != 0){
                        LOG.error("ProfileDataWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                    }
                    else {
                        for (int i = 0; i < tokens.length; i=i+2){
                            attributes.put(tokens[i], tokens[i + 1]);
                        }
                        synchronized (profilesMap){
                            profilesMap.put(child,attributes);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class MappingWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Mapping children changed [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    
                    Set<String> keyset = new HashSet<>(mappingsMap.keySet());
                    List<String> children = zkc.getChildren(znode,new MappingWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                if (keyset.contains(child)){
                                    keyset.remove(child);
                                    continue;
                                }
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                                if((tokens.length % 2) != 0){
                                    LOG.error("MappingWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                                }
                                else {
                                    for (int i = 0; i < tokens.length; i=i+2){
                                        attributes.put(tokens[i], tokens[i + 1]);
                                    }
                                    synchronized (mappingsMap){
                                        mappingsMap.put(child, attributes);
                                    }
                                }
                            }
                        }
                        for (String child : keyset) {
                            synchronized (mappingsMap){
                                mappingsMap.remove(child);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class MappingDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new MappingDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                    if((tokens.length % 2) != 0){
                        LOG.error("MappingDataWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                    }
                    else {
                        for (int i = 0; i < tokens.length; i=i+2){
                            attributes.put(tokens[i], tokens[i + 1]);
                        }
                        synchronized (mappingsMap){
                            mappingsMap.put(child,attributes);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class TenantWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Tenant children changed [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    
                    Set<String> keyset = new HashSet<>(tenantsMap.keySet());
                    
                    List<String> children = zkc.getChildren(znode,new TenantWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                if (keyset.contains(child)){
                                    keyset.remove(child);
                                    continue;
                                }
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new TenantDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                                if((tokens.length % 2) != 0){
                                    LOG.error("TenantWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                                }
                                else {
                                    for (int i = 0; i < tokens.length; i=i+2){
                                        attributes.put(tokens[i], tokens[i + 1]);
                                    }
                                    synchronized (tenantsMap){
                                        tenantsMap.put(child, attributes);
                                    }
                                }
                            }
                        }
                        for (String child : keyset) {
                           synchronized (tenantsMap){
                        	   tenantsMap.remove(child);
                           }
                        }
                    }else{
                        synchronized (tenantsMap){
                     	   tenantsMap.clear(); //Once we support defaultTenants, this would be a no-op
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
   class TenantDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new TenantDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                    if((tokens.length % 2) != 0){
                        LOG.error("TenantDataWatcher [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                    }
                    else {
                        for (int i = 0; i < tokens.length; i=i+2){
                            attributes.put(tokens[i], tokens[i + 1]);
                        }
                        synchronized (tenantsMap){
                        	tenantsMap.put(child,attributes);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }    
	private List<String> getChildren(String znode,Watcher watcher) throws Exception {
	    List<String> children = null;
	    children = zkc.getChildren(znode,watcher);
	    if( ! children.isEmpty()) 
	        Collections.sort(children);
	    return children;
	}
	private synchronized void getZkRunning() throws Exception {
	    if(LOG.isDebugEnabled())
	        LOG.debug("Reading " + parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING);
	    List<String> children = getChildren(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING, new RunningWatcher());

	    if( ! children.isEmpty()) {
        	//If dcsstop.sh is executed and rest server is not restarted, the nodes get appended to the
        	//existing list and we end with duplicate entries for the same servers with different timestamps
        	//Since the runningServers are only for the DcsServers, it is ok and not expensive to reconstruct 
        	//the list every time
			runningServers.clear();
			
	        for(String child : children) {

	        	//If stop-dcs.sh is executed and DCS_MANAGES_ZK then zookeeper is stopped abruptly.
	            //Second scenario is when ZooKeeper fails for some reason regardless of whether DCS
	            //manages it. When either happens the DcsServer running znodes still exist in ZooKeeper
	            //and we see them at next startup. When they eventually timeout
	            //we get node deleted events for a server that no longer exists. So, only recognize
	            //DcsServer running znodes that have timestamps after last DcsMaster startup.
	            Scanner scn = new Scanner(child);
	            scn.useDelimiter(":");
	            String hostName = scn.next(); 
	            String instance = scn.next(); 
                /* remove dead-code for avoiding to java.util.NoSuchElementException
	            int infoPort = Integer.parseInt(scn.next()); 
	            long serverStartTimestamp = Long.parseLong(scn.next());
                */
	            scn.close();

	            //if(serverStartTimestamp < startupTimestamp) 
	            //    continue;

	            if(! runningServers.contains(child)) {
	                if(LOG.isDebugEnabled())
	                    LOG.debug("Watching running [" + child + "]");
	                zkc.exists(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING + "/" + child, new RunningWatcher());
	                runningServers.add(child);
	            }
	        }
	        //metrics.setTotalRunning(runningServers.size());
	    } //else {
	    //  metrics.setTotalRunning(0);
	    //}
	}
	private synchronized void getZkRegistered() throws Exception {
	    if(LOG.isDebugEnabled())
	        LOG.debug("Reading " + parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED);
	    List<String> children =  getChildren(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED, new RegisteredWatcher());

	    if( ! children.isEmpty()) {
	        registeredServers.clear();
	        for(String child : children) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Registered [" + child + "]");
	            registeredServers.add(child);
	        }
	        //metrics.setTotalRegistered(registeredServers.size());
	    } else {
	        //metrics.setTotalRegistered(0);
	    }
	}
    private synchronized void initZkSlas() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkSlas " + parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS;
        
        List<String> children = null;
        synchronized(slasMap){
            slasMap.clear();
            children = zkc.getChildren(znode,new SlaWatcher());
            if( ! children.isEmpty()){ 
                for(String child : children) {
                    if(LOG.isDebugEnabled())
                        LOG.debug("child [" + child + "]");
                    stat = zkc.exists(znode + "/" + child,false);
                    if(stat != null) {
                        LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                        data = zkc.getData(znode + "/" + child, new SlaDataWatcher(), stat);
                        String delims = "[=:]";
                        String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                        if((tokens.length % 2) != 0){
                            LOG.error("Sla [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                        }
                        else {
                            for (int i = 0; i < tokens.length; i=i+2){
                                attributes.put(tokens[i], tokens[i + 1]);
                            }
                            //For migration from R2.2.x to R2.3.x and higher releases 
                            //isActive flag is a required property as of R2.3.x
                            if(!attributes.containsKey(Constants.IS_ACTIVE)) {
                            	attributes.put(Constants.IS_ACTIVE, "yes");
                            }
                            synchronized (slasMap){
                                slasMap.put(child,attributes);
                            }
                        }
                    }
                }
            }
        }
    }
    private synchronized void initZkProfiles() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkProfiles " + parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES;
        
        List<String> children = null;
        synchronized(profilesMap){
            profilesMap.clear();
            children = zkc.getChildren(znode,new ProfileWatcher());
            if( ! children.isEmpty()){ 
                for(String child : children) {
                    if(LOG.isDebugEnabled())
                        LOG.debug("child [" + child + "]");
                    stat = zkc.exists(znode + "/" + child,false);
                    if(stat != null) {
                        LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                        data = zkc.getData(znode + "/" + child, new ProfileDataWatcher(), stat);
                        String delims = "[=:]";
                        String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                        if((tokens.length % 2) != 0){
                            LOG.error("Profile [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                        }
                        else {
                            for (int i = 0; i < tokens.length; i=i+2){
                                attributes.put(tokens[i], tokens[i + 1]);
                            }
                            synchronized(profilesMap){
                                profilesMap.put(child,attributes);
                            }
                        }
                    }
                }
            }
        }
    }
    private synchronized void initZkMappings() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkProfiles " + parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS;
        
        List<String> children = null;
        synchronized(mappingsMap){
            mappingsMap.clear();
            children = zkc.getChildren(znode,new MappingWatcher());
            if( ! children.isEmpty()){ 
                for(String child : children) {
                    if(LOG.isDebugEnabled())
                        LOG.debug("child [" + child + "]");
                    stat = zkc.exists(znode + "/" + child,false);
                    if(stat != null) {
                        LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                        data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                        String delims = "[=:]";
                        String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                        if((tokens.length % 2) != 0){
                            LOG.error("Mapp [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                        }
                        else {
                            for (int i = 0; i < tokens.length; i=i+2){
                                attributes.put(tokens[i], tokens[i + 1]);
                            }
                            synchronized(mappingsMap){
                                mappingsMap.put(child,attributes);
                            }
                        }
                    }
                }
            }
        }
    }
    private synchronized void initZkTenants() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkTenants " + parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS;
        
        List<String> children = null;
        synchronized(tenantsMap){
        	tenantsMap.clear();
            children = zkc.getChildren(znode,new TenantWatcher());
            if( ! children.isEmpty()){ 
                for(String child : children) {
                    if(LOG.isDebugEnabled())
                        LOG.debug("child [" + child + "]");
                    stat = zkc.exists(znode + "/" + child,false);
                    if(stat != null) {
                        LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                        data = zkc.getData(znode + "/" + child, new TenantDataWatcher(), stat);
                        String delims = "[=:]";
                        String[] tokens = (new String(data, Charset.defaultCharset())).split(delims);
                        if((tokens.length % 2) != 0){
                            LOG.error("Tenant [" + child + "] incorrect format :" + (new String(data, Charset.defaultCharset())));
                        }
                        else {
                            for (int i = 0; i < tokens.length; i=i+2){
                                attributes.put(tokens[i], tokens[i + 1]);
                            }
                            synchronized(tenantsMap){
                                tenantsMap.put(child,attributes);
                            }
                        }
                    }
                }
            }
        }
    }
    private static void sortByValues(Map<String, LinkedHashMap<String,String>> map) { 
        List<Map.Entry> list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                 Map<String,String> m1 = ((LinkedHashMap<String,String>)((Map.Entry)(o1)).getValue());
                 String orderNumber1 = m1.get(Constants.ORDER_NUMBER);
                 Map<String,String> m2 = ((LinkedHashMap<String,String>)((Map.Entry)(o2)).getValue());
                 String orderNumber2 = m2.get(Constants.ORDER_NUMBER);
                 return Integer.valueOf(orderNumber1) -Integer.valueOf(orderNumber2);
             }
        });
        map.clear();
        for (Iterator<Map.Entry> it = list.iterator(); it.hasNext();) {
               Map.Entry entry = (Map.Entry)it.next();
               map.put((String)entry.getKey(), (LinkedHashMap<String,String>)entry.getValue());
        } 
   }
 	public synchronized List<RunningServer> getDcsServersList() {
	    ArrayList<RunningServer> serverList = new ArrayList<RunningServer>();
	    Stat stat = null;
	    byte[] data = null;

	    //int totalAvailable = 0;
	    //int totalConnecting = 0;
	    //int totalConnected = 0;

	    if(LOG.isDebugEnabled())
	        LOG.debug("Begin getServersList()");

	    if( ! runningServers.isEmpty()) {
	        for(String aRunningServer : runningServers) {
	            RunningServer runningServer = new RunningServer();
	            Scanner scn = new Scanner(aRunningServer);
	            scn.useDelimiter(":");
	            runningServer.setHostname(scn.next());
	            runningServer.setInstance(scn.next());
                /* remove dead-code for avoiding to java.util.NoSuchElementException
	            runningServer.setInfoPort(Integer.parseInt(scn.next()));
	            runningServer.setStartTime(Long.parseLong(scn.next()));
                */
	            scn.close();

	            if( ! registeredServers.isEmpty()) {
	                for(String aRegisteredServer : registeredServers) {
	                    if(aRegisteredServer.contains(runningServer.getHostname() + ":" + runningServer.getInstance() + ":")){
	                        try {
	                            RegisteredServer registeredServer = new RegisteredServer();
	                            stat = zkc.exists(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + aRegisteredServer,false);
	                            if(stat != null) {
	                                data = zkc.getData(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + aRegisteredServer, false, stat);
	                                scn = new Scanner(new String(data, Charset.defaultCharset()));
	                                scn.useDelimiter(":");
	                                if(LOG.isDebugEnabled())
	                                    LOG.debug("getDataRegistered [" + new String(data, Charset.defaultCharset()) + "]");
	                                registeredServer.setState(scn.next());
	                                String state = registeredServer.getState();
	                                //if(state.equals("AVAILABLE"))
	                                //    totalAvailable += 1;
	                                //else if(state.equals("CONNECTING"))
	                                //    totalConnecting += 1;
	                                //else if(state.equals("CONNECTED"))
	                                //    totalConnected += 1;
	                                registeredServer.setCreateTime(stat.getCtime());
	                                registeredServer.setTimestamp(Long.parseLong(scn.next()));
	                                registeredServer.setDialogueId(scn.next());  
	                                registeredServer.setNid(scn.next());
	                                registeredServer.setPid(scn.next());
	                                registeredServer.setProcessName(scn.next());
	                                registeredServer.setIpAddress(scn.next());
	                                registeredServer.setPort(scn.next());
	                                registeredServer.setClientName(scn.next());
	                                registeredServer.setClientIpAddress(scn.next());
	                                registeredServer.setClientPort(scn.next());
	                                registeredServer.setClientAppl(scn.next());
	                                if(state.equals("CONNECTED") || state.equals("CONNECTING")){
	                                    registeredServer.setSla(scn.next());
	                                    registeredServer.setConnectProfile(scn.next());
                                        registeredServer.setDisconnectProfile(scn.next());
                                        if(state.equals("CONNECTED")) {
                                        	registeredServer.setConnectTime(stat.getMtime());
	                                    	registeredServer.setConnectedInterval(((new Date()).getTime() - stat.getMtime())/1000);
                                        }
	                                    String updateTime = scn.next(); //not used right now
	                                    registeredServer.setUserName(scn.next());
	                                    if(LicenseHelper.isMultiTenancyEnabled()){
		                                    registeredServer.setTenantName(scn.next());
	                                    }else{
		                                    registeredServer.setTenantName("");
	                                    }
	                                }
                                    registeredServer.setIsRegistered();
	                                scn.close();
	                                runningServer.getRegistered().add(registeredServer);
	                            }
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                            if(LOG.isErrorEnabled())
	                                LOG.error("Exception: " + e.getMessage());
	                        }
	                    }
	                }
	            }

	            serverList.add(runningServer);
	        }
	    }

	    // metrics.setTotalAvailable(totalAvailable);
	    // metrics.setTotalConnecting(totalConnecting);
	    // metrics.setTotalConnected(totalConnected);

	    Collections.sort(serverList, new Comparator<RunningServer>(){
	        public int compare(RunningServer s1, RunningServer s2){
	            if(s1.getInstanceIntValue() == s2.getInstanceIntValue())
	                return 0;
	            return s1.getInstanceIntValue() < s2.getInstanceIntValue() ? -1 : 1;
	        }
	    });

	    if(LOG.isDebugEnabled())
	        LOG.debug("End getServersList()");

	    return serverList;
	}
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsSlasMap() throws Exception {

        if(LOG.isDebugEnabled())
            LOG.debug("getWmsSlasMap()");
 
        synchronized(slasMap){
            return slasMap;
        }
    }
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsProfilesMap() throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getWmsProfilesMap()");
        synchronized(profilesMap){
            return profilesMap;
        }
    }
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsMappingsMap() throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getWmsMappingsMap()");
        synchronized(mappingsMap){
            if( ! mappingsMap.isEmpty())
                sortByValues(mappingsMap);
            return mappingsMap;
        }
    }
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsTenantsMap() throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getWmsTenantsMap()");
        synchronized(tenantsMap){
            return tenantsMap;
        }
    }
    public synchronized String postWmsSla(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = null;
        String tdata = "";
        String result = "OK";
        boolean connProfileFound = false;
        
        if(LOG.isDebugEnabled())
            LOG.debug("postWmsSla [" + name + "] sdata :" + sdata);
        try {
            if(name.equals(Constants.DEFAULT_WMS_SLA_NAME)== false){
                String delims = "[=:]";
                String[] tokens = sdata.split(delims);
                if((tokens.length % 2) != 0){
                    LOG.error("postWmsSla [" + name + "] incorrect format :" + sdata);
                }
                else {
                    for (int i = 0; i < tokens.length; i=i+2){
                        switch(tokens[i]){
                        case Constants.IS_DEFAULT:
                            tokens[i + 1] = "no";
                            break;
                        case Constants.ON_CONNECT_PROFILE:
                            String connProfileName = tokens[i + 1];
                            if(connProfileName == null || connProfileName.length() < 1){
                                result = "OnConnect profile is required and cannot be empty";
                                LOG.error(result);
                                status = Response.Status.NOT_ACCEPTABLE;
                                throw new ResponseException("406 missing dependency");
                            }
                            connProfileFound = true;
                        case Constants.ON_DISCONNECT_PROFILE:
                            String profileName = tokens[i + 1];
                            if(profileName != null && profileName.length() > 0){
                                Set<String> profiles = new HashSet<>(profilesMap.keySet());
                                if(!profiles.contains(profileName)){
                                    result = "Profile " + profileName + " does not exist!";
                                    LOG.error(result);
                                    status = Response.Status.NOT_ACCEPTABLE;
                                    throw new ResponseException("406 missing dependency");
                               }
                            }
                            break;
                        }
                        if(i>0)
                            tdata = tdata + ":";
                        tdata = tdata + tokens[i] + "=" + tokens[i + 1];
                    }
                    if(!connProfileFound){
                        result = "OnConnect profile is required and cannot be empty";
                        LOG.error(result);
                        status = Response.Status.NOT_ACCEPTABLE;
                        throw new ResponseException("406 missing dependency");
                	
                    }
                    data = tdata.getBytes();
                    stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,false);
                    if (stat == null) {
                        status = Response.Status.CREATED;
                        zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,
                                data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } else {
                        zkc.setData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, data, -1);
                    }
                }
            }
        } catch (Exception e){}
        result = status.toString() + ": " + result;
        if (status == Response.Status.OK){
            result = "200 " + result;
        }
        else if (status == Response.Status.CREATED){
            result = "201 " + result;
        }
        else if (status == Response.Status.NOT_MODIFIED){
             result = "304 " + result;
        }
        else if (status == Response.Status.NOT_ACCEPTABLE){
             result = "406 " + result;
        }

        return result;
     }
    public synchronized Response.Status postWmsProfile(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = null;
        String tdata = "";
        
        if(LOG.isDebugEnabled())
            LOG.debug("sdata :" + sdata);
        if(name.equals(Constants.DEFAULT_WMS_PROFILE_NAME)== false){
            String delims = "[=:]";
            String[] tokens = sdata.split(delims);
            if((tokens.length % 2) != 0){
                LOG.error("postWmsProfile [" + name + "] incorrect format :" + sdata);
            }
            else {
                for (int i = 0; i < tokens.length; i=i+2){
                    switch(tokens[i]){
                    case Constants.IS_DEFAULT:
                        tokens[i + 1] = "no";
                        break;
                    }
                    if(i>0)
                        tdata = tdata + ":";
                    tdata = tdata + tokens[i] + "=" + tokens[i + 1];
                }
                data = tdata.getBytes();
                stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,false);
                if (stat == null) {
                    status = Response.Status.CREATED;
                    zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,
                            data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } else {
                    zkc.setData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, data, -1);
                }
            }
        }
        return status;
    }

    public synchronized Response.Status postWmsTenant(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = null;
        String tdata = "";
        
        if(LOG.isDebugEnabled())
            LOG.debug("sdata :" + sdata);
        if(name.equals(Constants.DEFAULT_WMS_TENANT_NAME)== false){
            String delims = "[=:]";
            String[] tokens = sdata.split(delims);
            if((tokens.length % 2) != 0){
                LOG.error("postWmsTenant [" + name + "] incorrect format :" + sdata);
            }
            else {
                for (int i = 0; i < tokens.length; i=i+2){
                    switch(tokens[i]){
                    case Constants.IS_DEFAULT:
                        tokens[i + 1] = "no";
                        break;
                    }
                    if(i>0)
                        tdata = tdata + ":";
                    tdata = tdata + tokens[i] + "=" + tokens[i + 1];
                }
                data = tdata.getBytes();
                stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name,false);
                if (stat == null) {
                    status = Response.Status.CREATED;
                    zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name,
                            data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } else {
                    zkc.setData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name, data, -1);
                }
            }
        }
        return status;
    
    }
    
    class ResponseException extends Exception {
        public ResponseException(String msg){
          super(msg);
        }
    }

    public synchronized String postWmsMapping(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        String result = "OK";
        byte[] data = null;
        short defaultOrderNumber = Short.valueOf(Constants.DEFAULT_ORDER_NUMBER);
        short currentOrderNumber = 0;
        String tdata = "";
        boolean slaFound = false;

        orderNumbers.clear();
        for(Map.Entry<String, LinkedHashMap<String,String>> entry : mappingsMap.entrySet()) {
            String key = entry.getKey();
            if(!key.equals(name)){
	      LinkedHashMap<String,String> value = entry.getValue();
	      String orderNumber = value.get(Constants.ORDER_NUMBER);
	      orderNumbers.add(orderNumber);
	    }
        }
       
        if(LOG.isDebugEnabled())
            LOG.debug("postWmsMapping [" + name + "] sdata :" + sdata);
      try {
        if(name.equals(Constants.DEFAULT_WMS_MAPPING_NAME)== false){
            String delims = "[=:]";
            String[] tokens = sdata.split(delims);
            if((tokens.length % 2) != 0){
                LOG.error("postWmsMapping [" + name + "] incorrect format :" + sdata);
                result = "Incorrect mapping format :" + sdata;
                status = Response.Status.NOT_ACCEPTABLE;
                throw new ResponseException("406 incorrect format");
            }
            else {
                for (int i = 0; i < tokens.length; i=i+2){
                    switch(tokens[i]){
                    case Constants.IS_DEFAULT:
                        tokens[i + 1] = "no";
                        break;
                    case Constants.SLA:
                        String slaName = tokens[i + 1];
                        if(slaName == null || slaName.length() < 1){
                            result = "SLA is required and cannot be empty";
                            LOG.error(result);
                            status = Response.Status.NOT_ACCEPTABLE;
                            throw new ResponseException("406 missing dependency");
                        }else{
                            Set<String> slas = new HashSet<>(slasMap.keySet());
                            if(!slas.contains(slaName)){
                                result = "SLA " + slaName + " does not exist!";
                                LOG.error(result);
                                status = Response.Status.NOT_ACCEPTABLE;
                                throw new ResponseException("406 missing dependency");
                           }
                        }
                        slaFound = true;
                        break;
                    case Constants.ORDER_NUMBER:
                        currentOrderNumber = Short.valueOf(tokens[i + 1]);
                        if (currentOrderNumber <= 0 || currentOrderNumber >= defaultOrderNumber){
			    //orderNumber out of range
                            if(LOG.isDebugEnabled())
                               LOG.debug("postWmsMapping [" + name + "] orderNumber out of range :" + currentOrderNumber + ". Allowed range is between 1 and " + (defaultOrderNumber - 1));
                            result = "Order number " + currentOrderNumber + " out of range. Allowed range is between 1 and " + (defaultOrderNumber - 1);
                            status = Response.Status.NOT_ACCEPTABLE;
                            throw new ResponseException("406 Out of range");
                        }
                        break;
                    }
                    if(i>0)
                        tdata = tdata + ":";
                    tdata = tdata + tokens[i] + "=" + tokens[i + 1];
                }
                if(!slaFound){
                    result = "SLA is required and cannot be empty";
                    LOG.error(result);
                    status = Response.Status.NOT_ACCEPTABLE;
                    throw new ResponseException("406 missing dependency");
                }
                String orderNumber = Short.toString(currentOrderNumber);
                if(orderNumbers.contains(orderNumber)){
		  //duplicated order number
		  if(LOG.isDebugEnabled())
		    LOG.debug("postWmsMapping [" + name + "] orderNumber is already in use by another mapping :" + orderNumber);
                  result = "Order number " + orderNumber + " is already in use by another mapping";
                  status = Response.Status.NOT_MODIFIED;
                  throw new ResponseException("304 duplicated number");
                }
                data = tdata.getBytes();
                stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,false);
                if (stat == null) {
                    status = Response.Status.CREATED;
                    result = "Mapping created.";
                    zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,
                      data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                      CreateMode.PERSISTENT);
                } else {
                    zkc.setData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, data, -1);
                }
              }
            }
        } catch (Exception e){}

        result = status.toString() + ": " + result;
        if (status == Response.Status.OK){
            result = "200 " + result;
        }
        else if (status == Response.Status.CREATED){
            result = "201 " + result;
        }
        else if (status == Response.Status.NOT_MODIFIED){
             result = "304 " + result;
        }
        else if (status == Response.Status.NOT_ACCEPTABLE){
             result = "406 " + result;
        }

        return result;
    }
    public synchronized String deleteWmsSla(String name) throws Exception{
	String result = "200 OK.";
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);
        if(name.equals(Constants.DEFAULT_WMS_SLA_NAME)== false){
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,false);
            if(stat != null){
        	//Reject delete request if sla is being used by mappings
                for(Map.Entry<String, LinkedHashMap<String,String>> entry : mappingsMap.entrySet()) {
                    LinkedHashMap<String,String> value = entry.getValue();
        	    String slaName = value.get(Constants.SLA);
        	    if(name.equals(slaName)){
                       result = "406 SLA " + name + " is being used mapping " +entry.getKey();
                       LOG.error(result);
       		       return result;
        	    }
                }
        	
                data = zkc.getData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, false, stat);
                if(data != null ){
                    String[] sData = (new String(data, Charset.defaultCharset())).split(":");
                    for (int index =0; index < sData.length; index++){
                        String element = sData[index];
                        if(element.equals("isDefault=no")){
                            zkc.delete(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, -1);
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }
    public synchronized String deleteWmsProfile(String name) throws Exception{
        String result = "200 OK.";
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);
        if(name.equals(Constants.DEFAULT_WMS_PROFILE_NAME)== false){
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,false);
            if(stat != null){
        	//Reject delete request if profile is being used by SLAs
                for(Map.Entry<String, LinkedHashMap<String,String>> entry : slasMap.entrySet()) {
                    LinkedHashMap<String,String> value = entry.getValue();
        	    String connProfileName = value.get(Constants.ON_CONNECT_PROFILE);
        	    String disconnProfileName = value.get(Constants.ON_DISCONNECT_PROFILE);
        	    if(name.equals(connProfileName) || name.equals(disconnProfileName)){
                       result = "406 Profile " + name + " is being used SLA " +entry.getKey();
                       LOG.error(result);
       		       return result;
        	    }
                }
                
                data = zkc.getData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, false, stat);
                if(data != null ){
                    String[] sData = (new String(data, Charset.defaultCharset())).split(":");
                    for (int index =0; index < sData.length; index++){
                        String element = sData[index];
                        if(element.equals("isDefault=no")){
                            zkc.delete(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, -1);
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }
    public synchronized String deleteWmsTenant(String name) throws Exception{
        String result = "200 OK.";
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);
        if(name.equals(Constants.DEFAULT_WMS_TENANT_NAME)== false){
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name,false);
            if(stat != null){
        	//Reject delete request if tenant is being used by mappings
                for(Map.Entry<String, LinkedHashMap<String,String>> entry : mappingsMap.entrySet()) {
                    LinkedHashMap<String,String> value = entry.getValue();
        	    String tenantName = value.get(Constants.TENANT_NAME);
        	    if(name.equals(tenantName)){
                       result = "406 Tenant " + name + " is being used mapping " +entry.getKey();
                       LOG.error(result);
       		       return result;
        	    }
                }
                
                data = zkc.getData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name, false, stat);
                if(data != null ){
                    String[] sData = (new String(data, Charset.defaultCharset())).split(":");
                    for (int index =0; index < sData.length; index++){
                        String element = sData[index];
                        if(element.equals("isDefault=no")){
                            zkc.delete(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS + "/" + name, -1);
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }
    public synchronized void deleteWmsMapping(String name) throws Exception{
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);
        if(name.equals(Constants.DEFAULT_WMS_MAPPING_NAME)== false){
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,false);
            if(stat != null){
                data = zkc.getData(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, false, stat);
                if(data != null ){
                    String[] sData = (new String(data, Charset.defaultCharset())).split(":");
                    for (int index =0; index < sData.length; index++){
                        String element = sData[index];
                        if(element.equals("isDefault=no")){
                            LinkedHashMap<String,String> attributes = mappingsMap.get(name);
                            if(attributes != null){
                                String orderNumber = attributes.get(Constants.ORDER_NUMBER);
                                if (orderNumber != null){
                                    if(LOG.isDebugEnabled())
                                        LOG.debug("deleteWmsMapping deletes znode :" + name + " and removes orderNumber :" + orderNumber);
                                    //for(Map.Entry<String, LinkedHashMap<String,String>> entry : mappingsMap.entrySet()) {
                                        //String key = entry.getKey();
                                        //LinkedHashMap<String,String> value = entry.getValue();
                                    // }
                                }
                            }
                            zkc.delete(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, -1);
                            break;
                        }
                    }
                }
            }
        }
    }

}
