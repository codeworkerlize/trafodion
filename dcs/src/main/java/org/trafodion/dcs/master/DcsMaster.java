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
package org.trafodion.dcs.master;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.nio.ListenerService;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.util.DcsConfiguration;
import org.trafodion.dcs.util.DcsNetworkConfiguration;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.util.InfoServer;
import org.trafodion.dcs.util.VersionInfo;
import org.trafodion.dcs.zookeeper.ZKConfig;
import org.trafodion.dcs.zookeeper.ZkClient;
import com.esgyn.common.ASNodes;


public class DcsMaster extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(DcsMaster.class);
    private ZkClient zkc = null;
    private Configuration conf;
    private DcsNetworkConfiguration netConf;
    private String[] args;
    private String instance = null;
    private int port;
    private int portRange;
    private String serverName;
    private String serverIp;
    private int infoPort;
    private long startTime;
    private ServerManager serverManager;
    private ListenerService ls;
    public static final String MASTER = "master";
    private Metrics metrics;
    private String parentDcsZnode;
    private String parentWmsZnode;
    private ExecutorService pool = null;
    private static String trafodionLog;
    private CountDownLatch isLeader = new CountDownLatch(1);
    private CountDownLatch clusterServers = new CountDownLatch(1);
    private MasterLeaderElection mle = null;

    private class JVMShutdownHook extends Thread {
        public void run() {
            LOG.info("JVM shutdown hook is running");
            try {
                if (zkc != null) {
                    zkc.close();
                }
            } catch (InterruptedException ie) {
                LOG.warn(ie.getMessage(), ie);
            }
        }
    }

    public DcsMaster(String[] args) {
        this.args = args;
        conf = DcsConfiguration.create();
        port = conf.getInt(Constants.DCS_MASTER_PORT,
                Constants.DEFAULT_DCS_MASTER_PORT);
        portRange = conf.getInt(Constants.DCS_MASTER_PORT_RANGE,
                Constants.DEFAULT_DCS_MASTER_PORT_RANGE);
        parentDcsZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT, Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
        String trafInstanceId = System.getenv("TRAF_INSTANCE_ID");
        if (trafInstanceId != null) {
            parentWmsZnode = parentDcsZnode.substring(0,parentDcsZnode.lastIndexOf("/"));
        } else  {
            parentWmsZnode = parentDcsZnode;
        }
        infoPort = conf.getInt(Constants.DCS_MASTER_INFO_PORT,
                Constants.DEFAULT_DCS_MASTER_INFO_PORT);
        trafodionLog = GetJavaProperty.getProperty(Constants.DCS_TRAFODION_LOG);
        JVMShutdownHook jvmShutdownHook = new JVMShutdownHook();
        Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
        this.start();
    }

    public void run() {
        VersionInfo.logVersion();

        Options opt = new Options();
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opt, args);
            if (LOG.isInfoEnabled()) {
                LOG.info("Dcs master start args <{}>.", cmd.getArgList());
            }
            instance = cmd.getArgList().get(0).toString();
        } catch (NumberFormatException nfe) {
            instance = "1";
        } catch (NullPointerException e) {
            LOG.error("No args found: ", e);
            System.exit(1);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            System.exit(1);
        }

        try {
            zkc = new ZkClient();
            zkc.connect();
            if (LOG.isInfoEnabled()) {
                LOG.info("Connected to ZooKeeper");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

        metrics = new Metrics();
        startTime = System.currentTimeMillis();

        try {
            // Create the persistent DCS znodes
            Stat stat = zkc.exists(parentDcsZnode, false);
            if (stat == null) {
                zkc.create(parentDcsZnode, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT, false);
            if (stat == null) {
                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER, false);
            if (stat == null) {
                int epoch = 1;
                byte[] data = Bytes.toBytes(Long.toString(epoch));

                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER,
                        data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER, false);
            if (stat == null) {
                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS, false);
            if (stat == null) {
                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING, false);
            if (stat == null) {
                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                    false);
            if (stat == null) {
                zkc.create(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            
            stat = zkc.exists(parentWmsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PARENT,
                    false);
            if (stat == null) {
                zkc.create(parentWmsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PARENT,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES, false);
            if (stat == null) {
                zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES , false);
            if (stat == null) {
                zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_SYSTEM , false);
            if (stat == null) {
                zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_SYSTEM, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            processSLA(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS);
            processProfile(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES);
            processMapping(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
            processTenant(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS);
            // ip whitelist
            processWhitelist(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_IPWHITELIST);
        } catch (KeeperException.NodeExistsException e) {
            // do nothing...some other server has created znodes
            LOG.warn(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(0);
        }

        try {
            netConf = new DcsNetworkConfiguration(conf);
            if (LOG.isInfoEnabled()) {
                LOG.info("Dcs Network Configuration OK.");
            }

            serverName = netConf.getHostName();
            serverIp = netConf.getServerIp();
            if (serverName == null || serverIp == null) {
                LOG.error("DNS Interface <{}> configured in dcs.site.xml is not found!",
                        conf.get(Constants.DCS_DNS_INTERFACE, Constants.DEFAULT_DCS_DNS_INTERFACE));
                System.exit(1);
            }
            
            mle = new MasterLeaderElection(this);
            if (LOG.isInfoEnabled()) {
                LOG.info("Master Leader Election, END.");
            }
            isLeader.await();

            String path = parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER + "/"
                    + netConf.getHostName() + ":" + port + ":" + portRange
                    + ":" + startTime;
            zkc.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            Stat stat = zkc.exists(path, false);
			if (stat == null) {
				LOG.error("Node <{}> does not exist!", path);
				throw new Exception("Node [" + path + "] does not exist!");
			}
            long currCTime = stat.getCtime();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Created znode <{}>, timestamp <{}>.", path, currCTime);
            }

            int requestTimeout = conf.getInt(
                    Constants.DCS_MASTER_LISTENER_REQUEST_TIMEOUT,
                    Constants.DEFAULT_LISTENER_REQUEST_TIMEOUT);
            int selectorTimeout = conf.getInt(
                    Constants.DCS_MASTER_LISTENER_SELECTOR_TIMEOUT,
                    Constants.DEFAULT_LISTENER_SELECTOR_TIMEOUT);
            ls = new ListenerService(zkc, port, requestTimeout, selectorTimeout, metrics, conf);
            if (LOG.isInfoEnabled()) {
                LOG.info("Listening for clients on port <{}>", port);
            }
            serverName = netConf.getHostName();

            // Start the info server.
            if (infoPort >= 0) {
                String a = conf.get(Constants.DCS_MASTER_INFO_BIND_ADDRESS,
                        Constants.DEFAULT_DCS_MASTER_INFO_BIND_ADDRESS);
                InfoServer infoServer = new InfoServer(MASTER, a, infoPort, false, conf);
                infoServer.addServlet("status", "/master-status",
                        MasterStatusServlet.class);
                infoServer.setAttribute(MASTER, this);
                infoServer.start();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Start http info server on <{}:{}>.", a, infoPort);
                }
            }

            pool = Executors.newSingleThreadExecutor();
            serverManager = new ServerManager(this, conf, zkc, netConf,
            		currCTime, metrics);
            Future future = pool.submit(serverManager);
            if (LOG.isInfoEnabled()) {
                LOG.info("Start server manager.");
            }
            clusterServers.await();
            
            createSystemTenant();

            List<String> masters = zkc.getChildren(parentDcsZnode
                            + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER,
                    new MasterWatcher());

            future.get();// block

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            if (pool != null)
                pool.shutdown();
            System.exit(0);
        }
    }
    private void processSLA(String path) throws Exception{
        List<String> first;
        List<String> second;
        
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node <{}> does not exist!", path);
            throw new Exception("Node [" + path + "] does not exist!");
        }
        createDefaultSLA(path + "/" + Constants.DEFAULT_WMS_SLA_NAME );
        
        first = zkc.getChildren(path, null);
        second = zkc.getChildren(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES , null);
        ArrayList<String> secondCopy = new ArrayList<String>(second);
        if (LOG.isDebugEnabled()) {
            LOG.debug("First {}, second {}, secondCopy {}", first, second, secondCopy);
        }
        second.removeAll(first);
        for (String sla : second) {
            deleteSLAResources(sla);
        }
        first.removeAll(secondCopy);
        for (String sla : first) {
            createSLAResources(sla);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Result add first {}, result del second {}", first, second);
        }
    }
    private void createDefaultSLA(String path) throws KeeperException, InterruptedException{
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%d:%s=%s",
	                    Constants.IS_DEFAULT,"yes",
	                    Constants.IS_ACTIVE,"yes",
	                    Constants.PRIORITY, Constants.PRTY_MEDIUM, 
	                    Constants.LIMIT, "",
	                    Constants.SESSION_LIMIT, "",
	                    Constants.THROUGHPUT, "",
	                    Constants.ON_CONNECT_PROFILE, Constants.DEFAULT_WMS_PROFILE_NAME,
	                    Constants.ON_DISCONNECT_PROFILE, Constants.DEFAULT_WMS_PROFILE_NAME,
	                    Constants.LAST_UPDATE, startTime,
	                    Constants.CONTROL_SCRIPT, "wms_default_script.py"
	                    ));
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
	        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            try {
                zkc.setData(path, data, zkc.exists(path, true).getVersion());
            } catch (KeeperException.BadVersionException e) {
                LOG.warn(e.getMessage(), e);
            }
        }
    }
    private void createSLAResources(String SLA) throws KeeperException, InterruptedException{
        Stat stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES + "/" + SLA, false);
        if (stat == null) {
            zkc.create(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES + "/" + SLA, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
    private void deleteSLAResources(String SLA) throws KeeperException, InterruptedException{
        Stat stat = zkc.exists(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES + "/" + SLA, false);
        if (stat != null) {
            zkc.delete(parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_RESOURCES_QUERIES + "/" + SLA, -1);
        }
    }

    private void processWhitelist(String path) throws Exception{
        List<String> children = null;
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node <{}> does not exist!", path);
            throw new Exception("Node [" + path + "] does not exist!");
        }
        children = zkc.getChildren(path, null);
        //Currently there will only be one child node under the ipwhitelist node
        if (!children.isEmpty()) {
            return;
        }
        createDefaultWhitelist(path + "/" + Constants.DEFAULT_WMS_WHITELIST_NAME);
    }
    private void createDefaultWhitelist(String path) throws KeeperException, InterruptedException{

        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%s:%s=%d",
                Constants.IS_OPEN,"no",
                Constants.IP_WHITELIST,"",
                Constants.LAST_UPDATE, startTime
        ));
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void processProfile(String path) throws Exception{
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node <{}> does not exist!", path);
            throw new Exception("Node [" + path + "] does not exist!");
        }
        List<String> children = zkc.getChildren(path, null);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(child.equals(Constants.DEFAULT_WMS_PROFILE_NAME))
                    return;
            }
        } 
        createDefaultProfile(path + "/" + Constants.DEFAULT_WMS_PROFILE_NAME );
    }
    private void createDefaultProfile(String path) throws KeeperException, InterruptedException{
        
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%s:%s=%s:%s=%s:%s=%d",
                Constants.IS_DEFAULT,"yes",
                Constants.CQD,"",
                Constants.SET,"",
                Constants.HOST_LIST,"",
                Constants.LAST_UPDATE, startTime
                ));
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    private void processMapping(String path) throws Exception{
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node <{}> does not exist!", path);
            throw new Exception("Node [" + path + "] does not exist!");
        }
        List<String> children = zkc.getChildren(path, null);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(child.equals(Constants.DEFAULT_WMS_MAPPING_NAME))
                    return;
            }
        } 
        createDefaultMapping(path + "/" + Constants.DEFAULT_WMS_MAPPING_NAME );
    }
    private void createDefaultMapping(String path) throws KeeperException, InterruptedException{
        
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%d",
                Constants.IS_DEFAULT,"yes",
                Constants.IS_ACTIVE,"yes",
                Constants.USER_NAME, "",
                Constants.APPLICATION_NAME, "",
                Constants.SESSION_NAME, "",
                Constants.ROLE_NAME, "",
                Constants.CLIENT_IP_ADDRESS, "",
                Constants.CLIENT_HOST_NAME, "",
                Constants.SLA,Constants.DEFAULT_WMS_SLA_NAME,
                Constants.ORDER_NUMBER, Constants.DEFAULT_ORDER_NUMBER,
                Constants.LAST_UPDATE, startTime
                ));
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void processTenant(String path) throws Exception{
        Stat stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node <{}> does not exist!", path);
            throw new Exception("Node [" + path + "] does not exist!");
        }
    }

    private void createSystemTenant() throws KeeperException, InterruptedException {
        String tenantPath = parentWmsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_TENANTS;
        String sysTenantPath = tenantPath + "/" + Constants.SYSTEM_WMS_TENANT_NAME;
        ASNodes nodes = new ASNodes();

        byte data[] = Bytes.toBytes(String.format("%s=%d:%s=%d:%s=%d:%s=%s:%s=%s:%s=%s:%s=%s:%s=%d",
                Constants.COMPUTE_UNITS,   nodes.getSize(),
                Constants.AFFINITY,        nodes.getAffinity(),
                Constants.MAX_NODES,       nodes.getClusterSize(),
                Constants.NODES,           nodes.toSerializedForm(),
                Constants.IS_DEFAULT,      "no",
                Constants.DEFAULT_SCHEMA,  "",
                Constants.SESSION_LIMIT,   "",
                Constants.LAST_UPDATE,     startTime
                ));
        Stat stat = zkc.exists(sysTenantPath, false);
        if (stat == null) {
            zkc.create(sysTenantPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            try {
                zkc.setData(sysTenantPath, data, stat.getVersion());
            } catch (KeeperException.BadVersionException e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        
        //We do not support defaulTenant now. So delete any existing Znode entries for defaultTenant
        String defTenantPath = tenantPath + "/defaultTenant";
        stat = zkc.exists(defTenantPath, false);
        if(stat != null) {
        	zkc.delete(defTenantPath, -1);
        }


    }

    public String getServerName() {
        return serverName;
    }

    public String getServerIp(){
        return serverIp;
    }

    public int getInfoPort() {
        return infoPort;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public ServerManager getServerManager() {
        return serverManager;
    }

    public ListenerService getListenerService() {
        return ls;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getPort() {
        return port;
    }

    public int getPortRange() {
        return portRange;
    }

    public String getZKQuorumServersString() {
        return ZKConfig.getZKQuorumServersString(conf);
    }

    public String getZKParentDcsZnode() {
        return parentDcsZnode;
    }

    public String getZKParentWmsZnode() {
        return parentWmsZnode;
    }

    public String getMetrics() {
        return metrics.toString();
    }

    public String getTrafodionLog() {
        return trafodionLog;
    }

    public ZkClient getZkClient() {
        return zkc;
    }

    public String getInstance() {
        return instance;
    }

    public boolean isFollower() {
           return mle.isFollower();
    }

    public void setIsLeader() {
        isLeader.countDown();
    }

    public DcsNetworkConfiguration getNetConf() {
        return netConf;
    }

    public void getClusterServers() {
        clusterServers.countDown();
    }

    public static void main(String[] args) {
        DcsMaster server = new DcsMaster(args);
    }

    private class MasterWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.Expired) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Zookeeper session is expired. Try to exit DcsMaster.");
                }
                try {
                    System.exit(1);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }
}
