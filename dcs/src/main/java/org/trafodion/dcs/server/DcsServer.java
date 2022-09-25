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
package org.trafodion.dcs.server;

import java.io.IOException;
import java.net.BindException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.util.DcsConfiguration;
import org.trafodion.dcs.util.DcsNetworkConfiguration;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.util.InfoServer;
import org.trafodion.dcs.util.VersionInfo;
import org.trafodion.dcs.zookeeper.ZKConfig;
import org.trafodion.dcs.zookeeper.ZkClient;

public final class DcsServer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(DcsServer.class);
	private Thread thrd;
	private ZkClient zkc = null;
	private int zkSessionTimeout;
    private Configuration conf;
    private DcsNetworkConfiguration netConf;
	private String[] args;
	private String instance = null;
	private int childServers;
	private long startTime;
	private String serverName;
	private InfoServer infoServer;
	private int infoPort;
    public static final String SERVER = "server";
    private Metrics metrics;
    private ServerManager serverManager;
    private ExecutorService pool=null;
    private JVMShutdownHook jvmShutdownHook;
	private static String trafodionLog;
	private String parentDcsZnode;
    private int mxoAliveCheckInterval;
    private int mxoAliveCheckStartTime;
    private HashMap<Integer,Integer> lostMxoMap;
    
    private class JVMShutdownHook extends Thread {
    	public void run() {
		    if (LOG.isInfoEnabled()) {
			    LOG.info("JVM shutdown hook is running");
		    }
    		try {
    			zkc.close();
    		} catch (InterruptedException ie) {};
    	}
    }

    public DcsServer(String[] args) {
	    this.args = args;
	    conf = DcsConfiguration.create();
	    parentDcsZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT, Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
	    jvmShutdownHook = new JVMShutdownHook();
	    Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
	    thrd = new Thread(this);
	    thrd.start();
    }

	public void run () {
	
		VersionInfo.logVersion();
		
	   	Options opt = new Options();
		CommandLine cmd;
		try {
			cmd = new GnuParser().parse(opt, args);
			if (LOG.isInfoEnabled()){
				LOG.info("Dcs server start args {}.", cmd.getArgList());
			}
			instance = cmd.getArgList().get(0).toString();
			if(cmd.getArgs().length > 2)
				childServers = Integer.parseInt(cmd.getArgList().get(1).toString());
			else
				childServers = 1;
		} catch (NullPointerException e) {
            LOG.error("No args found: {}", e.getMessage(), e);
			System.exit(1);
		} catch (ParseException e) {
            LOG.error("Could not parse: {}", e.getMessage(), e);
			System.exit(1);
		}
		
		trafodionLog = GetJavaProperty.getProperty(Constants.DCS_TRAFODION_LOG);

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
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if (LOG.isInfoEnabled()) {
					LOG.info("Shutdown Hook is running");
				}
				try {
					zkc.close();
				} catch (InterruptedException ie) {
				    LOG.warn(ie.getMessage(), ie);
				};
			}
		});
		
		metrics = new Metrics();
		startTime=System.currentTimeMillis();

		try {
		   	netConf = new DcsNetworkConfiguration(conf);
			serverName = netConf.getHostName();
			// Start the info server.
			String bindAddr = conf.get(Constants.DCS_SERVER_INFO_BIND_ADDRESS, Constants.DEFAULT_DCS_SERVER_INFO_BIND_ADDRESS);
            infoPort = conf
                .getInt(Constants.DCS_SERVER_INFO_PORT, Constants.DEFAULT_DCS_SERVER_INFO_PORT);
            infoPort += Integer.parseInt(instance);
            boolean auto = conf.getBoolean(Constants.DCS_SERVER_INFO_PORT_AUTO, false);
            mxoAliveCheckStartTime = conf
                .getInt(Constants.MXO_ALIVE_CHECK_START_MINS, Constants.MXO_ALIVE_CHECK_START_DEFAULT_TIME);
            mxoAliveCheckInterval = conf
                .getInt(Constants.MXO_ALIVE_CHECK_INTERVAL_MINS, Constants.MXO_ALIVE_CHECK_INTERVAL_DEFAULT_TIME);
		    while (true) {
		    	try {
		    		if (infoPort >= 0) {
		    			infoServer = new InfoServer(SERVER, bindAddr, infoPort, false, this.conf);
		    			infoServer.addServlet("status", "/server-status", ServerStatusServlet.class);
		    			infoServer.setAttribute(SERVER, this);
		    			infoServer.start();
					    if (LOG.isInfoEnabled()) {
						    LOG.info("Start http info server on port <{}>.", infoPort);
					    }
		    		} else {
                        LOG.warn("Http server info port is disabled");
		    		}
		    		break;
		    	} catch (BindException e) {
		    		if (!auto) {
		    			// auto bind disabled throw BindException
                        LOG.error(e.getMessage(), e);
		    			throw e;
		    		}
		    		// auto bind enabled, try to use another port
                    LOG.warn("Failed binding http info server to port <{}>, add one port and bind again.",
                            infoPort);
		    		infoPort++;
		    	}
		    }

            pool = Executors.newSingleThreadExecutor();
            serverManager = new ServerManager(conf, zkc, netConf, instance, infoPort, childServers);
            Future future = pool.submit(serverManager);
			if (LOG.isInfoEnabled()) {
				LOG.info("Start Dcs server manager.");
			}
            checkMxoExists();
            future.get();
		} catch (Exception e) {
            LOG.error(e.getMessage(), e);
		} finally {
			if(pool != null)
				pool.shutdown();
			System.exit(0);
		}
	}

	private void checkMxoExists() throws Exception{
        final String serverName = this.serverName;
        final int childNum = this.childServers;
        final int instance = Integer.parseInt(this.instance);
		if (LOG.isDebugEnabled()) {
			LOG.debug("checkMxoExists enter, severName = {}, childNum = {}, instance = {}",
					serverName, childNum, instance);
		}
        lostMxoMap = new HashMap<>();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                //if mxo not exists,restart mxo
                String registeredPath = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
                String nodeRegisteredPath = "";
                Stat stat = null;

                if (!registeredPath.startsWith("/")){
                    registeredPath = "/" + registeredPath;
                }
                ZkClient zkClient = new ZkClient();
                try {
                    zkClient.connect();
                    zkClient.sync(registeredPath, null, null);
                    List<String> childList = zkClient.getChildren(registeredPath, null);
                    List<String> numList = new ArrayList<>();
                    String tmpPath = serverName + ":" + instance + ":";
                    for (int i = 1; i <= childNum; i++){
                        numList.add(tmpPath + i);
                    }
                    if (childList.size() == childNum){
                        lostMxoMap.clear();
                        return;
                    }else{
                        for(String tmpChild : childList){
                            if (numList.remove(tmpChild)) {
                                int exsitsChildNum = Integer.parseInt(tmpChild.substring(tmpChild.lastIndexOf(":") + 1));
                                if (lostMxoMap.containsKey(exsitsChildNum)) {
                                    lostMxoMap.remove(exsitsChildNum);
                                }
                            }
                        }
                    }
                    if(lostMxoMap.size() == childNum){
                        return;
                    }
                    StringBuffer sb = new StringBuffer();
                    for (String str : numList) {
                        int key = Integer.parseInt(str.substring(str.lastIndexOf(":") + 1));
                        sb.setLength(0);
                        sb.append(registeredPath)
                            .append("/")
                            .append(str);
                        nodeRegisteredPath = sb.toString();
                        int tmpNum = 0;
                        if(!lostMxoMap.containsKey(key)){
                            lostMxoMap.put(key, 0);
                        }
                        tmpNum = lostMxoMap.get(key);
                        if (tmpNum < 2) {
                            lostMxoMap.put(key, tmpNum + 1);
                        } else {
                            ServerManager.ServerHandler serverHandler = serverManager
                                .getServerHandlers()[key - 1];
                            stat = zkClient.exists(nodeRegisteredPath, false);
                            if (stat == null) {
                                if (serverManager.addToServerHandlerSet(serverHandler)) {
                                    LOG.warn("check_mxo_existing submit success, mxo = {}", nodeRegisteredPath);
                                    serverManager.getCompletionService().submit(serverHandler);
                                }
                                lostMxoMap.remove(key);
                            }
                        }
                    }
                } catch (KeeperException ke) {
	                LOG.error("checkMxoExists.KeeperException: {}", ke.getMessage(), ke);
                } catch (InterruptedException ire) {
                    LOG.error("checkMxoExists.InterruptedException: {}", ire.getMessage(), ire);
                } catch (BufferUnderflowException be) {
                    LOG.error("checkMxoExists.BufferUnderflowException: {}", be.getMessage(), be);
                } catch (IOException ie) {
                    LOG.error("connet zkClient io exception : {}", ie.getMessage(), ie);
                }finally {
                    try {
                        zkClient.close();
                    } catch (InterruptedException e) {
                        LOG.error("zkClient close failed : {}", e.getMessage(), e);
                    }
                }
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, mxoAliveCheckStartTime * 60 * 1000,mxoAliveCheckInterval * 60 * 1000);
    }

	public String getMetrics(){
		return metrics.toString();
	}
	
	public long getStartTime(){
		return startTime;
	}
	
	public String getServerName(){
		return serverName;
	}
	
	public String getMasterHostName() {
		return serverManager.getMasterHostName();
	}
	
	public InfoServer getInfoServer(){
		return infoServer;
	}
	
	public int getInfoPort(){
		return infoPort;
	}
	
	public Configuration getConfiguration(){
		return conf;
	}
	
	public String getZKQuorumServersString() {
		return ZKConfig.getZKQuorumServersString(conf);
	}
	
	public String getZKParentDcsZnode() {
		return serverManager.getZKParentDcsZnode();
	}
	
	public String getUserProgramHome() {
		return serverManager.getUserProgramHome();
	}
	
	public String getTrafodionLog() {
		return trafodionLog;
	}
	
	public static void main(String [] args) {
		DcsServer server = new DcsServer(args);
	}
}
