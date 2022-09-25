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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.util.DcsNetworkConfiguration;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.util.JdbcT4Util;
import org.trafodion.dcs.util.RetryCounter;
import org.trafodion.dcs.util.RetryCounterFactory;
import org.trafodion.dcs.zookeeper.ZkClient;

public class ServerManager implements Callable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);
    private DcsMaster master;
    private Configuration conf;
    private DcsNetworkConfiguration netConf;
    private ZkClient zkc = null;
    private long startupTimestamp;
    private int maxRestartAttempts;
    private int retryIntervalMillis;
    private ExecutorService pool = null;
    private Metrics metrics;
    private String parentDcsZnode;
    private final ArrayList<String> configuredServers = new ArrayList<String>();
    public final static List<String> clusterServers = new ArrayList<String>();
    private final Map<String, ServerPortMap> serverPortMap = new HashMap<String, ServerPortMap>();
    private final static Map<String, String> runningServers = new HashMap<String, String>();
    private final ArrayList<String> registeredServers = new ArrayList<String>();
    private final Queue<RestartHandler> restartQueue = new LinkedList<RestartHandler>();
    private final Queue<RestartHandler> restartCheckQueue = new LinkedList<RestartHandler>();
    private final ArrayList<ServerItem> serverItemList = new ArrayList<ServerItem>();
    private boolean trafodionQueryToolsEnabled;
    private JdbcT4Util jdbcT4Util = null;

    public ServerManager(DcsMaster master, Configuration conf, ZkClient zkc,
            DcsNetworkConfiguration netConf, long startupTimestamp,
            Metrics metrics) throws Exception {
        try {
            this.master = master;
            this.conf = conf;
            this.zkc = zkc;
            this.netConf = netConf;
            this.startupTimestamp = startupTimestamp;
            this.metrics = metrics;
            maxRestartAttempts = conf
                    .getInt(Constants.DCS_MASTER_SERVER_RESTART_HANDLER_ATTEMPTS,
                            Constants.DEFAULT_DCS_MASTER_SERVER_RESTART_HANDLER_ATTEMPTS);
            retryIntervalMillis = conf
                    .getInt(Constants.DCS_MASTER_SERVER_RESTART_HANDLER_RETRY_INTERVAL_MILLIS,
                            Constants.DEFAULT_DCS_MASTER_SERVER_RESTART_HANDLER_RETRY_INTERVAL_MILLIS);
            trafodionQueryToolsEnabled = conf.getBoolean(
                    Constants.DCS_MASTER_TRAFODION_QUERY_TOOLS,
                    Constants.DEFAULT_DCS_MASTER_TRAFODION_QUERY_TOOLS);
            if (trafodionQueryToolsEnabled)
                jdbcT4Util = new JdbcT4Util(conf, netConf);

            parentDcsZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,
                    Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
            pool = Executors.newSingleThreadExecutor();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }

    public class RestartHandler implements Callable<ScriptContext> {
        private ScriptContext scriptContext = new ScriptContext();
        private String znodePath;
        private int childCount;
        private RetryCounter retry;

        public RestartHandler(String znodePath, int childCount, RetryCounter retry) {
            this.znodePath = znodePath;
            this.childCount = childCount;
            this.retry = retry;
        }
        public RestartHandler(String znodePath, int childCount) {
            this.znodePath = znodePath;
            this.childCount = childCount;
            this.retry = RetryCounterFactory.create(maxRestartAttempts, retryIntervalMillis, TimeUnit.MILLISECONDS);
        }

        public void resetRetry() {
            retry.resetAttemptTimes();
        }

        @Override
        public ScriptContext call() throws Exception {
            try {
                Scanner scn = new Scanner(znodePath);
                scn.useDelimiter(":");
                String hostName = scn.next();// host name
                String instance = scn.next();// instance
                scn.close();

                // Get the --config property from classpath...it's always first
                // in the classpath
                String cp = GetJavaProperty.getProperty("java.class.path");
                scn = new Scanner(cp);
                scn.useDelimiter(":");
                String confDir = scn.next();
                scn.close();
                LOG.debug("Conf dir <{}>.", confDir);

                // Get -Ddcs.home.dir
                String dcsHome = GetJavaProperty.getDcsHome();

                // If stop-dcs.sh is executed and DCS_MANAGES_ZK then zookeeper
                // is stopped abruptly.
                // Second scenario is when ZooKeeper fails for some reason
                // regardless of whether DCS
                // manages it. When either happens the DcsServer running znodes
                // still exist in ZooKeeper
                // and we see them at next startup. When they eventually timeout
                // we get node deleted events for a server that no longer
                // exists. So, only recognize
                // DcsServer running znodes that have timestamps after last
                // DcsMaster startup.
                //
                // But, if we are DcsMaster follower that is taking over from
                // failed one then ignore timestamp issues described above.
                // See MasterLeaderElection.elect()

                // New situation :
                // All DCSMasters is killed, but remain All DCSServers,
                // then start all DCSMaster, at this time, server start time less than master start time,
                // master still leader, then DCSServers exit accidentally and never restarted.
                // Find problem in CM Installer when restart connection master.
                if ((master.isFollower() == false)
                        || (runningServers.size() < configuredServers.size())) {
                    scriptContext.setHostName(hostName);
                    scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);

                    if (hostName.equalsIgnoreCase(netConf.getHostName())) {
                        scriptContext.setCommand("cd "+ dcsHome 
                                + " ;bin/dcs-daemon.sh --config "
                                + confDir + " conditional-start server " + instance + " "
                                + childCount);
                    }
                    else {
                        scriptContext.setCommand("edb_pdsh -w " + hostName
                                + " \" source ~/.bashrc; cd " + dcsHome
                                + ";bin/dcs-daemon.sh --config " + confDir
                                + " conditional-start server " + instance + " "
                                + childCount + "\"");
                    }

                    while (retry.shouldRetry()) {
                        scriptContext.cleanStdDatas();
                        if (LOG.isInfoEnabled()){
                            LOG.info("Restarting DcsServer <{}:{}>, script < {} >.", hostName, instance, scriptContext.toString());
                        }
                        ScriptManager.getInstance().runScript(scriptContext);

                        if (scriptContext.getExitCode() == 0) {
                            if (LOG.isInfoEnabled()){
                                LOG.info("DcsServer <{}:{}> restarted.", hostName, instance);
                            }
                            break;
                        } else {
                            StringBuilder sb = new StringBuilder();
                            sb.append("exit code <").append(scriptContext.getExitCode()).append(">");
                            if (!scriptContext.getStdOut().toString().isEmpty()) {
                                sb.append(", stdout <").append(scriptContext.getStdOut().toString()).append(">");
                            }
                            if (!scriptContext.getStdErr().toString().isEmpty()) {
                                sb.append(", stderr <").append(scriptContext.getStdErr().toString()).append(">.");
                            }
                            LOG.error(sb.toString());
                        }
                        retry.sleepUntilNextRetry();
                        retry.useRetry();
                    }
                    if (!retry.shouldRetry()) {
                        LOG.error("DcsServer <{}:{}> restart failed after <{}> retries.", hostName, instance, retry.getMaxRetries());
                    }
                } else {
                    if (LOG.isDebugEnabled()) { 
                      StringBuffer sb = new StringBuffer();
                      sb.append("No restart for <").append(znodePath).append(">.").append(System.lineSeparator());
                      sb.append("DcsMaster isFollower <").append(master.isFollower()).append(">, ");
                      sb.append("DcsMaster start time <")
                            .append(DateFormat.getDateTimeInstance().format(new Date(startupTimestamp))).append(">, ");
                      sb.append("running DcsServer num is <").append(runningServers.size())
                            .append(">, registered DcsServer num is <").append(registeredServers.size()).append(">.");

                      LOG.debug(sb.toString());
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            return scriptContext;
        }
    }

    class RunningWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Running children changed <{}>.", event.getPath());
                }
                try {
                    getZkRunning();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == Event.EventType.NodeDataChanged){
                String znodePath = event.getPath();
                String key = znodePath.substring(znodePath.lastIndexOf('/') + 1);
                String data = null;
                try {
                    Stat stat = zkc.exists(znodePath, new RunningWatcher());
                    data = new String(zkc.getData(znodePath, false, stat));
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                runningServers.put(key, data);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Running znode <{}> data changed <{}>.", key, data);
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                String znodePath = event.getPath();
                LOG.warn("Running znode deleted <{}>.", znodePath);
                try {
                    restartServer(znodePath);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class RegisteredWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Registered children changed <{}>.", event.getPath());
                }
                try {
                    getZkRegistered();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public Boolean call() throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info("Start ServerManager.");
        }
        long timeoutMillis = 5000;

        try {
            getServersFile();
            createServersPortMap();
            getZkRunning();
            getUnwathedServers();
            getZkRegistered();

            while (true) {
                if (!restartQueue.isEmpty()) {
                    if (LOG.isInfoEnabled()){
                        LOG.info("Restart queue size <{}>.", restartQueue.size());
                    }
                    RestartHandler handler = restartQueue.poll();
                    Future<ScriptContext> runner = pool.submit(handler);
                    ScriptContext scriptContext = runner.get();// blocking call

                    // In some situation, there may restart dcs server replicated.
                    // Exit code == 100 means dcs server had been started,
                    // no needs to add to restart queue.
                    if (scriptContext.getExitCode() != 0 && scriptContext.getExitCode() != 100) {
                        restartQueue.add(handler);
                    } else {
                        RestartHandler checkHandler = new RestartHandler(handler.znodePath, handler.childCount);
                        restartCheckQueue.add(handler);
                    }
                }

                if (!restartCheckQueue.isEmpty()) {
                    if (LOG.isInfoEnabled()){
                        LOG.info("Restart check queue size <{}>.", restartCheckQueue.size());
                    }
                    // poll from restartCheckQueue
                    RestartHandler handler = restartCheckQueue.poll();
                    String server = handler.znodePath.substring(0, handler.znodePath.lastIndexOf(":") + 1);
                    // do zk check
                    List<String> children = zkc.getChildren( parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING,
                            null);
                    boolean exist = false;
                    for (String child : children) {
                        if (child.startsWith(server)) {
                            exist = true;
                            break;
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Check node <{}> exists <{}>", server, exist);
                    }
                    // if node not exist, sleep and wait to the next check
                    if (!exist) {
                        if (handler.retry.shouldRetry()) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Retry restart check for DCS server .");
                            }
                            handler.retry.sleepUntilNextRetry();
                            handler.retry.useRetry();
                            restartCheckQueue.add(handler);
                        } else {
                            LOG.warn("Restart check failure after <{}> times, add to restart queue again to restart DCS server",
                                    maxRestartAttempts);
                            handler.resetRetry();
                            restartQueue.add(handler);
                        }
                    }
                }
                try {
                    Thread.sleep(timeoutMillis);
                } catch (InterruptedException e) {
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            pool.shutdown();
            throw e;
        }
    }

    private List<String> getChildren(String znode, Watcher watcher)
            throws Exception {
        List<String> children = zkc.getChildren(znode, watcher);
        if (!children.isEmpty())
            Collections.sort(children);
        return children;
    }

    private void getServersFile() throws Exception {
        InputStream is = this.getClass().getResourceAsStream("/servers");
        if (is == null)
            throw new IOException("Cannot find servers file.");

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        configuredServers.clear();
        clusterServers.clear();
        String line;
        int lineNum = 1;
        while ((line = br.readLine()) != null) {
            Scanner scn = new Scanner(line);
            scn.useDelimiter("\\s+");
            String hostName = null;
            String serverCount = null;
            if (scn.hasNext())
                hostName = scn.next();// host name
            else
                hostName = new String("localhost");
            if (scn.hasNext())
                serverCount = scn.next();// optional
            else
                serverCount = "1";
            scn.close();
            if (hostName.equalsIgnoreCase("localhost")) {
                hostName = netConf.getHostName();
            }
            configuredServers.add(hostName.toLowerCase() + ":" + lineNum + ":" + serverCount);
            clusterServers.add(hostName.toLowerCase());
            lineNum++;
        }

        Collections.sort(configuredServers);
        Collections.sort(clusterServers);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configured servers after sort : {}.", configuredServers);
            LOG.debug("Cluster servers after sort : {}.", clusterServers);
        }
        this.master.getClusterServers();
        if (configuredServers.size() < 1)
            throw new IOException("No entries found in servers file");

    }

    class ServerPortMap {
        int begPortNum = conf.getInt(Constants.DCS_MASTER_PORT,
                Constants.DEFAULT_DCS_MASTER_PORT) + 1;
        int endPortNum = begPortNum;
        StringBuilder sb = new StringBuilder();

        public void add(int instance, int childCount) {
            for (int i = 1; i <= childCount; i++) {
                if (endPortNum > begPortNum)
                    sb.append(":");
                sb.append(instance + ":" + i + ":" + endPortNum);
                endPortNum++;
            }
        }

        public String toString() {
            return sb.toString();
        }
    }

    private void createServersPortMap() throws Exception {
        serverPortMap.clear();

        for (String aServer : configuredServers) {
            Scanner scn = new Scanner(aServer);
            scn.useDelimiter(":");
            String hostName = scn.next();
            int instance = Integer.parseInt(scn.next());
            int childCount = Integer.parseInt(scn.next());
            scn.close();

            ServerPortMap spm = serverPortMap.get(hostName);
            if (spm == null)
                spm = new ServerPortMap();
            spm.add(instance, childCount);
            serverPortMap.put(hostName, spm);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finish set serverPortMap : {}.", serverPortMap);
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ServerPortMap> entry : serverPortMap.entrySet()) {
            sb.append(entry.getValue());
            sb.append(":");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting <{}> data <{}>",
                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                    sb.toString());
        }
        byte[] data = Bytes.toBytes(sb.toString());
        zkc.setData(parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED, data, -1);
    }

    private synchronized void getZkRunning() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reading <{}>.",
                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING);
        }
		String path = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING;
		List<String> children = getChildren(path, new RunningWatcher());

        if (!children.isEmpty()) {
            for (String child : children) {
                // If stop-dcs.sh is executed and DCS_MANAGES_ZK then zookeeper
                // is stopped abruptly.
                // Second scenario is when ZooKeeper fails for some reason
                // regardless of whether DCS
                // manages it. When either happens the DcsServer running znodes
                // still exist in ZooKeeper
                // and we see them at next startup. When they eventually timeout
                // we get node deleted events for a server that no longer
                // exists. So, only recognize
                // DcsServer running znodes that have timestamps after last
                // DcsMaster startup.
                Scanner scn = new Scanner(child);
                scn.useDelimiter(":");
                String hostName = scn.next();
                String instance = scn.next();
                scn.close();

                if (!runningServers.containsKey(child)) {
                    if (LOG.isInfoEnabled()){
                        LOG.info("Add RunningWatcher for <{}>", child);
                    }

                    Stat stat = zkc.exists(parentDcsZnode
                            + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING
                            + "/" + child, new RunningWatcher());
                    String data = new String(zkc.getData(path + "/" + child, false, stat));
                    runningServers.put(child, data);
                    if (LOG.isInfoEnabled()){
                        LOG.info("runningServers <{}>, data <{}>", runningServers, data);
                    }
                }
            }
            metrics.setTotalRunning(runningServers.size());
        } else {
            metrics.setTotalRunning(0);
        }
    }

    private void getUnwathedServers() {
        // In some situation when open HA, if DCS Server does not have znode info in zookeeper
        // when DCS Master is starting, then server will never be watched by zookeeper,
        // and if it downs, it will never be restarted.
        
        // configuredServers
        // hostName + ":" + lineNum + ":" + serverCount
        // runningServers
        // hostName + ":" + instance + ":" + infoPort + ":" + serverStartTimestamp
        // eg : gy26.esgyncn.local:3:24413:1515056285028
        // RestartHandler need to know hostName, instanceNum(lineNum), serverStartTimestamp(for if condition)
        if (!master.isFollower() || runningServers.size() == configuredServers.size()) {
            if (LOG.isDebugEnabled()) {
                if (!master.isFollower()) {
                    LOG.debug("DcsMaster start normally, no need to add watchers.");
                } else {
                    LOG.debug("Backup master start, all DcsServers have started, no need to add watchers.");
                }
            }
            return;
        }

        boolean found = false;
        for (String configured : configuredServers) {
            Scanner configuredScn = new Scanner(configured);
            configuredScn.useDelimiter(":");
            String hostName = configuredScn.next();
            int instance = Integer.parseInt(configuredScn.next());
            int serverCount = Integer.parseInt(configuredScn.next());
            configuredScn.close();
            for (String running : runningServers.keySet()) {
                Scanner runningScn = new Scanner(running);
                runningScn.useDelimiter(":");
                String runningHostName = runningScn.next();

                runningScn.close();
                if (runningHostName.equals(hostName)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                found = false;
            } else {
                LOG.warn("DcsServer <{}:{}> does not started when starting DcsMaster <{}>. add it to restart queue.",
                        hostName, instance, master.getServerName());
                // add to the restart handler
                String simulatePath = hostName + ":" + instance + ":0:" + System.currentTimeMillis();
                RetryCounter retryCounter = RetryCounterFactory.create(maxRestartAttempts, retryIntervalMillis, TimeUnit.MILLISECONDS);
                RestartHandler handler = new RestartHandler(simulatePath, serverCount, retryCounter);
                restartQueue.add(handler);
            }
        }
    }


    private synchronized void restartServer(String znodePath) throws Exception {
        String child = znodePath.replace(parentDcsZnode
            + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING + "/", "");
        Scanner scn = new Scanner(child);

        scn.useDelimiter(":");
        String hostName = scn.next();
        String instance = scn.next();
        scn.close();

        LOG.warn("DcsServer <{}:{}> failed. Trying to add to restart queue.", hostName, instance);

        if (runningServers.containsKey(child)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found <{}>, deleting from running servers list", znodePath);
            }
            runningServers.remove(child);
            metrics.setTotalRunning(runningServers.size());
        }

        // Extract the server count for the restarting instance
        int count = 1;
        boolean found = false;
        for (String aServer : configuredServers) {
            scn = new Scanner(aServer);
            scn.useDelimiter(":");
            String srvrHostName = scn.next();
            String srvrInstance = scn.next();
            int srvrCount = new Integer(scn.next()).intValue();
            scn.close();
            if (srvrHostName.equals(hostName) && srvrInstance.equals(instance)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found <{}:{}:{}> in configured servers", srvrHostName, srvrInstance,
                            srvrCount);
                }
                found = true;
                if (srvrCount > 0)
                    count = srvrCount;
                break;
            }
        }

        // For local-servers.sh don't restart anything that's not in the servers
        // file
        if (!found) {
            if (LOG.isInfoEnabled()){
                LOG.info("DcsServer <{}:{}> not in servers file. Not restarting", hostName, instance);
            }
            return;
        }

        RetryCounter retryCounter = RetryCounterFactory.create(maxRestartAttempts, retryIntervalMillis, TimeUnit.MILLISECONDS);
        RestartHandler handler = new RestartHandler(child, count, retryCounter);
        restartQueue.add(handler);
    }

    private synchronized void getZkRegistered() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reading <{}>.",
                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED);
        }
        List<String> children = getChildren(parentDcsZnode
                + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                new RegisteredWatcher());

        if (!children.isEmpty()) {
            registeredServers.clear();
            for (String child : children) {
                registeredServers.add(child);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Registered {}", registeredServers);
            }
            metrics.setTotalRegistered(registeredServers.size());
        } else {
            metrics.setTotalRegistered(0);
        }
    }

    public synchronized List<RunningServer> getServersList() {
        ArrayList<RunningServer> serverList = new ArrayList<RunningServer>();
        Stat stat = null;
        byte[] data = null;

        int totalAvailable = 0;
        int totalConnecting = 0;
        int totalConnected = 0;
        int totalRebalance = 0;
        int totalRestart = 0;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin getServersList()");
        }

        if (!runningServers.isEmpty()) {
            for (Map.Entry<String, String> entry : runningServers.entrySet()) {
                RunningServer runningServer = new RunningServer();
                Scanner scn = new Scanner(entry.getKey());
                scn.useDelimiter(":");
                runningServer.setHostname(scn.next());
                runningServer.setInstance(scn.next());
                scn.close();
                scn = new Scanner(entry.getValue());
                scn.useDelimiter(":");
                scn.next(); // ENABLE or DISABLE
                runningServer.setInfoPort(Integer.parseInt(scn.next()));
                runningServer.setStartTime(Long.parseLong(scn.next()));
                scn.close();

                if (!registeredServers.isEmpty()) {
                    for (String aRegisteredServer : registeredServers) {
                        if (aRegisteredServer.contains(runningServer
                                .getHostname()
                                + ":"
                                + runningServer.getInstance() + ":")) {
                            try {
                                RegisteredServer registeredServer = new RegisteredServer();
                                stat = zkc.exists(parentDcsZnode
                                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/"
                                        + aRegisteredServer, false);
                                if (stat != null) {
                                    registeredServer.setCtime(stat.getCtime());
                                    registeredServer.setMtime(stat.getMtime());
                                    data = zkc.getData(parentDcsZnode
                                            + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED
                                            + "/" + aRegisteredServer, false, stat);
                                    scn = new Scanner(new String(data));
                                    scn.useDelimiter(":");
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("getDataRegistered <{}>.", new String(data));
                                    }
                                    registeredServer.setState(scn.next());
                                    String state = registeredServer.getState();
                                    if (state.equals("AVAILABLE"))
                                        totalAvailable += 1;
                                    else if (state.equals("CONNECTING"))
                                        totalConnecting += 1;
                                    else if (state.equals("CONNECTED"))
                                        totalConnected += 1;
                                    else if (state.equals("REBALANCE"))
                                        totalRebalance += 1;
                                    else if (state.equals("RESTART"))
                                        totalRestart += 1;
                                    registeredServer.setTimestamp(Long
                                            .parseLong(scn.next()));
                                    registeredServer.setDialogueId(scn.next());
                                    registeredServer.setNid(scn.next());
                                    registeredServer.setPid(scn.next());
                                    registeredServer.setProcessName(scn.next());
                                    registeredServer.setIpAddress(scn.next());
                                    registeredServer.setPort(scn.next());
                                    registeredServer.setClientName(scn.next());
                                    registeredServer.setClientIpAddress(scn
                                            .next());
                                    registeredServer.setClientPort(scn.next());
                                    registeredServer.setClientAppl(scn.next());
                                    if (state.equals("CONNECTED") || state.equals("REBALANCE") || state.equals("RESTART")){
                                        registeredServer.setSla(scn.next());
                                        registeredServer.setConnectProfile(scn.next());
                                        registeredServer.setDisconnectProfile(scn.next());
                                    } else {
                                        registeredServer.setSla("");
                                        registeredServer.setConnectProfile("");
                                        registeredServer.setDisconnectProfile("");
                                    }
                                    registeredServer.setIsRegistered();
                                    scn.close();
                                    runningServer.getRegistered().add(
                                            registeredServer);
                                }
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    }
                }

                serverList.add(runningServer);
            }
        }

        metrics.setTotalAvailable(totalAvailable);
        metrics.setTotalConnecting(totalConnecting);
        metrics.setTotalConnected(totalConnected);
        metrics.setTotalRebalance(totalRebalance);
        metrics.setTotalRestart(totalRestart);

        Collections.sort(serverList, new Comparator<RunningServer>() {
            public int compare(RunningServer s1, RunningServer s2) {
                if (s1.getInstanceIntValue() == s2.getInstanceIntValue())
                    return 0;
                return s1.getInstanceIntValue() < s2.getInstanceIntValue() ? -1
                        : 1;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("End getServersList()");
        }

        return serverList;
    }

    public synchronized List<ServerItem> getServerItemList() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin getServerItemList()");
        }

        serverItemList.clear();
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (RunningServer aRunningServer : this.getServersList()) {
            for (RegisteredServer aRegisteredServer : aRunningServer
                    .getRegistered()) {
                ServerItem serverItem = new ServerItem();
                serverItem.setHostname(aRunningServer.getHostname());
                serverItem.setinfoPort(aRunningServer.getInfoPort() + "");
                serverItem.setInstance(aRunningServer.getInstance());
                serverItem.setStartTime(aRunningServer.getStartTimeAsDate());
                serverItem.setIsRegistered(aRegisteredServer.getIsRegistered());
                serverItem.setState(aRegisteredServer.getState());
                serverItem.setNid(aRegisteredServer.getNid());
                serverItem.setPid(aRegisteredServer.getPid());
                serverItem.setProcessName(aRegisteredServer.getProcessName());
                serverItem.setIpAddress(aRegisteredServer.getIpAddress());
                serverItem.setPort(aRegisteredServer.getPort());
                serverItem.setClientName(aRegisteredServer.getClientName());
                if ("AVAILABLE".equalsIgnoreCase(aRegisteredServer.getState())
                    && !aRegisteredServer.getClientAppl().isEmpty())
                    serverItem.setClientAppl("");
                else
                    serverItem.setClientAppl(aRegisteredServer.getClientAppl());
                serverItem.setClientIpAddress(aRegisteredServer
                        .getClientIpAddress());
                serverItem.setClientPort(aRegisteredServer.getClientPort());
                serverItem.setMtime(aRegisteredServer.getMtime());
                serverItem.setCtime(dateformat.format(aRegisteredServer.getCtime()));
                serverItem.setSla(aRegisteredServer.getSla());
                serverItem.setConnectProfile(aRegisteredServer.getConnectProfile());
                serverItem.setDisconnectProfile(aRegisteredServer.getDisconnectProfile());
                serverItemList.add(serverItem);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("End getServerItemList()");
        }
        return serverItemList;
    }

    public synchronized List<JSONObject> getRepositoryItemList(String command) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin getRepositoryItemList()");
        }

        JSONArray reposList = null;
        reposList = getRepositoryListT4Driver(command);
        List<JSONObject> objList = new ArrayList<JSONObject>();

        if (reposList != null) {
            try {
                for (int i = 0; i < reposList.length(); i++) {
                    objList.add(reposList.getJSONObject(i));
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("End getRepositoryItemList()");
        }

        return objList;
    }

    public synchronized JSONArray getRepositoryListT4Driver(String command) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin getRepositoryListT4Driver()");
        }

        JSONArray reposList = null;

        StringBuilder sb = new StringBuilder();
        if (command.equals(Constants.TRAFODION_REPOS_METRIC_SESSION_TABLE)) {
            sb.append(conf
                    .get(Constants.TRAFODION_REPOS_METRIC_SESSION_TABLE_QUERY,
                            Constants.DEFAULT_TRAFODION_REPOS_METRIC_SESSION_TABLE_QUERY));
        } else if (command.equals(Constants.TRAFODION_REPOS_METRIC_QUERY_TABLE)) {
            sb.append(conf.get(
                    Constants.TRAFODION_REPOS_METRIC_QUERY_TABLE_QUERY,
                    Constants.DEFAULT_TRAFODION_REPOS_METRIC_QUERY_TABLE_QUERY));
        } else if (command
                .equals(Constants.TRAFODION_REPOS_METRIC_QUERY_AGGR_TABLE)) {
            sb.append(conf
                    .get(Constants.TRAFODION_REPOS_METRIC_QUERY_AGGR_TABLE_QUERY,
                            Constants.DEFAULT_TRAFODION_REPOS_METRIC_QUERY_AGGR_TABLE_QUERY));
        } else
            sb.append(command);

        if (LOG.isDebugEnabled()) {
            LOG.debug("command [" + sb.toString() + "]");
            LOG.debug("End getRepositoryListT4Driver()");
        }
        return reposList;
    }

    public static boolean nodeEnabled(String node){
        String value = runningServers.get(node);
        if (value == null)
            return false;
        else
            return value.startsWith("ENABLE");
    }

    public String getZKParentDcsZnode() {
        return parentDcsZnode;
    }

    public ZkClient getZkClient() {
        return zkc;
    }

    public JdbcT4Util getJdbcT4Util() {
        return jdbcT4Util;
    }

}
