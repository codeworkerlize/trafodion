/**********************************************************************
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**********************************************************************/
package org.trafodion.dcs.server;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.util.DcsNetworkConfiguration;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.util.RetryCounter;
import org.trafodion.dcs.util.RetryCounterFactory;
import org.trafodion.dcs.zookeeper.ZkClient;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.script.ScriptContext;

public final class ServerManager implements Callable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);
    private Configuration conf;
    private ZkClient zkc;
    private boolean userProgEnabled;
    private String userProgramHome;
    private String userProgramCommand;
    private String hostName;
    private String masterHostName;
    private long masterStartTime;
    private int port;
    private int portRange;
    private DcsNetworkConfiguration netConf;
    private int instance;
    private int childServers;
    private String parentDcsZnode;
    private int connectingTimeout;
    private int zkSessionTimeout;
    private int userProgExitAfterDisconnect;
    private int infoPort;
    private int maxHeapPctExit;
    private int statisticsIntervalTime;
    private int statisticsLimitTime;
    private int statisticsCacheSize;
    private String statisticsType;
    private String statisticsEnable;
    private String internalQueriesInRepo;
    private String sqlplanEnable;
    private int userProgPortMapToSecs;
    private int userProgPortBindToSecs;
    private String publishStatsToOpenTSDB;
    private String tsdHost;
    private int tsdPort;
    private int exitSessionsCount;
    private int exitLiveTimeMinutes;
    private int wmsTimeoutSeconds;
    private ServerHandler[] serverHandlers;
    private int maxRestartAttempts;
    private int retryIntervalMillis;
    private String useSSLEnable;
    private String mdsEnable;
    private String mdsLimitMs;
    private String userProgKeepaliveStatus;
    private int userProgKeepaliveIdletime;
    private int userProgKeepaliveIntervaltime;
    private int userProgKeepaliveRetrycount;
    private String userProgIsGoingWms;
    private int userProgRMSMemLimit;
    private boolean previousTrafodionState = false;
    private int preloadTablesEnable;
    private int encryptBase64Enable;
    private int cpuLimitPercentWhileStart;

    private ExecutorService executorService;
    private CompletionService<Integer> completionService;
    private Set<ServerHandler> serverHandlerSet;

    private String numaNode;

    class RegisteredWatcher implements Watcher {
        CountDownLatch startSignal;

        public RegisteredWatcher(CountDownLatch startSignal) {
            this.startSignal = startSignal;
        }

        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Registered znode deleted <{}>.", event.getPath());
                }
                try {
                    startSignal.countDown();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (event.getType() == EventType.NodeDataChanged) {
                try {
                    if (LOG.isInfoEnabled()) {
                        LOG.info(
                                "znode data changed with <{}>, re add watcher", event.getPath());
                    }
                    zkc.exists(event.getPath(), this);
                } catch (KeeperException | InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    class ServerMonitor {
        ScriptContext scriptContext = new ScriptContext();
        int childInstance;
        String registeredPath;
        Stat stat = null;
        boolean isRunning = false;
        String nid;
        String pid;

        public ServerMonitor(int childInstance, String registeredPath) {
            this.childInstance = childInstance;
            this.registeredPath = registeredPath;
        }

        public boolean monitor() {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Check whether registered path <{}> exist.", registeredPath);
                }
                stat = zkc.exists(registeredPath, false);
                if (stat != null) { // User program znode found in
                                    // /registered...check pid
                    isRunning = isPidRunning();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<{}:{}.{}> isRunning <{}>.", (Integer.parseInt(nid) + 1),
                                childInstance, pid, isRunning);
                    }
                }else{
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("isRunning = {}, then set value to false", isRunning);
                    }
                    isRunning = false;
                }
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
                isRunning = false;
                return isRunning;
            }

            return isRunning;
        }

        private boolean isPidRunning() throws Exception {
            String data = Bytes.toString(zkc.getData(registeredPath, false,
                    stat));
            Scanner scn = new Scanner(data);
            scn.useDelimiter(":");
            scn.next();// state
            scn.next();// timestamp
            scn.next();// dialogue Id
            nid = scn.next();// nid
            pid = scn.next();// pid
            scn.close();
            scriptContext.setHostName(hostName);
            scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
            scriptContext.setCommand("ps -p " + pid);
            ScriptManager.getInstance().runScript(scriptContext);
            return scriptContext.getExitCode() != 0 ? false : true;
        }
    }

    class ServerRunner {
        ScriptContext scriptContext;
        String registeredPath;
        int childInstance;

        public ServerRunner(int childInstance, String registeredPath, String cpuNodeBind) {
            this.scriptContext = new ScriptContext();
            this.childInstance = childInstance;
            this.registeredPath = registeredPath;
            scriptContext.setHostName(hostName);
            scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
            String command = userProgramCommand
                    .replace("--CPUNODEBIND", cpuNodeBind)
                    .replace("-ZKHOST", "-ZKHOST " + zkc.getZkQuorum() + " ")
                    .replace("-PRELOADENABLE", "-PRELOADENABLE" + " " + preloadTablesEnable + " ")
                    .replace("-ENCRYPTBASE64ENABLE", "-ENCRYPTBASE64ENABLE" + " " + encryptBase64Enable  + " ")
                    .replace(
                            "-RZ",
                            "-RZ " + hostName + ":" + instance + ":"
                                    + childInstance + " ")
                    .replace("-ZKPNODE",
                            "-ZKPNODE " + "\"" + parentDcsZnode + "\"" + " ")
                    .replace("-CNGTO", "-CNGTO " + connectingTimeout + " ")
                    .replace("-ZKSTO", "-ZKSTO " + zkSessionTimeout + " ")
                    .replace("-EADSCO",
                            "-EADSCO " + userProgExitAfterDisconnect + " ")
                    .replace("-TCPADD",
                            "-TCPADD " + netConf.getExtHostAddress() + " ")
                    .replace("-MAXHEAPPCT",
                            "-MAXHEAPPCT " + maxHeapPctExit + " ")
                    .replace(
                            "-STATISTICSINTERVAL",
                            "-STATISTICSINTERVAL " + statisticsIntervalTime
                                    + " ")
                    .replace("-STATISTICSLIMIT",
                            "-STATISTICSLIMIT " + statisticsLimitTime + " ")
                    .replace("-STATISTICSCACHESIZE",
                            "-STATISTICSCACHESIZE " + statisticsCacheSize + " ")
                    .replace("-STATISTICSTYPE",
                            "-STATISTICSTYPE " + statisticsType + " ")
                    .replace("-STATISTICSENABLE",
                            "-STATISTICSENABLE " + statisticsEnable + " ")
                    .replace("-STOREINTERNALQUERIESINREPO",
                            "-STOREINTERNALQUERIESINREPO " + internalQueriesInRepo + " ")
                    .replace("-SQLPLAN", "-SQLPLAN " + sqlplanEnable + " ")
                    .replace("-PORTMAPTOSECS",
                            "-PORTMAPTOSECS " + userProgPortMapToSecs + " ")
                    .replace("-PORTBINDTOSECS",
                            "-PORTBINDTOSECS " + userProgPortBindToSecs + " ")
                    .replace("-PUBLISHSTATSTOTSDB",
                            "-PUBLISHSTATSTOTSDB " + publishStatsToOpenTSDB 
                                   + " ")
                    .replace("-OPENTSDURL",
                            "-OPENTSDURL " + tsdHost+":" + tsdPort + " ")
                    .replace("-EXITSESSIONSCOUNT",
                            "-EXITSESSIONSCOUNT " + exitSessionsCount + " ")
                    .replace("-EXITLIVETIME",
                            "-EXITLIVETIME " + exitLiveTimeMinutes + " ")
                    .replace("-WMSTIMEOUT",
                            "-WMSTIMEOUT " + wmsTimeoutSeconds + " ")
					.replace("-USESSLENABLE",
                            "-USESSLENABLE " + useSSLEnable + " ")
					.replace("-MDSENABLE",
                            "-MDSENABLE " + mdsEnable + " ")

					.replace("-MDSLIMITMS",
                            "-MDSLIMITMS " + mdsLimitMs + " ")
                    .replace("-TCPKEEPALIVESTATUS",
                            "-TCPKEEPALIVESTATUS " + userProgKeepaliveStatus + " ")
                    .replace("-TCPKEEPALIVEIDLETIME",
                            "-TCPKEEPALIVEIDLETIME " + userProgKeepaliveIdletime + " ")
                    .replace("-TCPKEEPALIVEINTERVAL",
                            "-TCPKEEPALIVEINTERVAL " + userProgKeepaliveIntervaltime + " ")
                    .replace("-TCPKEEPALIVERETRYCOUNT",
                            "-TCPKEEPALIVERETRYCOUNT " + userProgKeepaliveRetrycount + " ")
                    .replace("-IFGOINGWMS",
                            "-IFGOINGWMS " + userProgIsGoingWms + " ")
                    .replace("-RMSMEMLIMIT",
                            "-RMSMEMLIMIT " + userProgRMSMemLimit + " ")
                    .replace("-CPULIMITPERWHILESTART",
                            "-CPULIMITPERWHILESTART " + cpuLimitPercentWhileStart + " ")
                    .replace("&lt;", "<").replace("&amp;", "&")
                    .replace("&gt;", ">");
            scriptContext.setCommand(command);
        }

        public void exec() throws Exception {
            cleanupZk();
            if (LOG.isInfoEnabled()) {
                LOG.info("Instance : <{}>, User program exec <{}>.", childInstance,
                        scriptContext.getCommand());
            }
            scriptContext.cleanStdDatas();

            // This will block while user prog is running
            ScriptManager.getInstance().runScript(scriptContext);
            if (LOG.isInfoEnabled()) {
                LOG.info("Instance : <{}>, User program exit <{}>.", childInstance,
                        scriptContext.getExitCode());
            }
            StringBuilder sb = new StringBuilder();
            sb.append("exit code <").append(scriptContext.getExitCode()).append(">");
            if (!scriptContext.getStdOut().toString().isEmpty())
                sb.append(", stdout <").append(scriptContext.getStdOut().toString()).append(">");
            if (!scriptContext.getStdErr().toString().isEmpty())
                sb.append(", stderr <").append(scriptContext.getStdErr().toString()).append(">.");
            if (LOG.isInfoEnabled()){
                LOG.info(sb.toString());
            }

            switch (scriptContext.getExitCode()) {
            case 3:
                LOG.error("Trafodion is not running");
                break;
            case 127:
                LOG.error("Cannot find user program executable");
                break;
            case 137:
                cleanupZk();
                break;
            default:
            }
        }

        private void cleanupZk() {
            try {
                Stat stat = zkc.exists(registeredPath, false);
                if (stat != null){
                    if (LOG.isInfoEnabled()) {
                        LOG.info("delete node <{}>", registeredPath);
                    }
                    zkc.delete(registeredPath, -1);
                }
            } catch (KeeperException | InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }

    }

    class ServerHandler implements Callable<Integer>{
        ServerMonitor serverMonitor;
        ServerRunner serverRunner;
        int childInstance;
        String registeredPath;
        CountDownLatch startSignal = new CountDownLatch(1);
        RetryCounter retryCounter;

        public ServerHandler(Configuration conf ,int childInstance, String cpuNodeBind) {
            int maxRestartAttempts = conf.getInt(Constants.DCS_SERVER_STARTUP_MXOSRVR_USER_PROGRAM_RESTART_HANDLER_ATTEMPTS,
                    Constants.DEFAULT_DCS_SERVER_STARTUP_MXOSRVR_USER_PROGRAM_RESTART_HANDLER_ATTEMPTS);
            int retryTimeoutMinutes = conf.getInt(
                    Constants.DCS_SERVER_STARTUP_MXOSRVR_USER_PROGRAM_RESTART_HANDLER_RETRY_TIMEOUT_MINUTES,
                    Constants.DEFAULT_DCS_SERVER_STARTUP_MXOSRVR_USER_PROGRAM_RESTART_HANDLER_RETRY_TIMEOUT_MINUTES);
            this.childInstance = childInstance;
            this.registeredPath = parentDcsZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED
                    + "/" + hostName + ":" + instance + ":" + childInstance;
            retryCounter = RetryCounterFactory.create(maxRestartAttempts, retryTimeoutMinutes, TimeUnit.MINUTES);
            serverMonitor = new ServerMonitor(childInstance, registeredPath);
            serverRunner = new ServerRunner(childInstance, registeredPath, cpuNodeBind);
        }

        @Override
        public Integer call() throws Exception {
            Integer result = new Integer(childInstance);

            if (serverMonitor.monitor()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Server handler <{}:{}> is running.", instance, childInstance);
                }
                zkc.exists(registeredPath, new RegisteredWatcher(startSignal));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Waiting for start signal.");
                }
                startSignal.await();
                serverRunner.exec();
            } else {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Server handler <{}:{}> is not running", instance, childInstance);
                }
                serverRunner.exec();
            }

            return result;
        }
    }

    public ServerManager(Configuration conf, ZkClient zkc,
            DcsNetworkConfiguration netConf, String instance, int infoPort,
            int childServers) throws Exception {
        this.conf = conf;
        this.zkc = zkc;
        this.netConf = netConf;
        this.hostName = netConf.getHostName();
        this.instance = Integer.parseInt(instance);
        this.infoPort = infoPort;
        this.childServers = childServers;
        this.parentDcsZnode = this.conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,
                Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
        this.preloadTablesEnable = this.conf
                .getInt(Constants.DCS_SERVER_PROGRAM_PRELOAD_TABLES,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_PRELOAD_TABLES);
        this.encryptBase64Enable = this.conf
                .getInt(Constants.DCS_SERVER_PROGRAM_ENCRYPT_BASE64_ENABLE,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_ENCRYPT_BASE64_ENABLE);
        this.connectingTimeout = this.conf.getInt(
                Constants.DCS_SERVER_USER_PROGRAM_CONNECTING_TIMEOUT,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_CONNECTING_TIMEOUT);
        this.zkSessionTimeout = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_ZOOKEEPER_SESSION_TIMEOUT,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_ZOOKEEPER_SESSION_TIMEOUT);
        this.userProgExitAfterDisconnect = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_EXIT_AFTER_DISCONNECT,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_EXIT_AFTER_DISCONNECT);
        this.maxHeapPctExit = this.conf.getInt(
                Constants.DCS_SERVER_USER_PROGRAM_MAX_HEAP_PCT_EXIT,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_MAX_HEAP_PCT_EXIT);
        this.statisticsIntervalTime = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_INTERVAL_TIME,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_INTERVAL_TIME);
        this.statisticsLimitTime = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_LIMIT_TIME,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_LIMIT_TIME);
        this.statisticsCacheSize = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_CACHE_SIZE,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_CACHE_SIZE);
        this.statisticsType = this.conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_TYPE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_TYPE);
        this.statisticsEnable = this.conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_ENABLE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_ENABLE);
        this.internalQueriesInRepo = this.conf
                .get(Constants.DCS_SERVER_USER_PROGRAM_STORE_INTERNAL_QUERIES_IN_REPO,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_INTERNAL_QUERIES_IN_REPO);
        this.sqlplanEnable = this.conf
                .get(Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_SQLPLAN_ENABLE,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_SQLPLAN_ENABLE);
        this.publishStatsToOpenTSDB = this.conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_STATISTICS_OPENTSDB_ENABLE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_STATISTICS_OPENTSDB_ENABLE);
        this.tsdHost = this.conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_OPENTSDB_HOST,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_OPENTSDB_HOST);
        this.tsdPort = this.conf.getInt(
                Constants.DCS_SERVER_USER_PROGRAM_OPENTSDB_PORT,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_OPENTSDB_PORT);
        this.userProgPortMapToSecs = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_PORT_MAP_TIMEOUT_SECONDS,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_PORT_MAP_TIMEOUT_SECONDS);
        this.userProgPortBindToSecs = this.conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_PORT_BIND_TIMEOUT_SECONDS,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_PORT_BIND_TIMEOUT_SECONDS);
        this.exitSessionsCount = conf
                .getInt(Constants.DCS_SERVER_PROGRAM_EXIT_SESSIONS_COMPLETED_COUNT,
                        Constants.DEFAULT_DCS_SERVER_PROGRAM_EXIT_SESSIONS_COMPLETED_COUNT);
        this.exitLiveTimeMinutes = conf
                .getInt(Constants.DCS_SERVER_PROGRAM_EXIT_LIVE_TIME,
                        Constants.DEFAULT_DCS_SERVER_PROGRAM_EXIT_LIVE_TIME);
        this.wmsTimeoutSeconds = conf
                .getInt(Constants.DCS_SERVER_PROGRAM_WMS_TIMEOUT,
                        Constants.DEFAULT_DCS_SERVER_PROGRAM_WMS_TIMEOUT);
        this.maxRestartAttempts = conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_RESTART_HANDLER_ATTEMPTS,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_RESTART_HANDLER_ATTEMPTS);
        this.retryIntervalMillis = conf
                .getInt(Constants.DCS_SERVER_USER_PROGRAM_RESTART_HANDLER_RETRY_INTERVAL_MILLIS,
                        Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_RESTART_HANDLER_RETRY_INTERVAL_MILLIS);

        this.useSSLEnable = conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_USESSL_ENABLE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_USESSL_ENABLE);
        this.mdsEnable = conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_MDS_ENABLE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_MDS_ENABLE);

        this.mdsLimitMs = conf.get(
                Constants.DCS_SERVER_USER_PROGRAM_MDS_LIMIT_MS,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_MDS_LIMIT_MS);
        this.userProgKeepaliveStatus = conf.get(
                Constants.DCS_SERVER_PROGRAM_TCP_KEEPALIVE_STATUS,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_TCP_KEEPALIVE_STATUS);
        this.userProgKeepaliveIdletime = conf.getInt(
                Constants.DCS_SERVER_PROGRAM_TCP_KEEPALIVE_IDLETIME,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_TCP_KEEPALIVE_IDLETIME);
        this.userProgKeepaliveIntervaltime = conf.getInt(
                Constants.DCS_SERVER_PROGRAM_TCP_KEEPALIVE_INTERVALTIME,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_TCP_KEEPALIVE_INTERVALTIME);
        this.userProgKeepaliveRetrycount = conf.getInt(
                Constants.DCS_SERVER_PROGRAM_TCP_KEEPALIVE_RETRYCOUNT,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_TCP_KEEPALIVE_RETRYCOUNT);
        this.userProgIsGoingWms = conf.get(
                Constants.DCS_SERVER_PROGRAM_IFGOINGWMS_STATUS,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_IFGOINGWMS_STATUS);
        this.userProgRMSMemLimit = conf.getInt(Constants.DCS_SERVER_PROGRAM_RMS_MEM_LIMIT_WARNING,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_RMS_MEM_LIMIT_WARNING);
        if (userProgRMSMemLimit < 10 || userProgRMSMemLimit > 90) {
            LOG.warn("Invalid property setting : {}, value {}. change to default {}",
                    Constants.DCS_SERVER_PROGRAM_RMS_MEM_LIMIT_WARNING, userProgRMSMemLimit,
                    Constants.DEFAULT_DCS_SERVER_PROGRAM_RMS_MEM_LIMIT_WARNING);
            userProgRMSMemLimit = Constants.DEFAULT_DCS_SERVER_PROGRAM_RMS_MEM_LIMIT_WARNING;
        }
        this.numaNode = conf.get(Constants.DCS_SERVER_USER_PROGRAM_NUMA_NODE_RANGE, Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_NUMA_NODE_RANGE);
        this.cpuLimitPercentWhileStart = conf.getInt(Constants.DCS_SERVER_PROGRAM_CPU_LIMIT_PERCENT_WHILE_START,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_CPU_LIMIT_PERCENT_WHILE_START);
        if (cpuLimitPercentWhileStart <= 20 || cpuLimitPercentWhileStart > 100) {
            LOG.warn("Invalid property setting : {}, value {}. change to default {}",
                    Constants.DCS_SERVER_PROGRAM_CPU_LIMIT_PERCENT_WHILE_START, this.cpuLimitPercentWhileStart,
                    Constants.DEFAULT_DCS_SERVER_PROGRAM_CPU_LIMIT_PERCENT_WHILE_START);
            this.cpuLimitPercentWhileStart = Constants.DEFAULT_DCS_SERVER_PROGRAM_CPU_LIMIT_PERCENT_WHILE_START;
        }

        serverHandlers = new ServerHandler[this.childServers];
        serverHandlerSet = new HashSet<>();
        executorService = Executors
            .newFixedThreadPool(this.childServers);
        completionService = new ExecutorCompletionService<Integer>(
            executorService);
    }

    private void loadMetadataCache() {

        // Check if Load cache cqd is enabled and then load metadata cache locally on each node
        int  exitCode=0;
        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setHostName(hostName);
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        String command = "load_shared_cache local";
        scriptContext.setCommand(command);
        for (int i=0; i<3; i++) {
           scriptContext.cleanStdDatas();
           ScriptManager.getInstance().runScript(scriptContext);
           exitCode = scriptContext.getExitCode();
            if (LOG.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Exit code for load_shared_cache <").append(exitCode).append(">");
                if (!scriptContext.getStdOut().toString().isEmpty())
                    sb.append(", stdout <").append(scriptContext.getStdOut().toString()).append(">.");
                if (!scriptContext.getStdErr().toString().isEmpty())
                    sb.append(", stderr <").append(scriptContext.getStdErr().toString()).append(">.");
                LOG.debug(sb.toString());
            }
           if (exitCode != 2) 
              break;
           if ( i < 2 ) {
             try {
               Thread.sleep(5000);
             } catch (InterruptedException ie) {}
           }
        }
    }

    private void loadSharedDataCache() {

        // Check if Load cache cqd is enabled and then load data cache locally on each node
        int  exitCode=0;
        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setHostName(hostName);
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        String command = "load_shared_data_cache local";
        scriptContext.setCommand(command);
        for (int i=0; i<3; i++) {
           scriptContext.cleanStdDatas();
           ScriptManager.getInstance().runScript(scriptContext);
           exitCode = scriptContext.getExitCode();
           StringBuilder sb = new StringBuilder();
           sb.append("Exit code for load_shared_data_cache <" + exitCode + ">");
           if (!scriptContext.getStdOut().toString().isEmpty())
              sb.append(", stdout <" + scriptContext.getStdOut().toString() + ">.");
           if (!scriptContext.getStdErr().toString().isEmpty())
              sb.append(", stderr <" + scriptContext.getStdErr().toString() + ">.");
            if (LOG.isInfoEnabled())
                LOG.info(sb.toString());
           if (exitCode != 2) 
              break;
           if ( i < 2 ) {
             try {
               Thread.sleep(5000);
             } catch (InterruptedException ie) {}
           }
        }
    }

    private boolean isTrafodionRunning() {

        boolean currentTrafodionState = false;
        // Check if the given Node is up and running
        //
        // Calls node_hc script to check if
        // 1. monitor is running on the current node
        // 2. SQ_TXNSVC_READY is set to 1 and
        // 3. RMS processes(2) are up on the current node, if configured to start (default SQ_START_RMS=1).
        //
        //   node_hc returns:
        //     0 - Monitor is up, Transaction Service is ready, local RMS processes are up
        //     1 - if there is an error/timeout
        //
        // Returns true if node is ready for transactions and local RMS processes are up.
 
        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setHostName(hostName);
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        String command = "node_hc";
        scriptContext.setCommand(command);
        ScriptManager.getInstance().runScript(scriptContext);// This will
                                                             // block while
                                                             // script is
                                                             // running
        int exitCode = scriptContext.getExitCode();
        currentTrafodionState = (exitCode == 0) ;
        
        if ( previousTrafodionState == false && currentTrafodionState == true ) {
            loadMetadataCache();
            loadSharedDataCache();
        }

        previousTrafodionState = currentTrafodionState ;

        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("node_hc exit code <").append(scriptContext.getExitCode()).append(">");
            if (!scriptContext.getStdOut().toString().isEmpty())
                sb.append(", stdout <").append(scriptContext.getStdOut().toString()).append(">.");
            if (!scriptContext.getStdErr().toString().isEmpty())
                sb.append(", stderr <").append(scriptContext.getStdErr().toString()).append(">.");
            LOG.debug(sb.toString());
        }
        return exitCode == 0;
    }

    @Override
    public Boolean call() throws Exception {

        try {
            getMaster();
            featureCheck();
            registerInRunning(instance);
            RetryCounter retryCounter = RetryCounterFactory.create(maxRestartAttempts, retryIntervalMillis);
            while (!isTrafodionRunning()) {
               if (!retryCounter.shouldRetry()) {
                  throw new IOException("Trafodion is not up or partially up and not operational");
               } else {
                  retryCounter.sleepUntilNextRetry();
                  retryCounter.useRetry();
               }
            }

            // When started from bin/dcs-start.sh script childServers will
            // contain the
            // count passed in from the servers.sh script. However when
            // DcsServer is
            // killed or dies for any reason DcsMaster restarts it using
            // /bin/dcs-daemon script
            // which DOES NOT set childServers count.
            String cpuNodeBind = "numactl --cpunodebind whichnode --preferred whichnode";
            Object[] cpuNodes = parseNumaNode(numaNode);
            int averageSize = 0;
            if(cpuNodes !=null && cpuNodes.length != 0){
                averageSize = childServers/cpuNodes.length + 1;
            }

            for (int childInstance = 1; childInstance <= childServers; childInstance++) {
                if(cpuNodes == null || cpuNodes.length == 0){
                    serverHandlers[childInstance-1] = new ServerHandler(conf, childInstance, "");
                }else{
                    serverHandlers[childInstance-1] = new ServerHandler(conf, childInstance, cpuNodeBind.replaceAll("whichnode", String.valueOf(cpuNodes[childInstance/averageSize])));
                }
                if(addToServerHandlerSet(serverHandlers[childInstance-1])){
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Try to submit server handler job <{}:{}>", instance,
                                childInstance);
                    }
                    completionService.submit(serverHandlers[childInstance-1]);
                }
            }

            while (true) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Waiting for any server handler to exit.");
                }

                // blocks waiting for any ServerHandler to exit
                Future<Integer> f = completionService.take();
                if (f != null) {
                    Integer result = f.get();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Server handler <{}:{}> exit", instance, result);
                    }

                    retryCounter = RetryCounterFactory.create(maxRestartAttempts, retryIntervalMillis);
                    int childInstance = result.intValue();
                    ServerHandler previousServerHandler = serverHandlers[childInstance - 1];
                    boolean isRemoved = removeFromServerHandlerSet(previousServerHandler);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("ServerManager remove <{}:{}> : {}", instance, result, isRemoved);
                    }
                    while (!isTrafodionRunning()) {
                        if (!retryCounter.shouldRetry()) {
                            throw new IOException("Trafodion is not up or partially up and not operational");
                        } else {
                            retryCounter.sleepUntilNextRetry();
                            retryCounter.useRetry();
                        }
                    }
                    if (previousServerHandler.retryCounter.shouldRetryInnerMinutes()) {
                        serverHandlers[childInstance - 1] = previousServerHandler;
                        if (addToServerHandlerSet(serverHandlers[childInstance - 1])){
                            if (LOG.isInfoEnabled()) {
                                LOG.info("script submit success with <{}:{}>", instance,
                                        childInstance);
                                completionService.submit(serverHandlers[childInstance - 1]);
                            }
                        }
                    } else {
                        serverHandlers[childInstance - 1] = null;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            if (executorService != null)
                executorService.shutdown();
            throw e;
        }
        /*
         * ExecutorService pool = Executors.newSingleThreadExecutor();
         * 
         * try { getMaster(); registerInRunning(); featureCheck();
         * 
         * Callable<Boolean> serverMonitor = new ServerMonitor();
         * Callable<ScriptContext> serverRunner = new ServerRunner();
         * 
         * long timeoutMillis=5000;
         * 
         * while(true) { Future<Boolean> monitor = pool.submit(serverMonitor);
         * if(false == monitor.get().booleanValue()) { //blocking call
         * LOG.info("User program is not running"); Future<ScriptContext> runner
         * = pool.submit(serverRunner); ScriptContext scriptContext =
         * runner.get();//blocking call
         * 
         * StringBuilder sb = new StringBuilder(); sb.append("exit code [" +
         * scriptContext.getExitCode() + "]"); if(!
         * scriptContext.getStdOut().toString().isEmpty())
         * sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
         * if(! scriptContext.getStdErr().toString().isEmpty())
         * sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
         * LOG.info(sb.toString());
         * 
         * switch(scriptContext.getExitCode()) { case 3:
         * LOG.error("Trafodion is not running"); timeoutMillis=60000; break;
         * case 127: LOG.error("Cannot find user program executable");
         * timeoutMillis=60000; break; default: timeoutMillis=5000; }
         * 
         * } else { timeoutMillis=5000; }
         * 
         * try { Thread.sleep(timeoutMillis); } catch (InterruptedException e) {
         * } }
         * 
         * } catch (Exception e) { e.printStackTrace(); LOG.error(e);
         * pool.shutdown(); throw e; }
         */
    }

    private void featureCheck() {
        final String msg1 = "Property "
                + Constants.DCS_SERVER_USER_PROGRAM
                + " is false. "
                + "Please add to your dcs-site.xml file and set <value>false</value> to <value>true</value>.";
        final String msg2 = "Environment variable $TRAF_HOME is not set.";

        boolean ready = false;
        while (!ready) {
            userProgEnabled = conf.getBoolean(
                    Constants.DCS_SERVER_USER_PROGRAM,
                    Constants.DEFAULT_DCS_SERVER_USER_PROGRAM);
            userProgramHome = GetJavaProperty.getProperty(Constants.DCS_USER_PROGRAM_HOME);
            userProgramCommand = conf.get(
                    Constants.DCS_SERVER_USER_PROGRAM_COMMAND,
                    Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_COMMAND);

            if (userProgEnabled && !userProgramHome.isEmpty()
                    && !userProgramCommand.isEmpty()) {
                ready = true;
                continue;
            }

            if (!userProgEnabled)
                LOG.error(msg1);
            if (userProgramHome.isEmpty())
                LOG.error(msg2);

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("User program enabled");
        }
    }

    private void getMaster() {
        boolean found = false;

        while (!found) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Checking DcsMaster znode <{}>.",
                        parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER);
            }
            try {
                Stat stat = zkc.exists(parentDcsZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER, false);
                if (stat != null) {
                    List<String> nodes = zkc.getChildren(parentDcsZnode
                            + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER, null);
                    if (!nodes.isEmpty()) {
                        StringTokenizer st = new StringTokenizer(nodes.get(0),
                                ":");
                        while (st.hasMoreTokens()) {
                            masterHostName = st.nextToken();
                            port = Integer.parseInt(st.nextToken());
                            portRange = Integer.parseInt(st.nextToken());
                            masterStartTime = Long.parseLong(st.nextToken());
                        }
                        found = true;
                        if (LOG.isInfoEnabled()) {
                            LOG.info(
                                    "DcsMaster znode <{}> found. DcsMaster hostname <{}>, port <{}>, port range <{}>, start time <{}>",
                                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER,
                                    masterHostName, port, portRange, masterStartTime);
                        }
                    }
                } else {
                    LOG.warn("DcsMaster znode <{}> not found, wait 5 second and retry.",
                            parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER);
                }

                if (!found) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                    }
                }

            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void registerInRunning(int instance) {
        String znode = parentDcsZnode
                + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING + "/"
                + hostName + ":" + instance;
        String data = "ENABLE" + ":" + infoPort + ":"
                + System.currentTimeMillis();
        try {
            Stat stat = zkc.exists(znode, false);
            if (stat == null) {
                zkc.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                zkc.setData(znode, data.getBytes(), -1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Created znode <{}>, data <{}>", znode, data);
                }
            } else {
                zkc.setData(znode, data.getBytes(), 1);
                if (LOG.isInfoEnabled()) {
                    LOG.info("znode exist <{}>, modify data <{}>", znode, data);
                }
            }
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private Lock serverHandlerAddLock = new ReentrantLock();
    public boolean addToServerHandlerSet(ServerHandler serverHandler){
        if(!serverHandlerSet.contains(serverHandler)){
            try{
                serverHandlerAddLock.lock();
            if(!serverHandlerSet.contains(serverHandler)){
                return serverHandlerSet.add(serverHandler);
            }
            }finally {
                serverHandlerAddLock.unlock();
            }
        }
        return false;
    }

    private Lock serverHandlerRemoveLock = new ReentrantLock();
    public boolean removeFromServerHandlerSet(ServerHandler serverHandler){
        try{
            if (serverHandlerSet.contains(serverHandler)) {
                serverHandlerRemoveLock.lock();
                if (serverHandlerSet.contains(serverHandler)) {
                    return serverHandlerSet.remove(serverHandler);
                }
            }
        }finally {
            serverHandlerRemoveLock.unlock();
        }
        return false;
    }

    protected Object[] parseNumaNode(String str) {
        List<Integer> list = new ArrayList<>();
        if(str.equals(Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_NUMA_NODE_RANGE)){
            return null;
        }
        String[] arr = str.split(",");
        for (String str2 : arr) {
            if (!str2.contains("-")) {
                list.add(Integer.parseInt(str2));
            } else {
                String[] tmp = str2.split("-");
                for (int k = Integer.parseInt(tmp[0]); k <= Integer.parseInt(tmp[1]); k++) {
                    list.add(k);
                }
            }
        }
        Object[] res = list.toArray();
        return res;
    }

    public String getMasterHostName() {
        return masterHostName;
    }

    public String getZKParentDcsZnode() {
        return parentDcsZnode;
    }

    public String getUserProgramHome() {
        return userProgramHome;
    }

    public ServerHandler[] getServerHandlers(){
        return serverHandlers;
    }

    public CompletionService<Integer> getCompletionService(){
        return completionService;
    }

}
