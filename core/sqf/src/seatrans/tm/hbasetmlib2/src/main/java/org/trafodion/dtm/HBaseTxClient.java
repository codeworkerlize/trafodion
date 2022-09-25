// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.dtm;

import io.esgyn.client.*;
import io.esgyn.utils.*;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.PropertyConfigurator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.transactional.HBaseDCZK;
import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.TrafodionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.UnsuccessfulDDLException;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.ATRConfig;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;
import org.apache.hadoop.hbase.client.transactional.TransactionalReturn;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.client.transactional.TmDDL;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.transactional.XdcTransType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteArrayKey;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.trafodion.dtm.HBaseTmZK;
import org.trafodion.dtm.TmAuditTlog;
import org.trafodion.dtm.TransactionManagerException;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.hadoop.hbase.pit.SnapshotMeta;
import org.apache.hadoop.hbase.pit.SnapshotMetaRecordType;;
import org.apache.hadoop.hbase.pit.SnapshotMutationFlushRecord;;

import org.apache.zookeeper.KeeperException;
import org.trafodion.sql.TrafConfiguration;
//import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.*;

public class HBaseTxClient {

   static final Log LOG = LogFactory.getLog(HBaseTxClient.class);
   private static Connection connection;
   private static Configuration adminConf;
   private static Connection adminConnection;
   private static Object adminConnLock = new Object();
   private static TmAuditTlog tLog;
   private static HBaseTmZK tmZK;
   private static RecoveryThread recovThread;
   private static RecoveryThread peerRecovThread = null;
   private static MutationFlushThread mFlushThread = null;
   private static TmDDL tmDDL;
   private short dtmID;
   private int stallWhere;
   private static int dynamic_dtm_logging = 0;
   private IdTm idServer;
   private static final int ID_TM_SERVER_TIMEOUT = 1000;
   private static boolean bSynchronized = false;
   private static boolean recoveryToPitMode = false;
   private static boolean skipConflictCheck = false;
   protected static Map<Integer, TmAuditTlog> peer_tLogs;
   protected static Map<Integer, HBaseTmZK> peer_tmZKs;
   private static int trafClusterId = 1;
   private static int trafInstanceId = 1;
   private static boolean synchronousWrites;
   private static Map<Integer, Integer> commit_migration_clusters = new HashMap<Integer,Integer>();
   //private long lastTransid = 0L;
   //TRAF_RESERVED_NAMESPACE5 is defined in sql/common/ComSmallDefs.h
   //Any change in namespace needs to be reflected here and ComSmallDefs.h
   private static String nameSpace = "TRAF_RSRVD_5";
   static long timerCountPrint = 200;

   public enum AlgorithmType{
     MVCC, SSCC
   }
   public AlgorithmType TRANSACTION_ALGORITHM;

   boolean useTlog;
   boolean useForgotten;
   boolean forceForgotten;
   boolean useRecovThread;
   boolean useCrashRecov = false;
   
   boolean useMonarch = false;

   private static Configuration config;
   TransactionManager trxManager;
   MTransactionManager mtrxManager;
   static Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
   static Map<Long, MTransactionState> mapMTransactionStates;
   Map<Integer, RecoveryThread> mapRecoveryThreads = new HashMap<Integer, org.trafodion.dtm.HBaseTxClient.RecoveryThread>();
   static final Object mapLock = new Object();

   private static STRConfig pSTRConfig = null;
   private static ATRConfig pATRConfig = null;
   private static ArrayList<Integer> peerId_list = new ArrayList<Integer>();
   private static int peerId_index = 0;

   public static final int XDC_UP          = 1;   // 000000001
   public static final int XDC_DOWN        = 2;   // 000000010
   public static final int SYNCHRONIZED    = 4;   // 000000100
   public static final int SKIP_CONFLICT   = 8;   // 000001000
   public static final int SKIP_REMOTE_CK  = 16;  // 000010000
   public static final int INCREMENTALBR   = 32;  // 000100000
   public static final int TABLE_ATTR_SET  = 64;  // 001000000
   public static final int SKIP_SDN_CDC    = 128; // 010000000
   public static final int PIT_ALL         = 256; // 100000000
   public static final int TIMELINE        = 512; // 1000000000 
   public static final int ADD_TOTALNUM    = 1024; // 10000000000

   void setupLog4j() {
        System.setProperty("trafodion.logdir", System.getenv("TRAF_LOG"));
        System.setProperty("hostName", System.getenv("HOSTNAME"));
        String confFile = System.getenv("TRAF_CONF")
            + "/log4j.dtm.config";
        PropertyConfigurator.configure(confFile);
	System.out.println("In seupLog4j"
			   + ", confFile: " + confFile
			   );
   }

   //This init is never used. Can be removed?

   public boolean init(short dtmid) throws IOException {
      setupLog4j();
      if (LOG.isDebugEnabled()) LOG.debug("Enter init(" + dtmid + ")");
      if (config == null) {
         config = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
         //config = HBaseConfiguration.create();
         // TODO:: Should it get this connection from pSTRConfig
         //this.config.set("hbase.client.retries.number", "3");
         
         int clientRetryInt = 300;  
         String clientRetry = System.getenv("TM_STRCONFIG_CLIENT_RETRIES_NUMBER");
         if (clientRetry != null){
           clientRetryInt = Integer.parseInt(clientRetry.trim());
           if (LOG.isInfoEnabled()) LOG.info("HBaseTxClient: TM_STRCONFIG_RPC_TIMEOUT: " + clientRetryInt);
         }
         String value1 = this.config.getTrimmed("hbase.client.retries.number");
         if (LOG.isDebugEnabled()) LOG.debug("HBaseTxClient: STRConfig " + "hbase.client.retries.number " + value1);
         if (  (value1 == null) || (Integer.parseInt(value1) > clientRetryInt)  ) {
             this.config.set("hbase.client.retries.number", Integer.toString(clientRetryInt));
             value1 = this.config.getTrimmed("hbase.client.retries.number");
             if (LOG.isInfoEnabled()) LOG.info("HBaseTxClient: STRConfig " + "revised hbase.client.retries.number " + value1);
         }          

         int connThreadPoolMaxInt = 2;
         String connThreadPoolMax = System.getenv("HBASE_HCONNECTION_THREADS_MAX");
         if (connThreadPoolMax != null){
           connThreadPoolMaxInt = Integer.parseInt(connThreadPoolMax.trim());
           if (LOG.isInfoEnabled()) LOG.info("HBaseTxClient: HBASE_HCONNECTION_THREADS_MAX: " + connThreadPoolMaxInt);
         }

         int connThreadPoolCoreInt = 2;
         String connThreadPoolCore = System.getenv("HBASE_HCONNECTION_THREADS_CORE");
         if (connThreadPoolCore != null){
           connThreadPoolCoreInt = Integer.parseInt(connThreadPoolCore.trim());
           if (LOG.isInfoEnabled()) LOG.info("HBaseTxClient: HBASE_HCONNECTION_THREADS_CORE: " + connThreadPoolCoreInt);
         }
         
         this.config.set("hbase.client.pause", "1000");
         this.config.set("zookeeper.recovery.retry", "1");
         this.config.setInt("hbase.hconnection.threads.max", connThreadPoolMaxInt);
         this.config.setInt("hbase.hconnection.threads.core", connThreadPoolCoreInt);

         String tmRpcTimeout = System.getenv("TM_RPC_TIMEOUT");
         if (tmRpcTimeout != null) {
             this.config.set("hbase.rpc.timeout", tmRpcTimeout.trim());
         } else {
             this.config.set("hbase.rpc.timeout", Integer.toString(60000));
         }

         connection = ConnectionFactory.createConnection(config);

         String rpcTimeout = System.getenv("HAX_ADMIN_RPC_TIMEOUT");
         if (rpcTimeout != null){
           synchronized(adminConnLock) {
              if (adminConnection == null) {
                 adminConf = new Configuration(config);
                 int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
                 String value = adminConf.getTrimmed("hbase.rpc.timeout");
                 adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
                 String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
                 LOG.info("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
                 adminConnection = ConnectionFactory.createConnection(adminConf);
              }
            } // sync
         }
         else {
              adminConnection = this.connection;
         }

      }
 
      try {
         pSTRConfig = STRConfig.getInstance(config);
         pATRConfig = ATRConfig.newInstance(config);
      }
      catch (ZooKeeperConnectionException zke) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " , zke);
         throw new IOException(zke);
      }
      catch (KeeperException zke1) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " , zke1);
         throw new IOException(zke1);
      }
      catch (InterruptedException zke2) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " , zke2);
         throw new IOException(zke2);
      }
      trafClusterId = 1;
      if (pSTRConfig != null) {
         LOG.info("Number of Trafodion Nodes: " + pSTRConfig.getTrafodionNodeCount());

         trafClusterId = pSTRConfig.getTrafClusterIdInt();
         LOG.info("trafClusterId set to " + trafClusterId);
         trafInstanceId = pSTRConfig.getTrafInstanceIdInt();
         LOG.info("trafInstanceId set to " + trafInstanceId);

         LOG.info("Setting up " + pSTRConfig.getPeerConfigurations().size() + " peer configuration");
         for ( Map.Entry<Integer, Configuration> e : pSTRConfig.getPeerConfigurations().entrySet() ) {
            LOG.info("Setting dtmid for entry " + e.getKey());
            e.getValue().set("dtmid", String.valueOf(dtmid));
            e.getValue().set("NAME_SPACE", nameSpace);
            e.getValue().set("CONTROL_POINT_TABLE_NAME", nameSpace + ":" + "TRAFODION._DTM_.TLOG" + String.valueOf(dtmid) + "_CONTROL_POINT");
            e.getValue().set("TLOG_TABLE_NAME", nameSpace + ":" + "TRAFODION._DTM_.TLOG" + String.valueOf(dtmid));
         }
      }

      this.dtmID = dtmid;
      this.useRecovThread = false;
      this.useCrashRecov = false;
      this.stallWhere = 0;
      skipConflictCheck = false;
      boolean nsNotExist = false;

      Admin admin = connection.getAdmin();
      try{
          TableName[] listTableName = admin.listTableNamesByNamespace(nameSpace);
      }
      catch(Exception e)
      {
        nsNotExist = true;
      }
      if( nsNotExist == true) {
        try {
          
          NamespaceDescriptor.Builder builder = 
              NamespaceDescriptor.create(nameSpace);
          NamespaceDescriptor nsd = builder.build();
          admin.createNamespace(nsd);
        }catch(NamespaceExistException ne) {
//          if(LOG.isTraceEnabled()) LOG.trace("Ignoring NamespaceExistsException already exists exception ", ne);
        }catch(IOException e) {
          LOG.error("NameSpace creation Exception: ", e);
          throw new IOException ("NameSpace creation Exception: ", e);
        } finally {
          admin.close();
        }
      }
      else
          admin.close();

      String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
      if( usePIT != null)
      {
          recoveryToPitMode = (Integer.parseInt(usePIT.trim()) == 1) ? true : false;
      }

      String useTimer = System.getenv("TM_USE_RECOVER_TIMER_PRINT");
      if( useTimer != null)
      {
          timerCountPrint = Integer.parseInt(useTimer);
      }

      String useSSCC = System.getenv("TM_USE_SSCC");
      TRANSACTION_ALGORITHM = AlgorithmType.MVCC;
      try {
         if (useSSCC != null)
         TRANSACTION_ALGORITHM = (Integer.parseInt(useSSCC.trim()) == 1) ? AlgorithmType.SSCC :AlgorithmType.MVCC ;
      } catch (NumberFormatException e) {
         LOG.error("TRANSACTION_ALGORITHM is not valid in ms.env");
      }

      synchronousWrites = false;
      try {
         String synchTlogsS = System.getenv("TM_TLOG_SYNCHRONOUS_WRITES");
         if (synchTlogsS != null){
            synchronousWrites = (Integer.parseInt(synchTlogsS.trim()) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("synchronousWrites != null");
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_SYNCHRONOUS_WRITES is not valid in ms.env");
      }

      LOG.info("synchronousWrites is " + synchronousWrites);

      useForgotten = true;
         String useAuditRecords = System.getenv("TM_ENABLE_FORGOTTEN_RECORDS");
      try {
         if (useAuditRecords != null) {
            useForgotten = (Integer.parseInt(useAuditRecords.trim()) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_FORGOTTEN_RECORDS is not valid in ms.env");
      }
      LOG.info("useForgotten is " + useForgotten);

      forceForgotten = false;
         String forgottenForce = System.getenv("TM_TLOG_FORCE_FORGOTTEN");
      try {
         if (forgottenForce != null){
            forceForgotten = (Integer.parseInt(forgottenForce.trim()) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_TLOG_FORCE_FORGOTTEN is not valid in ms.env");
      }
      LOG.info("forceForgotten is " + forceForgotten);

      useTlog = false;
      useRecovThread = false;
      String useAudit = System.getenv("TM_ENABLE_TLOG_WRITES");
      try {
         if (useAudit != null){
            useTlog = useRecovThread = (Integer.parseInt(useAudit.trim()) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_TLOG_WRITES is not valid in ms.env");
      }

      String crashRecovString = System.getenv("TMRECOV_CRASH_RECOVERY");
      if (crashRecovString != null){
         useCrashRecov = (Integer.parseInt(crashRecovString.trim()) == 1) ? true : false;
      }
      LOG.info("useCrashRecov is " + useCrashRecov);

      idServer = new IdTm(false);
      if (useTlog) {
         Connection lv_connection;
         Configuration lv_config;
         try {
            lv_connection = pSTRConfig.getPeerConnection(trafClusterId);
            lv_config = pSTRConfig.getPeerConfiguration(trafClusterId);
            if (LOG.isTraceEnabled()) LOG.trace("cluster " + trafClusterId + " retrieved connection "
                          + lv_connection + ", config: " + lv_config);
            tLog = new TmAuditTlog(lv_config, lv_connection); // connection 0 is the local node
            if (LOG.isTraceEnabled()) LOG.trace("Created local Tlog with peer " + trafClusterId + ", connection: "
               + lv_connection.toString() + ", config: " + lv_config.toString());
         } catch (IOException e ){
            LOG.error("Unable to create TmAuditTlog, throwing exception " , e);
            throw new IOException("Unable to create TmAuditTlog, throwing exception " , e);
         }

         bSynchronized = (pSTRConfig.getConfiguredPeerCount() > 0);

         if (bSynchronized) {
            peer_tLogs = new HashMap<Integer, TmAuditTlog>();
            peer_tmZKs = new HashMap<Integer, HBaseTmZK>();
            for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
               int lv_peerId = entry.getKey();
               if (lv_peerId == 0) continue;
	           if (! pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
		         continue;
	           }
               lv_connection = entry.getValue();
               lv_config = pSTRConfig.getPeerConfiguration(lv_peerId);
               try {
                  if (LOG.isTraceEnabled()) LOG.trace("Creating peer Tlog for peer " + lv_peerId +
                                          ", connection: " + lv_connection.toString() + ", config: " + lv_config.toString());
                  peer_tLogs.put(lv_peerId, new TmAuditTlog(lv_config, lv_connection));
                  if (LOG.isTraceEnabled()) LOG.trace("Peer Tlog for peer " + lv_peerId + " created");
                  peer_tmZKs.put(lv_peerId, new HBaseTmZK(lv_config, this.dtmID, true));
                  if (LOG.isTraceEnabled()) LOG.trace("Peer HBaseTmZK for peer " + lv_peerId + " created");
                  peerId_list.add(lv_peerId);
                  if (LOG.isTraceEnabled()) LOG.trace("Add peer id " + lv_peerId + " into peerId_list at index " + peerId_index);
                  peerId_index = peerId_index + 1;
               } catch (IOException e ){
                   LOG.error("Unable to create TmAuditTlog, throwing exception " , e);
                   throw new IOException("Unable to create TmAuditTlog, throwing exception " , e);
               }
            }
         }
         else {
            if (LOG.isTraceEnabled()) LOG.trace("bSynchronized is false ");
         }
      }
      else {
          if (LOG.isTraceEnabled()) LOG.trace("Tlog is not enabled ");
      }

      if (LOG.isTraceEnabled()) {
         LOG.trace("Environment Variables Start =============== ");
         Map<String, String> lv_env_map = System.getenv();
         for (Entry<String, String> lv_env_row : lv_env_map.entrySet()) {
	     LOG.trace(lv_env_row.getKey() + ":" + lv_env_row.getValue());
	  }
         LOG.trace("Environment Variables End =============== ");
      }

      tmDDL = new TmDDL(config);
      trxManager = TransactionManager.getInstance(config, connection);

	  if (LOG.isTraceEnabled()) LOG.trace("Calling trxManager.init(tmDDL)");
      trxManager.init(tmDDL);

      if (useRecovThread) {
         if (LOG.isDebugEnabled()) LOG.debug("Entering recovThread Usage");
          try {
              tmZK = new HBaseTmZK(config, dtmID);
          } catch (IOException e ) {
              LOG.error("Unable to create HBaseTmZK TM-zookeeper class, throwing exception", e);
              throw e;
          }
          recovThread = new RecoveryThread(tLog,
                                           tmZK,
                                           trxManager,
                                           this,
                                           useForgotten,
                                           forceForgotten,
                                           useTlog,
                                           false,
                                           false,
                                           false);
          recovThread.start();
      }

      if (useCrashRecov && (this.dtmID == 0)){
         mFlushThread = new MutationFlushThread(this.config, this.connection);
         mFlushThread.start();
      }

      try {
         String useMonarchString = System.getenv("TM_ENABLE_MONARCH");
	 if (LOG.isDebugEnabled()) LOG.debug("getenv(TM_ENABLE_MONARCH): " + useMonarchString);
         if (useMonarchString != null) {
            useMonarch = (Integer.parseInt(useMonarchString.trim()) != 0);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_ENABLE_MONARCH is not in ms.env");
      }

      if (useMonarch) {
	  if (LOG.isDebugEnabled()) LOG.debug("Creating Monarch Transaction Manager");
	  try {
	      mtrxManager = MTransactionManager.getInstance(config);
	  } catch (Exception e ){
	      LOG.error("Unable to create MTransactionManager, throwing exception", e);
	      throw new RuntimeException(e);
	  }

	  if (LOG.isDebugEnabled()) LOG.debug("Calling mtrxManager.init");
	  if (mtrxManager != null) {
	      try {
		  mtrxManager.init();
	      }
	      catch (Exception mtrx_init_exc) {
		  LOG.error("Error while initializing mtrxManager", mtrx_init_exc);
	      }
	  }
          mapMTransactionStates = MTransactionMap.getInstance();
      }
      
      if (LOG.isTraceEnabled()) LOG.trace("Exit init()");
      return true;
   }

   public short createEphemeralZKNode(byte[] pv_data) throws IOException {
       if (LOG.isInfoEnabled()) LOG.info("Enter createEphemeralZKNode for LDTM " + dtmID + ", data: " + new String(pv_data));

          //LDTM will start a new peer recovery thread to drive tlog sync if no been created
             if (peerRecovThread == null) {

                  try {
                      tmZK = new HBaseTmZK(config, (short) -2);
                  }catch (IOException e ){
                      LOG.error("Unable to create HBaseTmZK TM-zookeeper class, throwing exception");
                      throw new IOException("Unable to create HBaseTmZK TM-zookeeper class, throwing exception", e);
                  }
                  peerRecovThread = new RecoveryThread(tLog,
                                           tmZK,
                                           trxManager,
                                           this,
                                           useForgotten,
                                           forceForgotten,
                                           useTlog,
                                           true,
                                           false,
                                           false);
                  peerRecovThread.start();
             }


	   HBaseDCZK lv_zk = new HBaseDCZK(new Configuration(config));

	   String lv_my_cluster_id = lv_zk.get_my_id();
	   if (lv_my_cluster_id == null) {
	       if (LOG.isDebugEnabled()) LOG.debug("createEphemeralZKNode, my_cluster_id is null");
	       return 1;
	   }

	   String lv_node_id = new String(String.valueOf(dtmID));
	   String lv_node_data = new String(pv_data);
       
	   lv_zk.set_trafodion_znode(lv_my_cluster_id,
				     lv_node_id,
				     lv_node_data);
       return 0;
   }

   public TmDDL getTmDDL() {
        return tmDDL;
   }

   public synchronized void nodeDown(int nodeID) throws IOException {
       if(LOG.isInfoEnabled()) LOG.info("nodeDown -- ENTRY node ID: " + nodeID);

       RecoveryThread newRecovThread;
       if(dtmID == nodeID)
           throw new IOException("Down node ID is the same as current dtmID, Incorrect parameter");

       if (peerRecovThread == null){ //this is new LDTM, so start a new peer recovery thread to drive tlog sync
          tmZK = new HBaseTmZK(config, (short) -2);
          peerRecovThread = new RecoveryThread(tLog,
                                        tmZK,
                                        trxManager,
                                        this,
                                        useForgotten,
                                        forceForgotten,
                                        useTlog,
                                        true,
                                        false,
                                        false);
          peerRecovThread.start();
       }

           if(mapRecoveryThreads.containsKey(nodeID)) {
               if(LOG.isDebugEnabled()) LOG.debug("nodeDown called on a node that already has RecoveryThread running node ID: " + nodeID);
           }
           else {
               newRecovThread = new RecoveryThread(tLog,
                                                   new HBaseTmZK(config, (short) nodeID),
                                                   trxManager,
                                                   this,
                                                   useForgotten,
                                                   forceForgotten,
                                                   useTlog,
                                                   false,
                                                   true,
                                                   true);
               newRecovThread.start();
               mapRecoveryThreads.put(nodeID, newRecovThread);
               if(LOG.isTraceEnabled()) LOG.trace("nodeDown -- mapRecoveryThreads size: " + mapRecoveryThreads.size());
           }
       if(LOG.isInfoEnabled()) LOG.info("nodeDown -- EXIT node ID: " + nodeID);
   }

   public synchronized void nodeUp(int nodeID) throws IOException {
       if(LOG.isInfoEnabled()) LOG.info("nodeUp -- ENTRY node ID: " + nodeID);
       if (nodeID == dtmID) {
          LOG.error("Up node ID " + nodeID + " is the same as current dtmID, Incorrect parameter");
          return;
       }

       RecoveryThread rt = mapRecoveryThreads.get(nodeID);
       if(rt == null) {
           if(LOG.isWarnEnabled()) LOG.warn("nodeUp called on a node that has RecoveryThread removed already, node ID: " + nodeID);
           if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- EXIT node ID: " + nodeID);
           return;
       }
       rt.stopThread();
       boolean loopBack = false;
       do {
          try {
             loopBack = false;
             rt.join();
          } catch (InterruptedException e) { 
             LOG.warn("Problem while waiting for the recovery thread to stop for node ID: " + nodeID, e);
             loopBack = true; 
          }
       } while (loopBack);
       mapRecoveryThreads.remove(nodeID);
       if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- mapRecoveryThreads size: " + mapRecoveryThreads.size());
       if(LOG.isInfoEnabled()) LOG.info("nodeUp -- EXIT node ID: " + nodeID);
   }
   
   public int sdnTran() throws IOException {
     if(LOG.isTraceEnabled()) LOG.trace("sdnTrans -- ENTRY ");
     int count = getTransactionTypeCount(XdcTransType.XDC_TYPE_XDC_DOWN);
     if(LOG.isTraceEnabled()) LOG.trace("sdnTrans -- EXIT returning " + count);
     return count;
 }


   public short stall (int where) {
      if (LOG.isDebugEnabled()) LOG.debug("Entering stall with parameter " + where);
      if (where == 1001) {
         dynamic_dtm_logging = 1;
      }
      else if (where == 1000) {
         dynamic_dtm_logging = 0;
      }
      else {
         this.stallWhere = where;
      }
      return TransReturnCode.RET_OK.getShort();
   }

   public static Map<Long, TransactionState> getMap() {
     return mapTransactionStates;
   }

   /**
    * Called to get the count of transactions of a specific type
    *
    * @return int
    */
   public int getTransactionTypeCount(XdcTransType type){
     if (LOG.isTraceEnabled()) LOG.trace("Enter getTransactionTypeCount, type: " + type);

     int lv_count = 0;
     for(ConcurrentHashMap.Entry<Long, TransactionState> entry : mapTransactionStates.entrySet()){
        TransactionState ts = entry.getValue();
        if (ts.getXdcType().getValue() == type.getValue()){
           lv_count++;
        }
     }
     return lv_count;
   }

   public long beginTransaction(final long transactionId) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("Enter beginTransaction, txid: " + transactionId);
      TransactionState tx = null;
      try {
         tx = trxManager.beginTransaction(transactionId);
      } catch (IdTmException ite) {
          LOG.error("Begin Transaction Error caused by : ", ite);
          throw new IOException("Begin Transaction Error caused by :", ite);
      }  
      if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:beginTransaction new transactionState created: " + tx);

      synchronized(mapLock) {
         TransactionState tx2 = mapTransactionStates.get(transactionId);
         if (tx2 != null) {
            // Some other thread added the transaction while we were creating one.  It's already in the
            // map, so we can use the existing one.
            if (LOG.isDebugEnabled()) LOG.debug("beginTransaction, found TransactionState object while creating a new one " + tx2);
            tx = tx2;
         }
         else {
            if (LOG.isDebugEnabled()) LOG.debug("beginTransaction, adding new TransactionState to map " + tx);
            mapTransactionStates.put(transactionId, tx);
         }
	 tx.setXdcType(XdcTransType.XDC_TYPE_NONE);
      }
      
      if (LOG.isTraceEnabled()) LOG.trace("Txn Stat: Enter beginTransaction, tid: " + transactionId);
      tx.set_begin_time(System.currentTimeMillis());      
      
      if (LOG.isDebugEnabled()) LOG.debug("Exit beginTransaction, Transaction State: " + tx + " mapsize: " + mapTransactionStates.size());
      return transactionId;
   }

   public short abortTransactionCommon(final long transactionId) throws Exception {

       short lv_hbase_retcode = abortTransaction(transactionId);

       short lv_monarch_retcode = (useMonarch ? MabortTransaction(transactionId) : TransReturnCode.RET_NOTX.getShort());

       if (lv_monarch_retcode == TransReturnCode.RET_NOTX.getShort()) {
	   return lv_hbase_retcode;
       }

       return lv_monarch_retcode;
   }

   public short abortTransaction(final long transactionId) throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Enter abortTransaction, txId: " + transactionId);
      TransactionState ts = mapTransactionStates.get(transactionId);

      if(ts == null) {
          LOG.debug("Returning from abortTransaction, txId: " + transactionId + " retval: " + TransReturnCode.RET_NOTX.toString());
          return TransReturnCode.RET_NOTX.getShort();
      }
      
      if (LOG.isTraceEnabled()) LOG.trace("Txn Stat: Enter abortTransaction, tid: " + transactionId);
      ts.set_abortTransaction_time(System.currentTimeMillis());  

         ts.setStatus(TransState.STATE_ABORTED);
         if (useTlog) {
		    try {
               if (stallWhere == 16) {
                  FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in local TLOG.putSingleRecord abort write");
                  throw lv_fse;
               }
               if (LOG.isTraceEnabled()) LOG.trace("abortTransaction writing local "
                         + ts.getStatusString() + " state record for ts " + ts);
               tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), false /*  FORCED */);
            }
            catch (Exception e) {
               LOG.error("abortTransaction, local TLOG " + tLog
                     + " received Exception.  triggering local RECOVERY_ABORT record" , e);
               ts.setLocalWriteFailed(true);
               ts.setStatus(TransState.STATE_RECOVERY_ABORT);
            }

            if (stallWhere == 5 ) {
               if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2b (after local TLOG write, but before remote) for abortTransaction for transaction: " + transactionId);
               boolean loopBack = false;
               do
               {
                  try {
                     loopBack = false;
                     Thread.sleep(300000); // Initially set to run every 5 min
                  } catch (InterruptedException ie) {
                     loopBack = true;
                  }
               } while (loopBack);
            }

              if (bSynchronized && ts.hasRemotePeers()){
                for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                   int lv_peerId = entry.getKey();
                   if (lv_peerId == 0) // no peer for ourselves
                      continue;
                   TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                   if (lv_tLog == null){
                      LOG.error("Error during abortTransaction processing for tlog ABORT for peer: " + lv_peerId);
                      continue;
                   }
                   try {
                      if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                         if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing " + ts.getStatusString() + " state record");
                         if (synchronousWrites){
                             try {
                                if (stallWhere == 17) {
                                   FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.putSingleRecord abort write");
                                   throw lv_fse;
                                }
                                lv_tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), false /* FORCED */);
                             }
                             catch (Exception e) {
                                LOG.error("abortTransaction, remote TLOG " + lv_tLog
                                  + " received Exception.  triggering local RECOVERY_ABORT record" , e);
                                ts.setRemoteWriteFailed(true);
                                ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                                tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), false /* FORCED */);
                             }
                         }
                         else{
                            try {
                               if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite " + ts.getStatusString() + " for trans: " + ts);
                               if (stallWhere == 18) {
                                  FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.doTlogWrite abort write");
                                  throw lv_fse;
                               }
                               lv_tLog.doTlogWrite(ts, ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), false, -1);
                            }
                            catch (Exception e) {
                               LOG.error("abortTransaction, doTlogWrite remote TLOG " + lv_tLog
                                    + " received Exception.  triggering local RECOVERY_ABORT record for ts: "
                                    + ts + " " , e);
                               ts.setRemoteWriteFailed(true);
                               ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                               tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), false /* FORCED */);
                            }
                         }
                      }
                      else {
                         if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping "
                                    + ts.getStatusString() + " state record");
                      }
                   }
                   catch (Exception e) {
                      LOG.error("abortTransaction, lv_tLog " + lv_tLog + " EXCEPTION: " , e);
                      throw new RuntimeException(e);
                   }
                } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet())
             } // if (bSynchronized && ts.hasRemotePeers())

             if (! synchronousWrites){
               if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:abortTransaction, completing Tlog write of "
                          + ts.getStatusString() + " for transaction: " + transactionId);
               boolean loopBack = false;
               do
               {
                  try {
                     loopBack = false;
                     ts.completeRequest();
                  } catch (InterruptedException ie) {
                     loopBack = true;
                  }
               }
               while (loopBack);
             }
         }
      if ((stallWhere == 1) || (stallWhere == 3)) {
         if(LOG.isDebugEnabled()) LOG.debug("Stalling in phase 2 for abortTransaction");
         boolean loopBack = false;
         do {
            try {
               loopBack = false;
               Thread.sleep(300000); // Initially set to run every 5 min
            } catch(InterruptedException ie) {
               loopBack = true;
            }
         } while (loopBack);
      }

      try {
         trxManager.abort(ts);
      } 
      catch (UnsuccessfulDDLException ddle) {
          LOG.error("FATAL DDL Exception from HBaseTxClient:abort, WAITING INDEFINETLY !! retval: "
                    + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txid: " + transactionId, ddle);

          //Reaching here means several attempts to perform the DDL operation has failed in abort phase.
          //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
          //which causes a hang(abort work is outstanding forever) due to doAbortX thread holding the
          //commitSendLock (since doAbortX raised an exception and exited without clearing the commitSendLock count).
          //In the case of DDL exception, no doAbortX thread is involved and commitSendLock is not held. Hence to mimic
          //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
          //Long Term solution to this behaviour is currently TODO.
          Object commitDDLLock = new Object();
          synchronized(commitDDLLock)
          {
             boolean loopBack = false;
             do {
                try {
                    loopBack = false;
                    commitDDLLock.wait();
                } catch(InterruptedException ie) {
                    LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                    loopBack = true;
                }
             } while (loopBack);
          }
          return TransReturnCode.RET_EXCEPTION.getShort();
      }
      catch(IOException e) {
          synchronized(mapLock) {
             mapTransactionStates.remove(transactionId);
          }
          LOG.error("Returning from trxManager.abortTransaction, txid: " + transactionId + " retval: EXCEPTION", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
      }

      if (ts.getStatus().getValue() == TransState.STATE_ABORTED.getValue()){
         ts.setStatus(TransState.STATE_FORGOTTEN_ABORT);
      }
      else{
         // Transaction status should be TransState.STATE_RECOVERY_ABORT due to an exception
      }
      if (useTlog && useForgotten) {

          // Here we need to check to see if we encountered some sort of non-fatal exception during commit processing in the TransactionManager.
          // If we did encounter an exception we need to log a STATE_RECOVERY_ABORT record instead of a
          // STATE_FORGOTTEN_ABORT record in TLOG to ensure it never gets aged.  We do this for consistence with the COMMIT case and with
          // presumed abort this is less critical.
          if (! ts.getStatusString().contains("RECOVERY") && ts.getExceptionLogged()){
             ts.setStatus(TransState.STATE_RECOVERY_ABORT);
             if (LOG.isTraceEnabled()) LOG.trace("abortTransaction changing state to STATE_RECOVERY_ABORT due to an exception for ts: " + ts);
          }
          else{
             if (LOG.isTraceEnabled()) LOG.trace("abortTransaction leaving state as " + ts.getStatusString() + " for trans: " + ts.getTransactionId());
          }

          try{
             if (stallWhere == 19) {
                FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in local TLOG.putSingleRecord "
                      + ts.getStatusString() + " write");
                throw lv_fse;
             }
             if (LOG.isTraceEnabled()) LOG.trace("abortTransaction writing local " + ts.getStatusString()
                        + "  record in phase 2 for ts " + ts);
             tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(),
                     ts.hasRemotePeers(), forceForgotten); // forced flush?
          }
          catch (Exception e) {
             LOG.error("abortTransaction, local TLOG " + tLog + " received Exception writing "
                  + ts.getStatusString() + " record" , e);
             ts.setLocalWriteFailed(true);
             ts.setStatus(TransState.STATE_RECOVERY_ABORT);
          }

          if (bSynchronized && ts.hasRemotePeers()){
             for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                int lv_peerId = entry.getKey();
                if (lv_peerId == 0) // no peer for ourselves
                   continue;
                TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                if (lv_tLog == null){
                   LOG.error("Null Tlog during abortTransaction processing for tlog " + ts.getStatusString() + " for peer: " + lv_peerId);
                   continue;
                }
                try {
                   if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                      if (synchronousWrites){
                         try{
                            if (stallWhere == 20) {
                               FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.putSingleRecord "
                                    + ts.getStatusString() + " write");
                               throw lv_fse;
                            }
                            if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing "
                                  + ts.getStatusString() + " state record for ts " + ts);
                            lv_tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), true);
                         }
                         catch (Exception e) {
                             LOG.error("abortTransaction, remote TLOG " + lv_tLog + " received Exception writing "
                                        + ts.getStatusString() + " record" , e);
                             ts.setRemoteWriteFailed(true);
                             ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                             // Try to preserve the recovery abort in the local
                             tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(),
                                     ts.hasRemotePeers(), forceForgotten); // forced flush?
                         }
                      }
                      else{
                         try {
                            if (stallWhere == 21) {
                               FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.doTlogWrite "
                                    + ts.getStatusString() + " write");
                               throw lv_fse;
                            }
                            if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite " + ts.getStatusString() + " for trans: " + ts);
                            lv_tLog.doTlogWrite(ts, ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                         }
                         catch (Exception e) {
                             LOG.error("abortTransaction, remote TLOG " + lv_tLog + " received Exception writing "
                                        + ts.getStatusString() + " record" , e);
                             ts.setRemoteWriteFailed(true);
                             ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                             tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(),
                                     ts.hasRemotePeers(), forceForgotten); // forced flush?
                         }
                      }
                   }
                   else {
                      if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping " + ts.getStatusString() + " state record");
                   }
                }
                catch (Exception e) {
                   LOG.error("abortTransaction, lv_tLog writing abort forgotten " + lv_tLog + " EXCEPTION: ", e);
                }
             } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet())
          } // if (bSynchronized && ts.hasRemotePeers()){

//          if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
             try{
                if (LOG.isTraceEnabled()) LOG.trace("abortTransaction, completing Tlog write for " + ts.getStatusString() + " transaction: " + transactionId);
                ts.completeRequest();
             }
             catch(Exception e){
                LOG.error("Exception in abortTransaction completing Tlog write completeRequest for "
                       + ts.getStatusString() + " txId: " + transactionId + "Exception: ", e);
                //  Forgotten not written to remote side.  Return an error
                return TransReturnCode.RET_EXCEPTION.getShort();
             }
//          }
      }

      short retval = (ts.getLocalWriteFailed() || ts.getRemoteWriteFailed()) ? TransReturnCode.RET_EXCEPTION.getShort() :
                              TransReturnCode.RET_OK.getShort();
      //lastTransid = transactionId;
      if (LOG.isTraceEnabled()) LOG.trace("Exit abortTransaction, retval: " + retval + " transid: " + transactionId + " mapsize: " + mapTransactionStates.size());
      return retval;
   }

   public short MabortTransaction(final long transactionID) throws IOException 
    {
      if (LOG.isDebugEnabled()) LOG.debug("Enter MabortTransaction, txId: " + transactionID);
      MTransactionState ts = mapMTransactionStates.get(transactionID);

      if(ts == null) {
          LOG.error("Returning from MabortTransaction" 
		    + ", txId: " + transactionID 
		    + ", retval: " + TransReturnCode.RET_NOTX.toString()
		    );
          return TransReturnCode.RET_NOTX.getShort();
      }

      /* TBD try  */
	  {
         ts.setStatus(TransState.STATE_ABORTED);
	 /* TBD
         if (useTlog) {
            tLog.putSingleRecord(transactionID, ts.getStartId(), -1, ts.getTmFlags(), TransState.STATE_ABORTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), false); //force flush
            if (bSynchronized && ts.hasRemotePeers()){
               for (TmAuditTlog lv_tLog : peer_tLogs.values()) {
                  if (synchronousWrites){
                     lv_tLog.putSingleRecord(transactionID, ts.getStartId(), -1, ts.getTmFlags(), TransState.STATE_ABORTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), false); //force flush
                  }
                  else{
                     try {
                        if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite ABORTED for : " + ts.getTransactionId());
                        lv_tLog.doTlogWrite(ts, TransState.STATE_ABORTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true, -1);

                     }
                     catch (IOException e) {
                        LOG.error("Returning from doTlogWrite, txid: " + transactionID + 
                                   " tLog.doTlogWrite: EXCEPTION " , e);
                        return TransReturnCode.RET_EXCEPTION.getShort();
                     }
                  }
               }   
            }
            if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
                  if (LOG.isTraceEnabled()) LOG.trace("MabortTransaction, completing Tlog write for transaction: " + transactionID);
                  boolean loopBack = false;
                  do
                  {
                    try {
                      loopBack = false;
                      ts.completeRequest();
                    } catch (InterruptedException ie) {
                      loopBack = true;
                    }
                  }
                  while (loopBack);
            }
         }
	 */
      }
      /* TBD
      catch (CommitUnsuccessfulException cue) {
         LOG.error("Returning from MabortTransaction" 
		   + ", txid: " + transactionID 
		   + " tLog.putRecord: EXCEPTION ", cue
		   );
         return TransReturnCode.RET_EXCEPTION.getShort();
      } 
      catch(IOException e) {
         LOG.error("Returning from MabortTransaction" 
		   + ", txid: " + transactionID 
		   + " tLog.putRecord: EXCEPTION ", e
		   );
         return TransReturnCode.RET_EXCEPTION.getShort();
      } 
      */

      if ((stallWhere == 1) || (stallWhere == 3)) {
         if(LOG.isDebugEnabled()) LOG.debug("Stalling in phase 2 for MabortTransaction");
         boolean loopBack = false;
         do {
            try {
               loopBack = false;
               Thread.sleep(300000); // Initially set to run every 5 min
            } catch(InterruptedException ie) {
               loopBack = true;
            }
         } while (loopBack);
      }

      try {
         mtrxManager.abort(ts);
      } 
      /* TBD
      catch (UnsuccessfulDDLException ddle) {
          LOG.error("FATAL DDL Exception from abort, WAITING INDEFINETLY !!" 
		    + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() 
		    + ", UnsuccessfulDDLException" 
		    + ", txid: " + transactionID, ddle
		    );

          //Reaching here means several attempts to perform the DDL operation has failed in abort phase.
          //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
          //which causes a hang(abort work is outstanding forever) due to doAbortX thread holding the
          //commitSendLock (since doAbortX raised an exception and exited without clearing the commitSendLock count).
          //In the case of DDL exception, no doAbortX thread is involved and commitSendLock is not held. Hence to mimic
          //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
          //Long Term solution to this behaviour is currently TODO.
          Object commitDDLLock = new Object();
          synchronized(commitDDLLock)
          {
             boolean loopBack = false;
             do {
                try {
                    loopBack = false;
                    commitDDLLock.wait();
                } catch(InterruptedException ie) {
                    LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                    loopBack = true;
                }
             } while (loopBack);
          }
          return TransReturnCode.RET_EXCEPTION.getShort();
      } 
      */
      catch(IOException e) {
          synchronized(mapLock) {
             mapMTransactionStates.remove(transactionID);
          }
          LOG.error("Returning from MabortTransaction" 
		    + ", txid: " + transactionID 
		    + " retval: EXCEPTION", e
		    );
          return TransReturnCode.RET_EXCEPTION.getShort();
      }

      /* TBD
      if (useTlog && useForgotten) {
         tLog.putSingleRecord(transactionID, ts.getStartId(), -1, ts.getTmFlags(), TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?
         if (bSynchronized && ts.hasRemotePeers()){
            for (TmAuditTlog lv_tLog : peer_tLogs.values()) {
               if (synchronousWrites){
                  lv_tLog.putSingleRecord(transactionID, ts.getStartId(), -1, ts.getTmFlags(), TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?
               }
               else{
                  try {
                     if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite FORGOTTEN for : " + ts.getTransactionId());
                     lv_tLog.doTlogWrite(ts, TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                  }
                  catch (IOException e) {
                     LOG.error("Returning from doTlogWrite, txid: " + transactionID + 
                               " tLog.doTlogWrite: EXCEPTION ", e);
                     return TransReturnCode.RET_EXCEPTION.getShort();
                  }
               }
            }
         }

         if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
               if (LOG.isTraceEnabled()) LOG.trace("MabortTransaction, completing Tlog write for FORGOTTEN transaction: " + transactionID);
               boolean loopBack = false;
               do
               {
                 try {
                    loopBack = false;
                    ts.completeRequest();
                 } catch (InterruptedException ie) {
                    loopBack = true;
                 } catch (CommitUnsuccessfulException cue) {
                    LOG.error("MabortTransaction, completing Tlog write for FORGOTTEN transaction: " + transactionID, cue);
                    return TransReturnCode.RET_EXCEPTION.getShort();

                 }
               }
               while (loopBack);
         }
      }
      */

      if (LOG.isTraceEnabled()) LOG.trace("Exit MabortxTransaction, retval: OK" 
					  + ",  txId: " + transactionID 
					  + ", mapsize: " + mapMTransactionStates.size()
					  );
      return TransReturnCode.RET_OK.getShort();
   }
    
   public short prepareCommitCommon(long transactionId, byte[] querycontext) throws 
                                                    TransactionManagerException,
                                                    IOException {
       short lv_hbase_retcode = prepareCommit(transactionId, querycontext);
       short lv_monarch_retcode = TransReturnCode.RET_OK.getShort();

       if ((useMonarch) && 
	   ((lv_hbase_retcode == TransReturnCode.RET_OK.getShort()) ||
	    (lv_hbase_retcode == TransReturnCode.RET_NOTX.getShort()) ||
	    (lv_hbase_retcode == TransReturnCode.RET_READONLY.getShort())
	    )
	   ) { 
	   
	   lv_monarch_retcode = MprepareCommit(transactionId);
       }
       else {
	   return lv_hbase_retcode;
       }

       if (lv_monarch_retcode == TransReturnCode.RET_NOTX.getShort()) {
	   return lv_hbase_retcode;
       }

       return lv_monarch_retcode;
   }

    public short prepareCommit(long transactionId, byte [] querycontext) throws
                                               TransactionManagerException,
                                               IOException 
    {

     short result = 0;
     int peerId = -1;
     
     String querycontext_s = new String(querycontext);
     if (LOG.isDebugEnabled()) LOG.debug("Enter prepareCommit"
                  + ", txId: " + transactionId
                  + ", #txstate entries " + mapTransactionStates.size()
                  + " query context " + querycontext_s);
     TransactionState ts = mapTransactionStates.get(transactionId);
     
     if (ts == null) {
       LOG.error("Returning from prepareCommit" 
		 + ", txId: " + transactionId 
		 + ", retval: " + TransReturnCode.RET_NOTX.toString()
		 );
       return TransReturnCode.RET_NOTX.getShort();
     }

     //for binlog
     //if it is max protection mode ?
     if(pATRConfig.isMaxProtectionSyncMode() && pATRConfig.isATRXDCEnabled()) {
       //get idtm for commitID
       //LOG.info("HBaseBinlog: set commit ID");
       IdTmId commitId = null;
       long commitIdVal = -1;
            try {
               commitId = new IdTmId();
               idServer.id(ID_TM_SERVER_TIMEOUT, commitId);
            } catch (IdTmException exc) {
               LOG.error("doCommit: IdTm threw exception " ,  exc);
            }
            commitIdVal = commitId.val;
       ts.setCommitId(commitIdVal);
     }

     if (LOG.isTraceEnabled()) LOG.trace("Txn Stat Enter prepareCommit, tid: " + transactionId);
     ts.set_prepareCommit_time(System.currentTimeMillis());  

     if(ts.hasDDLTx()){
        if (LOG.isInfoEnabled()) LOG.info("Enter prepareCommit [" + ts.getTransactionId()
                   + "] has DDL "
                   + ", num participants: " + ts.getParticipantCount()
                   + ", num txstate entries " + mapTransactionStates.size());
     }
     else {
        if (LOG.isDebugEnabled()) LOG.debug("Enter prepareCommit"
					 + ", txId: " + transactionId
	                 + ", num participants: " + ts.getParticipantCount()
					 + ", num txstate entries " + mapTransactionStates.size());
     }

     try {
        if (skipConflictCheck) {
           ts.setSkipConflictCheck(true);
        }

//        if (pSTRConfig.getConfiguredPeerCount() > 0) // optimize efficiency later
//                   peerId = pSTRConfig.getFirstRemotePeerId(); // NARI XDC POC, access memory XDC cache, not ZK

//        if ((ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP) &&
//              (peerId > 0) && (pSTRConfig.getPeerStatus(peerId).contains(PeerInfo.STR_DOWN))) {
//                   LOG.info("Transaction " + transactionId + " get unilaterally aborted due to personality SUP, but courrent XDC state SDN. ");
//                   result = TransactionalReturn.COMMIT_UNSUCCESSFUL;
//        }
//        else {
//           result = (short) trxManager.prepareCommit(ts);
//        }

        ts.setQueryContext(querycontext_s);

        result = (short) trxManager.prepareCommit(ts);

        if (LOG.isDebugEnabled()) LOG.debug("prepareCommit, [ " + ts + " ], result " + result + ((result == TransactionalReturn.COMMIT_OK_READ_ONLY)?", Read-Only":""));
        switch (result) {
          case TransactionalReturn.COMMIT_OK:
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK prepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_OK.getShort();
          case TransactionalReturn.COMMIT_OK_READ_ONLY:
             synchronized(mapLock) {
                mapTransactionStates.remove(transactionId);
             }
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK_READ_ONLY prepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_READONLY.getShort();
          case TransactionalReturn.COMMIT_UNSUCCESSFUL:
             if (LOG.isInfoEnabled()) LOG.info("prepareCommit COMMIT_UNSUCCESSFUL for ts: " + ts);
             if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
               throw new TransactionManagerException(ts.getRecordedException(),
                                     TransReturnCode.RET_EXCEPTION.getShort());
             else
               throw new TransactionManagerException("Encountered COMMIT_UNSUCCESSFUL Exception, txid:" + transactionId,
                                     TransReturnCode.RET_EXCEPTION.getShort());
          case TransactionalReturn.COMMIT_CONFLICT:
             if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
               throw new TransactionManagerException(ts.getRecordedException(),
                                   TransReturnCode.RET_HASCONFLICT.getShort());
             else
               throw new TransactionManagerException("Encountered COMMIT_CONFLICT Exception, txid:" + transactionId,
                                   TransReturnCode.RET_HASCONFLICT.getShort());
          case TransactionalReturn.COMMIT_DOOMED:
             if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
               throw new TransactionManagerException(ts.getRecordedException(),
                                   TransReturnCode.RET_HASDOOMED.getShort());
             else
               throw new TransactionManagerException("Encountered COMMIT_DOOMED Exception, txid:" + transactionId,
                                   TransReturnCode.RET_HASDOOMED.getShort());
          case TransactionalReturn.COMMIT_SHIELDED:
              if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
                throw new TransactionManagerException(ts.getRecordedException(),
                                    TransReturnCode.RET_SHIELDED.getShort());
              else
                throw new TransactionManagerException("Encountered COMMIT_SHIELDED Exception, txid:" + transactionId,
                                    TransReturnCode.RET_SHIELDED.getShort());
          case TransactionalReturn.EPOCH_VIOLATION:
              if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
                throw new TransactionManagerException(ts.getRecordedException(),
                                    TransReturnCode.RET_EXCEPTION.getShort());
              else
                throw new TransactionManagerException("Encountered EPOCH_VIOLATION Exception, txid:" + transactionId,
                                    TransReturnCode.RET_EXCEPTION.getShort());
          default:
             if((ts.getRecordedException() != null) && !ts.getRecordedException().isEmpty())
               throw new TransactionManagerException(ts.getRecordedException(),
                                   TransReturnCode.RET_EXCEPTION.getShort());
             else
               throw new TransactionManagerException("Encountered COMMIT_UNSUCCESSFUL Exception, txid:" + transactionId,
                                   TransReturnCode.RET_EXCEPTION.getShort());
        }
     }catch (TransactionManagerException t) {
       if (t.getErrorCode() != TransReturnCode.RET_SHIELDED.getShort()){
          LOG.error("Returning from HBaseTxClient:prepareCommit, txid: " + transactionId + " retval: " + t.getErrorCode(), t);
       }
       throw t;
     }
     catch (CommitUnsuccessfulException e) {
       LOG.error("Returning from HBaseTxclient:prepareCommit, txId: " + transactionId + " retval: " + TransReturnCode.RET_NOCOMMITEX.toString() + " CommitUnsuccessfulException", e);
       throw new TransactionManagerException(e,
                                   TransReturnCode.RET_NOCOMMITEX.getShort());
     }
     catch (IOException e) {
       LOG.error("Returning from HBaseTxClient:prepareCommit, txId: " + transactionId + " retval: " + TransReturnCode.RET_IOEXCEPTION.toString() + " IOException", e);
       throw new TransactionManagerException(e,
                                   TransReturnCode.RET_IOEXCEPTION.getShort());
     }
   }

   public short MprepareCommit(long transactionId) throws IOException {
       try {
     if (LOG.isTraceEnabled()) LOG.trace("Enter MprepareCommit" 
					 + ", txId: " + transactionId
					 + ", #txstate entries:" + mapMTransactionStates.size()
					 );
     MTransactionState ts = mapMTransactionStates.get(transactionId);

     if(ts == null) {
	 if (LOG.isTraceEnabled()) LOG.trace("Returning from MprepareCommit" 
					     + ", txId: " + transactionId 
					     + ", retval: " + TransReturnCode.RET_NOTX.toString()
					     );
	 return TransReturnCode.RET_NOTX.getShort();
     }

     try {
        short result = (short) mtrxManager.prepareCommit(ts);
        if (LOG.isDebugEnabled()) LOG.debug("MprepareCommit" 
					    + ", txId: " + ts 
					    + " , result: " + result 
					    + ((result == TransactionalReturn.COMMIT_OK_READ_ONLY)?", Read-Only":"")
					    );
        switch (result) {
          case TransactionalReturn.COMMIT_OK:
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_OK.getShort();
          case TransactionalReturn.COMMIT_OK_READ_ONLY:
             synchronized(mapLock) {
                mapMTransactionStates.remove(transactionId);
             }
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK_READ_ONLY MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_READONLY.getShort();
          case TransactionalReturn.COMMIT_UNSUCCESSFUL:
             if(LOG.isDebugEnabled()) LOG.debug("Exit RET_EXCEPTION MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_EXCEPTION.getShort();
          case TransactionalReturn.COMMIT_CONFLICT:
             if(LOG.isDebugEnabled()) LOG.debug("Exit RET_HASCONFLICT MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_HASCONFLICT.getShort();
          case TransactionalReturn.COMMIT_DOOMED:
             if(LOG.isDebugEnabled()) LOG.debug("Exit RET_HASDOOMED MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_HASDOOMED.getShort();
          default:
             if(LOG.isDebugEnabled()) LOG.debug("Exit default RET_EXCEPTION MprepareCommit, txId: " + transactionId);
             return TransReturnCode.RET_EXCEPTION.getShort();
        }
     } catch (CommitUnsuccessfulException e) {
       LOG.error("Returning from MprepareCommit" 
		 + ", txId: " + transactionId 
		 + ", retval: " + TransReturnCode.RET_NOCOMMITEX.toString() 
		 + ", Exception:  CommitUnsuccessfulException"
		 );
       return TransReturnCode.RET_NOCOMMITEX.getShort();
     }
     catch (IOException e) {
       LOG.error("Returning from MprepareCommit" 
		 + ", txId: " + transactionId 
		 + ", retval: " + TransReturnCode.RET_IOEXCEPTION.toString() 
		 + ", Exception: IOException"
		 );
       return TransReturnCode.RET_IOEXCEPTION.getShort();
     }
       }
       catch (Throwable e) {
	   LOG.error("Exception", e);
           return TransReturnCode.RET_NOCOMMITEX.getShort();
       }
       
   }

   public short doCommitCommon(long transactionId) throws IOException {
       if (LOG.isDebugEnabled()) LOG.trace("Enter doCommitCommon, useMonarch: " + useMonarch + " txId: " + transactionId);
       short lv_hbase_retcode = doCommit(transactionId);
       if (LOG.isDebugEnabled()) LOG.trace("doCommitCommon received return code "
             + lv_hbase_retcode + " for doCommit for txId: " + transactionId);
       short lv_monarch_retcode = TransReturnCode.RET_OK.getShort();
       //lastTransid = transactionId;

       if ((useMonarch) &&
	   ((lv_hbase_retcode == TransReturnCode.RET_OK.getShort()) ||
	    (lv_hbase_retcode == TransReturnCode.RET_NOTX.getShort()) ||
	    (lv_hbase_retcode == TransReturnCode.RET_READONLY.getShort()))
	   ) { 
	   lv_monarch_retcode = MdoCommit(transactionId);
       }
       else {
          if (LOG.isDebugEnabled()) LOG.trace("doCommitCommon returning lv_hbase_retcode "
                   + lv_hbase_retcode + " in doCommitCommon for txId: " + transactionId);
          return lv_hbase_retcode;
       }

       if (lv_monarch_retcode == TransReturnCode.RET_NOTX.getShort()) {
           if (LOG.isDebugEnabled()) LOG.trace("doCommitCommon no Monarch transaction; returning lv_hbase_retcode "
                   + lv_hbase_retcode + " in doCommitCommon for txId: " + transactionId);
           return lv_hbase_retcode;
       }

       if (LOG.isDebugEnabled()) LOG.trace("doCommitCommon returning lv_hbase_retcode "
               + lv_monarch_retcode + " in doCommitCommon for txId: " + transactionId);
       return lv_monarch_retcode;
   }

   public short doCommit(long transactionId) throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Enter doCommit, txId: " + transactionId);
      TransactionState ts = mapTransactionStates.get(transactionId);

      //compute the totalNum and update into TransactionState and then write into TLog using tableAttr
      short totalNum = trxManager.computeTotalNum(ts);
      ts.tableAttrSetAndRegister("_TOTALNUM_", totalNum);

      if(ts == null) {
         if (! useMonarch) {
            LOG.error("Returning from doCommit, (null tx) retval: "
                 + TransReturnCode.RET_NOTX.toString()
                 + ", txId: " + transactionId);
         }
         return TransReturnCode.RET_NOTX.getShort();
      }

      // Check to see if we returned an error and the client is trying to redrive commit
      if (ts.getStatus().toString().contains("ABORT")){
         LOG.error("Attemping redrive commit for aborted tx: " + ts);
         try {
            // Try to preserve the recovery abort ts record
            ts.setStatus(TransState.STATE_RECOVERY_ABORT);
            tLog.putSingleRecord(transactionId, ts.getStartId(), -1, ts.getTmFlags(), ts.getStatusString(), ts.getTableAttr(), ts.hasRemotePeers(), true);
            trxManager.abort(ts);
            return TransReturnCode.RET_WALDOWN.getShort();
         }
         catch (UnsuccessfulDDLException ddle) {
            LOG.error("FATAL DDL Exception from HBaseTxClient: retry aborted commit, WAITING INDEFINETLY !! retval: "
                        + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txid: " + transactionId, ddle);

            //Reaching here means several attempts to perform the DDL operation has failed in abort phase.
            //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
            //which causes a hang(abort work is outstanding forever) due to doAbortX thread holding the
            //commitSendLock (since doAbortX raised an exception and exited without clearing the commitSendLock count).
            //In the case of DDL exception, no doAbortX thread is involved and commitSendLock is not held. Hence to mimic
            //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
            //Long Term solution to this behaviour is currently TODO.
            LOG.error("DDL encountered in commit for aborted tx: " + ts + " Requires manual kill of TM ");
            Object commitDDLLock = new Object();
            synchronized(commitDDLLock)
            {
               boolean loopBack = false;
               do {
                  try {
                     loopBack = false;
                     commitDDLLock.wait();
                  } catch(InterruptedException ie) {
                     LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                     loopBack = true;
                  }
               } while (loopBack);
            }
            return TransReturnCode.RET_EXCEPTION.getShort();
         }
         catch(IOException e) {
            synchronized(mapLock) {
               mapTransactionStates.remove(transactionId);
            }
            LOG.error("Returning from retry of trxManager.abortTransaction in doCommit, txid: " + transactionId + " retval: EXCEPTION", e);
            return TransReturnCode.RET_WALDOWN.getShort();
         }
      }
      
      if (LOG.isTraceEnabled()) LOG.trace("Txn Stat Enter doCommit, tid: " + transactionId);
      ts.set_doCommit_time(System.currentTimeMillis());        

       // Set the commitId
       IdTmId commitId = null;
       long commitIdVal = -1;
       if (recoveryToPitMode || ts.getGenerateMutations() ||
          (TRANSACTION_ALGORITHM == AlgorithmType.SSCC) ||
          (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN)) {
          try {
             commitId = new IdTmId();
             if (LOG.isTraceEnabled()) LOG.trace("doCommit getting new commitId");
             idServer.id(ID_TM_SERVER_TIMEOUT, commitId);
             if (LOG.isTraceEnabled()) LOG.trace("doCommit idServer.id returned: " + commitId.val);
          } catch (IdTmException exc) {
             LOG.error("doCommit: IdTm threw exception " ,  exc);
             throw new CommitUnsuccessfulException("doCommit: IdTm threw exception " ,  exc);
          }
          commitIdVal = commitId.val;
       }
       if (LOG.isTraceEnabled()) LOG.trace("doCommit setting commitId (" + commitIdVal + ") for tx: " + ts.getTransactionId());
       ts.setCommitId(commitIdVal);

       if (stallWhere == 4) {
    	  if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2a (before TLOG write) for doCommit for transaction: " + transactionId);
          boolean loopBack = false;
          do
          {
             try {
                loopBack = false;
                Thread.sleep(300000); // Initially set to run every 5 min
             } catch (InterruptedException ie) {
                loopBack = true;
             }
          } while (loopBack);
       }

       //try {
          ts.setStatus(TransState.STATE_COMMITTING);
          String writeValue = TransState.STATE_COMMITTED.toString();
          if (useTlog) {
             // We must write the local TLOG entries first because we can't tell without the state record whether this TransactionState
             // has remotePeers or not.  If we write to the local for xDC and local only transactions recovery will be consistent in
             // looking for the TS in the local TLOG for resolution
             boolean writeTlogError = false;
             if (synchronousWrites){
                try {
                   if (stallWhere == 9) {
                      FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in local TLOG.putSingleRecord commit write");
                      throw lv_fse;
                   }
                   if (LOG.isTraceEnabled()) LOG.trace("doCommit writing local COMMIT state record for ts " + ts);
                   tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true);
                }
                catch (Exception e) {
                   writeTlogError = true;
                   LOG.error("doCommit, local TLOG " + tLog + " received Exception writing STATE_COMMITTED record" , e);
                }
             }
             else {
                try {
                   if (stallWhere == 10) {
                      FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in local TLOG.doTlogWrite for commit ");
                      throw lv_fse;
                   }
                   if (LOG.isTraceEnabled()) LOG.trace("calling local doTlogWrite COMMITTED for ts: " + ts);
                   tLog.doTlogWrite(ts, writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                }
                catch (Exception e) {
                   LOG.error("doCommit, doTlogWrite local TLOG " + tLog
                          + " received Exception writing STATE_COMMITTED record" , e);
                   writeTlogError = true;
                }
             }

             int readCount = 0;
             boolean returnException = false;
             //check that putTlog is put successfully or not, otherwise recover thread may make wrong decisions.
             while (writeTlogError && readCount++ < 3) {
                 boolean hasException = false;
                 returnException = true;
                 TransactionState ts1 = new TransactionState(transactionId);
                 ts1.setStatus(TransState.STATE_BAD);
                 try {
                     tLog.getTransactionState(ts1, false);
                 } catch (Exception ex) {
                     LOG.error("doCommit, reading tlog failed and transaction id is " + transactionId, ex);
                     hasException = true;
                 }
                 if (hasException || ts1.getStatus() == TransState.STATE_BAD) {
                     try {
                         Thread.sleep(1000);
                     } catch (Exception interupt) {
                     }
                     continue;
                 }
                 if (ts1.getStatus() == TransState.STATE_NOTX) {
                     LOG.warn("doCommit, the tlog value just written is " + ts1.getStatusString() + " try to recommit transaction " + transactionId);
                     return TransReturnCode.RET_EXCEPTION.getShort();
                 }
                 LOG.info("doCommit, the tlog value just written is " + ts1.getStatusString() + " and transaction id is " + transactionId);
                 if (ts1.getStatus() != TransState.STATE_COMMITTED
                     && ts1.getStatus() != TransState.STATE_RECOVERY_COMMITTED) {
                     ts.setLocalWriteFailed(true);
                     ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                     writeValue = TransState.STATE_RECOVERY_ABORT.toString();
                 }
                 returnException = false;
                 break;
             }
             if (returnException) {
                 LOG.error("doCommit, local TLOG checking failed and transaction id is " + transactionId);
                 return TransReturnCode.RET_EXCEPTION.getShort();
             }

             if (stallWhere == 5) {
                 if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2b (after local TLOG write, but before remote) for doCommit for transaction: " + transactionId);
                 boolean loopBack = false;
                 do
                 {
                    try {
                       loopBack = false;
                       Thread.sleep(300000); // Initially set to run every 5 min
                    } catch (InterruptedException ie) {
                       loopBack = true;
                    }
                 } while (loopBack);
              }

              if (bSynchronized && ts.hasRemotePeers()){
                for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                   int lv_peerId = entry.getKey();
                   if (lv_peerId == 0) // no peer for ourselves
                      continue;
                   TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                   if (lv_tLog == null){
                      LOG.error("Error during doCommit processing for tlog COMMIT for peer: " + lv_peerId
                                + " Unable to get TLOG to write commit record for trans: " + ts );
                      continue;
                   }
                   try {
                      if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)){
                         if (LOG.isTraceEnabled()) LOG.trace("doCommit - PEER " + lv_peerId + " STATUS is UP; writing "
                                           + writeValue + " state record");
                         if (synchronousWrites){
                             try {
                                if (stallWhere == 11) {
                                    FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.putSingleRecord commit write");
                                    throw lv_fse;
                                }
                                if (LOG.isTraceEnabled()) LOG.trace("doCommit calling remote putSingleRecord " + writeValue + " for ts: " + ts);
                                lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true);
                             }
                             catch (Exception e) {
                                LOG.error("doCommit, remote TLOG " + lv_tLog
                                     + " received Exception writing " + writeValue + " record for transaction: " + transactionId + " " , e);
                                ts.setRemoteWriteFailed(true);
                                ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                                writeValue = TransState.STATE_RECOVERY_ABORT.toString();
                                LOG.error("doCommit, writing local TLOG " + writeValue + " record for transaction: " + ts);
                                tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true);
                             }
                         }
                         else{
                            try {
                               if (stallWhere == 12) {
                                  FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.doTlogWrite commit write");
                                  throw lv_fse;
                               }
                               if (LOG.isTraceEnabled()) LOG.trace("doCommit calling doTlogWrite " + writeValue
                                         + " for trans: " + ts);
                               lv_tLog.doTlogWrite(ts, writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                            }
                            catch (Exception e) {
                               LOG.error("doCommit, doTlogWrite remote TLOG " + lv_tLog
                                   + " received Exception writing " + writeValue + " record" , e);
                               ts.setRemoteWriteFailed(true);
                               ts.setStatus(TransState.STATE_RECOVERY_ABORT);
                               writeValue = TransState.STATE_RECOVERY_ABORT.toString();
                               tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true);
                            }
                         }
                      }
                      else {
                         if (LOG.isWarnEnabled()) LOG.warn("doCommit - PEER " + lv_peerId + " STATUS is DOWN; skipping "
                              + writeValue + " state record for ts: " + ts);
                      }
                   }
                   catch (IOException e) {
                      LOG.error("doCommit, lv_tLog " + lv_tLog + " EXCEPTION: " , e);
                      throw new RuntimeException(e);
                   }
                } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) 
             } // if (bSynchronized && ts.hasRemotePeers())

             if (! synchronousWrites){
               if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:doCommit, completing Tlog write of " + writeValue + " record for transaction: " + ts);
               boolean loopBack = false;
               do
               {
                  try {
                     loopBack = false;
                     ts.completeRequest();
                  } catch (InterruptedException ie) {
                     loopBack = true;
                  }
               }
               while (loopBack);
             }
          }
    /*
       } catch(IOException e) {
          LOG.error("Returning from HBaseTxClient:doCommit, txid: " + transactionId + " tLog.putRecord: EXCEPTION ", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       */

       if ((stallWhere == 2) || (stallWhere == 3)) {
          if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2 for doCommit for transaction: " + transactionId);
          boolean loopBack = false;
          do {
             try {
                loopBack = false;
                Thread.sleep(300000); // Initially set to run every 5 min
             } catch(InterruptedException ie) {
                 loopBack = true;
             }
          } while (loopBack);
       }

       if (ts.getStatus().getValue() == TransState.STATE_COMMITTING.getValue())
          ts.setStatus(TransState.STATE_COMMITTED);

       if (ts.getStatus().getValue() == TransState.STATE_COMMITTED.getValue()){
          // No error in writing final state records to TLOG; continue to commit
          try {
             int stallFlags = 0;
             if (stallWhere > 5) {
                stallFlags = stallWhere;
             }
             if (LOG.isTraceEnabled()) LOG.trace("doCommit, calling trxManager.doCommit(" + ts.getTransactionId() + ")" );
             trxManager.doCommit(ts, false /* IgnoreUnknownTransaction */, stallFlags);
//               trxManager.doCommit(ts, true /* IgnoreUnknownTransaction */, stallFlags);
          } catch (CommitUnsuccessfulException e) {
             LOG.error("Returning from doCommit, transaction: " + transactionId
                  + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException");
             return TransReturnCode.RET_EXCEPTION.getShort();
          }
          catch (UnsuccessfulDDLException ddle) {
             LOG.error("FATAL DDL Exception from doCommit, WAITING INDEFINETLY !! retval: " + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txId: " + transactionId);

             //Reaching here means several attempts to perform the DDL operation has failed in commit phase.
             //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
             //which causes a hang(commit work is outstanding forever) due to doCommitX thread holding the
             //commitSendLock (since doCommitX raised an exception and exited without clearing the commitSendLock count).
             //In the case of DDL exception, no doCommitX thread is involved and commitSendLock is not held. Hence to mimic
             //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
             //Long Term solution to this behaviour is currently TODO.
             LOG.error("DDL encountered in commit for aborted tx: " + ts + " Requires manual kill of TM ");
             Object commitDDLLock = new Object();
             synchronized(commitDDLLock)
             {
                boolean loopBack = false;
                do {
                   try {
                      loopBack = false;
                      commitDDLLock.wait();
                   } catch(InterruptedException ie) {
                       LOG.warn("Interrupting commitDDLLock.wait"
			                + ", txId: " + transactionId
			                + ", retrying ", ie);
                       loopBack = true;
                   }
                } while (loopBack);
             }
             return TransReturnCode.RET_EXCEPTION.getShort();
          }
       }
       else{
          try {
             LOG.error("Failures in writing TLOG state records in commit processing force HBaseTxClient to trigger abort tx: " + ts);
             trxManager.abort(ts);
          }
          catch (UnsuccessfulDDLException ddle) {
             LOG.error("FATAL DDL Exception from HBaseTxClient:aborted commit, WAITING INDEFINETLY !! retval: "
                        + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txid: " + transactionId, ddle);

             //Reaching here means several attempts to perform the DDL operation has failed in abort phase.
             //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
             //which causes a hang(abort work is outstanding forever) due to doAbortX thread holding the
             //commitSendLock (since doAbortX raised an exception and exited without clearing the commitSendLock count).
             //In the case of DDL exception, no doAbortX thread is involved and commitSendLock is not held. Hence to mimic
             //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
             //Long Term solution to this behaviour is currently TODO.
             Object commitDDLLock = new Object();
             synchronized(commitDDLLock)
             {
                boolean loopBack = false;
                do {
                   try {
                       loopBack = false;
                       commitDDLLock.wait();
                   } catch(InterruptedException ie) {
                       LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                       loopBack = true;
                   }
                } while (loopBack);
             }
             return TransReturnCode.RET_EXCEPTION.getShort();
         }
         catch(IOException e) {
             synchronized(mapLock) {
                mapTransactionStates.remove(transactionId);
             }
             LOG.error("Returning from trxManager.abortTransaction in doCommit, txid: " + transactionId + " retval: EXCEPTION", e);
             return TransReturnCode.RET_WALDOWN.getShort();
         }

       }

       if (useTlog && useForgotten) {
          TransState tmpState = TransState.STATE_NOTX;

          // Here we need to check to see if we encountered some sort of non-fatal exception during commit processing in the TransactionManager.
          // If we did encounter an exception we need to log a STATE_RECOVERY_COMMITTED or STATE_RECOVERY_ABORT record instead of a
          // STATE_FORGOTTEN_COMMITTED record in TLOG to ensure it never gets aged.  This is particularly important in xDC environments,
          // where a TLOG write was successful, but perhaps an unknownTransactionException was logged and we might not know all the
          // current regions for broadcast.
          if (! ts.getStatusString().contains("RECOVERY") && ts.getStatusString().contains("COMMIT")){
             if (LOG.isTraceEnabled()) LOG.trace("doCommit setting state to STATE_FORGOTTEN_COMMITED for trans: " + ts.getTransactionId());
             tmpState = TransState.STATE_FORGOTTEN_COMMITTED;
             writeValue = TransState.STATE_FORGOTTEN_COMMITTED.toString();
          }
          else if (ts.getExceptionLogged() && ts.getStatusString().contains("COMMIT")){
             tmpState = TransState.STATE_RECOVERY_COMMITTED;
             writeValue = TransState.STATE_RECOVERY_COMMITTED.toString();
             if (LOG.isTraceEnabled()) LOG.trace("doCommit changing state to STATE_RECOVERY_COMMITTED due to an exception for ts: " + ts);
          }
          else{
             if (LOG.isTraceEnabled()) LOG.trace("doCommit leaving state as " + ts.getStatusString() + " for trans: " + ts.getTransactionId());
          }
          try{
             if (stallWhere == 13) {
                FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in local TLOG.putSingleRecord commit forgotten write");
                throw lv_fse;
             }
             ts.processWalSync(true);
             if (LOG.isTraceEnabled()) LOG.trace("writing to local TLOG " + writeValue + " for trans: " + ts);
             tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue,
                           ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?
             if (tmpState != TransState.STATE_NOTX)
                ts.setStatus(tmpState);
          }
          catch (Exception e) {
             LOG.error("doCommit, putSingleRecord local TLOG " + tLog
                 + " received Exception writing " + writeValue + " record for ts " + ts + " " , e);
             ts.setLocalWriteFailed(true);
             if (ts.getStatusString().contains("COMMIT")){
                ts.setStatus(TransState.STATE_RECOVERY_COMMITTED);
                writeValue = TransState.STATE_RECOVERY_COMMITTED.toString();
             }
          }

          if (bSynchronized && ts.hasRemotePeers()){
             for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                int lv_peerId = entry.getKey();
                if (lv_peerId == 0) // no peer for ourselves
                   continue;
                TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                if (lv_tLog == null){
                   LOG.error("Error during doCommit processing for tlog FORGOTTEN for peer: " + lv_peerId
                             + " Unable to get TLOG to write FORGOTTEN record for trans: " + ts );
                   continue;
                }
                try {
                   if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                      if (synchronousWrites){
                         try{
                            if (stallWhere == 14) {
                               FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.putSingleRecord commit forgotten write");
                               throw lv_fse;
                            }
                            ts.processWalSync(true);
                            if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing " + writeValue + " state record for ts: " + ts);
                            lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true);
                         }
                         catch (Exception e) {
                            LOG.error("doCommit, putSingleRecord remote TLOG " + lv_tLog
                                + " received Exception writing " + writeValue + " record for ts " + ts + " " , e);
                            ts.setRemoteWriteFailed(true);
                            if (! ts.getStatusString().contains("RECOVERY") && ts.getStatusString().contains("COMMIT")){
                               ts.setStatus(TransState.STATE_RECOVERY_COMMITTED);
                               writeValue = TransState.STATE_RECOVERY_COMMITTED.toString();
                            }
                            LOG.error("doCommit, putSingleRecord to local TLOG writing " + writeValue + " record for ts " + ts);
                            tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue,
                                       ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?
                         }
                      }
                      else{
                         try {
                            if (stallWhere == 15) {
                               FailedServerException lv_fse = new FailedServerException("Throwing FailedServerException in remote TLOG.doTlogWrite commit forgotten write");
                               throw lv_fse;
                            }
                            ts.processWalSync(true);
                            if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite " + writeValue + " for trans: " + ts);
                            lv_tLog.doTlogWrite(ts, writeValue, ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                         }
                         catch (Exception e) {
                            LOG.error("doCommit, doTlogWrite remote TLOG " + lv_tLog
                                 + " received Exception writing " + writeValue + " record for ts " + ts , e);
                            ts.setRemoteWriteFailed(true);
                            if (! ts.getStatusString().contains("RECOVERY") && ts.getStatusString().contains("COMMIT")){
                               ts.setStatus(TransState.STATE_RECOVERY_COMMITTED);
                               writeValue = TransState.STATE_RECOVERY_COMMITTED.toString();
                            }
                            LOG.error("doCommit, putSingleRecord to local TLOG writing " + writeValue + " record for ts " + ts);
                            tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), writeValue,
                                    ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?
                         }
                      }
                   }
                   else {
                      if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping "
                                      + writeValue + " state record for ts: " + ts);
                   }
                }
                catch (Exception e) {
                   LOG.error("doCommit, lv_tLog writing " + writeValue + " for ts: " + ts + " to " + lv_tLog + " EXCEPTION: ", e);
                }
             } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) 
          } // if (bSynchronized && ts.hasRemotePeers()){
        	  
          if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
             try{
                if (LOG.isTraceEnabled()) LOG.trace("doCommit, completing Tlog write for "
                      + writeValue + " transaction: " + transactionId);
                ts.completeRequest();
             }
             catch(Exception e){
                LOG.error("Exception in doCommit completing Tlog write completeRequest for "
                         + writeValue + " txId: " + transactionId + " Exception: ", e);
                //  Forgotten not written to remote side.  Return an error
                return TransReturnCode.RET_EXCEPTION.getShort();
             }
          }
       }
       if(ts.getStatusString().contains("COMMIT")){
          if (LOG.isTraceEnabled()) LOG.trace("Exit doCommit, retval(ok): " + TransReturnCode.RET_OK.toString() +
                         " txId: " + transactionId + " mapsize: " + mapTransactionStates.size());
          if (ATRConfig.instance().isStrictReliabilitySyncMode() == true) {
             try {
                trxManager.doBinlogCheck(ts);
             } catch(Exception ex) {
                LOG.warn("doBinlogCheck with tid " + transactionId + " throws Exception ", ex);
             }
          }
          return TransReturnCode.RET_OK.getShort();
       }
       else{
          if (LOG.isTraceEnabled()) LOG.trace("Exit doCommit, state is not committed; returning RET_WALDOWN: " + TransReturnCode.RET_WALDOWN.toString() +
                " for tx: " + ts + " mapsize: " + mapTransactionStates.size());

          return TransReturnCode.RET_WALDOWN.getShort();

       }
   }

   public short MdoCommit(long transactionId) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Enter MdoCommit" 
					  + ", txId: " + transactionId
					  );
      MTransactionState ts = mapMTransactionStates.get(transactionId);

      if(ts == null) {
	  if (LOG.isTraceEnabled()) LOG.trace("Returning from MdoCommit, (null tx)" 
					      + ", retval: " + TransReturnCode.RET_NOTX.toString() 
					      + ", txId: " + transactionId
					      );
         return TransReturnCode.RET_NOTX.getShort();
      }

       // Set the commitId
       IdTmId commitId = null;
       long commitIdVal = -1;
       if (recoveryToPitMode || (TRANSACTION_ALGORITHM == AlgorithmType.SSCC)) {
          try {
             commitId = new IdTmId();
             if (LOG.isTraceEnabled()) LOG.trace("MdoCommit getting new commitId");
             idServer.id(ID_TM_SERVER_TIMEOUT, commitId);
             if (LOG.isTraceEnabled()) LOG.trace("MdoCommit idServer.id returned: " + commitId.val);
          } catch (IdTmException exc) {
             LOG.error("MdoCommit: IdTm threw exception ", exc);
             throw new CommitUnsuccessfulException("MdoCommit: IdTm threw exception ", exc);
          }
          commitIdVal = commitId.val;
       }

       if (LOG.isTraceEnabled()) LOG.trace("MdoCommit setting commitId: " + commitIdVal 
					   + ", txId: " + ts.getTransactionId()
					   );
       ts.setCommitId(commitIdVal);

       if (stallWhere == 4) {
          if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2a (before TLOG write) for MdoCommit for transaction: " + transactionId);
          boolean loopBack = false;
          do
          {
             try {
                loopBack = false;
                Thread.sleep(300000); // Initially set to run every 5 min
             } catch (InterruptedException ie) {
                loopBack = true;
             }
          } while (loopBack);
       }

       try {
          ts.setStatus(TransState.STATE_COMMITTED);
	  /*
          if (useTlog) {
             try {
                tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), TransState.STATE_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true);
             }
             catch (Exception e) {
                 LOG.error("MdoCommit: Local TLOG write threw exception during commit ", e);
                 System.exit(1);
             }

             if (bSynchronized && ts.hasRemotePeers()){
                for ( Map.Entry<Integer, HConnection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                   int lv_peerId = entry.getKey();
                   if (lv_peerId == 0) // no peer for ourselves
                      continue;
                   TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                   if (lv_tLog == null){
                      LOG.error("Error during MdoCommit processing for tlog COMMIT for peer: " + lv_peerId);
                      continue;
                   }
                   try {
                      if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                         if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing COMMIT state record");
                         if (synchronousWrites){
                             try {
                                lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), TransState.STATE_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true);
                             }
                             catch (Exception e) {
                                LOG.error("MdoCommit: Remote TLOG write threw exception during commit ", e);
                                throw e;
                             }
                         }                             
                         else{
                            try {
                               if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite COMMITTED for trans: " + ts.getTransactionId());
                               lv_tLog.doTlogWrite(ts, TransState.STATE_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                            }
                            catch (Exception e) {
                               LOG.error("doTlogWrite on remote for commit of txId: " + transactionId + 
                                         " exiting : EXCEPTION ", e);
                              throw e;
                            }
                         }
                      }
                      else {
                         if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping COMMITTED state record");            	   
                      }
                   }
                   catch (Exception e) {
                      LOG.error("MdoCommit, lv_tLog " + lv_tLog + " EXCEPTION: ", e);
                      System.exit(1);
                   }
                } // for ( Map.Entry<Integer, HConnection> entry : pSTRConfig.getPeerConnections().entrySet()) 
             } // if (bSynchronized && ts.hasRemotePeers()){

             // Write the local Tlog State record
             if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
                try{
                  if (LOG.isTraceEnabled()) LOG.trace("MdoCommit, completing Tlog write for transaction: " + transactionId);
                  ts.completeRequest();
                }
                catch(Exception e){
                   LOG.error("Exception in MdoCommit completing Tlog write completeRequest. txId: " + transactionId + ", exiting.  Exception: ", e);
                   // Careful here:  We had an exception writing a commi to the remote peer.  So we can't leave the
                   // records in an inconsistent state.  Will change to abort on local side as well since
                   // we haven't replied yet.
                   System.exit(1);
                }
             }
          }
	  */
       } catch(Exception e) {
          LOG.error("Returning from MdoCommit" 
		    + ", txId: " + transactionId 
		    + ", tLog.putRecord: EXCEPTION " , e
		    );
          return TransReturnCode.RET_EXCEPTION.getShort();
       }

       if ((stallWhere == 2) || (stallWhere == 3)) {
    	  if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2 for MdoCommit for transaction: " + transactionId);
          boolean loopBack = false;
          do {
             try {
                loopBack = false;
                Thread.sleep(300000); // Initially set to run every 5 min
             } catch(InterruptedException ie) {
                 loopBack = true;
             }
          } while (loopBack);
       }

       try {
          mtrxManager.doCommit(ts);
       } catch (CommitUnsuccessfulException e) {
          LOG.error("MdoCommit"
		    + ", txId: " + transactionId
		    + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException"
		    , e
		    );
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       catch (UnsuccessfulDDLException ddle) {
          LOG.error("FATAL DDL Exception from MdoCommit, WAITING INDEFINETLY !! retval: " + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txId: " + transactionId);

          //Reaching here means several attempts to perform the DDL operation has failed in commit phase.
          //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
          //which causes a hang(commit work is outstanding forever) due to MdoCommitX thread holding the
          //commitSendLock (since MdoCommitX raised an exception and exited without clearing the commitSendLock count).
          //In the case of DDL exception, no MdoCommitX thread is involved and commitSendLock is not held. Hence to mimic
          //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
          //Long Term solution to this behaviour is currently TODO.
          Object commitDDLLock = new Object();
          synchronized(commitDDLLock)
          {
             boolean loopBack = false;
             do {
                try {
                   loopBack = false;
                   commitDDLLock.wait();
                } catch(InterruptedException ie) {
                    LOG.warn("Interrupting commitDDLLock.wait" 
			     + ", txId: " + transactionId
			     + ", retrying ", ie);
                    loopBack = true;
                }
             } while (loopBack);

          }
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       /*TBD
       if (useTlog && useForgotten) {
          tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), TransState.STATE_FORGOTTEN_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), forceForgotten); // forced flush?

          if (bSynchronized && ts.hasRemotePeers()) {
             for ( Map.Entry<Integer, HConnection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                int lv_peerId = entry.getKey();
                if (lv_peerId == 0) // no peer for ourselves
                   continue;
                TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                if (lv_tLog == null){
                   LOG.error("Error during MdoCommit processing for tlog FORGOTTEN for peer: " + lv_peerId);
                   continue;
                }
                try {
                   if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                      if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing STATE_FORGOTTEN_COMMITTED state record");
                      if (synchronousWrites){
                         lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, ts.getTmFlags(), TransState.STATE_FORGOTTEN_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true);
                      }
                      else{
                         try {
                            if (LOG.isTraceEnabled()) LOG.trace("calling doTlogWrite STATE_FORGOTTEN_COMMITTED for trans: " + ts.getTransactionId());
                            lv_tLog.doTlogWrite(ts, TransState.STATE_FORGOTTEN_COMMITTED.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true, -1);
                         }
                         catch (Exception e) {
                            LOG.error("Returning from doTlogWrite, txId: " + transactionId + 
                                           " tLog.doTlogWrite: EXCEPTION ", e);
                            return TransReturnCode.RET_EXCEPTION.getShort();
                         }
                      }
                   }
                   else {
                      if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping COMMIT-FORGOTTEN state record");            	   
                   }
                }
                catch (Exception e) {
                   LOG.error("MdoCommit, lv_tLog writing commit forgotten " + lv_tLog + " EXCEPTION: ", e);
                   throw e;
                }
             } // for ( Map.Entry<Integer, HConnection> entry : pSTRConfig.getPeerConnections().entrySet()) 
          } // if (bSynchronized && ts.hasRemotePeers()){
        	  
          if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
             try{
                if (LOG.isTraceEnabled()) LOG.trace("MdoCommit, completing Tlog write for FORGOTTEN transaction: " + transactionId);
                ts.completeRequest();
             }
             catch(Exception e){
                LOG.error("Exception in MdoCommit completing Tlog write completeRequest for FORGOTTEN txId: " + transactionId + "Exception: ", e);
                //  Forgotten not written to remote side.  Return an error
                return TransReturnCode.RET_EXCEPTION.getShort();
             }
          }
       }
       */

       if (LOG.isTraceEnabled()) LOG.trace("Exit MdoCommit" 
					   + ", retval(ok): " + TransReturnCode.RET_OK.toString()
					   + ", txId: " + transactionId 
					   + ", mapsize: " + mapMTransactionStates.size()
					   );

       return TransReturnCode.RET_OK.getShort();
   }

   public short completeRequestCommon(final long transactionId) 
       throws IOException, CommitUnsuccessfulException       
    {

       short lv_hbase_retcode = completeRequest(transactionId);

       short lv_monarch_retcode = (useMonarch ? McompleteRequest(transactionId) : TransReturnCode.RET_NOTX.getShort());

       if (lv_monarch_retcode == TransReturnCode.RET_NOTX.getShort()) {
	   return lv_hbase_retcode;
       }

       return lv_monarch_retcode;
   }

    public short completeRequest(long transactionId)
	throws IOException, CommitUnsuccessfulException 
    {
     TransactionState ts = mapTransactionStates.get(transactionId);
     if (LOG.isTraceEnabled()) LOG.trace("Enter completeRequest" + ", ts: " + ts);

     if (ts == null) {
        if (! useMonarch) {
           LOG.error("Returning from completeRequest, (null tx) retval: "
		       + TransReturnCode.RET_NOTX.toString() 
		       + ", txId: " + transactionId
		       );
        }
        return TransReturnCode.RET_NOTX.getShort();
     }
     
     if (LOG.isTraceEnabled()) LOG.trace("Txn Stat: Enter completeRequest, tid: " + transactionId);
     ts.set_completeRequest_time(System.currentTimeMillis());       

     boolean loopBack = false;
     do {
	   try {
	       loopBack = false;
	       ts.completeRequest();
	   } 
	   catch(InterruptedException ie) {
	       LOG.warn("Interrupting completeRequest but retrying, ts.completeRequest" 
			+ ", txid: " + transactionId 
			+ ", EXCEPTION: "
			, ie);
	       loopBack = true;
	   } 
     } while (loopBack);

     if ( (dynamic_dtm_logging == 1) || (LOG.isDebugEnabled())  ) {     
          LOG.info("Txn Raw Stat: end completeRequest, tid: " + transactionId +
             " begin: " + ts.get_begin_time() +
             " prepareCommit: " + ts.get_prepareCommit_time() +
             " doCommit: " + ts.get_doCommit_time() + 
             " abortTransaction: " + ts.get_abortTransaction_time() + 
             " completeRequest:" + ts.get_completeRequest_time() + 
             " endRequest: " + System.currentTimeMillis()); 
        
          if (ts.get_doCommit_time() > 0) {   
             LOG.info("Txn Stat: txn statistics, tid: " + transactionId +
                 " phase 0: " + (ts.get_prepareCommit_time() - ts.get_begin_time()) +
                 " mills, \tphase 1: " + (ts.get_doCommit_time() - ts.get_prepareCommit_time()) +
                 " mills, \tphase 2 commit: " + (System.currentTimeMillis() - ts.get_doCommit_time()) + " mills " );  
          }
          else {
             long ph1_ts = 0;
             if (ts.get_prepareCommit_time() == 0) ph1_ts = ts.get_abortTransaction_time();
             else ph1_ts = ts.get_prepareCommit_time();
             LOG.info("Txn Stat: txn statistics, tid: " + transactionId +
                " phase 0: " + (ph1_ts - ts.get_begin_time()) +
                " mills, \tphase 1: " + (ts.get_abortTransaction_time() - ph1_ts) +
                " mills, \tphase 2 abortx: " + (System.currentTimeMillis() - ts.get_abortTransaction_time()) + " mills " ); 
          }
     } // dynamic_dtm_logging

     synchronized(mapLock) {
        mapTransactionStates.remove(transactionId);
     }

     if (LOG.isDebugEnabled()) LOG.debug("Exit completeRequest" 
					 + ", txId: " + transactionId 
					 + ", Hmapsize: " + mapTransactionStates.size());
     return TransReturnCode.RET_OK.getShort();
   }

   public short McompleteRequest(long transactionId) 
       throws IOException 
    {
     if (LOG.isDebugEnabled()) LOG.debug("Enter McompleteRequest" 
					 + ", txId: " + transactionId
					 );
     MTransactionState ts = mapMTransactionStates.get(transactionId);

     if(ts == null) {
	 if (! useMonarch) {
	     LOG.error("Returning from McompleteRequest, (null tx) retval: " 
		       + TransReturnCode.RET_NOTX.toString() 
		       + ", txId: " + transactionId
		       );
	 }
	 return TransReturnCode.RET_NOTX.getShort();
     }

     boolean loopBack = false;
     do {
	 try {
	     loopBack = false;
	     ts.completeRequest();
	 } 
	 catch(InterruptedException ie) {
	     LOG.warn("Interrupting McompleteRequest but retrying, ts.completeRequest" 
		      + ", txid: " + transactionId 
		      + ", EXCEPTION: "
		      , ie);
	     loopBack = true;
	 } 
     } while (loopBack);

     synchronized(mapLock) {
        mapMTransactionStates.remove(transactionId);
     }

     if (LOG.isDebugEnabled()) LOG.debug("Exit McompleteRequest" 
					 + ", txId: " + transactionId 
					 + ", Mmapsize: " + mapMTransactionStates.size());
     return TransReturnCode.RET_OK.getShort();
   }

   public short tryCommit(long transactionId) throws IOException, CommitUnsuccessfulException {
     if (LOG.isDebugEnabled()) LOG.debug("Enter tryCommit, txid: " + transactionId);
     short err, commitErr = TransReturnCode.RET_OK.getShort();

     try {
       err = prepareCommit(transactionId, null);
       if (err != TransReturnCode.RET_OK.getShort()) {
         if (LOG.isDebugEnabled()) LOG.debug("tryCommit prepare failed with error " + err);
         return err;
       }
       commitErr = doCommit(transactionId);
       if (commitErr != TransReturnCode.RET_OK.getShort()) {
         LOG.error("doCommit for committed transaction " + transactionId + " failed with error " + commitErr);
         // It is a violation of 2 PC protocol to try to abort the transaction after commit write
         return commitErr;
       }

       if (LOG.isTraceEnabled()) LOG.trace("TEMP tryCommit Calling CompleteRequest() Txid :" + transactionId);

       err = completeRequest(transactionId);
       if (err != TransReturnCode.RET_OK.getShort()){
         if (LOG.isDebugEnabled()) LOG.debug("tryCommit completeRequest for transaction " + transactionId + " failed with error " + err);
       }
     } catch(IOException e) {
       mapTransactionStates.remove(transactionId);
       LOG.error("Returning from HBaseTxClient:tryCommit, ts: EXCEPTION" + " txid: " + transactionId, e);
       throw new IOException("Exception during tryCommit, unable to commit.", e);
    }

    synchronized(mapLock) {
       mapTransactionStates.remove(transactionId);
    }

    if (LOG.isDebugEnabled()) LOG.debug("Exit tryCommit txId: " + transactionId + " mapsize: " + mapTransactionStates.size());
    return TransReturnCode.RET_OK.getShort();
  }

   public short callCreateTable(long transactionId, byte[] pv_htbldesc, Object[]  beginEndKeys, int options) throws IOException
   {
      TransactionState ts;
      HTableDescriptor htdesc = null;

      if (LOG.isTraceEnabled()) LOG.trace("Enter callCreateTable, txId: [" + transactionId
           + "],  htbldesc bytearray: " + pv_htbldesc
           + "desc in hex: " + Hex.encodeHexString(pv_htbldesc));

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from callCreateTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txId: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }
      try {
         htdesc = HTableDescriptor.parseFrom(pv_htbldesc);
         if (LOG.isTraceEnabled()) LOG.trace("Enter callCreateTable: "
              +  htdesc.getNameAsString() + " ts: " + ts 
					     + ", tableDescriptor: " + htdesc);
      } catch (DeserializationException de) {
         LOG.error("Error while getting HTableDescriptor caused by : ", de);
         throw new IOException("Error while getting HTableDescriptor caused by : ", de);
      }
      try {
         trxManager.createTable(ts, htdesc, beginEndKeys, options);
      }
      catch (IOException cte) {
         LOG.error("HBaseTxClient:callCreateTable exception trxManager.createTable, retval: " +
            TransReturnCode.RET_EXCEPTION.toString() +" txid: " + transactionId +" Exception: ", cte);
         throw new IOException("createTable call error", cte);
      }

      
      if (LOG.isTraceEnabled()) LOG.trace("Exit callCreateTable, txid: [" + transactionId + "] returning RET_OK");
      return TransReturnCode.RET_OK.getShort();
   }

   public short callPushOnlineEpoch(long transactionId, byte[] pv_htbldesc) throws IOException
   {
      TransactionState ts;
      HTableDescriptor htdesc = null;

      if (LOG.isTraceEnabled()) LOG.trace("Enter callPushOnlineEpoch, txId: [" + transactionId
           + "],  htbldesc bytearray: " + pv_htbldesc
           + "desc in hex: " + Hex.encodeHexString(pv_htbldesc));

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from callPushOnlineEpoch, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txId: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }
      try {
         htdesc = HTableDescriptor.parseFrom(pv_htbldesc);
         if (LOG.isTraceEnabled()) LOG.trace("Enter callPushOnlineEpoch: "
              +  htdesc.getNameAsString() + " ts: " + ts
					     + ", tableDescriptor: " + htdesc);
      } catch (DeserializationException de) {
         LOG.error("Error while getting HTableDescriptor caused by : ", de);
         throw new IOException("Error while getting HTableDescriptor caused by : ", de);
      }
      try {
         trxManager.pushRegionEpoch(htdesc, ts);
      }
      catch (IOException cte) {
         LOG.error("HBaseTxClient:callPushOnlineEpoch exception trxManager.callPushOnlineEpoch, retval: " +
            TransReturnCode.RET_EXCEPTION.toString() +" txid: " + transactionId +" Exception: ", cte);
         throw new IOException("pushOnlineEpoch call error", cte);
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit callPushOnlineEpoch, txid: [" + transactionId + "] returning RET_OK");
      return TransReturnCode.RET_OK.getShort();
   }

   public short callAlterTable(long transactionId, byte[] pv_tblname, Object[] tableOptions) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callAlterTable, txId: [" + transactionId + "],  tableName: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from callAlterTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txId: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.alterTable(ts, strTblName, tableOptions);
      return TransReturnCode.RET_OK.getShort();
   }

   public short callRegisterTruncateOnAbort(long transactionId, byte[] pv_tblname) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callRegisterTruncateOnAbort, txId: [" + transactionId + "],  tablename: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from callRegisterTruncateOnAbort, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txId: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.registerTruncateOnAbort(ts, strTblName);
      return TransReturnCode.RET_OK.getShort();
   }

   public short callDropTable(long transactionId, byte[] pv_tblname) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callDropTable, txId: [" + transactionId + "],  tablename: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from callDropTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txId: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.dropTable(ts, strTblName);
      return TransReturnCode.RET_OK.getShort();
   }

    public short callRegisterRegion(long transactionId,
                                    long startId,
                                    int  pv_port,
                                    byte[] pv_hostname,
                                    long pv_startcode,
                                    byte[] pv_regionInfo,
                                    int pv_peerId,
                                    int pv_tmFlags) throws IOException {
       String hostname    = new String(pv_hostname);
       if (LOG.isTraceEnabled()) LOG.trace("Enter callRegisterRegion, "
					   + "[peerId: " + pv_peerId + "] " 
					   + "txId: [" + transactionId + "]" 
					   + ", startId: " + startId 
					   + ", port: " + pv_port 
					   + ", hostname: " + hostname 
					   + ", startcode: " + pv_startcode 
					   + ", reg info len: " + pv_regionInfo.length 
                                           + ", TmFlags: " + pv_tmFlags
					   + " " + new String(pv_regionInfo, "UTF-8"));

       if (pv_startcode == 100) {
	   if(LOG.isDebugEnabled()) LOG.debug("callRegisterRegion: start code is 100, returning");
	   return TransReturnCode.RET_OK.getShort();
       }

       HRegionInfo lv_regionInfo;
       try {
          lv_regionInfo = HRegionInfo.parseFrom(pv_regionInfo);
       } catch (DeserializationException de) {
           LOG.error("Error while getting regionInfo caused by : ", de);
           throw new IOException("Error while getting regionInfo caused by : ", de);
       }

       String lv_hostname_port_string = hostname + ":" + pv_port;
       String lv_servername_string = ServerName.getServerName(lv_hostname_port_string, pv_startcode);
       ServerName lv_servername = ServerName.parseServerName(lv_servername_string);
       TransactionRegionLocation regionLocation = new TransactionRegionLocation(lv_regionInfo, lv_servername, pv_peerId);
       String regionTableName = regionLocation.getRegionInfo().getTable().getNameAsString();
       int tableFlags = 0;
                                           
       if (LOG.isDebugEnabled()) LOG.debug("HAX - RegisterRegion TxClient starts: " + regionTableName
               + " trans id: " + transactionId
               + " with tableFlags: " + tableFlags
               + " peer id: " + pv_peerId
               + " tmflags: " + pv_tmFlags
               + " region name detail: " + regionTableName);        

       TransactionState ts = mapTransactionStates.get(transactionId);
       if(ts == null) {
          if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion transactionId (" + transactionId +
                   ") not found in mapTransactionStates of size: " + mapTransactionStates.size());
          try {
             ts = trxManager.beginTransaction(transactionId);
          } catch (IdTmException ite) {
             LOG.error("Begin Transaction Error caused by : ", ite);
             throw new IOException("Begin Transaction Error caused by :", ite);
          }
          synchronized (mapLock) {
             TransactionState ts2 = mapTransactionStates.get(transactionId);
             if (ts2 != null) {
                // Some other thread added the transaction while we were creating one.  It's already in the
                // map, so we can use the existing one.
                if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion, found TransactionState object while creating a new one " + ts2);
                ts = ts2;
             }
             else {
                ts.setStartId(startId);
                if ((pv_tmFlags & SKIP_CONFLICT) == SKIP_CONFLICT){
                   ts.setSkipConflictCheck(true);
                }
                if ((pv_tmFlags & SKIP_SDN_CDC) == SKIP_SDN_CDC){
                   ts.setSkipSdnCDC(true);
                }
                if ((pv_tmFlags & XDC_UP) == XDC_UP){
                   ts.setXdcType(XdcTransType.XDC_TYPE_XDC_UP);
                }
                else if ((pv_tmFlags & XDC_DOWN) == XDC_DOWN){
                    ts.setXdcType(XdcTransType.XDC_TYPE_XDC_DOWN);
                }
                if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion new transactionState created: " + ts );
             }
          }// end synchronized
       }
       else {
          if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion existing transactionState found: " + ts );
          if (ts.getStartId() == -1) {
            ts.setStartId(startId);
            if ((pv_tmFlags & SKIP_CONFLICT) == SKIP_CONFLICT){
               ts.setSkipConflictCheck(true);
            }
            if ((pv_tmFlags & SKIP_SDN_CDC) == SKIP_SDN_CDC){
               ts.setSkipSdnCDC(true);
            }
            if ((pv_tmFlags & XDC_UP) == XDC_UP){
               ts.setXdcType(XdcTransType.XDC_TYPE_XDC_UP);
            }
            else if ((pv_tmFlags & XDC_DOWN) == XDC_DOWN){
               ts.setXdcType(XdcTransType.XDC_TYPE_XDC_DOWN);
            }
            if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion reset startId for transactionState: " + transactionId );
          }
       }

       if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN)
              tableFlags |= XDC_DOWN;
              
       if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)
              tableFlags |= XDC_UP;

       if (ts.getSkipSdnCDC())
              tableFlags |= SKIP_SDN_CDC;

       if ((pv_tmFlags & SYNCHRONIZED) == SYNCHRONIZED)
              tableFlags |= SYNCHRONIZED; 

       if (((pv_tmFlags & SYNCHRONIZED) == SYNCHRONIZED) &&
            (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN)){
              if ((pv_tmFlags & SKIP_SDN_CDC) == SKIP_SDN_CDC) {
                  tableFlags |= SKIP_SDN_CDC; 
                  ts.setSkipSdnCDC(true);
              }
              else { // no SKIP_SDN_CDC, original SDN behavior
                  regionLocation.setGenerateCatchupMutations(true);
                  ts.setGenerateMutations(true);
              }
              tableFlags |= XDC_DOWN; // table CD attr will be set as tableFlags in first table region registration
       }

      if (((pv_tmFlags & TABLE_ATTR_SET) == TABLE_ATTR_SET)){ // change ts table attr only, no region reg
             int action = ts.tableAttrSetAndRegister(regionTableName, pv_tmFlags);
             ts.setGenerateMutations(true);
             
            if (LOG.isDebugEnabled()) LOG.debug("HAX - Reset flags TxClient, table name: " + regionTableName
               + " trans id: " + transactionId
               + " ts xdc type: " + ts.getXdcType()
               + " with tableFlags: " + tableFlags
               + " peer id: " + pv_peerId
               + " tmflags: " + pv_tmFlags               
               + " region name detail: " + regionTableName);                         
             
             if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:registerRegion, action for table IBR/SYNC attribute " + action + " with flags " + pv_tmFlags);
             return TransReturnCode.RET_OK.getShort();
      }

       if (((pv_tmFlags & INCREMENTALBR) == INCREMENTALBR)){
              if (LOG.isTraceEnabled()) LOG.trace("callRegisterRegion set regionLocation and ts GenerateMutation true, " + ts );
              regionLocation.setGenerateCatchupMutations(true);
              tableFlags |= INCREMENTALBR;
              ts.setGenerateMutations(true);
       }
       
       if (((pv_tmFlags & TIMELINE) == TIMELINE)){
              tableFlags |= TIMELINE;
              if (LOG.isDebugEnabled()) LOG.debug("HBaseTxClient:registerRegion, pv_tmFlags " + pv_tmFlags +
                          " tableFlags " + tableFlags + " table " + regionTableName);              
       }       

       if (recoveryToPitMode) {
              tableFlags |= PIT_ALL;
       }

       try {
          if( (pv_tmFlags & ADD_TOTALNUM )  == ADD_TOTALNUM )  {
            if(
                 ( tableFlags & INCREMENTALBR) == INCREMENTALBR &&
                 !regionTableName.contains("TRAFODION._XDC_MD_.XDC_DDL") ) 
            regionLocation.addTotalNum=true;
          }
          trxManager.registerRegion(ts, regionLocation);
       } catch (IOException e) {
          LOG.error("callRegisterRegion exception in registerRegion call, txId: " + transactionId +
            " retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException ", e);
          throw new IOException("trxManager.registerRegion Error caused by :", e);
       }

       if (LOG.isDebugEnabled()) LOG.debug("RegisterRegion adding table name " + regionTableName
               + " with tableFlags " + tableFlags);

       //the idea is, if the new register request has IB flag, then update the TM 
       boolean addedIt = ts.addTableName(regionTableName, tableFlags);
       if(addedIt== false)
         ts.tableIBAttrSetAndRegister(regionTableName, tableFlags);

       if (LOG.isTraceEnabled()) LOG.trace("Exit callRegisterRegion, ts: " + ts + " with mapsize: "
                  + mapTransactionStates.size());

       if (LOG.isDebugEnabled()) LOG.debug("HAX - RegisterRegion TxClient end, table name: " + regionTableName
               + " trans id: " + transactionId
               + " ts xdc type: " + ts.getXdcType()
               + " with tableFlags: " + tableFlags
               + " peer id: " + pv_peerId
               + " tmflags: " + pv_tmFlags               
               + " region name detail: " + regionTableName);                  

       return TransReturnCode.RET_OK.getShort();
   }

    public short callMRegisterRegion(long transactionId,
				     long startId,
				     int  pv_port,
				     byte[] pv_hostname,
				     long pv_startcode,
				     byte[] pv_ba_esl,
				     int pv_peerId) throws Exception 
    {

	if (!useMonarch) return TransReturnCode.RET_OK.getShort();

	try {
	    String hostname    = new String(pv_hostname);

	    EsgynMServerLocation lv_esl = null;
	    try {
		lv_esl = EsgynMServerLocation.deserialize(pv_ba_esl);
	    }
	    catch (Throwable e) {
		LOG.error("Exception in deserialization", e);
	    }

	    if (LOG.isDebugEnabled()) LOG.debug("Enter callMRegisterRegion, "
						+ ", peerId: " + pv_peerId
						+ ", txId: " + transactionId 
						+ ", startId: " + startId 
						+ ", port: " + pv_port 
						+ ", hostname: " + hostname 
						+ ", startcode: " + pv_startcode 
						+ ", reg info len: " + pv_ba_esl.length 
						+ ", mapSize: " + mapMTransactionStates.size()
						+ ", location: " + lv_esl
						);

	    MTransactionState lv_ts = mapMTransactionStates.get(transactionId);

	    if(lv_ts == null) {
		try {
		    lv_ts = mtrxManager.beginTransaction(transactionId);
		}
		catch (IdTmException exc) {
		    LOG.error("callMRsgisterRegion: exception in beginTransaction()"
			      + ", txId: " + transactionId 
			      , exc
			      );
		    throw new IdTmException("mtrxmanager.beginTransaction" 
					    +", txId: " + transactionId 
					    + " caught exception ", exc);
		}
		catch (Throwable e) {
		    LOG.error("Exception in beginTransaction", e);
		}

		if (lv_ts != null) {
		    mapMTransactionStates.put(transactionId, lv_ts);
		}
	    }

	    try {
		mtrxManager.registerRegion(lv_ts, lv_esl);
	    } catch (IOException e) {
		LOG.error("callMRegisterRegion exception in registerRegion()"
			  + ", txId: " + transactionId
			  + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() 
			  + " IOException "
			  , e
			  );
		return TransReturnCode.RET_EXCEPTION.getShort();
	    }
	}
	catch (Throwable te) {
	    LOG.error("Catchall: ", te);
	}

	if (LOG.isTraceEnabled()) LOG.trace("Exit callMRegisterRegion" 
					    + ", txId: " + transactionId 
					    + ", mapsize: " + mapMTransactionStates.size()
					    );

	return TransReturnCode.RET_OK.getShort();
    }

   public int participatingRegions(long transactionId) 
       throws IOException 
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter participatingRegions, txId: " + transactionId);
       return (HparticipatingRegions(transactionId) + MparticipatingRegions(transactionId));
   }

   public int HparticipatingRegions(long transactionId)
       throws IOException 
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter HparticipatingRegions, txId: " + transactionId);
       TransactionState ts = mapTransactionStates.get(transactionId);
       if(ts == null) {
         if (LOG.isTraceEnabled()) LOG.trace("Returning from HparticipatingRegions" 
					     + ", txId: " + transactionId 
					     + " not found, returning: 0"
					     );
          return 0;
       }
       int participants = ts.getParticipantCount() - ts.getRegionsToIgnore();
       if (LOG.isTraceEnabled()) LOG.trace("Exit HparticipatingRegions" 
					   + ", txId: " + transactionId 
					   + ", #participants:" + participants
					   + ", has DDL operation(s): " + ts.hasDDLTx()
					   );
    
       //In some scenarios, it is possible only DDL operation is performed
       //within a transaction, example initialize trafodion, drop; In this
       //scenario, region participation is zero. For the prepareCommit to
       //continue to doCommit, there needs to be atleast one participant.
       if(participants == 0 && ts.hasDDLTx())
           participants++;

       //return (ts.getParticipantCount() - ts.getRegionsToIgnore());
       return participants;
   }

   public int MparticipatingRegions(long transactionId)
       throws IOException 
    {
       if (mapMTransactionStates == null)
          return 0;
       if (LOG.isTraceEnabled()) LOG.trace("Enter MparticipatingRegions, txId: " + transactionId);

       MTransactionState ts = mapMTransactionStates.get(transactionId);
       if(ts == null) {
         if (LOG.isTraceEnabled()) LOG.trace("Returning from MparticipatingRegions" 
					     + ", txId: " + transactionId 
					     + " not found, returning: 0"
					     );
          return 0;
       }

       int lv_participants = ts.getParticipantCount() - ts.getRegionsToIgnore();
       if (LOG.isTraceEnabled()) LOG.trace("Exit MparticipatingRegions" 
					   + ", txId: " + transactionId 
					   + ", #participants: " + lv_participants
					   + ", has DDL operation(s): " + ts.hasDDLTx()
					   );
    
       //In some scenarios, it is possible only DDL operation is performed
       //within a transaction, example initialize trafodion, drop; In this
       //scenario, region participation is zero. For the prepareCommit to
       //continue to doCommit, there needs to be atleast one participant.
       if(lv_participants == 0 && ts.hasDDLTx())
           lv_participants++;

       return lv_participants;
   }

   public long addControlPoint() throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Enter addControlPoint");
      long result = 0L;
       try {
      if (bSynchronized){
         for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
            int lv_peerId = entry.getKey();
            if (lv_peerId == 0) // no peer for ourselves
               continue;
            TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
            if (lv_tLog == null){
               LOG.error("Error during control point processing for tlog for peer: " + lv_peerId);
               continue;
            }
            if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
               if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; issuing control point");
               // only increment the CP number on the local connection
               try{
                  lv_tLog.addControlPoint(trafClusterId, trafInstanceId, mapTransactionStates, false);
               }
               catch (Exception e2){
                  if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " excetion skipping control point and returning -1 ", e2);
                  return -1;
               }
            }
            else {
               if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping control point");
            }
         }
      }
      try {
         // This code is left for convenience should TLOG parsing be needed in the future
         //if (LOG.isTraceEnabled())LOG.trace("AddControlPoint getting transid: " + lastTransid + " from TLOG" );
         //TransactionState ts1 = new TransactionState(lastTransid);
         //tLog.getTransactionState(ts1, true /* postAllRegions */);
         //if (LOG.isTraceEnabled())LOG.trace("getTransactionState for transid: " + lastTransid + " returned " + ts1 );

         if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient calling tLog.addControlPoint for the " +
                 TmAuditTlog.getTlogTableNameBase() + " set with mapsize " + mapTransactionStates.size());
         result = tLog.addControlPoint(trafClusterId, trafInstanceId, mapTransactionStates, true);
      }
      catch(IOException e){
          LOG.error("addControlPoint IOException ", e);
          throw e;
      }

      Long lowestStartId = Long.MAX_VALUE;
      for(ConcurrentHashMap.Entry<Long, TransactionState> entry : mapTransactionStates.entrySet()){
          TransactionState value;
          value = entry.getValue();
          long ts = value.getStartId();
          if( ts < lowestStartId) lowestStartId = ts;
      }
      if(lowestStartId < Long.MAX_VALUE)
      {
          tmZK.createGCzNode(Bytes.toBytes(lowestStartId));
      }

       }
       catch (Throwable e) {
	   LOG.error("Exception in addControlPoint", e);
       }

      if (LOG.isTraceEnabled()) LOG.trace("Exit addControlPoint, returning: " + result);
      return result;
   }

   public short doCommitSavepoint(long transactionId, long svptId, long pSvptId) throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Enter doCommitSavepoint, transactionId: " + transactionId
                                          + " svptId: " + svptId + " pSvptId: " + pSvptId);
      TransactionState ts = mapTransactionStates.get(transactionId);

      if(ts == null) {
         if (! useMonarch) {
            LOG.error("Returning from transactionId, (null tx) retval: "
                 + TransReturnCode.RET_NOTX.toString()
                 + ", txId: " + transactionId + " svptId: " + svptId + " pSvptId: " + pSvptId);
         }
         return TransReturnCode.RET_NOTX.getShort();
      }

      // Check to see if we returned an error and the client is trying to redrive commit
      if (ts.getStatus().toString().contains("ABORT")){
        LOG.error("Attemping redrive commit savepoint for aborted tx: " + ts
                + " svptId: " + svptId + " pSvptId: " + pSvptId);

        return TransReturnCode.RET_NOTX.getShort();
      }

      if (LOG.isTraceEnabled()) LOG.trace("Txn Stat Enter doCommitSavepoint, tid: " + transactionId
                                          + " svptId: " + svptId + " pSvptId: " + pSvptId);

      if (ts.getStatus().getValue() == TransState.STATE_ACTIVE.getValue()){
         // No error in writing final state records to TLOG; continue to commit
         try {
            int stallFlags = 0;
            if (stallWhere > 5) {
               stallFlags = stallWhere;
            }
            if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepoint, calling trxManager.doCommitSvpt("
                                              + ts.getTransactionId()
                                              + " , svptId: " + svptId + " ,pSvptId: " + pSvptId + ")" );
            trxManager.commitSavepoint(ts, svptId, pSvptId);
         } catch (CommitUnsuccessfulException e) {
            LOG.error("Returning from commitSavepoint, transaction: " + transactionId
                      + " svptId: " + svptId + " pSvptId: " + pSvptId
                      + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException");
            return TransReturnCode.RET_EXCEPTION.getShort();
         }
       }
       else{
         return TransReturnCode.RET_NOTX.getShort();
       }

       return TransReturnCode.RET_OK.getShort();
   }

   public short abortSavepoint(long transactionId, long svptId, long pSvptId) throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Enter abortSavepoint, transactionId: " + transactionId
                                          + " svptId: " + svptId);
      TransactionState ts = mapTransactionStates.get(transactionId);

      if(ts == null) {
         if (! useMonarch) {
            LOG.error("Returning from transactionId, (null tx) retval: "
                 + TransReturnCode.RET_NOTX.toString()
                 + ", txId: " + transactionId + " svptId: " + svptId);
         }
         return TransReturnCode.RET_NOTX.getShort();
      }

      // Check to see if we returned an error and the client is trying to redrive commit
      if (ts.getStatus().toString().contains("ABORT")){
        LOG.error("Attemping redrive abort savepoint for aborted tx: " + ts
                + " svptId: " + svptId);

        return TransReturnCode.RET_NOTX.getShort();
      }

      if (LOG.isTraceEnabled()) LOG.trace("Txn Stat Enter abortSavepoint, tid: " + transactionId
                                          + " svptId: " + svptId);

      if (ts.getStatus().getValue() == TransState.STATE_ACTIVE.getValue()){
         // No error in writing final state records to TLOG; continue to commit
         try {
            int stallFlags = 0;
            if (stallWhere > 5) {
               stallFlags = stallWhere;
            }
            if (LOG.isTraceEnabled()) LOG.trace("abortSavepoint, calling trxManager.abortSavepoint("
                                              + ts.getTransactionId()
                                              + " , svptId: " + svptId + ")" );
            trxManager.abortSavepoint(ts, svptId, pSvptId);
         } catch (CommitUnsuccessfulException e) {
            LOG.error("Returning from abortSavepoint, transaction: " + transactionId
                      + " svptId: " + svptId
                      + ", retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException");
            return TransReturnCode.RET_EXCEPTION.getShort();
         }
       }
       else{
         return TransReturnCode.RET_NOTX.getShort();
       }

       return TransReturnCode.RET_OK.getShort();
   }

   private static class MutationFlushThread extends Thread{

	     // Variables here
	     static final int INTERVAL_DELAY = 60000; // Initially set to run every 60 sec
	     private int envIntervalTimeInt;
	     private static Configuration config;
	     private static Connection connection;
	     private static STRConfig pSTRConfig = null;

	     public final int GENERIC_FLUSH_MUTATION_FILES = 2;
	     private long SNAPSHOT_MUTATION_FLUSH_KEY = 1L;

	     private boolean skipSleep = false;
	     private boolean continueThread = true;
	     private int retryCount = 0;
	     private int flushInterval = 0;

	     private long timeIdVal = 1L;
	     private IdTm idServer;
	     private IdTmId timeId;

	     private SnapshotMeta sm;
	     private SnapshotMutationFlushRecord smfr;

         protected static ExecutorService mThreadPool = null;
         protected static CompletionService<Integer> mCompPool = null;

	     private abstract class mutationFlushCallable implements Callable<Integer>{

	        TransactionalTable tTable;

	        mutationFlushCallable(TransactionalTable lvTable) throws IOException {
               this.tTable = lvTable;
	        }

	        public Integer flushMutationX(TransactionalTable tTable) throws Exception  {

	           int lv_status = 0;
	           final boolean flag = false;
	           String hbaseTableName = tTable.getName().getNameAsString();
	           try {
	              lv_status = tTable.broadcastRequest(GENERIC_FLUSH_MUTATION_FILES, flag);
	              if (lv_status != 0) {
	                 throw new IOException("flushMutationX request failed with status " + lv_status);
	              }
               } catch(Exception e) {
                  if (e instanceof NotServingRegionException){
                     if (LOG.isErrorEnabled()) LOG.error("flushMutationX encountered exception on table: "
                              + hbaseTableName + " " , e);
                     throw e;
                  }
	              if (! (e instanceof TableNotFoundException)){
	                 if (LOG.isErrorEnabled()) LOG.error("flushMutationX encountered exception on table: "
	                          + hbaseTableName + " " , e);
	                 throw new IOException(e);
	              }
	              else {
	                 if (LOG.isWarnEnabled()) LOG.warn("flushMutationX ignoring TableNotFoundException on table: " + hbaseTableName);
	              }
	           }
	           finally{
                  // tTable.close();
	           }
	           return lv_status;
	        }
	     } // mutationFlushCallable

	     public MutationFlushThread (Configuration pv_config, Connection pv_connection) throws IOException{

	        this.config = new Configuration(pv_config);
	        this.connection = pv_connection;

	        mThreadPool = Executors.newFixedThreadPool(8);
	        mCompPool = new ExecutorCompletionService<Integer>(mThreadPool);

            if (LOG.isInfoEnabled()) LOG.info("constructor mCompPool: " + mCompPool);

             try {
                sm = new SnapshotMeta(this.config);
             } catch (IOException e ){
                LOG.error("Unable to create SnapshotMeta, throwing exception " , e);
                throw new IOException("Unable to create SnapshotMeta, throwing exception " , e);
             }

             int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec

             idServer = new IdTm(false);
             timeId = new IdTmId();
             try {
                idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
             } catch (IdTmException ide) {
                throw new IOException(ide);
             }
             timeIdVal = timeId.val;
             try{
               SnapshotMutationFlushRecord smfr = sm.getNthFlushRecord(0);
               if (smfr != null){
                  flushInterval = sm.getNthFlushRecord(0).getInterval();
               }
               else {
                  flushInterval = 0;
               }
            }
	        catch(Exception e){
	           if (LOG.isWarnEnabled()) LOG.warn("MutationFlushThread caught exception getting flush record ",e);
	           throw new IOException(e);
	        }
	     }

	     public long getIdTmVal() throws Exception {
	        IdTmId LvId;
	        IdTm idServer = new IdTm(false);
	        LvId = new IdTmId();
//	        idServer.id(ID_TM_SERVER_TIMEOUT, LvId);
	        idServer.id(1000, LvId);
	        return LvId.val;
	     }

	     public void stopThread() {
	        this.continueThread = false;
	        mThreadPool.shutdown();
	     }

	     private ArrayList<String> getTableNames() throws IOException{

	        SnapshotMutationFlushRecord tmpSMFR = sm.getFlushRecord();
	        if (LOG.isInfoEnabled()) LOG.info("getTableNames retrieved flush record " + (tmpSMFR == null ? "null" : tmpSMFR));

	        ArrayList<String> tables = null;
	        if (tmpSMFR != null){
	           tables = tmpSMFR.getTableList();
	        }
	        else{
	           tables = new ArrayList<String>();
	        }

	        if (LOG.isInfoEnabled()) LOG.info("getTableNames retrieved " + tables.size() + " tables");
	        return tables;
	     }

	     private void broadcastFlushes() throws Exception{
	        if (LOG.isDebugEnabled()) LOG.debug("broadcastFlushes start for interval: " + flushInterval);

	        ArrayList<String> tableNames = getTableNames();
	        int loopCount = 0;
	        for (final String t : tableNames) {
	           try {
                 if (LOG.isDebugEnabled()) LOG.debug("broadcastFlushes table: " + t);
//                 if (LOG.isInfoEnabled()) LOG.info("broadcastFlushes connection: " + connection);
	             // Send work to thread
                 final TransactionalTable tab = new TransactionalTable(t, connection);
//                 if (LOG.isInfoEnabled()) LOG.info("broadcastFlushes tab: " + tab);
//                 if (LOG.isInfoEnabled()) LOG.info("broadcastFlushes mCompPool: " + mCompPool);
                 mCompPool.submit(new mutationFlushCallable(tab) {
                     public Integer call() throws Exception {
						 if (!Thread.currentThread().getName().startsWith("m")) {
							 Thread.currentThread().setName("mutationFlushCallable-" + Thread.currentThread().getName());
						 }

                        return flushMutationX(tab);
                     }
                   });
	             loopCount++;
	           }catch (Exception e) {
	              if (e instanceof org.apache.hadoop.hbase.NotServingRegionException){
	                 if (LOG.isErrorEnabled()) LOG.error("broadcastFlushes exception : ", e);
	                 throw e;
	              }
	              else if (! (e instanceof org.apache.hadoop.hbase.TableNotFoundException)){
	                 if (LOG.isErrorEnabled()) LOG.error("broadcastFlushes exception : ", e);
	                 throw new IOException(e);
	              }
	           }
	        } // for
	        // simply to make sure they all complete, no return codes necessary at the moment
	        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
	          try {
	             int returnValue = mCompPool.take().get();
	          }catch (Exception e) {
	             if (e.toString().contains("Coprocessor result is null, retries exhausted")){
	                if (LOG.isInfoEnabled()) LOG.info("broadcastFlushes null coprocessor result; rethrowing");
	                throw e;
	             }
	             else if (e.toString().contains("TableNotFoundException")){
	                if (LOG.isInfoEnabled()) LOG.info("broadcastFlushes TableNotFoundException");
	             }
	             else{
	                if (LOG.isErrorEnabled()) LOG.error("broadcastFlushes exception retrieving results : ", e);
	                throw new IOException(e);
	             }
	          }
	        }
	        if (LOG.isDebugEnabled()) LOG.debug("broadcastFlushes end for interval: " + flushInterval);
	     }

	     @Override
	     public void run() {


	        int ID_TM_SERVER_TIMEOUT = 1000;

	        String idtmTimeout = System.getenv("TM_IDTM_TIMEOUT");
	        if (idtmTimeout != null){
	          ID_TM_SERVER_TIMEOUT = Integer.parseInt(idtmTimeout.trim());
	        }

	        String intervalString = System.getenv("TMRECOV_MUTATION_FLUSH_INTERVAL");
	        if (intervalString != null){
	           envIntervalTimeInt = Integer.parseInt(intervalString.trim());
	        }
	        else{
	           envIntervalTimeInt = INTERVAL_DELAY;
	        }
	        if (LOG.isInfoEnabled()) LOG.info("DTM Mutation Flush Thread interval set to " + envIntervalTimeInt + " ms");

	        while (this.continueThread) {

	           try {
	              if(this.continueThread) {
	                 if (LOG.isInfoEnabled()) LOG.info("DTM Mutation Thread running interval: " + flushInterval);
	                 try {
	                    broadcastFlushes();
	                    timeIdVal = getIdTmVal();
	                    smfr = sm.getFlushRecord();
	                    if (smfr != null){
	                       ArrayList<String> tables = sm.getFlushRecord().getTableList();
	                       smfr = new SnapshotMutationFlushRecord(SNAPSHOT_MUTATION_FLUSH_KEY,
	                                  SnapshotMetaRecordType.getVersion(), flushInterval, timeIdVal, tables);
	                       sm.putRecord(smfr);

	                       // Only increment the interval if the rest of the operations were successful.
	                       // This is analogous to slipping a control point.
	                       flushInterval++;
	                    }
	                    else {
	                       if (LOG.isInfoEnabled()) LOG.info("DTM Mutation Thread record not found; skipping interval: " + flushInterval);
	                    }

	                 }catch (Exception e) {
	                    LOG.error("exception in broadcastFlushes for interval: " + flushInterval + " ", e);
	                 }
	                 if (!skipSleep) {
	                    Thread.sleep(envIntervalTimeInt);
	                 }
	              }
	              retryCount = 0;
	           } catch (InterruptedException e) {
	              LOG.error("Error in MutationFlushThread: ", e);
	           }

	        }
	     }

	   }  // MutationFlushThread

     /**
      * Thread to gather recovery information for regions that need to be recovered 
      */
   /*
    * DDL specific recovery operation use cases are as follows:
    * In case of phase 1 prepare , phase 2 commit, DDL operations are
    * completed once the region operations are complete. However incase of TM
    * going down and restarting, some of the state information is lost.
    * 1. If the DDL operation also involves DML operations, regions that have
    * in doubt transaction request for help. Recovery thread here reconstructs 
    * the TS state and also DDL operation state from TMDDL and redrives the operation.
    * 
    * 2. If all the regions have completed their operations and if only DDL operation
    * is pending and TM goes down and restarted, there are no regions that 
    * would seek help to redrive the operations. if there is pending DDL operation
    * it will be left starving as there are no triggers to redrive the operation. 
    * To handle this case, every time TM starts, as part of recovery thread start
    * a general scan of TMDDL is made to check for owning transIDs and those that 
    * have active DDL is checked against state of transaction and appropriately redriven.
    * 
    * 3. Failure of TM and restart of TM can happen at any state of DDL operation 
    * in progress and before that operation is recorded as complete. One way to 
    * accurately keep note of this operation in progress is to record the operation 
    * before and after the operation. For this, the table against which the operation
    * is being performed would be the key in a new log, we choose another table
    * called TmDDLObject table. This acts as a global semaphore for DDL table 
    * operation. Recovery Thread as part of its startup processing always checks 
    * against TmDDLObject table and if it owns the transaction, continues to
    * recover the DDL operation. 
    *
    */
    /* DDL Use cases and detailed info :

    Use case                Tx result     phase 1       phase 2
    ----------              ----------    -------     ------------
    Create Table            commit        no op         no op 
    Create Table            rollback      no op         disable ,delete             
    
    create table ,insert    commit        no op         no op
    create table, insert    rollback      no op         disable, delete    Notes:need flag for preclose, make this flag persistent in tmddl and region
    
    
    Drop Table              commit        disable       delete                       
    Drop Table              rollback      no op         no op                        
    
    Insert, drop table      commit        disable       delete             Notes: flag for preclose,   make this flag persistent in tmddl and region.
    Insert, drop table      rollback      no op         no op
    
    
    0. Commit thread and recovery thread do that same thing, either commit or abort.
    
    1. Commit thread is aware of other regions. Recovery thread is not aware of other regions.
       Commit thread doing commit or abort will perform end point calls to all regions involved followed by DDL. 
       Recovery thread doing commit or abort will perform DDL operation separate and region operation separate.
       Question is can DDL and DML commit processing be done in parallel or decoupled. Looks like its ok, MD dml 
       and actual DDL have same decision either to commit or abort.  
       One concern is visibility of the table based on corresponding MD changes being recovered( commit or abort)
       at different times. 
       
       With create table scenario, unwinding DDL and MD at different times does not matter since
       creation of new table from a new thread will get errors from Hmaster since table still exists in hmaster.
       One caviot is TM thinking table is created, but before creating TM dies. When recovery thread attempts to recover abort,
       it goes ahead and deletes the table( which some other thread already succeded in creating the table) which is very dangerous. 
       This scenario is handled as part of step 3 below.   
       
       With drop table scenario, unwinding drop DDL and MD at slightly different times is acceptable since:
       
       In general, whether recovery thread or commit thread is driving the commit or abort, visibility of the table
       is the same as originally intended. 
       
       Having DDL table and DML in the same table, having insertsDrop and createInserts flag persistent is needed for 
       redrive of commit or abort.
      
      
    2. Commit thread and recovery thread cannot overlap or do same execution in parallel. 
       Transaction status in Tlog must be the sole decision maker for recovery thread to redrive. If recovery thread
       finds a active transaction state object in memory, it assumes commit thread is handling it.
    
       
    3. Failure of TM and restart of TM can happen at any state of DDL operation in progress and before that operation is recorded as complete. 
       
       In case of create table,  TM dies before create table,  tx is now aborted.  Visibility of table is "Does not exist". However
       the recovery thread should not attempt to delete the table if it really did not create it ( some other thread might have created it in parallel).
       This is where we need a global semaphore on table name.
       
       In case of create table,  TM dies after create table,  tx is now aborted. Visibility of table is "Does not exist". recovery thread
       attempting to delete the table is ok here.
       
       In case of drop table, the visibility of the table at Hmaster prevents other threads from creating a duplicate table. There is scenario of
       accidently dropping a duplicate table.
    
       In general, a global semaphore kind of method helps to reduce the complexity and recover from various stages of failures.  
    
    */
   
     private static class RecoveryThread extends Thread{
             static final int SLEEP_DELAY = 1000; // Initially set to run every 1sec
             private int sleepTimeInt = 0;
             private boolean skipSleep = false;
             private TmAuditTlog audit;
             private HBaseTmZK zookeeper;
             private TransactionManager txnManager;
             private short tmID;
             private Set<Long> inDoubtList;
             private boolean continueThread = true;
             private int recoveryIterations = -1;
             private int retryCount = 0;
             private boolean useForgotten;
             private boolean forceForgotten;
             private boolean useTlog;
             private boolean leadtm;
             private boolean takeover;
             HBaseTxClient hbtx;
             private int my_local_nodecount = 1; // min node number in a cluster
             private boolean msenv_tlog_sync = false;
             private static int envSleepTimeInt;
             private static int recoveryWaitingTime = 60000;
             private static int possibleRetries = 43200;
             private static boolean env_tlog_sync;
             private boolean ddlOnlyRecoveryCheck = true;
             private Connection rt_connection;
             private Map<Long, Long> trxNoTXMap;
             private Map<Long, TransState> trxTXState;
             private boolean nonnativeThread;

         static {
            String sleepTime = System.getenv("TMRECOV_SLEEP");
            if (sleepTime != null) 
               envSleepTimeInt = Integer.parseInt(sleepTime.trim());
            else
               envSleepTimeInt = SLEEP_DELAY;
            if (LOG.isInfoEnabled()) LOG.info("DTM Recovery Thread sleep set to " + envSleepTimeInt + " ms");
            String retryCount = System.getenv("TMRECOV_RETRY_COUNT");
            if (retryCount != null)
               possibleRetries = Integer.parseInt(retryCount.trim());
            LOG.info("DTM Recovery Thread retry count set to " + possibleRetries);
            String waitingTime = System.getenv("TMRECOV_WAITTING_IN_SECONDS");
            if (waitingTime != null) {
               recoveryWaitingTime = Integer.parseInt(waitingTime.trim());
               recoveryWaitingTime = recoveryWaitingTime * 1000;
            }
             LOG.info("DTM Recovery waitting time set to " + recoveryWaitingTime + " ms");
            String TlogSync = System.getenv("TM_TLOG_SYNC");
            if (TlogSync != null) 
               env_tlog_sync = (Integer.parseInt(TlogSync.trim()) != 0);
            else
               env_tlog_sync = false;
            if (LOG.isInfoEnabled()) LOG.info("DTM Recovery Thread TM_TLOG_SYNC set to " + env_tlog_sync);
         }

            public RecoveryThread(TmAuditTlog audit,
                               HBaseTmZK zookeeper,
                               TransactionManager txnManager,
                               HBaseTxClient hbtx,
                               boolean useForgotten,
                               boolean forceForgotten,
                               boolean useTlog,
                               boolean leadtm,
                               boolean takeover,
                               boolean threadForOthers) {
             this(audit, zookeeper, txnManager);
             this.hbtx = hbtx;
             this.useForgotten = useForgotten;
             this.forceForgotten = forceForgotten;
             this.useTlog= useTlog;
             this.leadtm = leadtm;
             this.takeover = takeover;
             this.nonnativeThread = threadForOthers; //recovery thread for other nodes
             if (leadtm) this.tmID = -2; // for peer recovery thread

             try {
                   rt_connection = ConnectionFactory.createConnection(config);
                   if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " create rt connection in thread startup, " + rt_connection);
              }
              catch (IOException ioe1) {
                   LOG.error("Traf Recover Thread hits exception when trying to create rt connection during init " + ioe1);
              }
              
             try {
                   pSTRConfig = STRConfig.getInstance(config);
              }
              catch (Exception zke) {
                   LOG.error("Traf Recover Thread hits exception when trying  to get STRConfig instance during init " + zke);
              }

             this.my_local_nodecount = pSTRConfig.getTrafodionNodeCount();
             if(LOG.isDebugEnabled()) LOG.debug("Traf Recovery Thread starts for DTM " + tmID + " at cluster "
                   + pSTRConfig.getTrafClusterIdInt() + " Node Count " + my_local_nodecount +
                             " LDTM " + leadtm + " Takeover " + takeover);

             // NOTE. R 2.0, doing commit log reload/sync or commit-takeover-write/cump-CP would require an off-line ENV (at least updated transactions are drained and then stopped)
             // skip tlog sync if ms_env says so

             msenv_tlog_sync = env_tlog_sync;
            }
             /**
              *
              * @param audit
              * @param zookeeper
              * @param txnManager
              */
            public RecoveryThread(TmAuditTlog audit,
                                   HBaseTmZK zookeeper,
                                   TransactionManager txnManager)
            {
                          this.audit = audit;
                          this.zookeeper = zookeeper;
                          this.txnManager = txnManager;
                          this.inDoubtList = new HashSet<Long> ();
                          this.tmID = zookeeper.getTMID();
                          this.sleepTimeInt = envSleepTimeInt;
                          this.trxNoTXMap = new HashMap<Long, Long>();
                          this.trxTXState = new HashMap<Long, TransState>();
            }

            public void stopThread() {
                 this.continueThread = false;
            }

            private void addRegionToTS(String hostnamePort, byte[] regionInfo, TransactionState ts) throws IOException {
                 HRegionInfo regionInfoLoc; // = new HRegionInfo();
                 final byte [] delimiter = ",".getBytes();
                 String[] result = hostnamePort.split(new String(delimiter), 3);

                 if (result.length < 2)
                         throw new IllegalArgumentException("Region array format is incorrect");

                 String hostname = result[0];
                 int port = Integer.parseInt(result[1]);
                 try {
                    regionInfoLoc = HRegionInfo.parseFrom(regionInfo);
                 } catch (DeserializationException de) {
                    throw new IOException(de);
                 }

                 String lv_hostname_port_string = hostname + ":" + port;
                 String lv_servername_string = ServerName.getServerName(lv_hostname_port_string, 0);
                 ServerName lv_servername = ServerName.parseServerName(lv_servername_string);

                 TransactionRegionLocation loc = new TransactionRegionLocation(regionInfoLoc,
									       lv_servername,
									       0);
                 ts.addRegion(loc);
            }

            private TmAuditTlog getTlog(int clusterToConnect) {
                  TmAuditTlog target = peer_tLogs.get(clusterToConnect);
                   if (target == null) {
                      LOG.error("Tlog object for clusterId: " + clusterToConnect + " is not in the peer_tLogs");
                   }                      
                   return target;
            }

            private HBaseTmZK getTmZK(int clusterToConnect) {
                  HBaseTmZK target = peer_tmZKs.get(clusterToConnect);
                   if (target == null) {
                      LOG.error("HBaseTMZK object for clusterId: " + clusterToConnect + " is not in the peer_tmZKs");
                   }                      
                   return target;
            }
             
            private long getClusterCP(int clusterToConnect, int clustertoRetrieve) throws IOException {
                  long cp = 0;
                  TmAuditTlog target;
                  
                  if (clusterToConnect == pSTRConfig.getTrafClusterIdInt()) target = audit;
                  else target = getTlog(clusterToConnect);
                  try {
                       cp = target.getAuditCP(clustertoRetrieve, pSTRConfig.getTrafInstanceIdInt());
                  }
                 catch (Exception e) {
                       LOG.error("Control point for clusterId: " + clustertoRetrieve + " is not in the table");
                       throw new IOException("Control point for clusterId: " + clustertoRetrieve + " is not in the table, throwing IOException ", e);
                  }
                  return cp;
            }

            private int tlogSync() throws IOException {

                 long localCluster_peerCP, localCluster_localCP, peerCluster_peerCP, peerCluster_localCP = 0;
                 int peer_leader = -2;
                 int peer_count = 0;;
                 boolean tlog_sync_local_needed = false;
                 int synced = 0;
                 
                 // NOTE. R 2.0, doing commit log reload/sync would require an off-line ENV (at least updated transactions are drained and then stopped)
                 // skip tlog sync if ms_env says so

                     if (!msenv_tlog_sync) { // no tlog sync 
                         if(LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                             + pSTRConfig.getTrafClusterIdInt() + " does not perform tlog sync during startup as ms_env indicates");
                         synced = 2;
                         return synced;
                     }
                     else {
                         if(LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                            + pSTRConfig.getTrafClusterIdInt() + " starts to perform tlog sync during startup as ms_env indicates");
                     }

                 // a) check which peer is up from STRConfig and select the most updated as the commit log leader
                 //     (will it be better to have PeerInfo status STR_UP with timestamp, therefore easier to find the oldest cluster)
                 //     At R 2.0, the survival one must be up, and we only support 2 clusters
                 //     Get the other peer's cluster id as the leader, if peer is DOWN, cannot proceed
                 //     Proceed by manually setting ENV variable (TLOG_SYNC = 0)
                 //     For convenience, we use A and B for the two clusters, and A is down and restart while B takes over from A
                 //     for commiting reponsibility

                     peer_count = pSTRConfig.getConfiguredPeerCount();
                     peer_leader = pSTRConfig.getTrafClusterIdInt();
 
                     if (peer_count == 0) { // no peer is configures, skip TLOG sync as configuration indicates
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                            + pSTRConfig.getTrafClusterIdInt() + " has no peer configured from PSTRConfig " + peer_count);
                         synced = 1;
                         return synced;
                     }

                     // R2.0 only works for 1 peer, later the join/reload of a region/table will be extended to peer_count > 1
                     // The peer serves the last term of leader will be used
                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                          + pSTRConfig.getTrafClusterIdInt() + " detect " + peer_count + " peers from STRConfig during startup");
                     for ( Map.Entry<Integer, PeerInfo> entry : pSTRConfig.getPeerInfos().entrySet()) {
                         int peerId = entry.getKey();
                         PeerInfo peerInfo= entry.getValue();
                         if ((peerId != 0) && (peerId != pSTRConfig.getTrafClusterIdInt())) { // peer
                        	 if (peerInfo.isTrafodionUp()) { // at least peer HBase must be up to check TLOG + CP tables
                        	     peer_leader = peerId;
                        	     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                                   + pSTRConfig.getTrafClusterIdInt() + " choose cluster id "
                        	       + peerId + " as the TLOG leader during startup");
                        	 }
                                 else {
                                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                                              + pSTRConfig.getTrafClusterIdInt() + " detect cluster id "
                                              + peerId + " but its Trafodion is DOWN during startup");
                                 }
                         }
                     }

                     if (peer_leader == pSTRConfig.getTrafClusterIdInt()) { // no peer is up to sync_comlpete
                         // proceed w/o consulting with peer (this may cause inconsistency, ~ disk mirror, current down peer may have "commit-takeover"
                         // this is how to recover from consecutive site failures (A down, B takes over, B down, and bring up A ?)
                         // to ensure consistency, once a takeover happens (B), B must be bring up first before A
                         // and both A and B must be up (Trafodion UP) to allow TLOG sync first
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                                 + pSTRConfig.getTrafClusterIdInt() + " can not find any peer available for TLOG sync during startup");
                         synced = 0;
                         return synced;
                     }
                     
                     try { // steps b, c, d

                 // b) if peer is up, read peer GCP-A' (HBase client get)
                 //    if there is no peer, move ahead
                 //    if peer is configured but down, then hold since peer may have more up-to-update commit LOG (~ mirror startup)
                 //    any exception here ? --> down the LDTM initially ??
                 //    probably use static method for the following API?

                     localCluster_peerCP = getClusterCP(pSTRConfig.getTrafClusterIdInt(), peer_leader); 
                     peerCluster_peerCP = getClusterCP(peer_leader, peer_leader);
                     localCluster_localCP = getClusterCP(pSTRConfig.getTrafClusterIdInt(), pSTRConfig.getTrafClusterIdInt());  // CP-A
                     peerCluster_localCP = getClusterCP(peer_leader, pSTRConfig.getTrafClusterIdInt()); // CP-A'

                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " read initial CP during startup for R2.0 ");
                     if (LOG.isDebugEnabled()) LOG.debug("Local cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " CP table has cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " record for CP " + localCluster_localCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Local cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " CP table has cluster " + peer_leader + " record for CP " + localCluster_peerCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Peer cluster " + peer_leader + " CP table has cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " record for CP " + peerCluster_localCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Peer cluster " + peer_leader + " CP table has cluster " + peer_leader + " record for CP " + peerCluster_peerCP);

                 // c) determine role (from A's point of view)
                 //    if CP-A' > CP-A + 2 && GCP-B' >= GCP-B --> peer is the leader
                 //    otherwise there is no commit take ovver by B for A, move on
                 //    Also need to bring txn state records orginiated from B to A (due to active active mode)
                 // *) for later release, this process will become a "consensus join" and a "replay from current leader to reload commit log"

                     if (localCluster_localCP + 2 < peerCluster_localCP) {
                         tlog_sync_local_needed = true;
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " determine to perform tlog sync from peer "
                              + peer_leader + " with sync flag " + tlog_sync_local_needed);
                     }
                     else {
                         tlog_sync_local_needed = false;
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster "
                              + pSTRConfig.getTrafClusterIdInt() + " determine not to perform tlog sync from peer "
                              + peer_leader + " with sync flag " + tlog_sync_local_needed);
                     }
                     
                 // choose a peer is the leader on commit log (TLOG), try to do a reload/sync
                 // R 2.0 does TLOG reload/sync (while transaction processing will be held on B to reload TLOG and data tables)
                 // later an online reload will be supported 

                 // Below example is used to reload A from B after A is restarted
                 // call B_Tlog.getRecordInterval( CP-A) for transactions started at A before crash
                 // when B takes over, it will bump CP-A at B by 5
                 // so if B makes commit decision for transactions A for local regions, the
                 // transcation state in TLOG at B will have asn positioned by updated original CP-A  + 5

                 // In active-active mode, there maybe in-doubt remote branches at A started by B, therefore either
                 //    a) bring all TLOG state records started at B for all transactions with asn larger then Cp-B at A's CP table pointed
                 //    b) ignore this and instead ask B's TLOG (? can this handle consecutive failures)
                 // since A could be down for a while and there may be many transactions started and completed at B while
                 // A is down, it may easier to just ask TLOG at B, only after all the in-doubt branches get resolved (how to
                 // ensure this?, all regions at A must be onlint and started), the active-active is back. In R2.0, the database will get
                 // reloaded offline at A from B, and then the TLOGs will get reset (~ cleanAT), so there is no hurry for R 2.0

                 // API needed:
                 // CP --> getCP value for a particular cluster
                 // CP --> bump CP and write into CP table for a particular TLOG (called after tlog sync completes and before commit takeover)
                 // TLOG --> for a TLOG (implying a particular cluster) get state records started at A with asn < asn from local old CP-A + 2
                 // TLOG --> put record into a TLOG only 

                    if (tlog_sync_local_needed) {      // for transcations started at A, but taken over by B

                       TmAuditTlog leader_Tlog = getTlog(peer_leader);
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM starts TLOG sync from peer leader " + peer_leader);

                       // Since LDTM peer thread will sync all the TLOGs in the cluster, so it has to 
                       // 1) on behalf of all the other TMs (each TM has a TM-TLOG object)
                       // 2) each TM-TLOG will has tlogNumLogs sub-TLOG tables, which each table could contain multiple regionserver
                       // 3) the Audit API will return the commit-migrated txn state records for step 2
                       // 4) The caller has to loop on step 1 to collect all the txn state records from all the TM-TLOGs
                       // get the ASN from local CP, and then retrieve all state records > ASN from leader

                       for (int nodeId = 0; nodeId < my_local_nodecount; nodeId++) { // node number from pSTRConfig for local cluster
                            ArrayList<TransactionState> commit_migrated_txn_list;
                            long starting_asn = audit.getStartingAuditSeqNum(pSTRConfig.getTrafClusterIdInt(), pSTRConfig.getTrafInstanceIdInt()); // TBD need nodeId too in order to access other TM-TLOGs
                            if (LOG.isDebugEnabled()) LOG.debug("LDTM starts TLOG sync from peer leader " + peer_leader + " for node " + nodeId + " ASN " + starting_asn);

                            commit_migrated_txn_list = leader_Tlog.getTransactionStatesFromInterval(pSTRConfig.getTrafClusterIdInt(), nodeId, starting_asn);
                            for (int i = 0; i < commit_migrated_txn_list.size(); i++) {
                                TransactionState ts = commit_migrated_txn_list.get(i);
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM sync TLOG record for tid " + ts.getTransactionId() + " node " + nodeId + 
                                          " status " + ts.getStatusString() + " ASN " + ts.getRecoveryASN() + " from peer leader " + peer_leader);
                                audit.putSingleRecord(ts.getTransactionId(),
                                		ts.getStartId(),
                                		-1,
                                        ts.getTmFlags(),
                                        ts.getStatusString(),
                                		null,
                                		ts.hasRemotePeers(),
                                		true,
                                		ts.getRecoveryASN());
                            }
                       } // loop on TM-TLOG (i.e. node count)

                    } // sync is needed

                    // Do we want to bump again ?
                    // audit.bumpControlPoint(downPeerClusterId, 5);

                   // Is this needed if we need to copy all txn state records started at B after crash from previous step
                   // or to get the lowest asn between (local A-CP and B-CP) and conservatively reload from that lowest position in TLOG
                   // speifically, we may not need to get all the txn state records started at B after A is declared down (so could be bound
                   // by 1-2 CP at most, or by the idServer current sequence number)
                   // in-doubt remote branches at A should ask B (consecutive failures requires both clusters to be up -- no commit
                   // takeover during restart), in R 2.0 since database requires reload offine (active-active), it's better not to do this sync
                   /*
                    if (sync for B is needed) { 
                        list of commit migrated trans state record = getStateRecords(peer_leader, local_localCP);
                        for all the commit migrated transaction,
                            putLocalTLOGStateRecords with with local_localCP // this will be between old GCP and failover GCP
                        
                    }
                    */

                 // d) bump CP-A at all instances after all sync complete, is this needed in R 2.0
                 
                    synced = 1;
                        		
                     } catch (Exception e) { // try block for all the sequential processing on TLOG reload (idempotent if any exception)
                        LOG.error("An ERROR occurred while LDTM " + tmID + " does TLOG sync during startup");
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        LOG.error(sw.toString());
                        synced = 0;
                     }

                 // f) if sync completes without issues, return 1 
                 //     if peer access fails or any exception then should retry later and returns 0

                 return synced;
            }

            public void commit_takeover(int downPeerClusterId) {

                 // if takeover has been perfomed at this instance, don't bump again (how to ensure this if LDTM restarts, it's probably fine to bump CP
                 // even a few times)

                if (commit_migration_clusters.containsKey(downPeerClusterId)) { // this cluster has taken commit migration for the down cluster
                     if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread has taken commit migration from STR down cluster " + downPeerClusterId + " skip CP bump");
                     return;
                }

                // add STR down cluster into commit taken-over cluster map of local cluster
                 commit_migration_clusters.put(downPeerClusterId, 0);
                 if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread starts to take commit migration from STR down cluster " + downPeerClusterId + " and bump CP");

                 if (!msenv_tlog_sync) { // no tlog sync, do not need to put single TLOG
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread does not bump CP during STR down cluster " + downPeerClusterId +
                                   " based on msenv value " + msenv_tlog_sync);
                       return;
                 }

                 // get the number of nodes from the downed cluster in order to get the number of TLOG configured (from pSTRConfig is better)

                 // TBD Need to loop on every TM-TLOG for each peer bump
                 // bump control point by 5 for downPeerClusterId CP record to recognize a commit takeover happened
                 // by committing decision made later for transcations reginiated from the down instance could be detected
 
                 // First bump local cluster (for number of TLOGs), and then go through the peer_tLogs
                  try {
                       audit.bumpControlPoint(downPeerClusterId, pSTRConfig.getTrafInstanceIdInt(), 5); // TBD need to pass nodeId
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM bumps CP at local cluster for STR down cluster " + downPeerClusterId);
                  }
                  catch (Exception e) {
                       LOG.error("LDTM encounters errors while tries to bump CP at local cluster for STR down cluster " + downPeerClusterId +
                           " audit.commit_takeover_bumpCP: EXCEPTION ", e);
                  }

                  for (Entry<Integer, TmAuditTlog> lv_tLog_entry : peer_tLogs.entrySet()) {
                      Integer clusterid = lv_tLog_entry.getKey();
                      TmAuditTlog lv_tLog = lv_tLog_entry.getValue();
                      try {
                            if (clusterid != downPeerClusterId) {
                                lv_tLog.bumpControlPoint(downPeerClusterId, pSTRConfig.getTrafInstanceIdInt(), 5); // TBD need to pass nodeId
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM bumps CP at cluster " + clusterid + " for STR down cluster " + downPeerClusterId);
                            }
                            else {
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM skips to bump CP at downed cluster " + clusterid + " for STR down cluster " + downPeerClusterId);
                            }
                       }
                       catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to bump CP at cluster " + clusterid + " for STR down cluster " + downPeerClusterId +
                                " tLog.commit_takeover_bumpCP: EXCEPTION ", e);
                        }
                  }
                  return;
            }

            public void put_single_tlog_record_during_commit_takeover(int downPeerClusterId, long tid, TransactionState ts) {

                   if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " during recovery after commit takeover ");
                   if (!msenv_tlog_sync) { // no tlog sync, do not need to put single TLOG
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM does not write txn state record for txid " + tid +
                                    " during recovery after commit takeover based on msenv value" + msenv_tlog_sync);
                       return;
                   }

                   try { // TBD temporarily put 0 (for ABORTED)  in asn to force the Audit modeule picking the nodeid from tid to address which TLOG
                         audit.putSingleRecord(tid,
                        		 ts.getStartId(),
                        		 -1,
                                 ts.getTmFlags(),
                                 TransState.STATE_RECOVERY_ABORT.toString(),
                                 ts.getTableAttr(),
                        		 ts.hasRemotePeers(),
                        		 true,
                                 0 /* Recovery ASN*/);
                         if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " at local cluster during recovery after commit takeover ");
                   }
                   catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to put single tlog record at local cluster for tid " + tid + " for STR down cluster " + downPeerClusterId +
                                " audit.put_single_tlog_record_during_commit_takeover: EXCEPTION ", e);
                   }

                  for (Entry<Integer, TmAuditTlog> lv_tLog_entry : peer_tLogs.entrySet()) {
                      Integer clusterid = lv_tLog_entry.getKey();
                      TmAuditTlog lv_tLog = lv_tLog_entry.getValue();
                      try {
                            if (clusterid != downPeerClusterId) {
                                lv_tLog.putSingleRecord(tid, ts.getStartId(), -1, ts.getTmFlags(), TransState.STATE_RECOVERY_ABORT.toString(), ts.getTableAttr(), ts.hasRemotePeers(), true, 0 /* Recovery ASN*/);
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " to cluster " + clusterid + " during recovery after commit takeover ");
                            }
                            else {
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM skip txn state record for txid " + tid + " to STR down cluster " + clusterid + " during recovery after commit takeover ");
                            }
                       }
                       catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to put single tlog record for tid " + tid + " for STR down cluster " + downPeerClusterId +
                                " tLog.put_single_tlog_record_during_commit_takeover: EXCEPTION ", e);
                        }
                  }
                  return;
            }

    @Override
    public void run() {
        
        //Start of recovery thread could be due to only two scenarios.
        //1. TM coming up as part of node up. In this case recovery thread 
        //   assigned node id is the same as own node.
        //2. As part of TM going down, LDTM starting a new recovery thread
        //   corresponding to the node id of TM that just went down. In this
        //   case, recovery thread node id is that of remote node that went down.
        //
        //In both scenarios, there may be DDL only recovery or DDL/DML region
        //recovery. DDL/DML region recovery is driven from the affected regions.
        //In the below loop, first DDL only recovery is checked first. This check
        //is performed only once in the beginning. Rest of the recovery is
        //is followed by DDL DML recovery check .
      
             int sync_complete = 0;
             boolean LDTM_ready = false;
             long time0 = System.currentTimeMillis();
             long time1, time2, time3, time4, time5, time6, time7, time8;
             long timerCount = 0;

                if (this.leadtm) { // this is LDTM peer recovery thread, first if this is a startup, drive a TLOG sync
                   try {
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to delete TLOG sync node during startup");
                        zookeeper.deleteTLOGSyncNode();
                   } catch (Exception e) {
                          LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to reset TLOG sync 0 during startup");
                          StringWriter sw = new StringWriter();
                          PrintWriter pw = new PrintWriter(sw);
                          e.printStackTrace(pw);
                          LOG.error(sw.toString());
                   }
                   
                   try {
                        String data = "LDTM";
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to create LDTM ezNode during startup");
                        zookeeper.createLDTMezNode(data.getBytes());
                   } catch (Exception e) {
                       LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread create LDTM ezNode during startup");
                       StringWriter sw = new StringWriter();
                       PrintWriter pw = new PrintWriter(sw);
                       e.printStackTrace(pw);
                       LOG.error(sw.toString());
                   }
                   
                   while (sync_complete <= 0) {
                	   try {
                               if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to do TLOG sync during startup");
                               sync_complete = tlogSync();
                               if (sync_complete <= 0) Thread.sleep(1000); // wait for 1 seconds to retry
                       } catch (Exception e) {
                           LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to do TLOG sync during startup" + sync_complete);
                           StringWriter sw = new StringWriter();
                           PrintWriter pw = new PrintWriter(sw);
                           e.printStackTrace(pw);
                           LOG.error(sw.toString());
                       }
                   }
                   
                   try {
                        String data = "1";
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to create TLOG sync node during startup");
                        zookeeper.createTLOGSyncNode(data.getBytes());
                   } catch (Exception e) {
                       LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to set TLOG sync 1 during startup");
                       StringWriter sw = new StringWriter();
                       PrintWriter pw = new PrintWriter(sw);
                       e.printStackTrace(pw);
                       LOG.error(sw.toString());
                   }
                }
                else { //regular recovery thread will wait for the creation for LDTM ezNode
                       while (! LDTM_ready) {
                	    try {
                                if (LOG.isDebugEnabled()) LOG.debug("DTM " + tmID + " standard recovery thread tries to check if LDTM ready during startup");
                                LDTM_ready = zookeeper.isLDTMezNodeCreated();
                                if (!LDTM_ready) Thread.sleep(2000); // wait for 1 seconds to retry
                            } catch (Exception e) {
                                LOG.error("An error occurred while DTM " + tmID + " recovery thread is waiting for LDTM ezNode during startup" + LDTM_ready + " ", e);
                            }
                       } // wait for LDTM ready
                }

                // All recovery threads has to wait until commit log (TLOG) sync completes
                
                boolean tlogReady = false;
                while (! tlogReady) {
                	try {
                             if (LOG.isDebugEnabled()) LOG.debug("DTM " + tmID + " recovery thread tries to check if local TLOG ready during startup");
                	     tlogReady = zookeeper.isTLOGReady();
                             if (!tlogReady) Thread.sleep(5000); // wait for 5 seconds to retry
                    } catch (Exception e) {
                        LOG.error("An ERROR occurred while local recovery thread tmID " + tmID + " tries to check if TLOG is ready during startup ");
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        LOG.error(sw.toString());
                    }               	
                }

		int peerid;
                if (pSTRConfig.getConfiguredPeerCount() > 0) { // has peer confiogured, get its peer id
                         // peerid = peerId_list.get(0); // retrieve 1st peer, limited case in R 2.0
                         peerid = pSTRConfig.getFirstRemotePeerId(); // NARI XDC POC
                }
                else {
                          peerid = -1; 
                }
                if (tmID == -2) {
                   if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD at local cluster in LDTM "
                       + pSTRConfig.getTrafClusterIdInt() + " for DTM " + tmID + " dedicated first peer id is "
                       + peerid + " starts after TLOG synced");
                }
                else {
                   if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD at local cluster "
                       + pSTRConfig.getTrafClusterIdInt() + " for DTM " + tmID + " starts after TLOG synced");
                }

                while (this.continueThread) {
                    try {
                        skipSleep = false;
                        Map<String, byte[]> regions = null;
                        Map<String, byte[]> regionsHavingLocks = null;
                        Map<Long, TransactionState> transactionStates = null;
                        boolean loopBack = false;
                        time1 = System.currentTimeMillis();
                        if(ddlOnlyRecoveryCheck)
                        {
                          ddlOnlyRecoveryCheck = false;
                          transactionStates = getTransactionsFromTmDDL();
                          if(transactionStates != null)
                            recoverTransactions(transactionStates);
                        }
                        time2 = System.currentTimeMillis();
                        int loopc = 0;
                        do {
                           try {
                               loopBack = false;
                               regions = zookeeper.checkForRecovery();
                           } catch(InterruptedException ie) {
                              loopc++;
                              LOG.warn("TRAF RCOV: " + this.tmID + " Interrupted in checkForRecovery, sleep for 1 sec and retry " + loopc);
                              // sleep for a second else the log got (very) quickly filled up with the above
                              // error when the ZK was not available.
                              try { Thread.sleep(1000);} catch(InterruptedException ie_again) {};
                              loopBack = true;
                           } catch (KeeperException ze) {
                              loopc++;
                              if (loopc > 55) {
                                       LOG.error("TRAF RCOV: " + tmID + " checkForRecovery has keeperException after " +
                                                                                      loopc + " retries, exit and restart!! ");
                                       System.exit(4);
                              }
                              int sleep_time = (loopc < 6) ? 3000 : 10000;
                              LOG.warn("TRAF RCOV: " + tmID + " checkForRecovery keeperEXception, sleep " + sleep_time/1000 + " second and retry " + loopc);
                              try { Thread.sleep(sleep_time);} catch(InterruptedException ie) {};
                              loopBack = true;
                           }
                        } while (loopBack);

                        time3 = System.currentTimeMillis();
                        if(regions != null) {
                            skipSleep = true;
                            recoveryIterations++;
                            transactionStates = getTransactionsFromRegions(regions);
                            time4 = System.currentTimeMillis();                           
                            if(transactionStates != null)
                                recoverTransactions(transactionStates);

                        } // region not null
                        else {
                            if (recoveryIterations > 0) {
                                if(LOG.isDebugEnabled()) LOG.debug("Recovery completed for TM" + tmID);
                            }
                            recoveryIterations = -1;
                            time4 = System.currentTimeMillis();   
                        }
                        processTlogFromPeerReq();
                        time5 = System.currentTimeMillis();

                        try {
                            if (false == this.nonnativeThread || (time5 - time0) > recoveryWaitingTime)
                                regionsHavingLocks = zookeeper.checkForLockRelease();
                        } catch(InterruptedException ie) {
                            skipSleep = true;
                            regionsHavingLocks = null;
                            LOG.error("Error when calling checkForLockRelease: ", ie); 
                        } catch (KeeperException ze) {
                            skipSleep = true;
                            regionsHavingLocks = null;
                            LOG.error("Error when calling checkForLockRelease: ", ze);
                        }
                        time6 = System.currentTimeMillis();
                        if (regionsHavingLocks != null) {
                           skipSleep = true;
                           releaseLocksFromRegion(regionsHavingLocks);
                        }

                        time7 = System.currentTimeMillis();
                        try {
                            if(continueThread) {
                                if (!skipSleep) {
                                   Thread.sleep(sleepTimeInt);
                                }
                            }
                            retryCount = 0;
                        } catch (InterruptedException e) {
                            LOG.error("Error in recoveryThread: ", e);
                        }
                        time8 = System.currentTimeMillis();
                        if ((timerCount++ % timerCountPrint) == 0) {   // default 200, ~ 3 min
                          if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD operational heartbeat " +
                                      " zone1, " + (time2-time1) + " zone2, " + (time3-time2) +
                                      " zone3, " + (time4-time3) + " zone4, " + (time5-time4) +
                                      " zone5, " + (time6-time5) + " zone6, " + (time7-time6) +
                                      " zone7, " + (time8-time7) +
                                      " count, " + timerCount + " time8, " + time8 + " print interval, " + timerCountPrint); 
                        }
                    } catch (IOException e) {
                        LOG.error("TRAF RCOV: " + tmID + " Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, e);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("TRAF RCOV: " + tmID + " Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY);
                        } catch(InterruptedException se) {
                        }
                    } catch (KeeperException ze) {
                        LOG.error("TRAF RCOV: " + tmID + " Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, ze);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("TRAF RCOV: " + tmID + " Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY);
                        } catch(InterruptedException se) {
                        }
                    } catch (DeserializationException de) {
                        LOG.error("TRAF RCOV: " + tmID + " Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, de);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("TRAF RCOV: " + tmID + " Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY);
                        } catch(InterruptedException se) {
                        }
                    } catch (Throwable tr) {
                        //catch Throwable in case RecoveryThread exit silently with unknown exception
                        LOG.error("TRAF RCOV: " + tmID + " Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, tr);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("TRAF RCOV: " + tmID + " Recovery thread failure, aborting process");
                            System.exit(4);
                        }
                        try {
                            Thread.sleep(SLEEP_DELAY);
                        } catch(InterruptedException se) {
                        }
                    }
                }
                if(LOG.isDebugEnabled()) LOG.debug("Exiting recovery thread for tm ID: " + tmID);
            }

    private void queuePeerTlogReq(long tid) {

        // Option: only LDTM need this (TmID = -2), now will create peer HBaseTMZK at all TMs
        // peer HBaseTmZK created in init, like peer TmAuditTlog
        // call createPeerTxnZNode
        // tolerate exception
        // the originator TM (not necessray LDTM) will pick up from /trafodion/recovery/TMnn/remote
        //       then it will resolve it (but will not do phase2 -- we don;t add region in the ts for RT
        //       the resolution will come in the peer LDTM -- it will get a resolution (exit from NO-TX NO-TX case)
        // Txn committing is done in originator TM, or in takeover mode (either LDTM, or taking-over DC after peer down

        int peer = (int) TransactionState.getClusterId(tid);

        HBaseTmZK peer_ZK = getTmZK(peer);
        if (peer_ZK == null) {
             LOG.error("TRAF RCOV: TM " + tmID + " PEER THREAD queuePeerTlogReq for peer " + peer +
                       " trans id " + tid + " get null HBaseTmZK in peer_tmZKs");
             return;
        }
        try {       
             peer_ZK.createPeerTxnZNode(tid);
        } catch (IOException ioe) {
             LOG.error("TRAF RCOV: " + tmID + " PEER THREAD queuePeerTlogReq " + peer + 
                       " trans id " + tid + " hit IOException in createPeerTxnZNode ", ioe);
        }
        if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: TM " + tmID + " PEER THREAD queuePeerTlogReq for peer " +
                  peer + " trans id " + tid + " create peer transaction tlog request successfully");
    }

    private  void processTlogFromPeerReq()
    {
        List<Long> indoubtTransactions = null;
        TransactionState ts = null;
        Map<Long, TransactionState> transactionStates = null;
        if(LOG.isTraceEnabled()) LOG.trace("TRAF RCOV: " + tmID + " PEER THREAD processTlogFromPeerReq ");
        try {
           indoubtTransactions = zookeeper.checkForPeerRecovery();
           if ((indoubtTransactions != null) && (indoubtTransactions.size() > 0)) {
              transactionStates = new HashMap<Long, TransactionState>();
              for (Long txid : indoubtTransactions) {
                  ts = transactionStates.get(txid);
                  if (ts == null) {
                       ts = new TransactionState(txid);
                       if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: TM " + tmID + " THREAD: writeTlogFromPeerReq, new ts for transaction " +
                                     + txid.longValue());
                  }
                  ts.setWriteTlogOnly(true);
                  if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: writeTlogFromPeerReq, add into existing ts for in-doubt transaction " +
                                   + txid.longValue() + " writeTlogOnly " + ts.getWriteTlogOnly());
                  transactionStates.put(txid, ts); // ts should have empty participating region, so it only writes trans state, but no phase 2
              }
              recoverTransactions(transactionStates);
            } // some peer transactions
         } catch (Exception ex) {
            if (LOG.isWarnEnabled()) LOG.warn("TRAF RCOV: " + tmID + " THREAD: writeTlogFromPeerReq hit exception ", ex);
         }
    }
            
    private  Map<Long, TransactionState> getTransactionsFromRegions(
                           Map<String, byte[]> regions)
                           throws IOException, KeeperException,
                           DeserializationException
    {
        if (regions.size() > 0){
           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: processing in-doubt regions with size " + regions.size());
        }
        Map<Long, TransactionState> transactionStates = new HashMap<Long, TransactionState>();
        for (Map.Entry<String, byte[]> regionEntry : regions.entrySet()) {

            List<Long> TxRecoverList = new ArrayList<Long>();
            String hostnamePort = regionEntry.getKey();
            byte[] regionBytes = regionEntry.getValue();
            if (LOG.isInfoEnabled())
                LOG.info("TRAF RCOV: " + tmID + " THREAD: Processing in-doubt region: " + new String(regionBytes));
                  //  Let's get the host name
            final byte [] delimiter = ",".getBytes();
            String[] hostname = hostnamePort.split(new String(delimiter), 3);
            if (hostname.length < 2) {
                   throw new IllegalArgumentException("hostnamePort format is incorrect");
            }
            LOG.info ("TRAF RCOV: " + tmID + " THREAD: Starting recovery request, region hostname and region id: " + hostnamePort +
                           " Recovery iterations: " + recoveryIterations);

            try {
                TxRecoverList = txnManager.recoveryRequest(rt_connection, hostnamePort, regionBytes, tmID);
            }
            catch (IOException e) {
               // For all cases of Exception, we rely on the region to redrive the request.
               // Likely there is nothing to recover, due to a stale region entry, but it is always safe to redrive.
               // We log a warning event and delete the ZKNode entry.
               LOG.warn("TRAF RCOV: " + tmID + " THREAD:Exception calling txnManager.recoveryRequest. " + "TM: " +
                          tmID + " regionBytes: [" + regionBytes + "].  Deleting zookeeper region entry. \n exception: ", e);
               // reset rt_connection 
               rt_connection.close();
               rt_connection = ConnectionFactory.createConnection(config);
               zookeeper.deleteRegionEntry(regionEntry);
               
               LOG.info("TRAF RCOV: " + tmID + " recreate rt connection in IOException, " + rt_connection);
 
/*             // for the aging issue on region comes back very late, use TLOG RECOVERY_STATE state record
               // In the case of NotServingRegionException we will repost the ZKNode after refreshing the table.
               if ((e instanceof NotServingRegionException) || (e.getCause() instanceof NotServingRegionException)){
                   // Create a local HTable object using the regionInfo
                   HTable table = new HTable(config, HRegionInfo.parseFrom(regionBytes).getTable().getNameAsString());
                   // Repost a zookeeper entry for all current regions in the table
                   zookeeper.postAllRegionEntries(table);
               }
*/
            } // IOException
  
            if (TxRecoverList != null) {
                if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: tmID " + tmID + " size of TxRecoverList " + TxRecoverList.size()
                               + " from in-doubt region " + new String(regionBytes));

                if (TxRecoverList.size() == 0) {
                     // First delete the zookeeper entry
                     LOG.debug("TRAF RCOV: " + tmID + " THREAD:Leftover Znode, 0 in-doubt txn calling txnManager.recoveryRequest. " + "TM: " +
                             tmID + " regionBytes: [" + regionBytes + "].  Deleting zookeeper region entry. ");
                     zookeeper.deleteRegionEntry(regionEntry);
               }
               for (Long txid : TxRecoverList) {
                  TransactionState ts = transactionStates.get(txid);
                  if (ts == null) {
                     ts = new TransactionState(txid);

                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: new ts for in-doubt transaction " +
                                   + txid.longValue() + " from in-doubt region " + new String(regionBytes));
                     //Identify if DDL is part of this transaction and valid
                     TmDDL tmDDL = hbtx.getTmDDL();
                     StringBuilder state = new StringBuilder ();
                     tmDDL.getState(txid,state);
                     if(state.toString().equals("VALID")){
                        ts.setDDLTx(true);
                     }
                  }
                  else {
                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: add into existing ts for in-doubt transaction " +
                                   + txid.longValue() + " from in-doubt region " + new String(regionBytes));
                  }

                  this.addRegionToTS(hostnamePort, regionBytes, ts);
                  transactionStates.put(txid, ts);
               }
            }
            else{
               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: TxRecoverList is NULL ");
            }
        }
        if (transactionStates.size() > 0){
           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: total in-doubt transaction from recovery request "
                            + transactionStates.size() + " for this in-doubt region batch with size " + regions.size());
        }
        return transactionStates;
    }

    private void releaseLocksFromRegion(Map<String, byte[]> regionsHavingLock) //throws IOException, KeeperException, DeserializationException
    {
        if (regionsHavingLock.size() > 0) {
            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: release locks from regions with size " + regionsHavingLock.size());
        }
        for (Map.Entry<String, byte[]> regionEntry : regionsHavingLock.entrySet()) {
            String keyInfo = regionEntry.getKey();
            byte[] regionInfo = regionEntry.getValue();
            int regionSize = (regionInfo[3] & 0xFF) + ((regionInfo[2] & 0xFF) << 8) + ((regionInfo[1] & 0xFF) << 16) + ((regionInfo[0] & 0xFF) << 24);
            if (regionSize <= 0 || regionSize >= regionInfo.length) {
                LOG.error("TRAF RCOV: releaseLocksFromRegion region encoded name:" + keyInfo + " region size: " + regionSize + " total size: "  + regionInfo.length);
                continue;
            }
            byte[] regionArray = Arrays.copyOfRange(regionInfo, 4, regionSize); 
            byte[] tidBytes = Arrays.copyOfRange(regionInfo, regionSize + 4, regionInfo.length);
            String tidString = new String(tidBytes);
            LOG.info("TRAF RCOV: releaseLocksFromRegion region encoded name:" + keyInfo + " and trans ids:"  + tidString);
            String tidArray[] = tidString.split(",");
            List<Long> tidList = new ArrayList<Long>();

            for (String tidStr : tidArray) {
                Long txId = Long.valueOf(tidStr.trim());
                if (!this.nonnativeThread && HBaseTxClient.mapTransactionStates.get(txId) != null)
                    continue;
                tidList.add(txId);
            }

            if (tidList.size() > 0) {
                try {
                    txnManager.releaseRowLock(rt_connection, regionArray, tidList);
                } catch(Exception ex) {
                    LOG.error("TRAF RCOV: " + tmID + " THREAD: fails to release locks from region: " + keyInfo, ex);
                    try {
                        rt_connection.close();
                        rt_connection = ConnectionFactory.createConnection(config);
                    } catch(Exception connEx) {
                        LOG.error("TRAF RCOV: " + tmID + " THREAD: fails to reestablish connection in releaseLocksFromRegion.", connEx);
                    }
                } 
            }
        }
    }
              
    private  Map<Long, TransactionState> getTransactionsFromTmDDL()
                                                throws IOException
    {
      if (LOG.isDebugEnabled()) LOG.debug("TRAF RCOV: " + tmID + " THREAD: Checking for DDL only recovery");
    
      Map<Long, TransactionState> transactionStates = null;
      TmDDL tmDDL = hbtx.getTmDDL();
      List<Long> txIdList  = tmDDL.getTxIdList(tmID);
      
      //This list of txID is specific to tmID owner.
      //This list may include txId that are:
      //1. currently in ACTIVE state. RecoverTransactions() call takes care of
      //ignoring TxId which are currently actively in progress.
      //2. Txids regions which have not yet requested for help(regions requesting help
      //from zookeeper) , probably will, could be timing. 
      //3. Txids regions which have already requested for help.
      //4. Txids whose regions have already serviced, but only require recovery
      //from DDL perspective.
      //For 2 and 3 use cases above, those regions will ultimately seek help if
      //they need help. So no need to handle those regions here. We are only
      //interested to handle use case 4. If usecase 4 also involves DML regions
      //it is ok to recover the DDL only here and not dependent on DML regions.
      //
      //Note that recoverTransactions() attempts recovery, its a no-op if those
      //txids are completed for some reason, some of the regions might have completed
      //processing, ignoreUnknownTransactionException is enabled.
      if(txIdList != null && txIdList.size() > 0)
      {
        transactionStates = new HashMap<Long, TransactionState>();
        for (Long txid : txIdList)
        {
          //build ts object
          TransactionState  ts = new TransactionState(txid);
          ts.setDDLTx(true);
          transactionStates.put(txid, ts);
        }
        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: Checking for DDL only recovery with trand size " +
                                                            transactionStates.size());
      }
      return transactionStates;
    }

    public enum CommitPath {    UNKNOWN,
                                ORIGINATOR_ONLY,     // 1 use originator TLOG, no remote participant
                                BOTH_CONSENSUS,      // 2 use both for consensus
                                DEFER_RESOLUTION,    // 3 use defer resolution
                                ORIGINATOR_STR_DOWN, // 4 use local originator TLOG due to peer STR_DOWN
                                REPLICA_STR_DOWN,    // 5 use local replica TLOG due to originator STR_DOWN -- takeover
                                STILL_ALIVE;         // 6 transaction is still alive and in memory,
                                                     //   defer resolution, committing process (ph1 to ph2) takes too long
    };

    private void recoverTransactions(Map<Long, TransactionState> transactionStates) throws IOException
    {
        if (transactionStates.size() > 0){
           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: in-doubt transaction size " + transactionStates.size());
        }
        for (Map.Entry<Long, TransactionState> tsEntry: transactionStates.entrySet()) {
            int isTransactionStillAlive = 0;
            TransactionState ts1 = null;
            TransactionState ts = tsEntry.getValue();
            Long txID = ts.getTransactionId();
            int clusterid = (int)ts.getClusterId();
            TransactionState ts_p = new TransactionState(txID);
            TmAuditTlog peerTlog = null;
            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: processing in-doubt transaction txID " + txID);

            /*
            short commitPath = 0; // 1 use originator TLOG, no remote participant
                                  // 2 use both for consensus
                                  // 3 use defer resolution
                                  // 4 use local originator TLOG due to peer STR_DOWN
                                  // 5 use local replica TLOG due to originator STR_DOWN -- takeover
                                  // 6 transaction is still alive and in memory, defer resolution, committing process (ph1 to ph2) takes too long ...
            */
            CommitPath commitPath = CommitPath.UNKNOWN;

            int peerid;
            if (pSTRConfig.getConfiguredPeerCount() > 0) { // has peer confiogured, get its peer id
                // peerid = peerId_list.get(0); // retrieve 1st peer, limited case in R 2.0
                peerid = pSTRConfig.getFirstRemotePeerId(); // NARI XDC POC
            }
            else {
                peerid = -1;
            }

            // It is possible for long prepare situations that involve multiple DDL
            // operations, multiple prompts from RS is received. Hence check to see if there
            // is a TS object in main TS list and transaction is still active.
            // Note that tsEntry is local TS object.
            if (HBaseTxClient.mapTransactionStates.get(txID) != null) {
                if (HBaseTxClient.mapTransactionStates.get(txID).getStatus().toString().contains("ACTIVE")
                    || HBaseTxClient.mapTransactionStates.get(txID).getStatus().toString().contains("COMMITTING")
                    || HBaseTxClient.mapTransactionStates.get(txID).getStatus() == TransState.STATE_COMMITTED) {
                    isTransactionStillAlive = 1;
                }
                if (LOG.isInfoEnabled()) 
                    LOG.info("TRAF RCOV: " + tmID + " THREAD: txID " + txID
                            + " still has TS object in TM memory. TS details: "
                            + HBaseTxClient.mapTransactionStates.get(txID).toString()
                            + " transactionAlive: " + isTransactionStillAlive);
                if(isTransactionStillAlive == 1)
                    continue; //for loop
            }
           
                                try {
                                   // For transactions started by local cluster, commit processing has to wait if it can't get enough quorum during commit log write
                                   // In a 2-cluster xDC, that implies the local cluster must own the quorum (or authority) before move into phase 2 (i.e. do the commit
                                   // decision without peer's vote)
                                   if (   ((clusterid == 1) && (pSTRConfig.getConfiguredPeerCount() == 0)) || 
                                           (clusterid == pSTRConfig.getTrafClusterIdInt())   ) { // transactions started by local cluster
                                        // CASE 1: ts & audit is the originator, ts_p & peerTlog is the replica
                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                        		+ " resolution : case 1 : local is commit originator, local cluster id " +
                                                        pSTRConfig.getTrafClusterIdInt() + " peer count " + pSTRConfig.getConfiguredPeerCount());

                                        // Check if the transaction (started locally) is still owned by a txn thread
                                        if (HBaseTxClient.getMap().get(txID) != null) {
                                            if (HBaseTxClient.getMap().get(txID).getStatusString().equals(TransState.STATE_ACTIVE.toString())
                                                || HBaseTxClient.getMap().get(txID).getStatusString().equals(TransState.STATE_COMMITTING.toString())
                                                || HBaseTxClient.getMap().get(txID).getStatus() == TransState.STATE_COMMITTED) {
                                                isTransactionStillAlive = 1;
                                            }
                                            if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                    + " still has ts object in DTM memory with state "
                                                    + HBaseTxClient.getMap().get(txID).getStatusString() + " transactionAlive " + isTransactionStillAlive);
                                        }
                                        else {
                                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID + " no ts object in DTM memory ");
                                        }

                                        ts1 = new TransactionState(txID);
                                        audit.getTransactionState(ts1, true); // acquire local TLOG state record
                                        boolean saveState = false;
                                        //The following recovery logic (fix M-17857) are not taken into account in sync xdc scenario.
                                        if (this.nonnativeThread && trxNoTXMap != null && trxTXState != null) {
                                            TransState currState = ts1.getStatus();
                                            if (TransState.STATE_NOTX == currState || TransState.STATE_BAD == currState) {
                                                Long beginTime = trxNoTXMap.get(txID);
                                                Long curTime = System.currentTimeMillis();
                                                if (false == trxTXState.containsKey(txID)) {
                                                    if (beginTime == null) {
                                                        if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID 
                                                               + " need to wait " + recoveryWaitingTime + " milliseconds");
                                                        trxNoTXMap.put(txID, curTime);
                                                        continue;
                                                    } else if (curTime - beginTime < recoveryWaitingTime) {
                                                        if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                               + " do not exceed the time limit and still wait");
                                                        continue;
                                                    } else {
                                                        saveState = true;
                                                    }
                                                }
                                            } else {
                                                trxNoTXMap.remove(txID);
                                            }
                                        }

                                        if ((! ts1.getStatusString().contains("RECOVERY")) && (ts1.getStatusString().contains("COMMITTED"))
                                                && (!ts1.hasRemotePeers())){
                                            // Write a RECOVERY_COMMIT record to TLOG to prevent aging
                                            long nextAsn = audit.getNextAuditSeqNum((int)ts1.getClusterId(), (int)ts1.getInstanceId(), (int)ts1.getNodeId());
                                            audit.putSingleRecord(txID,
                                            		          ts1.getStartId(),
                                            		          ts1.getCommitId(),
                                                                  ts1.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                  ts1.getTableAttr(),
                                                                  ts1.hasRemotePeers(),
                                                                  true,
                                                                  nextAsn);
                                        }
                                        audit.getTransactionState(ts, false);
                                        if (this.nonnativeThread && trxTXState != null) {
                                            TransState txState = ts.getStatus();
                                            if (saveState) {
                                               if (txState == TransState.STATE_NOTX) {
                                                   if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID 
                                                       + " change state from STATE_NOTX to STATE_ABORTED and put it to TLOG");
                                                   long nextAsn = audit.getNextAuditSeqNum((int)ts1.getClusterId(), (int)ts1.getInstanceId(), (int)ts1.getNodeId());
                                                   txState = TransState.STATE_ABORTED;
                                                   audit.putSingleRecord(txID,
                                                                         ts1.getStartId(),
                                                                         ts1.getCommitId(),
                                                                         ts1.getTmFlags(),
                                                                         txState.toString(),
                                                                         ts1.getTableAttr(),
                                                                         ts1.hasRemotePeers(),
                                                                         forceForgotten,
                                                                         nextAsn);
                                               }
                                               if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                   + " exceed the time limit and put state: " + txState.toString() + " to map");
                                               ts.setStatus(txState);
                                               trxTXState.put(txID, txState);
                                            } else if (trxTXState.containsKey(txID)) {
                                               TransState savedState = trxTXState.get(txID);
                                               if (txState != savedState) {
                                                   LOG.warn("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID + " the latest state ("
                                                       + ts.getStatusString() + ") is not same with save state(" + savedState.toString() + ")");
                                                   ts.setStatus(savedState);
                                               }
                                            } 
                                        }

                                        if (isTransactionStillAlive == 1) {
                                           commitPath = CommitPath.STILL_ALIVE;
                                           if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                            		+ " still has ts object in DTM memory with state "
                                                    + HBaseTxClient.getMap().get(txID).getStatusString());
                                        }
                                        else if (peerid == -1) { // no peer is configured, single instance case
                                            // audit.getTransactionState(ts, true);
                                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID + " has no peer configured " + peerid
                                                    + " ,commit authority is handled by local owner " + clusterid
                                                    + " with ts status " + ts.getStatusString());
                                            commitPath = CommitPath.ORIGINATOR_ONLY;
                                        }
                                        else if ((!ts.getStatusString().contains("NOTX")) && (!ts.hasRemotePeers())) { // only has local participant (no STR peer region)
                                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                    + " has no remote participants, commit authority is handled by local owner "
                                                    + clusterid + " with ts status " + ts.getStatusString());
                                            commitPath = CommitPath.ORIGINATOR_ONLY;
                                        }
                                        else if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN) { // sdn transaction in 2 instance XDC case, "> 2 instances" TBD
                                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                    + " is SDN type transaction, commit authority is handled by local owner "
                                                    + clusterid + " with ts status " + ts.getStatusString());
                                            commitPath = CommitPath.ORIGINATOR_ONLY;
                                        }
                                        else { // has peer participant as TS (from TLOG) indicates
                                           if (peerid != -1) {
                                              // has peer configured from PSTR, only handle 2 instance XDC now ---- "> 2 instances" TBD
                                              if (pSTRConfig.getPeerStatus(peerid).contains(PeerInfo.STR_DOWN)) {
                                                 // peer STR is down, do commit takeover based on local TLOG
                                                 // TLOG consensus will be determined by local for 2 instance XDC case
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                		 + " commit authority is handled locally at cluster " + clusterid
                                                		 + " due to STR_DOWN at peer " + peerid);
                                                 commitPath = CommitPath.ORIGINATOR_STR_DOWN;
                                              }
                                              else if (pSTRConfig.getPeerStatus(peerid).contains(PeerInfo.STR_UP)) {
                                                 // STR is up, TLOG consensus is needed (check peer TLOG)
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                                		 + " check Peer due to STR_UP at peer " + peerid);
                                                 commitPath = CommitPath.BOTH_CONSENSUS;
                                                 try {
                                                    peerTlog = getTlog(peerid);
                                                    peerTlog.getTransactionState(ts_p, false);
                                                    // get peer TLOG ts state record only, but need to do consensus, don't just directly overwrite
                                                    // since this txn has peer participant, so must be a SUP type txn
                                                    // leave resolution and write any updated TLOG state record in path 2 code
/*                                                    if ((! ts_p.getStatusString().contains("RECOVERY")) &&  (ts_p.getStatusString().contains("COMMITTED"))){
                                                    	// Write a RECOVERY_COMMIT record to TLOG to prevent aging
                                                        long nextAsn = audit.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getNodeId());
                                                        peerTlog.putSingleRecord(txID, 
                                                        		ts_p.getStartId(),
                                                        		ts_p.getCommitId(),
                                                                ts.getTmFlags(),
                                                                TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                null,
                                                                ts_p.hasRemotePeers(),
                                                                forceForgotten,
                                                                nextAsn);                                        		
                                                    }
*/
                                                 } catch (Exception e2) {
                                                    LOG.warn("TRAF RCOV: " + tmID + " getTransactionState from Peer " + peerid + " for tid "
                                                             + ts.getTransactionId() + "  in SUP state hit Exception2, resolution deferred, ", e2);
                                                    commitPath = CommitPath.DEFER_RESOLUTION;
                                                 }
                                              }
                                              else {
                                                 LOG.error("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID +
                                                		 " commit originator status is unknown, neither STR_UP or STR_DOWN "
                                                		 + clusterid);
                                                 commitPath = CommitPath.DEFER_RESOLUTION;
                                              } // peer status
                                           } // has legit peer configured, peerid != -1
                                           else { // ts indicates has peer but there is no peer config -- internal error
                                              LOG.error("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                            		  + " ts has peer participants, but no peer configured " + peerid
                                            		  + ", commit authority is handled by local owner " + clusterid);
                                              commitPath = CommitPath.ORIGINATOR_STR_DOWN;
                                           } // error for per id
                                        } // has peer participant

                                        LOG.info("TRAF RCOV: " + tmID + ", THREAD:TID " + txID
                                                + ", case 1 resolution path " + commitPath + ", status string: " + ts.getStatusString());
                                        if (commitPath == CommitPath.ORIGINATOR_ONLY) { // no remote participant
                                            if (ts.getStatusString().contains("COMMITTED")) {
                                               boolean walAllSync = ((ts.getTmTableAttr("_#ALL_REGION_WAL_SYNC#_")
                                                                      & TransactionState.WAL_SYNC_OK) == TransactionState.WAL_SYNC_OK);
                                               if (walAllSync) {
                                                  LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort with wal-all-sync for " + txID);
                                                  txnManager.abort(ts);
                                                  LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID + " completes ");
                                               } else {
                                                    List<TransactionRegionLocation> skippedList = new ArrayList<TransactionRegionLocation>();
                                                    boolean needCommit = ts.skipRegionWithWalSynced(skippedList);
                                                    if (needCommit) { 
                                                        // committed
                                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                                      + " number of regions " + ts.getParticipatingRegions().regionCount(peerid) +
                                                                      " and tolerating UnknownTransactionExceptions");
                                                        txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                                      + " completes ");
                                                        ts.processWalSync(false);
                                                        audit.putSingleRecord(txID,
                                                                  ts.getStartId(),
                                                                  ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  true,
                                                                  -1);
                                                    }
                                                    if (skippedList.size() > 0) {
                                                        ts.renewParticipatingRegions();
                                                        for (TransactionRegionLocation location : skippedList)
                                                            ts.addRegion(location);
                                                        LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort with size " + skippedList.size() + " for " + txID);
                                                        txnManager.abort(ts);
                                                        LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID + " completes ");
                                                    }
                                               }
                                            } else if (ts.getStatusString().contains("ABORT")) {
                                                // aborted
                                                if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                txnManager.abort(ts);
                                                if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID
                                                        + " completes ");
                                            } else {
                                                LOG.error("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                        + ", status is not COMMIT or ABORT. Aborting; current state: "
                                                        + ts.getStatusString());
                                                txnManager.abort(ts);
                                                if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID
                                                        + " completes ");
                                            }
                                        } // path 1
                                        else if (commitPath == CommitPath.ORIGINATOR_STR_DOWN) { // peer is sdn in STRCONFIG
                                            // for path 4, need to write a RECOVERY state record for possible resolution when brings up peers
                                            if (ts.getStatusString().contains("COMMITTED")) {
                                               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                           + " number of regions " + ts.getParticipatingRegions().regionCount(peerid) +
                                                              " and tolerating UnknownTransactionExceptions");
                                               long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                               audit.putSingleRecord(txID,
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                            }  // committed
                                            else if (ts.getStatusString().contains("ABORT")) {
                                               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                               long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                               audit.putSingleRecord(txID,
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_ABORT.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.abort(ts);
                                            } // aborted
                                            else {
                                               LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                       + ", status is not COMMIT or ABORT. Aborting; current state: "
                                                       + ts.getStatusString());
                                              long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                              audit.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_ABORT.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.abort(ts);
                                            } // else
                                        } // path 4
                                        else if (commitPath == CommitPath.BOTH_CONSENSUS) { // get a consensus from peer's TLOG state records
                                           if (ts.getStatusString().contains("COMMITTED")) {
                                                 if (ts_p.getStatusString().contains("COMMITTED")) {
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if (ts_p.getStatusString().contains("NOTX")) {
                                                     if (useTlog) {
                                                         // TBD: do we need to write a RECOVERY_COMMITTED in local TLOG ??
                                                         if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_COMMIT from local for "
                                                              + txID + " to peer TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                         peerTlog.putSingleRecord(txID,
                                                        		 ts.getStartId(),
                                                        		 ts.getCommitId(),
                                                                 ts.getTmFlags(),
                                                                 TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                 ts.getTableAttr(),
                                                        		 ts.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if ((ts_p.getStatusString().contains("RECOVERY")) && (ts_p.getStatusString().contains("ABORT"))) {
                                                      // this is the case where the peer takes over commit responsibility when the local is sdn
                                                      // while local comes back, need to get the take-over decision from peer to reset local state record
                                                      LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                            + ", peer status is RECOVERY_ABORTED overwriting local COMMITTED, likely due to sdn local. Aborting current state: "
                                                            + ts.getStatusString());
                                                      long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                      audit.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                      ts.getTmFlags(),
                                                                      TransState.STATE_RECOVERY_ABORT.toString(),
                                                                      ts.getTableAttr(),
                                                                      ts.hasRemotePeers(),
                                                                      forceForgotten,
                                                                      nextAsn);
                                                      txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID
                                                    		 + " TLOG local COMMIT replica ABORT or others" + peerid
                                                             + " state " + ts_p.getStatusString());
                                                 }
                                           }
                                           else if (ts.getStatusString().contains("ABORT")) {
                                                 // we are psrseumed-abort, so always abort a transcation during sdn takeover
                                                 if (ts_p.getStatusString().contains("ABORT")) {
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts_p.getStatusString().contains("NOTX")) {
                                                     if (useTlog) {
                                                         if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT from local for "
                                                                + txID + " to peer TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                         peerTlog.putSingleRecord(txID,
                                                        		 ts.getStartId(),
                                                        		 ts.getCommitId(),
                                                                 ts.getTmFlags(),
                                                                 TransState.STATE_RECOVERY_ABORT.toString(),
                                                        		 null,
                                                        		 ts.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID
                                                    		 + " TLOG local ABORT replica COMMIT or others" + peerid
                                                             + " state " + ts_p.getStatusString());
                                                 }
                                           }
                                           else if (ts.getStatusString().contains("NOTX")) {
                                                 if (ts_p.getStatusString().contains("COMMITTED")) {
                                                     if (useTlog) {
                                                         //peerTlog.getTransactionState(ts_p, true);
                                                         LOG.error("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_COMMIT from replica for "
                                                                   + txID + " to local TLOG (NOTX) " + peerid);
                                                         long nextAsn = audit.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                         audit.putSingleRecord(txID,
                                                        		 ts_p.getStartId(),
                                                        		 ts_p.getCommitId(),
                                                                 ts_p.getTmFlags(),
                                                                 TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                 ts_p.getTableAttr(),
                                                        		 ts_p.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                         audit.getTransactionState(ts, true);

                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if (ts_p.getStatusString().contains("ABORT")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT from replica for "
                                                                  + txID + " to local TLOG (NOTX) " + peerid);
                                                         long nextAsn = audit.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                         audit.putSingleRecord(txID,
                                                        		 ts_p.getStartId(),
                                                        		 ts_p.getCommitId(),
                                                                 ts_p.getTmFlags(),
                                                        		 TransState.STATE_RECOVERY_ABORT.toString(),
                                                                 ts_p.getTableAttr(),
                                                        		 ts_p.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                         audit.getTransactionState(ts, true);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts_p.getStatusString().contains("NOTX")) { // either ABORT or illegal state (internal error)
                                                     if (HBaseTxClient.getMap().get(txID) == null) {
                                                         LOG.warn("TRAF RCOV: " + tmID + " THREAD: can not find pending transaction " + txID
                                                        		   + " in mapTransactionStates ");
                                                         takeover = true;
                                                     }
                                                     if (takeover) { // originator TM fails, abort transaction
                                                         if (useTlog) {
                                                             LOG.warn("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT by LDTM for " + txID
                                                            		 + " to local TLOG (NOTX) " + peerid);
                                                             long nextAsn = audit.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                             audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), ts.getTmFlags(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                             LOG.warn("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT by LDTM for " + txID
                                                            		 + " to peer TLOG (NOTX) " + peerid);
                                                             nextAsn = peerTlog.getNextAuditSeqNum((int)ts_p.getClusterId(), (int)ts_p.getInstanceId(), (int)ts_p.getNodeId());
                                                             peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), ts.getTmFlags(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                         }
                                                         LOG.warn("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                         txnManager.abort(ts);
                                                     }
                                                     else {
                                                         LOG.warn("TRAF RCOV: " + tmID + " THREAD: commit pending state " + txID
                                                        		 + " TLOG local NOTX replica NOTX, check HBase/ERsgyn/XDC health ");
                                                     }
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID
                                                    		 + " TLOG peer not ABORT, COMMIT, or NOTX"
                                                             + ts_p.getStatusString());
                                                 }
                                           } 
                                           else {
                                                 LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                		 + ", local originator status is not set to COMMITTED or ABORTED or NOTX. Aborting with state "
                                                         + ts.getStatusString() + " TRAF RCOV THREAD:Redriving abort");
                                                 txnManager.abort(ts);
                                            }
                                        }  // path 2
                                       else if (commitPath == CommitPath.STILL_ALIVE) { // preparing time is longer than normal > 2 chore thread wakeup time (10 sec) in Region
                                            LOG.warn("TRAF RCOV: " + tmID + " Tansaction " + txID
                                                    + " RCOV thread resolution is deferred due to the transaction object is still alive in DTM memory, preparing time takes too long ");
                                       }
                                       else { // 3 for defer resolution
                                            LOG.warn("TRAF RCOV: " + tmID + " Tansaction " + txID
                                            		+ " resolution is deferred due to inaccessibility to peer, check HBase health at cluster id "
                                                    + clusterid + " originator state " +  ts.getStatusString() + " peer "
                                                    + ts_p.getStatusString());
                                       }
                                   } // indoubt transaction started at local node
                                   else { // transcations started by peers, here we do similar commit decision like regular commit processing if take over
                                        // CASE 2: ts & audit is the replica, ts_p & peerTlog is the originator
                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                        		+ " resolution : case 2 :local is replica, commit originator is " + clusterid + 
                                                        " local cluster id " + pSTRConfig.getTrafClusterIdInt() +
                                                        " peer count " + pSTRConfig.getConfiguredPeerCount());
                                        audit.getTransactionState(ts, false);

                                        if (pSTRConfig.getPeerStatus(clusterid).contains(PeerInfo.STR_DOWN)) {
                                           // STR is down, do commit takeover based on local TLOG
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                        		   + " commit authority is taken over due to STR_DOWN at peer " + clusterid);
                                           //commit_takeover(clusterid); // perform takeover preprocessing before starts to resolve transactions
                                           commitPath = CommitPath.REPLICA_STR_DOWN;
                                        }
                                        else if (pSTRConfig.getPeerStatus(clusterid).contains(PeerInfo.STR_UP)) {  // STR is up, ask peer to get a consensus
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                        		   + " commit authority is sent to Peer due to STR_UP at peer " + clusterid);
                                           commitPath = CommitPath.BOTH_CONSENSUS;
                                           try {
                                               peerTlog = getTlog(clusterid); // peerTlog is the originator
                                               peerTlog.getTransactionState(ts_p, false);
                                           } catch (Exception e2) {
                                               LOG.error("TRAF RCOV: " + tmID + " getTransactionState from Peer " + clusterid + " for tid "
                                                 + ts.getTransactionId() + "  hit Exception2 ", e2);
                                               commitPath = CommitPath.DEFER_RESOLUTION;
                                           }
                                        }
                                        else {
                                           if(LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " PEER THREAD: txID " + txID
                                        		   + " commit originator status is unknown, neither STR_UP or STR_DOWN " + clusterid);
                                           commitPath = CommitPath.DEFER_RESOLUTION;
                                        }
                                             
                                        // No need to post all the regions in R2.0 for the takeover case since remote peer could be down and this could cause 
                                        // transaction manager to be stuck, only send decision to indoubt regions
                                        // pass "false" in the second parameter for getTransactionState postAllRegions

                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:TID " + txID
                                        		+ " case 2 resolution path " + commitPath);
                                        if (commitPath == CommitPath.REPLICA_STR_DOWN) { // peer (orignator) is down, local takes over after peer sdn
                                            if (ts.getStatusString().contains("COMMITTED")) {
                                               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                              + " number of regions " + ts.getParticipatingRegions().regionCount(peerid) +
                                                              " and tolerating UnknownTransactionExceptions");
                                               // write a RECOVERY_COMMITTED in  local to avoid later aging
                                               long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                               audit.putSingleRecord(txID,
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                            }  // committed
                                            else if (ts.getStatusString().contains("ABORT")) {
                                               if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                               // write a RECOVERY_ABORT in  local to avoid later aging
                                               long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                               audit.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_ABORT.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.abort(ts);
                                            } // aborted
                                            else {
                                               LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                            		   + ", status is not COMMIT or ABORT. Aborting with state "
                                                       + ts.getStatusString());
                                               // write a RECOVERY_ABORT in  local to avoid later aging
                                               long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                               audit.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                  ts.getTmFlags(),
                                                                  TransState.STATE_RECOVERY_ABORT.toString(),
                                                                  ts.getTableAttr(),
                                                                  ts.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);
                                               txnManager.abort(ts);
                                            } // else
                                        } // path 5
                                        else if (commitPath == CommitPath.BOTH_CONSENSUS) {
                                           if (ts_p.getStatusString().contains("COMMITTED")) {
                                                 if (ts.getStatusString().contains("COMMITTED")) {
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if (ts.getStatusString().contains("NOTX")) {
                                                     if (useTlog) {
                                                         if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_COMMIT from originator "
                                                               + peerid + " for " + txID + " to local TLOG (NOTX) ");
                                                         long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                         audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), ts.getTmFlags(), TransState.STATE_RECOVERY_COMMITTED.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if ((ts.getStatusString().contains("RECOVERY")) && (ts.getStatusString().contains("ABORT"))) {
                                                      // this is the case where the local takes over commit responsibility when the peer is sdn
                                                      // while peer comes back, need to get the take-over decision from local to reset peer state record
                                                      LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                            + ", local status is RECOVERY_ABORTED overwriting peer COMMITTED, likely due to sdn peer. Aborting current state: "
                                                            + ts.getStatusString());
                                                      long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                      peerTlog.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                      ts.getTmFlags(),
                                                                      TransState.STATE_RECOVERY_ABORT.toString(),
                                                                      ts.getTableAttr(),
                                                                      ts.hasRemotePeers(),
                                                                      forceForgotten,
                                                                      nextAsn);
                                                      txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID + " TLOG originator "
                                                          + peerid + " COMMIT local ABORT or others " + ts.getStatusString());
                                                 }
                                           }
                                           else if (ts_p.getStatusString().contains("ABORT")) {
                                                 if (ts.getStatusString().contains("ABORT")) {
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts.getStatusString().contains("NOTX")) {
                                                     if (useTlog) {
                                                         if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT from originator "
                                                            + peerid + " for " + txID + " to local TLOG (NOTX) ");
                                                         long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                         audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), ts.getTmFlags(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts_p.getStatusString().contains("RECOVERY")) {
                                                      // local may have commit, but peer has recovery abort, which should supersede the local commit
                                                      LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                            + ", local status is RECOVERY_ABORTED overwriting peer COMMITTED, likely due to sdn peer. Aborting current state: "
                                                            + ts.getStatusString());
                                                      long nextAsn = audit.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                      audit.putSingleRecord(txID, 
                                                                      ts.getStartId(),
                                                                      ts.getCommitId(),
                                                                      ts.getTmFlags(),
                                                                      TransState.STATE_RECOVERY_ABORT.toString(),
                                                                      ts.getTableAttr(),
                                                                      ts.hasRemotePeers(),
                                                                      forceForgotten,
                                                                      nextAsn);
                                                      txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID + " TLOG originator "
                                                            + peerid + " ABORT local COMMIT or others " + ts.getStatusString());
                                                 }
                                           }
                                           else if (ts_p.getStatusString().contains("NOTX")) { // TBD ?? is there a window to see contradition ??
                                                 if (ts.getStatusString().contains("COMMITTED")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_COMMIT from local replica for "
                                                               + txID + " to originator TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                         peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(),
                                                                 ts.getTmFlags(), TransState.STATE_RECOVERY_COMMITTED.toString(),
                                                                 null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving commit for " + txID
                                                             + " number of regions " + ts.getParticipatingRegions().regionCount(peerid)
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/, 0 /* stallFlags */);
                                                 }
                                                 else if (ts.getStatusString().contains("ABORT")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV: " + tmID + " THREAD: Write RECOVERY_ABORT from local replica for "
                                                           + txID + " to originator TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)ts.getClusterId(), (int)ts.getInstanceId(), (int)ts.getNodeId());
                                                        peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), ts.getTmFlags(),
                                                               TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(),
                                                               forceForgotten, nextAsn);
                                                     }
                                                     if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts.getStatusString().contains("NOTX")) { // primary may fail in preparing or long prepare, queue a tlog req to origintor
                                                         LOG.warn("TRAF RCOV: " + tmID + " THREAD: commit pending state " + txID
                                                        		 + " TLOG originator NOTX local NOTX, defer resolution, check HBase/ERsgyn/XDC health ");
                                                         queuePeerTlogReq(txID);
                                                     }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV: " + tmID + " THREAD: Internal Error " + txID
                                                    		 + " TLOG peer not ABORT, COMMIT, or NOTX with state "
                                                             + ts.getStatusString());
                                                 }
                                           } 
                                           else {
                                                 LOG.warn("TRAF RCOV: " + tmID + " Recovering transaction " + txID
                                                		 + ", originator status is not set to COMMITTED or ABORTED or NOTX. Aborting with state "
                                                         + ts_p.getStatusString());
                                                 if(LOG.isDebugEnabled()) LOG.debug("TRAF RCOV: " + tmID + " THREAD:Redriving abort for " + txID);
                                                 txnManager.abort(ts);
                                            }
                                        } // path 2
                                       else { // 3 for defer resolution
                                            LOG.warn("TRAF RCOV: " + tmID + " Tansaction " + txID
                                            		+ " resolution is deferred due to inaccessibility to peer, check HBase health at cluster id "
                                                    + clusterid + " originator state " +  ts_p.getStatusString() + " local "
                                                    + ts.getStatusString());
                                       }
                                   } // indoubt transaction started at peer
            } catch (UnsuccessfulDDLException ddle) {
                LOG.error("TRAF RCOV: " + tmID + " UnsuccessfulDDLException encountered by Recovery Thread. Registering for retry. txID: " + txID + "Exception " , ddle);
  
                //Do not change the state of txId in tmDDL. Let the recovery thread
                //detect this txID again and redrive. Reset flag to loop back and
                //check for tmDDL again.
                ddlOnlyRecoveryCheck = true;
                
            } catch (CommitUnsuccessfulException cue) {
                LOG.error("TRAF RCOV: " + tmID + " CommitUnsuccessfulException encountered by Recovery Thread. Registering for retry. txID: " + txID + "Exception " , cue);
                
                //Do not change the state of txId in tmDDL. Let the recovery thread
                //detect this txID again and redrive. Reset flag to loop back and
                //check for tmDDL again.
                ddlOnlyRecoveryCheck = true;

            }
        }
      }//recoverTransactions()
          
   } //class RecoveryThread


     
     //================================================================================
     // DTMCI Calls
     //================================================================================

     //--------------------------------------------------------------------------------
     // callRequestRegionInfo
     // Purpose: Prepares HashMapArray class to get region information
     //--------------------------------------------------------------------------------
   public HashMapArray callRequestRegionInfo() throws IOException {

      int peerId = 0; // local cluster
      TransactionState ts;
      String tablename = null;
      String encoded_region_name = null;
      String region_name = null;
      String is_offline = null;
      String region_id = null;
      String hostname = null;
      String port = null;
      String thn;
      String startkey, endkey;
      int tnum = 0; // Transaction number

      if (LOG.isTraceEnabled()) LOG.trace(":callRequestRegionInfo:: start\n");

      HashMapArray hm = new HashMapArray();

      for (ConcurrentHashMap.Entry<Long, TransactionState> entry : mapTransactionStates.entrySet()) {
//          key = entry.getKey();
         ts = entry.getValue();
         long id = ts.getTransactionId();
         for (Map.Entry<String, HashMap<ByteArrayKey,TransactionRegionLocation>> tableMap : 
                        ts.getParticipatingRegions().getList(peerId).entrySet()) {
            // TableName
            tablename = tableMap.getKey();
            for (TransactionRegionLocation loc : tableMap.getValue().values()) {
               // Encoded Region Name
               if (encoded_region_name == null)
                  encoded_region_name = loc.getRegionInfo().getEncodedName();
               else
                  encoded_region_name = encoded_region_name + ";" + loc.getRegionInfo().getEncodedName();
               // Region Name
               if (region_name == null)
                  region_name = loc.getRegionInfo().getTable().getNameAsString();
               else
                  region_name = region_name + ";" + loc.getRegionInfo().getTable().getNameAsString();
               // Region Offline
               boolean is_offline_bool = loc.getRegionInfo().isOffline();
               is_offline = String.valueOf(is_offline_bool);
               // Region ID
               if (region_id == null)
                  region_id = String.valueOf(loc.getRegionInfo().getRegionId());
               else
                  region_id = region_id + ";" + loc.getRegionInfo().getRegionId();
               // Hostname
               if (hostname == null) {
                  thn = String.valueOf(loc.getHostname());
                  hostname = thn.substring(0, thn.length()-1);
               }
               else {
                  thn = String.valueOf(loc.getHostname());
                  hostname = hostname + ";" + thn.substring(0, thn.length()-1);
               }
   
               // Port
               if (port == null) 
                  port = String.valueOf(loc.getPort());
               else
                  port = port + ";" + String.valueOf(loc.getPort());
            }
            hm.addElement(tnum, "TableName", tablename);
            hm.addElement(tnum, "EncodedRegionName", encoded_region_name);
            hm.addElement(tnum, "RegionName", region_name);
            hm.addElement(tnum, "RegionOffline", is_offline);
            hm.addElement(tnum, "RegionID", region_id);
            hm.addElement(tnum, "Hostname", hostname);
            hm.addElement(tnum, "Port", port);
         }
         tnum = tnum + 1;
      }
     if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient::callRequestRegionInfo:: end size: " + hm.getSize());
     return hm;
   }
}

