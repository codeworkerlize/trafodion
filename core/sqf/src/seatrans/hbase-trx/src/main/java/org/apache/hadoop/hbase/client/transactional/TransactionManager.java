/**
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
**/


package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.lang.management.ManagementFactory;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.ServerName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortSavepointRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortSavepointResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionMultipleRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionMultipleResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitMultipleRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitMultipleResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestMultipleRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitSavepointRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitSavepointResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.RecoveryRequestRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.RecoveryRequestResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafSetStoragePolicyResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafSetStoragePolicyRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ReleaseLockRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ReleaseLockResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GeneralBinlogCommandRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GeneralBinlogCommandResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.TrxEnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ByteArrayKey;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.client.transactional.TmDDL;
import org.apache.hadoop.hbase.client.transactional.RMInterface.AlgorithmType;
import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

// Sscc imports
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccRegionService;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortSavepointRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortSavepointResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortTransactionRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortTransactionResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitRequestRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitRequestResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitSavepointRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitSavepointResponse;

import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.ATRConfig;
import org.apache.hadoop.hbase.client.NoServerForRegionException;

import org.apache.hadoop.hbase.pit.BackupRestoreClient;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

import com.google.protobuf.ServiceException;
/**
 * Transaction Manager. Responsible for committing transactions.
 */
public class TransactionManager {

  // Singleton TransactionManager class
  private static TransactionManager g_TransactionManager = null;
  private static List<String> g_createdTables;
  static final Log LOG = LogFactory.getLog(TransactionManager.class);
  public AlgorithmType TRANSACTION_ALGORITHM;

  private int RETRY_ATTEMPTS;
  private final TransactionLogger transactionLogger;
  private JtaXAResource xAResource;
  private Connection connection;
  private Configuration adminConf;
  private Connection adminConnection;
  private TmDDL tmDDL;
  private BackupRestoreClient brClient;
  private boolean recoveryToPitMode = false;
  private boolean logCommits = false;

  public static final int HBASE_NAME = 0;
  public static final int HBASE_MAX_VERSIONS = 1;
  public static final int HBASE_MIN_VERSIONS = 2;
  public static final int HBASE_TTL = 3;
  public static final int HBASE_BLOCKCACHE = 4;
  public static final int HBASE_IN_MEMORY = 5;
  public static final int HBASE_COMPRESSION = 6;
  public static final int HBASE_BLOOMFILTER = 7;
  public static final int HBASE_BLOCKSIZE = 8;
  public static final int HBASE_DATA_BLOCK_ENCODING = 9;
  public static final int HBASE_CACHE_BLOOMS_ON_WRITE = 10;
  public static final int HBASE_CACHE_DATA_ON_WRITE = 11;
  public static final int HBASE_CACHE_INDEXES_ON_WRITE = 12;
  public static final int HBASE_COMPACT_COMPRESSION = 13;
  public static final int HBASE_PREFIX_LENGTH_KEY = 14;
  public static final int HBASE_EVICT_BLOCKS_ON_CLOSE = 15;
  public static final int HBASE_KEEP_DELETED_CELLS = 16;
  public static final int HBASE_REPLICATION_SCOPE = 17;
  public static final int HBASE_MAX_FILESIZE = 18;
  public static final int HBASE_COMPACT = 19;
  public static final int HBASE_DURABILITY = 20;
  public static final int HBASE_MEMSTORE_FLUSH_SIZE = 21;
  public static final int HBASE_SPLIT_POLICY = 22;
  public static final int HBASE_ENCRYPTION = 23;
  public static final int HBASE_CACHE_DATA_IN_L1 = 24;
  public static final int HBASE_PREFETCH_BLOCKS_ON_OPEN = 25;
  public static final int HBASE_HDFS_STORAGE_POLICY= 26;

  private static final int BINLOG_CMD_WID_FLUSHED = 1;
  public static final int BINLOG_FLUSH_OK   =   0;
  public static final int BINLOG_FLUSH_WAIT =   1;
  public static final int BINLOG_WRITE_ERROR = -1;

  public static final int TM_COMMIT_FALSE = 0;
  public static final int TM_COMMIT_READ_ONLY = 1;
  public static final int TM_COMMIT_TRUE = 2;
  public static final int TM_COMMIT_FALSE_CONFLICT = 3;
  public static final int TM_COMMIT_FALSE_SHIELDED = 7;
  public static final int TM_COMMIT_FALSE_DOOMED = 8;
  public static final int TM_COMMIT_BROADCAST = 10;
  public static final int TM_COMMIT_FALSE_EPOCH_VIOLATION = 13;

  public static final int TM_SLEEP = 1000;      // One second
  public static final int TM_SLEEP_INCR = 5000; // Five seconds
  public static final int TM_RETRY_ATTEMPTS = 5;
  Configuration config; 

  private IdTm idServer;
  private static int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec

  public static final int STALL_LOCAL_COMMIT   = 6;
  public static final int STALL_REMOTE_COMMIT  = 7;
  public static final int STALL_BOTH_COMMIT    = 8;

  private static boolean enableRowLevelLock = false;
  private static Integer costTh = -1;
  private static String PID;

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

  static ExecutorService    cp_tpe;
  static int intThreads = 16;

  private STRConfig pSTRConfig        = null;
  private static Object peerReconnectLock = new Object();

  public enum AlgorithmType{
    MVCC, SSCC
  }

    static String getNotEmptyEnvVar(String envName) {
      String envVar = System.getenv(envName);
      if (envVar != null) {
          envVar = envVar.trim();
          return !envVar.isEmpty() ? envVar : null;
      }
      return null;
    }

  static {
      g_createdTables = Collections.synchronizedList(new ArrayList<String>());

      String envEnableRowLevelLock = getNotEmptyEnvVar("ENABLE_ROW_LEVEL_LOCK");
      if (envEnableRowLevelLock != null) {
          enableRowLevelLock = (Integer.parseInt(envEnableRowLevelLock.trim()) == 0) ? false : true;
      }

      String costThreshold = System.getenv("RECORD_TIME_COST_HBASE");
      if (costThreshold != null && false == costThreshold.trim().isEmpty())
        costTh = Integer.parseInt(costThreshold);

      PID = ManagementFactory.getRuntimeMXBean().getName();
      PID = PID.split("@")[0];
  } // static

  // getInstance to return the singleton object for TransactionManager
  public synchronized static TransactionManager getInstance(final Configuration conf, Connection connection) 
      throws ZooKeeperConnectionException, IOException {
    if (g_TransactionManager == null) {
      g_TransactionManager = new TransactionManager(conf, connection);
    }
    return g_TransactionManager;
  }

  public int retry(int retrySleep) {
     boolean keepPolling = true;
     while (keepPolling) {
         try {
             Thread.sleep(retrySleep);
             keepPolling = false;
         } catch(InterruptedException ex) {
          // ignore the interruption and keep going
         }
     }
     return (retrySleep += TM_SLEEP_INCR);
  }
 
  /* increment/decrement for positive value */
  /* This method copied from o.a.h.h.utils.Bytes */
  public static byte [] binaryIncrementPos(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = value[value.length-i-1] & 0x0ff;
      int total = val + cur;
      if(total > 255) {
        amo += sign;
        total %= 256;
      } else if (total < 0) {
        amo -= sign;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  public void init(final TmDDL tmddl) throws IOException {
    this.tmDDL = tmddl;
  }

  public boolean isPeerConnectionUp(int pv_peer_id, String tableName) throws IOException
  {

     synchronized(peerReconnectLock) {
          if (! isSTRUp(pv_peer_id)) {
               if (LOG.isWarnEnabled()) LOG.warn("XDC TM: peer " + pv_peer_id + " is down (SDN) when transaction " +
                                         " intend to access table " + tableName);
               return false;
          }
          if (pSTRConfig.getPeerConnectionState() == STRConfig.PEER_CONNECTION_DOWN) { // there is a SUP trans but peer has not added connection yet
               if (LOG.isInfoEnabled()) LOG.info("XDC TM: isPeerConnectionUp,"
                                                  + " PeerCount " + pSTRConfig.getConfiguredPeerCount()
                                                  + " lv_first_remote_peer_id: " + pv_peer_id
                                                  + " adding peer connection for peer connection state " + pSTRConfig.getPeerConnectionState()  );
               pSTRConfig.addPeerConnection(pv_peer_id);
               if (pSTRConfig.getPeerConnectionState() == STRConfig.PEER_CONNECTION_DOWN) {
                   if (LOG.isWarnEnabled()) LOG.warn("peer connection still not available after try to add ");
                   return false;
               }
          }
          return true;
     }
  }

  /**
   * TransactionManagerCallable  :  inner class for creating asynchronous requests
   */
  private abstract class TransactionManagerCallable implements Callable<Integer> {
        TransactionState transactionState;
        TransactionRegionLocation  location;
        HRegionInfo     lv_hri;
        TransactionalTable table;
        byte[] startKey;
        byte[] endKey_orig;
        byte[] endKey;
        int maxBinlogStatusPollingTimes = 100;

        TransactionManagerCallable(TransactionState txState, final TransactionRegionLocation location, Connection connection) 
               throws IOException {
        transactionState = txState;
        this.location = location;
        if (connection == null) {
           LOG.error("TransactionManagerCallable constructor: Connection is " + ((connection == null) ? "null" : connection )
                 + ", peerid: " + location.getPeerId() + " for transaction: " + txState);
        }
        if ((location.getPeerId() != 0) && (location.getPeerId() != pSTRConfig.getTrafClusterIdInt())   ){
           if (LOG.isTraceEnabled()) LOG.trace("TransactionManagerCallable constructor: Connection is " + ((connection == null) ? "null" : connection )
                 + ", peerid: " + location.getPeerId() + " for transaction: " + txState);
           if (! isPeerConnectionUp(location.getPeerId(),  location.getRegionInfo().getRegionNameAsString())  ) {
                throw new IOException("Problem in Transaction Manager callable setup: xdc peer is either SDN or connection is not ready ");
           }
        }
//        table = connection.getTable(location.getRegionInfo().getTable(), cp_tpe);

        table = new TransactionalTable(location.getRegionInfo().getTable(), connection, cp_tpe, intThreads);
        lv_hri = location.getRegionInfo();
        startKey = lv_hri.getStartKey();
        endKey_orig = lv_hri.getEndKey();
        if(endKey_orig == null || endKey_orig == HConstants.EMPTY_END_ROW)
          endKey = null;
        else
          endKey =  TransactionManager.binaryIncrementPos(endKey_orig, -1); 
    }

    public void refreshRegion(final String operation,
                              final long transactionId, 
                              final int participantNum)
                              throws IOException {
     try {
         table.clearRegionCache();
         RegionLocator locator = connection.getRegionLocator(table.getName());
         HRegionLocation lv_hrl = locator.getRegionLocation(startKey, true);
         lv_hri = lv_hrl.getRegionInfo();
         endKey_orig = lv_hri.getEndKey();
         if(endKey_orig == null || endKey_orig == HConstants.EMPTY_END_ROW)
           endKey = null;
         else
           endKey =  TransactionManager.binaryIncrementPos(endKey_orig, -1);
         if (LOG.isTraceEnabled()) LOG.trace(operation+ "-- location being refreshed : " + location.getRegionInfo().getRegionNameAsString() + " endKey: "
                 + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
         if (LOG.isInfoEnabled()) LOG.info(operation + "-- " 
                 + table.getName().getNameAsString() + " location being refreshed"
                 + "-- lv_hri: " + lv_hri
                 + "-- location.getRegionInfo(): " + location.getRegionInfo()
             );
     } catch (IOException e) {
         if (LOG.isWarnEnabled()) LOG.warn(operation + " region refresh encountered exception : participant:" + participantNum + " transid:" + transactionId , e);
         throw e;
     }
    }
    
    /**
     * Method  : doCommitX
     * Params  : transactionId - transaction identifier
     * Return  : Always 0, can ignore
     * Purpose : Call commit for a given regionserver
     */
  public Integer doCommitX(final long transactionId,
		                   final long commitId,
		                   final int participantNum,
		                   final boolean ignoreUnknownTransaction,
                           final short totalNum,
                           final int tmTableCDCAttr,
                           final int ddlNum ) throws CommitUnsuccessfulException, IOException {
        boolean retry = false;
        boolean refresh = false;
        boolean walSyncOk = false;

        int retryCount = 0;
        int retrySleep = TM_SLEEP;
        String regionEncodedName = null;

        if( TRANSACTION_ALGORITHM == AlgorithmType.MVCC){
        do {
          retry = false;
          refresh = false;
          walSyncOk = false;
          try {
            regionEncodedName = lv_hri.getEncodedName();
            if (LOG.isDebugEnabled()) LOG.debug("doCommitX -- ENTRY txid: " + transactionId
                    + " commitId " + commitId
                    + " participantNum " + participantNum
                    + " ignoreUnknownTransaction: " + ignoreUnknownTransaction
                    + " Location " + location.getRegionInfo().getRegionNameAsString()
                    + " tmTableCDCAttr: " + tmTableCDCAttr);
            Batch.Call<TrxRegionService, CommitResponse> callable =
               new Batch.Call<TrxRegionService, CommitResponse>() {
                 ServerRpcController controller = new ServerRpcController();
                 BlockingRpcCallback<CommitResponse> rpcCallback =
                    new BlockingRpcCallback<CommitResponse>();

                    @Override
                    public CommitResponse call(TrxRegionService instance) throws IOException {
                      org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequest.Builder builder = CommitRequest.newBuilder();
                      builder.setTransactionId(transactionId);
                      builder.setCommitId(commitId);
                      builder.setParticipantNum(participantNum);
                      builder.setTmTableCDCAttr(tmTableCDCAttr);
                      builder.setRegionName(ByteString.copyFromUtf8(""));
                      builder.setIgnoreUnknownTransactionException(ignoreUnknownTransaction);
                      builder.setTotalNum(totalNum);
                      builder.setDdlNum(ddlNum);

                      instance.commit(controller, builder.build(), rpcCallback);
                      return rpcCallback.get();
                  }
               };

               Map<byte[], CommitResponse> result = null;
               try {
                 if (ignoreUnknownTransaction){
                    if(LOG.isDebugEnabled())
                        LOG.debug("HAX - doCommitX, -- coprocessorService txid: " + transactionId +
                        " ignoreUnknownTransaction: " + ignoreUnknownTransaction
                        + " tabel attr: " + tmTableCDCAttr
                        + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));  

                    if(LOG.isDebugEnabled())
                        LOG.debug("doCommitX -- Recovery Redrive before coprocessorService txid: " + transactionId +
                        " ignoreUnknownTransaction: " + ignoreUnknownTransaction + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));
                 }
                 long timeCost = System.currentTimeMillis();
                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                 if (costTh >= 0) {
                   timeCost = System.currentTimeMillis() - timeCost;
                   if (timeCost >= costTh)
                     LOG.warn("doCommitX copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                 }
               } catch (ServiceException se) {
                  String msg = new String ("ERROR occurred while calling coprocessor service in doCommitX for transaction "
                              + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                  LOG.warn(msg, se);
                  throw new RetryTransactionException(msg,se);
               } catch (Throwable e) {
                  String msg = new String ("ERROR occurred while calling coprocessor service in doCommitX for transaction "
                              + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                  LOG.error(msg, e);
                  DoNotRetryIOException dnre =  new DoNotRetryIOException(msg,e);
                  transactionState.requestPendingCountDec(dnre, null, false);
                  throw dnre;
               }
               if (result.size() == 0) {
               
                  if ( (tmTableCDCAttr & TIMELINE) == TIMELINE ) { // tabel with TimeLine attr, ignore the undeleivery
                     LOG.info("doCommitX, received incorrect result size: " + result.size() + " txid: "
                       + transactionId + " participantNum " + participantNum
                       + " peerId " + location.getPeerId() + " Location " + location.getRegionInfo().getRegionNameAsString());
                     LOG.info("doCommmitX, ignore undelivery issue with read only Timeline ");
                     retry = false;
                     refresh = false;                   
                  }
                  else { // not TIMELINE

                     if (  // optimized for SDN situation, recovery req is excluded
                             (location.getPeerId() != 0) && (pSTRConfig.getConfiguredPeerCount() > 0) &&
                             ((tmTableCDCAttr & SYNCHRONIZED) == SYNCHRONIZED) && !isSTRUp(location.getPeerId())   ) {
                         refresh = false;
                         retry = false;
                         // don't retry or reconnect, just abandon abort if peer is SDN and this is sync table
                         LOG.info("doCommitX, received 0 region results for transaction " + transactionId + " tolerate commit since peer " +
                                   location.getPeerId() + " is SDN and table " +  table.getName().getNameAsString()  +
                                   " is with sync attr, assume TLOG step has either write COMMIT to both or COMMIT-R in originator ");
                     }
                     else { // standard case
                         LOG.error("doCommitX, received incorrect result size: " + result.size() + " txid: "
                              + transactionId + " participantNum " + participantNum
                              + " peerId " + location.getPeerId() + " Location " + location.getRegionInfo().getRegionNameAsString());
         
                         refresh = true;
                         retry = true;
                         //if transaction for DDL operation, it is possible this table is disabled
                         //as part of prepare if the table was intended for a drop. If this is the case
                         //this exception can be ignored.
                         if(transactionState.hasDDLTx())
                         {
                         if(LOG.isTraceEnabled()) LOG.trace("doCommitX, checking against DDL Drop list:  result size: " +
                             result.size() + " txid: " + transactionId + " location: " + location.getRegionInfo().getRegionNameAsString() + 
                             "table: " + table.getName().getNameAsString());
                         ArrayList<String> createList = new ArrayList<String>(); //This list is ignored.
                         ArrayList<String> createIncrList = new ArrayList<String>(); //This list is ignored.
                         ArrayList<String> dropList = new ArrayList<String>();
                         ArrayList<String> truncateList = new ArrayList<String>();
                         StringBuilder state = new StringBuilder ();
                         tmDDL.getRow(transactionState.getTransactionId(), state, createList, createIncrList, dropList, truncateList);
                         if(state.toString().equals("VALID") && dropList.size() > 0)
                         {
                           Iterator<String> di = dropList.iterator();
                           while (di.hasNext())
                           {
                             if(table.getName().getNameAsString().equals(di.next().toString()))
                             {
                               retry = false; //match found
                               refresh = false;//match found
                               if(LOG.isTraceEnabled()) LOG.trace("doCommitX, found table in  DDL Drop list, this is expected exception. result size: " +
                                 result.size() + " txid: " + transactionId + " location: " + location.getRegionInfo().getRegionNameAsString() +
                                 "table: " + table.getName().getNameAsString());
                             }
                           } // while
                         } // VALID
                      }  // DDLTx
                         else
                         {
                             LOG.error("doCommitX, received incorrect result size: " + result.size() + " txid: "
                            + transactionId + " participantNum " + participantNum + " location: " + location.getRegionInfo().getRegionNameAsString()
                            + ", attmpting to reestablish connection");

                           table.close();
                           connection.close();
                           connection = ConnectionFactory.createConnection(pSTRConfig.getPeerConfiguration(location.getPeerId()));
                           table = new TransactionalTable(location.getRegionInfo().getTable(), connection, cp_tpe, intThreads);
                           if(LOG.isTraceEnabled()) LOG.trace("doCommitX, Location " + location.getRegionInfo().getRegionNameAsString()
                               + " reestablished connection and table");
                         }
                      } // not optimized
                  } // not TimeLine
               }
               else {
                  if(LOG.isTraceEnabled()) LOG.trace("doCommitX, result size: " +
                           result.size() + " txid: " + transactionId + " location: " + location.getRegionInfo().getRegionNameAsString() +
                           "table: " + table.getName().getNameAsString());
                  for (CommitResponse cresponse : result.values()) {
                    walSyncOk = cresponse.getCommitOk();
                    if (cresponse.getHasException()) {
                      String exceptionString = new String (cresponse.getException());
                      if (exceptionString.contains("UnknownTransactionException")) {
                          throw new UnknownTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("DUPLICATE")) {
                         throw new UnknownTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("NonPendingTransactionException")) {
                          throw new NonPendingTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("Trafodion CDC Exception")) {
                          throw new RetryTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("org.apache.hadoop.hbase.exceptions.FailedSanityCheckException")) {
                         throw new org.apache.hadoop.hbase.exceptions.FailedSanityCheckException(cresponse.getException());
                      }
                      else {
                        throw new RetryTransactionException(cresponse.getException());
                      }
                    }
                   location.setBinlogWid(cresponse.getCurrentWid());
                   long mywid = cresponse.getCurrentWid();
                    if(mywid > 0)  
                      transactionState.addIntoFlushCheckRespMap(location, mywid);
                  }
                  retry = false;
               }
          }
          catch (UnknownTransactionException ute) {
             String errMsg = new String("doCommitX UnknownTransactionException for transaction "
                              + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
             if (ignoreUnknownTransaction) {
                if (LOG.isInfoEnabled()) LOG.info(errMsg + " ,but ignored", ute);
                transactionState.requestPendingCountDec(null, null, false);
             }
             else {
                LOG.error(errMsg, ute);
                transactionState.logExceptionDetails(location.getPeerId(), true, participantNum);
                transactionState.requestPendingCountDec(null, null, false);
//                throw ute;
             }
          }
          catch (NonPendingTransactionException npte) {
              String errMsg = new String("doCommitX NonPendingTransactionException for transaction "
                               + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
              LOG.error(errMsg, npte);
              transactionState.logExceptionDetails(location.getPeerId(), false, participantNum);
              transactionState.requestPendingCountDec(null, null, false);
//              throw npte;
          }
          catch (org.apache.hadoop.hbase.exceptions.FailedSanityCheckException fsce) {
              LOG.error("doCommitX FailedSanityCheckException for transaction " + transactionId + " participantNum " + participantNum + 
                 " Location " + location.getRegionInfo().getRegionNameAsString(), fsce);
              refresh = false;
              retry = false;
              transactionState.requestPendingCountDec(fsce, null, false);
              throw fsce;
          }
          catch (RetryTransactionException rte) {
             if(retryCount == RETRY_ATTEMPTS) {
                String errMsg;
                errMsg = new String("Exceeded " + retryCount + " retry attempts in doCommitX for transaction "
                        + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                // We have received our reply in the form of an exception,
                // so decrement outstanding count and wake up waiters to avoid
                // getting hung forever
                CommitUnsuccessfulException cue = new CommitUnsuccessfulException(errMsg, rte);
                transactionState.requestPendingCountDec(cue, null, false);
                throw cue;
             }
             LOG.error("doCommitX retrying transaction " + transactionId + " due to Exception: ", rte);
             refresh = true;
             retry = true;
          }
          catch (Throwable thr) {
             LOG.error("doCommitX Throwable for transaction " + transactionId + " participantNum " + participantNum +
                 " Location " + location.getRegionInfo().getRegionNameAsString(), thr);
             transactionState.requestPendingCountDec(thr, null, false);
             refresh = false;
             retry = false;
             if (thr instanceof IOException) {
                 throw (IOException)thr;
             } else {
                 throw new IOException(thr);
             }
          }

          if (refresh) {
             RegionLocator locator = null;
             HRegionLocation lv_hrl = null;
             try{
                table.clearRegionCache();
                locator = connection.getRegionLocator(table.getName());
                lv_hrl = locator.getRegionLocation(startKey, true);
                if (LOG.isInfoEnabled()) LOG.info("doCommitX -- location refreshed : "
                       + lv_hrl.getRegionInfo().getRegionNameAsString()
                       + " hostname: " + lv_hrl.getServerName()
                       + " peerId: " + location.getPeerId()
                       + " startKey: " + Hex.encodeHexString(lv_hrl.getRegionInfo().getStartKey())
                       + " endKey: " + Hex.encodeHexString(lv_hrl.getRegionInfo().getEndKey())
                       + " for transaction " + transactionId);
             }
             catch (Exception e){
                LOG.warn("doCommitX refresh; getRegionLocation exception for transaction " + transactionId + " participantNum: "
                         + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString() + " ", e);
                break;
             }
             finally {
               try {
                  if (locator != null) {
                     locator.close();
                     locator = null;
                  }
                } catch(Throwable thr) {
                   retry = false;
                   String errMsg = new String("doCommitX -- failed to close. transaction id is " + transactionId);
                   LOG.error(errMsg, thr);
                }
             }

             if (LOG.isWarnEnabled()) LOG.warn("doCommitX -- setting retry, count: " + retryCount);
             refresh = false;
           }
           if (retry) 
              retrySleep = retry(retrySleep);
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);
        }

        if( TRANSACTION_ALGORITHM == AlgorithmType.SSCC){
        do {
          retry = false;
          refresh = false;
          try {

            if (LOG.isTraceEnabled()) LOG.trace("doCommitX -- ENTRY txid: " + transactionId
                    + " participantNum " + participantNum
                    + " commitId " + commitId
                    + " ignoreUnknownTransaction: " + ignoreUnknownTransaction);

            Batch.Call<SsccRegionService, SsccCommitResponse> callable =
               new Batch.Call<SsccRegionService, SsccCommitResponse>() {
                 ServerRpcController controller = new ServerRpcController();
                 BlockingRpcCallback<SsccCommitResponse> rpcCallback =
                    new BlockingRpcCallback<SsccCommitResponse>();

                    @Override
                    public SsccCommitResponse call(SsccRegionService instance) throws IOException {
                      org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitRequest.Builder builder = SsccCommitRequest.newBuilder();
                      builder.setTransactionId(transactionId);
                      builder.setRegionName(ByteString.copyFromUtf8(""));
                      builder.setCommitId(commitId);
                      builder.setIgnoreUnknownTransactionException(ignoreUnknownTransaction);

                      instance.commit(controller, builder.build(), rpcCallback);
                      return rpcCallback.get();
                  }
               };

               Map<byte[], SsccCommitResponse> result = null;
               try {
                 if (LOG.isTraceEnabled()) LOG.trace("doCommitX -- before coprocessorService txid: " + transactionId +
                        " ignoreUnknownTransaction: " + ignoreUnknownTransaction + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));

                 long timeCost = System.currentTimeMillis();
                 result = table.coprocessorService(SsccRegionService.class, startKey, endKey, callable);
                 if (costTh >= 0) {
                   timeCost = System.currentTimeMillis() - timeCost;
                   if (timeCost >= costTh)
                     LOG.warn("doCommitX copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                 }
               } catch (ServiceException se) {
                  String msg = new String("ERROR occurred while calling coprocessor service in doCommitX for transaction "
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                  LOG.warn(msg + ":", se);
                  throw new RetryTransactionException(msg,se);
               } catch (Throwable e) {
                  String msg = new String("ERROR occurred while calling coprocessor service in doCommitX for transaction "
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                  LOG.error(msg + ":", e);
                  DoNotRetryIOException dnr = new DoNotRetryIOException(msg, e);
                  transactionState.requestPendingCountDec(dnr);
                  throw dnr;
               }
               if (result.size() != 1) {
                  LOG.error("doCommitX, received incorrect result size: " + result.size() + " in doCommitX for transaction "
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                  refresh = true;
                  retry = true;
               }
               else {
                  // size is 1
                  for (SsccCommitResponse cresponse : result.values()){
                    if(cresponse.getHasException()) {
                      String exceptionString = new String (cresponse.getException());
                      if (exceptionString.contains("UnknownTransactionException")) {
                          throw new UnknownTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("DUPLICATE")) {
                          LOG.error("doCommitX, coprocessor UnknownTransactionException: " + cresponse.getException());
                          throw new UnknownTransactionException(cresponse.getException());
                      }
                      else if (exceptionString.contains("NonPendingTransactionException")) {
                          LOG.error("doCommitX, coprocessor NonPendingTransactionException: " + cresponse.getException());
                          throw new NonPendingTransactionException(cresponse.getException());
                       }
                      else {
                        throw new RetryTransactionException(cresponse.getException());
                      }
                  }
               }
               retry = false;
             }
          }
          catch (UnknownTransactionException ute) {
             String errMsg = new String("doCommitX, UnknownTransactionException  for transaction "
                + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
             if (ignoreUnknownTransaction) {
                LOG.info(errMsg + " ,but ignored", ute);
                transactionState.requestPendingCountDec(null);
             }
             else {
                LOG.error(errMsg, ute);
                transactionState.logExceptionDetails(location.getPeerId(), true, participantNum);
                transactionState.requestPendingCountDec(null);
//                throw ute;
             }
          }
          catch (NonPendingTransactionException npte) {
              String errMsg = new String("doCommitX, NonPendingTransactionException  for transaction "
                 + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
              LOG.error(errMsg, npte);
              transactionState.logExceptionDetails(location.getPeerId(), false, participantNum);
              transactionState.requestPendingCountDec(null);
//              throw npte;
          }
          catch (RetryTransactionException rte) {
             if (retryCount == RETRY_ATTEMPTS) {
                String errMsg = new String("Exceeded " + retryCount + " retry attempts in doCommitX for transaction "
                        + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                // We have received our reply in the form of an exception,
                // so decrement outstanding count and wake up waiters to avoid
                // getting hung forever
                CommitUnsuccessfulException cue = new CommitUnsuccessfulException(errMsg, rte);
                transactionState.requestPendingCountDec(cue);
                throw cue;
             }

             LOG.error("doCommitX participant " + participantNum + " retrying transaction "
                      + transactionId + " due to Exception: " , rte);
             refresh = true;
             retry = true;
          }
          if (refresh) {
             RegionLocator locator = null;
             HRegionLocation lv_hrl = null;
             try {
                table.clearRegionCache();
                locator = connection.getRegionLocator(table.getName());
                lv_hrl = locator.getRegionLocation(startKey, true);
                if (LOG.isInfoEnabled()) LOG.info("doCommitX -- location being refreshed : "
                        + lv_hrl.getRegionInfo().getRegionNameAsString() + "endKey: "
                        + Hex.encodeHexString(lv_hrl.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
             }
             catch (Exception e){
                LOG.warn("doCommitX refresh; getRegionLocation exception for transaction " + transactionId + " participantNum: "
                         + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString() + " ", e);
                break;
             }
             finally {
                try {
                  if (locator != null) {
                      locator.close();
                      locator = null;
                   }
                } catch(Throwable thr) {
                   retry = false;
                   String errMsg = new String("doCommitX -- failed to close. transaction id is " + transactionId);
                   LOG.error(errMsg, thr);
                }
             }

             if (LOG.isTraceEnabled()) LOG.trace("doCommitX -- setting retry, count: " + retryCount);
             refresh = false;
           }
           if (retry) 
              retrySleep = retry(retrySleep);
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);

        }
        // We have received our reply so decrement outstanding count
        transactionState.requestPendingCountDec(null, regionEncodedName, walSyncOk);

        if (LOG.isTraceEnabled()) LOG.trace("doCommitX -- EXIT txid: " + transactionId);
        return 0;
    }

    public void setBinlogPollMaxTime(int t) {
      maxBinlogStatusPollingTimes = t;
    }

    /**
      * Method  : doBinlogCheckX
      * Params  : transactionId - transaction identifier
      * Return  : binlog flushed or not
      *           0   it is ok 
      *           1   binlog something wrong, don't continue if you care about binlog
      * Purpose : pooling the status of binlog flush 
     */
    public Integer doBinlogCheckX(final long transactionId, final long wid, int waitIntv)
          throws IOException, CommitUnsuccessfulException {

      boolean retry = false;
      int retryCount = 0;
      int retcode = 0;
      do {
        final int seqnum = retryCount;
        try {
          Batch.Call<TrxRegionService, GeneralBinlogCommandResponse > callable =
                new Batch.Call<TrxRegionService, GeneralBinlogCommandResponse >() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<GeneralBinlogCommandResponse > rpcCallback =
                new BlockingRpcCallback<GeneralBinlogCommandResponse >();

          @Override
          public GeneralBinlogCommandResponse  call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GeneralBinlogCommandRequest.Builder builder = GeneralBinlogCommandRequest.newBuilder();
                builder.setTransactionId(transactionId);
                builder.setCommandId(BINLOG_CMD_WID_FLUSHED);
                builder.setArgs(ByteString.copyFromUtf8(String.valueOf(wid)));
                builder.setSeqnum(seqnum);
                instance.generalBinlogCommand(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
          };

          //invoke the RPC
          Map<byte[], GeneralBinlogCommandResponse> result = null;

          try {
              long timeCost = System.currentTimeMillis();
              result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
              if (costTh >= 0) {
                timeCost = System.currentTimeMillis() - timeCost;
                if (timeCost >= costTh)
                   LOG.warn("doBinlogCheckX copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
              }
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doBinlogCheckX coprocessor service";
                  LOG.warn(msg,  se);
                  transactionState.requestPendingCountDec(se);
                  return 1;
              } catch (TableNotFoundException tnfe) {
                  String msg = "WARNING while calling doBinlogCheckX for table: "
                          + table.getName().getNameAsString() + ", ignoring ";
                  LOG.warn(msg,  tnfe);
                  transactionState.requestPendingCountDec(tnfe);
                  return 1;
              } catch (Throwable t) {
                  String msg = "ERROR occurred while calling doBinlogCheckX coprocessor service";
                  LOG.error(msg,  t);
              }          

              //process the result from RPC
              if(result.size() == 0) {
                 LOG.error("doBinlogCheckX, received 0 region results for transaction " + transactionId);
                 transactionState.requestPendingCountDec(null);
                 return 1;
              }
              else {
                 for (GeneralBinlogCommandResponse cresponse : result.values()) {
                   retcode = cresponse.getRetcode();
                   //parse the response
                   if(retcode == BINLOG_FLUSH_OK || retcode == BINLOG_WRITE_ERROR) //FLUSH_OK or FLUSH_ERROR
                      retry = false;
                   else
                      retry = true;
                 }
              }
        }
        catch (Exception e) {
           String msg = "ERROR occurred while calling doBinlogCheckX coprocessor service";
           LOG.warn(msg,  e);
           transactionState.requestPendingCountDec(e);
           throw new RetryTransactionException(msg, e);
        }
        if(retry == true) {
          retryCount++;
          try {
            Thread.sleep(waitIntv);
          } catch(Exception e) {}
        }
      } while(retry == true && retryCount < maxBinlogStatusPollingTimes);

      if( retryCount >= maxBinlogStatusPollingTimes) //timeout polling
        LOG.error("HBaseBinlog doBinlogCheck timeout for wid " + wid  + " tid " + transactionId);

      transactionState.requestPendingCountDec(null);
      if(retcode != BINLOG_FLUSH_OK)
      {
        transactionState.setFLushCheckFail();
        return 1;
      }
      else
        return 0;
    }


    /**
      * Method  : doPrepareX
      * Params  : transactionId - transaction identifier
      *           location -
      * Return  : Commit vote (yes, no, read only)
      * Purpose : Call prepare for a given regionserver
     */
    public Integer doPrepareX(final long transactionId, final boolean skipConflict, final long startEpoch, final int participantNum,
                      final TransactionRegionLocation location, final boolean ignoreUnknownTransactionTimeline,
                      final String query, final short totalNum)

          throws IOException, CommitUnsuccessfulException {
       if (LOG.isDebugEnabled()) LOG.debug("doPrepareX -- ENTRY txid: " + transactionId + " skipConflict " + skipConflict
                                                       + " startEpoch " + startEpoch
                                                       + " participantNum " + participantNum
                                                       + " ignoreUnknownTransactionTimeline " + ignoreUnknownTransactionTimeline
                                                       + " location " + location + " query " + query );
       int commitStatus = 0;
       boolean refresh = false;
       boolean retry = false;
       boolean justrefreshed = false;
       int retryCount = 0;
       int retrySleep = TM_SLEEP;
       boolean sendAgain = false;

       if ((participantNum == 1) &&
           (g_createdTables.contains(table.getName().getNameAsString()))){
          g_createdTables.remove(table.getName().getNameAsString());
          sendAgain = true;
       }

       if( TRANSACTION_ALGORITHM == AlgorithmType.MVCC){
       do {
          retry = false;
          refresh = false;
          location.setReadOnly(false);
          try {
             final String regionName = lv_hri.getRegionNameAsString();
             Batch.Call<TrxRegionService, CommitRequestResponse> callable =
                new Batch.Call<TrxRegionService, CommitRequestResponse>() {
                   ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<CommitRequestResponse> rpcCallback =
                   new BlockingRpcCallback<CommitRequestResponse>();

                @Override
                public CommitRequestResponse call(TrxRegionService instance) throws IOException {
                   org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestRequest.Builder builder = CommitRequestRequest.newBuilder();
                   builder.setTransactionId(transactionId);

                   //reuse this for binlog max protection mode to transfer commitID
                   long cid = transactionState.getCommitId();
                   if(cid >0) 
                     builder.setSavepointId(cid); // This is not an endStatement request
                   else
                     builder.setSavepointId(-1); // This is not an endStatement request

                   builder.setStartEpoch(startEpoch);
                   builder.setSkipConflictDetection(skipConflict);
                   builder.setRegionName(ByteString.copyFromUtf8(regionName));
                   builder.setParticipantNum(participantNum);
                   builder.setIgnoreUnknownTransactionException(ignoreUnknownTransactionTimeline);
                   builder.setQueryContext(HBaseZeroCopyByteString.wrap(Bytes.toBytes(query)));
                   builder.setTotalNum(totalNum);

                   builder.setDropTableRecorded(location.isTableRecodedDropped());
                   instance.commitRequest(controller, builder.build(), rpcCallback);
                   return rpcCallback.get();
                }
             };

             Map<byte[], CommitRequestResponse> result = null;

             try {
                long timeCost = System.currentTimeMillis();
                result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                if (costTh >= 0) {
                  timeCost = System.currentTimeMillis() - timeCost;
                  if (timeCost >=  costTh)
                    LOG.warn("commitRequest copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                }
             } catch (ServiceException se) {
                String errMsg = new String("ERROR occurred while calling coprocessor service in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.warn(errMsg, se);
                throw new RetryTransactionException(errMsg, se);
             } catch (Throwable e) {
                String errMsg = new String("ERROR occurred while calling coprocessor service in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.error(errMsg, e);
                CommitUnsuccessfulException cue =  new CommitUnsuccessfulException(errMsg, e);
                throw cue;
             }

             if(result.size() == 0)  {
                int tmTableAttr = transactionState.getTmTableAttr(table.getName().getNameAsString());
                if (ignoreUnknownTransactionTimeline) {
                   if(LOG.isInfoEnabled()) LOG.info("doPrepareX(MVCC), received incorrect result size: " + result.size() + " for transaction "
                        + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                   if(LOG.isInfoEnabled()) LOG.info("doPrepareX(MVCC), ignore in Timeline read only case ");
                   retry = false; 
                   commitStatus = TransactionalReturn.COMMIT_OK;
                }
                else { // not Timeline attr
                    if (  (location.getPeerId() != 0) && (pSTRConfig.getConfiguredPeerCount() > 0) &&
                             ((tmTableAttr & SYNCHRONIZED) == SYNCHRONIZED)   ) {
//                             ((tmTableAttr & SYNCHRONIZED) == SYNCHRONIZED) && !isSTRUp(location.getPeerId())   ) {
                       refresh = false;
                       retry = false;
                       commitStatus = TransactionalReturn.COMMIT_UNSUCCESSFUL;
                       transactionState.recordException(
                                   "doPreparedX, received 0 region results for transaction, vote abort dur to peer SDN and sync table ");
                       // don't retry or reconnect, just abandon the txn if peer is SDN and this is sync table
                       LOG.warn("doPreparedX, received 0 region results for transaction " + transactionId + " vote abort since peer " +
                                   location.getPeerId() + " is SDN and table " +  table.getName().getNameAsString()  + " is with sync attr ");
                    }
                    else { // normal case, not optimized for xDC
                       String errMsg = new String("doPrepareX(MVCC), received incorrect result size: " + result.size() + " for transaction "
                           + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                       throw new RetryTransactionException(errMsg);
                    } // not optimized
                }
             }
             else if(result.size() == 1) {
               if(LOG.isTraceEnabled()) LOG.trace("doPrepareX(MVCC), received result size: " + result.size() + " for transaction "
                   + transactionId + " participantNum " + participantNum + " Location " + location);
                // size is 1
                for (CommitRequestResponse cresponse : result.values()){
                   // Should only be one result
                   int value = cresponse.getResult();
                   commitStatus = value;
                   String thisTableName = cresponse.getRegionName().toStringUtf8();
                   transactionState.addIntoTotalNumMap(thisTableName, commitStatus);
                   location.setBinlogWid(cresponse.getCurrentWid());
                   if(LOG.isTraceEnabled()) LOG.trace("doPrepareX(MVCC), transaction "
                         + transactionId + " result: " + commitStatus
                         + " from participant participantNum " + participantNum + " Location " + location);
                   if(cresponse.getHasException()) {
                      switch(commitStatus){
                        case TransactionalReturn.COMMIT_CONFLICT:
                        case TransactionalReturn.COMMIT_DOOMED:
                        case TransactionalReturn.EPOCH_VIOLATION:
                          transactionState.recordException(cresponse.getException());
                          sendAgain = false;
                          break;
                        case TransactionalReturn.COMMIT_SHIELDED:
                          retry = false;
                          if(LOG.isDebugEnabled()) LOG.debug("doPrepareX(MVCC), received COMMIT_SHIELDED response for transaction "
                                      + transactionId + " participantNum " + participantNum + " Location "
                                      + location.getRegionInfo().getRegionNameAsString());
                          break;
                        default:
                          if(transactionState.hasRetried() &&
                             cresponse.getException().contains("UnknownTransactionException")) {
                               retry = false;
                               commitStatus = TransactionalReturn.COMMIT_OK;
                          }
                          else {
                             if (LOG.isTraceEnabled()) LOG.trace("doPrepareX coprocessor exception: " + cresponse.getException());
                             throw new RetryTransactionException(cresponse.getException());
                          }
                      }
                   }
                   if (value == TransactionalReturn.COMMIT_RESEND) {
                     int count = 0;
                     // Handle situation where repeated region is in list due to different endKeys
                     String tblName = location.getRegionInfo().getTable().getNameAsString();
                     HashMap<ByteArrayKey,TransactionRegionLocation> regionMap =  
                          transactionState.getParticipatingRegions().getList(location.getPeerId()).get(tblName);
                     if (regionMap != null) {
                        for (TransactionRegionLocation trl : regionMap.values()) {
                           if (Arrays.equals(trl.getRegionInfo().getStartKey(),
                                  location.getRegionInfo().getStartKey())) 
                              count++;
                        }
                     }

                     if(count > 1) {
                       commitStatus = TransactionalReturn.COMMIT_OK;
                       retry = false;
                     }
                     else {
                       retry = true;
                     }
                   }
                   else if (value == TransactionalReturn.PREPARE_REFRESH) {
                       if (LOG.isInfoEnabled()) LOG.info("TM Prepare Refresh txId: " + transactionId
                                 + " participantNum " + participantNum + " Location " + location);

                       // Need to set prepareRefresh to true so we ignoreUnknownTransaction to handle
                       // the case where a transaction has a table that is read-only and another
                       // table requests a prepare refresh.  In this case, the read-only table
                       // would throw a UTE on the commit.
                       transactionState.setPrepareRefresh(true);
                       sendAgain = true;
                       commitStatus = 0;
                   }
                   else {
                     retry = false;
                   }
                }
                if (sendAgain){
                    // This is the first prepare for a table just created.  Sometimes
                    // we don't broadcast to all the regions correctly on the first send, so
                    // we work around this HBase issue by sending the prepare twice.
                    if(LOG.isTraceEnabled()) LOG.trace("doPrepareX(MVCC) single reply, resending prepare for recently created table "
                            + table.getName().getNameAsString() + " for transaction "
                            + transactionId);
                    sendAgain = false;
                    retry = true;
                    refresh = true;
                }
                if (commitStatus == TransactionalReturn.COMMIT_OK_READ_ONLY) {
                    location.setReadOnly(true);
                    commitStatus = TransactionalReturn.COMMIT_OK;
                }
             }
             else {
//               location.setMustBroadcast(true);
               transactionState.setMustBroadcast(true);
               if(LOG.isInfoEnabled()) LOG.info("doPrepareX(MVCC), received result size: " + result.size() + " for transaction "
                         + transactionId + " participantNum " + participantNum + " mustBroadcast is true for location " + location);
               for(CommitRequestResponse cresponse : result.values()) {
                 if(cresponse.getResult() == TransactionalReturn.COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR ||
                  cresponse.getResult() == TransactionalReturn.COMMIT_CONFLICT ||
                  cresponse.getResult() == TransactionalReturn.COMMIT_DOOMED ||
                  cresponse.getResult() == TransactionalReturn.COMMIT_SHIELDED ||
                  cresponse.getResult() == TransactionalReturn.COMMIT_UNSUCCESSFUL ||
                  cresponse.getResult() == TransactionalReturn.EPOCH_VIOLATION ||
                  cresponse.getResult() == TransactionalReturn.PREPARE_REFRESH ||
                  commitStatus == 0) {
                     commitStatus = cresponse.getResult();

                     if(cresponse.getHasException()) {
                       switch (commitStatus){
                         case TransactionalReturn.COMMIT_CONFLICT:
                         case TransactionalReturn.COMMIT_DOOMED:
                         case TransactionalReturn.EPOCH_VIOLATION:
                           transactionState.recordException(cresponse.getException());
                           sendAgain = false;
                           break;
                         case TransactionalReturn.COMMIT_SHIELDED:
                           retry = false;
                           if(LOG.isTraceEnabled()) LOG.trace("doPrepareX(MVCC), received result COMMIT_SHIELDED for transaction "
                                   + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                           break;
                         default:
                           if (LOG.isTraceEnabled()) LOG.trace("doPrepareX coprocessor exception: " +
                                     cresponse.getException());
                           throw new RetryTransactionException(cresponse.getException());
                       }
                     } // has exception*
                     else if(commitStatus == TransactionalReturn.PREPARE_REFRESH){
                         sendAgain = true;
                     }
                 } // if non-ok commit status or exception
               } // for


               if( commitStatus == TransactionalReturn.COMMIT_OK_READ_ONLY )
                 location.setReadOnly(true);

                 if(commitStatus == TransactionalReturn.COMMIT_OK ||
                    commitStatus == TransactionalReturn.COMMIT_OK_READ_ONLY ||
                    commitStatus == TransactionalReturn.COMMIT_RESEND) {
                   commitStatus = TransactionalReturn.COMMIT_OK;
                 }

                 
               retry = false;
               if (sendAgain){
                    // This is the first prepare for a table just created.  Sometimes
                    // we don't broadcast to all the regions correctly on the first send, so
                    // we work around this HBase issue by sending the prepare twice.
                    if(LOG.isTraceEnabled()) LOG.trace("doPrepareX(MVCC) multiple replies, resending prepare for recently created table "
                            + table.getName().getNameAsString() + " for transaction "
                            + transactionId);
                    sendAgain = false;
                    retry = true;
                    refresh = true;
                    commitStatus = 0;
               }
             } // else
          } // try
          catch(RetryTransactionException rte) {
             if (retryCount == RETRY_ATTEMPTS) {
                String errMsg = new String("Exceeded retry attempts in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.error(errMsg, rte);
                CommitUnsuccessfulException cue = new CommitUnsuccessfulException(errMsg, rte);
                throw cue;
             }
             LOG.warn("doPrepareX participant " + participantNum + " retrying transaction "
                          + transactionId + " due to Exception: " , rte);
             refresh = true;
             retry = true;
          }
          if (refresh) {
              
            if(justrefreshed){
                //previous try after refresh did not help.
                //now sleep briefly before another refresh.
                retrySleep = retry(retrySleep);
                justrefreshed = false;
            }
            try {
                refreshRegion("doPrepareX", transactionId, participantNum);
                refresh = false;
                justrefreshed = true;
            } catch (IOException e) {
                justrefreshed = false;
            }
         }
         if (retry && (!justrefreshed)){
             //This retry will pause briefly.
              if (LOG.isWarnEnabled()) LOG.warn("doPrepareX -- setting retry, count: " + retryCount);
             retrySleep = retry(retrySleep);
             if (LOG.isWarnEnabled()) LOG.warn("doPrepareX -- setting retry, count: " + retryCount);
         }
         transactionState.setRetried(true); 
       } while (retry && retryCount++ <= RETRY_ATTEMPTS);

       }
       if( TRANSACTION_ALGORITHM == AlgorithmType.SSCC){
       do {
          retry = false;
          refresh = false;
          try {
             final String regionName = lv_hri.getRegionNameAsString();
             Batch.Call<SsccRegionService, SsccCommitRequestResponse> callable =
                new Batch.Call<SsccRegionService, SsccCommitRequestResponse>() {
                   ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<SsccCommitRequestResponse> rpcCallback =
                   new BlockingRpcCallback<SsccCommitRequestResponse>();

                @Override
                public SsccCommitRequestResponse call(SsccRegionService instance) throws IOException {
                   org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitRequestRequest.Builder builder = SsccCommitRequestRequest.newBuilder();
                   builder.setTransactionId(transactionId);
                   builder.setSavepointId(-1); // This is not an endStatement request
                   builder.setRegionName(ByteString.copyFromUtf8(regionName));

                   instance.commitRequest(controller, builder.build(), rpcCallback);
                   return rpcCallback.get();
                }
             };

             Map<byte[], SsccCommitRequestResponse> result = null;

             try {
                long timeCost = System.currentTimeMillis();
                result = table.coprocessorService(SsccRegionService.class, startKey, endKey, callable);
                if (costTh >= 0) {
                  timeCost = System.currentTimeMillis() - timeCost;
                  if (timeCost >= costTh)
                    LOG.warn("commitRequest copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                }
             } catch (ServiceException se) {
                String errMsg = new String("ERROR occurred while calling coprocessor service in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.warn(errMsg, se);
                throw new RetryTransactionException("Unable to call prepare, coprocessor error", se);
             } catch (Throwable e) {
                String errMsg = new String("ERROR occurred while calling coprocessor service in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.error(errMsg, e);
                CommitUnsuccessfulException cue =  new CommitUnsuccessfulException(errMsg, e);
                throw cue;
             }
             if(result.size() != 1)  {
                String errMsg = new String("doPrepareX (SSCC), received incorrect result size: " + result.size());
                LOG.error(errMsg);
                throw new RetryTransactionException(errMsg);
             }
             else {
                // size is 1
                for (SsccCommitRequestResponse cresponse : result.values()){
                   // Should only be one result
                   int value = cresponse.getResult();
                   commitStatus = value;
                   if(cresponse.getHasException()) {
                      if (LOG.isTraceEnabled()) LOG.trace("doPrepareX coprocessor exception: " + cresponse.getException());
                      throw new RetryTransactionException(cresponse.getException());
                   }
                }
                retry = false;
             }
          }
          catch (RetryTransactionException rte) {
             if (retryCount == RETRY_ATTEMPTS) {
                String errMsg = new String("Exceeded " + retryCount + " retry attempts in doPrepareX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                LOG.error(errMsg, rte);
                CommitUnsuccessfulException cue =  new CommitUnsuccessfulException(errMsg, rte);
                throw cue;
             }
             LOG.warn("doPrepareX participant " + participantNum + " retrying transaction "
                      + transactionId + " due to Exception: ", rte);
             refresh = true;
             retry = true;
          }

          if (refresh) {
              
              if(justrefreshed){
                  //previous try after refresh did not help.
                  //now sleep briefly before another refresh.
                  retrySleep = retry(retrySleep);
                  justrefreshed = false;
              }
              try {
                  refreshRegion("doPrepareX", transactionId, participantNum);
                  refresh = false;
                  justrefreshed = true;
              } catch (IOException e) {
                  justrefreshed = false;
              }
           }
           if (retry && (!justrefreshed)){
               //This retry will pause briefly.
               retrySleep = retry(retrySleep);
               if (LOG.isWarnEnabled()) LOG.warn("doPrepareX -- setting retry, count: " + retryCount);
           }
       
       } while (retry && retryCount++ <= RETRY_ATTEMPTS);

       }
       if (LOG.isTraceEnabled()) LOG.trace("commitStatus: " + commitStatus + " for transId(" + transactionId
                       + ") TableName " + table.getName().getNameAsString());
       String errMsg = new String(" returned from commit request in doPrepareX for transaction "
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
       boolean canCommit = true;
       boolean readOnly = false;
       String tmpStatus = null;

       switch (commitStatus) {
          case TransactionalReturn.COMMIT_OK:
            break;
          case TransactionalReturn.COMMIT_RESEND:
          case TransactionalReturn.COMMIT_OK_READ_ONLY:
            if (transactionState.getMustBroadcast()){
               commitStatus = TransactionalReturn.COMMIT_BROADCAST;
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("Adding participant: " + participantNum + " for transId(" + transactionId
                        + ") TableName " + table.getName().getNameAsString() + " to regionsToIgnore. Location " + location);
               transactionState.addRegionToIgnore(location); // No need to doCommit for read-onlys
               readOnly = true;
            }
            break;
          case TransactionalReturn.COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR:
             tmpStatus = new String("COMMIT_UNSUCCESSFUL_FROM_COPROCESSOR");
             if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
             canCommit = false;
             if (! transactionState.getMustBroadcast()){
                transactionState.addRegionToIgnore(location); // No need to re-abort.
             }
             break;
          case TransactionalReturn.COMMIT_CONFLICT:
             tmpStatus = new String("COMMIT_CONFLICT");
             if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
             canCommit = false;
             if (! transactionState.getMustBroadcast()){
                transactionState.addRegionToIgnore(location); // No need to re-abort.
             }
             break;
          case TransactionalReturn.COMMIT_DOOMED:
             tmpStatus = new String("COMMIT_DOOMED");
             if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
             canCommit = false;
             if (! transactionState.getMustBroadcast()){
                transactionState.addRegionToIgnore(location); // No need to re-abort.
             }
             break;
          case TransactionalReturn.COMMIT_SHIELDED:
              tmpStatus = new String("COMMIT_SHIELDED");
              if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
              canCommit = false;
              if (! transactionState.getMustBroadcast()){
                 transactionState.addRegionToIgnore(location); // No need to re-abort.
              }
              break;
          case TransactionalReturn.COMMIT_UNSUCCESSFUL:
             tmpStatus = new String("COMMIT_UNSUCCESSFUL");
             if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
             canCommit = false;
             if (! transactionState.getMustBroadcast()){
                transactionState.addRegionToIgnore(location); // No need to re-abort.
             }
             break;
          case TransactionalReturn.EPOCH_VIOLATION:
              tmpStatus = new String("EPOCH_VIOLATION");
              if (LOG.isInfoEnabled()) LOG.info(tmpStatus + errMsg);
              canCommit = false;
              if (! transactionState.getMustBroadcast()){
                 transactionState.addRegionToIgnore(location); // No need to re-abort.
              }
              break;
          default:
             CommitUnsuccessfulException cue = new CommitUnsuccessfulException("commitStatus=" + commitStatus + errMsg);
             LOG.error("commitStatus=" + commitStatus + errMsg, cue);
             throw cue;
       }

       if (!canCommit) {
         // track regions which indicate they could not commit for better diagnostics
         LOG.warn("Region [" + location.getRegionInfo().getRegionNameAsString() + "] votes "
                 +  "to abort" + (readOnly ? " Read-only ":"") + " transaction "
                 + transactionState.getTransactionId());
         //System.out.println("Region [" + location.getRegionInfo().getRegionNameAsString() + "] votes "
         //        +  "to abort" + (readOnly ? " Read-only ":"") + " transaction "
         //        + transactionState.getTransactionId());
         switch (commitStatus) {
           case TransactionalReturn.COMMIT_CONFLICT:
             return TM_COMMIT_FALSE_CONFLICT;
           case TransactionalReturn.COMMIT_DOOMED:
             return TM_COMMIT_FALSE_DOOMED;
           case TransactionalReturn.COMMIT_SHIELDED:
             return TM_COMMIT_FALSE_SHIELDED;
           case TransactionalReturn.EPOCH_VIOLATION:
             return TM_COMMIT_FALSE_EPOCH_VIOLATION;

           default:
             return TM_COMMIT_FALSE;
         }
       }

       if (readOnly)
         return TM_COMMIT_READ_ONLY;

       return TM_COMMIT_TRUE;
  }

    /**
     * Method  : doAbortX
     * Params  : transactionId - transaction identifier
     * Return  : Ignored
     * Purpose : Call abort for a given regionserver
     */
    public Integer doAbortX(final long transactionId, final int participantNum,
               final boolean dropTableRecorded, boolean ignoreUnknownTransaction) throws IOException{
        if(LOG.isDebugEnabled()) LOG.debug("doAbortX -- ENTRY txID: " + transactionId + " participantNum "
                        + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString() +
                        " dropTableRecorded " + dropTableRecorded);
        boolean retry = false;
        boolean refresh = false;
        int retryCount = 0;
            int retrySleep = TM_SLEEP;

        if( TRANSACTION_ALGORITHM == AlgorithmType.MVCC) {
        do {
            retry = false;
            refresh = false;
            try {

              Batch.Call<TrxRegionService, AbortTransactionResponse> callable =
                new Batch.Call<TrxRegionService, AbortTransactionResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<AbortTransactionResponse> rpcCallback =
                new BlockingRpcCallback<AbortTransactionResponse>();

              @Override
              public AbortTransactionResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionRequest.Builder builder = AbortTransactionRequest.newBuilder();
                builder.setTransactionId(transactionId);
                builder.setParticipantNum(participantNum);
                builder.setRegionName(ByteString.copyFromUtf8(""));
                builder.setDropTableRecorded(dropTableRecorded);
                builder.setIgnoreUnknownTransactionException(true);
                instance.abortTransaction(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            Map<byte[], AbortTransactionResponse> result = null;
              try {
                 if (LOG.isTraceEnabled()) LOG.trace("doAbortX -- before coprocessorService txid: "
                        + transactionId + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));

                 // reload region location information.
                 if (enableRowLevelLock) {
                     table.getRegionLocation(startKey, /* reload */ true);
                 }
                 long timeCost = System.currentTimeMillis();
                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                 if (costTh >= 0) {
                   timeCost = System.currentTimeMillis() - timeCost;
                   if (timeCost >= costTh)
                     LOG.warn("doAbortX copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                 }
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doAbortX coprocessor service";
                  LOG.warn(msg,  se);
                  throw new RetryTransactionException(msg, se);
              } catch (TableNotFoundException tnfe) {
                  String msg = "WARNING while calling doAbortX for table: "
                          + table.getName().getNameAsString() + ", ignoring ";
                  LOG.warn(msg,  tnfe);
                  transactionState.requestPendingCountDec(null);
                  return 0;
              } catch (Throwable t) {
                  String msg = "ERROR occurred while calling doAbortX coprocessor service";
                  LOG.error(msg,  t);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg, t);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if(result.size() == 0) {
                 boolean needRetry = true;
                 LOG.error("doAbortX, received 0 region results for transaction " + transactionId
                                  + " participantNum: " + participantNum + " reconnect, refresh, and retry, peerId " + location.getPeerId());

                 //Map<Long, TransactionState> mapT = HBaseTxClient.getMap();
                 //TransactionState ts = mapT.get(transactionId);
                 int tmTableAttr = transactionState.getTmTableAttr(table.getName().getNameAsString());
                 LOG.info("doAbortX, received 0 region results for transaction: table " + table.getName().getNameAsString() + 
                                        " table attr: " + tmTableAttr + " STRUP for peer " +
                                        location.getPeerId() + " " + isSTRUp(location.getPeerId())     );
                 if (  (location.getPeerId() != 0) && (pSTRConfig.getConfiguredPeerCount() > 0) &&
                             ((tmTableAttr & SYNCHRONIZED) == SYNCHRONIZED) && !isSTRUp(location.getPeerId())   ) {
                    needRetry = false;
                    refresh = false;
                    retry = false;
                    // don't retry or reconnect, just abandon abort if peer is SDN and this is sync table
                    LOG.info("doAbortX, received 0 region results for transaction " + transactionId + " abandon abort since peer " +
                                   location.getPeerId() + " is SDN and table " +  table.getName().getNameAsString()  + " is with sync attr ");
                 }

                 try {
                    if (needRetry) {
                       table.close();
                       connection.close();
                       connection = ConnectionFactory.createConnection(pSTRConfig.getPeerConfiguration(location.getPeerId()));
                       table = new TransactionalTable(location.getRegionInfo().getTable(), connection, cp_tpe, intThreads);
                       refresh = true;
                       retry = true;
                     }
                  } catch (Throwable thr) {
                    LOG.warn("doAbortX, ERROR occurred while recreating connection",  thr);
                    transactionState.requestPendingCountDec(null);
                    return 0;
                  }
              }
              else {
                 for (AbortTransactionResponse cresponse : result.values()) {
                   if (cresponse.getHasException()) {
                      String exceptionString = cresponse.getException();
                      if (exceptionString.contains("UnknownTransactionException"))
                         throw new UnknownTransactionException(exceptionString);
                      else if (exceptionString.contains("NonPendingTransactionException"))
                         throw new NonPendingTransactionException(exceptionString);
                      else
                         throw new RetryTransactionException(cresponse.getException());
                   }
                 }
                 retry = false;
              }
          }
          catch (UnknownTransactionException ute) {
             String errMsg = new String("doAbortX UnknownTransactionException for transaction "
                              + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
             if (ignoreUnknownTransaction) {
                LOG.info(errMsg + " ,but ignored", ute);
                transactionState.requestPendingCountDec(null);
             }
             else {
                LOG.error(errMsg, ute);
                transactionState.logExceptionDetails(location.getPeerId(), true, participantNum);
                transactionState.requestPendingCountDec(null);
             }
          }
          catch (NonPendingTransactionException npte) {
             String errMsg = new String("doAbortX NonPendingTransactionException for transaction "
                                 + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
             LOG.error(errMsg, npte);
             transactionState.logExceptionDetails(location.getPeerId(), false, participantNum);
             transactionState.requestPendingCountDec(null);
          }
          catch (RetryTransactionException rte) {
              if (retryCount == RETRY_ATTEMPTS) {
                 String errMsg = new String("Exceeded " + retryCount + " retry attempts in doAbortX for transaction " 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                 DoNotRetryIOException dnre = new DoNotRetryIOException(errMsg, rte);
                 LOG.error(errMsg, dnre);
                 transactionState.requestPendingCountDec(dnre);
                 throw dnre;
              }
              else if (rte.toString().contains("Asked to commit a non-pending transaction ")) {
                 String errMsg = new String("doAbortX will not retry transaction" 
                     + transactionId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                 LOG.warn(errMsg,rte);
                 refresh = false;
                 retry = false;
              }
              else {
                  LOG.warn("doAbortX retrying " + retryCount + " time for transaction " + transactionId + " participantNum: "
                      + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                 refresh = true;
                 retry = true;
              }
            }
            if (refresh) {
                Admin admin = null;
                RegionLocator locator = null;
                HRegionLocation lv_hrl = null;
                try {
                   table.clearRegionCache();
                   admin = connection.getAdmin();
                   locator = connection.getRegionLocator(table.getName());
                   if (admin.isTableEnabled(table.getName())) {
                      lv_hrl = locator.getRegionLocation(startKey, true);
                      if (LOG.isInfoEnabled()) LOG.info("doAbortX -- location being refreshed : "
                           + lv_hrl.getRegionInfo().getRegionNameAsString() + "endKey: "
                           + Hex.encodeHexString(lv_hrl.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
                   }
                   else {
                      LOG.error("doAbortX -- table: " + table.getName().getNameAsString() + " is disabled, ignoring table and returning");
                      transactionState.requestPendingCountDec(null);
                      return 0;
                   }
                }
                catch (Throwable thr){
                   LOG.warn("doAbortX refresh; getRegionLocation exception for transaction " + transactionId + " participantNum: "
                            + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString() + " ", thr);
                   transactionState.requestPendingCountDec(null);
                   return 0;
                }
                finally {
                   try {
                      if (admin != null) {
                         admin.close();
                         admin = null;
                      }
                      if (locator != null) {
                         locator.close();
                         locator = null;
                      }
                   } catch(Throwable thr) {
                      retry = false;
                      String errMsg = new String("doAbortX -- failed to close. transaction id is " + transactionId);
                      LOG.error(errMsg, thr);
                   }
                }
                if (LOG.isWarnEnabled()) LOG.warn("doAbortX -- setting retry, count: " + retryCount);
                refresh = false;
            }
            if (retry)
               retrySleep = retry(retrySleep);
          } while (retry && retryCount++ <= RETRY_ATTEMPTS);
        }

        if( TRANSACTION_ALGORITHM == AlgorithmType.SSCC){
        do {
             retry = false;
             refresh = false;
             try {

              Batch.Call<SsccRegionService, SsccAbortTransactionResponse> callable =
                new Batch.Call<SsccRegionService, SsccAbortTransactionResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<SsccAbortTransactionResponse> rpcCallback =
                new BlockingRpcCallback<SsccAbortTransactionResponse>();

              @Override
              public SsccAbortTransactionResponse call(SsccRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortTransactionRequest.Builder builder = SsccAbortTransactionRequest.newBuilder();
                builder.setTransactionId(transactionId);
                builder.setRegionName(ByteString.copyFromUtf8(""));

                instance.abortTransaction(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            Map<byte[], SsccAbortTransactionResponse> result = null;
              try {
                  if (LOG.isTraceEnabled()) LOG.trace("doAbortX -- before coprocessorService txid: " + transactionId
                          + " table: " + table.getName().getNameAsString()
                          + " startKey " + ((startKey != null) ?
                                  (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                          + " endKey " +  ((endKey != null) ?
                                  (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));

                  long timeCost = System.currentTimeMillis();
                  result = table.coprocessorService(SsccRegionService.class, startKey, endKey, callable);
                  if (costTh >= 0) {
                    timeCost = System.currentTimeMillis() - timeCost;
                    if (timeCost >= costTh)
                      LOG.warn("doAbortX copro PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + table.getName());
                  }
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doAbortX coprocessor service";
                  LOG.warn(msg + ":",  se);
                  throw new RetryTransactionException(msg, se);
              } catch (Throwable e) {
                  String msg = "ERROR occurred while calling doAbortX coprocessor service";
                  LOG.error(msg + ":",  e);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg,e);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if (result.size() != 1) {
                 LOG.error("doAbortX, received incorrect result size: " + result.size());
                 refresh = true;
                 retry = true;
              }
              else {
                 for (SsccAbortTransactionResponse cresponse : result.values()) {
                    if (cresponse.getHasException()) {
                       String exceptionString = cresponse.getException();
                       if (exceptionString.contains("UnknownTransactionException")) {
                          throw new UnknownTransactionException(exceptionString);
                       }
                       throw new RetryTransactionException(cresponse.getException());
                    }
                 }
              }
              retry = false;
          }
          catch (UnknownTransactionException ute) {
             String errMsg = new String("Got unknown exception in doAbortX by participant " + participantNum
                       + " for transaction " + transactionId);
             if (ignoreUnknownTransaction) {
                LOG.info(errMsg + " ,but ignored", ute);
                transactionState.requestPendingCountDec(null);
             }
             else {
                LOG.error(errMsg, ute);
                transactionState.logExceptionDetails(location.getPeerId(), true, participantNum);
                transactionState.requestPendingCountDec(null);
             }
          }
          catch (RetryTransactionException rte) {
              if (retryCount == RETRY_ATTEMPTS){
                   String errMsg = new String ("Exceeded retry attempts in doAbortX: " + retryCount + " (Not ingoring)");
                   LOG.error(errMsg);
                   RollbackUnsuccessfulException rue = new RollbackUnsuccessfulException(errMsg, rte);
                   transactionState.requestPendingCountDec(rue);
                   throw rue;
              }
              LOG.warn("doAbortX participant " + participantNum + " retrying transaction "
                      + transactionId + " due to Exception: " + rte);
              refresh = true;
              retry = true;
          }
              if (refresh) {
                 Admin admin = null;
                 RegionLocator locator = null;
                 HRegionLocation lv_hrl = null;

                 try {
                    table.clearRegionCache();
                    admin = connection.getAdmin();
                    locator = connection.getRegionLocator(table.getName());
                    if (admin.isTableEnabled(table.getName())) {
                       lv_hrl = locator.getRegionLocation(startKey, true);
                       if (LOG.isInfoEnabled()) LOG.info("doAbortX -- location being refreshed : "
                             + lv_hrl.getRegionInfo().getRegionNameAsString() + "endKey: "
                             + Hex.encodeHexString(lv_hrl.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
                    }
                    else {
                       LOG.error("doAbortX -- table: " + table.getName().getNameAsString() + " is disabled, ignoring table and returning");
                       transactionState.requestPendingCountDec(null);
                       return 0;
                    }
                 }
                 catch (Throwable thr) {
                    LOG.warn("doAbortX refresh; getRegionLocation exception for transaction " + transactionId + " participantNum: "
                             + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString() + " ", thr);
                    transactionState.requestPendingCountDec(null);
                    return 0;
                 }
                 finally {
                   try {
                       if (admin != null) {
                          admin.close();
                          admin = null;
                       }
                       if (locator != null) {
                          locator.close();
                          locator = null;
                       }
                    } catch(Throwable thr) {
                      retry = false;
                      String errMsg = new String("doAbortX -- failed to close. transaction id is " + transactionId);
                      LOG.error(errMsg, thr); 
                   }
                 }
                 if (LOG.isWarnEnabled()) LOG.warn("doAbortX -- setting retry, count: " + retryCount);
                 refresh = false;
              }
              if (retry)
                 retrySleep = retry(retrySleep);
           } while (retry && retryCount++ <= RETRY_ATTEMPTS);

        }
      // We have received our reply so decrement outstanding count
      transactionState.requestPendingCountDec(null);

      if(LOG.isTraceEnabled()) LOG.trace("doAbortX -- EXIT txID: " + transactionId);
      return 0;
    }

    /**
     * Method  : doAbortSavepointX
     * Params  : transactionId - transaction identifier
     *         : savepointId - savepoint identifier
     *         : participantNum - number of the participant in the participating list
     *         : IgnnoreUnknownTransaction -
     * Return  : Ignored
     * Purpose : Call abortSavepoint for a given regionserver
     */
    public Integer doAbortSavepointX(final long transactionId,
           final long savepointId, final long pSavepointId,
           final int participantNum, final String regionName, boolean ignoreUnknownTransaction) throws IOException{
        if(LOG.isDebugEnabled()) LOG.debug("doAbortSavepointX -- ENTRY txID: " + transactionId + " savepointId " + savepointId
              + " participantNum " + participantNum);
        boolean isRefreshed = false;
        boolean retry = false;
        boolean refresh = false;
        int retryCount = 0;
        int retrySleep = TM_SLEEP;

        if( TRANSACTION_ALGORITHM == AlgorithmType.MVCC) {
        do {
            retry = false;
            refresh = false;
            try {
              Batch.Call<TrxRegionService, AbortSavepointResponse> callable = null;

              // ignore region name check after region refreshed
              if (isRefreshed) {
                callable = new Batch.Call<TrxRegionService, AbortSavepointResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<AbortSavepointResponse> rpcCallback =
                    new BlockingRpcCallback<AbortSavepointResponse>();

                  @Override
                  public AbortSavepointResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortSavepointRequest.Builder builder = AbortSavepointRequest.newBuilder();
                    builder.setTransactionId(transactionId);
                    builder.setSavepointId(savepointId);
                    builder.setPSavepointId(pSavepointId);
                    builder.setParticipantNum(participantNum);
                    builder.setRegionName(ByteString.copyFromUtf8("")); //ignore region name to skip region name check
                    builder.setIgnoreUnknownTransactionException(true);
                    instance.abortSavepoint(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                  }
                };
              }
              // region name needs to be verified after region splits
              else {
                callable = new Batch.Call<TrxRegionService, AbortSavepointResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<AbortSavepointResponse> rpcCallback =
                    new BlockingRpcCallback<AbortSavepointResponse>();

                  @Override
                  public AbortSavepointResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortSavepointRequest.Builder builder = AbortSavepointRequest.newBuilder();
                    builder.setTransactionId(transactionId);
                    builder.setSavepointId(savepointId);
                    builder.setPSavepointId(pSavepointId);
                    builder.setParticipantNum(participantNum);
                    builder.setRegionName(ByteString.copyFromUtf8(regionName)); //given region name
                    builder.setIgnoreUnknownTransactionException(true);
                    instance.abortSavepoint(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                  }
                };
              }

              Map<byte[], AbortSavepointResponse> result = null;
              try {
                 if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepointX -- before coprocessorService txid: "
                        + transactionId + " savepointId " + savepointId + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));

                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doAbortSavepointX coprocessor service";
                  LOG.warn(msg,  se);
                  throw new RetryTransactionException(msg, se);
              } catch (Throwable t) {
                  String msg = "ERROR occurred while calling doAbortSavepointX coprocessor service";
                  LOG.error(msg,  t);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg, t);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepointX result size is : " + result.size());
              if(result.size() == 0) {
                 LOG.error("doAbortSavepointX, received 0 region results for transaction " + transactionId
                    + " savepointId " + savepointId + " participantNum: " + participantNum);
                 refresh = true;
                 retry = true;
              }
              else {
                 retry = false;
                 for (AbortSavepointResponse cresponse : result.values()) {
                   if (cresponse.getHasException()) {
                      String exceptionString = cresponse.getException();
                      if (exceptionString.contains("NonPendingTransactionException"))
                         throw new NonPendingTransactionException(exceptionString);
                      else
                         throw new RetryTransactionException(cresponse.getException());
                   }
                   else if (cresponse.getResult() == TransactionalReturn.PREPARE_REFRESH) {
                     LOG.warn("doAbortSavepointX received PREPARE_REFRESH message, will retry after refresh region");
                     refresh = true;
                     retry = true;
                     break;
                   }
                 }
              }
          }
          catch (NonPendingTransactionException npte) {
             String errMsg = new String("doAbortSavepointX NonPendingTransactionException for transaction "
                 + transactionId + " savepointId " + savepointId + " participantNum " + participantNum
                 + " Location " + location.getRegionInfo().getRegionNameAsString());
             LOG.error(errMsg, npte);
             transactionState.logExceptionDetails(location.getPeerId(), false, participantNum);
             transactionState.requestPendingCountDec(null);
             DoNotRetryIOException dnre = new DoNotRetryIOException(errMsg, npte);
             throw dnre;
          }
          catch (RetryTransactionException rte) {
             if (retryCount == RETRY_ATTEMPTS) {
                String errMsg = new String("Exceeded " + retryCount + " retry attempts in doAbortSavepointX for transaction "
                     + transactionId + " savepointId " + savepointId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                DoNotRetryIOException dnre = new DoNotRetryIOException(errMsg, rte);
                LOG.error(errMsg, dnre);
                transactionState.requestPendingCountDec(dnre);
                throw dnre;
             }
             else {
                LOG.warn("doAbortSavepointX retrying " + retryCount + " time for transaction " + transactionId
                      + " savepointId " + savepointId + " participantNum: "
                      + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                refresh = true;
                retry = true;
             }
          }
          if (retry){
             retrySleep = retry(retrySleep);
          }
          if (refresh) {

             // Make sure we refresh after sleeping in above retry so that regions
             // have a chance to relocate if necessary
             RegionLocator locator = null;
             Admin admin = null;
             boolean tblEnabled = false;
             if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepointX -- location being refreshed : " + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                     + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
                 try {
                   table.clearRegionCache();
                   admin = connection.getAdmin();
                   locator = connection.getRegionLocator(table.getName());
                   tblEnabled = admin.isTableEnabled(table.getName());
                   if (tblEnabled)
                     locator.getRegionLocation(startKey, true);
                 } catch(IOException ioe) {
                   String errMsg = new String("doAbortSavepointX -- failed to refresh location for table " + table.getName() + " within transaction " + transactionId);
                   LOG.error(errMsg, ioe);
                   transactionState.requestPendingCountDec(null);
                   throw ioe;
                 } catch(Throwable thr) {
                   String errMsg = new String("doAbortSavepointX -- failed to refresh location for table " + table.getName() +  " within transaction " + transactionId);
                   LOG.error(errMsg, thr);
                   transactionState.requestPendingCountDec(null);
                   throw new IOException(thr);
                 } finally {
                   try {
                     if (locator != null) {
                       locator.close();
                       locator = null;
                     }
                     if (admin != null) {
                       admin.close();
                       admin = null;
                     }
                   } catch(Throwable thr) {
                     retry = false;
                     String errMsg = new String("doAbortSavepointX -- failed to close. transaction id is " + transactionId);
                     LOG.error(errMsg, thr);
                   }
                 }
                 if (tblEnabled == false) {
                    LOG.error("doAbortSavepointX -- table: " + table.getName().getNameAsString() + " is disabled, unable to abortSavepoint");
                    transactionState.requestPendingCountDec(null);
                    DoNotRetryIOException dnre = new DoNotRetryIOException("doAbortSavepointX -- table: "
                               + table.getName().getNameAsString() + " is disabled, unable to abortSavepoint");
                    throw dnre;
                 }
                 if (LOG.isWarnEnabled()) LOG.warn("doAbortSavepointX -- setting retry, count: " + retryCount);
                 refresh = false;
                 isRefreshed = true;
            }
          } while (retry && retryCount++ <= RETRY_ATTEMPTS);
        }

        if( TRANSACTION_ALGORITHM == AlgorithmType.SSCC){
        do {
             retry = false;
             refresh = false;
             try {

              Batch.Call<SsccRegionService, SsccAbortSavepointResponse> callable =
                new Batch.Call<SsccRegionService, SsccAbortSavepointResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<SsccAbortSavepointResponse> rpcCallback =
                new BlockingRpcCallback<SsccAbortSavepointResponse>();

              @Override
              public SsccAbortSavepointResponse call(SsccRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccAbortSavepointRequest.Builder builder = SsccAbortSavepointRequest.newBuilder();
                builder.setTransactionId(transactionId);
                builder.setSavepointId(savepointId);
                builder.setRegionName(ByteString.copyFromUtf8(""));

                instance.abortSavepoint(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            Map<byte[], SsccAbortSavepointResponse> result = null;
              try {
                  if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepointX -- before coprocessorService txid: " + transactionId
                        + " savepointId " + savepointId + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));
                  result = table.coprocessorService(SsccRegionService.class, startKey, endKey, callable);
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doAbortSavepointX coprocessor service";
                  LOG.warn(msg + ":",  se);
                  throw new RetryTransactionException(msg, se);
              } catch (Throwable e) {
                  String msg = "ERROR occurred while calling doAbortSavepointX coprocessor service";
                  LOG.error(msg + ":",  e);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg,e);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if (result.size() != 1) {
                 LOG.error("doAbortSavepointX, received incorrect result size: " + result.size()
                     + " for transaction " + transactionId + " savepointId " + savepointId );
                 refresh = true;
                 retry = true;
              }
              else {
                 for (SsccAbortSavepointResponse cresponse : result.values()) {
                    if (cresponse.getHasException()) {
                       throw new RetryTransactionException(cresponse.getException());
                    }
                 }
              }
              retry = false;
          }
          catch (RetryTransactionException rte) {
              if (retryCount == RETRY_ATTEMPTS){
                   String errMsg = new String ("Exceeded retry attempts in doAbortSavepointX: " + retryCount + " (Not ingoring)");
                   LOG.error(errMsg);
                   RollbackUnsuccessfulException rue = new RollbackUnsuccessfulException(errMsg, rte);
                   transactionState.requestPendingCountDec(rue);
                   throw rue;
              }
              LOG.warn("doAbortSavepointX participant " + participantNum + " retrying transaction "
                      + transactionId + " savepointId " + savepointId  + " due to Exception: " + rte);
              refresh = true;
              retry = true;
          }
          if (retry){
             retrySleep = retry(retrySleep);
          }
          if (refresh) {
             // Make sure we refresh after sleeping in above retry so that regions
             // have a chance to relocate if necessary
             RegionLocator locator = null;
             Admin admin = null;
             boolean tblEnabled = false;
             if (LOG.isInfoEnabled()) LOG.info("doAbortSavepointX -- location being refreshed : "
                    + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                    + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                    + " for transaction " + transactionId + " savepointId " + savepointId);
             try {
                 table.clearRegionCache();
                 admin = connection.getAdmin();
                 locator = connection.getRegionLocator(table.getName());
                 tblEnabled = admin.isTableEnabled(table.getName());
                 if (tblEnabled)
                     locator.getRegionLocation(startKey, true);
             } catch(IOException ioe) {
                 String errMsg = new String("doAbortSavepointX -- failed to refresh location for table " + table.getName() + " within transaction " + transactionId);
                 LOG.error(errMsg, ioe);
                 transactionState.requestPendingCountDec(null);
                 throw ioe;
             } catch(Throwable thr) { 
                 String errMsg = new String("doAbortSavepointX -- failed to refresh location for table " + table.getName() +  " within transaction " + transactionId);
                 LOG.error(errMsg, thr);
                 transactionState.requestPendingCountDec(null);
                 throw new IOException(thr);
             } finally {
                 try {
                     if (locator != null) {
                        locator.close();
                        locator = null;
                     }
                     if (admin != null) {
                        admin.close();
                        admin = null;
                     }
                 } catch(Throwable thr) {
                     retry = false;
                     String errMsg = new String("doAbortSavepointX -- failed to close. transaction id is " + transactionId);
                     LOG.error(errMsg, thr);
                 }
             }
             if (tblEnabled == false) {
                LOG.error("doAbortSavepointX -- table: " + table.getName().getNameAsString() + " is disabled, unable to abortSavepoint");
                transactionState.requestPendingCountDec(null);
                DoNotRetryIOException dnre = new DoNotRetryIOException("doAbortSavepointX -- table: "
                        + table.getName().getNameAsString() + " is disabled, unable to abortSavepoint");
                throw dnre;
             }
             if (LOG.isWarnEnabled()) LOG.warn("doAbortSavepointX -- setting retry, count: " + retryCount);
             refresh = false;
          }
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);

      }

      // We have received our reply so decrement outstanding count
      transactionState.requestPendingCountDec(null);

      if(LOG.isTraceEnabled()) LOG.trace("doAbortSavepointX -- EXIT txID: " + transactionId + " savepointId " + savepointId );
      return 0;
    }

    /**
     * Method  : doCommitSavepointX
     * Params  : transactionId - transaction identifier
     *         : savepointId - savepoint identifier
     *         : participantNum - number of the participant in the participating list
     *         : IgnnoreUnknownTransaction -
     * Return  : Ignored
     * Purpose : Call commitSavepoint for a given regionserver
     */
    public Integer doCommitSavepointX(final long transactionId, final long savepointId, final long pSavepointId,
           final int participantNum, final String regionName, boolean ignoreUnknownTransaction) throws IOException{
        if(LOG.isDebugEnabled()) LOG.debug("doCommitSavepointX -- ENTRY txID: " + transactionId + " savepointId " + savepointId
              + " participantNum " + participantNum);
        boolean isRefreshed = false;
        boolean retry = false;
        boolean refresh = false;
        int retryCount = 0;
        int retrySleep = TM_SLEEP;

        if( TRANSACTION_ALGORITHM == AlgorithmType.MVCC) {
        do {
            retry = false;
            refresh = false;
            try {

              Batch.Call<TrxRegionService, CommitSavepointResponse> callable = null;
              if (isRefreshed) {
                callable = new Batch.Call<TrxRegionService, CommitSavepointResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<CommitSavepointResponse> rpcCallback =
                    new BlockingRpcCallback<CommitSavepointResponse>();

                  @Override
                  public CommitSavepointResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitSavepointRequest.Builder builder = CommitSavepointRequest.newBuilder();
                    builder.setTransactionId(transactionId);
                    builder.setSavepointId(savepointId);
                    builder.setPSavepointId(pSavepointId);
                    builder.setParticipantNum(participantNum);
                    builder.setRegionName(ByteString.copyFromUtf8(""));
                    builder.setIgnoreUnknownTransactionException(true);
                    instance.commitSavepoint(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                  }
                };
              }
              else {
                callable = new Batch.Call<TrxRegionService, CommitSavepointResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<CommitSavepointResponse> rpcCallback =
                    new BlockingRpcCallback<CommitSavepointResponse>();

                  @Override
                  public CommitSavepointResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitSavepointRequest.Builder builder = CommitSavepointRequest.newBuilder();
                    builder.setTransactionId(transactionId);
                    builder.setSavepointId(savepointId);
                    builder.setPSavepointId(pSavepointId);
                    builder.setParticipantNum(participantNum);
                    builder.setRegionName(ByteString.copyFromUtf8(regionName));
                    builder.setIgnoreUnknownTransactionException(true);
                    instance.commitSavepoint(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                  }
                };
              }

            Map<byte[], CommitSavepointResponse> result = null;
              try {
                 if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepointX -- before coprocessorService txid: "
                        + transactionId + " savepointId " + savepointId + " table: " + table.getName().getNameAsString() + " startKey: "
                        + ((startKey != null) ?
                                (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                                (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));
                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doCommitSavepointX coprocessor service";
                  LOG.warn(msg,  se);
                  throw new RetryTransactionException(msg, se);
              } catch (Throwable t) {
                  String msg = "ERROR occurred while calling doCommitSavepointX coprocessor service";
                  LOG.error(msg,  t);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg, t);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepointX result size is : " + result.size());
              if(result.size() == 0) {
                 LOG.error("doCommitSavepointX, received 0 region results for transaction " + transactionId
                    + " savepointId " + savepointId + " participantNum: " + participantNum);
                 refresh = true;
                 retry = true;
              }
              else {
                 retry = false;
                 for (CommitSavepointResponse cresponse : result.values()) {
                   if (cresponse.getHasException()) {
                      String exceptionString = cresponse.getException();
                      if (exceptionString.contains("NonPendingTransactionException"))
                         throw new NonPendingTransactionException(exceptionString);
                      else
                         throw new RetryTransactionException(cresponse.getException());
                   }
                   else if (cresponse.getResult() == TransactionalReturn.PREPARE_REFRESH) {
                     LOG.warn("doCommitSavepointX received PREPARE_REFRESH message, will retry after refresh region");
                     refresh = true;
                     retry = true;
                     break;
                   }
                 }
              }
          }
          catch (NonPendingTransactionException npte) {
             String errMsg = new String("doCommitSavepointX NonPendingTransactionException for transaction "
                 + transactionId + " savepointId " + savepointId + " participantNum " + participantNum
                 + " Location " + location.getRegionInfo().getRegionNameAsString());
             LOG.error(errMsg, npte);
             transactionState.logExceptionDetails(location.getPeerId(), false, participantNum);
             transactionState.requestPendingCountDec(null);
             DoNotRetryIOException dnre = new DoNotRetryIOException(errMsg, npte);
             throw dnre;
          }
          catch (RetryTransactionException rte) {
             if (retryCount == RETRY_ATTEMPTS) {
                String errMsg = new String("Exceeded " + retryCount + " retry attempts in doCommitSavepointX for transaction "
                     + transactionId + " savepointId " + savepointId + " participantNum " + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                DoNotRetryIOException dnre = new DoNotRetryIOException(errMsg, rte);
                LOG.error(errMsg, dnre);
                transactionState.requestPendingCountDec(dnre);
                throw dnre;
             }
             else {
                LOG.warn("doCommitSavepointX retrying " + retryCount + " time for transaction " + transactionId
                      + " savepointId " + savepointId + " participantNum: "
                      + participantNum + " Location " + location.getRegionInfo().getRegionNameAsString());
                refresh = true;
                retry = true;
             }
          }
          if (retry){
             retrySleep = retry(retrySleep);
          }
          if (refresh) {
             // Make sure we refresh after sleeping in above retry so that regions
             // have a chance to relocate if necessary
             RegionLocator locator = null;
             Admin admin = null;
             boolean tblEnabled = false;

             if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepointX -- location being refreshed : "
                     + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                     + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for transaction " + transactionId);
                 try {
                   table.clearRegionCache();
                   admin = connection.getAdmin();
                   locator = connection.getRegionLocator(table.getName());
                   tblEnabled = admin.isTableEnabled(table.getName());
                   if (tblEnabled)
                     locator.getRegionLocation(startKey, true);
                 } catch(IOException ioe) {
                   String errMsg = new String("doCommitSavepointX -- failed to refresh location for table " + table.getName() + " within transaction " + transactionId);
                   LOG.error(errMsg, ioe);
                   transactionState.requestPendingCountDec(null);
                   throw ioe;
                 } catch(Throwable thr) {
                   String errMsg = new String("doCommitSavepointX -- failed to refresh location for table " + table.getName() +  " within transaction " + transactionId);
                   LOG.error(errMsg, thr);
                   transactionState.requestPendingCountDec(null);
                   throw new IOException(thr);
                 } finally {
                   try {
                     if (locator != null) {
                        locator.close();
                        locator = null;
                     }
                     if (admin != null) {
                        admin.close();
                        admin = null;
                     }
                   } catch(Throwable thr) {
                     retry = false;
                     String errMsg = new String("doCommitSavepointX -- failed to close. transaction id is " + transactionId);
                     LOG.error(errMsg, thr);
                   }
                 }
                 if (tblEnabled == false) {
                    LOG.error("doCommitSavepointX -- table: " + table.getName().getNameAsString() + " is disabled, unable to commitSavepoint");
                    transactionState.requestPendingCountDec(null);
                    DoNotRetryIOException dnre = new DoNotRetryIOException("doCommitSavepointX -- table: "
                               + table.getName().getNameAsString() + " is disabled, unable to abortSavepoint");
                    throw dnre;
                 }
                 if (LOG.isWarnEnabled()) LOG.warn("doCommitSavepointX -- setting retry, count: " + retryCount);
                 refresh = false;
                 isRefreshed = true;
            }
          } while (retry && retryCount++ <= RETRY_ATTEMPTS);
        }

        if( TRANSACTION_ALGORITHM == AlgorithmType.SSCC){
        do {
             retry = false;
             refresh = false;
             try {

              Batch.Call<SsccRegionService, SsccCommitSavepointResponse> callable =
                new Batch.Call<SsccRegionService, SsccCommitSavepointResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<SsccCommitSavepointResponse> rpcCallback =
                new BlockingRpcCallback<SsccCommitSavepointResponse>();

              @Override
              public SsccCommitSavepointResponse call(SsccRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.SsccRegionProtos.SsccCommitSavepointRequest.Builder builder = SsccCommitSavepointRequest.newBuilder();
                builder.setTransactionId(transactionId);
                builder.setSavepointId(savepointId);
                builder.setRegionName(ByteString.copyFromUtf8(""));

                instance.commitSavepoint(controller, builder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            Map<byte[], SsccCommitSavepointResponse> result = null;
              try {
                  if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepointX -- before coprocessorService txid: " + transactionId
                        + " savepointId " + savepointId + " table: " + table.getName().getNameAsString()
                        + " startKey " + ((startKey != null) ?
                               (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                        + " endKey " +  ((endKey != null) ?
                               (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL"));

                  result = table.coprocessorService(SsccRegionService.class, startKey, endKey, callable);
              } catch (ServiceException se) {
                  String msg = "ERROR occurred while calling doCommitSavepointX coprocessor service";
                  LOG.warn(msg + ":",  se);
                  throw new RetryTransactionException(msg, se);
              } catch (Throwable e) {
                  String msg = "ERROR occurred while calling doCommitSavepointX coprocessor service";
                  LOG.error(msg + ":",  e);
                  DoNotRetryIOException dnre = new DoNotRetryIOException(msg,e);
                  transactionState.requestPendingCountDec(dnre);
                  throw dnre;
              }

              if (result.size() != 1) {
                 LOG.error("doCommitSavepointX, received incorrect result size: " + result.size()
                     + " for transaction " + transactionId + " savepointId " + savepointId );
                 refresh = true;
                 retry = true;
              }
              else {
                 for (SsccCommitSavepointResponse cresponse : result.values()) {
                    if (cresponse.getHasException()) {
                       throw new RetryTransactionException(cresponse.getException());
                    }
                 }
              }
              retry = false;
          }
          catch (RetryTransactionException rte) {
              if (retryCount == RETRY_ATTEMPTS){
                   String errMsg = new String ("Exceeded retry attempts in doCommitSavepointX: " + retryCount + " (Not ingoring)");
                   LOG.error(errMsg);
                   RollbackUnsuccessfulException rue = new RollbackUnsuccessfulException(errMsg, rte);
                   transactionState.requestPendingCountDec(rue);
                   throw rue;
              }
              LOG.warn("doCommitSavepointX participant " + participantNum + " retrying transaction "
                      + transactionId + " savepointId " + savepointId  + " due to Exception: " + rte);
              refresh = true;
              retry = true;
          }
          if (retry){
             retrySleep = retry(retrySleep);
          }
          if (refresh) {
             // Make sure we refresh after sleeping in above retry so that regions
             // have a chance to relocate if necessary
             RegionLocator locator = null;
             Admin admin = null;
             boolean tblEnabled = false;

             if (LOG.isInfoEnabled()) LOG.info("doCommitSavepointX -- location being refreshed : "
                    + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                    + Hex.encodeHexString(location.getRegionInfo().getEndKey())
                    + " for transaction " + transactionId + " savepointId " + savepointId);

             try {
               table.clearRegionCache();
               admin = connection.getAdmin();
               locator = connection.getRegionLocator(table.getName());
               tblEnabled = admin.isTableEnabled(table.getName());
               if (tblEnabled)
                 locator.getRegionLocation(startKey, true);
             } catch(IOException ioe) {
               String errMsg = new String("doCommitSavepointX -- failed to refresh location for table " + table.getName() + " within transaction " + transactionId);
               LOG.error(errMsg, ioe);
               transactionState.requestPendingCountDec(null);
               throw ioe;
             } catch(Throwable thr) {
                String errMsg = new String("doCommitSavepointX -- failed to refresh location for table " + table.getName() +  " within transaction " + transactionId);
                LOG.error(errMsg, thr);
                transactionState.requestPendingCountDec(null);
                throw new IOException(thr);
             } finally {
               try {
                 if (locator != null) {
                   locator.close();
                   locator = null;
                 }
                 if (admin != null) {
                   admin.close();
                   admin = null;
                 }
               } catch(Throwable thr) {
                  retry = false;
                  String errMsg = new String("doCommitSavepointX -- failed to close. transaction id is " + transactionId);
                  LOG.error(errMsg, thr);
               }
             }
             if (tblEnabled == false) {
                LOG.error("doCommitSavepointX -- table: " + table.getName().getNameAsString() + " is disabled, unable to commitSavepoint");
                transactionState.requestPendingCountDec(null);
                DoNotRetryIOException dnre = new DoNotRetryIOException("doCommitSavepointX -- table: "
                        + table.getName().getNameAsString() + " is disabled, unable to commitSavepoint");
                throw dnre;
             }
             if (LOG.isWarnEnabled()) LOG.warn("doCommitSavepointX -- setting retry, count: " + retryCount);
             refresh = false;
          }
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);

      }

      // We have received our reply so decrement outstanding count
      transactionState.requestPendingCountDec(null);

      if(LOG.isTraceEnabled()) LOG.trace("doCommitSavepointX -- EXIT txID: " + transactionId + " savepointId " + savepointId );
      return 0;
    }

  } // TransactionManagerCallable

/*
  private void checkException(TransactionState ts, List<TransactionRegionLocation> locations, List<String> exceptions) throws IOException {
    if(LOG.isTraceEnabled()) LOG.trace("checkException -- ENTRY txid: " + ts.getTransactionId());
    ts.clearRetryRegions();
    Iterator<String> exceptionIt = exceptions.iterator();
    StringBuilder logException = new StringBuilder();
    for(int i=0; i < exceptions.size(); i++) {
        String exception = exceptionIt.next();
        if(exception.equals(BatchException.EXCEPTION_OK.toString())) {
            continue;
        }
        else if (exception.equals(BatchException.EXCEPTION_UNKNOWNTRX_ERR.toString()) ||
                 exception.equals(BatchException.EXCEPTION_NORETRY_ERR.toString())) {
            // No need to add to retry list, throw exception if not ignoring
            logException.append("Encountered " + exception + " on region: " +
                                 locations.get(i).getRegionInfo().getRegionNameAsString());
            throw new DoNotRetryIOException(logException.toString());
        }
        else if (exception.equals(BatchException.EXCEPTION_RETRY_ERR.toString()) ||
                 exception.equals(BatchException.EXCEPTION_REGIONNOTFOUND_ERR.toString())) {
            if(LOG.isWarnEnabled()) LOG.warn("Encountered batch error, adding region to retry list: " +
                                              locations.get(i).getRegionInfo().getRegionNameAsString());
            ts.addRegionToRetry(locations.get(i));
        }
        if(logException.length() > 0) {
            throw new RetryTransactionException(logException.toString());
        }
    }
    if(LOG.isTraceEnabled()) LOG.trace("checkException -- EXIT txid: " + ts.getTransactionId());

   }
*/
  /**
   * threadPool - pool of thread for asynchronous requests
   */
    ExecutorService threadPool;
    private Set<Integer> peers;

    /**
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    private TransactionManager(final Configuration conf, Connection connection) throws ZooKeeperConnectionException, IOException {
        this(LocalTransactionLogger.getInstance(), conf, connection);
        this.connection = connection;
        this.config = new Configuration(conf);
        String retryAttempts = getNotEmptyEnvVar("TMCLIENT_RETRY_ATTEMPTS");
        String numThreads = getNotEmptyEnvVar("TM_JAVA_THREAD_POOL_SIZE");
        String useSSCC = getNotEmptyEnvVar("TM_USE_SSCC");
        String usePIT = getNotEmptyEnvVar("TM_USE_PIT_RECOVERY");

        if( usePIT != null){
           this.recoveryToPitMode = (Integer.parseInt(usePIT.trim()) == 1) ? true : false;
        }
        if (LOG.isTraceEnabled()) LOG.trace("TM_USE_PIT_RECOVERY: " + this.recoveryToPitMode);

        String idtmTimeout = getNotEmptyEnvVar("TM_IDTM_TIMEOUT");
        if (idtmTimeout != null){
           ID_TM_SERVER_TIMEOUT = Integer.parseInt(idtmTimeout.trim());
           if (LOG.isInfoEnabled()) LOG.info("TM_IDTM_TIMEOUT: " + this.ID_TM_SERVER_TIMEOUT);
        }

        String envLogCommits = getNotEmptyEnvVar("TM_LOG_COMMITS");
        if (envLogCommits != null){
           this.logCommits = (Integer.parseInt(envLogCommits.trim()) == 1) ? true : false;
           if (LOG.isInfoEnabled()) LOG.info("TM_LOG_COMMITS: " + this.logCommits);
        }

        if (retryAttempts != null)
            RETRY_ATTEMPTS = Integer.parseInt(retryAttempts.trim());
        else
            RETRY_ATTEMPTS = TM_RETRY_ATTEMPTS;

        if (numThreads != null)
            intThreads = Integer.parseInt(numThreads.trim());
            
        if (LOG.isInfoEnabled()) LOG.info("TM_JAVA_THREAD_POOL_SIZE: " + intThreads);

        String tmRpcTimeout = getNotEmptyEnvVar("TM_RPC_TIMEOUT");
        if (tmRpcTimeout != null) {
            this.config.set("hbase.rpc.timeout", tmRpcTimeout.trim());
        } else {
            this.config.set("hbase.rpc.timeout", Integer.toString(60000));
        }

        String rpcTimeout = getNotEmptyEnvVar("HAX_ADMIN_RPC_TIMEOUT");
        if (rpcTimeout != null){
           adminConf = new Configuration(this.config);
           int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
           String value = adminConf.getTrimmed("hbase.rpc.timeout");
           adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
           String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
           LOG.info("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
           adminConnection = ConnectionFactory.createConnection(adminConf);
        }
        else {
           adminConnection = this.connection;
        }

        TRANSACTION_ALGORITHM = AlgorithmType.MVCC;
        if (useSSCC != null)
           TRANSACTION_ALGORITHM = (Integer.parseInt(useSSCC.trim()) == 1) ? AlgorithmType.SSCC :AlgorithmType.MVCC ;

        idServer = new IdTm(false);

        threadPool = Executors.newFixedThreadPool(intThreads);

	cp_tpe = Executors.newFixedThreadPool(intThreads);

	pSTRConfig = STRConfig.getInstance(this.config);
        peers = pSTRConfig.getPeerIds();
        brClient = new BackupRestoreClient(new Configuration(conf));
    }

    /**
     * @param transactionLogger
     * @param conf
     * @throws ZooKeeperConnectionException
     */

    protected TransactionManager(final TransactionLogger transactionLogger, final Configuration conf, Connection conn)
            throws ZooKeeperConnectionException, IOException {
        this.transactionLogger = transactionLogger;
        this.config = new Configuration(conf);
        //conf.setInt("hbase.client.retries.number", 3);
 
        int clientRetryInt = 3;  
        String clientRetry = getNotEmptyEnvVar("TM_STRCONFIG_CLIENT_RETRIES_NUMBER");
        if (clientRetry != null){
           clientRetryInt = Integer.parseInt(clientRetry.trim());
           if (LOG.isInfoEnabled()) LOG.info("Transaction Manager: TM_STRCONFIG_RPC_TIMEOUT: " + clientRetryInt);
        }   
        String value = conf.getTrimmed("hbase.client.retries.number");
        if (LOG.isDebugEnabled()) LOG.debug("Transaction Manager: STRConfig " + "hbase.client.retries.number " + value);
        if (  (value == null) || (Integer.parseInt(value) > clientRetryInt)  ) {
             conf.set("hbase.client.retries.number", Integer.toString(clientRetryInt));
             value = conf.getTrimmed("hbase.client.retries.number");
             if (LOG.isInfoEnabled()) LOG.info("Transaction Manager: STRConfig " + "revised hbase.client.retries.number " + value);
        } 

        String tmRpcTimeout = getNotEmptyEnvVar("TM_RPC_TIMEOUT");
        if (tmRpcTimeout != null) {
            this.config.set("hbase.rpc.timeout", tmRpcTimeout.trim());
        } else {
            this.config.set("hbase.rpc.timeout", Integer.toString(60000));
        }
        
        connection = conn;
        pSTRConfig = STRConfig.getInstance(this.config);
        peers = pSTRConfig.getPeerIds();
    }

    public boolean isSTRUp(int pv_peer_id) 
    {

        PeerInfo lv_pi = pSTRConfig.getPeerInfo(pv_peer_id);

        if (lv_pi == null) {
            if (LOG.isTraceEnabled()) LOG.trace("Peer id: " 
                                                + pv_peer_id
                                                + " does not seem to exist"
                                                );
            return false;
        }

        return lv_pi.isSTRUp();

    }

    /**
     * Called to start a transaction.
     *
     * @return new transaction state
     */
    public TransactionState beginTransaction() throws IOException {
        long transactionId = transactionLogger.createNewTransactionLog();
        if (LOG.isTraceEnabled()) LOG.trace("Beginning transaction " + transactionId);
        return new TransactionState(transactionId);
    }

    /**
     * Called to start a transaction with transactionID input
     *
     * @return new transaction state
     */
    public TransactionState beginTransaction(long transactionId) throws IOException, IdTmException {
        //long transactionId =
      if (LOG.isTraceEnabled()) LOG.trace("Enter beginTransaction, txid: " + transactionId);
      TransactionState ts = new TransactionState(transactionId);
      ts.setStartEpoch(TrxEnvironmentEdgeManager.currentTime());
      long startIdVal = -1;

      // Set the startid
      if (ts.islocalTransaction() &&
         ((TRANSACTION_ALGORITHM == AlgorithmType.SSCC) || (recoveryToPitMode))) {
         IdTmId startId;
         try {
            startId = new IdTmId();
            if (LOG.isTraceEnabled()) LOG.trace("beginTransaction (local) getting new startId");
            idServer.id(ID_TM_SERVER_TIMEOUT, startId);
            if (LOG.isTraceEnabled()) LOG.trace("beginTransaction (local) idServer.id returned: " + startId.val);
         } catch (IdTmException exc) {
            LOG.error("beginTransaction (local) : IdTm threw exception ", exc);
            throw new IdTmException("beginTransaction (local) : IdTm threw exception ", exc);
         }
         startIdVal = startId.val;
      }
      else {
         if (LOG.isTraceEnabled()) LOG.trace("beginTransaction NOT retrieving new startId");
      }
      if (LOG.isTraceEnabled()) LOG.trace("beginTransaction setting transaction [" + ts.getTransactionId() +
                      "], startEpoch: " + ts.getStartEpoch() + " and startId: " + startIdVal);
      ts.setStartId(startIdVal);
      return ts;
    }

    /**
     * Prepare to commit a transaction.
     *
     * @param transactionState
     * @return commitStatusCode (see {@link TransactionalRegionInterface})
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    public int prepareCommit(final TransactionState transactionState) throws CommitUnsuccessfulException, IOException 
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter prepareCommit, txid: " + transactionState.getTransactionId()
                          + " with " + transactionState.getParticipantCount() + " participants");

       transactionState.InitializeMessageCounts();
       if(transactionState.hasDDLTx()){
          if (LOG.isInfoEnabled()) LOG.info("prepareCommit() [" + transactionState.getTransactionId()
                                + "] has DDL");
       }

       int loopCount = 0;
       // (need one CompletionService per request for thread safety, can share pool of threads
       CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
       boolean allReadOnly = true;

/*
       if (batchRegionServer && (TRANSACTION_ALGORITHM == AlgorithmType.MVCC)) {
           if(LOG.isTraceEnabled()) LOG.trace("TransactionManager.prepareCommit global transaction " + transactionState.getTransactionId());

         for (Integer peerId : peers) {
            if (peerId >= transactionState.getParticipatingRegions().size())
               continue;
            if (transactionState.getParticipatingRegions().getList(peerId) == null)
               continue;
            ServerName servername;
            List<TransactionRegionLocation> regionList;
            Map<ServerName, List<TransactionRegionLocation>> locations = new HashMap<ServerName, List<TransactionRegionLocation>>();
            for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
              for (TransactionRegionLocation location : tableMap.values()) {
                servername = location.getServerName();
                if(!locations.containsKey(servername)) {
                    regionList = new ArrayList<TransactionRegionLocation>();
                    locations.put(servername, regionList);
                }
                else {
                    regionList = locations.get(servername);
                }
                regionList.add(location);
              }
            }
           } // for peers
            for(final Map.Entry<ServerName, List<TransactionRegionLocation>> entry : locations.entrySet()) {
                loopCount++;
                final int lv_participant = loopCount;
                int lv_peerId = entry.getValue().get(0).peerId;
                    
                compPool.submit(new TransactionManagerCallable(transactionState, 
							       entry.getValue().iterator().next(),
							       pSTRConfig.getPeerConnections().get(lv_peerId)) {

                   public Integer call() throws CommitUnsuccessfulException, IOException {
				       if (!Thread.currentThread().getName().startsWith("T")) {
					       Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					   }

                       return doPrepareX(entry.getValue(), transactionState.getTransactionId(), transactionState.getSkipConflictCheck(),
                                          lv_participant);
                   }
                });
            }

          // loop to retrieve replies
          int commitError = 0;
            for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
              boolean loopExit = false;
              Integer canCommit = null;
              do
              {
                try {
                  canCommit = compPool.take().get();
                  loopExit = true; 
                } 
                catch (InterruptedException ie) {}
                catch (ExecutionException e) {
                  if(LOG.isInfoEnabled()) LOG.info("TransactionManager.prepareCommit local transaction " + transactionState.getTransactionId()
                       + " caught exception ", e);
                  throw new CommitUnsuccessfulException(e);
                }
              } while (loopExit == false);
              switch (canCommit) {
                 case TM_COMMIT_TRUE:
                   allReadOnly = false;
                   break;
                 case TM_COMMIT_READ_ONLY:
                   break;
                 case TM_COMMIT_FALSE_CONFLICT:
                   commitError = TransactionalReturn.COMMIT_CONFLICT;
                   break;
                 case TM_COMMIT_FALSE_DOOMED:
                   commitError = TransactionalReturn.COMMIT_DOOMED;
                   break;
                 case TM_COMMIT_FALSE:
                   // Commit conflict takes precedence
                   if(commitError != TransactionalReturn.COMMIT_CONFLICT)
                      commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;
                   break;
                 default:
                   LOG.error("Unexpected value of canCommit in prepareCommit (during completion processing): " + canCommit);
                   commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;;
              }
            }
            loopCount = 0;
            if(transactionState.getRegionsRetryCount() > 0) {
                for (TransactionRegionLocation location : transactionState.getRetryRegions()) {
                    loopCount++;
                    final int lvParticipantNum = loopCount;
                    if (LOG.isTraceEnabled()) LOG.trace("prepareCommit submitting coprocessor request for location "
                          + location + " transaction " + transactionState.getTransactionId());
                    compPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
                        public Integer call() throws CommitUnsuccessfulException, IOException {
						    if (!Thread.currentThread().getName().startsWith("T")) {
							    Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
							}

                            return doPrepareX(location.getRegionInfo().getRegionName(),
                                    transactionState.getTransactionId(), transactionState.getSkipConflictCheck(),
                                    transactionState.getStartEpoch(), lvParticipantNum,
                                    location);
                        }
                    });
                }
                transactionState.clearRetryRegions();
            }
                for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
                boolean loopExit = false;
                Integer canCommit = null;
                do
                {
                   try {
                     canCommit = compPool.take().get();
                    loopExit = true; 
                   } 
                   catch (InterruptedException ie) {}
                   catch (ExecutionException e) {
                      if(LOG.isInfoEnabled()) LOG.info("TransactionManager.prepareCommit local transaction " + transactionState.getTransactionId()
                          + " caught exception ", e);
                      throw new CommitUnsuccessfulException(e);
                   }
                } while (loopExit == false);
                    switch (canCommit) {
                       case TM_COMMIT_TRUE:
                         allReadOnly = false;
                         break;
                       case TM_COMMIT_READ_ONLY:
                         break;
                       case TM_COMMIT_FALSE_CONFLICT:
                         commitError = TransactionalReturn.COMMIT_CONFLICT;
                         break;
                       case TM_COMMIT_FALSE_DOOMED:
                         commitError = TransactionalReturn.COMMIT_DOOMED;
                         break;
                       case TM_COMMIT_FALSE_SHIELDED:
                         commitError = TransactionalReturn.COMMIT_SHIELDED;
                         break;
                       case TM_COMMIT_FALSE:
                         // Commit conflict takes precedence
                         if(commitError != TransactionalReturn.COMMIT_CONFLICT)
                            commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;
                         break;
                       default:
                         commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;;
                    }
                }
          if(commitError != 0)
             return commitError;

          return allReadOnly ? TransactionalReturn.COMMIT_OK_READ_ONLY:
                               TransactionalReturn.COMMIT_OK;
       }
*/
//     else {
//       ServerName servername;
//       List<TransactionRegionLocation> regionList;
//       Map<ServerName, List<TransactionRegionLocation>> locations = null;

       if (transactionState.islocalTransaction()){
         //System.out.println("prepare islocal");
         if(LOG.isTraceEnabled()) LOG.trace("TransactionManager.prepareCommit local transaction " + transactionState.getTransactionId());
       }
       else
         if(LOG.isTraceEnabled()) LOG.trace("TransactionManager.prepareCommit global transaction " + transactionState.getTransactionId());

       for (Integer peerId : peers) {
          if (peerId >= transactionState.getParticipatingRegions().size())
             continue;
          if (transactionState.getParticipatingRegions().getList(peerId) == null)
             continue;
          for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
           for (TransactionRegionLocation location : tableMap.values()) {
             loopCount++;
             final TransactionRegionLocation myLocation = location;
             final int lvParticipantNum = loopCount;
             boolean ignoreUnknownTransactionTimeline = false;   
             String regionTableName = location.getRegionInfo().getTable().getNameAsString();  
             if ( (transactionState.getTmTableAttr(regionTableName) & TIMELINE) == TIMELINE ) {
                ignoreUnknownTransactionTimeline = true;
                if (LOG.isDebugEnabled()) LOG.debug("prepareCommit TIMELINE set to ignore UTE, table " +
                                regionTableName + " transid " + transactionState.getTransactionId());
             }
             final boolean ignoreTimeline = ignoreUnknownTransactionTimeline;

             if (LOG.isTraceEnabled()) LOG.trace("prepareCommit submitting coprocessor request for location "
                     + location + " transaction " + transactionState.getTransactionId());
             final int tmTableCDCAttr = transactionState.getTmTableAttr(location.getRegionInfo().getTable().getNameAsString());
             if((tmTableCDCAttr & INCREMENTALBR) != INCREMENTALBR)
             {
               compPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
                 public Integer call() throws IOException, CommitUnsuccessfulException {
				   if (!Thread.currentThread().getName().startsWith("T")) {
					   Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
				   }

                   return doPrepareX(transactionState.getTransactionId(), transactionState.getSkipConflictCheck(), transactionState.getStartEpoch(),
                              lvParticipantNum, myLocation, ignoreTimeline, transactionState.getQueryContext(),  (short)-3);
                 }
               });
             }
             else
             compPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
               public Integer call() throws IOException, CommitUnsuccessfulException {
				   if (!Thread.currentThread().getName().startsWith("T")) {
					   Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
				   }

                 return doPrepareX(transactionState.getTransactionId(), transactionState.getSkipConflictCheck(), transactionState.getStartEpoch(),
                              lvParticipantNum, myLocation, ignoreTimeline, transactionState.getQueryContext(),  (short)transactionState.getTotalNum());
               }
             });
            }
          }
        }
        // loop to retrieve replies
        int commitError = 0;
          for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
             boolean loopExit = false;
             Integer canCommit = null;
             do
             {
               try {
                  canCommit = compPool.take().get();
                  loopExit = true; 
               } 
               catch (InterruptedException ie) {}
               catch (ExecutionException e) {
                  if(LOG.isInfoEnabled()) LOG.info("TransactionManager.prepareCommit local transaction " + transactionState.getTransactionId()
                        + " caught exception ", e);
                  throw new CommitUnsuccessfulException(e);
               }
            } while (loopExit == false);
            switch (canCommit) {
               case TM_COMMIT_TRUE:
                 allReadOnly = false;
                 break;
               case TM_COMMIT_READ_ONLY:
                 break;
               case TM_COMMIT_FALSE_CONFLICT:
                 commitError = TransactionalReturn.COMMIT_CONFLICT;
                 break;
               case TM_COMMIT_FALSE_DOOMED:
                 commitError = TransactionalReturn.COMMIT_DOOMED;
                 break;
               case TM_COMMIT_FALSE_SHIELDED:
                 commitError = TransactionalReturn.COMMIT_SHIELDED;
                 break;
               case TM_COMMIT_FALSE_EPOCH_VIOLATION:
                 commitError = TransactionalReturn.EPOCH_VIOLATION;
                 break;
               case TM_COMMIT_FALSE:
                 // Commit conflict takes precedence
                 if(commitError != TransactionalReturn.COMMIT_CONFLICT)
                    commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;
                 break;
               default:
                 LOG.error("Unexpected value of canCommit in prepareCommit (during completion processing): " + canCommit);
                 commitError = TransactionalReturn.COMMIT_UNSUCCESSFUL;;
            }
          }
        if(commitError != 0)
           return commitError;

        //in mode 3, we need to loop to check the wid to make sure binlog is flushed
        //if fail, abort transaction
        if(ATRConfig.instance().isMaxProtectionSyncMode() == true) {
          int checkret = doBinlogCheck(transactionState);
          if(checkret == 1)
            return TransactionalReturn.COMMIT_UNSUCCESSFUL; 
        }

        //Before replying prepare success, check for DDL transaction.
        //If prepare already has errors (commitError != 0), an abort is automatically
        //triggered by TM which would take care of ddl abort.
        //if prepare is success upto this point, DDL operation needs to check if any
        //drop table requests were recorded as part of phase 0. If any drop table
        //requests is recorded, then those tables need to disabled as part of prepare.
        if(transactionState.hasDDLTx())
        {
            if (LOG.isTraceEnabled()) LOG.trace("prepareCommit process DDL operations, txid: " + transactionState.getTransactionId());
            //since DDL is involved, mark this prepare allReadOnly as false.
            //There are cases such as initialize drop, that only has DDL operations. 
             allReadOnly = false;

            //if tables were created, then nothing else needs to be done.
            //if tables were recorded dropped, then they need to be disabled.
            //Disabled tables will ultimately be deleted in commit phase.
            ArrayList<String> createList = new ArrayList<String>(); //This list is ignored.
            ArrayList<String> createIncrList = new ArrayList<String>(); //This list is ignored.
            ArrayList<String> dropList = new ArrayList<String>();
            ArrayList<String> truncateList = new ArrayList<String>();
            StringBuilder state = new StringBuilder ();
            tmDDL.getRow(transactionState.getTransactionId(), state, createList, createIncrList, dropList, truncateList);
            if(state.toString().equals("VALID") && dropList.size() > 0)
            {
                Iterator<String> di = dropList.iterator();
                while (di.hasNext())
                {
                    //physical drop of table from hbase.
                    disableTable(transactionState, di.next());
                }
            }
        }

        if(commitError != 0)
           return commitError;

        return allReadOnly ? TransactionalReturn.COMMIT_OK_READ_ONLY:
                             TransactionalReturn.COMMIT_OK;
//      }
    }

    /**
     * Try and commit a transaction. This does both phases of the 2-phase protocol: prepare and commit.
     *
     * @param transactionState
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    public void tryCommit(final TransactionState transactionState)
        throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
        long startTime = TrxEnvironmentEdgeManager.currentTime();
        if (LOG.isTraceEnabled()) LOG.trace("Attempting to commit transaction " + transactionState.toString());
        int status = prepareCommit(transactionState);

        if (status == TransactionalReturn.COMMIT_OK) {
          if (LOG.isTraceEnabled()) LOG.trace("doCommit txid:" + transactionState.getTransactionId());

          doCommit(transactionState);
        } else if (status == TransactionalReturn.COMMIT_OK_READ_ONLY) {
            // no requests sent for fully read only transaction
          transactionState.completeSendInvoke(0);
        } else if (status == TransactionalReturn.COMMIT_UNSUCCESSFUL) {
          // We have already aborted at this point
          throw new CommitUnsuccessfulException();
        }
        if (LOG.isTraceEnabled()) LOG.trace("Committed transaction [" + transactionState.getTransactionId() + "] in ["
                + ((TrxEnvironmentEdgeManager.currentTime() - startTime)) + "]ms");
    }

    public void retryCommit(final TransactionState transactionState, final boolean ignoreUnknownTransaction) throws IOException {
      if(LOG.isTraceEnabled()) LOG.trace("retryCommit -- ENTRY -- txid: " + transactionState.getTransactionId());
      synchronized(transactionState.getRetryRegions()) {
          List<TransactionRegionLocation> completedList = new ArrayList<TransactionRegionLocation>();
          int loopCount = 0;
          final short totalNum = (short) transactionState.getRetryRegions().size();
          final int ddlNum =  transactionState.getDDLNum();
          for (TransactionRegionLocation location : transactionState.getRetryRegions()) {
            loopCount++;
            final int participantNum = loopCount;
            if(LOG.isTraceEnabled()) LOG.trace("retryCommit retrying commit for transaction "
                    + transactionState.getTransactionId() + ", participant: " + participantNum
                    + ", peer: " + location.peerId);
            final int tmTableCDCAttr = transactionState.getTmTableAttr(location.getRegionInfo().getTable().getNameAsString());
            threadPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
                public Integer call() throws CommitUnsuccessfulException, IOException {
					if (!Thread.currentThread().getName().startsWith("T")) {
						Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					}

                    return doCommitX(transactionState.getTransactionId(),
                            transactionState.getCommitId(),
                            participantNum,
                            ignoreUnknownTransaction,
                            totalNum,
                            tmTableCDCAttr,
                            ddlNum);
                }
              });
              completedList.add(location);
            }
            transactionState.getRetryRegions().removeAll(completedList);
        }
      if(LOG.isTraceEnabled()) LOG.trace("retryCommit -- EXIT -- txid: " + transactionState.getTransactionId());
    }

    public Integer pushRegionEpoch(String tableName, final long transID, final long epochTs  ) throws IOException {
       short retry = 0;
       boolean loopBack = false;
       boolean refresh = false;
       boolean succeed = false;
       boolean retryConnect = true;
       int retryConnCounter = 0;
       TransactionalTable ttable = null;
       Map<byte[], PushEpochResponse> result = null;
       if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch -- Entry transId: " + transID +
                                             " Table: " + tableName); 
       do {
             while(retryConnect == true) {
               try {
                 ttable = new TransactionalTable(Bytes.toBytes(tableName), connection);
                 retryConnect = false;
               }
               catch(Exception tte) {
                 retryConnCounter++;
                 connection.close();
                 connection = ConnectionFactory.createConnection(this.config);
                 if(retryConnCounter > 3) {
                   retryConnect = false;
                   LOG.error("pushRegionEpoch new TransactionalTable error due to : " + tte );
                   throw new IOException("pushRegionEpoch new TransactionalTable error " ,tte);
                 }
               }
             }
 
             Batch.Call<TrxRegionService, PushEpochResponse> callable =
                   new Batch.Call<TrxRegionService, PushEpochResponse>() {
                             ServerRpcController controller = new ServerRpcController();
                             BlockingRpcCallback<PushEpochResponse> rpcCallback =
                             new BlockingRpcCallback<PushEpochResponse>();

                  public PushEpochResponse call(TrxRegionService instance) throws IOException {
                     org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochRequest.Builder
                     builder = PushEpochRequest.newBuilder();
                     builder.setTransactionId(transID);
                     builder.setEpoch(epochTs);
                     builder.setRegionName(ByteString.copyFromUtf8(""));   //not required.
                     instance.pushOnlineEpoch(controller, builder.build(), rpcCallback);
                     return rpcCallback.get();
                  }
             };

             try {
                   long timeCost = System.currentTimeMillis();
                   result = ttable.coprocessorService(TrxRegionService.class,
                              HConstants.EMPTY_START_ROW,
                              HConstants.EMPTY_END_ROW,
                              callable);
                   if (costTh >= 0) {
                     timeCost = System.currentTimeMillis() - timeCost;
                     if (timeCost >= costTh)
                       LOG.warn("pushRegionEpoch copro PID " + PID + " txID " + transID + " TC " + timeCost + " " + ttable.getName());
                   }
                   succeed = true;
             } catch (ServiceException se) {
                  if (LOG.isErrorEnabled()) LOG.trace("ERROR pushRegionEpoch service exception ", se);
                  throw new IOException("ERROR pushRegionEpoch service exception ", se);
             } catch (IOException nsfre) {
                  //change NoServerForRegionException to IOException for
                  //IOException with 'hconnection-0x**** closed' which is need to reconnection.
                  //NoServerForRegionException is subClass of IOException
                  retry++;
                  if (LOG.isErrorEnabled()) LOG.error("ERROR pushRegionEpoch NoServerForRegion exception, retry it " +
                                         retry + " refreshed " + refresh, nsfre);
                  if (retry >= 3) {
                      if (refresh) {
                         if (LOG.isErrorEnabled()) LOG.error("ERROR NoServerForRegion exception ");
                         throw new IOException("ERROR pushRegionEpoch NoServerForRegion exception after retry and refresh ", nsfre);
                      }
                      else { // fresh the connection once, and try one more time
                         if (connection != null) {
                            try {
                                connection.close();
                            } catch (Exception e) {/*don't care*/}
                         }
                         connection = ConnectionFactory.createConnection(this.config);
                         refresh = true;
                         retry--; // only retry once
                         if (LOG.isInfoEnabled()) LOG.info("ERROR pushRegionEpoch NoServerForRegion exception, retry " +
                                         retry + " refresh connection " + refresh, nsfre);
                      }
                  }
                   do {
                         try {
                               loopBack = false;
                               Thread.sleep(2000); // 2 seconds delay
                          } catch (InterruptedException ie) {
                                loopBack = true;
                          }
                  } while (loopBack);
             } catch (Throwable e) {
                  if (LOG.isErrorEnabled()) LOG.error("ERROR pushRegionEpoch throwable exception ", e);
                  throw new IOException("ERROR pushRegionEpoch throwable exception ", e);
             }
             finally{
                  ttable.close();
             }
       } while ((!succeed) && (retry < 3));


       if(result == null) {
           throw new IOException("ERROR pushRegionEpoch result is null :" );
       }
        for (PushEpochResponse eresponse : result.values()){
          if(eresponse.getHasException()) {
             String exceptionString = new String (eresponse.getException().toString());
             LOG.error("ERROR pushRegionEpoch - coprocessor exceptionString: " + exceptionString);
             throw new IOException("ERROR pushRegionEpoch - coprocessor exceptionString: " + exceptionString);
          }
       }
       
       if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch -- Exit  txid: "
                 + transID + " Table: " + tableName);
       
       return 0;
    }

    public Integer pushRegionEpoch(HTableDescriptor desc, final TransactionState ts) throws IOException {

       short retry = 0;
       boolean loopBack = false;
       boolean refresh = false;
       boolean succeed = false;
       boolean retryConnect = true;
       int retryConnCounter = 0;
       TransactionalTable ttable = null;
       Map<byte[], PushEpochResponse> result = null;
       if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch -- Entry transId: " + ts.getTransactionId() +
                                             " Table: " + desc.getNameAsString()); 

       do {
             while(retryConnect == true) {
               try {
                 ttable = new TransactionalTable(Bytes.toBytes(desc.getNameAsString()), connection);
                 retryConnect = false;
               }
               catch(Exception tte) {
                 retryConnCounter++;
                 connection.close();
                 connection = ConnectionFactory.createConnection(this.config);
                 if(retryConnCounter > 3) {
                   retryConnect = false;
                   LOG.error("pushRegionEpoch new TransactionalTable error due to : " + tte );
                   throw new IOException("pushRegionEpoch new TransactionalTable error " ,tte);
                 }
               }
             }
       
             Batch.Call<TrxRegionService, PushEpochResponse> callable =
                   new Batch.Call<TrxRegionService, PushEpochResponse>() {
                             ServerRpcController controller = new ServerRpcController();
                             BlockingRpcCallback<PushEpochResponse> rpcCallback =
                             new BlockingRpcCallback<PushEpochResponse>();

                  public PushEpochResponse call(TrxRegionService instance) throws IOException {
                     org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochRequest.Builder
                     builder = PushEpochRequest.newBuilder();
                     builder.setTransactionId(ts.getTransactionId());
                     builder.setEpoch(ts.getStartEpoch());
                     builder.setRegionName(ByteString.copyFromUtf8(""));   //not required.
                     instance.pushOnlineEpoch(controller, builder.build(), rpcCallback);
                     return rpcCallback.get();
                  }
             };

             if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch -- coprocessor call, tid: " + ts.getTransactionId() +
                                             " Table: " + desc.getNameAsString()); 

             try {
                   long timeCost = System.currentTimeMillis();
                   result = ttable.coprocessorService(TrxRegionService.class,
                              HConstants.EMPTY_START_ROW,
                              HConstants.EMPTY_END_ROW,
                              callable);
                   if (costTh >= 0) {
                     timeCost = System.currentTimeMillis() - timeCost;
                     if (timeCost >= costTh)
                       LOG.warn("pushRegionEpoch copro PID " + PID + " txID " + ts.getTransactionId() + " TC " + timeCost + " " + ttable.getName());
                   }
                   succeed = true;
             } catch (ServiceException se) {
                  if (LOG.isErrorEnabled()) LOG.trace("ERROR pushRegionEpoch service exception ", se);
                  throw new IOException("ERROR pushRegionEpoch service exception ", se);
             } catch (NoServerForRegionException nsfre) {
                  retry++;
                  if (LOG.isErrorEnabled()) LOG.error("ERROR pushRegionEpoch NoServerForRegion exception, retry it " +
                                         retry + " refreshed " + refresh, nsfre);
                  if (retry >= 3) {
                      if (refresh) {
                         if (LOG.isErrorEnabled()) LOG.error("ERROR NoServerForRegion exception ");
                         throw new IOException("ERROR pushRegionEpoch NoServerForRegion exception after retry and refresh ", nsfre);
                      }
                      else { // fresh the connection once, and try one more time
                         connection.close();
                         connection = ConnectionFactory.createConnection(this.config);
                         refresh = true;
                         retry--; // only retry once
                         if (LOG.isInfoEnabled()) LOG.info("ERROR pushRegionEpoch NoServerForRegion exception, retry " +
                                         retry + " refresh connection " + refresh, nsfre);
                      }
                  }
                  // sleep for a while
                  do {
                         try {
                               loopBack = false;
                               Thread.sleep(2000); // 2 seconds delay
                          } catch (InterruptedException ie) {
                                loopBack = true;
                          }
                  } while (loopBack);
             } catch (Throwable e) {
                  if (LOG.isErrorEnabled()) LOG.error("ERROR pushRegionEpoch throwable exception ", e);
                  throw new IOException("ERROR pushRegionEpoch throwable exception ", e);
             }
             finally{
                  ttable.close();
             }
       } while ((!succeed) && (retry < 3));


       if(result == null) {
           throw new IOException("ERROR pushRegionEpoch result is null :" +
                      "transId: " + ts.getTransactionId() + " Table: " + desc.getNameAsString());
       }
       // size is 1
       for (PushEpochResponse eresponse : result.values()){
          if(eresponse.getHasException()) {
             String exceptionString = new String (eresponse.getException().toString());
             LOG.error("ERROR pushRegionEpoch - coprocessor exceptionString: " + exceptionString);
             throw new IOException("ERROR pushRegionEpoch - coprocessor exceptionString: " + exceptionString);
          }
       }
       
       if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch -- Exit  txid: "
                 + ts.getTransactionId() + " Table: " + desc.getNameAsString());
       
       return 0;
    }


    public void retryAbort(final TransactionState transactionState) throws IOException {
      if(LOG.isTraceEnabled()) LOG.trace("retryAbort -- ENTRY -- txid: " + transactionState.getTransactionId());
      synchronized(transactionState.getRetryRegions()) {
          List<TransactionRegionLocation> completedList = new ArrayList<TransactionRegionLocation>();
          int loopCount = 0;
          for (TransactionRegionLocation location : transactionState.getRetryRegions()) {
            loopCount++;
            final int participantNum = loopCount;
            if(LOG.isTraceEnabled()) LOG.trace("retryAbort retrying abort for transaction "
                    + transactionState.getTransactionId() + ", participant: " + participantNum
                    + ", peer: " + location.peerId);
	     //TBD : The parameter '0' to getPeerConnections().get() may need to be something else
             threadPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
                  public Integer call() throws CommitUnsuccessfulException, IOException {
					  if (!Thread.currentThread().getName().startsWith("T")) {
						  Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					  }

                      return doAbortX(transactionState.getTransactionId(), participantNum, location.isTableRecodedDropped(), false);
                  }
              });
              completedList.add(location);
          }
          transactionState.getRetryRegions().removeAll(completedList);
      }
      if(LOG.isTraceEnabled()) LOG.trace("retryAbort -- EXIT -- txid: " + transactionState.getTransactionId());
    }
    public short computeTotalNum(TransactionState transactionState) {
      short retVal = -1;
      int algo =2;
      try {
        algo =  ATRConfig.instance().getTotalNumAlgo() ;
      } catch (Exception e) {
        LOG.error("HBaseBinlog: get totalNum algo failure ", e);
        algo = 2; //use default new way
      }
      if( algo == 1) {
        List<TransactionRegionLocation> uniqueLocations = new ArrayList<TransactionRegionLocation>();
        for (Integer peerId : peers) {
          if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
          if (transactionState.getParticipatingRegions().getList(peerId) == null)
             continue;
          for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
            for (TransactionRegionLocation location : tableMap.values()) {
                if (location.getReadOnly()) {
                    location.setReadOnly(false); //restore it
	             continue;
	        }
                //check if it is MD table and if it has IB attr
                int tmTableCDCAttr = transactionState.getTmTableAttr(location.getRegionInfo().getTable().getNameAsString());
                if(  (tmTableCDCAttr & INCREMENTALBR ) == INCREMENTALBR &&
                     !location.getRegionInfo().getTable().getNameAsString().contains("TRAFODION._XDC_MD_.XDC_DDL"))
                  uniqueLocations.add(location);
             }
           }
        }

        Map<String, Integer> noDupLocations =  new HashMap<String, Integer>();
        for(TransactionRegionLocation loc : uniqueLocations ) {
          
          if( noDupLocations.containsKey(loc.getRegionInfo().getEncodedName()) == true) //dup
           continue;
          else
              noDupLocations.put(loc.getRegionInfo().getEncodedName(), 1);
        }
        transactionState.setTotalNum((short)noDupLocations.size());
        retVal = (short)noDupLocations.size();
     }
     else  {
       //need to restore readonly flag
        for (Integer peerId : peers) {
          if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
          if (transactionState.getParticipatingRegions().getList(peerId) == null)
             continue;
          for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap :
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
            for (TransactionRegionLocation location : tableMap.values()) {
                if (location.getReadOnly()) {
                    location.setReadOnly(false); //restore it
                     continue;
                }
             }
           }
        }
       //go through totalNumRespMap
       Iterator<Map.Entry<String, Integer>> pIter = transactionState.getTotalNumRespMap().entrySet().iterator();
       short currTotalNum = 0;
       int currDdlNum = 0;
       while (pIter.hasNext()){
         Map.Entry<String, Integer> entry =
                (Map.Entry<String, Integer>)pIter.next();
         int commitStatus = entry.getValue();
         String who = entry.getKey();
         int tmTableCDCAttr = transactionState.getTmTableAttr(who.substring(0,who.indexOf(",")));
         // participates who have no IB attr should not count in totalNum
         // XDC_DDL is used for DDL operation, so bypass it, this is for mixed DDL and DML scenario
         // then the last condition is the participate has binlog to generate so it returns COMMIT_OK
         if(  (tmTableCDCAttr & INCREMENTALBR ) == INCREMENTALBR &&
               !who.contains("TRAFODION._XDC_MD_.XDC_DDL") &&
               (commitStatus == TransactionalReturn.COMMIT_OK) ) 
            currTotalNum++;
         if(who.contains("TRAFODION._XDC_MD_.XDC_DDL") )
            currDdlNum++;
       }
       retVal = currTotalNum;
       transactionState.setTotalNum(retVal);
       transactionState.setDDLNum(currDdlNum);
     }
     return retVal;
   }


    /**
     * Do the commit. This is the 2nd phase of the 2-phase protocol.
     *
     * @param transactionState
     * @throws CommitUnsuccessfulException
     */
    public void doCommit(final TransactionState transactionState)
        throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
       if (LOG.isTraceEnabled()) LOG.trace("doCommit [" + transactionState.getTransactionId() +
                      "] ignoreUnknownTransaction not supplied;  stallFlags not supplied");

       doCommit(transactionState, false /* IgnoreUnknownTransaction */, 0 /* stallFlags */);
//       doCommit(transactionState, true /* IgnoreUnknownTransaction */, 0 /* stallFlags */);
    }

    /**
     * Do the commit. This is the 2nd phase of the 2-phase protocol.
     *
     * @param transactionState
     * @param ignoreUnknownTransaction
     * @throws CommitUnsuccessfulException
     */
    public void doCommit(final TransactionState transactionState, final boolean ignoreUnknownTransaction, final int stallFlags)
                    throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
        int loopCount = 0;
        final boolean lv_ignoreUnknownTransaction =
            (transactionState.getMustBroadcast() ? true : (transactionState.getPrepareRefresh() ? true : ignoreUnknownTransaction));
        transactionState.InitializeMessageCounts();

        if(transactionState.hasDDLTx()) {
           if (LOG.isInfoEnabled()) LOG.info("doCommit() [" + transactionState.getTransactionId()
                                + "] has DDL");
        }
        if(this.logCommits){
           LOG.info("Committing [" + transactionState.getTransactionId() +
                "] , commitId : " + transactionState.getCommitId() +
                " ignoreUnknownTransaction: " + lv_ignoreUnknownTransaction + " stallFlags " + stallFlags );
        }
        else{
           if (LOG.isDebugEnabled()) LOG.debug("Committing [" + transactionState.getTransactionId() +
                "] , commitId : " + transactionState.getCommitId() +
                " ignoreUnknownTransaction: " + lv_ignoreUnknownTransaction + " stallFlags " + stallFlags );
        }

        if (LOG.isDebugEnabled()) {
           for (Integer peerId : peers) {
              if (peerId >= transactionState.getParticipatingRegions().size())
                 continue;
              if (transactionState.getParticipatingRegions().getList(peerId) == null)
                 continue;
              LOG.debug("sending commits for ts: " + transactionState + ", with commitId: "
                    + transactionState.getCommitId() + " and " + transactionState.getParticipantCount() + " participants" );
              for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                    transactionState.getParticipatingRegions().getList(peerId).values()) {
                 for (TransactionRegionLocation location : tableMap.values()) {
                     LOG.debug("TransactionRegionLocation Name: "
                    + location.getRegionInfo().getRegionNameAsString()
                    + "\n Start key    : " + Hex.encodeHexString(location.getRegionInfo().getStartKey())
                    + "\n End key    : " + Hex.encodeHexString(location.getRegionInfo().getEndKey()));
                 }
              }
           }
        }

        boolean timeToStall = false;
        boolean localSent = false;
        boolean remoteSent = false;


        //in normal commit path, totalNum is computed after phase I and save into transactionState
        //for recovery commit path, totalNum is updated from Tlog and save into transactionState
        final short totalNum = transactionState.getTotalNum();
        final int ddlNum = transactionState.getDDLNum();

        // (Asynchronously send commit
        for (Integer peerId : peers) {
           if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
           if (transactionState.getParticipatingRegions().getList(peerId) == null)
              continue;
           for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
              for (TransactionRegionLocation location : tableMap.values()) {
                 if (LOG.isTraceEnabled()) LOG.trace("sending commits ... [" + transactionState.getTransactionId() + "]");

                 // If prepareRefresh is true, we will send commit to all regions
                 // with the instruction to ignore unknownTransactionExceptions.
                 // This is in case there are two tables, one read only and the other sends
                 // prepare refresh response.
                 if (! transactionState.getPrepareRefresh()){
                    if (location.getReadOnly()) {
                       if (LOG.isTraceEnabled()) LOG.trace("found participant " + (loopCount + 1)
                             + " in regionsToIgnore for txid "+ transactionState.getTransactionId() + "; ignoring");
                       continue;
                    }
                 }
                 loopCount++;
                 final int participantNum = loopCount;

                 if (stallFlags >= STALL_LOCAL_COMMIT) {
                 switch (stallFlags) {
                    case STALL_LOCAL_COMMIT:
                       if (location.peerId == pSTRConfig.getTrafClusterIdInt()){
                          // This is the first local participant.  Send the request and stall
                          timeToStall = true;
                          localSent = true;
                          LOG.info("First local commit being sent with stallFlags set to " + stallFlags + " exiting loop in order to stall, location " + location);
                       }
                       break;

                    case STALL_REMOTE_COMMIT:
                        if ((location.peerId != 0) && (location.peerId != pSTRConfig.getTrafClusterIdInt())){
                           // This is the first remote participant.  Send the request and stall
                           timeToStall = true;
                           remoteSent = true;
                           LOG.info("First remote commit being sent with stallFlags set to " + stallFlags + " exiting loop in order to stall, location " + location);
                        }
                        break;

                    case STALL_BOTH_COMMIT:
                        if ((location.peerId != 0) && (location.peerId != pSTRConfig.getTrafClusterIdInt())){
                           // This is the first remote participant.  Send the request and stall
                           remoteSent = true;
                           LOG.info("First remote commit being sent with stallFlags set to " + stallFlags + ", location " + location);
                        }
                        else if (location.peerId == pSTRConfig.getTrafClusterIdInt()){
                           localSent = true;
                           LOG.info("First local commit being sent with stallFlags set to " + stallFlags + ", location " + location);
                        }
                        if (localSent && remoteSent){
                           timeToStall = true;
                           LOG.info("First remote AND first local commit being sent with stallFlags set to " + stallFlags + " exiting loop in order to stall");
                        }
                        break;

                    default:
                       LOG.error("Unknown stallFlags in doCommit " + stallFlags);
                 }
              }
              //TransactionalRegionInterface transactionalRegionServer = (TransactionalRegionInterface) connection
              //      .getHRegionConnection(location.getServerName());
              if (LOG.isDebugEnabled()) LOG.debug("doCommit() submitting [" + transactionState.getTransactionId()
                     + "], commitId : " + transactionState.getCommitId() + " participantNum " + participantNum
                     + ", peer: " + location.peerId + " ignoreUnknownTransaction: " + lv_ignoreUnknownTransaction);
              final int tmTableCDCAttr = transactionState.getTmTableAttr(location.getRegionInfo().getTable().getNameAsString());
              threadPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
                 public Integer call() throws CommitUnsuccessfulException, IOException {
                    if (LOG.isDebugEnabled()) LOG.debug("before doCommitX() [" + transactionState.getTransactionId()
                              + "], commitId : " + transactionState.getCommitId() + " participantNum " + participantNum
                              + ", peer: " + location.peerId + " ignoreUnknownTransaction: " + lv_ignoreUnknownTransaction);
					if (!Thread.currentThread().getName().startsWith("T")) {
						Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					}

                    return doCommitX(transactionState.getTransactionId(),
                            transactionState.getCommitId(),
                            participantNum,
                            lv_ignoreUnknownTransaction,
                            totalNum,
                            tmTableCDCAttr,
                            ddlNum);
                 }
              });
              if (timeToStall) {
                 if (LOG.isInfoEnabled())LOG.info("Stalling in doCommit for transaction: " + transactionState + " with stallFlags " + stallFlags);
                 transactionState.completeSendInvoke(loopCount);
                 boolean loopBack = false;
                 do
                 {
                    try {
                       loopBack = false;
                       Thread.sleep(600000); // Initially set to run every 5 min
                    } catch (InterruptedException ie) {
                       loopBack = true;
                    }
                 } while (loopBack);

              }
            }
          }
        }
        
        if (LOG.isDebugEnabled()) LOG.debug("Transaction Manager doCommit [" + transactionState.getTransactionId()
               + "], commitId : " + transactionState.getCommitId() + ", # of participant " + loopCount
               + ", ignoreUnknownTransaction: " + lv_ignoreUnknownTransaction);        

        // all requests sent at this point, can record the count
        transactionState.completeSendInvoke(loopCount);

       //First wait for commit requests sent to all regions is received back.
       //This TM thread gets SUSPENDED until all commit threads complete!!!
       boolean loopExit = false;
       do
       {
         try {
           transactionState.completeRequest();
           loopExit = true;
         }
         catch (InterruptedException ie) {}
         catch (IOException e) {
            loopExit = true;
            LOG.error("Exception at the time of committing DML before processing DDL ", e);
            throw e;
         }
       } while (loopExit == false);

       //if DDL is involved with this transaction, need to complete it.
       if(transactionState.hasDDLTx()){
          if (LOG.isDebugEnabled()) LOG.debug("doCommit() [" + transactionState.getTransactionId()
                              + "] performing commit DDL");

          doCommitDDL(transactionState);
       }
    }

    /*
     * return 0 ok
     * retrun 1 check fail
     */
    public int doBinlogCheck(final TransactionState transactionState) 
    throws CommitUnsuccessfulException, UnsuccessfulDDLException, IOException {
           transactionState.InitializeMessageCounts();
           int loopCount = 0;
           int sendCount = 0;
           int retcode = 0;
           CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
           for (TransactionRegionLocation location : transactionState.getFlushCheckRespMap().keySet()) {
             loopCount++;
             final TransactionRegionLocation myLocation = location;
             final int lvParticipantNum = loopCount;
             boolean ignoreUnknownTransactionTimeline = false;
             final int retryIntv = ATRConfig.instance().getWriteIDFlushCheckInterval();
             final int tmTableCDCAttr = transactionState.getTmTableAttr(location.getRegionInfo().getTable().getNameAsString());
             if((tmTableCDCAttr & INCREMENTALBR) == INCREMENTALBR) {
               compPool.submit(new TransactionManagerCallable(transactionState, location, pSTRConfig.getPeerConnections().get(location.peerId)) {
               public Integer call() throws IOException, CommitUnsuccessfulException {
                  if (!Thread.currentThread().getName().startsWith("T")) {
                      Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
                   }
                  return doBinlogCheckX(transactionState.getTransactionId() , location.getBinlogWid(), retryIntv);
                  }
                });
                sendCount++;
              }
          }
          transactionState.completeSendInvoke(sendCount);
          boolean loopExit = false;
          do
          {
            try {
              transactionState.completeRequest();
              loopExit = true;
            }
            catch (InterruptedException ie) {}
            catch (IOException e) {
              loopExit = true;
              LOG.error("Exception at the time of committing DML before processing DDL ", e);
              throw e;
            }
           } while (loopExit == false);
   
/*For unknown reason this will hung the commit, no time to investigate why for now

        // loop to retrieve replies
        int commitError = 0;
          for (int loopIndex = 0; loopIndex < sendCount; loopIndex ++) {
             boolean loopExit = false;
             Integer canCommit = 0;
             do
             {
               try {
                  canCommit = compPool.take().get();
                  loopExit = true; 
               } 
               catch (InterruptedException ie) {}
               catch (ExecutionException e) {
                  if(LOG.isInfoEnabled()) LOG.info("TransactionManager.doBinlogCheck local transaction " + transactionState.getTransactionId()
                        + " caught exception ", e);
                  //if run in max protection mode, abort transaction
                  if(ATRConfig.instance().isMaxProtectionSyncMode() == true) 
                    throw new CommitUnsuccessfulException(e);
                  else
                    commitError = 1;
               }
            } while (loopExit == false);
            switch (canCommit) {
               case 1:
                 commitError = 1;
                 break;
               case 0:
                 break;
               default:
                 LOG.error("Unexpected value of canCommit in doBinlogCheck (during completion processing): " + canCommit);
                 commitError = 1;
            }
          }

      return commitError; 
*/
      return 0;
    }


    public void doCommitDDL(final TransactionState transactionState) throws UnsuccessfulDDLException
    {
      
        if (LOG.isTraceEnabled()) LOG.trace("doCommitDDL  ENTRY [" + transactionState.getTransactionId() + "]"); 

        //if tables were created, then nothing else needs to be done.
        //if tables were recorded dropped, then they need to be physically dropped.
        //Tables recorded dropped would already be disabled as part of prepare commit.
        ArrayList<String> createList = new ArrayList<String>(); //This list is ignored.
        ArrayList<String> createIncrList = new ArrayList<String>(); //This list is ignored.
        ArrayList<String> dropList = new ArrayList<String>();
        ArrayList<String> truncateList = new ArrayList<String>(); //This list is ignored.
        StringBuilder state = new StringBuilder ();
        boolean retry = true;
        int retryCount = 0;
        int retrySleep = TM_SLEEP;
        do
        {
            try {
                tmDDL.getRow(transactionState.getTransactionId(), state, createList, createIncrList, dropList, truncateList);
                retry = false;
            }
            catch(IOException e){
                if(LOG.isInfoEnabled()) LOG.info("Exception in doCommitDDL, Step: getRow. txID: " + transactionState.getTransactionId() + "Exception: " , e);

                if (retryCount == RETRY_ATTEMPTS)
                {
                    LOG.error("Fatal Exception in doCommitDDL, Step: getRow. Raising CommitUnsuccessfulException txID: " + transactionState.getTransactionId() + "Exception: " + e);

                   //if tmDDL is unreachable at this point, it is fatal.
                    throw new UnsuccessfulDDLException(e);
                }
                if (retry) 
                   retrySleep = retry(retrySleep);
             }
         }  while (retry && retryCount++ <= RETRY_ATTEMPTS);


        if (state.toString().equals("VALID") && dropList.size() > 0)
        {
            Iterator<String> di = dropList.iterator();
            while (di.hasNext())
            {
                retryCount = 0;
                retrySleep = TM_SLEEP;
                retry = true;
                String tblName = di.next();
                do
                {
                    try {
                        //physical drop of table from hbase.
                        deleteTable(transactionState, tblName);
                        retry = false;
                    }
                    catch (TableNotFoundException t) {
                        //Check for TableNotFoundException, if that is the case, no further
                        //processing needed. This is not an error. Possible we are retrying the entire set of DDL changes
                        //because tis transaction was pinned for some reason.
                        if(LOG.isInfoEnabled()) LOG.info(" Exception for " + tblName + ", but continuing txID: " + transactionState.getTransactionId(), t);
                        retry = false;
                    }
                    catch (IOException e) {
                        if(LOG.isInfoEnabled()) LOG.info("Fatal exception in doCommitDDL, Step : DeleteTable: TxID:" + transactionState.getTransactionId() + "Exception: " , e);

                        if(retryCount == RETRY_ATTEMPTS)
                        {
                            LOG.error("Fatal Exception in doCommitDDL, Step: DeleteTable. Raising CommitUnsuccessfulException TxID:" + transactionState.getTransactionId() );

                            //Throw this exception after all retry attempts.
                            //Throwing a new exception gets out of the loop.
                            throw new UnsuccessfulDDLException(e);
                        }
                        if (retry) 
                            retrySleep = retry(retrySleep);
                    }
                } while (retry && retryCount++ <= RETRY_ATTEMPTS);
            }//while
    }

    //update TDDL post operation, delete the transaction from TmDDL.
    retryCount = 0;
    retrySleep = TM_SLEEP;
    retry = true;
    do
    {
        try{
            tmDDL.deleteRow(transactionState.getTransactionId());
            retry = false;
        }
        catch (IOException e)
        {
            if(LOG.isInfoEnabled()) LOG.info("Fatal Exception in doCommitDDL, Step: deleteRow. txID: " + transactionState.getTransactionId() + "Exception: " , e);

            if(retryCount == RETRY_ATTEMPTS)
            {
                LOG.error("Fatal Exception in doCommitDDL, Step: deleteRow. Raising CommitUnsuccessfulException. txID: " + transactionState.getTransactionId());

                //Throw this exception after all retry attempts.
                //Throwing a new exception gets out of the loop.
                throw new UnsuccessfulDDLException(e);
            }

            if (retry) 
                retrySleep = retry(retrySleep);
        }
    } while (retry && retryCount++ <= RETRY_ATTEMPTS);

    if (LOG.isTraceEnabled()) LOG.trace("doCommitDDL  EXIT [" + transactionState.getTransactionId() + "]");
    }

    /**
     * Abort a savepoint.
     *
     * @param transactionState
     * @param savepointId
     * @throws IOException
     */
    public void abortSavepoint(final TransactionState transactionState,
                                  final long savepointId, final long pSavepointId) throws IOException {

      LOG.info("abortSavepoint -- ENTRY txId: " + transactionState.getTransactionId()
                        + " savepointId: " + savepointId);
      int loopCount = 0;
      transactionState.InitializeMessageCounts();

        List<String> tables = new ArrayList<String>();
        List<TransactionRegionLocation> uniqueLocations = new ArrayList<TransactionRegionLocation>();
        for (Integer peerId : peers) {
          if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
          if (transactionState.getParticipatingRegions().getList(peerId) == null)
             continue;
          for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
            for (TransactionRegionLocation location : tableMap.values()) {
                if (location.getReadOnly()) {
	             continue;
	        }
                if(LOG.isTraceEnabled()) {
                    LOG.trace("abortSavepoint for transaction: " + transactionState.getTransactionId()
                           + " savepointId: " + savepointId + " adding table: " + location.getRegionInfo().getTable() );
                }
                uniqueLocations.add(location);
             }
           }
        }

        for (final TransactionRegionLocation loc : uniqueLocations) {
                loopCount++;
                final int participantNum = loopCount;

			if (LOG.isTraceEnabled())
				LOG.trace("Submitting abortSavepoint for transaction: " + transactionState.getTransactionId()
                                          + " savepointId: " + savepointId + ", participant: " + participantNum
                                          + ", peer: " + loc.peerId + ", location: " + loc);
				
				threadPool.submit(new TransactionManagerCallable(transactionState, loc,
                                                                                 pSTRConfig.getPeerConnections().get(loc.peerId)) {
				public Integer call() throws IOException {
					if (!Thread.currentThread().getName().startsWith("T")) {
						Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					}

					return doAbortSavepointX(transactionState.getTransactionId(), savepointId, pSavepointId,
							participantNum, loc.getRegionInfo().getRegionNameAsString(), true /* ignoreUnknownTransaction */);
				}
			});
       }
       // all requests sent at this point, can record the count
       transactionState.completeSendInvoke(loopCount);
 
      //Wait for abort requests sent to all regions is received back.
      IOException savedException = null;
      boolean loopExit = false;
      do {
        try {
           transactionState.completeRequest();
           loopExit = true;
        }
        catch (InterruptedException ie) {}
        catch (IOException e) {
           loopExit = true;
           LOG.error("Exception in abortSavepoint for txId " + transactionState.getTransactionId()
                      + " savepointId " + savepointId, e);
           IOException e2 = new IOException("Exception in abortSavepoint for txId " + transactionState.getTransactionId()
                      + " savepointId " + savepointId);
           savedException = e2;
        }
      } while (loopExit == false);
      if (savedException != null)
        throw savedException;

      LOG.info("abortSavepoint -- EXIT txID: "
           + transactionState.getTransactionId() + " savepointId " + savepointId);
    }

    /**
     * Commit a savepoint.
     *
     * @param transactionState
     * @param savepointId
     * @throws IOException
     */
    public void commitSavepoint(final TransactionState transactionState,
                                             final long savepointId,
                                             final long pSavepointId) throws IOException {

      LOG.info("commitSavepoint -- ENTRY for savepointId: " + savepointId + " ts: " + transactionState);
      int loopCount = 0;
      transactionState.InitializeMessageCounts();

	List<String> tables = new ArrayList<String>();
	List<TransactionRegionLocation> uniqueLocations = new ArrayList<TransactionRegionLocation>();
        for (Integer peerId : peers) {
           if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
           if (transactionState.getParticipatingRegions().getList(peerId) == null)
              continue;
        Iterator<Map.Entry<String, HashMap<ByteArrayKey,TransactionRegionLocation>>>pIter =
                transactionState.getParticipatingRegions().getList(peerId).entrySet().iterator();
        while (pIter.hasNext()){
           Map.Entry<String, HashMap<ByteArrayKey,TransactionRegionLocation>> entry =
                (Map.Entry<String, HashMap<ByteArrayKey,TransactionRegionLocation>>)pIter.next();
           HashMap<ByteArrayKey,TransactionRegionLocation> locs = entry.getValue();
           Iterator<Map.Entry<ByteArrayKey,TransactionRegionLocation>> li = locs.entrySet().iterator();
           while (li.hasNext()){
              Map.Entry<ByteArrayKey,TransactionRegionLocation> keyEntry = (Map.Entry<ByteArrayKey,TransactionRegionLocation>)li.next();
              TransactionRegionLocation location = keyEntry.getValue();
              if (location.getReadOnly()) {
                 continue;
              }
              if(LOG.isTraceEnabled()) {
                 LOG.trace("commitSavepoint for transaction: " + transactionState.getTransactionId()
                    + " savepointId: " + savepointId + " adding table: " + location.getRegionInfo().getTable() );
               }
                   uniqueLocations.add(location);
	     }
           }
        }
		CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
        for (final TransactionRegionLocation loc : uniqueLocations) {
			loopCount++;
			final int participantNum = loopCount;

            if(LOG.isTraceEnabled()) LOG.trace("Submitting commitSavepoint for transaction: "
                    + transactionState.getTransactionId()
                    + " savepointId: " + savepointId + ", participant: " + participantNum
                    + ", peer: " + loc.peerId
                    + ", location: " + loc);
         compPool.submit(new TransactionManagerCallable(transactionState, loc, pSTRConfig.getPeerConnections().get(loc.peerId)) {
             public Integer call() throws IOException {
				 if (!Thread.currentThread().getName().startsWith("T")) {
					 Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
				 }

                 return doCommitSavepointX(transactionState.getTransactionId(),
				 	                          savepointId, pSavepointId, participantNum,
                                                                  loc.getRegionInfo().getRegionNameAsString(), true /*ignoreUnknownTransaction */);
             }
         });
      }
      // all requests sent at this point, can record the count
      transactionState.completeSendInvoke(loopCount);

      //Wait for commit requests sent to all regions is received back.
      IOException savedException = null;
      boolean loopExit = false;
      do {
        try {
           transactionState.completeRequest();
           loopExit = true;
        }
        catch (InterruptedException ie) {}
        catch (IOException e) {
           loopExit = true;
           LOG.error("Exception in commitSavepoint for txId " + transactionState.getTransactionId()
                      + " savepointId " + savepointId, e);
           IOException e2 = new IOException("Exception in commitSavepoint for txId " + transactionState.getTransactionId()
                      + " savepointId " + savepointId);
           savedException = e2;
        }
      } while (loopExit == false);
      if (savedException != null)
        throw savedException;

      LOG.info("commitSavepoint -- EXIT txID: "
           + transactionState.getTransactionId() + " savepointId " + savepointId);
    }

    /**
     * Abort a s transaction.
     *
     * @param transactionState
     * @throws IOException
     */
    public void abort(final TransactionState transactionState) throws IOException, UnsuccessfulDDLException {

      if(LOG.isTraceEnabled()) LOG.trace("Abort -- ENTRY txID: " + transactionState.getTransactionId());
      int loopCount = 0;
      transactionState.InitializeMessageCounts();

      if (transactionState.getStatus().getValue() == TransState.STATE_RECOVERY_ABORT.getValue()){
          if(LOG.isTraceEnabled()) LOG.trace("Abort -- ts already in RECOVERY_ABORT state " + transactionState.getTransactionId());
      }
      else{
         transactionState.setStatus(TransState.STATE_ABORTED);
      }
      // (Asynchronously send aborts

      for (Integer peerId : peers) {
         if (peerId >= transactionState.getParticipatingRegions().size())
            continue;
         if (transactionState.getParticipatingRegions().getList(peerId) == null)
              continue;
         for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                 transactionState.getParticipatingRegions().getList(peerId).values()) {
          for (TransactionRegionLocation location : tableMap.values()) {
              if (location.getReadOnly()) {
                 continue;
              }
//            Add fix here for 1.4.1, optimization only applied to peer cluster
//              if ( location.getPeerId() != 0 && location.getPeerId() != getmyclusterId()  )
//              if ( !isSTRUp(location.getPeerId())   ) {
//                  // just abandon abort request if peer is SDN (local eerId should be up always) 
//                  LOG.warn("abort, abandon abort request for transaction " + transactionState.getTransactionId() +
//                               " to peer regions since peer " +
//                               peerId + " is SDN and location peer id is " + location.getPeerId());
//                  continue;
//              }  
              loopCount++;
              final int participantNum = loopCount;

              if(LOG.isDebugEnabled()) LOG.debug("Submitting abort for transaction: "
                    + transactionState.getTransactionId() + ", participant: " + participantNum
                    + ", peer: " + location.peerId
                    + ", location: " + location.getRegionInfo().getRegionNameAsString());
                    
             boolean ignoreUnknownTransactionTimeline = false;   
             String regionTableName = location.getRegionInfo().getTable().getNameAsString();  
             if ( (transactionState.getTmTableAttr(regionTableName) & TIMELINE) == TIMELINE ) {
                ignoreUnknownTransactionTimeline = true;
                if (LOG.isInfoEnabled()) LOG.info("abort TIMELINE set to ignore UTE, table " +
                                regionTableName + " transid " + transactionState.getTransactionId());
             }
             final boolean ignoreTimeline = ignoreUnknownTransactionTimeline;                    
              threadPool.submit(new TransactionManagerCallable(transactionState, location, 
                          pSTRConfig.getPeerConnections().get(location.peerId)) {
                public Integer call() throws IOException {
					if (!Thread.currentThread().getName().startsWith("T")) {
						Thread.currentThread().setName("TransactionManagerCallable-" + Thread.currentThread().getName());
					}

                  return doAbortX(transactionState.getTransactionId(), participantNum, location.isTableRecodedDropped(), ignoreTimeline);
                }
              });
           }
         }
      }
        // all requests sent at this point, can record the count
        transactionState.completeSendInvoke(loopCount);
         
       IOException savedException = null;

       //First wait for abort requests sent to all regions is received back.
       //This TM thread gets SUSPENDED until all abort threads complete!!!
       boolean loopExit = false;
       do
       {
         try {
           transactionState.completeRequest();
           loopExit = true;
         }
         catch (InterruptedException ie) {}
         catch (IOException e) {
            loopExit = true;
            LOG.error("Exception at the time of aborting DML before processing DDL", e);
            savedException = e;
         }
       } while (loopExit == false);
       //if DDL is involved with this transaction, need to unwind it.
       if(transactionState.hasDDLTx()){
          abortDDL(transactionState);
       }
       if (savedException != null){
         throw savedException;
       }

       if(LOG.isTraceEnabled()) LOG.trace("Abort -- EXIT txID: " + transactionState.getTransactionId());

    }

    void abortDDL(final TransactionState transactionState) throws IOException, UnsuccessfulDDLException
    {
        //if tables were created, then they need to be dropped.
        ArrayList<String> createList = new ArrayList<String>();
        ArrayList<String> createIncrList = new ArrayList<String>();
        ArrayList<String> dropList = new ArrayList<String>();
        ArrayList<String> truncateList = new ArrayList<String>();
        StringBuilder state = new StringBuilder ();
        boolean retry = true;
        int retryCount = 0;
        int retrySleep = TM_SLEEP;

        do
        {
            try {
                tmDDL.getRow(transactionState.getTransactionId(), state, createList, createIncrList, dropList, truncateList);
                retry = false;
            }
            catch (IOException e){
                if(LOG.isInfoEnabled()) LOG.info("Fatal Exception in abortDDL, Step: getRow. txID: " + transactionState.getTransactionId() + "Exception: " , e);

                if(retryCount == RETRY_ATTEMPTS)
                {
                    LOG.error("Fatal Exception in abortDDL, Step: getRow. Raising UnsuccessfulDDLException txID: " + transactionState.getTransactionId() + "Exception: ", e);

                   //if tmDDL is unreachable at this point, it is fatal.
                    throw new UnsuccessfulDDLException(e);
                }

                if (retry) 
                    retrySleep = retry(retrySleep);
            }
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);


        // if tables were recorded to be truncated on an upsert using load,
        // then they will be truncated on an abort transaction
        if (state.toString().equals("VALID") && truncateList.size() > 0)
        {
            if(LOG.isTraceEnabled()) LOG.trace("truncateList -- ENTRY txID: " + transactionState.getTransactionId());

            Iterator<String> ci = truncateList.iterator();
            while (ci.hasNext())
            {
                retryCount = 0;
                retrySleep = TM_SLEEP;
                retry = true;
                String tblName = ci.next();
                do
                {
                    try {
                        truncateTable(transactionState, tblName);
                        retry = false;
                    }
                    catch (IOException e){
                        if(LOG.isInfoEnabled()) LOG.info("Fatal exception in abortDDL, Step : truncateTable: TxID:" + transactionState.getTransactionId() + "Exception: ", e);

                        if(retryCount == RETRY_ATTEMPTS)
                        {
                            LOG.error("Fatal Exception in abortDDL, Step: truncateTable. Raising UnsuccessfulDDLException TxID:" + transactionState.getTransactionId() );

                            //Throw this exception after all retry attempts.
                            //Throwing a new exception gets out of the loop.
                            throw new UnsuccessfulDDLException(e);
                        }
                        if (retry) 
                           retrySleep = retry(retrySleep);
                    }
                } while (retry && retryCount++ <= RETRY_ATTEMPTS);
            }//while
        }

        if(state.toString().equals("VALID") && createList.size() > 0)
        {
            Iterator<String> ci = createList.iterator();
            while (ci.hasNext())
            {
                retryCount = 0;
                retrySleep = TM_SLEEP;
                retry = true;
                String tblName = ci.next();
                do
                {
                    try {
                        deleteTable(transactionState, tblName);
                        retry = false;
                    }
                    catch (TableNotFoundException t) {
                        //Check for TableNotFoundException, if that is the case, no further
                        //processing needed. This is not an error. Possible we are retrying the entire set of DDL changes
                        //because this transaction is being redriven for some reason.
                        if(LOG.isWarnEnabled()) LOG.warn(" Exception for " + tblName
                                + ", but continuing txID: " + transactionState.getTransactionId(), t);
                        retry = false;
                    }
                    catch (IOException e) {
                        if(LOG.isInfoEnabled()) LOG.info("Fatal exception in abortDDL, Step : DeleteTable: TxID:" + transactionState.getTransactionId() + "Exception: " , e);

                        if(retryCount == RETRY_ATTEMPTS)
                        {
                            LOG.error("Fatal Exception in abortDDL, Step: DeleteTable. Raising UnsuccessfulDDLException TxID:" + transactionState.getTransactionId() );

                            //Throw this exception after all retry attempts.
                            //Throwing a new exception gets out of the loop.
                            throw new UnsuccessfulDDLException(e);
                        }

                        if (retry) 
                           retrySleep = retry(retrySleep);
                    }
                } while (retry && retryCount++ <= RETRY_ATTEMPTS);
            }//while
        }
        
        if(state.toString().equals("VALID") && createIncrList.size() > 0)
        {
            Iterator<String> ci = createIncrList.iterator();
            while (ci.hasNext())
            {
                retryCount = 0;
                retrySleep = TM_SLEEP;
                retry = true;
                String tblName = ci.next();
                do
                {
                    try {
                        deleteTable(transactionState, tblName);
                        retry = false;
                    }
                    catch (TableNotFoundException t) {
                        //Check for TableNotFoundException, if that is the case, no further
                        //processing needed. This is not an error. Possible we are retrying the entire set of DDL changes
                        //because this transaction is being redriven for some reason.
                        if(LOG.isWarnEnabled()) LOG.warn(" Exception for " + tblName
                                + ", but continuing txID: " + transactionState.getTransactionId(), t);
                        retry = false;
                    }
                    catch (IOException e) {
                        if(LOG.isInfoEnabled()) LOG.info("Fatal exception in abortDDL, Step : DeleteTable: TxID:" + transactionState.getTransactionId() + "Exception: " , e);

                        if(retryCount == RETRY_ATTEMPTS)
                        {
                            LOG.error("Fatal Exception in abortDDL, Step: DeleteTable. Raising UnsuccessfulDDLException TxID:" + transactionState.getTransactionId() );

                            //Throw this exception after all retry attempts.
                            //Throwing a new exception gets out of the loop.
                            throw new UnsuccessfulDDLException(e);
                        }

                        if (retry) 
                           retrySleep = retry(retrySleep);
                    }
                } while (retry && retryCount++ <= RETRY_ATTEMPTS);
                
                try {
                  //Now delete the snapshot registered during create.
                    final String tag = tblName + "_" + String.valueOf(transactionState.getTransactionId()) + "_BRC_";
                    brClient.deleteBackup(tag, false, false, false);
                }catch (Exception e) {
                  //Log as warning until full concurrency support in BR. 
                  LOG.warn("Exception in abortDDL, Step:deleteBackup TableName: " + tblName + " TxID: " + transactionState.getTransactionId(), e);
                  //throw new IOException(e);
                }
                
            }//while
        }


        //if tables were recorded dropped, then they need to be reinstated,
        //depending on the state of the transaction. The table recorded as dropped in phase 0,
        //will be disabled as part of prepareCommit and physically dropped as part of doCommit.
        if(state.toString().equals("VALID") && dropList.size() > 0)
        {
            Iterator<String> di = dropList.iterator();
            while (di.hasNext())
            {
                retryCount = 0;
                retrySleep = TM_SLEEP;
                retry = true;
                String tblName = di.next();
                do
                {
                    try {
                        enableTable(transactionState, tblName);
                        retry = false;
                    }
                    catch (TableNotFoundException t) {
                        //Check for TableNotFoundException, if that is the case, no further
                        //processing needed. This would happen if the table is created and dropped in the same transaction 
                        if(LOG.isWarnEnabled()) LOG.warn(" Exception enabling table " + tblName
                                + ", but continuing txID: " + transactionState.getTransactionId(), t);
                        retry = false;
                    }
                    catch (IOException e) {
                        if(LOG.isInfoEnabled()) LOG.info("Fatal exception in abortDDL, Step : enableTable: TxID:" + transactionState.getTransactionId() + "Exception: ", e);
                        if(retryCount == RETRY_ATTEMPTS)
                        {
                            LOG.error("Fatal Exception in abortDDL, Step: enableTable. Raising UnsuccessfulDDLException TxID:" + transactionState.getTransactionId() );

                            //Throw this exception after all retry attempts.
                            //Throwing a new exception gets out of the loop.
                            throw new UnsuccessfulDDLException(e);
                        }

                        if (retry) 
                           retrySleep = retry(retrySleep);
                    }
                } while (retry && retryCount++ <= RETRY_ATTEMPTS);

            }//while
        }

        //update TDDL post operation, delete the transaction from TmDDL
        retryCount = 0;
        retrySleep = TM_SLEEP;
        retry = true;
        do
        {
            try{
                tmDDL.deleteRow(transactionState.getTransactionId());
                retry = false;
            }
            catch (IOException e)
            {
                if(LOG.isInfoEnabled()) LOG.info("Fatal Exception in abortDDL, Step: deleteRow. txID: " + transactionState.getTransactionId() + "Exception: ", e);
                if(retryCount == RETRY_ATTEMPTS)
                {
                    LOG.error("Fatal Exception in abortDDL, Step: deleteRow. Raising UnsuccessfulDDLException. txID: " + transactionState.getTransactionId());
                    //Throw this exception after all retry attempts.
                    //Throwing a new exception gets out of the loop.
                    throw new UnsuccessfulDDLException(e);
                }
                if (retry) 
                   retrySleep = retry(retrySleep);
            }
        } while (retry && retryCount++ <= RETRY_ATTEMPTS);
    }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }

    public void registerRegion(final TransactionState transactionState, TransactionRegionLocation location)throws IOException{

        if (LOG.isTraceEnabled()) LOG.trace("registerRegion ENTRY, transactionState:" + transactionState
                     + " location: " + location);

        if(transactionState.addRegion(location)){
           if ((location.peerId != 0) && (location.peerId != pSTRConfig.getTrafClusterIdInt())) {
              transactionState.setHasRemotePeers(true);
              if (LOG.isTraceEnabled()) LOG.trace("registerRegion: txn has RemotePeer" 
                            + ", this cluster ID: " + pSTRConfig.getTrafClusterIdInt()
                            + ", participant cluster ID: " + location.peerId);
           }
           if (LOG.isTraceEnabled()) LOG.trace("registerRegion -- added region: " + location + " to tx " + transactionState.getTransactionId()
                  + " [peer id: " + location.peerId + "]");
        }
        else {
           if (LOG.isTraceEnabled()) LOG.trace("registerRegion -- region previously added: " + location.getRegionInfo().getRegionNameAsString() + " with endKey: "
                      + Hex.encodeHexString(location.getRegionInfo().getEndKey()));
        }
        if (LOG.isTraceEnabled()) LOG.trace("registerRegion EXIT");
    }

     public void createTable(final TransactionState transactionState, 
                                    HTableDescriptor desc,
                                    Object[]  beginEndKeys,
                                    int options) throws IOException {
       
        if (LOG.isTraceEnabled()) LOG.trace("createTable ENTRY, table: "
                 + desc.getTableName().getNameAsString()
                 + " transactionState: " + transactionState.getTransactionId()
                 + " increment attribute : " + options);
        
        Admin admin = connection.getAdmin();
        try {
          if (beginEndKeys != null && beginEndKeys.length > 0) {
             byte[][] keys = new byte[beginEndKeys.length][];
             if (LOG.isTraceEnabled()) LOG.trace("createTable partition # 0 with key value [empty]");
             for (int i = 0; i < beginEndKeys.length; i++){
                keys[i] = (byte[])beginEndKeys[i];
                if (LOG.isTraceEnabled()) LOG.trace("createTable partition # "
                          + (i + 1) + " with key value " + Hex.encodeHexString(keys[i]));
             }
             admin.createTable(desc, keys);
          }
          else {
            admin.createTable(desc);
          }
          
          // make sure the table is enabled
          TableName tableName = desc.getTableName();
          boolean retry = false;
          do {
            retry = false;
            if (!admin.isTableEnabled(tableName) || !admin.isTableAvailable(tableName)) {
              retry = true;
            }
            if (retry) 
              retry(TM_SLEEP);
          } while (retry);          
          
          if (LOG.isTraceEnabled()) LOG.trace("Adding \'" + desc.getNameAsString() + "\' to g_createdTables");
          g_createdTables.add(desc.getNameAsString());
          List<HRegionLocation> locs = connection.getRegionLocator(desc.getTableName()).getAllRegionLocations();
          if (LOG.isTraceEnabled()) LOG.trace("createTable retrieved " + locs.size() + " locations for tx: " + transactionState.getTransactionId());
    
          // Set transaction state object as participating in ddl transaction
          transactionState.setDDLTx(true);
    
          //record this create in TmDDL.
          tmDDL.putRow( transactionState.getTransactionId(),
              (options == 1)? "CREATE_INCR" : "CREATE", desc.getNameAsString());
    
          if (LOG.isTraceEnabled()) LOG.trace("createTable: epoch pushed into regions for : " + desc.getNameAsString()
              + " and transId: " + transactionState.getTransactionId());
          pushRegionEpoch(desc, transactionState);
          
          //options set to 1 means incremental backup type.
          //Incremental backup requires a snapshot of table 
          //registered in SnapshotMeta.
          if(options == 1) {
            try {
              Object [] backupTableList = new Object[1];
              backupTableList[0] = desc.getNameAsString();
              brClient.createSnapshotForIncrBackup(backupTableList, "nullSecurity",
               desc.getNameAsString() + "_" + String.valueOf(transactionState.getTransactionId()) + "_BRC_");
            } catch (Exception e) {
              throw new IOException(e);
            }
          }
        } finally {
          admin.close();
        }
        if (LOG.isTraceEnabled()) LOG.trace("createTable EXIT, transactionState: " + transactionState.getTransactionId());
    }

    private class ChangeFlags {
        boolean tableDescriptorChanged;
        boolean columnDescriptorChanged;
        boolean storagePolicyChanged;
        String storagePolicy_;

        ChangeFlags() {
           tableDescriptorChanged = false;
           columnDescriptorChanged = false;
           storagePolicyChanged = false;
        }

        void setTableDescriptorChanged() {
           tableDescriptorChanged = true;
        }

        void setColumnDescriptorChanged() {
           columnDescriptorChanged = true;
       }

       boolean tableDescriptorChanged() {
          return tableDescriptorChanged;
       }

       boolean columnDescriptorChanged() {
          return columnDescriptorChanged;
       }

       void setStoragePolicyChanged(String str) {
           storagePolicy_ = str;
           storagePolicyChanged = true;
       }

       boolean storagePolicyChanged()    {
           return storagePolicyChanged;
       }
 
    }

   private ChangeFlags setDescriptors(Object[] tableOptions,
                                      HTableDescriptor desc,
                                      HColumnDescriptor colDesc,
                                      int defaultVersionsValue) {
       ChangeFlags returnStatus = new ChangeFlags();
       String trueStr = "TRUE";
       for (int i = 0; i < tableOptions.length; i++) {
           if (i == HBASE_NAME)
               continue ;
           String tableOption = (String)tableOptions[i];
           if ((i != HBASE_MAX_VERSIONS) && (tableOption.isEmpty()))
               continue ;
           switch (i) {
           case HBASE_MAX_VERSIONS:
               if (tableOption.isEmpty()) {
                   if (colDesc.getMaxVersions() != defaultVersionsValue) {
                       colDesc.setMaxVersions(defaultVersionsValue);
                       returnStatus.setColumnDescriptorChanged();
                   }
               }
               else {
                   colDesc.setMaxVersions
                       (Integer.parseInt(tableOption));
                   returnStatus.setColumnDescriptorChanged();
               }
               break ;
           case HBASE_MIN_VERSIONS:
               colDesc.setMinVersions
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_TTL:
               colDesc.setTimeToLive
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKCACHE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setBlockCacheEnabled(true);
               else
                   colDesc.setBlockCacheEnabled(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_IN_MEMORY:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setInMemory(true);
               else
                   colDesc.setInMemory(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ"))
                   colDesc.setCompressionType(Algorithm.GZ);
               else if (tableOption.equalsIgnoreCase("LZ4"))
                   colDesc.setCompressionType(Algorithm.LZ4);
               else if (tableOption.equalsIgnoreCase("LZO"))
                   colDesc.setCompressionType(Algorithm.LZO);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY"))
                   colDesc.setCompressionType(Algorithm.SNAPPY);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOOMFILTER:
               if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setBloomFilterType(BloomType.NONE);
               else if (tableOption.equalsIgnoreCase("ROW"))
                   colDesc.setBloomFilterType(BloomType.ROW);
               else if (tableOption.equalsIgnoreCase("ROWCOL"))
                   colDesc.setBloomFilterType(BloomType.ROWCOL);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKSIZE:
               colDesc.setBlocksize
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_ENCRYPTION:
               if (tableOption.equalsIgnoreCase("AES"))
                   colDesc.setEncryptionType("AES");
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_DATA_BLOCK_ENCODING:
               if (tableOption.equalsIgnoreCase("DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
               else if (tableOption.equalsIgnoreCase("FAST_DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
               else if (tableOption.equalsIgnoreCase("PREFIX"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
               else if (tableOption.equalsIgnoreCase("PREFIX_TREE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_BLOOMS_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheBloomsOnWrite(true);
               else
                   colDesc.setCacheBloomsOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_DATA_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheDataOnWrite(true);
               else
                   colDesc.setCacheDataOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_INDEXES_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheIndexesOnWrite(true);
               else
                   colDesc.setCacheIndexesOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPACT_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ"))
                   colDesc.setCompactionCompressionType(Algorithm.GZ);
               else if (tableOption.equalsIgnoreCase("LZ4"))
                   colDesc.setCompactionCompressionType(Algorithm.LZ4);
               else if (tableOption.equalsIgnoreCase("LZO"))
                   colDesc.setCompactionCompressionType(Algorithm.LZO);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompactionCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY"))
                   colDesc.setCompactionCompressionType(Algorithm.SNAPPY);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_PREFIX_LENGTH_KEY:
               desc.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY,
                             tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_EVICT_BLOCKS_ON_CLOSE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setEvictBlocksOnClose(true);
               else
                   colDesc.setEvictBlocksOnClose(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_KEEP_DELETED_CELLS:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setKeepDeletedCells(KeepDeletedCells.TRUE);
               else
                   colDesc.setKeepDeletedCells(KeepDeletedCells.FALSE);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_REPLICATION_SCOPE:
               colDesc.setScope
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_MAX_FILESIZE:
               desc.setMaxFileSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_COMPACT:
              if (tableOption.equalsIgnoreCase(trueStr))
                   desc.setCompactionEnabled(true);
               else
                   desc.setCompactionEnabled(false);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_DURABILITY:
               if (tableOption.equalsIgnoreCase("ASYNC_WAL"))
                   desc.setDurability(Durability.ASYNC_WAL);
               else if (tableOption.equalsIgnoreCase("FSYNC_WAL"))
                   desc.setDurability(Durability.FSYNC_WAL);
               else if (tableOption.equalsIgnoreCase("SKIP_WAL"))
                   desc.setDurability(Durability.SKIP_WAL);
               else if (tableOption.equalsIgnoreCase("SYNC_WAL"))
                   desc.setDurability(Durability.SYNC_WAL);
               else if (tableOption.equalsIgnoreCase("USE_DEFAULT"))
                   desc.setDurability(Durability.USE_DEFAULT);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_MEMSTORE_FLUSH_SIZE:
               desc.setMemStoreFlushSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_HDFS_STORAGE_POLICY:
               //TODO HBase 2.0 support this
               //So when come to HBase 2.0, no need to do this via HDFS, just set here
             returnStatus.setStoragePolicyChanged(tableOption);
             break ;
           case HBASE_SPLIT_POLICY:
                  // This method not yet available in earlier versions
                  // desc.setRegionSplitPolicyClassName(tableOption)); 
               desc.setValue(HTableDescriptor.SPLIT_POLICY, tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           default:
               break;
           }
       }

       return returnStatus;
   }

    private void waitForCompletion(String tblName,Admin admin)
       throws IOException {
       // poll for completion of an asynchronous operation
       boolean keepPolling = true;
       while (keepPolling) {
          // status.getFirst() returns the number of regions yet to be updated
          // status.getSecond() returns the total number of regions
          Pair<Integer,Integer> status = admin.getAlterStatus(tblName.getBytes());

          keepPolling = (status.getFirst() > 0) && (status.getSecond() > 0);
          if (keepPolling) {
          try {
             Thread.sleep(2000); // sleep two seconds or until interrupted
             }
             catch (InterruptedException e) {
                // ignore the interruption and keep going
             }
          }  
       }
    }       

 
    public void alterTable(final TransactionState transactionState, String tblName, Object[]  tableOptions)
           throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("alterTable ENTRY, transactionState: " + transactionState.getTransactionId());
        Admin admin = connection.getAdmin();
        try { 
           TableName tableName = TableName.valueOf(tblName);
           HTableDescriptor htblDesc = admin.getTableDescriptor(tableName);
           HColumnDescriptor[] families = htblDesc.getColumnFamilies();
           HColumnDescriptor colDesc = families[0];  // Trafodion keeps SQL columns only in first column family
           int defaultVersionsValue = colDesc.getMaxVersions();

           ChangeFlags status =
              setDescriptors(tableOptions,htblDesc /*out*/,colDesc /*out*/, defaultVersionsValue);
           
           if (status.tableDescriptorChanged()) {
              admin.modifyTable(tableName,htblDesc);
              waitForCompletion(tblName,admin);
           }
           else if (status.columnDescriptorChanged()) {
              admin.modifyColumn(tableName,colDesc);
              waitForCompletion(tblName,admin);
           }
           else if (status.storagePolicyChanged()) {
             setStoragePolicy(tblName, status.storagePolicy_);
           }
        } finally {
           admin.close();
        }

           // Set transaction state object as participating in ddl transaction
           transactionState.setDDLTx(true);

           //record this create in TmDDL.
           tmDDL.putRow( transactionState.getTransactionId(), "ALTER", tblName);
           if (LOG.isInfoEnabled()) LOG.info("alterTable EXIT for table: "
              + tblName + ", transactionState: " + transactionState.getTransactionId());
    }

    public void registerTruncateOnAbort(final TransactionState transactionState, String tblName)
            throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("registerTruncateOnAbort ENTRY, TxID " + transactionState.getTransactionId() +
            " tableName: " + tblName);

        // register the truncate on abort to TmDDL
            // add truncate record to TmDDL
            tmDDL.putRow(transactionState.getTransactionId(), "TRUNCATE", tblName);

            // Set transaction state object as participating in ddl transaction.
            transactionState.setDDLTx(true);
    }

    public void dropTable(final TransactionState transactionState, String tblName)
            throws IOException {
        if (LOG.isInfoEnabled()) LOG.info("dropTable ENTRY, TxId: "
             + transactionState.getTransactionId() + " TableName: " + tblName);

        //Record this drop table request in TmDDL.
        //Note that physical disable of this table happens in prepare phase.
        //Followed by physical drop of this table in commit phase.
        // add drop record to TmDDL.
        tmDDL.putRow( transactionState.getTransactionId(), "DROP", tblName);

        // Set transaction state object as participating in ddl transaction.
        transactionState.setDDLTx(true);
           
        for (Integer peerId : peers) {
           if (peerId >= transactionState.getParticipatingRegions().size())
              continue;
           if (transactionState.getParticipatingRegions().getList(peerId) == null)
              continue;
        // Also set a flag in all current participating regions belonging to this table
        // to indicate this table is recorded for drop.
            HashMap<ByteArrayKey,TransactionRegionLocation> regionMap =  
                  transactionState.getParticipatingRegions().getList(peerId).get(tblName);
            if (regionMap != null) {
               for (TransactionRegionLocation trRegion : regionMap.values()) 
                  trRegion.setTableRecordedDropped();
           }
        }
    }

    //Called only by Abort or Commit processing.
    public void deleteTable(final TransactionState transactionState, final String tblName)
            throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("deleteTable ENTRY, TxId: " + transactionState.getTransactionId() + " tableName " + tblName);
        disableTable(transactionState, tblName);
        Admin admin = connection.getAdmin();
        admin.deleteTable(TableName.valueOf(tblName));
        admin.close();
    }

    //Called only by Abort processing.
    public void enableTable(final TransactionState transactionState, String tblName)
            throws IOException{
        if (LOG.isTraceEnabled()) LOG.trace("enableTable ENTRY, TxID: " + transactionState.getTransactionId() + " tableName " + tblName);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tblName);
        if (admin.isTableDisabled(tableName))
           admin.enableTable(TableName.valueOf(tblName));
        admin.close();
    }

    // Called only by Abort processing to delete data from a table
    public void truncateTable(final TransactionState transactionState, String tblName)
            throws IOException{
        if (LOG.isTraceEnabled()) LOG.trace("truncateTable ENTRY, TxID: " + transactionState.getTransactionId() +
            "table: " + tblName);
        Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tblName);
            HTableDescriptor hdesc = admin.getTableDescriptor(tableName);

            // To be changed in 2.0 for truncate table
            if (admin.isTableEnabled(tableName))
               admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.createTable(hdesc);
            admin.close();
    }
    
    //Called only by DoPrepare.
    public void disableTable(final TransactionState transactionState, String tblName)
            throws IOException{
        if (LOG.isInfoEnabled()) LOG.info("disableTable ENTRY, TxID: " + transactionState.getTransactionId() + " tableName " + tblName);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tblName);
            if (admin.isTableEnabled(tableName))
               admin.disableTable(tableName);
            admin.close();
        if (LOG.isTraceEnabled()) LOG.trace("disableTable EXIT, TxID: " + transactionState.getTransactionId() + " tableName " + tblName);
    }

    /**
     * @param hostnamePort
     * @param regionArray
     * @param tmid
     * @return
     * @throws Exception
     */
    public List<Long> recoveryRequest (Connection rt_conn, String hostnamePort, byte[] regionArray, int tmid) throws DeserializationException, IOException {
        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmid + " recoveryRequest -- ENTRY TM" + tmid + " hostname " + hostnamePort +
                                " rt connection, " + rt_conn);        
        HRegionInfo regionInfo = null;
        Table table = null;

        regionInfo = HRegionInfo.parseFrom(regionArray);
//        RegionLocator rl = pSTRConfig.getPeerConnections().get(0).getRegionLocator(regionInfo.getTable());
//        RegionLocator rl = this.connection.getRegionLocator(regionInfo.getTable());
        RegionLocator rl = rt_conn.getRegionLocator(regionInfo.getTable());
        HRegionLocation hrl = rl.getRegionLocation(regionInfo.getStartKey(), true);
        regionInfo = hrl.getRegionInfo();
        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: " + tmid + " TransactionManager:recoveryRequest tmid " + tmid
             + " regionInfo refreshed for: ["
             + hrl.getRegionInfo().getRegionNameAsString() + "] startKey: "
             + Hex.encodeHexString(hrl.getRegionInfo().getStartKey()));        

        final String regionName = regionInfo.getRegionNameAsString();
        final int tmID = tmid;
        if (LOG.isTraceEnabled()) LOG.trace("TransactionManager:recoveryRequest regionInfo encoded name: [" + regionInfo.getEncodedName() + "]" + " hostname " + hostnamePort);
        Batch.Call<TrxRegionService, RecoveryRequestResponse> callable =
                new Batch.Call<TrxRegionService, RecoveryRequestResponse>() {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<RecoveryRequestResponse> rpcCallback =
                new BlockingRpcCallback<RecoveryRequestResponse>();

              @Override
              public RecoveryRequestResponse call(TrxRegionService instance) throws IOException {
                org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.RecoveryRequestRequest.Builder rbuilder = RecoveryRequestRequest.newBuilder();
                rbuilder.setTransactionId(-1);
                rbuilder.setRegionName(ByteString.copyFromUtf8(regionName));
                rbuilder.setTmId(tmID);

                instance.recoveryRequest(controller, rbuilder.build(), rpcCallback);
                return rpcCallback.get();
              }
            };

            // Working out the begin and end keys
            byte[] startKey = regionInfo.getStartKey();
            byte[] endKey = regionInfo.getEndKey();

            if(endKey != HConstants.EMPTY_END_ROW)
               endKey = TransactionManager.binaryIncrementPos(endKey, -1);

	    //TBD : The parameter '0' to getPeerConnections().get() may need to be something else
//            table = pSTRConfig.getPeerConnections().get(0).getTable(regionInfo.getTable(), cp_tpe);
//            table = this.connection.getTable(regionInfo.getTable());
            table = rt_conn.getTable(regionInfo.getTable());

            Map<byte[], RecoveryRequestResponse> rresult = null;
            try {
              rresult = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
            }
            catch (ServiceException se) {
                LOG.error("Service exception thrown in recoveryRequest: ", se);
                throw new IOException("Service exception thrown in recoveryRequest:", se);
            }
            catch (Throwable t) {
                LOG.error("Exception thrown in recoveryRequest: ", t);
                throw new IOException("Exception thrown in recoveryRequest: ", t);
            }
            finally {
            	table.close();
            }

        Collection<RecoveryRequestResponse> results = rresult.values();
        RecoveryRequestResponse[] resultArray = new RecoveryRequestResponse[results.size()];
        results.toArray(resultArray);

        if(resultArray.length == 0) {
            table.close();
            LOG.error("Problem with calling recoveryRequest, no regions returned result.  TMID " + tmID
                    + " region: " + regionName);
            // reestablish connection
            this.connection.close();
            this.connection = ConnectionFactory.createConnection(this.config);
            throw new IOException("Problem with calling recoveryRequest, no regions returned result \n"
                                   + " TMid: " + tmid + " region: " + regionName);
        }

        //return tri.recoveryRequest(regionInfo.getRegionName(), tmid);
        table.close();
        if (LOG.isTraceEnabled()) LOG.trace("recoveryRequest -- EXIT TM" + tmid);

        return resultArray[0].getResultList();
    }

    public void releaseRowLock(Connection rt_conn, byte[] regionArray, final List<Long> tidList) throws DeserializationException, IOException {
        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV: releaseRowLock -- ENTRY rt connection, " + rt_conn); 
        Table table = null;
        HRegionInfo regionInfo = HRegionInfo.parseFrom(regionArray);
        RegionLocator rl = rt_conn.getRegionLocator(regionInfo.getTable());
        boolean refresh = false;
        int retryCount = 0;

        do {
            HRegionLocation hrl = rl.getRegionLocation(regionInfo.getStartKey(), refresh);
            refresh = false;
            retryCount++;
            regionInfo = hrl.getRegionInfo();
            final String regionName = regionInfo.getRegionNameAsString();
            Batch.Call<TrxRegionService, ReleaseLockResponse> callable =
                    new Batch.Call<TrxRegionService, ReleaseLockResponse>() {
                  ServerRpcController controller = new ServerRpcController();
                  BlockingRpcCallback<ReleaseLockResponse> rpcCallback =
                    new BlockingRpcCallback<ReleaseLockResponse>();

                  @Override
                  public ReleaseLockResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.ReleaseLockRequest.Builder rbuilder = ReleaseLockRequest.newBuilder();
                    rbuilder.addAllTransactionIds(tidList);
                    rbuilder.setRegionName(ByteString.copyFromUtf8(regionName));

                    instance.releaseLock(controller, rbuilder.build(), rpcCallback);
                    return rpcCallback.get();
                  }
                };

            byte[] startKey = regionInfo.getStartKey();
            byte[] endKey = regionInfo.getEndKey();
            if(endKey != HConstants.EMPTY_END_ROW)
                endKey = TransactionManager.binaryIncrementPos(endKey, -1);
            table = rt_conn.getTable(regionInfo.getTable());

            Map<byte[], ReleaseLockResponse> rresult = null;
            try {
                rresult = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
            } catch (ServiceException se) {
                refresh = true;
                LOG.error("Service exception thrown in releaseLock: ", se);
                if (retryCount == 2)
                    throw new IOException("Service exception thrown in releaseLock:", se);
            } catch (Throwable t) {
                refresh = true;
                LOG.error("Exception thrown in releaseLock: ", t);
                if (retryCount == 2)
                    throw new IOException("Exception thrown in releaseLock: ", t);
            } finally {
                table.close();
            }

            if (rresult.size() == 0) {
                refresh = true;
                LOG.error("releaseRowLock received incorrect result size: 0. Location " + regionInfo.getRegionNameAsString());
            } else if (rresult.size() == 1) {
                ReleaseLockResponse response = rresult.values().iterator().next();
                if (response.getHasException()) {
                    LOG.error("releaseRowLock received exception: " + response.getException());
                }
            }
        } while (refresh && retryCount < 2);

    }

    public void setStoragePolicy(String tblName, String policy)
      throws IOException {

      int retryCount = 0;
      int retrySleep = TM_SLEEP;
      boolean retry = false;
      org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService.BlockingInterface service;
      org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafSetStoragePolicyRequest.Builder request ; 
      try {
          Table tbl = connection.getTable(TableName.valueOf(tblName));
          String rowkey = "0";
          CoprocessorRpcChannel channel = tbl.coprocessorService(rowkey.getBytes());
          service =
            org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService.newBlockingStub(channel);
          request =
           org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafSetStoragePolicyRequest.newBuilder();
          String hbaseRoot = config.get("hbase.rootdir");
          FileSystem fs = FileSystem.get(config);
          //Construct the HDFS dir
          //find out if namespace is there
          String[] parts = tblName.split(":");
          String namespacestr="";
          String fullPath = hbaseRoot + "/data/" ;
          String fullPath2 = hbaseRoot + "/data/default/";
          if(fs.exists(new Path(fullPath2)) && parts.length == 1)  // no namespace in the path
            fullPath = fullPath2;

          if(parts.length >1) //have namespace
          {
            fullPath = fullPath + parts[0] + "/" + parts[1];
          }
          else
            fullPath = fullPath + tblName;

          request.setPath(fullPath);
          request.setPolicy(policy);
      }
      catch (Exception e) {
          throw new IOException(e);
      }
        
      do {
          org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafSetStoragePolicyResponse ret = null;
          try{
            ret = service.setStoragePolicy(null,request.build());
           }
           catch(ServiceException se) {
             String msg = new String ("ERROR occurred while calling coprocessor service in setStoragePolicy, retry due to ");
             LOG.warn(msg, se);
             retry = true;
          }
          catch(Throwable te)
          {
             LOG.error("ERROR occurred while calling coprocessor service in setStoragePolicy, not retry due to ", te);
             retry = false;
          }
          //handle result and error
          if( ret == null)
          {
            retry = false;
            LOG.error("setStoragePolicy Response ret null , not retry");
          }
          else if (ret.getStatus() == false)
          {
            LOG.error("setStoragePolicy Response ret false." +  ret.getException());
            throw new IOException(ret.getException());
          }
          if(retryCount == RETRY_ATTEMPTS)
          {
            throw new IOException("coprocessor not response");
          }
          if (retry) 
              retrySleep = retry(retrySleep);
      } while (retry && retryCount++ < RETRY_ATTEMPTS);
    }
}

