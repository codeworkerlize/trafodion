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

import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.SimpleDateFormat;
import java.lang.Integer;
import java.lang.management.ManagementFactory;

import org.apache.commons.codec.binary.Hex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteArrayKey;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;
import com.esgyn.common.LicenseHelper;

import org.apache.hadoop.hbase.client.transactional.TransactionalReturn;

/**
 * Holds client-side transaction information. Client's use them as opaque objects passed around to transaction
 * operations.
 */
public class TransactionState {

    static final Log LOG = LogFactory.getLog(TransactionState.class);

    // Current 64 bit transactionId has the following composition:
    //  int   sequenceId
    //  short nodeId
    //  byte  clusterId
    //  byte  instanceId
    private final long transactionId;
    private TransState status;
    private long startEpoch;
    private long startId;
    private long commitId;
    private long recoveryASN;
    private AtomicInteger regionsToIgnore;
    private short totalNum;

    private boolean m_HasRemotePeers;
    /**
     * 
     * requestPendingCount - how many requests send
     * requestReceivedCount - how many replies received
     * countLock - synchronize access to above
     * 
     * commitSendDone - did we sent all requests yet
     * commitSendLock - synchronize access to above
     */
    private int requestPendingCount;
    private int requestReceivedCount;
    private Object countLock;
    private boolean commitSendDone;
    private Object commitSendLock;
    private Object tnHashMapLock;
    private Object flushCheckMapLock;
    private Throwable hasError;
    private boolean localTransaction;
    private static boolean envLocalTransaction;
    private boolean ddlTrans;
    private int ddlNum;
    private static boolean useConcurrentHM = false;
    private boolean hasRetried = false;
    private boolean exceptionLogged = false;
    private String recordException;
    private static boolean skipConflictCheck = false;
    private boolean skipSdnCDC = false;
    private boolean skipRemoteCheck = false;
    private boolean generateMutations = false;
    private boolean localWriteFailed = false;
    private boolean remoteWriteFailed = false;
    private boolean mustBroadcast = false;
    private XdcTransType xdcTransType;
    private int nodeId;
    private long clusterId;
    private long instanceId;
    private boolean writeTlogOnly = false;
    private int flushCheckStatus = 0;
    private static long TM_MAX_REGIONSERVER_STRING = 3072;
    public Set<String> tableNames = Collections.synchronizedSet(new HashSet<String>());
    private TrafodionLocationList participatingRegions;
    private Object tableAttrLock = new Object();
    public HashMap<String, Integer> tableAttr = new HashMap<String, Integer>();
    public HashMap<String, Boolean> tableWalSync = new HashMap<String, Boolean>();
    public long begin_Time = 0;
    public long prepareCommit_Time = 0;
    public long doCommit_Time = 0;
    public long abortTransaction_Time = 0;
    public long completeRequest_Time = 0;
    public String queryContext = null;
    public ConcurrentHashMap<String, Integer> totalNumRespMap = new ConcurrentHashMap<String, Integer>();
    public ConcurrentHashMap<TransactionRegionLocation, Long> flushCheckRespMap = new ConcurrentHashMap<TransactionRegionLocation, Long>();

    /**
     * Regions to ignore in the twoPase commit protocol. They were read only, or already said to abort.
     */
    public boolean prepareRefresh;
    private Set<TransactionRegionLocation> retryRegions = Collections.synchronizedSet(new HashSet<TransactionRegionLocation>());

    private static Integer costTh = -1;
    private static String PID;

    public static final int XDC_UP          = 1;   // 000001
    public static final int XDC_DOWN        = 2;   // 000010
    public static final int SYNCHRONIZED    = 4;   // 000100
    public static final int SKIP_CONFLICT   = 8;   // 001000
    public static final int SKIP_REMOTE_CK  = 16;  // 010000
    public static final int INCREMENTALBR   = 32;  // 100000
    public static final int TABLE_ATTR_SET  = 64;  // 1000000
    public static final int SKIP_SDN_CDC  = 128;  // 10000000
    public static final int PIT_ALL  = 256;       // 100000000
    public static final int TIMELINE  = 512;      // 1000000000     
    public static final int WAL_SYNC_OK = 2048;


    public static final int ACTION_NONE          = 0;
    public static final int ACTION_ATTR_SET          = 1;
    public static final int ACTION_ATTR_SET_SEND          = 2;
    private volatile boolean canceled = false;

    public boolean bypassRegisterFirstRead = false;

    private native int registerRegion(long transid, long startid, int port, byte[] hostname, long startcode, byte[] regionInfo, int peerId, int tmFlags);

    public boolean islocalTransaction() {
      return localTransaction;
    }

    static {
       String localTxns = System.getenv("DTM_LOCAL_TRANSACTIONS");
       if (localTxns != null && !localTxns.trim().isEmpty()) 
          envLocalTransaction = (Integer.parseInt(localTxns.trim()) == 0) ? false : true;
       else
          envLocalTransaction = false; 
       String envSkipConflictCheck = System.getenv("TM_SKIP_CONFLICT_CHECK");
       if (envSkipConflictCheck != null && !envSkipConflictCheck.trim().isEmpty()) 
          skipConflictCheck = (Integer.parseInt(envSkipConflictCheck.trim()) == 0) ? false : true;
       String envEnableRowLevelLock = System.getenv("ENABLE_ROW_LEVEL_LOCK");
       if (envEnableRowLevelLock != null && !envEnableRowLevelLock.trim().isEmpty()) {
          skipConflictCheck = (Integer.parseInt(envEnableRowLevelLock.trim()) == 0) ? false : true;
          //check license
          if(skipConflictCheck && !LicenseHelper.isModuleOpen(LicenseHelper.Modules.ROW_LOCK))
             skipConflictCheck = false;
       }
       String costThreshold = System.getenv("RECORD_TIME_COST_TM");
       if (costThreshold != null && false == costThreshold.trim().isEmpty())
         costTh = Integer.parseInt(costThreshold);
       PID = ManagementFactory.getRuntimeMXBean().getName();
       PID = PID.split("@")[0]; 
    }

    public TransactionState(final long transactionId) throws IOException { 
        this.transactionId = transactionId;
        setStatus(TransState.STATE_ACTIVE);
        countLock = new Object();
        commitSendLock = new Object();
        tnHashMapLock = new Object();
        flushCheckMapLock = new Object();
        requestPendingCount = 0;
        requestReceivedCount = 0;
        commitSendDone = false;
        totalNum = -1;
        hasError = null;
        ddlTrans = false;
        ddlNum = 0;
        recordException = null;
        m_HasRemotePeers = false;
        recoveryASN = -1;
        flushCheckStatus = 0;
        setNodeId();
        setClusterId();
        xdcTransType = XdcTransType.XDC_TYPE_NONE;
        regionsToIgnore = new AtomicInteger(0);

        participatingRegions = new TrafodionLocationList();
        localTransaction = envLocalTransaction;
        prepareRefresh = false;
        if (localTransaction) {
          if (LOG.isTraceEnabled()) LOG.trace("TransactionState local transaction begun: " + transactionId);
        }
        else {
          if (LOG.isDebugEnabled()) LOG.debug("TransactionState global transaction begun: " + transactionId);
        }
    }

    public static TransactionState getInstance(long transactionID) throws IOException
    {
       Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
       TransactionState ts = new TransactionState(transactionID);

       long startIdVal = -1;

       // Set the startid
       if ((RMInterface.recoveryToPitMode)  || (RMInterface.envTransactionAlgorithm == RMInterface.AlgorithmType.SSCC)) {
          startIdVal = RMInterface.getTmId();
       }
       ts.setStartId(startIdVal);

       synchronized (mapTransactionStates) {
           TransactionState ts2 = mapTransactionStates.get(transactionID);
           if (ts2 != null) {
              // Some other thread added the transaction while we were creating one.  It's already in the
              // map, so we can use the existing one.
              if (LOG.isTraceEnabled()) LOG.trace("TransactionState:getInstance, found TransactionState object while creating a new one " + ts2);
               ts = ts2;
           }
           else {
              if (LOG.isTraceEnabled()) LOG.trace("TransactionState:getInstance,, adding new TransactionState to map " 
						     + ts);
              mapTransactionStates.put(transactionID, ts);
           }
       }// end synchronized
       STRConfig pSTRConfig = RMInterface.pSTRConfig;
       if (pSTRConfig.getConfiguredPeerCount() > 0){
          int lv_first_remote_peer_id = pSTRConfig.getFirstRemotePeerId();
          if (LOG.isTraceEnabled()) LOG.trace("TransactionState:getInstance,,"
						  + " PeerCount " + pSTRConfig.getConfiguredPeerCount()
						  + " lv_first_remote_peer_id: " + lv_first_remote_peer_id
						  );
          if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id).contains(PeerInfo.STR_UP)) {
              ts.setXdcType(XdcTransType.XDC_TYPE_XDC_UP);
          }
          else if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id).contains(PeerInfo.STR_DOWN)){
             ts.setXdcType(XdcTransType.XDC_TYPE_XDC_DOWN);
          }
          else {
             if (LOG.isTraceEnabled()) LOG.trace("TransactionState:getInstance, XDC status of first peerid: " 
						     + pSTRConfig.getPeerStatus(lv_first_remote_peer_id));
          }

          if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id, true).contains(PeerInfo.PEER_ATTRIBUTE_NO_CHECK)) {
              ts.setSkipRemoteCheck(true);
          }
              
          if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id, true).contains(PeerInfo.PEER_ATTRIBUTE_NO_CDC_XDC)) {
               ts.setSkipSdnCDC(true);
          }

      }
      else {
         ts.setXdcType(XdcTransType.XDC_TYPE_NONE);
      }
      if (LOG.isTraceEnabled()) LOG.trace("TransactionState:getInstance - new ts created: " + ts);
    
      if (LOG.isDebugEnabled()) LOG.debug("HAX - TransactionState:getInstance - new ts created: "
              + " trans id: " + transactionID
              + " xdc type: " + ts.getXdcType()
              + " ts details: " + ts);

      return ts;
    }

    public boolean addTableName(final String table, int tableFlags) {
        boolean added =  tableNames.add(table);
        if (added) {
           if (LOG.isTraceEnabled()) LOG.trace("Adding new table name [" + table + "] to transaction state ["
                    + transactionId + "] with table flags " + tableFlags);
           if (! tableAttr.containsKey(table)){
              // Set table attributes to 0 for now
              //int attributeValue = 0;
              if(LOG.isDebugEnabled()) LOG.debug("TransactionState.registerLocation adding table attribute "
                   + tableFlags + " to attribute map for table " + table);
              tableAttr.put(table, new Integer(tableFlags));
           }
        }
        return added;
    }

    /**
     * 
     * Method  : requestAllComplete
     * Params  : None
     * Return  : True if all replies received, False if not
     * Purpose : Make sure all requests have been sent, then determine
     *           if we've received all the replies.  Non blocking version
     */
    public boolean requestAllComplete() {

        // Make sure that we've completed sending the requests
        synchronized (commitSendLock)
        {
            if (commitSendDone != true)
                return false;
        }

        synchronized (countLock)
        {
            if (requestReceivedCount >= requestPendingCount){
               if (requestReceivedCount > requestPendingCount){
                  if (LOG.isWarnEnabled()) LOG.warn("requestAllComplete requestReceivedCount: "
                      + requestReceivedCount + " exceeds requestPendingCount: " + requestPendingCount + " for ts: " + this.toString());
               }
               return true;
            }
            return false;
        }
    }

    /**
     * 
     * Method  : requestPendingcount
     * Params  : count - how many requests were sent
     * Return  : void
     * Purpose : Record how many requests were sent
     */
    public void requestPendingCount(int count)
    {
        synchronized (countLock)
        {
            hasError = null;  // reset, just in case
            requestPendingCount = count;
        }
    }

    /**
     * 
     * method  : requestpendingcountdec
     * params  : none
     * return  : void
     * purpose : decrease number of outstanding replies needed and wake up any waiters
     *           if we receive the last one 
     */
    public void  requestPendingCountDec(Throwable exception)
    {
       synchronized (countLock)
       {
          requestReceivedCount++;
          if (exception != null && hasError == null)
             hasError = exception;
          if (requestReceivedCount == requestPendingCount)
             countLock.notify();
       }
    }

    public void requestPendingCountDec(Throwable exception, String regionName, Boolean walSyncOk)
    {
       synchronized (tableWalSync)
       {
          if (walSyncOk && regionName != null) {
              tableWalSync.put(regionName, walSyncOk);
          } else if (walSyncOk == false && regionName != null)
              if (LOG.isWarnEnabled()) {
                  LOG.warn("requestPendingCountDec region name: " + regionName + " walSyncOk: " + walSyncOk + " tid: " + transactionId);
              }
       }
       requestPendingCountDec(exception);
    }

    public void processWalSync(boolean optimize) {
       synchronized (tableWalSync) {
           int mapSize = tableWalSync.size();
           if (optimize && mapSize > 0 && mapSize >= requestPendingCount) {
               tableAttrSetAndRegister("_#ALL_REGION_WAL_SYNC#_", WAL_SYNC_OK);
               if (LOG.isDebugEnabled()) {
                   LOG.debug("processWalSync: _#ALL_REGION_WAL_SYNC#_ is going to be written to tlog ");
               }
           } else {
               for (String regionName : tableWalSync.keySet()) {
                   tableAttrSetAndRegister(regionName, WAL_SYNC_OK);
               }
               if (LOG.isDebugEnabled()) {
                   LOG.debug("processWalSync: individual region is going to be written to tlog, region size is " + tableWalSync.size());
               }
           }
           tableWalSync.clear();
       }
    }

    /**
     * 
     * Method  : completeRequest
     * Params  : None
     * Return  : Void
     * Purpose : Hang thread until all replies have been received
     */
    public void completeRequest() throws InterruptedException, IOException
    {
        // Make sure we've completed sending all requests first, if not, then wait
        synchronized (commitSendLock)
        {
            if (commitSendDone == false)
            {
                commitSendLock.wait();
            }
        }

        // if we haven't received all replies, then wait
        synchronized (countLock)
        {
            if (requestPendingCount > requestReceivedCount)
                countLock.wait();
        }

        if (hasError != null)  {
            hasError.fillInStackTrace();
            throw new IOException("Exception at completeRequest()", hasError);
        }
        return;

    }

    /**
     *
     * Method  : completeSendInvoke
     * Params  : count : number of requests sent
     * Return  : void
     * Purpose : wake up waiter that are waiting on completion of sending requests 
     */
    public void completeSendInvoke(int count)
    {

        // record how many requests sent
        requestPendingCount(count);
        if (LOG.isDebugEnabled()) LOG.debug("completeSendInvoke START with pending count: " + count + " for ts: " + this.toString());

        // wake up waiters and record that we've sent all requests
        synchronized (commitSendLock)
        {
            commitSendDone = true;
            commitSendLock.notify();
        }
        if (LOG.isDebugEnabled()) LOG.debug("completeSendInvoke complete for ts: " + this.toString());
    }
    
    /**
    *
    * Method  : recordException
    * Params  : 
    * Return  : void
    * Purpose : Record an exception encountered by executor threads. 
    */
    public synchronized void recordException(String exception)
    {
      if (exception != null && recordException == null)
        recordException = exception;
    }
    
    public String getRecordedException()
    {
      if (recordException == null){
         return "";
      }
      else{
         return recordException;
      }
    }
    public void resetRecordedException()
    {
      recordException = null;
    }
    public boolean recordedExceptionIsNull()
    {
      return recordException == null;
    }

    /**
    *
    * Method  : InitializeMessageCounts
    * Return  : void
    * Purpose : Initialize request send and received counts in anticipation of
    *           sending async messages to regions
    */
   public void InitializeMessageCounts()
   {
       synchronized (countLock)
       {
           hasError = null;  // reset, just in case
           requestPendingCount = 0;
           requestReceivedCount = 0;
           commitSendDone = false;
       }
   }

    // Used at the client end - the one performing the mutation - e.g. the SQL process
    public void registerLocation(final HRegionLocation location) throws IOException {
       if (LOG.isTraceEnabled()) LOG.trace("TransactionState.registerLocation: [" + location.getRegionInfo().getEncodedName() +
                "], endKey: " + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " transaction [" + transactionId + "]"
                 + " peerId not supplied, using 0");
       registerLocation(location, 0);  //Peer not supplied
    }

    public void registerLocation(final HRegionLocation location, final int pv_peerId) throws IOException {
       registerLocation(location, pv_peerId, 0);
    }
    // Used at the client end - the one performing the mutation - e.g. the SQL process
    public void registerLocation(final HRegionLocation location, final int pv_peerId, final int pv_tmFlags) throws IOException {
        byte [] lv_hostname = location.getHostname().getBytes();
        int lv_port = location.getPort();
        long lv_startcode = location.getServerName().getStartcode();
        byte [] lv_byte_region_info = location.getRegionInfo().toByteArray();
        if (LOG.isTraceEnabled()) LOG.trace("TransactionState.registerLocation: "
            + "[ hostname " + location.getHostname()
            + " encodedName " + location.getRegionInfo().getEncodedName()
            + "], endKey: " + Hex.encodeHexString(location.getRegionInfo().getEndKey())
            + " transaction [" + transactionId + "]"
            + "[peerId: " + pv_peerId + "], tmFlags " + pv_tmFlags);

        if (islocalTransaction()) {
          if(LOG.isTraceEnabled()) LOG.trace("TransactionState.registerLocation local transaction not sending registerRegion.");
        }
        else {
          if (lv_byte_region_info.length > TM_MAX_REGIONSERVER_STRING){
             String skey = (Bytes.equals(location.getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW)) ? "skey=null" : ("skey=" + Hex.encodeHexString(location.getRegionInfo().getStartKey()));
             String ekey = (Bytes.equals(location.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW)) ? "ekey=null" : ("ekey=" + Hex.encodeHexString(location.getRegionInfo().getEndKey()));
             IOException ioe = new IOException("RegionInfo is too large (" + lv_byte_region_info.length
                     + "), try reducing table keys for "
                     + location.getRegionInfo().getTable().getNameAsString()
                     + " skey: " + skey + " ekey: " + ekey);
             LOG.error("RegionInfo is too large (" + lv_byte_region_info.length
                     + "), try reducing table keys for "
                     + location.getRegionInfo().getTable().getNameAsString()
                     + " skey: " + skey + " ekey: " + ekey, ioe);
             throw ioe;
          }
          if (LOG.isDebugEnabled()){
              String skey = (Bytes.equals(location.getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW)) ? "skey=null" : ("skey=" + Hex.encodeHexString(location.getRegionInfo().getStartKey()));
              String ekey = (Bytes.equals(location.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW)) ? "ekey=null" : ("ekey=" + Hex.encodeHexString(location.getRegionInfo().getEndKey()));
              LOG.debug("TransactionState.registerLocation global transaction registering region "
                   + location.getRegionInfo().getTable().getNameAsString()
                   + " using byte array size: " + lv_byte_region_info.length + " for ts: "
                   + transactionId + " and startId: " + startId + " skey: " + skey + " ekey: " + ekey + " tmFlags " + pv_tmFlags);
          }
          long timeCost = System.currentTimeMillis();
          int ret = registerRegion(transactionId, startId, lv_port, lv_hostname, lv_startcode, lv_byte_region_info, pv_peerId, pv_tmFlags);
          if (ret == 4) //RET_EXCEPTION
          {
            LOG.error("registerRegion failed. transactionId = " + transactionId + ", tableName =" + location.getRegionInfo().getTable().getNameAsString());
            IOException ioe = new IOException("registerRegion failed. transactionId = " + transactionId + ", tableName = " + location.getRegionInfo().getTable().getNameAsString());
            throw ioe;
          }
          if (costTh >= 0) {
            timeCost = System.currentTimeMillis() - timeCost;
            if (timeCost >= costTh)
                if (LOG.isWarnEnabled()) {
                    LOG.warn("call tm registerRegion PID " + PID + " txID " + transactionId + " TC " + timeCost + " " + location.getRegionInfo().getRegionNameAsString());
                }
          }
        }
      }

    public boolean isMDRegion(String s) {
      if(s.contains("TRAF_RSRVD_1:TRAFODION._MD_.") ) return true;
      if(s.contains("TRAF_RSRVD_1:TRAFODION._PRIVMGR_MD_.") ) return true;
      if(s.contains("SB_HISTOGRAM") ) return true;
      if(s.contains("SB_PERSISTENT_SAMPLES") ) return true;
      return false;
    }

    //this function should be called on condition that Status is commited and
    //_#ALL_REGION_WAL_SYNC#_ does not exist in tableAttr map 
    public boolean skipRegionWithWalSynced(List<TransactionRegionLocation> skippedList) throws IOException {
        List<TransactionRegionLocation> commitList = new ArrayList<TransactionRegionLocation>();
        int prSize = getParticipatingRegions().size();
        for (int peerId = 0; peerId < prSize; peerId++) {
            if (getParticipatingRegions().getList(peerId) == null)
                continue;
            for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                               getParticipatingRegions().getList(peerId).values()) {
                for (TransactionRegionLocation location : tableMap.values()) {
                    HRegionInfo lv_hri = location.getRegionInfo();
                    String regionName = lv_hri.getEncodedName();
                    if ((getTmTableAttr(regionName) & WAL_SYNC_OK) == WAL_SYNC_OK)
                        skippedList.add(location);
                    else
                        commitList.add(location);
                }
            }
        }

        if (skippedList.size() > 0 && commitList.size() > 0) {
            renewParticipatingRegions();
            for (TransactionRegionLocation location : commitList) {
                addRegion(location);
            }
        }

        return commitList.size() > 0;
    }

    public boolean addRegion(final TransactionRegionLocation trRegion) {
        if (LOG.isDebugEnabled()) LOG.debug("addRegion ENTRY with trRegion [" + trRegion.getRegionInfo().getRegionNameAsString()
                + "], startKey: " + Hex.encodeHexString(trRegion.getRegionInfo().getStartKey()) + ", endKey: "
                  + Hex.encodeHexString(trRegion.getRegionInfo().getEndKey()) + " and transaction [" + transactionId + "]");

        boolean added = participatingRegions.add(trRegion);

        if (added) {
           if (LOG.isDebugEnabled()) LOG.debug("Added new trRegion (#" + participatingRegions.regionCount(trRegion.getPeerId())
                        + ") to participatingRegions ["        + trRegion.getRegionInfo().getRegionNameAsString()
                        + "], startKey: "      + Hex.encodeHexString(trRegion.getRegionInfo().getStartKey())
                        + ", endKey: "        + Hex.encodeHexString(trRegion.getRegionInfo().getEndKey())
                        + " and transaction [" + transactionId + "]");
        }
        else {
           if (LOG.isDebugEnabled()) LOG.debug("trRegion already present in (" + participatingRegions.regionCount(trRegion.getPeerId())
                       + ") participatinRegions ["    + trRegion.getRegionInfo().getRegionNameAsString()
                       + "], startKey: "      + Hex.encodeHexString(trRegion.getRegionInfo().getStartKey())
                       + ", endKey: "        + Hex.encodeHexString(trRegion.getRegionInfo().getEndKey())
                       + " and transaction [" + transactionId + "]");
        }

        return added;
    }

    public HashMap<String, Integer> getTableAttr() {
        return tableAttr;
    }

    public TrafodionLocationList getParticipatingRegions() {
        return participatingRegions;
    }

    public void clearParticipatingRegions() {
        participatingRegions.clear();
    }

    public void renewParticipatingRegions() throws IOException {
        participatingRegions = new TrafodionLocationList();
    }

    public void clearRetryRegions() {
        retryRegions.clear();
    }

    public int getRegionsToIgnore() {
       return regionsToIgnore.get();
    }

    void addRegionToIgnore(final TransactionRegionLocation region) {
        // Changing to an HRegionLocation for now
        if (LOG.isTraceEnabled()) LOG.trace("addRegionToIgnore region " + region
           + " added to to transaction " + this.getTransactionId());
        regionsToIgnore.getAndIncrement();
        region.setReadOnly(true);
        if (LOG.isTraceEnabled()) LOG.trace("addRegionToIgnore region added.  Current size: "
             + regionsToIgnore.get());
    }

    Set<TransactionRegionLocation> getRetryRegions() {
        return retryRegions;
    }

    void addRegionToRetry(final TransactionRegionLocation region) {
        // Changing to an HRegionLocation for now
        retryRegions.add(region);
    }

   //only update the IB attribute
   public void tableIBAttrSetAndRegister(final String tableName, int flags) {

        Integer attr = null;
        Integer newAttr = null;

        synchronized (tableAttrLock) {
             if ( (attr = tableAttr.get(tableName)) != null) { // table has been registered with TM
                 if (((attr.intValue() & INCREMENTALBR) != INCREMENTALBR)
                               && ((flags & INCREMENTALBR) == INCREMENTALBR))  { // table has been registered without IBR attrib
                      newAttr = new Integer(attr.intValue() | INCREMENTALBR);
                      tableAttr.replace(tableName, newAttr);
                 }
             }
        } // synchronnized
   }

   public int tableAttrSetAndRegister(final String tableName, int flags) {

        Integer attr = null;
        Integer newAttr = null;
        int reRegister = 1;

        synchronized (tableAttrLock) {

             if ( (attr = tableAttr.get(tableName)) != null) { // table has been registered with TM
                 reRegister = 0; // table attr no changed
                 if (((attr.intValue() & INCREMENTALBR) != INCREMENTALBR)
                               && ((flags & INCREMENTALBR) == INCREMENTALBR))  { // table has been registered without IBR attribute
                      newAttr = new Integer(attr.intValue() | INCREMENTALBR);
                      tableAttr.replace(tableName, newAttr);
                      reRegister = 2; // table attr changed and need re-register with TM
                 }
                 if (((attr.intValue() & SYNCHRONIZED) != SYNCHRONIZED)
                               && ((flags & SYNCHRONIZED) == SYNCHRONIZED))  { // table has been registered without SYNCHRONOUS attribute
                      newAttr = new Integer(attr.intValue() | SYNCHRONIZED);
                      tableAttr.replace(tableName, newAttr);
                      reRegister = 2; // table attr changed and need re-register with TM
                 }
                 if (((attr.intValue() & TIMELINE) != TIMELINE)
                               && ((flags & TIMELINE) == TIMELINE))  { // table has been registered without TIMELINE attribute
                      newAttr = new Integer(attr.intValue() | TIMELINE);
                      tableAttr.replace(tableName, newAttr);
                      reRegister = 2; // table attr changed and need re-register with TM
                 }
                 if (((attr.intValue() & WAL_SYNC_OK) != WAL_SYNC_OK)
                               && ((flags & WAL_SYNC_OK) == WAL_SYNC_OK))  {
                      newAttr = new Integer(attr.intValue() | WAL_SYNC_OK);
                      tableAttr.replace(tableName, newAttr);
                      reRegister = 2;
                 }
                 if (LOG.isDebugEnabled()) LOG.debug("AttrSet1 " + "table " + tableName +
                          " attr " + tableAttr.get(tableName).intValue()+ " " + attr.intValue() + " flag " + flags + " action " + reRegister);
             }
             else { // create new entry for this table
                  attr = new Integer(flags);
                  tableAttr.put(tableName, attr);
                  if (LOG.isDebugEnabled()) LOG.debug("AttrSet2 " + "table " + tableName
                          + " attr " + attr.intValue() + " flag " + flags + " action " + reRegister);
                  reRegister = 1; // table attr changed but no need to register with TM
             }
        } // synchronnized

        return reRegister;
    }

    public int getTmTableAttr(final String tableName) {

        Integer attr = null;
        int value = 0; // if not found, returns 0
        synchronized (tableAttrLock) {
            if ( (attr = tableAttr.get(tableName)) != null)
                value = attr.intValue();
        } // synchronnized
        return value;
     }

    public boolean getLocalWriteFailed() {
        return this.localWriteFailed;
    }

    public void setLocalWriteFailed(final boolean value) {
        this.localWriteFailed = value;
    }

    public boolean getRemoteWriteFailed() {
        return this.remoteWriteFailed;
    }

    public void setRemoteWriteFailed(final boolean value) {
        this.remoteWriteFailed = value;
    }

    public boolean getSkipConflictCheck() {
        return this.skipConflictCheck;
    }

    public boolean getSkipSdnCDC() {
        return this.skipSdnCDC;
    }

    public void setSkipConflictCheck(final boolean value) {
        this.skipConflictCheck = value;
    }

    public boolean getSkipRemoteCheck() {
        return this.skipRemoteCheck;
    }

    public void setMustBroadcast(final boolean value) {
        this.mustBroadcast = value;
    }

    public boolean getMustBroadcast() {
        return this.mustBroadcast;
    }

    public String getQueryContext() {
        return queryContext;
    }

    public void setQueryContext(String value) {
        queryContext = value;
    }

    public void setWriteTlogOnly(final boolean value) {
        this.writeTlogOnly = value;
    }

    public boolean getWriteTlogOnly() {
        return this.writeTlogOnly;
    }

    public void setSkipRemoteCheck(final boolean value) {
        this.skipRemoteCheck = value;
    }

    public void setGenerateMutations(final boolean value) {
        this.generateMutations = value;
    }

    public boolean getGenerateMutations() {
        return this.generateMutations;
    }

    public void setSkipSdnCDC(final boolean value) {
        this.skipSdnCDC = value;
    }

    /**
     * Get the transactionId.
     *
     * @return Return the transactionId.
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Get the sequenceNum portion of the transactionId.
     *
     * @return Return the sequenceNum.
     */
    public long getTransSeqNum() {

       return transactionId & 0xFFFFFFFFL;
    }

    /**
     * Get the sequenceNum portion of the passed in transId.
     *
     * @return Return the sequenceNum.
     */
    public static long getTransSeqNum(long transId) {

       return transId & 0xFFFFFFFFL;
    }

    /**
     * Get the originating node of the transaction.
     *
     * @return Return the nodeId.
     */
    public int getNodeId() {
       return nodeId;
    }

    /**
     * Set the originating node of the transaction.
     *
     */
    private void setNodeId() {

       nodeId = (int) ((transactionId >> 32) & 0xFFL);
    }

    /**
     * Get the originating node of the passed in transaction.
     *
     * @return Return the nodeId.
     */
    public static int getNodeId(long transId) {

        return (int) ((transId >> 32) & 0xFFL);
    }

    /**
     * Get the originating clusterId of the transaction.
     *
     * @return Return the clusterId.
     */
    public long getClusterId() {

        return clusterId;

    }

    /**
     * Get the originating clusterId of the passed in transaction.
     *
     * @return Return the clusterId.
     */
    public static long getClusterId(long transId) {

        return ((transId >> 48) & 0xFFL);
    }

    /**
     * Set the originating clusterId of the passed in transaction.
     *
     */
    private void setClusterId() {

        clusterId = ((transactionId >> 48) & 0xFFL);
    }

    /**
     * Get the originating instanceId of the transaction.
     *
     * @return Return the instanceId.
     */
    public long getInstanceId() {

       return instanceId;
    }

    /**
     * Get the originating instanceId of the passed in transaction.
     *
     * @return Return the instanceId.
     */
    public static long getInstanceId(long transId) {

       return (transId >> 56);
    }

    /**
     * Set the originating instanceId of the passed in transaction.
     *
     */
    private void setInstanceId() {

       instanceId = (transactionId >> 56);
    }

    /**
     * Set the startEpoc.
     *
     */
    public void setStartEpoch(final long epoch) {
        this.startEpoch = epoch;
    }

    /**
     * Get the startEpoch.
     *
     * @return Return the startEpoch.
     */
    public long getStartEpoch() {
        return startEpoch;
    }

    /**
     * Set the startId.
     *
     */
    public void setStartId(final long id) {
        this.startId = id;
    }

    /**
     * Get the startId.
     *
     * @return Return the startId.
     */
    public long getStartId() {
        return startId;
    }

    /**
     * Set the commitId.
     *
     */
    public void setCommitId(final long id) {
        this.commitId = id;
    }

    /**
     * Get the commitId.
     *
     * @return Return the commitId.
     */
    public long getCommitId() {
        return commitId;
    }

    /**
     * Set the recoveryASN.
     *
     */
    public void setRecoveryASN(final long value) {
        this.recoveryASN = value;
    }

    /**
     * Get the recoveryASN.
     *
     * @return Return the recoveryASN.
     */
    public long getRecoveryASN() {
        return recoveryASN;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "transactionId: " + transactionId + ", nodeId: " + getNodeId() + ", clusterId: " + getClusterId()
               + ", instanceId: " + getInstanceId() + ", seqNum " + getTransSeqNum() + ", startId: " + startId + ", commitId: " + commitId
               + ", startEpoch: " + startEpoch + " (" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(startEpoch)) + "), skipConflictCheck " + getSkipConflictCheck()
               + ", skipSdnCDC " + getSkipSdnCDC() + ", skipRemoteCheck " + getSkipRemoteCheck()
               + ", XdcTransType " + xdcTransType + ", participants: " + getParticipantCount()
               + ", ignoring: " + regionsToIgnore.get() + ", hasDDL: " + hasDDLTx()
               + ", recoveryASN: " + getRecoveryASN() + ", remotePeers: " + hasRemotePeers()
               + ", state: " + status.toString() + " localWriteFailed: " + getLocalWriteFailed()
               + ", remoteWriteFailed: " + getRemoteWriteFailed() + ", tmFlags: " + getTmFlags()
               + ", requestPendingCount: " + requestPendingCount + ", requestReceivedCount: " + requestReceivedCount
               + ", commitSendDone: " + commitSendDone + ", mustBroadcast: " + mustBroadcast
               + ", prepareRefresh: " + prepareRefresh;
    }

    public String displayCompleteString() {
       StringBuilder builder = new StringBuilder();
       builder.append(", Table attr list: ");
       for (Map.Entry<String, Integer> entry : getTableAttr().entrySet()){
          builder.append(entry.getKey() + ":" + entry.getValue() + ", ");
       }
       return this.toString() + builder.toString();
    }

    public int getTmFlags() {
       int Flags = 0;
       if (getSkipRemoteCheck()){
          Flags |= SKIP_REMOTE_CK;
       }
       if (getSkipConflictCheck()){
          Flags |= SKIP_CONFLICT;
       }
       if (getSkipSdnCDC()){
          Flags |= SKIP_SDN_CDC;
       }
       if (getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN){
          Flags |= XDC_DOWN;
       }
       return Flags;
    }

    public void setTmFlags(int lvFlags) {

       setSkipRemoteCheck((lvFlags & SKIP_REMOTE_CK) == SKIP_REMOTE_CK);
       setSkipConflictCheck((lvFlags & SKIP_CONFLICT) == SKIP_CONFLICT);
       setSkipSdnCDC((lvFlags & SKIP_SDN_CDC) == SKIP_SDN_CDC);
       if((lvFlags & XDC_DOWN) == XDC_DOWN){
          setXdcType(XdcTransType.XDC_TYPE_XDC_DOWN);
       }
    }

    public boolean getExceptionLogged() {
        return exceptionLogged;
    }

    public int getParticipantCount() {
        return participatingRegions.regionCount();
    }

    public int getRegionsRetryCount() {
        return retryRegions.size();
    }

    public String getXdcTypeAsString() {
      return xdcTransType.toString();
    }

    public XdcTransType getXdcType() {
      return xdcTransType;
    }

    public void setXdcType(final XdcTransType type) {
      this.xdcTransType = type;
    }
    
    public void set_begin_time(long ts) {
        this.begin_Time = ts;
    }
    
    public long get_begin_time() {
        return this.begin_Time;
    }
    
    public void set_prepareCommit_time(long ts) {
        this.prepareCommit_Time = ts;
    }
    
    public long get_prepareCommit_time() {
        return this.prepareCommit_Time;
    }
    
    public void set_doCommit_time(long ts) {
        this.doCommit_Time = ts;
    }
    
    public long get_doCommit_time() {
        return this.doCommit_Time;
    } 
    
    public void set_abortTransaction_time(long ts) {
        this.abortTransaction_Time = ts;
    }
    
    public long get_abortTransaction_time() {
        return this.abortTransaction_Time;
    }     

    public void set_completeRequest_time(long ts) {
        this.completeRequest_Time = ts;
    }
    
    public long get_completeRequest_time() {
        return this.completeRequest_Time;
    } 
    
    public String getStatusString() {
      return status.toString();
    }

    public TransState getStatus() {
       return status;
    }

    public void setStatus(final TransState status) {
      this.status = status;
    }

    public boolean hasDDLTx() {
        return ddlTrans;   
    }

    public void setDDLTx(final boolean status) {
        this.ddlTrans = status;
    }

    public void addDDLNum() {
        this.ddlNum++;
    }

    public void resetDDLNum() {
        this.ddlNum = 0;
    }

    public void setDDLNum(int d) {
        this.ddlNum = d;
    }

    public int getDDLNum() {
        return this.ddlNum;
    }

    public void setRetried(boolean val) {
      this.hasRetried = val;
    }

    public boolean hasRetried() {
      return this.hasRetried;
    }

    public void setPrepareRefresh(boolean val) {
      this.prepareRefresh = val;
    }

    public boolean getPrepareRefresh() {
      return this.prepareRefresh;
    }

    public void setHasRemotePeers(boolean pv_HasRemotePeers) {
       this.m_HasRemotePeers = pv_HasRemotePeers;
    }

    public boolean hasRemotePeers() {
       return this.m_HasRemotePeers;
    }

    public synchronized void logExceptionDetails(int peerId, final boolean ute, final int participant)
    {
       if (exceptionLogged)
          return;
       int participantNum = 0;
       byte[] startKey;
       byte[] endKey_orig;
       byte[] endKey;
       boolean isIgnoredRegion = false;
       
       TransactionRegionLocation errLocation = null;

       LOG.error("Starting " + (ute == true ? "UTE " : "NPTE ") + "for trans: " + this.toString());
         for (HashMap<ByteArrayKey,TransactionRegionLocation> tableMap : 
                                    getParticipatingRegions().getList(peerId).values()) {
           for (TransactionRegionLocation location : tableMap.values()) {
             LOG.error("ParticipatingRegions has " + getParticipatingRegions().tableCount(peerId)
               + getParticipatingRegions().regionCount(peerId) + " regions.  Regions to ignore " + regionsToIgnore.get());
             isIgnoredRegion = location.getReadOnly();
             if (isIgnoredRegion) {
                LOG.error("ignoring read-only location " + location);
             }
             else{
                LOG.error("location not read-only " + location);
                participantNum++;
             }
             startKey = location.getRegionInfo().getStartKey();
             endKey_orig = location.getRegionInfo().getEndKey();
             if (endKey_orig == null || endKey_orig == HConstants.EMPTY_END_ROW)
                endKey = null;
             else
                endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);


          startKey = location.getRegionInfo().getStartKey();
          endKey_orig = location.getRegionInfo().getEndKey();
          if (endKey_orig == null || endKey_orig == HConstants.EMPTY_END_ROW)
              endKey = null;
          else
              endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);
             LOG.error((ute == true ? "UTE " : "NPTE ") + "for transId: " + getTransactionId()
                    + " participantNum " + (isIgnoredRegion ? "Ignored region " : participantNum)
                    + " table " + location.getRegionInfo().getTable().getNameAsString()
                    + " table length " + location.getRegionInfo().getTable().getNameAsString().length()
                    + " peerId " + location.getPeerId()
                    + " startKey " + ((startKey != null)?
                             (Bytes.equals(startKey, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startKey)) : "NULL")
                    + " startKey length " + startKey.length
                    + " endKey " +  ((endKey != null) ?
                             (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey)) : "NULL")
                    + " endKey length " + endKey.length
                    + " RegionEndKey " + ((endKey_orig != null) ?
                             (Bytes.equals(endKey_orig, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endKey_orig)) : "NULL")
                    + (errLocation ==  null ? " errLocation is null " :
                        " comparison of errLocation to this is " + errLocation.compareTo(location)
                        + " errLocation hostname: " + errLocation.getHostname()
                        + " location hostname: " + location.getHostname()));

          }
       }
       exceptionLogged = true;
    }

    public synchronized void setCancelOperation(boolean canceled) {
        this.canceled = canceled;
    }

    public synchronized boolean getCancelOperation() {
        return this.canceled;
    }

    public short getTotalNum() {
        if(totalNum >= 0 ) return totalNum;
        return (short)participatingRegions.getTotalNum();
    }

    public void setTotalNum(short tn) 
    {
        totalNum = tn;
    }

    public int addIntoTotalNumMap(String who, int flag) {
     synchronized(tnHashMapLock) {
       if(totalNumRespMap.containsKey(who) ) // duplicated
       {
         if (LOG.isWarnEnabled()) {
             LOG.warn("HBaseBinlog: particate " + who + " replied duplicated with " + flag + " previous flag is " + totalNumRespMap.get(who));
         }
         //overwrite flag, since we believe it is idompodent
         if(flag == TransactionalReturn.COMMIT_OK || flag == TransactionalReturn.COMMIT_OK_READ_ONLY )
           totalNumRespMap.put(who, flag);
       }
       else
       {
         if(flag == TransactionalReturn.COMMIT_OK || flag == TransactionalReturn.COMMIT_OK_READ_ONLY )
           totalNumRespMap.put(who, flag);
       }
     }
       return 0; //success
    }

    public ConcurrentHashMap<String, Integer> getTotalNumRespMap() {
      return totalNumRespMap;
    }



    public ConcurrentHashMap<TransactionRegionLocation, Long> getFlushCheckRespMap() {
      return flushCheckRespMap;
    }
    
    public int addIntoFlushCheckRespMap(TransactionRegionLocation who, long wid) {
     synchronized (flushCheckMapLock) {
       if(flushCheckRespMap.containsKey(who) ) // duplicated
       {
           if (LOG.isWarnEnabled()) {
               LOG.warn("HBaseBinlog: particate " + who + " replied duplicated with " + wid + " previous wid is " + flushCheckRespMap.get(who));
           }
       }
       else
       {
           flushCheckRespMap.put(who, wid);
       }
     }
     return 0; //success
    }

    public int removeFromFlushCheckRespMap(TransactionRegionLocation who) {
      if(flushCheckRespMap.containsKey(who) ) 
        flushCheckRespMap.remove(who);
      return 0;
    }

    public void setFlushCheckStatus(int v) { flushCheckStatus = v; }
    public int getFlushCheckStatus(int v) { return flushCheckStatus ; }
    public void setFLushCheckFail()  { flushCheckStatus = 1; }
    public boolean isFlushCheckFail() { 
      if(flushCheckStatus == 1) return true; 
      else return false;
    }

    public boolean isReadOnlyLocation(TransactionRegionLocation trl) {
      return participatingRegions.isReadOnlyLocation(trl);
    }
}
