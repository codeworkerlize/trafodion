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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.client.transactional.SsccTransactionalTable;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.NotServingRegionException;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.pit.ReplayEngine;

public class RMInterface {
    static final Log LOG = LogFactory.getLog(RMInterface.class);
    static Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
    static final Object mapLock = new Object();

    public AlgorithmType TRANSACTION_ALGORITHM;
    static Map<Long, Set<RMInterface>> mapRMsPerTransaction = new HashMap<Long,  Set<RMInterface>>();
    private TransactionalTable ttable = null;
    private String pTableName;
    private boolean bSynchronized=false;
    private boolean bIncrementalBR=false;
    private int rmiConsistency=0; // 101 has timeline, 102 has IUD, not allowed to proceed if timeline is set, or vice versa
    private boolean asyncCalls = false;
    private boolean bRegisterRegionsAtTransEnd;
    private ExecutorService threadPool;
    private CompletionService<Integer> compPool;
    private int intThreads = 16;
    protected Map<Integer, TransactionalTable> peer_tables;
    private Connection connection;
    static TransactionManager txm;
    protected static boolean recoveryToPitMode;
    private static boolean envBroadcastMode;
    private static boolean envRowsetParallelMode;
    private static boolean envAsyncCalls;
    private static boolean envIBRNonTxn;
    private static boolean envTmRedoRegister;
    private static boolean envTmBypassFirstRead = false;
    private static List<String> createdTables;
    private static AtomicLong iBRTransactionId = new AtomicLong(0);
    private static Object peerReconnectLock = new Object();

    static {
        System.loadLibrary("stmlib");
        String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
        if (usePIT != null)
           recoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
        else
           recoveryToPitMode = false;
        String envset = System.getenv("TM_USE_SSCC");
        if (envset != null)
           envTransactionAlgorithm = (Integer.parseInt(envset) == 1) ? AlgorithmType.SSCC : AlgorithmType.MVCC;
        else
           envTransactionAlgorithm = AlgorithmType.MVCC;
        String asyncSet = System.getenv("TM_ASYNC_RMI");
        if (asyncSet != null) 
           envAsyncCalls = (Integer.parseInt(asyncSet) == 1) ? true : false;
        else
           envAsyncCalls = false;

        String useBroadcastMode = System.getenv("TM_USE_BROADCAST_MODE");
        if (useBroadcastMode != null)
          envBroadcastMode = (Integer.parseInt(useBroadcastMode) == 1) ? true : false;

        String useRowsetParallelMode = System.getenv("TM_USE_ROWSET_PARALLEL");
        if (useRowsetParallelMode != null)
          envRowsetParallelMode = (Integer.parseInt(useRowsetParallelMode) == 1) ? true : false;
        else 
          envRowsetParallelMode = false;

        String useIBRNonTxn = System.getenv("TM_USE_IBR_NONTXN");
        if (useIBRNonTxn != null)
          envIBRNonTxn = (Integer.parseInt(useRowsetParallelMode) == 1) ? true : false;
        else 
          envIBRNonTxn = true; // default is to generate CDC for tables with incremental backup attribute

        iBRTransactionId.set(2001);

        String tmRedoRegister = System.getenv("TM_REDO_REGISTER_FOR_WRITE");
        if (tmRedoRegister != null)
          envTmRedoRegister = (Integer.parseInt(tmRedoRegister) == 1) ? true : false;
        else 
          envTmRedoRegister = true; // default is to redo register for write operation 

        String tmBypassReadRegister= System.getenv("TM_BYPASS_FIRST_READ");
        if (tmBypassReadRegister != null)
          envTmBypassFirstRead= (Integer.parseInt(tmBypassReadRegister) == 1) ? true : false;

        createdTables = new ArrayList<String>();
    } // static

    protected static STRConfig pSTRConfig = null;

    private native String createTableReq(byte[] lv_byte_htabledesc, byte[][] keys, int numSplits, int keyLength, long transID, byte[] tblName, int options);
    private native String dropTableReq(byte[] lv_byte_tblname, long transID);
    private native String truncateOnAbortReq(byte[] lv_byte_tblName, long transID); 
    private native String alterTableReq(byte[] lv_byte_tblname, Object[] tableOptions, long transID);
    private native String pushEpochReq(byte[] lv_byte_htabledesc, long transID, byte[] tblName);

    public static void main(String[] args) {
      System.out.println("MAIN ENTRY");
    }

    private Connection getConnection() throws IOException
    {
       if (connection.isClosed())
          throw new IOException("HBase connection is stale");
       return connection;     
    }

    protected static IdTm idServer;
    protected static int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec 
    String idtmTimeout = System.getenv("TM_IDTM_TIMEOUT");

    public enum AlgorithmType {
       MVCC, SSCC
    }

    protected static AlgorithmType envTransactionAlgorithm;
    private AlgorithmType transactionAlgorithm;

    // XDC transaction types are defined in XdcTransType.java
    public static final int XDC_NONE        = 0;   // 00001
    public static final int XDC_UP          = 1;   // 00001
    public static final int XDC_DOWN        = 2;   // 00010

    public static final int SYNCHRONIZED    = 4;   // 00100
    public static final int SKIP_CONFLICT   = 8;   // 01000
    public static final int SKIP_REMOTE_CK  = 16;  // 10000
    public static final int INCREMENTALBR  = 32;  // 100000
    public static final int TABLE_ATTR_SET  = 64;  // 1000000
    public static final int SKIP_SDN_CDC  = 128;  // 10000000
    public static final int PIT_ALL  = 256;       // 100000000
    public static final int TIMELINE  = 512;      // 1000000000    
    public static final int ADD_TOTALNUM = 1024;  // 10000000000    

    public static final int ACTION_NONE          = 0;
    public static final int ACTION_ATTR_SET          = 1;
    public static final int ACTION_ATTR_SET_SEND          = 2;

    public static final int TM_SLEEP = 1000;      // One second
    public static final int TM_SLEEP_INCR = 5000; // Five seconds
    public static final int TM_RETRY_ATTEMPTS = 5;
    
    public static final int HAS_TIMELINE_ACCESS = 101;
    public static final int HAS_UPDATE = 102; 

    public RMInterface(final String tableName, Connection connection, boolean pb_synchronized) throws IOException {

        this.RMInterface2(tableName, connection, pb_synchronized, false); 

    }

    public RMInterface(final String tableName, Connection connection, boolean pb_synchronized, 
                                              boolean pb_incrementalBR) throws IOException {

        this.RMInterface2(tableName, connection, pb_synchronized, pb_incrementalBR); 

    }

    public void RMInterface2(final String tableName, Connection connection, boolean pb_synchronized, 
                                              boolean pb_incrementalBR) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor:"
					    + " tableName: " + tableName
					    + " synchronized: " + pb_synchronized + " incrementalBR " + pb_incrementalBR );
        if (pSTRConfig == null)
           pSTRConfig = STRConfig.getInstance(connection.getConfiguration());

        if (idtmTimeout != null){
           ID_TM_SERVER_TIMEOUT = Integer.parseInt(idtmTimeout.trim());
        }

        pTableName = tableName;
        bSynchronized = pb_synchronized;
        bIncrementalBR = pb_incrementalBR;
        bRegisterRegionsAtTransEnd = true; 
        if (LOG.isTraceEnabled()) 
              LOG.trace("Incremental BR set in RMinterface constructor for table " + tableName
                                       + " IBR " + pb_incrementalBR + " envForNonTxnCDC " + envIBRNonTxn);

        this.connection = connection;
        transactionAlgorithm = envTransactionAlgorithm;
        if( transactionAlgorithm == AlgorithmType.MVCC) //MVCC
        {
           if (LOG.isTraceEnabled()) LOG.trace("Algorithm type: MVCC"
						+ " tableName: " + tableName
						+ " configured peerCount: " + pSTRConfig.getConfiguredPeerCount());
           ttable = new TransactionalTable(Bytes.toBytes(tableName), connection);
        }
        else if(transactionAlgorithm == AlgorithmType.SSCC)
        {
           ttable = new SsccTransactionalTable( Bytes.toBytes(tableName), connection);
        }

        setSynchronized(bSynchronized);

        // if bSynchronized (sync table for XDC, then set rowset parallel as default
        if (bSynchronized) envRowsetParallelMode = true;
        if (LOG.isTraceEnabled()) LOG.trace("Rowset put parallel mode set " + envRowsetParallelMode);

        idServer = new IdTm(false);

        asyncCalls = envAsyncCalls;
        if (asyncCalls) {
          if (LOG.isTraceEnabled()) LOG.trace("Asynchronous RMInterface calls set");
          threadPool = Executors.newFixedThreadPool(intThreads);
          compPool = new ExecutorCompletionService<Integer>(threadPool);
        }
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor exit");
    }

    public RMInterface(Connection connection) throws IOException {
       this.connection = connection;
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

    public boolean isPeerConnectionUp(int pv_peer_id) throws IOException
    {

      synchronized(peerReconnectLock) {
          if (! isSTRUp(pv_peer_id)) {
               if (LOG.isWarnEnabled()) LOG.warn("XDC: peer " + pv_peer_id + " is down (SDN) when transaction " +
                                         " intend to access table " + this.pTableName);
               return false;
          }
          if (pSTRConfig.getPeerConnectionState() == STRConfig.PEER_CONNECTION_DOWN) { // there is a SUP trans but peer has not added connection yet
               if (LOG.isDebugEnabled()) LOG.debug("isPeerConnectionUp,"
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

    public int broadcastRequest(final int requestType) throws IOException {
       if (LOG.isDebugEnabled()) LOG.debug("broadcastRequest start; requestType: " + requestType + " no boolean");
       return broadcastRequest(requestType, false);
    }

    public int broadcastRequest(final int requestType, final boolean requestBool) throws IOException {
       if (LOG.isDebugEnabled()) LOG.debug("broadcastRequest start; requestType: " + requestType + " requestBool: " + requestBool);
       int returnCode = ttable.broadcastRequest(requestType, requestBool);

       if (LOG.isDebugEnabled()) LOG.debug("broadcastRequest end; result: " + returnCode);
       return returnCode;
    }

    public void setSynchronized(boolean pv_synchronize) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface setSynchronized:"
					    + " table: " + new String(ttable.getTableName())
					    + " configured peerCount: " + pSTRConfig.getConfiguredPeerCount()
					    + " synchronize flag: " + pv_synchronize
					    );
	
	bSynchronized = pv_synchronize;

	if (bSynchronized && 
	    (peer_tables == null) && 
	    (pSTRConfig.getConfiguredPeerCount() > 0)) {
	    if (LOG.isTraceEnabled()) LOG.trace(" configured peerCount: " + pSTRConfig.getConfiguredPeerCount());
	    if( transactionAlgorithm == AlgorithmType.MVCC) {
		peer_tables = new HashMap<Integer, TransactionalTable>();
		for ( Map.Entry<Integer, Connection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) continue;
		    if (! isSTRUp(lv_peerId)) {
			if (LOG.isTraceEnabled()) LOG.trace("setSynchronized, STR is DOWN for "
							    + " peerId: " + lv_peerId + " but still add peer content into peer_tables ");
//			continue;
		    }

		    if (LOG.isTraceEnabled()) LOG.trace("setSynchronized" 
							+ " peerId: " + lv_peerId);
		    peer_tables.put(lv_peerId, new TransactionalTable(ttable.getTableName(), e.getValue()));
		}
	    }
	    else if(transactionAlgorithm == AlgorithmType.SSCC) {
		peer_tables = new HashMap<Integer, TransactionalTable>();
		for ( Map.Entry<Integer, Connection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) 
			continue;
//		    if (! isSTRUp(lv_peerId)) 
//			continue;
		    peer_tables.put(lv_peerId, new SsccTransactionalTable(ttable.getTableName(), e.getValue()));
		}
	    }
	}
    }

    public boolean isSynchronized() {
       return bSynchronized;
    }

    private abstract class RMCallable implements Callable<Integer>{
      TransactionalTable tableClient;
      TransactionState transactionState;
      long savepointId;
      long pSavepointId;
      String queryContext;

      RMCallable(TransactionalTable tableClient,
                 TransactionState txState,
                 long savepointId,
                 long pSavepointId,
                 String queryContext) {
        this.tableClient = tableClient;
        this.transactionState = txState;
        this.savepointId = savepointId;
        this.pSavepointId = pSavepointId;
        this.queryContext = queryContext;
      }

      public Integer checkAndDeleteX(
          final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
          final Delete delete, final boolean skipCheck) throws IOException {
          boolean returnCode = tableClient.checkAndDelete(transactionState,
                                                          savepointId,
                                                          pSavepointId,
                                                          row,
                                                          family,
                                                          qualifier,
                                                          value,
                                                          delete,
                                                          skipCheck, queryContext);
          return returnCode ? new Integer(1) : new Integer(0);
      }

      public Integer checkAndPutX(
          final byte[] row, final byte[] family, final byte[] qualifier,
          final byte[] value, final Put put, final boolean skipCheck, final int nodeId) throws IOException {
          boolean returnCode = tableClient.checkAndPut(transactionState,
                                                       savepointId,
                                                       pSavepointId,
                                                       row,
                                                       family,
                                                       qualifier,
                                                       value,
                                                       put,
                                                       skipCheck, nodeId, queryContext);
        return returnCode ? new Integer(1) : new Integer(0);
      }

      public Integer deleteX(final Delete delete,
                             final boolean bool_addLocation) throws IOException
      {
        tableClient.delete(transactionState, savepointId, pSavepointId, delete, bool_addLocation, false, queryContext);
        return new Integer(0);
      }

      public Integer deleteX(List<Delete> deletes) throws IOException{
        tableClient.delete(transactionState, savepointId, pSavepointId, deletes, false, queryContext);
        return new Integer(0);
      }

      public Integer putX(final Put put,
                          final boolean bool_addLocation) throws IOException {
        tableClient.put(transactionState, savepointId, pSavepointId, put, bool_addLocation, false, queryContext);
        return new Integer(0);
      }

      public Integer putX(final List<Put> puts) throws IOException {
        tableClient.put(transactionState, savepointId, pSavepointId, puts, false, queryContext);
        return new Integer(0);
      }
    }

    public TransactionState registerTransaction(final TransactionalTable pv_table, 
							     final long transactionID, 
							     final byte[] startRow,
							     final int pv_peerId,
							     final boolean pv_skipConflictCheck,
							     int pv_tmFlags) throws IOException 
    {
       return registerTransaction (pv_table, transactionID, startRow, null, pv_peerId,
                                 pv_skipConflictCheck, pv_tmFlags, false );
   
    }
    public TransactionState registerTransaction(final TransactionalTable pv_table, 
							     final long transactionID, 
							     final byte[] startRow,
							     final byte[] endRow,			
							     final int pv_peerId,
							     final boolean pv_skipConflictCheck,
							     int pv_tmFlags) throws IOException 
    {
       return registerTransaction (pv_table, transactionID, startRow, endRow, pv_peerId,
                                 pv_skipConflictCheck, pv_tmFlags, true );
    }

    public TransactionState registerTransaction(final TransactionalTable pv_table, 
							     final long transactionID, 
							     final byte[] startRow,
							     final byte[] endRow,			
							     final int pv_peerId,
							     final boolean pv_skipConflictCheck,
							     int pv_tmFlags, boolean scanRange) throws IOException 
    {
        if (LOG.isDebugEnabled()) LOG.debug("Enter registerTransaction, table: " + getTableNameAsString() + " transaction ID: " + transactionID
 //            + " startRow " + Hex.encodeHexString(startRow)
 //            + " endRow " + Hex.encodeHexString(endRow)
           + " startRow " + ((startRow != null) ?
                                (Bytes.equals(startRow, HConstants.EMPTY_START_ROW) ? "INFINITE" : Hex.encodeHexString(startRow)) : "NULL")
                        + " endRow " +  ((endRow != null) ?
                                (Bytes.equals(endRow, HConstants.EMPTY_END_ROW) ? "INFINITE" : Hex.encodeHexString(endRow)) : "NULL")
             + " skipConflictCheck " + pv_skipConflictCheck
             + " table " + pTableName + " tm_flags " + pv_tmFlags + " peerId: " + pv_peerId);
        boolean register = false;
        boolean registerTM = false;
        
        TransactionState ts = mapTransactionStates.get(transactionID);

        if (LOG.isTraceEnabled()) 
           LOG.trace("mapTransactionStates " + mapTransactionStates + " entries " + mapTransactionStates.size());

        // if we don't have a TransactionState for this ID we need to register it with the TM
        if (ts == null) { 
           ts = TransactionState.getInstance(transactionID);
           if (pSTRConfig.getConfiguredPeerCount() > 0){
              int lv_first_remote_peer_id = pSTRConfig.getFirstRemotePeerId();
              if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction,"
						  + " PeerCount " + pSTRConfig.getConfiguredPeerCount()
						  + " lv_first_remote_peer_id: " + lv_first_remote_peer_id
						  );
              // use ts.getXdcType (set in getInstance of ts, to avoid race)
              //if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id).contains(PeerInfo.STR_UP)) {
              if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP) {
                 pv_tmFlags |= XDC_UP;
              }
              //else if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id).contains(PeerInfo.STR_DOWN)){
              else if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_DOWN) {
                 pv_tmFlags |= XDC_DOWN;
              }
              else {
                 if (LOG.isDebugEnabled()) LOG.debug("HAX - XDC None, RMInterface:registerTransaction, XDC status of first peerid: " 
						     + pSTRConfig.getPeerStatus(lv_first_remote_peer_id));
              }

              if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id, true).contains(PeerInfo.PEER_ATTRIBUTE_NO_CHECK)) {
                 pv_tmFlags |= SKIP_REMOTE_CK;
              }

              if (pSTRConfig.getPeerStatus(lv_first_remote_peer_id, true).contains(PeerInfo.PEER_ATTRIBUTE_NO_CDC_XDC)) {
                 pv_tmFlags |= SKIP_SDN_CDC;
              }

           }
           if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction - new ts created: " + ts);
        }

        if (bSynchronized) {
           pv_tmFlags |= SYNCHRONIZED;
        }

        if (bIncrementalBR) {
           pv_tmFlags |= INCREMENTALBR;
           if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, set tmFlags with IBR " + transactionID);
        }
        
        if (rmiConsistency == HAS_TIMELINE_ACCESS) { // get or getScanner will set this before call registerTransaction
           pv_tmFlags |= TIMELINE;
        } 

        if (LOG.isDebugEnabled()) LOG.debug("RMInterface:registerTransaction,"
					    + " ts: " + ts
					    + " tmFlags: " + pv_tmFlags
					    + " table " + pTableName
					    );

        HRegionLocation locationRow;
        boolean refresh = false;
        if (createdTables.size() > 0 && createdTables.contains(Bytes.toString(pv_table.getTableName()))){
            if (LOG.isTraceEnabled()) LOG.trace("Locations being refreshed : "
                + Bytes.toString(pv_table.getTableName()) + " for transaction " + transactionID
                + "  CreatedTables size " + createdTables.size());
            refresh = true;
            createdTables.remove(Bytes.toString(pv_table.getTableName()));
        }

        HRegionLocation location = null;
        List<HRegionLocation> scanRegionLocations = null;
        Iterator<HRegionLocation> scanRegionLocationsIter = null;
        
        // set keys to infinite
        if ((envBroadcastMode == true) && (pv_skipConflictCheck == true))
        { 
            locationRow = pv_table.getRegionLocation(startRow, refresh);
            if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction - setting keys to infinite for location " + locationRow);
            HRegionInfo regionInfo = new HRegionInfo (locationRow.getRegionInfo().getTable(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
            location = new HRegionLocation(regionInfo, locationRow.getServerName());
        }
        if (location == null) {
           if (scanRange) {
              scanRegionLocations = pv_table.getRegionsInRange(startRow, endRow, refresh);
              scanRegionLocationsIter = scanRegionLocations.iterator();
              location = scanRegionLocationsIter.next();
           }
           else 
              location = pv_table.getRegionLocation(startRow, refresh);
        }
        boolean readOnlyScan = true;
        if(envTmBypassFirstRead == true && scanRange == true)
        {
          do {
            TransactionRegionLocation trLocation = new TransactionRegionLocation(location.getRegionInfo(),
                                                                             location.getServerName(),
                                                                             pv_peerId);
            if(ts.isReadOnlyLocation(trLocation) == false)
            {
              readOnlyScan = false;
              break;
            }
            if (scanRegionLocationsIter.hasNext())
              location = scanRegionLocationsIter.next();
            else 
              break;
          } while(true);
          scanRegionLocationsIter = scanRegionLocations.iterator();
          location = scanRegionLocationsIter.next();
        }
        else
          readOnlyScan = false;
        do {
           TransactionRegionLocation trLocation = new TransactionRegionLocation(location.getRegionInfo(),
                                                                             location.getServerName(),
                                                                             pv_peerId);
           if (bSynchronized) {
              trLocation.setGenerateCatchupMutations((pv_tmFlags & XDC_DOWN) == XDC_DOWN);
           }
           if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, created TransactionRegionLocation ["
              + trLocation.getRegionInfo().getRegionNameAsString()
              + "], startKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getStartKey())
              + "], endKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey())
              + " and transaction [" + transactionID + "]");

           int action = ts.tableAttrSetAndRegister(pTableName, pv_tmFlags);
           if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, action for table IBR/SYNC attribute " + action);
           
           if (LOG.isDebugEnabled()) LOG.debug("HAX - RegisteTransaction RMInterface: "
              + " trans id: " + transactionID
              + " tmFlags: " + pv_tmFlags
              + " action: " + action
              + " location: " + trLocation.getRegionInfo().getRegionNameAsString()
              + " startKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getStartKey())
              + " endKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey())              );

           if (action == ACTION_ATTR_SET_SEND) { // set table attr with TM (override table attr with IBR CDC enabled)           
              int temp_pv_tmFlags = pv_tmFlags | TABLE_ATTR_SET;
              // this is to tell TM to set table attr not region registration (overload with reg message template
              if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, called registerLocation to set table IBR attribute");
              ts.registerLocation(location, pv_peerId, temp_pv_tmFlags);
           }
           // if this region hasn't been registered as participating in the transaction, we need to register it
           if(envTmRedoRegister == true) {
             if((pv_tmFlags & ADD_TOTALNUM) == ADD_TOTALNUM ) { trLocation.addTotalNum = true; }
           }
           if (ts.addRegion(trLocation)) {
              if( (pv_tmFlags & ADD_TOTALNUM) == ADD_TOTALNUM ) trLocation.addTotalNum = true;
              register = true;
              if (LOG.isDebugEnabled()) LOG.debug("RMInterface:registerTransaction, added TransactionRegionLocation ["
                        + trLocation.getRegionInfo().getRegionNameAsString()
                        + "], startKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getStartKey())
                        + "], endKey: " + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey())
                        + " to transaction " + transactionID
                        + " with " + ts.getParticipantCount() + " participants");
           }
           if(envTmBypassFirstRead == true) {
             if(scanRange == false) {
               if(ts.isReadOnlyLocation(trLocation) == true)
               {
                 register = false;
                 ts.bypassRegisterFirstRead = true; //reset it after use this flag in get/scan interface , ugly
               }
               else
                 ts.bypassRegisterFirstRead = false;
             }
             else
             {
               if( readOnlyScan == true )
               {
                 register = false;
                 ts.bypassRegisterFirstRead = true; //reset it after use this flag in get/scan interface , ugly
               }
               else
                 ts.bypassRegisterFirstRead = false;
             }
           }
           // register region with TM.
           if (register) {
     	   //add any flags that need to be passed as part of registerLocation.
               if (pv_skipConflictCheck) {
                  if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, adding SKIP_CONFLICT to tmFlags for ts " + ts);
        	  pv_tmFlags |= SKIP_CONFLICT;
               }
               ts.registerLocation(location, pv_peerId, pv_tmFlags);
               registerTM = register;
               if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, called registerLocation TransactionRegionLocation [" + trLocation.getRegionInfo().getRegionNameAsString() +  "\nEncodedName: [" + trLocation.getRegionInfo().getEncodedName() + "], endKey: "
                  + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey()) + " to transaction [" + transactionID + "]");
           }
           else {
              if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction did not send registerRegion for transaction " + transactionID);
           }
           if (scanRange && scanRegionLocationsIter.hasNext())
              location = scanRegionLocationsIter.next();
           else
              break;
           register = false;
        } while (true);
        
        if (LOG.isDebugEnabled()) LOG.debug("Exit registerTransaction, transaction ID: " + transactionID + ", startId: " + ts.getStartId()
                           + ", table " + pTableName + ", register to TM " + registerTM);        
        return ts;
    }

    // Default registration method if SQL isn't passing any tmFlags
    public TransactionState registerTransaction(final long transactionID,
		     final byte[] row,
		     final boolean pv_sendToPeers,
		     final boolean pv_skipConflictCheck) throws IOException {
       return registerTransaction (transactionID, row, pv_sendToPeers,
                                 pv_skipConflictCheck, 0 /* flags are absent */);
    }

    public TransactionState registerTransaction(final long transactionID,
							     final byte[] row,
							     final boolean pv_sendToPeers,
							     final boolean pv_skipConflictCheck,
							     final int pv_tmFlags) throws IOException {

       if (LOG.isTraceEnabled()) LOG.trace("Enter registerTransaction," 
					    + " transaction ID: " + transactionID
					    + " sendToPeers: " + pv_sendToPeers
					    + " skipConflictCheck: " + pv_skipConflictCheck
                        + " tmFlags: " + pv_tmFlags);

       boolean refresh = true;
       TransactionState ts = registerTransaction(ttable, transactionID, row, 0,
                                               pv_skipConflictCheck, pv_tmFlags);

       if (bSynchronized && pv_sendToPeers && (pSTRConfig.getConfiguredPeerCount() > 0)) {
          TransactionalTable lv_table = null;
          int lv_peerId = -1;
          int lv_first_remote_peer_id = pSTRConfig.getFirstRemotePeerId();

          // Note. this refresh might only work for 1 peer scenario, not appropriate for peer count > 1
          if (LOG.isDebugEnabled()) LOG.debug(" xDC: register transaction: first remote peer id " + lv_first_remote_peer_id + " strconfig peer count " +
                         pSTRConfig.getConfiguredPeerCount() + " peer table size " + peer_tables.size()  );
          if (  (isSTRUp(lv_first_remote_peer_id)) && (pSTRConfig.getConfiguredPeerCount() > peer_tables.size())    ){
                 peer_tables = null;
                 if (! isPeerConnectionUp(lv_first_remote_peer_id)) {
                              refresh = false;
                              throw new IOException("Problem in register Transaction: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                 }
                 setSynchronized(bSynchronized);
                 if (LOG.isWarnEnabled()) LOG.warn("xDC: reset peer table: first remote peer id " + lv_first_remote_peer_id + " strconfig peer count " +
                         pSTRConfig.getConfiguredPeerCount() + " peer table size " + peer_tables.size()  );
          }

          try {
             for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                   lv_peerId = e.getKey();
                   if (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP) {
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in register Transaction: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                         }
                         lv_table = e.getValue();
                         registerTransaction(lv_table, transactionID, row, lv_peerId,
                                 pv_skipConflictCheck, pv_tmFlags);
                   }
              } // for
          } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
          }
       }

       if (LOG.isTraceEnabled()) LOG.trace("Exit registerTransaction, transaction ID: " + transactionID);
       return ts;
    }

    protected static long getTmId() throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter getTmId");

        long IdVal = -1;
        IdTmId Id;
        try {
           Id = new IdTmId();
           if (LOG.isTraceEnabled()) LOG.trace("getTmId getting new Id with timeout " + ID_TM_SERVER_TIMEOUT);
           idServer.id(ID_TM_SERVER_TIMEOUT, Id);
           if (LOG.isTraceEnabled()) LOG.trace("getTmId idServer.id returned: " + Id.val);
        } catch (IdTmException exc) {
           LOG.error("getTmId: IdTm threw exception " , exc);
           throw new IOException("getTmId: IdTm threw exception ", exc);
        }
        IdVal = Id.val;

        if (LOG.isTraceEnabled()) LOG.trace("Exit getTmId, ID: " + IdVal);
        return IdVal;
    }

    public void pushEpoch(HTableDescriptor desc, long transID) throws IOException {
       if (LOG.isTraceEnabled()) LOG.trace("Enter pushEpoch, txid: " + transID + " Table: " + desc.getNameAsString()
              + ", tableDesc: " + desc);
       byte[] lv_byte_desc = desc.toByteArray();
       byte[] lv_byte_tblname = desc.getNameAsString().getBytes();
       if (LOG.isWarnEnabled()) LOG.warn("pushEpoch for Table: " + desc.getNameAsString());
       String retstr = pushEpochReq(lv_byte_desc, transID, lv_byte_tblname);
       if(retstr != null){
          LOG.error("pushEpoch exception. Unable to push epoch to table " + desc.getNameAsString() + " txid " + transID + " exception " + retstr);
          throw new IOException("pushEpoch exception. Unable to push epoch to table " + desc.getNameAsString() + " Reason: " + retstr);
       }
       if (LOG.isTraceEnabled()) LOG.trace("Exit pushEpoch, txid: " + transID
            + " Table: " + desc.getNameAsString());
    }

    public void createTable(HTableDescriptor desc, byte[][] keys, int numSplits, int keyLength, long transID, boolean incrBackupEnabled) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter createTable, txid: " + transID + " Table: " + desc.getNameAsString()
					    + ", tableDesc: " + desc
					    );
        byte[] lv_byte_desc = desc.toByteArray();
        byte[] lv_byte_tblname = desc.getNameAsString().getBytes();
        //For now only this option. Later extend if needed.
        int options = incrBackupEnabled ? 1 : 0;
        if (LOG.isInfoEnabled()) LOG.info("createTable for Table: " + desc.getNameAsString() + " options: " + options);
        if (LOG.isTraceEnabled()) LOG.trace("createTable: htabledesc bytearray: " + lv_byte_desc + "desc in hex: " + Hex.encodeHexString(lv_byte_desc));
        String retstr = createTableReq(lv_byte_desc, keys, numSplits, keyLength, transID, lv_byte_tblname, options);
        if(retstr != null)
        {
        	LOG.error("createTable exception. Unable to create table " + desc.getNameAsString() + " txid " + transID + " exception " + retstr);
        	throw new IOException("createTable exception. Unable to create table " + desc.getNameAsString() + " Reason: " + retstr);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Adding \'" + desc.getNameAsString() + "\' to createdTables");
        createdTables.add(desc.getNameAsString());
        if (LOG.isTraceEnabled()) LOG.trace("Exit createTable, txid: " + transID
            + " Table: " + desc.getNameAsString() + " createdTables size: " + createdTables.size());
    }

    public void truncateTableOnAbort(String tblName, long transID) throws IOException {
		if (LOG.isTraceEnabled()) LOG.trace("Enter truncateTableOnAbort, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblName = tblName.getBytes();
        String retstr = truncateOnAbortReq(lv_byte_tblName, transID);
        if(retstr != null)
        {
        	LOG.error("truncateTableOnAbort exception. Unable to truncate table" + tblName + " txid " + transID + " exception " + retstr);
        	throw new IOException("truncateTableOnAbort exception. Unable to truncate table " + tblName + " Reason: " + retstr);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit truncateTableOnAbort, txid: " + transID + " Table: " + tblName);
    }

    public void dropTable(String tblName, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter dropTable, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblname = tblName.getBytes();
        String retstr = dropTableReq(lv_byte_tblname, transID);
        if(retstr != null)
        {
        	LOG.error("dropTable exception. Unable to drop table" + tblName + " txid " + transID + " exception " + retstr);
        	throw new IOException("dropTable exception. Unable to drop table" + tblName + " Reason: " + retstr);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit dropTable, txid: " + transID + " Table: " + tblName);
    }

  public int lockRequired(long transID, long savepointID, long pSavepointId, String tableName, int lockMode, boolean registerRegion, String queryContext) throws IOException {
    if (LOG.isTraceEnabled()) LOG.trace("Enter lockRequired, txid: " + transID + " Table: " + tableName + " lockMode: " + lockMode);
    boolean returnCode = false;
    if (mapTransactionStates.get(transID) == null) {
      TransactionState ts = new TransactionState(transID);
      synchronized (mapTransactionStates) {
        mapTransactionStates.put(transID, ts);
      }
      returnCode = ttable.lockRequired(ts, savepointID, pSavepointId, tableName, lockMode, registerRegion, queryContext);
    }
    else {
      returnCode = ttable.lockRequired(getTransactionState(transID), savepointID, pSavepointId, tableName, lockMode, registerRegion, queryContext);
    }
    return returnCode ? 1 : 0;
  }

    public void alter(String tblName, Object[] tableOptions, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter alterTable, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblname = tblName.getBytes();
        String retstr = alterTableReq(lv_byte_tblname, tableOptions, transID);
        if(retstr != null)
        {
        	LOG.error("alter Table exception. Unable to alter table" + tblName + " txid " + transID + " exception " + retstr);
        	throw new IOException("alter Table exception. Unable to alter table" + tblName + " Reason: " + retstr);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit alterTable, txid: " + transID + " Table: " + tblName);
    }   

    static public void replayEngineStart(final long timestamp) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart ENTRY with timestamp: " + timestamp);

      int pit_thread = 0;
      String pitThreadCount = System.getenv("TM_PIT_THREAD");
      if (pitThreadCount != null) {
    	  pit_thread = Integer.parseInt(pitThreadCount);
    	  if(LOG.isDebugEnabled()) LOG.debug("PIT thread count set to: " + pit_thread);
      }
      
      try {
          if (pit_thread < 1) {
	         new ReplayEngine(timestamp, 0, false, false);
          }
          else {
	         new ReplayEngine(timestamp, pit_thread, 0, false, false);
           }
      } catch (Exception e) {
          if (LOG.isTraceEnabled()) LOG.trace("Exception caught creating the ReplayEngine : exception: ", e);
          throw e;
      }
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart EXIT");
    }

    static public void clearTransactionStates(final long transactionID) {
      if (LOG.isTraceEnabled()) LOG.trace("clearTransactionStates enter txid: " + transactionID);

      unregisterTransaction(transactionID);

      if (LOG.isDebugEnabled()) LOG.debug("End Transaction, clearTransactionStates exit txid: " + transactionID);
    }

    static public void unregisterTransaction(final long transactionID) {
      TransactionState ts = null;
      if (LOG.isTraceEnabled()) LOG.trace("Enter unregisterTransaction txid: " + transactionID);
      ts = mapTransactionStates.remove(transactionID);
      if (ts == null) {
         if (LOG.isDebugEnabled())
            LOG.debug("mapTransactionStates.remove did not find transid " + transactionID);
      }
      if (LOG.isTraceEnabled()) LOG.trace("Exit unregisterTransaction txid: " + transactionID);
    }

    // Not used?
    static public void unregisterTransaction(TransactionState ts) {
        if (LOG.isTraceEnabled()) LOG.trace("Enter unregisterTransaction ts: " + ts.getTransactionId());
        mapTransactionStates.remove(ts.getTransactionId());
        if (LOG.isTraceEnabled()) LOG.trace("Exit unregisterTransaction ts: " + ts.getTransactionId());
    }

    public TransactionState getTransactionState(final long transactionID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("getTransactionState txid: " + transactionID);
        TransactionState ts = mapTransactionStates.get(transactionID);
        if (ts == null) {
            if (LOG.isTraceEnabled()) LOG.trace("TransactionState for txid: " + transactionID + " not found; throwing IOException");
            throw new IOException("TransactionState for txid: " + transactionID + " not found" );
        }
        if (LOG.isTraceEnabled()) LOG.trace("EXIT getTransactionState");
        return ts;
    }

    public Result get(final long transactionID, final long savepointId, final long pSavepointId, final int isolationLevel,
                      final int lockMode, final boolean skipConflictAccess, final boolean waitOnSelectForUpdate, final Get get, final boolean firstReadBypassTm, final String queryContext) throws IOException {
        if (LOG.isDebugEnabled()) LOG.debug("get txid: " + transactionID + " savepointId " + savepointId
                + " skipConflictAccess: " + skipConflictAccess);
        if (get.getConsistency() == Consistency.TIMELINE) {
           if (this.rmiConsistency == HAS_UPDATE) { // already has IUD on this RMI
              LOG.warn("RMInterface: conflict with Timeline get with previous IUD for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: Timeline get conflicts with previous IUD for table " + pTableName + 
              //                      " transaction " + transactionID);
           }
           rmiConsistency = HAS_TIMELINE_ACCESS;
        }
        boolean oldTmBypassValue = envTmBypassFirstRead;
        if( lockMode == 8 || waitOnSelectForUpdate == true || firstReadBypassTm == false) envTmBypassFirstRead = false;
        TransactionState ts = registerTransaction(transactionID, get.getRow(), false, false);
        if( lockMode == 8 || waitOnSelectForUpdate == true || firstReadBypassTm == false) envTmBypassFirstRead = oldTmBypassValue;
        if(ts.bypassRegisterFirstRead == true && lockMode != 8 && waitOnSelectForUpdate == false && firstReadBypassTm == true) 
        {
          ts.bypassRegisterFirstRead = false; //reset this flag 
          return ttable.get(get);
        }
        Result res = ttable.get(ts, savepointId, pSavepointId, isolationLevel, lockMode, skipConflictAccess, waitOnSelectForUpdate, get, /* migrate */ false, queryContext);
        if (LOG.isTraceEnabled()) LOG.trace("EXIT get -- result: " + res.toString());
        return res;	
    }

    public Result[] get(final long transactionID, final long savepointId, final long pSavepointId, final int isolationLevel,
                      final int lockMode, final boolean skipConflictCheck, final boolean waitOnSelectForUpdate, final List<Get> gets, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter get<List> txid: " + transactionID + " savepointId " + savepointId
                + " isolationLevel " + isolationLevel + " lockMode " + " skipConflictCheck: " + skipConflictCheck
                + " with list size: " + gets.size());

        if (gets.get(0).getConsistency() == Consistency.TIMELINE) {
           if (this.rmiConsistency == HAS_UPDATE) { // already has IUD on this RMI
              LOG.warn("RMInterface: conflict with Timeline get with previous IUD for table " + pTableName +
                              " transaction " + transactionID);
              //throw new IOException("RMInterface: Timeline get conflicts with previous IUD for table " + pTableName +
              //                      " transaction " + transactionID);
           }
           rmiConsistency = HAS_TIMELINE_ACCESS;
        }

        TransactionState ts = null;
        if ((envBroadcastMode == false) || (skipConflictCheck == false))
        {
           for (Get get : gets) {
              ts = registerTransaction(transactionID, get.getRow(), true, skipConflictCheck);
           }
        }
        // The only way to get here is if both envBroadcastMode AND skipConflictCheck are true
        else
        {
           if (gets.size() > 0)
              ts = registerTransaction(transactionID, gets.get(0).getRow(), true, skipConflictCheck);
        }
        if (ts == null){
           ts = mapTransactionStates.get(transactionID);
        }

        Result[] results = ttable.get_rowset(ts, savepointId, pSavepointId, isolationLevel, lockMode, waitOnSelectForUpdate, skipConflictCheck, gets, queryContext);
        if (LOG.isTraceEnabled()) LOG.trace("EXIT get<List> -- txid: " + transactionID + " result size: " + results.length);
        return results;
    }

    public void delete(final long transactionID, final long savepointId, final long pSavepointId,
                        final boolean skipConflictCheck, final Delete delete, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("delete txid: " + transactionID + " savepointId " + savepointId);
        boolean refresh = true;
        
        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: delete conflicts with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: delete conflicts with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;        
        
        int addTotalFlag = ADD_TOTALNUM;
        TransactionState ts = registerTransaction(transactionID, delete.getRow(), true, false, addTotalFlag);

        if(asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                return deleteX(delete, false);
              }
            });

            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                      if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async delete: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                      }
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                  return deleteX(delete, false);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.delete(ts, savepointId, pSavepointId, delete, false, skipConflictCheck, queryContext);
          }
        }
        else {
          ttable.delete(ts, savepointId, pSavepointId, delete, false, skipConflictCheck, queryContext);
          // RegionServerStoppedException, NotServingRegionException
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                      if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in delete: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                      }
                      lv_table.delete(ts, savepointId, pSavepointId, delete, false,skipConflictCheck, queryContext);
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }
        }
    }

    public void peerReconnect(int peerId, long transactionID, IOException ioe, boolean refresh) throws IOException{

        if (refresh) {
           TransactionalTable p_table;
            if ((ioe instanceof RegionServerStoppedException) || (ioe instanceof NotServingRegionException)) {
               if (LOG.isWarnEnabled()) LOG.warn("XDC reconnect peer, txid: " + transactionID + " due to ", ioe);
               if (isSTRUp(peerId)) {
                  if (LOG.isWarnEnabled()) LOG.warn("XDC peer still up, txid: " + transactionID + ", refresh peer connection ");

                  p_table = peer_tables.get(peerId);
                  if (p_table != null) p_table.close();

                  Connection rt_conn = pSTRConfig.resetPeerConnection(peerId);
                  peer_tables.put(peerId, new TransactionalTable(ttable.getTableName(), rt_conn));
              }
               else { // STR DOWN, no retry for any statement, abort and redrive the whole transaction (to get new SDN personality
                  if (LOG.isWarnEnabled()) LOG.warn("XDC peer down, txid: " + transactionID + ", abort transaction and redrive ");
                  throw new IOException("XDC peer access failed, peer is in down state ", ioe);
                }
           }
        }

        throw ioe;
    }

    public void deleteRegionTx(final Delete delete, final boolean autoCommit, final String queryContext) throws IOException {
        long tid;
        if (recoveryToPitMode){
          tid = getTmId();
        }
        else {
          tid = -1;
        }
        if (LOG.isTraceEnabled()) LOG.trace("deleteRegionTx tid: " + tid
              + " recoveryToPitMode " + recoveryToPitMode + " autoCommit " + autoCommit);
        ttable.deleteRegionTx(tid, delete, autoCommit, recoveryToPitMode, queryContext);
        if (LOG.isTraceEnabled()) LOG.trace("deleteRegionTx EXIT tid: " + tid);
    }

    public void delete(final long transactionID, final long savepointId, final long pSavepointId, final boolean skipConflictCheck, final List<Delete> deletes, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter delete (list of deletes) txid: " + transactionID + " savepointId " + savepointId
             + " with list size: " + deletes.size());
        boolean refresh = true;

//        ttable.delete_nontxn_rowset(transactionID, savepointId, deletes);

//*

        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: deletes conflict with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: deletes conflict with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;
        
        TransactionState ts = null;
        for (Delete delete : deletes) {
           int flag = ADD_TOTALNUM;
           ts = registerTransaction(transactionID, delete.getRow(), true, skipConflictCheck, flag);
        }
        if (ts == null){
           ts = mapTransactionStates.get(transactionID);
        }
        if(asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                return deleteX(deletes);
              }
            });

            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async deletes: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                         }
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                  return deleteX(deletes);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.delete(ts, savepointId, pSavepointId, deletes, noConflictCheckForIndex, queryContext);
          }
        }
        else {
          ttable.delete(ts, savepointId, pSavepointId, deletes, noConflictCheckForIndex, queryContext);
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             //for (TransactionalTable lv_table : peer_tables.values()) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                      if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in deletes: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                      }
                      lv_table.delete(ts, savepointId, pSavepointId, deletes, noConflictCheckForIndex, queryContext);
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }
        }
//*/

        if (LOG.isTraceEnabled()) LOG.trace("Exit delete (list of deletes) txid: " + transactionID);
    }

    public ResultScanner getScanner(final long transactionID, final long savepointId, final long pSavepointId, final int isolationLevel,
                                    final int lockMode, final boolean skipConflictAccess, final Scan scan, boolean firstReadBypassTm, final String queryContext) throws IOException {
        if (LOG.isDebugEnabled()) LOG.debug("getScanner txid: " + transactionID + " savepointId " + savepointId
           + " skipConflictAccess " + skipConflictAccess
           + " scan startRow=" + (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW) ?
                   "INFINITE" : Hex.encodeHexString(scan.getStartRow())) + ", endRow="
           + (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) ?
                   "INFINITE" : Hex.encodeHexString(scan.getStopRow())));

        if (scan.getConsistency() == Consistency.TIMELINE) {
           if (this.rmiConsistency == HAS_UPDATE) { // already has IUD on this RMI
              LOG.warn("RMInterface: Timeline scan conflicts with previous IUD for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: Timeline scan conflicts with previous IUD for table " + pTableName + 
              //                      " transaction " + transactionID);
           }
           rmiConsistency = HAS_TIMELINE_ACCESS;
        }
        
        boolean oldTmBypassValue = envTmBypassFirstRead;
        if(lockMode == 8 || firstReadBypassTm == false ) envTmBypassFirstRead = false;

        TransactionState ts = registerTransaction(ttable, transactionID, scan.getStartRow(), scan.getStopRow(), 0, false, 0);

        if(lockMode == 8 || firstReadBypassTm == false) envTmBypassFirstRead = oldTmBypassValue;

        if(ts.bypassRegisterFirstRead == true && lockMode !=8  && firstReadBypassTm == true)
        {
          ts.bypassRegisterFirstRead = false; //reset this flag 
          return ttable.getScanner(scan,0);
        }
        ResultScanner res = ttable.getScanner(ts, savepointId, pSavepointId, isolationLevel, lockMode, skipConflictAccess, scan, queryContext);
        if (LOG.isTraceEnabled()) LOG.trace("EXIT getScanner");
        return res;
    }

    public void putRegionTx(final Put put, final boolean autoCommit, final String queryContext) throws IOException {
        long tid;
        if (recoveryToPitMode){
          tid = getTmId();
        }
        else {
          tid = -1;
        }
        if (LOG.isTraceEnabled()) LOG.trace("Enter putRegionTx, autoCommit: " + autoCommit
               + ", tid " + tid + " recoveryToPitMode " + recoveryToPitMode);
        ttable.putRegionTx(tid, put, autoCommit, recoveryToPitMode, queryContext);
        if (LOG.isTraceEnabled()) LOG.trace("putRegionTx Exit tid: " + tid);
    }

    public void put(final long transactionID, final long savepointId, final long pSavepointId, final boolean skipConflictCheck, final Put put, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter Put txid: " + transactionID + " savepointId " + savepointId);
        boolean refresh = true;

//       List<Put> list = new ArrayList<Put>();
//       list.add(put);
//        ttable.put_nontxn_rowset(transactionID, savepointId, list);
        
//*

        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: put conflicts with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: put conflicts with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;
        int flag = ADD_TOTALNUM ;        
        TransactionState ts = registerTransaction(transactionID, put.getRow(), true, false, flag);

        if(asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                return putX(put, false);
              }
            });

            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async put: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                         }
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                  return putX(put, false);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.put(ts, savepointId, pSavepointId, put, false, skipConflictCheck, queryContext);
          }
        }
        else {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             //for (TransactionalTable lv_table : peer_tables.values()) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                      if (! isPeerConnectionUp(lv_peerId)) {
                           refresh = false;
                           throw new IOException("Problem in put: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                      }
                      lv_table.put(ts, savepointId, pSavepointId, put, false, skipConflictCheck, queryContext);
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }

          ttable.put(ts, savepointId, pSavepointId, put, false, skipConflictCheck, queryContext);
        }
//*/
        if (LOG.isTraceEnabled()) LOG.trace("Exit Put txid: " + transactionID);
    }

    public void put(final long transactionID, final long savepointId, final long pSavepointId, final boolean skipConflictCheck, final List<Put> puts, final boolean noConflictCheckForIndex, final String queryContext) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter put (list of puts) txid: " + transactionID + " savepointId " + savepointId
             + " with list size: " + puts.size());
        boolean refresh = true;

//        ttable.put_nontxn_rowset(transactionID, savepointId, puts);

//*

        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: puts conflict with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: puts conflict with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;
        
        TransactionState ts = null;
        if ((envBroadcastMode == false) || (skipConflictCheck == false))
        {
            for (Put put : puts) {
               int flag = ADD_TOTALNUM;
               ts = registerTransaction(transactionID, put.getRow(), true, skipConflictCheck, flag);
            }
        }  
        // The only way to get here is if both envBroadcastMode AND skipConflictCheck are true
        else 
        {
             if (puts.size() > 0) {
                 int flag = ADD_TOTALNUM;
                 ts = registerTransaction(transactionID, puts.get(0).getRow(), true, skipConflictCheck, flag);
             }
        } 
        if (ts == null){
           ts = mapTransactionStates.get(transactionID);
        }

        if(asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                return putX(puts);
              }
            });

            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async puts: " +transactionID + " xdc peer is either SDN or connection is not ready ");
                         }
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                  return putX(puts);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.put(ts, savepointId, pSavepointId, puts, noConflictCheckForIndex, queryContext);
          }
        }
        else {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             //for (TransactionalTable lv_table : peer_tables.values()) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in puts: xdc peer is either SDN or connection is not ready ");
                         }
                      lv_table.put_rowset(ts, savepointId, pSavepointId, puts, queryContext);
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }
          if (bSynchronized || envRowsetParallelMode) ttable.put_rowset(ts, savepointId, pSavepointId, puts, queryContext);
          else ttable.put(ts, savepointId, pSavepointId, puts, noConflictCheckForIndex, queryContext);
          //ttable.put(ts, savepointId, puts);
        }

//*/
        if (LOG.isTraceEnabled()) LOG.trace("Exit put (list of puts) txid: " + transactionID);
    }

    public boolean checkAndPut(final long transactionID, final long savepointId, final long pSavepointId,
                                            final byte[] row,
                                            final byte[] family,
                                            final byte[] qualifier,
                                            final byte[] value,
                                            final Put put, final int nodeId, final String queryContext) throws IOException {
        boolean refresh = true;

        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: checkAndPut conflicts with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: checkAndPut conflicts with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;                                            
                                            
        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndPut txid: " + transactionID + " savepointId " + savepointId);
        int addFlag = ADD_TOTALNUM;
        TransactionState ts = registerTransaction(transactionID, row, true, false, addFlag);
        final boolean skipRemoteCheck = ts.getSkipRemoteCheck();
        boolean putSucceeded = false;

        if (LOG.isTraceEnabled()) LOG.trace("checkAndPut"
					    + " bSynchronized: " + bSynchronized
					    + " sup peerCount: " + pSTRConfig.getSupPeerCount()
                        + " skipRemoteCheck " + skipRemoteCheck);
        if(asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
              public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                return checkAndPutX(row, family, qualifier, value, put, false, nodeId);
              }
            });
            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async check and put: xdc peer is either SDN or connection is not ready ");
                         }
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
				  if (!Thread.currentThread().getName().startsWith("R")) {
					  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
				  }

                  return checkAndPutX(row, family, qualifier, value, put, skipRemoteCheck, nodeId);
                }
              });
            }
            boolean returnCode = true;
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                Integer result = compPool.take().get();
                if(result == 0) {
                  returnCode = false;
                }
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
            return returnCode;
          }
          else {
            return ttable.checkAndPut(ts, savepointId, pSavepointId, row, family, qualifier, value, put, false, nodeId, queryContext);
          }
        }
        else {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             //for (TransactionalTable lv_table : peer_tables.values()) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in check and put: xdc peer is either SDN or connection is not ready ");
                         }
                      if (LOG.isTraceEnabled()) LOG.trace("Table Info: " + lv_table + " skipRemoteCheck " + skipRemoteCheck);
                      putSucceeded = lv_table.checkAndPut(ts, savepointId, pSavepointId, row, family, qualifier, value, put, skipRemoteCheck, nodeId, queryContext);
                      if (putSucceeded == false){
                          // The put failed  No need to continue on to other tables, we can trurn early.
                          if (LOG.isTraceEnabled()) LOG.trace("checkAndPut failed for " + lv_table + " returning false");
                          return false;
                      }
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }

          putSucceeded = ttable.checkAndPut(ts, savepointId, pSavepointId, row, family, qualifier, value, put, /* skipRemoteCheck */ false, nodeId, queryContext);
          if (LOG.isTraceEnabled()) LOG.trace("checkAndPut returning " + putSucceeded);
          return putSucceeded;
        }
    }

    public synchronized boolean checkAndPutRegionTx(byte[] row, byte[] family,
    		byte[] qualifier, byte[] value, Put put, final boolean autoCommit, final String queryContext) throws IOException {

        long tid;
        if (recoveryToPitMode){
          tid = getTmId();
        }
        else {
          tid = -1;
        }
        if (LOG.isTraceEnabled()) LOG.trace("checkAndPutRegionTx, autoCommit: " + autoCommit
                           + ", tid " + tid + " recoveryToPitMode " + recoveryToPitMode);
        return ttable.checkAndPutRegionTx(tid, row, family, qualifier, value,
                   put, autoCommit, recoveryToPitMode, queryContext);
    }

    public boolean checkAndDelete(final long transactionID, final long savepointId,
                                               final long pSavepointId,
                                               final byte[] row,
                                               final byte[] family,
                                               final byte[] qualifier,
                                               final byte[] value,
                                               final Delete delete, final String queryContext) throws IOException {

        boolean refresh = true;

        if (this.rmiConsistency == HAS_TIMELINE_ACCESS) { // already has Timeline get or scan on this RMI
              LOG.warn("RMInterface: checkAndDelete conflicts with previous Timeline get/scan for table " + pTableName + 
                             " transaction " + transactionID);
              //throw new IOException("RMInterface: checkAndDelete conflicts with previous Timeline get/scan for table " + pTableName + 
              //                      " transaction " + transactionID);
        }
        rmiConsistency = HAS_UPDATE;                                               
                                               
        int addFlag = ADD_TOTALNUM;
        TransactionState ts = registerTransaction(transactionID, row, true, false,addFlag);
        final boolean skipRemoteCheck = ts.getSkipRemoteCheck();
        boolean deleteSucceeded = false;
        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndDelete txid: " + transactionID + " savepointId " + savepointId
                + " skipRemoteCheck " + skipRemoteCheck);

        if (asyncCalls) {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
              int loopCount = 1;
              compPool.submit(new RMCallable(ttable, ts, savepointId, pSavepointId, queryContext) {
                public Integer call() throws IOException {
                  return checkAndDeleteX(row, family, qualifier, value, delete, false);
                }
              });

            TransactionalTable lv_table = null;
            int lv_peerId = -1;
            for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in async check and delete: xdc peer is either SDN or connection is not ready ");
                         }
                loopCount++;
                compPool.submit(new RMCallable(lv_table, ts, savepointId, pSavepointId, queryContext) {
                  public Integer call() throws IOException {
					  if (!Thread.currentThread().getName().startsWith("R")) {
						  Thread.currentThread().setName("RMCallable-" + Thread.currentThread().getName());
					  }

                    return checkAndDeleteX(row, family, qualifier, value, delete, skipRemoteCheck);
                  }
                });
              }
              try {
                for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                  compPool.take().get();
                }
                return true;
              } catch(Exception ex) {
                throw new IOException(ex);
              }
          }
          else {
            return ttable.checkAndDelete(ts, savepointId, pSavepointId, row, family, qualifier, value, delete, false, queryContext);
          }
        }
        else {
          if (bSynchronized && (ts.getXdcType() == XdcTransType.XDC_TYPE_XDC_UP)) {
             TransactionalTable lv_table = null;
             int lv_peerId = -1;
             try {
                 //for (TransactionalTable lv_table : peer_tables.values()) {
                 for ( Map.Entry<Integer, TransactionalTable> e : peer_tables.entrySet() ) {
                      lv_peerId = e.getKey();
                      lv_table = e.getValue();
                         if (! isPeerConnectionUp(lv_peerId)) {
                              refresh = false;
                              throw new IOException("Problem in check and delete: xdc peer is either SDN or connection is not ready ");
                         }
                      deleteSucceeded = lv_table.checkAndDelete(ts, savepointId, pSavepointId, row, family, qualifier, value, delete, skipRemoteCheck, queryContext);
                      if (deleteSucceeded == false){
                          // The delete failed  No need to continue on to other tables, we can return early.
                          if (LOG.isTraceEnabled()) LOG.trace("checkAndDelete failed for " + lv_table + " returning false");
                          return false;
                      }
                 }
             } catch (IOException ioe) {
                 peerReconnect(lv_peerId, transactionID, ioe, refresh);
             }
          }
          deleteSucceeded = ttable.checkAndDelete(ts, savepointId, pSavepointId, row, family, qualifier, value, delete, false, queryContext);
          if (LOG.isTraceEnabled()) LOG.trace("checkAndDelete returning " + deleteSucceeded);
          return deleteSucceeded;
        }
    }

    public synchronized boolean checkAndDeleteRegionTx(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete, final boolean autoCommit, final String queryContext) throws IOException {
        long tid;
        if (recoveryToPitMode){
          tid = getTmId();
        }
        else {
          tid = -1;
        }
       if (LOG.isTraceEnabled()) LOG.trace("checkAndDeleteRegionTx, autoCommit: " + autoCommit
               + ", tid " + tid + " recoveryToPitMode " + recoveryToPitMode);
       return ttable.checkAndDeleteRegionTx(tid, row, family, qualifier, value, delete, autoCommit, recoveryToPitMode, queryContext);
    }

    /**
     * Abort a savepoint.
     *
     * @param transactionId
     * @param savepointId
     * @throws IOException
     */
    public static short doAbortSavepoint(final long transactionId, final long savepointId, final long pSavepointId,
               final Configuration config, final Connection conn) throws IOException {

       if (LOG.isDebugEnabled()) LOG.debug("Enter doAbortSavepoint, txId: " + transactionId + " savepointId " + savepointId);
       TransactionState ts = mapTransactionStates.get(transactionId);

       if(ts == null) {
          LOG.warn("Returning from doAbortSavepoint for , txId: " + transactionId  + ", savepointId " + savepointId + ", (null tx) retval: "
               + TransReturnCode.RET_NOTX.toString());
          return TransReturnCode.RET_OK.getShort();
       }

       TransactionManager lv_tm;
       try {
          if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepoint, calling trxManager.abortSavepoint(" + ts.getTransactionId() + ")" );
          try {
             lv_tm = TransactionManager.getInstance(config, conn);
          } catch (IOException e ){
             LOG.error("trxManager Initialization failure throwing exception", e);
             throw e;
          }
          lv_tm.abortSavepoint(ts, savepointId, pSavepointId);
       } catch (IOException e) {
          LOG.error("abortSavepoint, transaction: " + transactionId + " savepointId " + savepointId
                + ", Exception ", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       if (LOG.isTraceEnabled()) LOG.trace("doAbortSavepoint, trxManager.abortSavepoint complete, returning " + TransReturnCode.RET_OK.getShort());
       return TransReturnCode.RET_OK.getShort();
    }

    /**
     * Commit a savepoint.
     *
     * @param transactionId
     * @param savepointId
     * @throws IOException
     */
    public static short doCommitSavepoint(final long transactionId,
                    final long savepointId, final long pSavepointId,
                    final Configuration config, final Connection conn) throws IOException {

       if (LOG.isDebugEnabled()) LOG.debug("Enter doCommitSavepoint, txId: " + transactionId + " savepointId " + savepointId);

       TransactionState ts = mapTransactionStates.get(transactionId);

       if(ts == null) {
          LOG.warn("Returning from doCommitSavepoint for , txId: " + transactionId  + ", savepointId " + savepointId + ", (null tx) retval: "
                  + TransReturnCode.RET_NOTX.toString() + "{" + TransReturnCode.RET_NOTX.getShort() + ")");
          LOG.warn("txId: " + transactionId  + " not found in map of size " + mapTransactionStates.size());
          return TransReturnCode.RET_OK.getShort();
       }

       TransactionManager lv_tm;
       try {
          if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepoint, calling trxManager.commitSavepoint(" + ts.getTransactionId() + ")" );
          try {
             lv_tm = TransactionManager.getInstance(config, conn);
          } catch (Exception e ){
             LOG.error("trxManager Initialization failure throwing exception", e);
             throw e;
          }
          lv_tm.commitSavepoint(ts, savepointId, pSavepointId);
       } catch (IOException e) {
          LOG.error("commitSavepoint, transaction: " + transactionId + " savepointId " + savepointId
                + ", Exception ", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       if (LOG.isTraceEnabled()) LOG.trace("doCommitSavepoint, trxManager.commitSavepoint complete, returning " + TransReturnCode.RET_OK.getShort());
       return TransReturnCode.RET_OK.getShort();
    }

    public void close()  throws IOException
    {
        ttable.close();
    }

    public void setAutoFlush(boolean autoFlush, boolean f)
    {
        ttable.setAutoFlush(autoFlush,f);
    }
    public org.apache.hadoop.conf.Configuration getConfiguration()
    {
        return ttable.getConfiguration();
    }
    public void flushCommits() throws IOException {
         ttable.flushCommits();
    }
    public byte[][] getEndKeys()
                    throws IOException
    {
        return ttable.getEndKeys();
    }
    public byte[][] getStartKeys() throws IOException
    {
        return ttable.getStartKeys();
    }
    public void setWriteBufferSize(long writeBufferSize) throws IOException
    {
        ttable.setWriteBufferSize(writeBufferSize);
    }
    public long getWriteBufferSize()
    {
        return ttable.getWriteBufferSize();
    }
    public byte[] getTableName()
    {
        return ttable.getTableName();
    }
    public String getTableNameAsString()
    {
        return Bytes.toString(ttable.getTableName());
    }
    public ResultScanner getScanner(Scan scan, float dopParallelScanner) throws IOException
    {
        return ttable.getScanner(scan, dopParallelScanner);
    }
    public Result get(Get g) throws IOException
    {
        return ttable.get(g);
    }

    public Result[] get( List<Get> g) throws IOException
    {
        return ttable.get(g);
    }
    public void delete(Delete d) throws IOException
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn delete for IBR table " + bIncrementalBR);
       if ((bIncrementalBR) && (envIBRNonTxn)) {
           long iBRid = iBRTransactionId.getAndIncrement();
           List<Delete> list = new ArrayList<Delete>();
           list.add(d);
           if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn delete txid: " + iBRid + " for IBR table with list size: " + list.size());
           delete(iBRid, (long) INCREMENTALBR, list);
        }
        else ttable.delete(d);
    }
    public void delete(List<Delete> deletes) throws IOException
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn delete (list of deletes) for IBR table " + bIncrementalBR);
       if ((bIncrementalBR) && (envIBRNonTxn)) {
           long iBRid = iBRTransactionId.getAndIncrement();
           if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn delete (list of deletes) txid: " + iBRid + " for IBR with list size: " + deletes.size());
           delete(iBRid, (long) INCREMENTALBR, deletes);
        }
        else ttable.delete(deletes);
    }
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException
    {
        if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn checkAndPut for IBR table " + bIncrementalBR);
        return ttable.checkAndPut(row,family,qualifier,value,put);
    }
    public void put(Put p) throws IOException
    {
       if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn put for IBR table " + bIncrementalBR);
       if ((bIncrementalBR) && (envIBRNonTxn)) {
           long iBRid = iBRTransactionId.getAndIncrement();
           List<Put> list = new ArrayList<Put>();
           list.add(p);
           if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn put txid: " + iBRid + " for IBR table with list size: " + list.size());
           put(iBRid, (long) INCREMENTALBR, list);
        }
        else ttable.put(p);
    }
    public void put(List<Put> p) throws IOException
    {
        if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn put (list of puts) for IBR table " + bIncrementalBR);
        if ((bIncrementalBR) && (envIBRNonTxn)) {
           long iBRid = iBRTransactionId.getAndIncrement();
           if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn put (list of puts) txid: " + iBRid + " for IBR table with list size: " + p.size());
           put(iBRid, (long) INCREMENTALBR, p);
        }
        else ttable.put(p);
    }
    public void put(final long nonTransactionId, long flags, List<Put> p) throws IOException
    {
        int retryCount = 0;
        int retrySleep = TM_SLEEP;
        boolean excep = false;
        long cid = 1001;
        do {
              try {
                     excep = false;
                     cid = getTmId();
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn put -- getTmId: " + nonTransactionId + " cid " + cid);
                     ttable.put_nontxn_rowset(nonTransactionId, cid, flags, p);
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn put -- do non-txn put: " + nonTransactionId + " cid " + cid + " retry " + retryCount);
              } catch (Throwable e) {
                     if (LOG.isWarnEnabled()) LOG.warn("Non-txn put EH -- Retry " + retryCount + " with new HTable, ERROR while calling putMultipleNonTransactional ",e);
                     if (retryCount++ >= TM_RETRY_ATTEMPTS) {
                             if (LOG.isWarnEnabled()) LOG.warn("Non-txn put EH -- Exhaust Retry " + retryCount);
                             throw e;
                     }
                     excep = true;
                     retrySleep = retry(retrySleep);
                     ttable = new TransactionalTable(Bytes.toBytes(this.pTableName), getConnection());
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn put EH -- acquire new TT " + nonTransactionId + " cid " + cid + " retry " + retryCount + " sleep " + retrySleep);
              }
         } while (excep);
         // must succeed
         if (LOG.isTraceEnabled()) LOG.trace("Non-txn put complete txid: " + nonTransactionId + " cid " + cid + " retry " + retryCount);
    }
    public void delete(final long nonTransactionId, long flags, List<Delete> d) throws IOException
    {
        int retryCount = 0;
        int retrySleep = TM_SLEEP;
        boolean excep = false;
        long cid = 1002;
        do {
              try {
                     excep = false;
                     cid = getTmId();
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn delete -- getTmId: " + nonTransactionId + " cid " + cid);
                     ttable.delete_nontxn_rowset(nonTransactionId, cid, flags, d);
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn delete -- do non-txn delete: " + nonTransactionId + " cid " + cid + " retry " + retryCount);
              } catch (Throwable e) {
                     if (LOG.isWarnEnabled()) LOG.warn("Non-txn delete EH -- Retry " + retryCount + " with new HTable ERROR while calling deleteMultipleNonTransactional ",e);
                     if (retryCount++ >= TM_RETRY_ATTEMPTS) {
                             if (LOG.isWarnEnabled()) LOG.warn("Non-txn delete EH -- Exhaust Retry " + retryCount);
                             throw e;
                     }
                     excep = true;
                     retrySleep = retry(retrySleep);
                     ttable = new TransactionalTable(Bytes.toBytes(this.pTableName), getConnection());
                     if (LOG.isTraceEnabled()) LOG.trace("Non-txn delete EH -- acquire new TT " + nonTransactionId + " cid " + cid + " retry " + retryCount + " sleep " + retrySleep);
              }
         } while (excep);
         // must succeed
         if (LOG.isTraceEnabled()) LOG.trace("Non-txn delete complete txid: " + nonTransactionId + " cid " + cid + " retry " + retryCount);
    }
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,  Delete delete) throws IOException
    {
        if (LOG.isTraceEnabled()) LOG.trace("Enter non-txn checkAndDelete for IBR table " + bIncrementalBR);
        return ttable.checkAndDelete(row,family,qualifier,value,delete);
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
     } // retry method

   public short pushRegionEpoch(String tableName, long transID, long epochTs, 
          final Configuration config, final Connection conn) throws IOException {
      //invoke the pushRegionEpoch from TransactionManager
      TransactionManager lv_tm;
      try {
        try {
             lv_tm = TransactionManager.getInstance(config, conn);
        } catch (IOException e ){
          LOG.error("trxManager Initialization failure throwing exception", e);
          throw e;
        }
        lv_tm.pushRegionEpoch(tableName, transID, epochTs);
      }
      catch (IOException e) {
          LOG.error("pushRegionEpoch, transaction: " + transID  
                + ", Exception ", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
      }      
      return 0;
   }
}
