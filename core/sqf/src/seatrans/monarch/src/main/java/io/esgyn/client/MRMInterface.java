package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.AmpoolUtils;
import io.esgyn.utils.EsgynMServerLocation;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import org.apache.hadoop.hbase.exceptions.DeserializationException;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;

//import org.trafodion.pit.ReplayEngine;

public class MRMInterface {
    static final Logger LOG = LogManager.getLogger(MRMInterface.class.getName());
    static Map<Long, MTransactionState> mapTransactionStates = MTransactionMap.getInstance();
    static final Object mapLock = new Object();

    static Map<Long, Set<MRMInterface>> mapRMsPerTransaction = new HashMap<Long,  Set<MRMInterface>>();
    private MTransactionalTableClient ttable = null;
    private boolean bSynchronized=false;
    private boolean asyncCalls = false;
    private boolean recoveryToPitMode = false;
    private ExecutorService threadPool;
    private CompletionService<Integer> compPool;
    private int intThreads = 16;
    protected Map<Integer, MTransactionalTableClient> peer_tables;
    private static boolean envRecoveryToPitMode;
    private static boolean envAsyncCalls;
    static {
        System.loadLibrary("stmlib");
        String envset = System.getenv("TM_USE_SSCC");
        if (envset != null)
           envTransactionAlgorithm = (Integer.parseInt(envset) == 1) ? AlgorithmType.SSCC : AlgorithmType.MVCC;
        else
           envTransactionAlgorithm = AlgorithmType.MVCC;
        String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
        if (usePIT != null)
           envRecoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
        else
           envRecoveryToPitMode = false;
        String asyncSet = System.getenv("TM_ASYNC_RMI");
        if (asyncSet != null) 
           envAsyncCalls = (Integer.parseInt(asyncSet) == 1) ? true : false;
        else
           envAsyncCalls = false;
    }

    private static STRConfig pSTRConfig = null;
    static {
       Configuration lv_config = HBaseConfiguration.create();
       try {
          pSTRConfig = STRConfig.getInstance(lv_config);
       }
       catch (IOException ioe) {
	   LOG.error("IO Exception trying to get STRConfig instance: ", ioe);
       }
    }

    private native void createTableReq(byte[] lv_byte_htabledesc, byte[][] keys, int numSplits, int keyLength, long transID, byte[] tblName);
    private native void dropTableReq(byte[] lv_byte_tblname, long transID);
    private native void truncateOnAbortReq(byte[] lv_byte_tblName, long transID); 
    private native void alterTableReq(byte[] lv_byte_tblname, Object[] tableOptions, long transID);

    public static void main(String[] args) {
      System.out.println("MAIN ENTRY");
    }

    private IdTm idServer;
    private static final int ID_TM_SERVER_TIMEOUT = 1000;

    public enum AlgorithmType {
       MVCC, SSCC
    }

    private AlgorithmType transactionAlgorithm;
    private static AlgorithmType envTransactionAlgorithm;

    public MRMInterface(final String tableName, boolean pb_synchronized) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("MRMInterface constructor:"
					    + ", tableName: " + tableName
					    + ", synchronized: " + pb_synchronized
					    );
        bSynchronized = pb_synchronized;
        recoveryToPitMode = envRecoveryToPitMode;

        transactionAlgorithm = envTransactionAlgorithm;
        if( transactionAlgorithm == AlgorithmType.MVCC) //MVCC
        {
           if (LOG.isTraceEnabled()) LOG.trace("Algorithm type: MVCC"
						+ ", tableName: " + tableName
						+ ", peerCount: " + pSTRConfig.getConfiguredPeerCount()
					       );
           ttable = new MTransactionalTable(Bytes.toBytes(tableName));
        }
        else if(transactionAlgorithm == AlgorithmType.SSCC)
        {
	    //ttable = new SsccTransactionalTable( Bytes.toBytes(tableName));
        }

	setSynchronized(bSynchronized);

        idServer = new IdTm(false);

        asyncCalls = envAsyncCalls;
        if(asyncCalls) {
          if (LOG.isTraceEnabled()) LOG.trace("Asynchronous MRMInterface calls set");
          threadPool = Executors.newFixedThreadPool(intThreads);
          compPool = new ExecutorCompletionService<Integer>(threadPool);
        }
        if (LOG.isTraceEnabled()) LOG.trace("MRMInterface constructor exit");
    }

    public MRMInterface() throws IOException {

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

    public void setSynchronized(boolean pv_synchronize) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("MRMInterface setSynchronized:"
					    + ", table: " + new String(ttable.getTableName())
					    + ", peerCount: " + pSTRConfig.getConfiguredPeerCount()
					    + ", synchronize flag: " + pv_synchronize
					    );
	
	bSynchronized = pv_synchronize;

	if (bSynchronized && 
	    (peer_tables == null) && 
	    (pSTRConfig.getConfiguredPeerCount() > 0)) {
	    if (LOG.isTraceEnabled()) LOG.trace(" peerCount: " + pSTRConfig.getConfiguredPeerCount());
	    if( transactionAlgorithm == AlgorithmType.MVCC) {
		peer_tables = new HashMap<Integer, MTransactionalTableClient>();
		for ( Map.Entry<Integer, Connection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) continue;
		    if (! isSTRUp(lv_peerId)) {
			if (LOG.isTraceEnabled()) LOG.trace("setSynchronized, STR is DOWN"
							    + ", peerId: " + lv_peerId
							    );
			continue;
		    }

		    if (LOG.isTraceEnabled()) LOG.trace("setSynchronized" 
							+ ", peerId: " + lv_peerId
							);
		    peer_tables.put(lv_peerId, new MTransactionalTable(ttable.getTableName(), e.getValue()));
		}
	    }
	    else if(transactionAlgorithm == AlgorithmType.SSCC) {
		/*
		peer_tables = new HashMap<Integer, TransactionalTableClient>();
		for ( Map.Entry<Integer, HConnection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) continue;
		    peer_tables.put(lv_peerId, new SsccTransactionalTable(ttable.getTableName()));
		}
		*/
	    }
	}
    }

    public boolean isSynchronized() {
       return bSynchronized;
    }

    public synchronized MTransactionState registerTransaction(final MTransactionalTableClient pv_table, 
							      final long transactionID, 
							      final byte[] row,
							      final int pv_peerId) throws IOException 
    {

        if (LOG.isTraceEnabled()) LOG.trace("registerTransaction Entry" 
					    + ", txId: " + transactionID 
					    + ", peerId: " + pv_peerId
					    );
        boolean register = false;
        short ret = 0;

        MTransactionState ts = mapTransactionStates.get(transactionID);

        if (LOG.isTraceEnabled()) LOG.trace("mapTransactionStates" 
					    + ", # entries: " + mapTransactionStates.size()
					    );

        // if we don't have a MTransactionState for this ID we need to register it with the TM
        if (ts == null) {
           if (LOG.isTraceEnabled()) LOG.trace("registerTransaction"
					       + ", txId: " + transactionID 
					       + " not found in mapTransactionStates.size: " 
					       + mapTransactionStates.size()
					       );
           ts = new MTransactionState(transactionID);

           long startIdVal = -1;

           // Set the startid
           if ((recoveryToPitMode)  || (transactionAlgorithm == AlgorithmType.SSCC)) {
              IdTmId startId;
              try {
                 startId = new IdTmId();
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction getting new startId");
                 idServer.id(ID_TM_SERVER_TIMEOUT, startId);
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction idServer.id returned: " + startId.val);
              } catch (IdTmException exc) {
                 LOG.error("registerTransaction: IdTm threw exception " , exc);
                 throw new IOException("registerTransaction: IdTm threw exception ", exc);
              }
              startIdVal = startId.val;
           }

           ts.setStartId(startIdVal);

	   mapTransactionStates.put(transactionID, ts);

           register = true;
        }
        else {
            if (LOG.isTraceEnabled()) LOG.trace("registerTransaction - Entry already exists in map"
						+ ", txState: " + ts
						);
        }

        EsgynMServerLocation location = pv_table.getRegionLocation(row, false);

        // if this region hasn't been registered as participating in the transaction, we need to register it
        if (ts.addRegion(location)) {
          register = true;
          if (LOG.isTraceEnabled()) LOG.trace("registerTransaction"
					      + ", added location: " + location.toString()
					      + ", txId: " + transactionID
					      );
        }

        // register region with TM.
        if (register) {
            ts.registerLocation(location, pv_peerId);
	    if (LOG.isTraceEnabled()) LOG.trace("registerTransaction" 
						+ ", back from registration" 
						+ ", location: " + location.toString()
						+ ", txId: " + transactionID
						);
        }
        else {
          if (LOG.isTraceEnabled()) LOG.trace("registerTransaction did not send registerRegion for " 
					      + ", txId: " + transactionID
					      );
        }

        if ((ts == null) || (ret != 0)) {
            LOG.error("registerTransaction failed, MTransactionState is NULL"); 
            throw new IOException("registerTransaction failed with error.");
        }

        if (LOG.isTraceEnabled()) LOG.trace("registerTransaction Exit" 
					    + ", txId: " + transactionID 
					    + ", startId: " + ts.getStartId()
					    );
        return ts;
    }

    public synchronized MTransactionState registerTransaction(final long transactionID,
							      final byte[] row,
							      final boolean pv_sendToPeers) throws IOException {
	
       if (LOG.isTraceEnabled()) LOG.trace("registerTransaction Entry" 
					    + ", txId: " + transactionID
					    + ", sendToPeers: " + pv_sendToPeers
					   );

       MTransactionState ts = registerTransaction(ttable, transactionID, row, 0);

       if (bSynchronized && pv_sendToPeers && (pSTRConfig.getConfiguredPeerCount() > 0)) {
          for ( Map.Entry<Integer, MTransactionalTableClient> e : peer_tables.entrySet() ) {
             int                      lv_peerId = e.getKey();
	     if (! isSTRUp(lv_peerId)) {
		 continue;
	     }
             MTransactionalTableClient lv_table = e.getValue();
             registerTransaction(lv_table, transactionID, row, lv_peerId);
          }
       }

       if (LOG.isTraceEnabled()) LOG.trace("registerTransaction Exit" 
					   + ", txId: " + transactionID
					   );
       return ts;
    }

    public void createTable(HTableDescriptor desc, byte[][] keys, int numSplits, int keyLength, long transID) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("createTable ENTER:");
            byte[] lv_byte_desc = desc.toByteArray();
            byte[] lv_byte_tblname = desc.getNameAsString().getBytes();
            if (LOG.isTraceEnabled()) LOG.trace("createTable: htabledesc bytearray: " + lv_byte_desc + "desc in hex: " + Hex.encodeHexString(lv_byte_desc));
            createTableReq(lv_byte_desc, keys, numSplits, keyLength, transID, lv_byte_tblname);
    }

    public void truncateTableOnAbort(String tblName, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("truncateTableOnAbort ENTER: ");
            byte[] lv_byte_tblName = tblName.getBytes();
            truncateOnAbortReq(lv_byte_tblName, transID);
    }

    public void dropTable(String tblName, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("dropTable ENTER: ");

            byte[] lv_byte_tblname = tblName.getBytes();
            dropTableReq(lv_byte_tblname, transID);
    }

    public void alter(String tblName, Object[] tableOptions, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("alter ENTER: ");

            byte[] lv_byte_tblname = tblName.getBytes();
            alterTableReq(lv_byte_tblname, tableOptions, transID);
    }   

    /*
    static public void replayEngineStart(final long timestamp) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart ENTRY with timestamp: " + timestamp);

      try {
          ReplayEngine re = new ReplayEngine(timestamp);
      } catch (Exception e) {
          if (LOG.isTraceEnabled()) LOG.trace("Exception caught creating the ReplayEnding : exception: " + e);
          throw e;
      }
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart EXIT");
    }
    */

    static public void clearTransactionStates(final long transactionID) {
      if (LOG.isTraceEnabled()) LOG.trace("cts1 Entry" 
					  + ", txId: " + transactionID
					  );

      unregisterTransaction(transactionID);

      if (LOG.isTraceEnabled()) LOG.trace("cts2 txId: " + transactionID);
    }

    static public synchronized void unregisterTransaction(final long transactionID) {
      MTransactionState ts = null;
      if (LOG.isTraceEnabled()) LOG.trace("unregisterTransaction Entry" 
					  + ", txId: " + transactionID
					  );
        ts = mapTransactionStates.remove(transactionID);
      if (ts == null) {
        LOG.warn("mapTransactionStates.remove did not find txId: " + transactionID);
      }
    }

    // Not used?
    static public synchronized void unregisterTransaction(MTransactionState ts) {
        mapTransactionStates.remove(ts.getTransactionId());
    }

    public synchronized MTransactionState getTransactionState(final long transactionID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("getTransactionState Entry" 
					    + ", txId: " + transactionID);
        MTransactionState ts = mapTransactionStates.get(transactionID);
        if (ts == null) {
            if (LOG.isTraceEnabled()) LOG.trace("MTransactionState for" 
						+ " txId: " + transactionID 
						+ " not found; throwing IOException"
						);
        	throw new IOException("MTransactionState for txId: " + transactionID + " not found" );
        }
        if (LOG.isTraceEnabled()) LOG.trace("getTransactionState Exit"
					    + ", txId: " + transactionID
					    );
        return ts;
    }

    public synchronized Row get(final long transactionID, final EsgynMGet get) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("get Entry" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = registerTransaction(transactionID, get.getRowKey(), false);
        Row res = ttable.get(ts, get, false);
        if (LOG.isTraceEnabled()) LOG.trace("get Exit -- result: " + res.toString());
        return res;	
    }

    public synchronized void delete(final long transactionID, final Delete delete) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("delete Entry"
					    + ", txId: " + transactionID
					    );

        MTransactionState ts = registerTransaction(transactionID, delete.getRowKey(), true);

	ttable.delete(ts, delete, false);
	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
		lv_table.delete(ts, delete, false);
	    }
	}
    }

    public synchronized void delete(final long transactionID, final List<Delete> deletes) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("delete Entry (list of deletes)" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = null;
        for (Delete delete : deletes) {
	    ts = registerTransaction(transactionID, delete.getRowKey(), true);
        }
        if (ts == null){
	    ts = mapTransactionStates.get(transactionID);
        }
	ttable.delete(ts, deletes);
	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.delete(ts, deletes);
	    }
	}
        if (LOG.isTraceEnabled()) LOG.trace("delete Exit (list of deletes) txId: " + transactionID);
    }

    public synchronized Scanner getScanner(final long transactionID, final Scan scan) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("getScanner Entry" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = registerTransaction(transactionID, scan.getStartRow(), false);
        Scanner res = ttable.getScanner(ts, scan);
        if (LOG.isTraceEnabled()) LOG.trace("getScanner Exit"
					    + ", txId: " + transactionID
					    );
        return res;
    }

    public synchronized void put(final long transactionID, final EsgynMPut put) throws IOException {
        if (LOG.isDebugEnabled()) LOG.debug("put Entry" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = registerTransaction(transactionID, put.getRowKey(), true);

	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.put(ts, put, false);
	    }
	}

	ttable.put(ts, put, false);

        if (LOG.isTraceEnabled()) LOG.trace("put Exit" 
					    + ", txId: " + transactionID
					    );
    }

    public synchronized void put(final long transactionID, final List<EsgynMPut> puts) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("put Entry (list of puts)" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = null;
      	for (EsgynMPut put : puts) {
      	    ts = registerTransaction(transactionID, put.getRowKey(), true);
      	}
        if (ts == null){
	    ts = mapTransactionStates.get(transactionID);
        }

	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.put(ts, puts);
	    }
	}
	ttable.put(ts, puts);

        if (LOG.isTraceEnabled()) LOG.trace("put Exit (list of puts)"
					    + ", txId: " + transactionID
					    );
    }

    public synchronized boolean checkAndPut(final long transactionID,
                                            final byte[] row,
                                            final byte[] column,
                                            final byte[] value,
                                            final EsgynMPut put) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("checkAndPut Entry" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = registerTransaction(transactionID, row, true);

        if (LOG.isTraceEnabled()) LOG.trace("checkAndPut"
					    + ", bSynchronized: " + bSynchronized
					    + ", peerCount: " + pSTRConfig.getConfiguredPeerCount()
					    );
	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
                if (LOG.isTraceEnabled()) LOG.trace("Table Info: " + lv_table);
                lv_table.checkAndPut(ts, row, column, value, put);
	    }
	}

	return ttable.checkAndPut(ts, row, column, value, put);
    }

    public synchronized boolean checkAndDelete(final long transactionID,
                                               final byte[] row,
                                               final byte[] column,
                                               final byte[] value,
                                               final EsgynMDelete delete) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("checkAndDelete Entry" 
					    + ", txId: " + transactionID
					    );
        MTransactionState ts = registerTransaction(transactionID, row, true);

	if (bSynchronized && pSTRConfig.getConfiguredPeerCount() > 0) {
	    for (MTransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.checkAndDelete(ts, row, column, value, delete);
	    }
	}

	return ttable.checkAndDelete(ts, row, column, value, delete);
    }

    public void close()  throws IOException
    {
        ttable.close();
    }

    public void setAutoFlush(boolean autoFlush, boolean f)
    {
        ttable.setAutoFlush(autoFlush,f);
    }

    public void flushCommits() throws IOException {
	//TBD ttable.flushCommits();
	return;
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

    public byte[] getTableName()
    {
        return ttable.getTableName();
    }

    public Scanner getScanner(Scan pv_scan) throws IOException
    {
        return this.getScanner(pv_scan, 0);
    }

    public Scanner getScanner(Scan pv_scan, float pv_dopParallelScanner) throws IOException
    {
        return ttable.getScanner(pv_scan, pv_dopParallelScanner);
    }

    public Row get(Get g) throws IOException
    {
        return ttable.get(g);
    }

    public Row[] get( List<Get> g) throws IOException
    {
        return ttable.get(g);
    }

    public void delete(EsgynMDelete d) throws IOException
    {
        ttable.delete(d);
    }

    public void delete(List<Delete> deletes) throws IOException
    {
        ttable.delete(deletes);
    }

    public boolean checkAndPut(byte[] row, byte[] column, byte[] value, EsgynMPut put) throws IOException
    {
        return ttable.checkAndPut(row,column,value,put);
    }

    public void put(EsgynMPut p) throws IOException
    {
        ttable.put(p);
    }

    public void put(List<Put> p) throws IOException
    {
        ttable.put(p);
    }

    public boolean checkAndDelete(byte[] row, byte[] column, byte[] value,  EsgynMDelete delete) throws IOException
    {
        return ttable.checkAndDelete(row,column,value,delete);
    }

}
