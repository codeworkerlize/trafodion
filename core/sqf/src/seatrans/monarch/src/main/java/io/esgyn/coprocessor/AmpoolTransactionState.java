
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

package io.esgyn.coprocessor;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.internal.CellRef;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTableDescriptor;

import io.ampool.monarch.types.BasicTypes;

import io.esgyn.coprocessor.EsgynMDelete;
import io.esgyn.coprocessor.EsgynMGet;
import io.esgyn.coprocessor.EsgynMPut;
import io.esgyn.utils.AmpoolUtils;
import io.esgyn.utils.GeneralUtils;

import java.io.IOException;
import java.lang.Class;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TrxEnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.io.DataInputBuffer;

/**
 * Holds the state of a transaction. This includes a buffer of all writes, a record of all reads / scans, and
 * information about which other transactions we need to check against.
 */
public class AmpoolTransactionState  extends TransactionState  implements java.io.Serializable{

    /**
     * Simple container of the range of the scanners we've opened. Used to check for conflicting writes.
     */
    private static class ScanRange {

        protected byte[] startRow;
        protected byte[] endRow;

        public ScanRange(final byte[] startRow, final byte[] endRow) {
            this.startRow = startRow == HConstants.EMPTY_START_ROW ? null : startRow;
            this.endRow = endRow == HConstants.EMPTY_END_ROW ? null : endRow;
        }

        /**
         * Check if this scan range contains the given key.
         * 
         * @param rowKey
         * @return boolean
         */
        public boolean contains(final byte[] rowKey) {
            if (startRow != null && Bytes.compareTo(rowKey, startRow) < 0) {
                return false;
            }
            if (endRow != null && Bytes.compareTo(endRow, rowKey) < 0) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "startRow: " + (startRow == null ? "null" : Bytes.toStringBinary(startRow)) + ", endRow: "
                    + (endRow == null ? "null" : Bytes.toStringBinary(endRow));
        }
    }

    private List<ScanRange> scans = Collections.synchronizedList(new LinkedList<ScanRange>());
    private List<Delete> deletes = Collections.synchronizedList(new LinkedList<Delete>());
    private List<WriteAction> writeOrdering = Collections.synchronizedList(new LinkedList<WriteAction>());
    private Set<AmpoolTransactionState> transactionsToCheck = Collections.synchronizedSet(new HashSet<AmpoolTransactionState>());
    private WALEdit e;

    public AmpoolTransactionState(final long transactionId, 
				  final long rLogStartSequenceId, 
				  AtomicLong hlogSeqId, 
				  final AmpoolRegionInfo regionInfo,
				  MTableDescriptor htd, 
				  WAL hLog, 
				  boolean logging) 
    {
        super(transactionId,
	      rLogStartSequenceId,
	      hlogSeqId,
	      regionInfo,
	      htd,
	      hLog,
	      logging);

	if (LOG.isTraceEnabled()) LOG.trace("Create Ampool TS object" 
					    + ", txId: " + transactionId 
					    + ", early logging: " + logging
					    );
        this.e = new WALEdit();
    }

    boolean isColumnNameInList(byte[] pv_column_name, EsgynMGet p_get) 
    {
	List<byte[]> lv_columns = p_get.getColumnNameList();

	if (LOG.isInfoEnabled()) LOG.info("isColumnNameInList"
					  + ", #columns in p_get: " + lv_columns.size()
					  + ", column name to check: " + new String(pv_column_name)
					  );
	
	return lv_columns.contains(pv_column_name);
	
    }

    Row packPutInRow(EsgynMPut p_put, 
			    EsgynMGet p_get) 
    {
	if (LOG.isInfoEnabled()) LOG.info("packPutInRow" 
					  + ", txid: " + transactionId
					  + ", row key: " + new String(p_put.getRowKey())
					  );
	
	EsgynMResult lv_result = new EsgynMResult(p_put.getRowKey());
	lv_result.setRowTimeStamp(p_put.getTimeStamp());
	lv_result.setDeletedFlag(false);
	java.util.List<Cell> lv_cells = new ArrayList<Cell>();

	// Get columns from the put 
	Map<io.ampool.monarch.table.internal.ByteArrayKey,java.lang.Object> lv_cvm = p_put.getColumnValueMap();
	List<byte[]> lv_col_name_list_in_get = p_get.getColumnNameList();
	for (Entry<io.ampool.monarch.table.internal.ByteArrayKey, java.lang.Object> lv_e : lv_cvm.entrySet()) {
	    io.ampool.monarch.table.internal.ByteArrayKey lv_e_k = lv_e.getKey();
	    boolean lv_column_requested = GeneralUtils.listContains(lv_col_name_list_in_get,
								    lv_e_k.getByteArray(),
								    true
								    );
	    
	    if (LOG.isTraceEnabled()) LOG.trace("packPutInRow" 
						+ ", columnName: " + new String(lv_e_k.getByteArray())
						+ ", existsInTheGetList: " + lv_column_requested
						+ ", getListSize: " + lv_col_name_list_in_get.size()
						);
	    if (! lv_column_requested) {
		continue;
	    }

	    if (LOG.isTraceEnabled()) LOG.trace("packPutInRow" 
						+ ", lv_cfm key: " + new String(lv_e_k.getByteArray())
						+ ", lv_cfm val: " + new String((byte[]) lv_e.getValue())
						+ ", lv_cfm type of val: " + lv_e.getValue().getClass().getName()
						);
	    CellRef lv_cell_ref = new CellRef(lv_e_k.getByteArray(), 
						BasicTypes.BINARY, 
						(byte[]) lv_e.getValue());
	    lv_cells.add(lv_cell_ref);
	}

	// Get remaining columns (if any)
	Get lv_get_subset = null;
	Row lv_result_subset = null;
	if (lv_col_name_list_in_get.size() > 0) {
	    if (LOG.isInfoEnabled()) LOG.info("packPutInRow - more columns(not updated in this txn) needed" 
					      + ", getListSize: " + lv_col_name_list_in_get.size()
					      );
	    lv_get_subset = AmpoolUtils.prepare_mget_from_list(p_get.getRowKey(),
							       lv_col_name_list_in_get);
	    lv_result_subset = this.regionInfo.m_tableregion.get(lv_get_subset);

	    lv_cells.addAll(lv_result_subset.getCells());
	}

	lv_result.setCells(lv_cells);

	return lv_result;
    }

    Row packDeleteInRow(EsgynMDelete p_delete, 
			       EsgynMGet    p_get) 
    {
	if (LOG.isInfoEnabled()) LOG.info("packDeleteInRow" 
					  + ", txid: " + transactionId
					  + ", row key: " + new String(p_delete.getRowKey())
					  );

	EsgynMResult lv_result = new EsgynMResult(p_delete.getRowKey());
	lv_result.setDeletedFlag(true);

	return lv_result;
    }

    Row get(EsgynMGet p_get) {

	if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactionState.get"
					  + ", txid: " + transactionId
					  + ", writeOrder.size(): " + writeOrdering.size()
					  + ", rowkey: " + new String(p_get.getRowKey())
					  + ", region: " + this.regionInfo
					  );

	byte[] lv_get_rk = p_get.getRowKey();
	for (WriteAction lv_wa : this.writeOrdering) {

	    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactionState.get(), searching for rowkey");
	    EsgynMPut lv_put;
	    EsgynMDelete lv_delete;
	    byte[] lv_tx_rk;

	    lv_put = lv_wa.getPut();
	    if (lv_put != null) {
		lv_tx_rk = lv_put.getRowKey();
		if (Bytes.equals(lv_tx_rk,
				 lv_get_rk)) {
		    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactionState.get(), found rowkey in the writeOrdering.put");
		    return packPutInRow(lv_put, 
					   p_get);
		    
		}
	    }
	    else {
		lv_delete = lv_wa.getDelete();
		lv_tx_rk = lv_delete.getRowKey();
		if (Bytes.equals(lv_tx_rk,
				 lv_get_rk)) {
		    if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactionState.get(), found rowkey in the writeOrdering.delete");
		    return packDeleteInRow(lv_delete, 
					      p_get);
		    
		}
	    }

	}

	if (LOG.isTraceEnabled()) LOG.trace("AmpoolTransactionState.get(), did NOT find the row in the writeOrdering");

	Get lv_get = AmpoolUtils.prepare_mget(p_get);

	return this.regionInfo.m_tableregion.get(lv_get);

    }

    public synchronized void addRead(final byte[] rowKey) {
        scans.add(new ScanRange(rowKey, rowKey));
    }

    public synchronized void addWrite(final EsgynMPut write) {

        if (LOG.isInfoEnabled()) LOG.info("addWrite -- ENTRY" 
					  + ", region: " + this.regionInfo
					  + ", write: " + write.toString()
					  + ", writeOrdering: " + writeOrdering
					  + ", writeOrdering.size(): " + writeOrdering.size()
					  );
        WriteAction waction;
        KeyValue kv;
        //TBD WALEdit e1 = new WALEdit();
	//TBD    updateLatestTimestamp(write.getFamilyCellMap().values(), TrxEnvironmentEdgeManager.currentTime());
        // Adding read scan on a write action
	addRead(new WriteAction(write).getRow());

        ListIterator<WriteAction> writeOrderIter = writeOrdering.listIterator();
        writeOrderIter.add(waction = new WriteAction(write));

	//TBD        if (this.earlyLogging) { // immeditaely write edit out to HLOG during DML (actve transaction state)
	//TBD           for (Cell value : waction.getCells()) {
	//TBD             //KeyValue kv = KeyValueUtil.ensureKeyValue(value);
	//TBD             kv = KeyValue.cloneAndAddTags(value, tagList);
	//TBD             //if (LOG.isInfoEnabled()) LOG.info("KV hex dump " + Hex.encodeHexString(kv.getValueArray() /*kv.getBuffer()*/));
	//TBD             e1.add(kv);
	//TBD             e.add(kv);
	//TBD            }
	//TBD           try {
	//TBD           //long txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
	//TBD           //         e1, new ArrayList<UUID>(), TrxEnvironmentEdgeManager.currentTimeMillis(), this.tabledescriptor,
	//TBD           //         this.logSeqId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
	//TBD           	final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), this.regionInfo.getTable(), TrxEnvironmentEdgeManager.currentTime());;
	//TBD      	 	long txid = this.tHLog.append(this.tabledescriptor,this.regionInfo, wk , e1,
	//TBD      	 			this.logSeqId, false, null);
	//TBD           //if (LOG.isInfoEnabled()) LOG.info("Trafodion Recovery: Y11 write edit to HLOG during put with txid " + txid + " ts flush id " + this.flushTxId);
	//TBD           if (txid > this.flushTxId) this.flushTxId = txid; // save the log txid into TS object, later sync on largestSeqid during phase 1
	//TBD           }
	//TBD           catch (IOException exp1) {
	//TBD	       LOG.info("TrxRegionEndpoint coprocessor addWrite writing to HLOG for early logging: Threw an exception");
	//TBD           //throw exp1;
	//TBD           }
	//TBD}
	//TBD        else 
	//TBD	    { // edits are buffered in ts and written out to HLOG in phase 1
	//TBD            for (Cell value : waction.getCells()) {
	//TBD		//             kv = KeyValue.cloneAndAddTags(value, tagList);
	//TBD             e.add(value);
	//TBD             }
	//TBD        }
        if (LOG.isTraceEnabled()) LOG.trace("addWrite -- EXIT");
    }

    public boolean hasWrite() {
        return writeOrdering.size() > 0;
    }

    public int writeSize() {
       return writeOrdering.size();
    }

   public synchronized void addDelete(final EsgynMDelete delete) {
        if (LOG.isInfoEnabled()) LOG.info("addDelete -- ENTRY:" 
					  + " writeOrdering: " + writeOrdering
					  + " writeOrdering.size(): " + writeOrdering.size()
					  );

        WriteAction waction;
        WALEdit e1  = new WALEdit();
        long now = TrxEnvironmentEdgeManager.currentTime();
	//TBD        updateLatestTimestamp(delete.getFamilyCellMap().values(), now);
        if (delete.getTimeStamp() == HConstants.LATEST_TIMESTAMP) {
            delete.setTimeStamp(now);
        }
        deletes.add(delete);

	ListIterator<WriteAction> writeOrderIter = writeOrdering.listIterator();
	writeOrderIter.add(waction = new WriteAction(delete));

	//TBD        if (this.earlyLogging) {
	//TBD           for (Cell value : waction.getCells()) {
	//TBD               KeyValue kv = KeyValue.cloneAndAddTags(value, tagList);
	//TBD               e1.add(kv);
	//TBD               e.add(kv);
	//TBD           }
	//TBD           try {
	//TBD           //long txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
	//TBD           //         e1, new ArrayList<UUID>(), TrxEnvironmentEdgeManager.currentTimeMillis(), this.tabledescriptor,
	//TBD           //         this.logSeqId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
	//TBD           final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), this.regionInfo.getTable(), TrxEnvironmentEdgeManager.currentTime());;
	//TBD      	   long txid = this.tHLog.append(this.tabledescriptor,this.regionInfo, wk , e1,
	//TBD      			 this.logSeqId, false, null);
	//TBD                    //if (LOG.isInfoEnabled()) LOG.info("Trafodion Recovery: Y00 write edit to HLOG during delete with txid " + txid + " ts flush id " + this.flushTxId);
	//TBD           if (txid > this.flushTxId) this.flushTxId = txid; // save the log txid into TS object, later sync on largestSeqid during phase 1
	//TBD           }
	//TBD           catch (IOException exp1) {
	//TBD           LOG.info("TrxRegionEndpoint coprocessor addDelete writing to HLOG for early logging: Threw an exception");
	//TBD           }
	//TBD       }
	//TBD       else {
	//TBD           for (Cell value : waction.getCells()) {
	//TBD               KeyValue kv = KeyValue.cloneAndAddTags(value, tagList);
	//TBD               e.add(kv);
	//TBD           }
	//TBD       }
    
       if (LOG.isTraceEnabled()) LOG.trace("addDelete -- EXIT");
    }

    /*
    public synchronized void applyDeletes(final List<Cell> input, final long minTime, final long maxTime) {
        if (deletes.isEmpty()) {
            return;
        }
        for (ListIterator<Cell> itr = input.listIterator(); itr.hasNext();) {
            Cell included = applyDeletes(itr.next(), minTime, maxTime);
            if (null == included) {
                itr.remove();
            }
        }
    }

   public synchronized Cell applyDeletes(final Cell kv, final long minTime, final long maxTime) {
        if (deletes.isEmpty()) {
            return kv;
        }

        ListIterator<EsgynMDelete> deletesIter = null;

        for (deletesIter = deletes.listIterator();
             deletesIter.hasNext();) {
            EsgynMDelete delete = deletesIter.next();

            // Skip if delete should not apply
            if (!Bytes.equals(kv.getRow(), delete.getRow()) || kv.getTimestamp() > delete.getTimeStamp()
                    || delete.getTimeStamp() > maxTime || delete.getTimeStamp() < minTime) {
                continue;
           }

            // Whole-row delete
            if (delete.isEmpty()) {
                return null;
            }

            for (Entry<byte[], List<Cell>> deleteEntry : delete.getFamilyCellMap().entrySet()) {
                byte[] family = deleteEntry.getKey();
                if (Bytes.equals(kv.getFamilyArray(), family)) {
		    List<Cell> familyDeletes = deleteEntry.getValue();
		    if (familyDeletes == null) {
			return null;
		    }
		    for (Cell keyDeletes : familyDeletes) {
			byte[] deleteQualifier = keyDeletes.getQualifierArray();
			byte[] kvQualifier = kv.getQualifierArray();
			if (keyDeletes.getTimestamp() > kv.getTimestamp() && Bytes.equals(deleteQualifier, kvQualifier)) {
			    return null;
			}
		    }
		}
            }
        }

        return kv;
    }
    */

    public void clearState() {

      clearTransactionsToCheck();
      clearWriteOrdering();
      clearScanRange();
      clearDeletes();
      clearTags();
      clearWALEdit();
    }

    public void clearTransactionsToCheck() {
        transactionsToCheck.clear();
    }

    public void clearWriteOrdering() {
        writeOrdering.clear();
    }

    public void clearScanRange() {
        scans.clear();
    }

    public void clearDeletes() {
        deletes.clear();
    }

    public void clearTags() {
        tagList.clear();
    }

    public void clearWALEdit() {
      if (e.size() > 0) {

        DataInputBuffer in = new DataInputBuffer();
        try {
          e.readFields(in);
	  //TBD          e.getCells().clear();
        } catch (java.io.EOFException eeof) { 
          // DataInputBuffer was empty, successfully emptied kvs
        } catch (Exception e) { 
          if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactionState clearWALEdit:  Clearing WALEdit caught an exception for transaction "+ this.transactionId + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "]" + ": exception " + e.toString());
        } finally {
          try {
            in.close();
          } catch (IOException io) {
          }
        }
      }
      if (e.size() > 0) 
        if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactionState clearWALEdit:  Possible leak with kvs entries in WALEDIT, for transaction "+ this.transactionId + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "], e is " + e.toString());
    }

    public void addTransactionToCheck(final AmpoolTransactionState transaction) {
        transactionsToCheck.add(transaction);
    }

    public synchronized boolean hasConflict() 
    {

        for (AmpoolTransactionState transactionState : transactionsToCheck) {
            try {
		if (hasConflict(transactionState)) {
		    if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactionState hasConflict: Returning true for " 
						      + transactionState.toString() 
						      + ", region: " + regionInfo.getRegionNameAsString()
						      );
		    return true;
		}
	    } catch (Exception e) {
                // We are unable to ascertain if we have had a conflict with 
                // the rows we are trying to modify.  We will return true, 
                // indicating that we can not allow the pending changes 
                // for this transaction to commit.
                LOG.error("AmpoolTransactionState hasConflict: Returning true. Caught exception" 
			  + ", transaction: " + transactionState.toString() 
			  + ", regionInfo: " + regionInfo.getRegionNameAsString() 
			  , e
			  );
                return true;
            }
        }
        return false;
    }

    private boolean hasConflict(final AmpoolTransactionState checkAgainst) throws Exception{
        if (checkAgainst.getStatus().equals(TransactionState.Status.ABORTED)) {
            return false; // Cannot conflict with aborted transactions
        }

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = checkAgainst.writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
          WriteAction otherUpdate = writeOrderIter.next();

          try {
            byte[] row = otherUpdate.getRow();
            if (row == null) {
              LOG.warn("AmpoolTransactionState hasConflict: row is null" 
		       + ", Transaction: " + this.toString() 
		       + ", checkAgainst Transaction: " + checkAgainst.toString() 
		       );
            }
            if (this.getTransactionId() == checkAgainst.getTransactionId())
            {
              if (LOG.isInfoEnabled()) LOG.info("AmpoolTransactionState hasConflict: Continuing" 
						+ ", this Transaction: " + this.toString() 
						+ " is the same as the against Transaction: " + checkAgainst.toString()
						);
              continue;
            }
            if (this.scans != null && !this.scans.isEmpty()) {
              ListIterator<ScanRange> scansIter = null;

              for (scansIter = this.scans.listIterator();
                   scansIter.hasNext();) {
                ScanRange scanRange = scansIter.next();

                if (scanRange == null)
                    if (LOG.isInfoEnabled()) LOG.info("Transaction [" + this.toString() + "] scansRange is null");
                if (scanRange != null && scanRange.contains(row)) {
                    LOG.warn("Transaction [" + this.toString() + "] has scan which conflicts with ["
                            + checkAgainst.toString() + "]: region [" + regionInfo.getRegionNameAsString()
                            + "], scanRange[" + scanRange.toString() + "] ,row[" + Bytes.toStringBinary(row) + "]");
                    return true;
                }
                //else {
                //    LOG.info("Transaction [" + this.toString() + "] has scanRange checked against ["
                //            + checkAgainst.toString() + "]: region [" + regionInfo.getRegionNameAsString()
                //            + "], scanRange[" + scanRange.toString() + "] ,row[" + Bytes.toStringBinary(row) + "]");
                //}
              }
            }
            else {
             if (this.scans == null)
               LOG.info("Transaction [" + this.toString() + "] scans was equal to null");
             else 
               LOG.info("Transaction [" + this.toString() + "] scans was empty ");
            }
        }
          catch (Exception e) {
              LOG.warn("AmpoolTransactionState hasConflict: Unable to get row - this Transaction [" + this.toString() + "] checkAgainst Transaction ["
                       + checkAgainst.toString() + "] " + " Exception: " + e);
              throw e;
          }
        }
        return false;
    }

    public WALEdit getEdit() {
       return e;
    }


    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("[transactionId: ");
        result.append(transactionId);
        result.append(" status: ");
        result.append(status.name());
        result.append(" scan Size: ");
        result.append(scans.size());
        result.append(" write Size: ");
        result.append(getWriteOrdering().size());
        result.append(" startSQ: ");
        result.append(startSequenceNumber);
        if (sequenceNumber != null) {
            result.append(" commitedSQ:");
            result.append(sequenceNumber);
        }
        result.append("]");

        return result.toString();
    }

    public synchronized void addScan(final Scan scan) {
        ScanRange scanRange = new ScanRange(scan.getStartRow(), scan.getStopRow());

        if (LOG.isInfoEnabled()) LOG.info(String.format("Adding scan for transaction [%s], from startRow [%s] to endRow [%s]", transactionId,
            scanRange.startRow == null ? "null" : Bytes.toStringBinary(scanRange.startRow), scanRange.endRow == null ? "null"
                    : Bytes.toStringBinary(scanRange.endRow)));
        scans.add(scanRange);
    }

    /**
     * Get deletes.
     * 
     * @return deletes
     */
    public synchronized List<Delete> getDeletes() {
        return deletes;
    }

    /**
     * Get a scanner to go through the puts and deletes from this transaction. Used to weave together the local trx puts
     * with the global state.
     * 
     * @return scanner
     */
    //TBD    public KeyValueScanner getScanner(final Scan scan) {
    //TBD        return new TransactionScanner(scan);
    //TBD    }

    /* TBD
    private synchronized Cell[] getAllCells(final Scan scan) {
        //if (LOG.isInfoEnabled()) LOG.info("getAllCells -- ENTRY");
        List<Cell> kvList = new ArrayList<Cell>();

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
            WriteAction action = writeOrderIter.next();
            byte[] row = action.getRow();
            List<Cell> kvs = action.getCells();

            if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
                    && Bytes.compareTo(row, scan.getStartRow()) < 0) {
                continue;
            }
            if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
                    && Bytes.compareTo(row, scan.getStopRow()) > 0) {
                continue;
            }

	    if (!scan.hasFamilies()) {
            kvList.addAll(kvs);
		continue;
	    }
      // Pick only the Cell's that match the 'scan' specifications
      for (Cell lv_kv : kvs) {
    byte[] lv_kv_family = lv_kv.getFamilyArray();
    Map<byte [], NavigableSet<byte []>> lv_familyMap = scan.getFamilyMap();
    NavigableSet<byte []> set = lv_familyMap.get(lv_kv_family);
    if (set == null || set.size() == 0) {
          kvList.add(lv_kv);
        continue;
    }
    if (set.contains(lv_kv.getQualifierArray())) {
        kvList.add(lv_kv);
    }
      }
        }

        if (LOG.isInfoEnabled()) LOG.info("getAllCells -- EXIT kvList size = " + kvList.size());
        return kvList.toArray(new Cell[kvList.size()]);
    }
    
     private synchronized KeyValue[] getAllKVs(final Scan scan) {
        //if (LOG.isInfoEnabled()) LOG.info("getAllKVs -- ENTRY");
        List<KeyValue> kvList = new ArrayList<KeyValue>();

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
            WriteAction action = writeOrderIter.next();
            byte[] row = action.getRow();
            List<KeyValue> kvs = action.getKeyValues();

            if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
                    && Bytes.compareTo(row, scan.getStartRow()) < 0) {
                continue;
            }
            if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
                    && Bytes.compareTo(row, scan.getStopRow()) > 0) {
                continue;
            }

      if (!scan.hasFamilies()) {
            kvList.addAll(kvs);
    continue;
      }

	    // Pick only the Cell's that match the 'scan' specifications
		Map<byte [], NavigableSet<byte []>> lv_familyMap = scan.getFamilyMap();
	    for (KeyValue lv_kv : kvs) {
		byte[] lv_kv_family = lv_kv.getFamily();
		NavigableSet<byte []> set = lv_familyMap.get(lv_kv_family);
		if (set == null || set.size() == 0) {
					kvList.add(lv_kv);
		    continue;
		}
		if (set.contains(lv_kv.getQualifier())) {
		    kvList.add(lv_kv);
		}
	    }
        }

        if (LOG.isInfoEnabled()) LOG.info("getAllKVs -- EXIT kvList size = " + kvList.size());
        return kvList.toArray(new KeyValue[kvList.size()]);
    }
    */

    //TBD    private synchronized int getTransactionSequenceIndex(final Cell kv) {
    //TBD        ListIterator<WriteAction> writeOrderIter = null;
    //TBD        int i = 0;
    //TBD
    //TBD        for (writeOrderIter = writeOrdering.listIterator();
    //TBD             writeOrderIter.hasNext();) {
    //TBD            i++;
    //TBD            WriteAction action = writeOrderIter.next();
    //TBD            if (isKvInPut(kv, action.getPut())) {
    //TBD                return i;
    //TBD            }
    //TBD            if (isKvInDelete(kv, action.getDelete())) {
    //TBD                return i;
    //TBD            }
    //TBD        }
    //TBD        throw new IllegalStateException("Can not find kv in transaction writes");
    //TBD    }


    /**
     * Scanner of the puts and deletes that occur during this transaction.
     * 
     * @author clint.morgan
     */
//TBD    public class TransactionScanner extends KeyValueListScanner implements InternalScanner {
//TBD
//TBD        private ScanQueryMatcher matcher;
//TBD
//TBD        TransactionScanner(final Scan scan) {
//TBD            super(new KeyValue.KVComparator() {            	
//TBD                @Override
//TBD                public int compare(final Cell left, final Cell right) {
//TBD                    int result = super.compare(left, right);
//TBD                    if (result != 0) {
//TBD                        return result;
//TBD                    }
//TBD                    if (left == right) {
//TBD                        return 0;
//TBD                    }
//TBD                    int put1Number = getTransactionSequenceIndex(left);
//TBD                    int put2Number = getTransactionSequenceIndex(right);
//TBD                    return put2Number - put1Number;
//TBD                }
//TBD            }, getAllKVs(scan));
//TBD           
//TBD            // We want transaction scanner to always take priority over store
//TBD            // scanners.
//TBD            super.setSequenceID(Long.MAX_VALUE);
//TBD            
//TBD            //Store.ScanInfo scaninfo = new Store.ScanInfo(null, 0, 1, HConstants.FOREVER, false, 0, Cell.COMPARATOR);
//TBD            ScanInfo scaninfo = new ScanInfo(null, 0, 1, HConstants.FOREVER,KeepDeletedCells.FALSE, 0, KeyValue.COMPARATOR);
//TBD           
//TBD           
//TBD        }
//TBD
//TBD        /**
//TBD         * Get the next row of values from this transaction.
//TBD         * 
//TBD         * @param outRow
//TBD         * @param limit
//TBD         * @return true if there are more rows, false if scanner is done
//TBD	 */
//TBD        @Override
//TBD        public synchronized boolean next(final List<Cell> outRow, final int limit) throws IOException {          	
//TBD            Cell peeked = this.peek();            
//TBD            if (peeked == null) {            
//TBD                close();
//TBD                return false;
//TBD            }
//TBD            
//TBD            matcher.setRow(peeked.getRowArray(), peeked.getRowOffset(), peeked.getRowLength());
//TBD            
//TBD            KeyValue kv;
//TBD            List<Cell> results = new ArrayList<Cell>();
//TBD            LOOP: while ((kv = this.peek()) != null) {
//TBD                ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
//TBD                switch (qcode) {
//TBD                    case INCLUDE:
//TBD                        Cell next = this.next();
//TBD                        results.add(next);
//TBD                        if (limit > 0 && results.size() == limit) {
//TBD                            break LOOP;
//TBD                        }
//TBD                        continue;
//TBD
//TBD                    case DONE:
//TBD                        // copy jazz
//TBD                        outRow.addAll(0, results);
//TBD                        return true;
//TBD
//TBD                    case DONE_SCAN:
//TBD                        close();
//TBD
//TBD                        // copy jazz
//TBD                        outRow.addAll(0, results);
//TBD
//TBD                        return false;
//TBD
//TBD                    case SEEK_NEXT_ROW:
//TBD                        this.next();
//TBD                        break;
//TBD
//TBD                    case SEEK_NEXT_COL:
//TBD                        this.next();
//TBD                        break;
//TBD
//TBD                    case SKIP:
//TBD                        this.next();
//TBD                        break;
//TBD
//TBD                    default:
//TBD                        throw new RuntimeException("UNEXPECTED");
//TBD                }
//TBD            }
//TBD
//TBD            if (!results.isEmpty()) {
//TBD                // copy jazz
//TBD                outRow.addAll(0, results);
//TBD                return true;
//TBD            }
//TBD
//TBD            // No more keys
//TBD            close();
//TBD            return false;
//TBD        }
//TBD        @Override
//TBD        public synchronized boolean next(final List<Cell> results) throws IOException {
//TBD          return next(results, -1);
//TBD        }
//TBD     }
//TBD

    private synchronized boolean isKvInPut(final Cell p_cell, final EsgynMPut put) {
	if (put == null) {
	    return false;
	}

	for (Object put_cell: put.getFamilyCellMap().values()) {
	    if ((Cell) put_cell == p_cell) {
		return true;
	    }
	}

        return false;
    }

    /* TBD
    private synchronized boolean isKvInDelete(final Cell p_cell, final EsgynMDelete delete) {
	if (delete == null) {
	    return false;
	}

	for (Object delete_cell: delete.getFamilyCellMap().values()) {
	    if ((Cell) delete_cell == p_cell) {
		return true;
	    }
	}

        return false;
    }
    */
     
    /**
     * Get the puts and deletes in transaction order.
     * 
     * @return Return the writeOrdering.
     */
    public List<WriteAction> getWriteOrdering() {
        return writeOrdering;
    }    

    /*
     * Get the puts and deletes in transaction order.
     * 
     * @return Return the writeOrdering as an iterator.
     */
    public ListIterator<WriteAction> getWriteOrderingIter() {
        return writeOrdering.listIterator();
    }
 
    /**
     * Simple wrapper for Put and Delete since they don't have a common enough interface.
     */
    public class WriteAction {

        private EsgynMPut put;
        private EsgynMDelete delete;

        public WriteAction(final EsgynMPut put) {
            if (null == put) {
                throw new IllegalArgumentException("WriteAction requires a Put or a Delete.");
            }
            this.put = put;
        }

        public WriteAction(final EsgynMDelete delete) {
            if (null == delete) {
                throw new IllegalArgumentException("WriteAction requires a Put or a Delete.");
            }
            this.delete = delete;
        }

        public EsgynMPut getPut() {
            return put;
        }

        public EsgynMDelete getDelete() {
            return delete;
        }

        public synchronized byte[] getRow() {
            if (put != null) {
                return put.getRowKey();
            } else if (delete != null) {
                return delete.getRowKey();
            }
            throw new IllegalStateException("WriteAction is invalid");
        }

	//TBD        synchronized List<Cell> getCells() {
	//TBD            List<Cell> edits = new ArrayList<Cell>();
	//TBD            Collection<List<Cell>> kvsList;
	//TBD
	//TBD            if (put != null) {
	//TBD                kvsList = put.getFamilyCellMap().values();
	//TBD            } else if (delete != null) {
	//TBD                if (delete.getFamilyCellMap().isEmpty()) {
	//TBD                    // If whole-row delete then we need to expand for each
	//TBD                    // family
	//TBD                    kvsList = new ArrayList<List<Cell>>(1);
	//TBD                    for (byte[] family : tabledescriptor.getFamiliesKeys()) {
	//TBD                        Cell familyDelete = new KeyValue(delete.getRow(), family, null, delete.getTimeStamp(),
	//TBD                                KeyValue.Type.DeleteFamily);
	//TBD                        kvsList.add(Collections.singletonList(familyDelete));
	//TBD                    }
	//TBD                } else {
	//TBD                    kvsList = delete.getFamilyCellMap().values();
	//TBD                }
	//TBD            } else {
	//TBD                throw new IllegalStateException("WriteAction is invalid");
	//TBD            }
	//TBD
	//TBD            for (List<Cell> kvs : kvsList) {
	//TBD                for (Cell kv : kvs) {
	//TBD                    edits.add(kv);
	//TBD                    //if (LOG.isDebugEnabled()) LOG.debug("Trafodion Recovery:   " + regionInfo.getRegionNameAsString() + " create edits for transaction: "
	//TBD                    //               + transactionId + " with Op " + kv.getType());
	//TBD                }
	//TBD            }
	//TBD            return edits;
	//TBD        }
        

    }

    public Set<AmpoolTransactionState> getTransactionsToCheck() {
      return transactionsToCheck;
    }
    
    /**
     * before adding a put to the writeOrdering list, check whether we have deleted a row with the
     * same key.  If true remove the delete before adding the put
     */
    public void removeDelBeforePut(EsgynMPut put) {
        if (LOG.isInfoEnabled()) LOG.info("removeDelBeforePut put : " + put);
        byte[] putRow = put.getRow();
        KeyValue kv;

        for(WriteAction wa : getWriteOrdering()) {
           EsgynMDelete delete = wa.getDelete();
           if (delete != null){
              byte[] delRow = delete.getRow();
              if (LOG.isInfoEnabled()) LOG.info("putRow : " + Bytes.toString(putRow)
                             + " : delRow : " + Bytes.toString(delRow));

              if (Arrays.equals(putRow, delRow) ) {
               	if (LOG.isInfoEnabled()) LOG.info("Removing delete from writeOrdering for row : "
                                     + Bytes.toString(delRow));
//                for (Cell value : wa.getCells()) {
//                   kv = KeyValue.cloneAndAddTags(value, tagList);
//                   e.remove(kv);
//                }
               	deletes.remove(delete);
               	writeOrdering.remove(wa);
              }
           }        	
        }
    }
}
