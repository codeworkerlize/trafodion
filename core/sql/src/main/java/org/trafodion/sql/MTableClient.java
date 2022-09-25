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

package org.trafodion.sql;
import org.trafodion.sql.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.exceptions.*;
import io.ampool.monarch.table.client.*;
import io.ampool.monarch.table.client.coprocessor.AggregationClient;

import io.esgyn.client.*;
import io.esgyn.coprocessor.*;

/*
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.transactional.RMInterface;
import org.apache.hadoop.hbase.client.transactional.TransactionalAggregationClient;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
*/

import org.apache.log4j.Logger;

// H98 coprocessor needs
import java.util.*;
import java.util.UUID;
import java.security.InvalidParameterException;

public class MTableClient {
   private static final int GET_ROW = 1;
   private static final int BATCH_GET = 2;
   private static final int SCAN_FETCH = 3;
   private boolean useTRex;
   private boolean useTRexScanner;
   private static boolean envUseTRex;
   private static boolean envUseTRexScanner;
   private String tableName;

   private Scanner scanner = null;
   private ScanHelper scanHelper = null;
   Row[] getResultSet = null;
    //    MTable table = null;
    MRMInterface table = null;
   //ByteArrayList coprocAggrResult = null;
   //private boolean writeToWAL = false;
   int numRowsCached = 1;
   int numColsInScan = 0;
   int[] cellsValLen = null;
   int[] cellsValOffset = null;
   byte[][] cellsValBuffer = null;
   byte[][] rowIDs = null;
   long[] cellsTimestamp = null;
   int[] cellsPerRow = null;
   byte[][] cellsName = null;
   byte[][] rowKeys = null;
   byte[] rowKey = null;
   static ExecutorService executorService = null;
   Future future = null;
   boolean preFetch = false;
   int fetchType = 0;
   long jniObject = 0;

   class ScanHelper implements Callable {
      public Row[] call() throws Exception {
         return scanner.next(numRowsCached);
      }
   }
    
   static Logger logger = Logger.getLogger(MTableClient.class.getName());;
/*
   public boolean setWriteBufferSize(long writeBufferSize) throws IOException {
      if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::setWriteBufferSize, size  : " + writeBufferSize);
       table.setWriteBufferSize(writeBufferSize);
       return true;
     }
    public long getWriteBufferSize() {
       if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::getWriteBufferSize, size return : " + table.getWriteBufferSize());
       return table.getWriteBufferSize();
    }
   public boolean setWriteToWAL(boolean v) {
      if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::setWriteToWALL, size  : " + v);
       writeToWAL = v;
       return true;
     }
*/ 
   public boolean init(String tblName,
              boolean useTRex, boolean bSynchronized) throws IOException {
      if (logger.isDebugEnabled()) 
         logger.debug("Enter MTableClient::init, tableName: " + tblName);
      this.useTRex = useTRex;
      tableName = tblName;

      if ( !this.useTRex ) {
         this.useTRexScanner = false;
      }
      else {
      // If the parameter useTRex is false, then do not go thru this logic
         this.useTRex = envUseTRex;
         this.useTRexScanner = envUseTRexScanner;
      }

      //table = MClientCacheFactory.getAnyInstance().getTable(tblName);
      table = new MRMInterface(tblName, false);
       if (logger.isDebugEnabled())
          logger.debug("Exit MTableClient::init, table object: " + table);
       return true;
   }

   String getTableName() {
      return tableName;
   }

   String getMTableName() {
      if (table == null)
         return null;
      else
         return getTableName();
   }

   public boolean startScan(long transID, byte[] startRow, byte[] stopRow,
                                 Object[]  columns, long timestamp,
                                 boolean cacheBlocks,  boolean smallScanner, int numCacheRows,
                                 Object[] cellsNameToFilter, 
                                 Object[] compareOpList, 
                                 Object[] cellsValueToCompare,
                                 float samplePercent,
                                 boolean inPreFetch,
                                 boolean useSnapshotScan,
                                 int snapTimeout,
                                 String snapName,
                                 String tmpLoc,
                                 int espNum,
                                 int versions,
                                 long minTS,
                                 long maxTS,
                                 String hbaseAuths)
           throws IOException {
     if (logger.isTraceEnabled()) logger.trace("Enter startScan() " + tableName + " txid: " + transID+ " CacheBlocks: " + cacheBlocks + " numCacheRows: " + numCacheRows + " Bulkread: " + useSnapshotScan);

     Scan scan;

     if (startRow != null && (startRow.toString() == "" || startRow.length == 0))
       startRow = null;
     if (stopRow != null && (stopRow.toString() == "" || stopRow.length == 0))
       stopRow = null;

     if (startRow != null && stopRow != null)
       scan = new Scan(startRow, stopRow);
     else
       scan = new Scan();

     if (versions != 0) {
        if (versions == -1)
           scan.setMaxVersions(false);
        else if (versions == -2) {
           scan.setMaxVersions(false);
           //scan.setRaw(true);
            columns = null;
        }
        else if (versions > 0) {
           scan.setMaxVersions(versions, false);
        }
     }
/*
     if ((minTS != -1) && (maxTS != -1))
        scan.setTimeRange(minTS, maxTS);
     if (cacheBlocks == true) 
        scan.setCacheBlocks(true);
     else
        scan.setCacheBlocks(false);
     scan.setSmall(smallScanner);

 */         
     scan.setClientQueueSize(numCacheRows);
/*
     scan.enableBatchMode();
     scan.setBatchSize(numCacheRows);
*/

     numRowsCached = numCacheRows;
     if (columns != null) {
       numColsInScan = columns.length;
       for (int i = 0; i < columns.length ; i++) {
         byte[] col = (byte[]) columns[i];
         scan.addColumn(col);
       }
     }
     else
       numColsInScan = 0;
  
/*
 *  Filters are supported, but FilterList is not supported - 
 *  No support for RandomRowFilter, SingleColumnValueFilter
     if (cellsNameToFilter != null) {
       FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

       for (int i = 0; i < cellsNameToFilter.length; i++) {
         byte[] colName = (byte[])cellsNameToFilter[i];
         byte[] coByte = (byte[])compareOpList[i];
         byte[] colVal = (byte[])cellsValueToCompare[i];

         if ((coByte == null) || (colVal == null)) {
           return false;
         }

         String coStr = new String(coByte);
         CompareOp co = CompareOp.valueOf(coStr);

         SingleColumnValueFilter filter1 = 
             new SingleColumnValueFilter(getFamily(colName), getName(colName), 
                 co, colVal);
         list.addFilter(filter1);
       }

       if (samplePercent > 0.0f)
         list.addFilter(new RandomRowFilter(samplePercent));
       scan.setFilter(list);
     } else if (samplePercent > 0.0f) {
       scan.setFilter(new RandomRowFilter(samplePercent));
     }

     if (hbaseAuths != null) {
        List<String> listOfHA = Arrays.asList(hbaseAuths);
        Authorizations auths = new Authorizations(listOfHA);

        scan.setAuthorizations(auths);

        System.out.println("scan hbaseAuths " + hbaseAuths);
        System.out.println("listOfHA " + listOfHA);

        // System.out.println("hbaseAuths " + hbaseAuths);
        // System.out.println("listOfHA " + listOfHA);
     }

     if (!useSnapshotScan || transID != 0)
     {
       if (useTRexScanner && (transID != 0)) {
         scanner = table.getScanner(transID, scan);
       } else {
         scanner = table.getScanner(scan);
       }
       if (logger.isTraceEnabled()) logger.trace("startScan(). After getScanner. Scanner: " + scanner);
     }
     else
     {
       snapHelper = new SnapshotScanHelper(table.getConfiguration(), tmpLoc,snapName);

       if (logger.isTraceEnabled()) 
         logger.trace("[Snapshot Scan] MTableClient.startScan(). useSnapshotScan: " + useSnapshotScan + 
                      " espNumber: " + espNum + 
                      " tmpLoc: " + snapHelper.getTmpLocation() + 
                      " snapshot name: " + snapHelper.getSnapshotName());
       
       if (!snapHelper.snapshotExists())
         throw new Exception ("Snapshot " + snapHelper.getSnapshotName() + " does not exist.");

       snapHelper.createTableSnapshotScanner(snapTimeout, 5, espNum, scan);
       if (scanner==null)
         throw new Exception("Cannot create Table Snapshot Scanner");
     }
    
     if (useSnapshotScan)
        preFetch = false;
     else
*/
     if (table == null) 
        throw new IOException("Table doesn't exist");
     scanner = table.getScanner(scan);
     //preFetch = inPreFetch;
     preFetch = false;
     if (preFetch)
     {
       scanHelper = new ScanHelper(); 
            future = executorService.submit(scanHelper);
     }
          fetchType = SCAN_FETCH;
     if (logger.isTraceEnabled()) logger.trace("Exit startScan().");
     return true;
   }

   public Row  startGetHelper(byte[] rowID, 
                     Object[] columns, long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGetHelper(" + tableName + 
              " #cols: " + ((columns == null) ? 0:columns.length ) +
              " rowID: " + new String(rowID));
      Get get = new Get(rowID);
      if (columns != null) {
         for (int i = 0; i < columns.length; i++) {
            byte[] col = (byte[]) columns[i];
            get.addColumn(col);
         }
         numColsInScan = columns.length;
      }
      else
         numColsInScan = 0;
         
      return table.get(get);
   }

    public Row  startGetHelper_Transactional(long transID, byte[] rowID, 
                     Object[] columns, long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGetHelper_Transactional(" + tableName + 
              " #cols: " + ((columns == null) ? 0:columns.length ) +
              " rowID: " + new String(rowID));

      EsgynMGet get = new EsgynMGet(rowID);
      if (columns != null) {
         for (int i = 0; i < columns.length; i++) {
            byte[] col = (byte[]) columns[i];
            get.addColumn(col);
         }
         numColsInScan = columns.length;
      }
      else
         numColsInScan = 0;
         
      return table.get(transID, get);
   }

   public int  startGet(long transID, byte[] rowID, 
                     Object[] columns, long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGet(" + tableName + 
              " #cols: " + ((columns == null) ? 0:columns.length ) +
              " rowID: " + new String(rowID));

      fetchType = GET_ROW;
      rowKey = rowID;

      Row getResult;

      if (useTRex && (transID != 0)) 
	  getResult = startGetHelper_Transactional(transID, 
						   rowID,
						   columns, 
						   timestamp);
       else 
	  getResult = startGetHelper(rowID,
				     columns, 
				     timestamp);

      if (getResult == null ) {
         setJavaObject(jniObject);
         return 0;
      }
      if (logger.isTraceEnabled()) 
         logger.trace("startGet, result: " + getResult);
      pushRowsToJni(getResult);
      return 1;
   }
/*
   // The TransactionalTable class is missing the batch get operation,
   // so work around it.
   private Result[] batchGet(long transactionID, List<Get> gets)
         throws IOException {
      if (logger.isTraceEnabled()) logger.trace("Enter batchGet(multi-row) " + tableName);
      Result [] results = new Result[gets.size()];
      int i=0;
      for (Get g : gets) {
         Result r = table.get(transactionID, g);
         results[i++] = r;
      }
      return results;
   }
*/
   public int startGet(long transID, Object[] rows,
         Object[] columns, long timestamp)
                        throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGet(multi-row) " + tableName);

      List<Get> listOfGets = new ArrayList<Get>();
      if (rowKeys == null || rows.length > rowKeys.length)
         rowKeys = new byte[rows.length][];
      for (int i = 0; i < rows.length; i++) {
         byte[] rowID = (byte[])rows[i]; 
         Get get = new Get(rowID);
         rowKeys[i] = rowID;
         listOfGets.add(get);
         if (columns != null)
         {
            for (int j = 0; j < columns.length; j++ ) {
               byte[] col = (byte[])columns[j];
               get.addColumn(col);
            }
         }
      }
      if (columns != null)
         numColsInScan = columns.length;
      else
         numColsInScan = 0;

      /*
      if (useTRex && (transID != 0)) {
         getResultSet = batchGet(transID, listOfGets);
         fetchType = BATCH_GET; 
      } else 
      */
      {
         getResultSet = table.get(listOfGets);
         fetchType = BATCH_GET;
      }
      if (getResultSet != null && getResultSet.length > 0) {
         pushRowsToJni(getResultSet);
         return getResultSet.length;
      }
      else {
         setJavaObject(jniObject);
         return 0;
      }
   }

   public int getRows(long transID, short rowIDLen, Object rowIDs,
         Object[] columns) throws IOException {

      if (logger.isTraceEnabled())
         logger.trace("Enter getRows " + tableName);

      ByteBuffer bbRowIDs = (ByteBuffer)rowIDs;
      List<Get> listOfGets = new ArrayList<Get>();
      short numRows = bbRowIDs.getShort();
      if (rowKeys == null || numRows > rowKeys.length)
         rowKeys = new byte[numRows][];
      short actRowIDLen ;
      byte rowIDSuffix;
      byte[] rowID;

      for (int i = 0; i < numRows; i++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         rowKeys[i] = rowID;
         bbRowIDs.get(rowID, 0, actRowIDLen);
         Get get = new Get(rowID);
         listOfGets.add(get);
         if (columns != null) {
            for (int j = 0; j < columns.length; j++ ) {
               byte[] col = (byte[])columns[j];
               get.addColumn(col);
            }
         }
      }
      if (columns != null)
         numColsInScan = columns.length;
      else
         numColsInScan = 0;
/*
      if (useTRex && (transID != 0)) {
         getResultSet = batchGet(transID, listOfGets);
         fetchType = BATCH_GET; 
      } else
*/
      {
         getResultSet = table.get(listOfGets);
         fetchType = BATCH_GET;
      }
      if (getResultSet.length != numRows)
         throw new IOException("Number of rows retunred is not equal to requested number of rows");
      pushRowsToJni(getResultSet);
      return getResultSet.length;
   }

   public int fetchRows() throws IOException, 
         InterruptedException, ExecutionException {
      int rowsReturned = 0;

      if (logger.isTraceEnabled())
         logger.trace("Enter fetchRows(). Table: " + tableName);
      if (getResultSet != null) {
         rowsReturned = pushRowsToJni(getResultSet);
         getResultSet = null;
         return rowsReturned;
      }
      else
      {
         if (scanner == null) {
            throw new IOException("fetchRows() called before scanOpen().");
         }
         Row[] result = null;
         if (preFetch)
         {
            result = (Row[])future.get();
            rowsReturned = pushRowsToJni(result);
            future = null;
            if ((rowsReturned <= 0 || rowsReturned < numRowsCached))
               return rowsReturned;
                                future = executorService.submit(scanHelper);
         }
         else
         {
            result = scanner.next(numRowsCached);
            rowsReturned = pushRowsToJni(result);
         }
         return rowsReturned;
      }
   }

   protected int pushRowsToJni(Row[] result) 
         throws IOException {
      if (result == null || result.length == 0)
         return 0; 
      int rowsReturned = result.length;
 // This FOR loop should be removed when ampool fixes the result array length issue
      if (fetchType == SCAN_FETCH) { 
      for (int rowNum = 0; rowNum < result.length ; rowNum++) {
          if (result[rowNum] == null) {
              rowsReturned = rowNum;
              break;
          }
        }
      }
      int numTotalCells = 0;

      if (numColsInScan == 0) {
         for (int i = 0; i < rowsReturned; i++) {   
            numTotalCells += result[i].size();
         }
      }
      else
      // There can be maximum of 2 versions per kv
      // So, allocate place holder to keep cell info
      // for that many KVs
         numTotalCells = 2 * rowsReturned * numColsInScan;
      int numColsReturned;

      if (cellsValBuffer == null ||
          (cellsValBuffer != null && numTotalCells > cellsValBuffer.length))
      {
         cellsValBuffer = new byte[numTotalCells][];
         cellsName = new byte[numTotalCells][];
         cellsValLen = new int[numTotalCells];
         cellsValOffset = new int[numTotalCells];
         cellsTimestamp = new long[numTotalCells];
      }
  
      if (cellsPerRow == null ||
           (cellsPerRow != null && rowsReturned > cellsPerRow.length)) {
         cellsPerRow = new int[rowsReturned];
         rowKeys = new byte[rowsReturned][];
      }
      List<Cell> cells;         
      Cell cell;
      int cellNum = 0;
      boolean colFound = false;
      for (int rowNum = 0; rowNum < rowsReturned ; rowNum++)
      {
         if (result[rowNum] == null) {
            cellsPerRow[rowNum] = 0;
            continue;
         } 
         cells = result[rowNum].getCells();
         numColsReturned = cells.size();
         if ((cellNum + numColsReturned) > numTotalCells)
            throw new IOException("Insufficient cell array pre-allocated");
         cellsPerRow[rowNum] = numColsReturned;
         rowKeys[rowNum] = result[rowNum].getRowId();
         for (int colNum = 0 ; colNum < numColsReturned ; colNum++, cellNum++) { 
            cell = cells.get(colNum);
            cellsValBuffer[cellNum] = cell.getValueArray();
            cellsValLen[cellNum] = cell.getValueLength();
            cellsValOffset[cellNum] = cell.getValueOffset();
            cellsName[cellNum] = cell.getColumnName();
            cellsTimestamp[cellNum] = result[rowNum].getRowTimeStamp();
            colFound = true;
         }
      }
      int cellsReturned;
      if (colFound)
         cellsReturned = cellNum++;
      else
         cellsReturned = 0;
      if (cellsReturned == 0)
         setResultInfo(jniObject, null, null, null, null, null, rowKeys, cellsPerRow, cellsReturned, rowsReturned);
      else 
         setResultInfo(jniObject, cellsName, cellsValBuffer, cellsValOffset, cellsValLen,  cellsTimestamp, rowKeys, cellsPerRow, cellsReturned, rowsReturned);
      return rowsReturned;   
   }      
   
   protected int pushRowsToJni(Row result) 
         throws IOException {
       
       if (logger.isTraceEnabled()) logger.trace("Enter pushRowsToJNI() ");
      int rowsReturned = 1;
      int numTotalCells;

      if (numColsInScan == 0)
         numTotalCells = result.size();
      else
      // There can be maximum of 2 versions per kv
      // So, allocate place holder to keep cell info
      // for that many KVs
         numTotalCells = 2 * rowsReturned * numColsInScan;
      int numColsReturned;
      List<Cell> cells;
      Cell cell;

      if (cellsValBuffer == null ||
          (cellsValBuffer != null && numTotalCells > cellsValBuffer.length))
      {
         cellsValBuffer = new byte[numTotalCells][];
         cellsValLen = new int[numTotalCells];
         cellsValOffset = new int[numTotalCells];
         cellsName = new byte[numTotalCells][];
         cellsTimestamp = new long[numTotalCells];
      }

     if (cellsPerRow == null ||       
           (cellsPerRow != null && rowsReturned > cellsPerRow.length))
         cellsPerRow = new int[rowsReturned];

     if (rowKeys == null ||
           (rowKeys != null && rowsReturned > rowKeys.length))
         rowKeys = new byte[rowsReturned][];

      cells = result.getCells();
       if (cells == null)
         numColsReturned = 0; 
      else
         numColsReturned = cells.size();
      if ((numColsReturned) > numTotalCells)
         throw new IOException("Insufficient cell array pre-allocated");
      cellsPerRow[0] = numColsReturned;
      for (int colNum = 0 ; colNum < numColsReturned ; colNum++) { 
         cell = cells.get(colNum);
         cellsValBuffer[colNum] = cell.getValueArray();
         cellsValLen[colNum] = cell.getValueLength();
         cellsValOffset[colNum] = cell.getValueOffset();
         cellsName[colNum] = cell.getColumnName();
         cellsTimestamp[colNum] = result.getRowTimeStamp();
      }
      if (numColsReturned == 0)
         setResultInfo(jniObject, null, null, null, null, null, rowKeys, cellsPerRow, numColsReturned, rowsReturned);
      else 
         setResultInfo(jniObject, cellsName, cellsValBuffer, cellsValOffset, cellsValLen, cellsTimestamp, rowKeys, cellsPerRow, numColsReturned, rowsReturned);
      return rowsReturned;   
   }      
   
   public boolean deleteRow(final long transID, byte[] rowID, 
             Object[] columns,
             long timestamp, boolean asyncOperation, String hbaseAuths) throws IOException {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter deleteRow(" + new String(rowID) + ", " + timestamp + ") " 
		      + ", table: " + tableName 
		      + ", asyncOperation: " + asyncOperation
		      );

      final EsgynMDelete del = new EsgynMDelete(rowID);
      if (timestamp != -1)
         del.setTimestamp(timestamp);

      if (columns != null) {
         for (int i = 0; i < columns.length ; i++) {
            byte[] col = (byte[]) columns[i];
            del.addColumn(col);
         }
      }
      if (asyncOperation) {
         future = executorService.submit(new Callable() {
             public Object call() throws Exception {
               boolean res = true;

               if (useTRex && (transID != 0)) 
                       table.delete(transID, del);
	       else
		   table.delete(del);
	       return new Boolean(res);
	     }
         });
         return true;
      }
      else {

         if (useTRex && (transID != 0)) 
            table.delete(transID, del);
         else
            table.delete(del);
      }
      if (logger.isTraceEnabled())
         logger.trace("Exit deleteRow");
      return true;
   }

   public boolean deleteRows(final long transID, short rowIDLen, Object rowIDs,
            long timestamp, boolean asyncOperation, String hbaseAuths) throws IOException {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter deleteRows() " + tableName);

      final List<Delete> listOfDeletes = new ArrayList<Delete>();
      listOfDeletes.clear();
      ByteBuffer bbRowIDs = (ByteBuffer)rowIDs;
      short numRows = bbRowIDs.getShort();
      byte[] rowID;      
      byte rowIDSuffix;
      short actRowIDLen;
       
      for (short rowNum = 0; rowNum < numRows; rowNum++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         bbRowIDs.get(rowID, 0, actRowIDLen);

         EsgynMDelete del;
         del = new EsgynMDelete(rowID);
         if (timestamp != -1)
             del.setTimestamp(timestamp);
         listOfDeletes.add(del);
      }
      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
               if (useTRex && (transID != 0)) 
                  table.delete(transID, listOfDeletes);
               else
                  table.delete(listOfDeletes);
               return new Boolean(res);
            }
         });
         return true;
      }
      else {

         if (useTRex && (transID != 0)) 
            table.delete(transID, listOfDeletes);
         else
            table.delete(listOfDeletes);
      }
      if (logger.isTraceEnabled()) 
         logger.trace("Exit deleteRows");
      return true;
   }
/*
         public byte[] intToByteArray(int value) {
        return new byte[] {
       (byte)(value >>> 24),
       (byte)(value >>> 16),
       (byte)(value >>> 8),
       (byte)value};
    }
    
*/
   public boolean checkAndDeleteRow(long transID, byte[] rowID, 
                byte[] columnToCheck, byte[] colValToCheck,
                long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter checkAndDeleteRow(" 
						+ new String(rowID) 
						+ ", " + (columnToCheck == null ? "null" : new String(columnToCheck))
						+ ", " + (colValToCheck == null ? "null" : new String(colValToCheck))
						+ ", " + timestamp + ") " 
						+ tableName);

         EsgynMDelete del;
         del = new EsgynMDelete(rowID);
         if (timestamp != -1) 
            del.setTimestamp(timestamp);
         boolean res = true;
/*
         if (useTRex && (transID != 0)) 
             res = table.checkAndDelete(transID, rowID, columnToCheck, colValToCheck, del);
          else 
             res = table.checkAndDelete(rowID, columnToCheck, colValToCheck, del);
*/
         if (useTRex && (transID != 0)) 
	     table.delete(transID, del);
	 else 
	     table.delete(del);

      return res;
   }

   public boolean putRow(final long transID, final byte[] rowID, Object row,
      byte[] columnToCheck, final byte[] colValToCheck,
      final boolean checkAndPut, boolean asyncOperation) throws IOException, InterruptedException, 
                          ExecutionException {

      if (logger.isDebugEnabled()) 
         logger.debug("Enter putRow() " + tableName
		      + ", useTRex: " + useTRex
		      + ", txId: " + transID
		      + ", columnToCheck: " + (columnToCheck == null ? "null" : "not null")
		      + ", colValToCheck: " + (colValToCheck == null ? "null" : "not null")
		      + ", checkAndPut: " + checkAndPut
		      + ", asyncOperation: " + asyncOperation
		      );

      final EsgynMPut put;
      ByteBuffer bb;
      short numCols;
      short colNameLen;
      int colValueLen;
      byte[] colName, colValue;

      bb = (ByteBuffer)row;
      put = new EsgynMPut(rowID);
      numCols = bb.getShort();
      for (short colIndex = 0; colIndex < numCols; colIndex++) {
         colNameLen = bb.getShort();
         colName = new byte[colNameLen];
         bb.get(colName, 0, colNameLen);
         colValueLen = bb.getInt();   
         colValue = new byte[colValueLen];
         bb.get(colValue, 0, colValueLen);
         put.addColumn(colName, colValue);
      }

      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
               if (checkAndPut) {
		   /*
                  if (useTRex && (transID != 0)) 
                     res = table.checkAndPut(transID, rowID, 
                          columnToCheck, colValToCheck, put);
                  else 
                     res = table.checkAndPut(rowID, 
                        columnToCheck, colValToCheck, put);
		   */
                  if (useTRex && (transID != 0)) 
		      table.put(transID, put);
                  else 
                     table.put(put);
               }
               else {
                  if (useTRex && (transID != 0)) 
                     table.put(transID, put);
                  else 
                     table.put(put);
               }
               return new Boolean(res);
            }
         });
         return true;
      } else {
         boolean result = true;
         if (checkAndPut) {

            if (useTRex && (transID != 0)) 
                result = table.checkAndPut(transID, rowID, 
                  columnToCheck, colValToCheck, put);
            else 
		// TBD (need to change to checkAndPut() once it is fixed
		table.put(put);
         }
         else {
            if (useTRex && (transID != 0)) 
               table.put(transID, put);
            else 
                table.put(put);
         }
         return result;
      }   
   }

   public boolean insertRow(long transID, byte[] rowID, 
                         Object row, 
          long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException {
      return putRow(transID, rowID, row, null, null, false, asyncOperation);
   }

   public boolean putRows(final long transID, short rowIDLen, Object rowIDs, 
                       Object rows,
                       long timestamp, boolean asyncOperation)
         throws IOException, InterruptedException, ExecutionException  {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter putRows() " + tableName);

      Put put;
      ByteBuffer bbRows, bbRowIDs;
      short numCols, numRows;
      short colNameLen;
      int colValueLen;
      byte[] colName, colValue, rowID;
      byte rowIDSuffix;
      short actRowIDLen;
      bbRowIDs = (ByteBuffer)rowIDs;
      bbRows = (ByteBuffer)rows;

      final List<Put> listOfPuts = new ArrayList<Put>();
      numRows = bbRowIDs.getShort();
      
      for (short rowNum = 0; rowNum < numRows; rowNum++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         bbRowIDs.get(rowID, 0, actRowIDLen);
         put = new Put(rowID);
         numCols = bbRows.getShort();
         for (short colIndex = 0; colIndex < numCols; colIndex++) {
            colNameLen = bbRows.getShort();
            colName = new byte[colNameLen];
            bbRows.get(colName, 0, colNameLen);
            colValueLen = bbRows.getInt();   
            colValue = new byte[colValueLen];
            bbRows.get(colValue, 0, colValueLen);
            put.addColumn(colName, colValue); 
         }
/*
         if (writeToWAL)  
            put.setWriteToWAL(writeToWAL);
*/
         listOfPuts.add(put);
      }

      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
/*
               if (useTRex && (transID != 0)) 
                  table.put(transID, listOfPuts);
               else 
*/
                  table.put(listOfPuts);
               return new Boolean(res);
            }
         });
      }
      else {
/*
         if (useTRex && (transID != 0)) 
            table.put(transID, listOfPuts);
         else 
*/
            table.put(listOfPuts);
      }
      return true;
   } 

   public boolean completeAsyncOperation(int timeout, boolean resultArray[]) 
         throws InterruptedException, ExecutionException
   {
      if (timeout == -1) {
         if (! future.isDone()) 
            return false;
      }
      try {         
         Boolean result = (Boolean)future.get(timeout, TimeUnit.MILLISECONDS);
         // Need to enhance to return the result 
         // for each Put object
         for (int i = 0; i < resultArray.length; i++)
             resultArray[i] = result.booleanValue();
         future = null;
      } catch(TimeoutException te) {
         return false;
      } 
      return true;
   }

   public boolean checkAndInsertRow(long transID, byte[] rowID, 
                         Object row, 
          long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException  {
      return putRow(transID, rowID, row, null, null, 
            true, asyncOperation);
   }

   public boolean checkAndUpdateRow(long transID, byte[] rowID, 
             Object columns, byte[] columnToCheck, byte[] colValToCheck,
             long timestamp, boolean asyncOperation) throws IOException, InterruptedException, 
                                    ExecutionException, Throwable  {
      return putRow(transID, rowID, columns, columnToCheck, 
         colValToCheck, true, asyncOperation);
   }

   public byte[] coProcAggr(long transID, int aggrType, 
                            byte[] startRowID, 
                            byte[] stopRowID, byte[] colFamily, byte[] colName, 
                            boolean cacheBlocks, int numCacheRows) 
       throws IOException, Throwable {
       
       long rowCount = 0;
       
       AggregationClient aggregationClient = new AggregationClient();
       Scan scan = new Scan();
       rowCount = aggregationClient.rowCount(getMTableName(), scan);

       byte[] rcBytes = 
           ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(rowCount).array();

       //       System.out.println("monarch aggr count = " + rowCount);

       return rcBytes; 
   }
    
   public boolean release(boolean cleanJniObject) throws IOException {

      boolean retcode = false;
      // Complete the pending IO
      if (future != null) {
         try {
            future.get(30, TimeUnit.SECONDS);
         } catch(TimeoutException e) {
            logger.error("Asynchronous Thread is Cancelled (timeout), " + e);
            retcode = true;
            future.cancel(true); // Interrupt the thread
         } catch(InterruptedException e) {
            logger.error("Asynchronous Thread is Cancelled (interrupt), " + e);
            retcode = true;
            future.cancel(true); // Interrupt the thread
         } catch (ExecutionException ee) {
         }
         future = null;
      }
      if (scanner != null) {
         scanner.close();
         scanner = null;
      }
/*
      if (snapHelper !=null) {
         snapHelper.release();
         snapHelper = null;
      }
*/
      cleanScan();      
      getResultSet = null;
      if (cleanJniObject) {
         if (jniObject != 0)
            cleanup(jniObject);
         tableName = null;
      }
      scanHelper = null;
      jniObject = 0;
      return retcode;
   }

   public boolean close(boolean clearRegionCache, boolean cleanJniObject) throws IOException {
           if (logger.isTraceEnabled()) logger.trace("Enter close() " + tableName);
           if (table != null) 
           {
              //table.close();
              table = null;
           }
           return true;
   }

   public byte[][]  getEndKeys() throws IOException {
      if (logger.isTraceEnabled()) logger.trace("Enter getEndKeys() " + tableName);
      if (table == null) 
         return null;
      //byte[][] htableResult = table.getMTableLocationInfo().getEndKeys();
      byte[][] htableResult = table.getEndKeys();
      return htableResult;
   }

   public byte[][]  getStartKeys() throws IOException {
      if (logger.isTraceEnabled()) logger.trace("Enter getStartKeys() " + tableName);
      //byte[][] htableResult = table.getMTableLocationInfo().getStartKeys();
      byte[][] htableResult = table.getStartKeys();
      return htableResult;
   }

   private void cleanScan()
   {
      if (fetchType == GET_ROW || fetchType == BATCH_GET)
         return;
      numRowsCached = 1;
      numColsInScan = 0;
      cellsValLen = null;
      cellsValOffset = null;
      cellsValBuffer = null;
      cellsTimestamp = null;
      cellsPerRow = null;
      cellsName = null;
      rowIDs = null;
   }

   protected void setJniObject(long inJniObject) {
       jniObject = inJniObject;
   }    

    private native int setResultInfo(long jniObject,
            byte[][] cellsName, byte[][] cellsValBuffer,
            int[] cellsValOffset, int[] cellsValLen,
            long[] cellsTimesamp, byte[][] rowKeys, 
            int[] cellsPerRow, int numCellsReturned,
            int rowsReturned);

   private native void cleanup(long jniObject);

   protected native int setJavaObject(long jniObject);
 
   static {
      envUseTRex = true;
      envUseTRexScanner = true;
      String useTransactions = System.getenv("USE_TRANSACTIONS");
      if (useTransactions != null) {
          int lv_useTransactions = (Integer.parseInt(useTransactions));
          if (lv_useTransactions == 0) 
             envUseTRex = false;
      }
       
      String useTransactionsScanner = System.getenv("USE_TRANSACTIONS_SCANNER");
      if (useTransactionsScanner != null) {
          int lv_useTransactionsScanner = (Integer.parseInt(useTransactionsScanner));
          if (lv_useTransactionsScanner == 0) 
             envUseTRexScanner = false;
      }
      executorService = Executors.newCachedThreadPool();
      System.loadLibrary("executor");
   }
}
