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

/**
 * 
 */
package org.trafodion.libmgmt;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.trafodion.sql.TrafConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hbase.client.transactional.ATRConfig;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState.WriteAction;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.CellScanner;

import org.trafodion.sql.udr.*;

import org.trafodion.libmgmt.AlignedFormatTupleParser;
import org.apache.hadoop.hbase.pit.HBaseBinlog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrafBinlogReader extends UDR{
  private static Logger LOG = Logger.getLogger(TrafBinlogReader.class);
  static Configuration config = null; 
  private static String BINLOG_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.TRAFODION_BINLOG";
  private static String BINLOG_READER_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER";
  private static String BINLOG_META_COL_NAME = "mt_";
  private static final byte[] BINLOG_FAMILY = Bytes.toBytes("mf");
  private static final byte[] BINLOG_META_FAMILY = Bytes.toBytes("mt_");
  private static final byte[] BINLOG_COLS_COL = Bytes.toBytes("col");
  private static final byte[] BINLOG_KEYS_COL = Bytes.toBytes("key");
  private static final byte[] BINLOG_QUAL = Bytes.toBytes("mq");
  private static int partition_num = 1;
  private static Admin admin;

  private boolean doCommit = false;

  private int max_tx_count = -1; 
  private int tx_counter = 1;

  private boolean shutdown = false;

  private final int MAX_VALUE_LEN = 2048;
  private final int MAX_TBLNM_LEN = 256;

  private static final int MAX_TRANS_TIMEOUT = 2000; //max dangling trans time should be defined here , set it to 100 writeId
  private static final int MAX_TRANSLIST_SIZE = 10000; //max size of active transactions in the list, 1000 conccurency is something very big already
  private static final int MAX_BATCH_SIZE = 1000; //max size of active transactions in the list, 1000 conccurency is something very big already

  private int batchSize = MAX_BATCH_SIZE;
  private long readToTs = 0L;

  private ConcurrentHashMap<String,Integer> checkTableMap = new ConcurrentHashMap<String,Integer>();
  private ConcurrentHashMap<Integer,Long> lastTsPerSalt = new ConcurrentHashMap<Integer,Long>();
  private ConcurrentHashMap<Integer,Boolean> breakScanPerSalt = new ConcurrentHashMap<Integer,Boolean>();
  private ConcurrentHashMap<Integer,byte[]> lastRowkeyPerSalt = new ConcurrentHashMap<Integer,byte[]>();
  //private ConcurrentHashMap<Integer,Long> lastCidPerSalt = new ConcurrentHashMap<Integer,Long>();
  private ConcurrentHashMap<Integer,Long> lastWidPerSalt = new ConcurrentHashMap<Integer,Long>();
  private ConcurrentHashMap<Integer,Long> lastPreMinTsPerSalt = new ConcurrentHashMap<Integer,Long>();
  private List<String> tblPatterns = Collections.synchronizedList(new ArrayList<String>());
  private int binlogPartitionNumber = 0;

  private AlignedFormatTupleParser ddlparser = null;
  private String ddl_xdc_table = "_XDC_MD_.XDC_DDL";
  private String seq_xdc_table = "_XDC_MD_.XDC_SEQ";

  /*transaction manager*/
  private ConcurrentHashMap< Long, transInfo > transList = new ConcurrentHashMap<Long,transInfo>();

  long lastWriteId = 0L;

  private String tableList = null;
  private long startCommitId = 0L;
  private long startTimestamp = 0L;
  private static Connection connection = null;
  private static Table table = null;
  private static Table mtable = null;
  private static boolean initialized = false;
  private boolean terminated = false;

  private static String delimter = ",";

  private tableInfoCacheForBinlogReaderParser tblCache = new tableInfoCacheForBinlogReaderParser();

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  private byte[] currentRowKey = null;

  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
        int v = bytes[j] & 0xFF;
        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

    
  /**
  * Class Constructor
  */
  TrafBinlogReader() {
  }

  // determine output columns dynamically at compile time
  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info)
        throws UDRException
  {
	  if (info.par().getNumColumns() < 4 || info.par().getNumColumns() > 7){
		  UDRException e = new UDRException(38970, "This UDF needs to be called with four to seven input parameters");
		  LOG.error("Num of columns is: " + info.par().getNumColumns(), e);
		  throw e;
	  }
        for (int i=0; i<info.par().getNumColumns() ; i++)
            info.addFormalParameter(info.par().getColumn(i));

        TypeInfo str;
        TypeInfo longType;
        final int maxLength = 100000;
        int                       scale        = 0;
        int                       precision    = 0;
        TypeInfo.SQLTypeCode      sqlStringType      = TypeInfo.SQLTypeCode.VARCHAR;
        TypeInfo.SQLTypeCode      intStringType      = TypeInfo.SQLTypeCode.INT;	
        TypeInfo.SQLTypeCode      longStringType      = TypeInfo.SQLTypeCode.LARGEINT;			
        TypeInfo.SQLIntervalCode  intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
        TypeInfo.SQLCharsetCode   charset      = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
        TypeInfo.SQLCharsetCode   isocharset      = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
        TypeInfo.SQLCollationCode collation    = TypeInfo.SQLCollationCode.SYSTEM_COLLATION;
        //tableName
        str = new TypeInfo(
                sqlStringType,
                MAX_TBLNM_LEN,
                false,
                scale,
                isocharset,
                intervalCode,
                precision,
                collation);
        String colName = "TABLE_NAME";
        info.out().addColumn(new ColumnInfo(colName, str));

        //VALUES
        str = new TypeInfo(
                sqlStringType,
                MAX_VALUE_LEN,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "VALUES";
        info.out().addColumn(new ColumnInfo(colName, str));
		
        //Operation
        str = new TypeInfo(
                sqlStringType,
                16,
                false,
                scale,
                isocharset,
                intervalCode,
                precision,
                collation);
        colName = "EVENT";
        info.out().addColumn(new ColumnInfo(colName, str));		

        //cid
        str = new TypeInfo(
                longStringType,
                8,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);

        colName = "COMMIT_ID";
        info.out().addColumn(new ColumnInfo(colName, str));

        //ts
        str = new TypeInfo(
                longStringType,
                8,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "LAST_TS";
        info.out().addColumn(new ColumnInfo(colName, str));

        //change line
        str = new TypeInfo(
                intStringType,
                4,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "LINE_NUM";
        info.out().addColumn(new ColumnInfo(colName, str));

        //opertion timestamp
        //this is more for flashback query usage
        str = new TypeInfo(
                longStringType,
                8,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);

        colName = "OPTIME";
        info.out().addColumn(new ColumnInfo(colName, str));

        //tid
        str = new TypeInfo(
                longStringType,
                8,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);

        colName = "TX_ID";
        info.out().addColumn(new ColumnInfo(colName, str));

        //seq num 
        str = new TypeInfo(
                intStringType,
                4,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "SEQ_NUM";
        info.out().addColumn(new ColumnInfo(colName, str));

        //total number
        str = new TypeInfo(
                intStringType,
                4,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "TX_TOTAL_NUM";
        info.out().addColumn(new ColumnInfo(colName, str));

        //ddlnumber
        str = new TypeInfo(
                intStringType,
                4,
                false,
                scale,
                charset,
                intervalCode,
                precision,
                collation);
        colName = "TX_DDL_NUM";
        info.out().addColumn(new ColumnInfo(colName, str));
    }

  /**
   * check the name format
   * the name must be schemaName.tableName
   *
   * Name's format :
   * number 0-9
   * letter a-zA-Z
   * Underscore _
   * or
   * *
   *
   * @param t
   * @throws UDRException	 
  */
  private static void checkTableName(String t,UDRInvocationInfo info) throws UDRException{
    //check the name format
    String namePattern = "(\"[0-9a-zA-Z_]+\"|\\*|[0-9a-zA-Z_]+)\\.((\"[0-9a-zA-Z_]+\")|\\*|[0-9a-zA-Z_]+)";
    if(!Pattern.matches(namePattern,t)){
      UDRException e = new UDRException(
              38030,
              "Input table name %s is invalid, must have schema and table name, for UDF %s",
              t,
	      info.getUDRName());
      LOG.error("checkTableName", e);
      throw e;
    }
  }

  private String normalizeTableName(String t){  
    String tbns[] = t.split("\\.");
    int size = tbns.length;
    if(size < 2) return null;
    //last two parts schema and table name
    //String ret =  tbns[size-2] + "." + tbns[size-1];
    String schemaName = tbns[size-2].startsWith("\"") ? tbns[size-2].replaceAll("\"", "") : tbns[size-2].toUpperCase();
    String tableName = tbns[size-1].startsWith("\"") ? tbns[size-1].replaceAll("\"", "") : tbns[size-1].toUpperCase();
    String ret = schemaName +"."+tableName;
    return ret;
  }

  private byte[] genScanStartRowKey(int sa, long lastWriteId) {
    HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(sa, lastWriteId, 0, (short) 0);
    return rowkey.getRowArray();
  }

  private long getWriteIdFromRowkey(byte[] rk) {
    HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(rk);
    return rowkey.getWriteID();
  }

  private short getTotalNumFromRowkey(byte[] rk) {
    HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(rk);
    return rowkey.getTotalNum();
  }

  private int getSaltFromRowkey(byte[] rk) throws UDRException {
    try {
      HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(rk);
      return (int)rowkey.getSalt(Integer.parseInt(ATRConfig.instance().getBinlogPartNum()));
    }
    catch (Exception e) {
      LOG.error("Error 38970 This UDR get salt from rowkey failed with:", e);
      throw new UDRException(38970, "This UDR get salt from rowkey failed with: " + e);
    }
  }

  private long getMaxTs()
  {
    long maxts = 0;

    for(int i = 0; i< binlogPartitionNumber ; i++) {
      if (maxts < lastTsPerSalt.get(i) ) maxts = lastTsPerSalt.get(i);
    }

    return maxts;
  }

  private long getMinTs()
  {
    long mints = Long.MAX_VALUE;

    for(int i = 0; i< binlogPartitionNumber ; i++) {
      if (mints > lastTsPerSalt.get(i) ) mints = lastTsPerSalt.get(i);
    }

    return mints;
  }

  // override the runtime method
  // input
  // param 1: long commitID
  // param 2: long start timestamp
  // param 3: table list 
  // output:
  // col1 : TableName
  // col2 : Op Type : 1: insert 2: delete 3: update
  //                  commit, etc..
  // col3 : Values : string of values
  // col4 : last cid
  // col5 : last timestamp 
  // col6 : transaction ID
  @Override
  public void processData(UDRInvocationInfo info,
                          UDRPlanInfo plan)
    throws UDRException
  {
    byte[] rawdata;
    int endC = 0;
    long inputcid = info.par().getLong(0);
    long startTs = info.par().getLong(1);
    tableList = info.par().getString(2);
    int currTotalNum = 0;
    int currDdlNum = 0;

    //validation 
    if(inputcid <0 )
    {
      UDRException e = new UDRException(
            38030,
            "input commit id %d is invalid, must greater than 0, for UDF %s",
            inputcid,
            info.getUDRName());
      LOG.error("Error 38030", e);
      throw e;
    }
    //validation 
    if(startTs<0 )
    {
      UDRException e = new UDRException(
            38030,
            "input timestamp %d is invalid, must greater than 0, for UDF %s",
            startTs,
            info.getUDRName());
      LOG.error("Error 38030", e);
      throw e;
    }

    String tbs[] = tableList.split(";");
    for(String t : tbs) {
      checkTableName(t,info);
      if(t.contains("*")){
	tblPatterns.add(t.toUpperCase());
      }
      String nt = normalizeTableName(t);
      if(nt == null){
        UDRException e = new UDRException(
          38030,
          "Input table name %s is invalid, must have schema and table name, for UDF %s",
          t,
          info.getUDRName());
        LOG.error("Error 38030", e);
        throw e;
      }
      checkTableMap.put(nt,1);
    }

    int timeout = info.par().getInt(3);

    //validation 
    if(timeout <0 )
    {
      UDRException e = new UDRException(
            38030,
            "input timeout %d is invalid, must greater than 0, for UDF %s",
            timeout,
            info.getUDRName());
      LOG.error("Error 38030", e);
      throw e;
    }

    if( info.par().getNumColumns() >= 5 ) 
    {
      delimter = info.par().getString(4);
    }
    else delimter = ",";

    if( info.par().getNumColumns() >= 6 ) 
    {
       String doCommitStr = info.par().getString(5);
       doCommit = (Integer.parseInt(doCommitStr) == 1) ? true : false;
    }


    boolean haveData = false;
    try {
      binlogPartitionNumber = Integer.parseInt(ATRConfig.instance().getBinlogPartNum());
    }
    catch(Exception e)  {
      LOG.error("Error 38030", e);
      throw new UDRException(
            38030,
            "Cannot get binlog part number for UDF %s",
            info.getUDRName());
    }

    if( info.par().getNumColumns() >= 7 ) 
    {
       max_tx_count = info.par().getInt(6);
       batchSize = max_tx_count / binlogPartitionNumber;
       if(batchSize == 0) batchSize = 1; 
    }

    for(int s=0 ; s < binlogPartitionNumber ; s++) {
      //at first, make all starting ts to startTs
      if(startTs > 0)
        lastTsPerSalt.put(s , startTs );
      else
        lastTsPerSalt.put(s , 0L);
      //lastCidPerSalt.put(s, 0L);
      lastWidPerSalt.put(s, 0L);
      lastPreMinTsPerSalt.put(s, -1L);
      breakScanPerSalt.put(s, false);          
    }
    try {
      if(initialized == false){
        try {
          config = HBaseConfiguration.create();
          connection = ConnectionFactory.createConnection(config);
          table = connection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));
          admin = connection.getAdmin();
          final boolean binlogReaderTableExists = admin.isTableAvailable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
          if(binlogReaderTableExists == true)
            mtable = connection.getTable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
          else
            mtable = null;
          initialized = true;
        }
        catch (Exception e) {
          config = null;
          connection = null;
          table = null;
          initialized = false;
          LOG.error("Error 38030", e);
          throw new UDRException(
            38030,
            "Cannot access table %s for UDF %s",
            BINLOG_TABLE_NAME,
            info.getUDRName());
        }
      }
    }
    catch (Exception e) {
      LOG.error("Error 38030", e);
          throw new UDRException(
            38030,
            "Cannot get hbase connection for %s",
            e.toString());
    }
    boolean firstScan = true;

    //set the timestamp this read up to
    readToTs = System.currentTimeMillis();

    if(startTs > readToTs ) return;

    // set the output column
    while(terminated == false) {
      haveData = false;

      for(int currentSalt = 0 ; currentSalt < binlogPartitionNumber; currentSalt++) {

      //read from binlog table

      int readCount = 0;

      Scan s = new Scan();
      ResultScanner ss = null;
      if(table == null ){
        UDRException e = new UDRException(
            38030,
            "Cannot access table %s for UDF %s",
            BINLOG_TABLE_NAME,
            info.getUDRName());
        LOG.error("Error 38030", e);
        throw e;
      }
      else
      {
        try{
          //set the starting point as last one
          s.setTimeRange(lastTsPerSalt.get(currentSalt), readToTs);

          //if outOfLimit or everything done
          //make sure all partition finish reading to the last ts
          long maxtsnow = getMaxTs();
          if(shutdown == true) 
          {
            s.setTimeRange(lastTsPerSalt.get(currentSalt), maxtsnow + 1); //HBase stopRow is exclusive, so this means up to maxtsnow
            batchSize = Integer.MAX_VALUE;
          }
          long mintsnow = getMinTs();
          if(lastTsPerSalt.get(currentSalt) - mintsnow > 1000*10 && doCommit == true && shutdown == false) //I am too fast
          {
              //check previous min ts
              // if min ts not changed, it means all other salts reading is over, so even I am too fast, I should continue
              if(mintsnow > lastPreMinTsPerSalt.get(currentSalt) ) // changed
              {
                lastPreMinTsPerSalt.put(currentSalt, mintsnow); //update min ts
                haveData = true;
                continue;
              }
          }

          HBaseBinlog.RowKey reverseStartRow = new HBaseBinlog.RowKey((int)HBaseBinlog.RowKey.computeMapedSalt(currentSalt, binlogPartitionNumber), lastWidPerSalt.get(currentSalt), 0, (short)0);
          HBaseBinlog.RowKey reverseStopRow = new HBaseBinlog.RowKey((int)HBaseBinlog.RowKey.computeMapedSalt(currentSalt, binlogPartitionNumber) + 1, 0, 0, (short)0);

          if(breakScanPerSalt.get(currentSalt) == true) //start from last breakpoint's key
            s.setStartRow(lastRowkeyPerSalt.get(currentSalt));
          else
            s.setStartRow(reverseStartRow.getRowArray());

          s.setStopRow(reverseStopRow.getRowArray());

          s.addColumn(BINLOG_FAMILY,BINLOG_QUAL);

          ss = table.getScanner(s);
          LOG.debug("getScanner for salt " + currentSalt + " finished , batchSize " + batchSize);
          breakScanPerSalt.put(currentSalt, false);          

          for (Result r : ss) {
            currentRowKey = r.getRow();
            readCount++;
            if(readCount > batchSize) {
               breakScanPerSalt.put(currentSalt, true);
               lastRowkeyPerSalt.put(currentSalt, currentRowKey);
               haveData = true;
               break;
            }
            for (Cell cell : r.rawCells()) {
              long currts = cell.getTimestamp();
              byte[] input = CellUtil.cloneValue(cell);
              TransactionMutationMsg tmm = TransactionMutationMsg.parseFrom(input);
              if(tmm != null) {
                String tableName = tmm.getTableName();

                String ntn = normalizeTableName(tableName);
                Boolean tblMatch = false;
                Boolean emitBeginFlag = false;
                Boolean emitCommitFlag = false;

                long cid = tmm.getCommitId();
                long tid = tmm.getTxId();
                int ddlNum = tmm.getDdlNum();
                int saltNum = getSaltFromRowkey(currentRowKey);
                long writeId = getWriteIdFromRowkey(currentRowKey);

                if(writeId <= lastWidPerSalt.get(saltNum) ) //if wid is less than the last wid for this salt
                    continue;

                if(checkTableMap.containsKey(ntn) == true) //this is not the target table
                {
                   tblMatch = true;
                }
 
                //check pattern if not match
                if(tblMatch == false) {
                  for( String t : tblPatterns) {
                    if( wildcardEquals(t, ntn) != true ) 
                      continue;
                    else  //match
                    {
                      tblMatch = true;
                      break;
                    }
                  }
                }

                AlignedFormatTupleParser parser = tblCache.addTableDefToCache(ntn, currts);
                if(parser == null) //try use ntn to search again
                   parser = tblCache.addTableDefToCache(tableName, currts);
                int currentNum = 0;
                transInfo ti = null;

                if(doCommit == true) {
                  if( !transList.containsKey(cid)  ) {
                    if(!ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) { //do not track transaction for DDL event
                      emitBeginFlag = true;
                      ti = new transInfo(cid, writeId, saltNum);
                      ti.setTid(tid);
                      transList.put(cid,ti);
                    } 
                  }
                  else
                  {
                    ti = transList.get(cid);
                    currentNum = ti.getCurrentNum();
                  }

               
                  int totalNum = (int)getTotalNumFromRowkey(currentRowKey);
                  currTotalNum = totalNum;
                  if(totalNum == 0 && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) 
                  {
                    transList.remove(cid);
                    ti = null;
                    continue;
                  }
                  if(tmm.getIsMsgComplete() == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) {
                    currentNum++;
                    ti.setCurrentNum(currentNum);
                    transList.put(cid, ti); //update the transaction current number
                  }
                  if(currentNum >= totalNum ) 
                    emitCommitFlag = true;
                  }

                  if(tblMatch == false && doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) ti.setMatch(false);

                  if(!ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table) ) {
                  if(tblMatch == false ||  parser == null || parser.getValid() == false) {
                    if(currts > lastTsPerSalt.get(currentSalt) )
                      lastTsPerSalt.put(currentSalt, currts);

                    if(writeId > lastWidPerSalt.get(saltNum))
                    {
                      lastWidPerSalt.put(saltNum, writeId );
                    }
                    if(emitCommitFlag == true && doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table))
                    {
                      if( ti.isPending() == true ) //now I got all log and can emit Commit
                     {
                       transList.remove(cid);
                       emitCommit(info, cid, currts, tid, ddlNum);
                     }
                     else{
                       if(ti.getMatch() == false) transList.remove(cid);
                     }
                    }
                    
                    continue;
                  }
                }

                if(emitCommitFlag == true ) {
                    tx_counter++;
                }

                List<Boolean> putOrDel = tmm.getPutOrDelList();
                List<MutationProto> putProtos = tmm.getPutList();
                List<MutationProto> deleteProtos = tmm.getDeleteList(); 

                int putIndex = 0;
                int deleteIndex = 0;
                String op = "UNKNOWN";
                WriteAction waction;
                int seqnum = 0;
                if(doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) seqnum = ti.getSeqNum();
                for (Boolean put : putOrDel) {
                  if (put) {
                    Put writePut = ProtobufUtil.toPut(putProtos.get(putIndex++));
                    if( ntn.equals(ddl_xdc_table) )
                    {
                      NavigableMap<byte[], List<Cell>> familyCellMap = writePut.getFamilyCellMap();
                      String outputValue="";
                      String tblnm="";
                      String schnm="";
                      int colCounter = 0;
                      for (Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                        for (Iterator<Cell> iterator = entry.getValue().iterator(); iterator.hasNext();) {
                          colCounter++;
                          Cell cc = iterator.next();
                          byte[] value = CellUtil.cloneValue(cc);
                          if(colCounter == 2) schnm = Bytes.toString(value);
                          if(colCounter == 3) tblnm = Bytes.toString(value);
                          if(colCounter == 6) outputValue = Bytes.toString(value);
                        }
                      }
                     emitDDL(info,cid,currts,tid, outputValue, saltNum, currts, writeId, schnm+"."+tblnm, ddlNum);
                     if(doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table))
                       transList.remove(ti);
                     haveData = true;
                    }
                    else if(  ntn.equals(seq_xdc_table) )
                    {
                      NavigableMap<byte[], List<Cell>> familyCellMap = writePut.getFamilyCellMap();
                      long nextval = 0;
                      String tblnm="";
                      String schnm="";
                      int colCounter = 0;
                      for (Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                        for (Iterator<Cell> iterator = entry.getValue().iterator(); iterator.hasNext();) {
                          colCounter++;
                          Cell cc = iterator.next();
                          byte[] value = CellUtil.cloneValue(cc);
                          if(colCounter == 2) schnm = Bytes.toString(value);
                          if(colCounter == 3) tblnm = Bytes.toString(value);
                          if(colCounter == 4) {
                             ByteBuffer longbb = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
                             nextval = longbb.getLong(0);
                           }
                        }
                      }
                     emitSeq(info,cid,currts,tid, nextval, saltNum, currts, writeId, schnm+"."+tblnm);
                     haveData = true;
                    }
                    else {
                      byte[] isUpsertb = writePut.getAttribute("ISUPSERT");
                      int sop = 0;
                      if(isUpsertb == null ) //insert
                        sop = 0;
                      else {
                        if(Bytes.toInt(isUpsertb) == 0) //update
                          sop = 1;
                        else
                          sop = 2;
                      }
                      int ret = emitPut(writePut, info, tableName , cid, currts, saltNum,  tid, seqnum, sop, writeId, ntn, currTotalNum);
                      if(ret == 0) 
                      {
                        if(doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) {
                          seqnum++;
                          ti.incSeqNum();
                        }
                        else
                          seqnum++;
                        haveData = true;
                      }
                    }
                  }
                  else {
                    if( !ntn.equals(ddl_xdc_table)&& !ntn.equals(seq_xdc_table) ) {
                      Delete writeDelete = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
                      int ret = emitDelete(writeDelete, info, tableName , cid, currts, saltNum, tid, seqnum, writeId, ntn, currTotalNum);
                      if(ret == 0) 
                      {
                        if(doCommit == true && !ntn.equals(ddl_xdc_table)&& !ntn.equals(seq_xdc_table)) {
                          seqnum++;
                          ti.incSeqNum();
                        }
                        else
                          seqnum++;
                        haveData = true;
                      }
                    }
                  }
                }

                //handle commit
                if(emitCommitFlag == true && doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table))
                {
                  transList.remove(cid); //clear the transaction from list
                  ti = null;
                  emitCommit(info, cid, currts, tid, ddlNum);
                }
                else {
                  if(doCommit == true && !ntn.equals(ddl_xdc_table) && !ntn.equals(seq_xdc_table)) {
                    ti.setPending(true);
                    transList.put(cid,ti);
                  }
                }
              }
              else {
                info.out().setString(1, "UNKNONWNTABLE");
                info.out().setString(0, "bad value");
                info.out().setString(2, "NA");
                info.out().setLong(3,0);
                info.out().setLong(4,0);
                info.out().setInt(5,0);
                info.out().setLong(6,0);
                info.out().setLong(7,0);
                info.out().setInt(8,0);
                info.out().setInt(9,0);
                emitRow(info);
              }
            }
          } //for result set for
          ss.close();
        } // end of try clause
        catch(Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
          LOG.error("Error 38030", e);
            throw new UDRException(
                38030,
                "Get exception %s for UDF %s",
                sw.toString(),
                info.getUDRName());
        }
      } //end of else table is not null
    } // for salt
    firstScan = false;

    if( shutdown == true)
       terminated = true;

    if(haveData == false || (tx_counter > max_tx_count && max_tx_count >0) ) //it hit the max tx counter limit
    {
      LOG.info("shutdown the reader haveData is " + haveData + " out of limit is " +  (tx_counter > max_tx_count && max_tx_count >0) );
      shutdown = true;
       try {
         Thread.sleep(10); //sleep 10 ms
       }catch (Exception e) {}
    }

   } //while not terminated


   //flush all remain trx with commit message
   for ( transInfo ti : transList.values()  )
   {
     
     long cid = ti.getCid();
     long tid = ti.getTid();
     long currts = ti.getTs(); 
     try{
       emitCommit(info, cid, currts, tid, 0);
     }
     catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error("Error 38030", e);
            throw new UDRException(
                38030,
                "Get exception %s for UDF %s",
                sw.toString(),
                info.getUDRName());
     }
   }

  }


  public static String substring(String orignal, int count) throws IOException{
     if(!"".equals(orignal) && orignal != null){
       orignal = new String(orignal.getBytes(), "UTF8");
       if(count > 0 && count < orignal.length()){
         StringBuffer sb = new StringBuffer();
         char c;
         for (int i = 0; i < count; i++) {
           c = orignal.charAt(i);
           sb.append(c);
           if(isChineseChar(c)){
              --count;
           }
         }
         return sb.toString();
       }
     }
     return orignal;
  }

  public static boolean isChineseChar(char c) throws IOException{
    return String.valueOf(c).getBytes("UTF8").length > 1;
  }



  public List<String> getStrList(String value, int length) {
        char[] cs = value.toCharArray();
        StringBuilder result = new StringBuilder();
        List<String> resultList = new ArrayList<String>();
        int index = 0;
        for (char c: cs){
            index += String.valueOf(c).getBytes().length;
            if (index > length){
                resultList.add(result.toString());
                result.setLength(0);
                result.append(c);
                index = 0;
                index += String.valueOf(c).getBytes().length;
            }else {
                result.append(c);
            }
        }
        if(result.length()>0){
            resultList.add(result.toString());
        }
        return resultList;
  }

  // 1-5,5-10
  private List<Integer> getSaltListNumbers(String saltList) {
    String[] ranges = saltList.split(",");
    List<Integer> ret = Collections.synchronizedList(new ArrayList<Integer>());
    for(String r : ranges) {
      int start = 0;
      int end = 0;
      String[] n = r.split("-");
      if(n.length == 2) {
        start = Integer.parseInt(n[0]);
        end = Integer.parseInt(n[1]);
      }
      else
        start = end = Integer.parseInt(n[0]);
      //get all node number
      for(int i = 0 ; i < end +1  - start ; i++)
        ret.add(start+i);
    }
    return ret;
  }

  public String substring(String str, int f, int t) {
        if (f > str.length())
            return null;
        if (t > str.length()) {
            return str.substring(f, str.length());
        } else {
            return str.substring(f, t);
        }
  }


  private int emitDelete(Delete d, UDRInvocationInfo info,
                       String tableName,
                       long cid,
                       long currts, int sa, long tid, int seqnum, long wid, String ntn, int totalNum) throws IOException, UDRException{
    byte[] rk = d.getRow(); 
    int linenumber = 0;
    boolean parseError = false;

    //update the last ts
    if(currts > lastTsPerSalt.get(sa) ) 
      lastTsPerSalt.put(sa, currts);

    if(wid > lastWidPerSalt.get(sa))
    {
        lastWidPerSalt.put(sa, wid);
    }
    AlignedFormatTupleParser parser = tblCache.addTableDefToCache(ntn, currts);
    if(parser == null)
      parser = tblCache.addTableDefToCache(tableName, currts);
    
    if(parser == null || parser.getValid() == false)
    {
      return -1;
    }
    String result = "";
    if(parser.getNoPk())
    {
      byte[] drow = d.getAttribute("KEEP_DEL_ROW");
      if(drow != null)
      {
        try{
          result = parser.getValue(drow);  
        }
        catch(Exception e) {
          LOG.warn("Parse warn with drow: " + drow, e);
          parseError = true;
          result = bytesToHex(drow);
        }
      }
      else
        return -1; 
    }
    else
    {
      try{
        result = parser.getKey(rk);
      }
      catch (Exception e) {
        LOG.warn("Parse warn with rk: " + rk, e);
        parseError = true;
        result = bytesToHex(rk);
      }
    }
    int bytelen = result.getBytes("utf-8").length;
    if(bytelen <= MAX_VALUE_LEN ) {
      info.out().setString(0,ntn);
      info.out().setString(1,result);

      if(parseError == true) 
        info.out().setString(2,"ERROR");
      else
        info.out().setString(2,"DELETE");
      
      info.out().setLong(3,cid);
      info.out().setLong(4,currts);
      info.out().setInt(5,linenumber);
      info.out().setLong(6,0);
      info.out().setLong(7,tid);
      info.out().setInt(8,seqnum);
      info.out().setInt(9,totalNum);
      emitRow(info);
    }
    else {
      List<String> res = getStrList(result, MAX_VALUE_LEN );
      for(String s : res) {
        linenumber++;
        info.out().setString(0,ntn);
        info.out().setString(1,s);
        if(parseError == true) 
          info.out().setString(2,"ERROR");
        else
          info.out().setString(2,"DELETE");
        info.out().setLong(3,cid);
        info.out().setLong(4,currts);
        info.out().setInt(5,linenumber);
        info.out().setLong(6,0);
        info.out().setLong(7,tid);
        info.out().setInt(8,seqnum);
        info.out().setInt(9,totalNum);
        emitRow(info);
      }
    }
    return 0;
  }

  private void emitBegin(UDRInvocationInfo info, long cid, long tid) throws IOException, UDRException
  {
        info.out().setString(0,"");
        info.out().setString(1,"");
        info.out().setString(2,"BEGIN");
        info.out().setLong(3,cid);
        info.out().setLong(4,0L);
        info.out().setInt(5,0);
        info.out().setLong(6,0);
        info.out().setLong(7,tid);
        info.out().setInt(8,0);
        info.out().setInt(9,0);
        emitRow(info);
  }

   private void emitSeq(UDRInvocationInfo info, long cid, long ts, long tid, long nextval, int sa, long currts, long wid, String tableName) throws IOException, UDRException
  {
    if(currts > lastTsPerSalt.get(sa) )
      lastTsPerSalt.put(sa, currts);

    if(wid > lastWidPerSalt.get(sa))
    {
        lastWidPerSalt.put(sa, wid);
    }

    //filter the target table name
    boolean tblMatch = false;

    if(checkTableMap.containsKey(tableName) == true) //this is not the target table
    {
      tblMatch = true;
    }

    //check pattern if not match
    if(tblMatch == false) {
      for( String t : tblPatterns) {
        if( wildcardEquals(t, tableName) != true )
          continue;
        else  //match
        {
          tblMatch = true;
          break;
        }
      }
    }

    if(tblMatch == false) return;

 
    info.out().setString(0,tableName);
    info.out().setString(1,Long.toString(nextval));
    info.out().setString(2,"SEQUENCE");
    info.out().setLong(3,cid);
    info.out().setLong(4,currts);
    info.out().setInt(5,0);
    info.out().setLong(6,ts);
    info.out().setLong(7,tid);
    info.out().setInt(8,0);
    info.out().setInt(9,1);
    emitRow(info);
  } 

  private void emitDDL(UDRInvocationInfo info, long cid, long ts, long tid, String ddl, int sa, long currts, long wid, String tableName, int ddlNum) throws IOException, UDRException
  {
    if(currts > lastTsPerSalt.get(sa) )
      lastTsPerSalt.put(sa, currts);

    if(wid > lastWidPerSalt.get(sa))
    {
        lastWidPerSalt.put(sa, wid);
    }

    //filter the target table name
    boolean tblMatch = false;

    if(checkTableMap.containsKey(tableName) == true) //this is not the target table
    {
      tblMatch = true;
    }

    //check pattern if not match
    if(tblMatch == false) {
      for( String t : tblPatterns) {
        if( wildcardEquals(t, tableName) != true )
          continue;
        else  //match
        {
          tblMatch = true;
          break;
        }
      }
    }
 
    if(tblMatch == false) return; 

    int linenumber = 0;
    int bytelen = ddl.getBytes("utf-8").length;

      if(bytelen  <= MAX_VALUE_LEN) {
        info.out().setString(0,tableName);
        info.out().setString(1,ddl);
        info.out().setString(2,"DDL");
        info.out().setLong(3,cid);
        info.out().setLong(4,currts);
        info.out().setInt(5,0);
        info.out().setLong(6,ts);
        info.out().setLong(7,tid);
        info.out().setInt(8,0);
        info.out().setInt(9,1);
        info.out().setInt(10,ddlNum);
        emitRow(info);
      }
      else {
        List<String> res = getStrList(ddl, MAX_VALUE_LEN  );
        for(String s : res) {
          linenumber++;
          info.out().setString(0,tableName);
          info.out().setString(1,s);
          info.out().setString(2,"DDL");
          info.out().setLong(3,cid);
          info.out().setLong(4,currts);
          info.out().setInt(5,linenumber);
          info.out().setLong(6,ts);
          info.out().setLong(7,tid);
          info.out().setInt(8,0);
          info.out().setInt(9,1);
          info.out().setInt(10,ddlNum);
          emitRow(info);
        }
      }

  }

  private void emitCommit(UDRInvocationInfo info, long cid, long ts, long tid, int ddlNum) throws IOException, UDRException
  {
        info.out().setString(0,"");
        info.out().setString(1,"");
        info.out().setString(2,"COMMIT");
        info.out().setLong(3,cid);
        info.out().setLong(4,0L);
        info.out().setInt(5,0);
        info.out().setLong(6,ts);
        info.out().setLong(7,tid);
        info.out().setInt(8,0);
        info.out().setInt(9,0);
        info.out().setInt(10,ddlNum);
        emitRow(info);
  }
  private int emitPut(Put p, UDRInvocationInfo info,
                       String tableName,
                       long cid,
                       long currts, int sa, long tid, int seqnum,
                       int op, long wid ,String ntn, int totalNum) throws IOException, UDRException{
    int linenumber = 0;
    boolean parseError = false;
    //update the last ts
    if(currts > lastTsPerSalt.get(sa) ) 
      lastTsPerSalt.put(sa, currts); 
    if(wid > lastWidPerSalt.get(sa))
    {
        lastWidPerSalt.put(sa, wid);
    }

    CellScanner sc = p.cellScanner();
    while(sc.advance()) {
      Cell c = sc.current();
      byte[] input1 = CellUtil.cloneValue(c);
      AlignedFormatTupleParser parser = tblCache.addTableDefToCache(ntn, c.getTimestamp());
      if(parser == null)
        parser = tblCache.addTableDefToCache(tableName, currts);
      
      if(parser == null)
      {
        return -1;
        /* when there is index, we should just ignore this and do nothing 
        throw new UDRException(
          38030,
          "Cannot access table %s for UDF %s",
          tableName,
          info.getUDRName());
        */
      }
      String result = "";

      try {
        result = parser.getValue(input1);
      }
      catch(Exception e) {
        LOG.warn("Parse warn with input1: " + input1, e);
        result = bytesToHex(input1);
        parseError = true;
      }

      int bytelen = result.getBytes("utf-8").length;
      if(bytelen <= MAX_VALUE_LEN) {
        info.out().setString(0,ntn);
        info.out().setString(1,result);
        switch(op) {
          case 1: 
            info.out().setString(2,"UPDATE");
            break;
          case 2:
            info.out().setString(2,"UPSERT");
            break;
          case 0:
          default:
            info.out().setString(2,"INSERT");
            break;
        }
        if(parseError == true)
          info.out().setString(2, "ERROR");
        info.out().setLong(3,cid);
        info.out().setLong(4,currts);
        info.out().setInt(5,0);
        info.out().setLong(6,0);
        info.out().setLong(7,tid);
        info.out().setInt(8,seqnum);
        info.out().setInt(9,totalNum);
        emitRow(info);
      }
      else {
        List<String> res = getStrList(result, MAX_VALUE_LEN  );
        for(String s : res) {
          linenumber++;
          info.out().setString(0,ntn);
          info.out().setString(1,s);
          switch(op) {
          case 1: 
            info.out().setString(2,"UPDATE");
            break;
          case 2:
            info.out().setString(2,"UPSERT");
            break;
          case 0:
          default:
            info.out().setString(2,"INSERT");
            break;
          }
          if(parseError == true)
            info.out().setString(2, "ERROR");
          info.out().setLong(3,cid);
          info.out().setLong(4,currts);
          info.out().setInt(5,linenumber);
          info.out().setLong(6,0);
          info.out().setLong(7,tid);
          info.out().setInt(8,seqnum);
          info.out().setInt(9,totalNum);
          emitRow(info);
        }
      }

      break;
    }
    return 0;
  }

  private static String getRegPath(String path) {
        //The path come from tblPatterns . tblPatterns must have * and math checkTableName()
        path = path.replaceAll("\\*","(\\\"[0-9a-zA-Z_]+\\\"|\\\\*|[0-9a-zA-Z_]+)");
        return path;
    }

    private static boolean wildcardEquals(String whitePath, String reqPath) {
        String regPath = getRegPath(whitePath);
        return Pattern.compile(regPath).matcher(reqPath).matches();
    }

    //helper classes

    static class transInfo {
      long cid_ ;
      long ts_;
      int totalNum_;
      int currentNum_;
      boolean pending_;
      int seqNum_;
      boolean old_;
      int salt_;
      boolean match_;
      long tid_;

      transInfo(long cid, long ts, int salt) {
        cid_ = cid;
        ts_ = ts;
        pending_ = false;
        currentNum_ = 0; 
        seqNum_ = 0;
        old_ = false;
        salt_ = salt;
        match_ = true;
      }
      public void setMatch(boolean v) { match_ = v; }
      public boolean getMatch() {return match_; }

      public void incSeqNum() { seqNum_++; }
      public int getSeqNum() { return seqNum_ ; }

      public long getCid() { return cid_; }
      public long getTs() {return ts_; } 

      public long getTid() { return tid_; }
      public void setTid(long tid) { tid_ = tid; }

      public void setTotalNum (int t) { totalNum_ = t; }
      public int getTotalNum() { return totalNum_ ; }
    
      public void setCurrentNum(int c) { currentNum_ = c; }
      public int getCurrentNum() { return currentNum_ ; }

      public void setPending (boolean b) { pending_ = b; }
      public boolean isPending() { return pending_ ; }

      public void setOld (boolean b) { old_ = b; }
      public boolean isOld() { return old_; }

      public int getSalt() { return salt_; }

    }

    static class tableInfoCacheForBinlogReaderParser {
      protected ConcurrentHashMap<String,ArrayList<AlignedFormatTupleParser>> tableDefs = new ConcurrentHashMap<String,ArrayList<AlignedFormatTupleParser>>();
      tableInfoCacheForBinlogReaderParser() {
    }

    void addTableDef(String t, String d, String k, long ts) {
      if(!tableDefs.containsKey(t) ) {
        ArrayList<AlignedFormatTupleParser> al = new ArrayList<AlignedFormatTupleParser>();
        AlignedFormatTupleParser parser = new AlignedFormatTupleParser();
        parser.setSchema(d);
        parser.setDelimeter(delimter);
        parser.setTimestamp(ts);
        if( k.equals("syskey"))
          parser.setNoPk(true);
        else
          parser.setKey(k);
        al.add(parser);
        tableDefs.put(t,al);
      }
      else {
        ArrayList<AlignedFormatTupleParser> al = tableDefs.get(t);
        boolean found = false;
        for(AlignedFormatTupleParser p : al) {
          if(p.getTimestamp() == ts) {
            found = true;
            break;
          } 
        }
        if(found == false) // add it to the list
        {
          AlignedFormatTupleParser parser = new AlignedFormatTupleParser();
          parser.setSchema(d);
          parser.setDelimeter(delimter);
          parser.setTimestamp(ts);
          if( k.equals("syskey"))
            parser.setNoPk(true);
          else
            parser.setKey(k);
          al.add(parser);
          tableDefs.put(t,al);
        }
      }
    }

    AlignedFormatTupleParser addTableDefToCache(String t, long ts) {
      AlignedFormatTupleParser retParser = null; 
      if(tableDefs.containsKey(t) ) {
        //try to get the parser which has timestamp less and close to ts 
        ArrayList<AlignedFormatTupleParser> al = tableDefs.get(t);
        //I assume the parser is added to the list order by generated time 
        for(AlignedFormatTupleParser p : al ) { 
          if(p.getTimestamp() < ts ) { retParser = p; break; } 
        }
        if(retParser != null ) //found in cache
        {
          return retParser; 
        }
      }
      //not found in cache try to get from meta family
      {
        try{
          //set the rowkey
          Get g = new Get(Bytes.toBytes(t));
          g.setMaxVersions(500);
          g.addFamily(BINLOG_META_FAMILY);
          Result rs = null;
          if(mtable == null) {
            rs  = table.get(g);          
            List<Cell> cells = rs.listCells();
            ArrayList<String> allCols = new ArrayList<String>();
            ArrayList<String> allKeys= new ArrayList<String>();
            ArrayList<Long> allTs = new ArrayList<Long>();

            if(null != cells && !cells.isEmpty()){
              for(Cell ce:cells){
                if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("key")) {
                  allKeys.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                  allTs.add(ce.getTimestamp() );
                }
                if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("col")) {
                  allCols.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                }
              }
              for( int i = 0 ; i < allCols.size(); i++) {
                String cols =  allCols.get(i);
                String keys = allKeys.get(i);
                long ts1 = allTs.get(i);
                addTableDef(t,cols, keys, ts1);
              }
            }
            else
            {
               AlignedFormatTupleParser validparser = new AlignedFormatTupleParser();
               validparser.setValid(false);
               ArrayList<AlignedFormatTupleParser> al = new  ArrayList<AlignedFormatTupleParser>();
               al.add(validparser);
               tableDefs.put(t, al);
               return null;
            }
          }
          else //new version, meta in mtable
          {
            rs  = mtable.get(g);
            List<Cell> cells = rs.listCells();
            ArrayList<String> allCols = new ArrayList<String>();
            ArrayList<String> allKeys= new ArrayList<String>();
            ArrayList<Long> allTs = new ArrayList<Long>();

            if(null != cells && !cells.isEmpty()){
              for(Cell ce:cells){
                if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("key")) {
                  allKeys.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                  allTs.add(ce.getTimestamp() );
                }
                if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("col")) {
                  allCols.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                }
              }
              for( int i = 0 ; i < allCols.size(); i++) {
                String cols =  allCols.get(i);
                String keys = allKeys.get(i);
                long ts1 = allTs.get(i);
                addTableDef(t,cols, keys, ts1);
              }
            }
            else
            {
              rs  = table.get(g);
              cells = rs.listCells();
              allCols = new ArrayList<String>();
              allKeys= new ArrayList<String>();
              allTs = new ArrayList<Long>();
 
              if(null != cells && !cells.isEmpty()){
                for(Cell ce:cells){
                  if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("key")) {
                    allKeys.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                    allTs.add(ce.getTimestamp() );
                  }
                  if(Bytes.toString(CellUtil.cloneQualifier(ce)).equals("col")) {
                    allCols.add( Bytes.toString(CellUtil.cloneValue(ce)) );
                  }
                }
                for( int i = 0 ; i < allCols.size(); i++) {
                  String cols =  allCols.get(i);
                  String keys = allKeys.get(i);
                  long ts1 = allTs.get(i);
                  addTableDef(t,cols, keys, ts1);
                }
              }
              else
              {
                 AlignedFormatTupleParser validparser = new AlignedFormatTupleParser();
                 validparser.setValid(false);
                 ArrayList<AlignedFormatTupleParser> al = new  ArrayList<AlignedFormatTupleParser>();
                 al.add(validparser);
                 tableDefs.put(t, al);
                 return null;
              }
            }
          } 
        }
        catch (IOException e) {
          LOG.warn("addTableDefToCache warn: ", e);
          return null;
        }  
      }

      //all versions of table definition put into the cache if not return yet
      ArrayList<AlignedFormatTupleParser> al = tableDefs.get(t);

      //in some cases, all tables were cretaed before
      //binlog then cleared, met_ popoulated with tools
      //so meta data timestamp is newer than all the log entries, in this case, use the latest one by default

      if(al.size() > 0) 
        retParser = al.get(0); //the latestest one

      for(AlignedFormatTupleParser p : al ) {
          if(p.getTimestamp() < ts ) { retParser = p; break; }
      }

      return retParser;
    }
  }  
}	
