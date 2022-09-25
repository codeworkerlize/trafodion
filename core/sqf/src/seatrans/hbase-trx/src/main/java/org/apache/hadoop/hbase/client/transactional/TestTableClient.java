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

// TestTableClient.java
  
package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSplitter;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;

import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;

public class TestTableClient {

  static final Log LOG = LogFactory.getLog(TestTableClient.class);
  static String regionname = "RegionName";
  static Table t = null;
  static Table t2 = null;
  static Table t3 = null;
  static Table t4 = null;
  static Table t5 = null;
  static long transactionId = 1L;
  static long scannerId = 0L;
  static boolean checkResult = false;
  static boolean hasMore = false;
  static long totalRows = 0L;
  static boolean continuePerform = true;
  static byte [][] startKeys = null;
  static int startPos = 0;
  static byte [] startRow = null;
  static byte [] lastRow = null;
  static List<HRegionLocation> regionsList = null;
  static List<HRegionLocation> regionsList2 = null;
  static List<HRegionLocation> regionsList3 = null;
  static List<HRegionLocation> regionsList4 = null;
  static List<HRegionLocation> regionsList5 = null;
  static int regionCount = 0;
  static Scan scan = null;
  static Pair<byte[][], byte[][]> startEndKeys = null;

  private static final String TABLE_NAME = "TestTable";
  private static final String NEWORDER_TABLE_NAME = "TRAFODION.JAVABENCH.OE_NEWORDER_48";
  private static final String ORDERS_TABLE_NAME = "TRAFODION.JAVABENCH.OE_ORDERS_48";
  private static final String ORDERLINE_TABLE_NAME = "TRAFODION.JAVABENCH.OE_ORDERLINE_48";
  private static final String CUSTOMER_TABLE_NAME = "TRAFODION.JAVABENCH.OE_CUSTOMER_48";

  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] FAMILYBAD = Bytes.toBytes("familybad");
  private static final byte[] QUAL_A = Bytes.toBytes("a");
  private static final byte[] QUAL_B = Bytes.toBytes("b");

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final byte[] ROW4 = Bytes.toBytes("row4");
  private static final byte[] ROW5 = Bytes.toBytes("row5");
  private static final byte[] ROW6 = Bytes.toBytes("row6");
  private static final byte [] VALUE1 = Bytes.toBytes(1);
  private static final byte [] VALUE2 = Bytes.toBytes(2);
  static Map<Long, org.apache.hadoop.hbase.client.transactional.TransactionState> mapTransactionStates = TransactionMap.getInstance();

  private static Connection connection;
  private static Admin admin;
  static TransactionManager trxManager;
  
  static void setupLog4j() {
      System.setProperty("trafodion.logdir", System.getenv("TRAF_LOG"));
      System.setProperty("hostName", System.getenv("HOSTNAME"));
      String confFile = System.getenv("TRAF_CONF")
          + "/log4j.dtm.config";
      PropertyConfigurator.configure(confFile);
	System.out.println("In seupLog4j"
			   + ", confFile: " + confFile
			   );
 }


 // Initialize and set up tables 
    public static void initialize() throws Exception {

     Configuration config = HBaseConfiguration.create();
     connection = ConnectionFactory.createConnection(config);

     HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
     HTableDescriptor desc2 = new HTableDescriptor(TableName.valueOf(NEWORDER_TABLE_NAME));
     HTableDescriptor desc3 = new HTableDescriptor(TableName.valueOf(ORDERS_TABLE_NAME));
     HTableDescriptor desc4 = new HTableDescriptor(TableName.valueOf(ORDERLINE_TABLE_NAME));
     HTableDescriptor desc5 = new HTableDescriptor(TableName.valueOf(CUSTOMER_TABLE_NAME));
     desc.addFamily(new HColumnDescriptor(FAMILY));
     desc2.addFamily(new HColumnDescriptor(FAMILY));
     desc3.addFamily(new HColumnDescriptor(FAMILY));
     desc4.addFamily(new HColumnDescriptor(FAMILY));
     desc5.addFamily(new HColumnDescriptor(FAMILY));
     admin  = connection.getAdmin();
     byte [][] splitKeys = new RegionSplitter.HexStringSplit().split(10);

     try {
       System.out.println ("  Cleaning up the table " + TABLE_NAME);
       admin.disableTable(TableName.valueOf(TABLE_NAME));
       admin.deleteTable(TableName.valueOf(TABLE_NAME));
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + TABLE_NAME + " is not enabled");
     }

     try {
       System.out.println ("  Cleaning up the table " + NEWORDER_TABLE_NAME);
       admin.disableTable(TableName.valueOf(NEWORDER_TABLE_NAME));
       admin.deleteTable(TableName.valueOf(NEWORDER_TABLE_NAME));
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + NEWORDER_TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + NEWORDER_TABLE_NAME + " is not enabled");
     }

     try {
       System.out.println ("  Cleaning up the table " + ORDERS_TABLE_NAME);
       admin.disableTable(TableName.valueOf(ORDERS_TABLE_NAME));
       admin.deleteTable(TableName.valueOf(ORDERS_TABLE_NAME));
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + ORDERS_TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + ORDERS_TABLE_NAME + " is not enabled");
     }

     try {
       System.out.println ("  Cleaning up the table " + ORDERLINE_TABLE_NAME);
       admin.disableTable(TableName.valueOf(ORDERLINE_TABLE_NAME));
       admin.deleteTable(TableName.valueOf(ORDERLINE_TABLE_NAME));
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + ORDERLINE_TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + ORDERLINE_TABLE_NAME + " is not enabled");
     }

     try {
       System.out.println ("  Cleaning up the table " + CUSTOMER_TABLE_NAME);
       admin.disableTable(TableName.valueOf(CUSTOMER_TABLE_NAME));
       admin.deleteTable(TableName.valueOf(CUSTOMER_TABLE_NAME));
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + CUSTOMER_TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + CUSTOMER_TABLE_NAME + " is not enabled");
     }

     System.out.println ("  Creating TransactionManager ");
     trxManager = TransactionManager.getInstance(config, connection);

     try {
       System.out.println ("  Creating the table " + TABLE_NAME);
       admin.createTable(desc, splitKeys);
     }
     catch (TableExistsException e) {
       System.out.println("  Table " + TABLE_NAME + " already exists");
     }

     try {
         System.out.println ("  Creating the table " + NEWORDER_TABLE_NAME);
         admin.createTable(desc2, splitKeys);
     }
     catch (TableExistsException e) {
         System.out.println("  Table " + NEWORDER_TABLE_NAME + " already exists");
     }

     try {
         System.out.println ("  Creating the table " + ORDERS_TABLE_NAME);
         admin.createTable(desc3, splitKeys);
     }
     catch (TableExistsException e) {
         System.out.println("  Table " + ORDERS_TABLE_NAME + " already exists");
     }

     try {
         System.out.println ("  Creating the table " + ORDERLINE_TABLE_NAME);
         admin.createTable(desc4, splitKeys);
     }
     catch (TableExistsException e) {
         System.out.println("  Table " + ORDERLINE_TABLE_NAME + " already exists");
     }

     try {
         System.out.println ("  Creating the table " + CUSTOMER_TABLE_NAME);
         admin.createTable(desc5, splitKeys);
     }
     catch (TableExistsException e) {
         System.out.println("  Table " + CUSTOMER_TABLE_NAME + " already exists");
     }

     t = connection.getTable(desc.getTableName());
     t2 = connection.getTable(desc2.getTableName());
     t3 = connection.getTable(desc3.getTableName());
     t4 = connection.getTable(desc4.getTableName());
     t5 = connection.getTable(desc5.getTableName());
     try {
       startKeys = connection.getRegionLocator(t.getName()).getStartKeys();
       startRow = startKeys[startPos];
       System.out.println("  Table " + TABLE_NAME + " startRow is " + startRow);
     } catch (IOException e) {
       System.out.println("  Table " + TABLE_NAME + " unable to get start keys" + e);
     }
     for (int i = 0; i < startKeys.length; i++){
     String regionLocation = connection.getRegionLocator(t.getName()).getRegionLocation(startKeys[i]).
        getHostname();
       System.out.println("  Table " + TABLE_NAME + " region location" + regionLocation
             + ", startKey is " + Hex.encodeHexString(startKeys[i]));
     }

     try {
        startEndKeys = connection.getRegionLocator(t.getName()).getStartEndKeys();
        for (int i = 0; i < startEndKeys.getFirst().length; i++) {
          System.out.println(" First key: " + Hex.encodeHexString(startEndKeys.getFirst()[i])
                +  ", Second key: "  + Hex.encodeHexString(startEndKeys.getSecond()[i]));
        }
     } catch (Exception e) {
       System.out.println("  Table " + TABLE_NAME + " unable to get start and endkeys" + e);
     }

     regionsList = connection.getRegionLocator(t.getName()).getAllRegionLocations();
     regionsList2 = connection.getRegionLocator(t2.getName()).getAllRegionLocations();
     regionsList3 = connection.getRegionLocator(t3.getName()).getAllRegionLocations();
     regionsList4 = connection.getRegionLocator(t4.getName()).getAllRegionLocations();
     regionsList5 = connection.getRegionLocator(t5.getName()).getAllRegionLocations();

     int first = 0;
     TransactionRegionLocation lastRegion = null;
     for (HRegionLocation regionLocation : regionsList) {
        HRegionInfo region = regionLocation.getRegionInfo();
        TransactionRegionLocation trl = new TransactionRegionLocation(regionLocation.getRegionInfo(),
                                                  regionLocation.getServerName(),0);
        if (first == 0) {
          regionname = region.getRegionNameAsString();
          first++;
        }
        lastRegion = trl;
        System.out.println("\t\t" + lastRegion);
     }
  }

  static public void testAbortTransaction() throws IOException {

    System.out.println("Starting testAbortTransaction");

    Batch.Call<TrxRegionService, AbortTransactionResponse> callable = 
        new Batch.Call<TrxRegionService, AbortTransactionResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<AbortTransactionResponse> rpcCallback = 
        new BlockingRpcCallback<AbortTransactionResponse>();         

      @Override
      public AbortTransactionResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.AbortTransactionRequest.Builder builder = AbortTransactionRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.abortTransaction(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], AbortTransactionResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (AbortTransactionResponse aresponse : result.values())
      {
        boolean hasException = aresponse.getHasException();
        String exception = aresponse.getException();
        if (hasException)
        {
          System.out.println("AbortTransactionResponse exception " + exception );
          throw new IOException(exception);
        }
      }

    System.out.println("Finished testAbortTransaction");
    return;
  } 

  static public TransactionState testBeginTransaction(long transactionId) throws IOException {

    System.out.println("Starting testBeginTransaction transactionId: " + transactionId);

    TransactionState tx = null;
    try {
       tx = trxManager.beginTransaction(transactionId);
    } catch (IdTmException ite) {
    	System.out.println("Begin Transaction Error caused by : " + ite.toString());
        throw new IOException("Begin Transaction Error caused by :", ite);
    }  
    System.out.println("beginTransaction, adding new TransactionState to map " + tx);
    mapTransactionStates.put(transactionId, tx);

    System.out.println("Finished testBeginTransaction tx: " + tx.getTransactionId());
    return tx;
  } 

  static public void testDisplayAllRegions() throws IOException {

    System.out.println("Starting testDisplayAllRegions: ");

    Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
    RegionLocator locator = connection.getRegionLocator(table.getName());
    List<HRegionLocation> locations = new ArrayList<HRegionLocation>();

    try{
       locations = locator.getAllRegionLocations();
    }
    catch (Exception e){
       System.out.println("Finished testDisplayAllRegions: " + e.toString());
    }
    finally {
       locator.close();
       table.close();
    }
    for (HRegionLocation location : locations) {
        System.out.println("location " + location);
    }

    System.out.println("Finished testDisplayAllRegions ");
  } 

  static public void testCheckAndDelete() throws IOException {

    System.out.println("Starting testCheckAndDelete");

    Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndDeleteResponse>();         

      @Override
      public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE1));
        Delete d = new Delete(ROW1);
        d.addColumns(FAMILY, QUAL_A);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d);
        builder.setDelete(m1);
        
        instance.checkAndDelete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndDeleteResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (CheckAndDeleteResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndDeleteResponse exception " + exception );
        else
          System.out.println("  testCheckAndDeleteResponse result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndDelete");
    return;
  } 

  static public void testCheckAndDelete2() throws IOException {

    System.out.println("Starting testCheckAndDelete2");

    Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndDeleteResponse>();         

      @Override
      public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_B));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE2));
        Delete d = new Delete(ROW1);
        d.addColumns(FAMILY, QUAL_B);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d);
        builder.setDelete(m1);
        
        instance.checkAndDelete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndDeleteResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (CheckAndDeleteResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndDelete2Response exception " + exception );
        else
          System.out.println("  testCheckAndDelete2Response result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndDelete2");
    return;
  } 

  static public void testCheckAndDelete4() throws IOException {

    System.out.println("Starting testCheckAndDelete");

    Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndDeleteResponse>();         

      @Override
      public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW2));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE2));
        Delete d = new Delete(ROW2);
        d.addColumns(FAMILY, QUAL_A);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d);
        builder.setDelete(m1);
        
        instance.checkAndDelete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndDeleteResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (CheckAndDeleteResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndDeleteResponse exception " + exception );
        else
          System.out.println("  testCheckAndDeleteResponse result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndDelete");
    return;
  } 

  static public void testCheckAndPut() throws IOException {

    System.out.println("Starting testCheckAndPut");
    final byte[] emptyVal = new byte[] {};

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndPutResponse>();         

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(emptyVal));
        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPutResponse exception " + exception );
        else
          System.out.println("  testCheckAndPutResponse result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndPut");
    return;
  } 

  static public void testCheckAndPut2() throws IOException {

    System.out.println("Starting testCheckAndPut2");

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndPutResponse>();         

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE1));
        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_B, Bytes.toBytes(2));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPut2Response exception " + exception );
        else
          System.out.println("  testCheckAndPut2Response result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndPut2");
    return;
  } 

  static public void testCheckAndPut3() throws IOException {

    System.out.println("Starting testCheckAndPut3");

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndPutResponse>();         

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE1));
        Put p = new Put(ROW2).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPut3Response exception " + exception );
        else
          System.out.println("  testCheckAndPut3Response result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndPut3");
    return;
  } 

  static public void testCheckAndPut4() throws IOException {

    System.out.println("Starting testCheckAndPut4");

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndPutResponse>();         

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW2));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE2));
        Put p = new Put(ROW2).addColumn(FAMILY, QUAL_A, Bytes.toBytes(2));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPut4Response exception " + exception );
        else
          System.out.println("  testCheckAndPut4Response result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndPut4");
    return;
  } 

  static public void testCloseScanner() throws IOException {

    System.out.println("Starting testClosecanner");

    Batch.Call<TrxRegionService, CloseScannerResponse> callable = 
        new Batch.Call<TrxRegionService, CloseScannerResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CloseScannerResponse> rpcCallback = 
        new BlockingRpcCallback<CloseScannerResponse>();         

      @Override
      public CloseScannerResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CloseScannerRequest.Builder builder = CloseScannerRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setScannerId(scannerId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        instance.closeScanner(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], CloseScannerResponse> result = null;   

    try {
      result = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Throwable e) {
      e.printStackTrace();     
    }

      for (CloseScannerResponse cresponse : result.values())
      {
        boolean hasException = cresponse.getHasException();
        String exception = cresponse.getException();
        if (hasException)
          System.out.println("  testCloseScannerResponse exception " + exception );
      }

    System.out.println("Finished testCloseScanner");
    return;
  } 

  static public void testCommit() throws IOException {

    System.out.println("Starting testCommit");

    Batch.Call<TrxRegionService, CommitResponse> callable = 
        new Batch.Call<TrxRegionService, CommitResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitResponse> rpcCallback = 
        new BlockingRpcCallback<CommitResponse>();         

      @Override
      public CommitResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequest.Builder builder = CommitRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setCommitId(transactionId);
        builder.setParticipantNum(1);
        builder.setTmTableCDCAttr(0);
        //builder.setGenerateCatchupMutations(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.commit(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (CommitResponse cresponse : result.values())
      {
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
        {
          System.out.println("  CommitResponse exception " + exception );
          throw new IOException(exception);
        }
      }

    System.out.println("Finished testCommit");
    return;
  } 

  static public void testCommitRequest() throws IOException {

    System.out.println("Starting testCommitRequest");

    Batch.Call<TrxRegionService, CommitRequestResponse> callable = 
        new Batch.Call<TrxRegionService, CommitRequestResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitRequestResponse> rpcCallback = 
        new BlockingRpcCallback<CommitRequestResponse>();         

      @Override
      public CommitRequestResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestRequest.Builder builder = CommitRequestRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setSavepointId(-1);
        builder.setParticipantNum(1);
        builder.setDropTableRecorded(false);
        builder.setSkipConflictDetection(false);
        builder.setStartEpoch(1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.commitRequest(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitRequestResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (CommitRequestResponse cresponse : result.values())
      {
        int value = cresponse.getResult();
        System.out.println("  CommitRequestResponse value " + value );
      }

    System.out.println("Finished testCommitRequest");
    return;
  } 

  static public void testCommitIfPossible() throws IOException {

    System.out.println("Starting testCommitIfPossible");

    Batch.Call<TrxRegionService, CommitIfPossibleResponse> callable = 
        new Batch.Call<TrxRegionService, CommitIfPossibleResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitIfPossibleResponse> rpcCallback = 
        new BlockingRpcCallback<CommitIfPossibleResponse>();         

      @Override
      public CommitIfPossibleResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitIfPossibleRequest.Builder builder = CommitIfPossibleRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.commitIfPossible(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitIfPossibleResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testCommitIfPossible " + result);
    return;
  } 

  static public void testDelete() throws IOException {

    System.out.println("Starting testDelete");

    Batch.Call<TrxRegionService, DeleteTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, DeleteTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<DeleteTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<DeleteTransactionalResponse>();         

      @Override
      public DeleteTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteTransactionalRequest.Builder builder = DeleteTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setSavepointId(-1);
        builder.setStartId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Delete d = new Delete(ROW1);
        d.addColumns(FAMILY, QUAL_A);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d);
        builder.setDelete(m1);
        
        instance.delete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], DeleteTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testDelete " + result);
    return;
  } 

  static public void testDeleteMultiple() throws IOException {

    System.out.println("Starting testDeleteMultiple");

    Batch.Call<TrxRegionService, DeleteMultipleTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, DeleteMultipleTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<DeleteMultipleTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<DeleteMultipleTransactionalResponse>();         

      @Override
      public DeleteMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.DeleteMultipleTransactionalRequest.Builder builder = DeleteMultipleTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Delete d1 = new Delete(ROW1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d1);
        builder.addDelete(m1);
        Delete d2 = new Delete(ROW2);
        MutationProto m2 = ProtobufUtil.toMutation(MutationType.DELETE, d2);
        builder.addDelete(m2);
        Delete d3 = new Delete(ROW3);
        MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, d3);
        builder.addDelete(m3);
        Delete d4 = new Delete(ROW4);
        MutationProto m4 = ProtobufUtil.toMutation(MutationType.DELETE, d4);
        builder.addDelete(m4);
        Delete d5 = new Delete(ROW5);
        MutationProto m5 = ProtobufUtil.toMutation(MutationType.DELETE, d5);
        builder.addDelete(m5);
        Delete d6 = new Delete(ROW6);
        MutationProto m6 = ProtobufUtil.toMutation(MutationType.DELETE, d6);
        builder.addDelete(m6);
        
        instance.deleteMultiple(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], DeleteMultipleTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testDeleteMultiple " + result);
    return;
  } 

  static public void testGet() throws IOException {

    System.out.println("Starting testGet");

    Batch.Call<TrxRegionService, GetTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, GetTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<GetTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<GetTransactionalResponse>();         

      @Override
      public GetTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalRequest.Builder builder = GetTransactionalRequest.newBuilder();        
        //Get get = new Get(ROW1).addColumn(FAMILY, Bytes.toBytes(1));
        Get get = new Get(ROW1).addColumn(FAMILY, QUAL_A);
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.get(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], GetTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      for (GetTransactionalResponse gresponse : result.values())
      {
        Result resultFromGet = ProtobufUtil.toResult(gresponse.getResult());
        System.out.println("GetTransactionalResponse Get result before action is committed:" + resultFromGet.size() + ":" + resultFromGet);
      }

    System.out.println("Finished testGet");
    return;
  } 

  static public void testPut() throws IOException {

    System.out.println("Starting testPut");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut " + result);
    return;

  }

  static public void testPut1() throws IOException {

    System.out.println("Starting testPut1");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut1 " + result);
    return;
}

  static public void testPut2() throws IOException {

    System.out.println("Starting testPut2");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW2).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut2 " + result);
    return;
}

  static public void testPut3() throws IOException {

    System.out.println("Starting testPut3");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW3).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut3 " + result);
    return;
}

  static public void testPut4() throws IOException {

    System.out.println("Starting testPut4");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW4).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut4 " + result);
    return;
}

  static public void testPut5() throws IOException {

    System.out.println("Starting testPut5");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW5).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut5 " + result);
    return;
}

  static public void testPut6() throws IOException {

    System.out.println("Starting testPut6");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW6).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPut6 " + result);
    return;
  } 

  static public void testPutException() throws IOException {

    System.out.println("Starting testPutException");

    Batch.Call<TrxRegionService, PutTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutTransactionalResponse>();         

      @Override
      public PutTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutTransactionalRequest.Builder builder = PutTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILYBAD, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPutException " + result);
    return;
  } 

  static public void testPutMultiple() throws IOException {

    System.out.println("Starting testPutMultiple");

    Batch.Call<TrxRegionService, PutMultipleTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, PutMultipleTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PutMultipleTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<PutMultipleTransactionalResponse>();         

      @Override
      public PutMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PutMultipleTransactionalRequest.Builder builder = PutMultipleTransactionalRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, Bytes.toBytes(1));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.addPut(m1);
        
        instance.putMultiple(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutMultipleTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

    System.out.println("Finished testPutMultiple " + result);
    return;
  } 

  static public void testPerformScan() throws IOException {

    System.out.println("Starting testPerformScan");

    Batch.Call<TrxRegionService, PerformScanResponse> callable = 
        new Batch.Call<TrxRegionService, PerformScanResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PerformScanResponse> rpcCallback = 
        new BlockingRpcCallback<PerformScanResponse>();         

      @Override
      public PerformScanResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PerformScanRequest.Builder builder = PerformScanRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setScannerId(0);
        builder.setNumberOfRows(3);
        builder.setCloseScanner(false);
        builder.setNextCallSeq(0);

        instance.performScan(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], PerformScanResponse> presult = null;   
    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result[]
    results = null;


    try {
      presult = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Throwable e) {
      e.printStackTrace();     
    }

      int count = 0;
      boolean hasMore = false;

      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result
        result = null;
            
      for (PerformScanResponse presponse : presult.values())
      {
        if (presponse.getHasException())
        {
          String exception = presponse.getException();
          System.out.println("  testPerformScanResponse exception " + exception );
        }
        else
        {
          count = presponse.getResultCount();
          results = 
            new org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result[count];

          for (int i = 0; i < count; i++)
          {
            result = presponse.getResult(i);
            hasMore = presponse.getHasMore();
            results[i] = result;
            result = null;
            System.out.println("  testPerformScan response count " + count + ", hasMore is " + hasMore + ", result " + results[i] );
          }
        }
      } 

    System.out.println("Finished testPerformScan");
    return;
  } 

  static public void testOpenScanner() throws IOException {

    System.out.println("Starting testOpenScanner");

    Batch.Call<TrxRegionService, OpenScannerResponse> callable = 
        new Batch.Call<TrxRegionService, OpenScannerResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<OpenScannerResponse> rpcCallback = 
        new BlockingRpcCallback<OpenScannerResponse>();         

      @Override
      public OpenScannerResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.OpenScannerRequest.Builder builder = OpenScannerRequest.newBuilder();        
        builder.setTransactionId(transactionId);
        builder.setStartId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Scan scan = new Scan();
        scan.addColumn(FAMILY, Bytes.toBytes(1));

        builder.setScan(ProtobufUtil.toScan(scan));
        
        instance.openScanner(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], OpenScannerResponse> result = null;   

    try {
      result = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Throwable e) {
      e.printStackTrace();     
    }

      for (OpenScannerResponse oresponse : result.values())
      {
        scannerId = oresponse.getScannerId();
        String exception = oresponse.getException();
        boolean hasException = oresponse.getHasException();
        if (hasException)
          System.out.println("  testOpenScannerResponse exception " + exception );
        else
          System.out.println("  testOpenScannerResponse scannerId is " + scannerId );
      }

    System.out.println("Finished testOpenScanner");
    return;
  } 

  static public void testRecoveryRequest() throws IOException {

    System.out.println("Starting testRecoveryRequest");

    Batch.Call<TrxRegionService, RecoveryRequestResponse> callable = 
        new Batch.Call<TrxRegionService, RecoveryRequestResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<RecoveryRequestResponse> rpcCallback = 
        new BlockingRpcCallback<RecoveryRequestResponse>();         

      @Override
      public RecoveryRequestResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.RecoveryRequestRequest.Builder rbuilder = RecoveryRequestRequest.newBuilder();        
        rbuilder.setTransactionId(transactionId);
        rbuilder.setRegionName(ByteString.copyFromUtf8(regionname));
        rbuilder.setTmId(7);
        
        instance.recoveryRequest(controller, rbuilder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], RecoveryRequestResponse> rresult = null;   
      try {
        rresult = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        e.printStackTrace();     
      }

      int count = 0;
      long l = 0;
            
      for (RecoveryRequestResponse rresponse : rresult.values())
      {
        count = rresponse.getResultCount();
        l = rresponse.getResult(0);
        System.out.println("  testRecoveryResponse count " + count + ", result " + l );
      }

      System.out.println("Finished testRecoveryRequest");
      return;
  }
  
  public static boolean testRegisterRegion(final TransactionState transactionState, TransactionRegionLocation location)throws IOException{

	  System.out.println("testRegisterRegion ENTRY, transactionState:" + transactionState
      		+ " location: " + location);

	  boolean added = false;
      if(transactionState.addRegion(location)){
        System.out.println("testRegisterRegion -- added region: " + location + " to tx " + transactionState.getTransactionId()
                + " [peer id: " + location.peerId + "]");
        added = true;
      }
      else {
    	  System.out.println("testRegisterRegion -- region previously added: " + location);
      }
      System.out.println("testRegisterRegion EXITm added: " + added);
      return added;
  }

  
  static public void main(String[] args) {
    
    System.out.println("Starting TestTableClient");

    try {
      initialize();
    
      //Should be transaction transactionId 1

      System.out.println("TestTableClient initialization comeplete");

      testBeginTransaction(transactionId);
      testDisplayAllRegions();

      HRegionLocation hrl = null;
      for (HRegionLocation regionLocation : regionsList) {
         hrl = regionLocation;
      }
      TransactionRegionLocation trl = new TransactionRegionLocation(hrl.getRegionInfo(), hrl.getServerName(), 0);

      HRegionLocation hrl2 = null;
      for (HRegionLocation regionLocation : regionsList2) {
         hrl2 = regionLocation;
      }
      TransactionRegionLocation trl2 = new TransactionRegionLocation(hrl2.getRegionInfo(), hrl2.getServerName(), 0);

      HRegionLocation hrl3 = null;
      for (HRegionLocation regionLocation : regionsList3) {
         hrl3 = regionLocation;
      }
      TransactionRegionLocation trl3 = new TransactionRegionLocation(hrl3.getRegionInfo(), hrl3.getServerName(), 0);

      HRegionLocation hrl4 = null;
      for (HRegionLocation regionLocation : regionsList4) {
         hrl4 = regionLocation;
      }
      TransactionRegionLocation trl4 = new TransactionRegionLocation(hrl4.getRegionInfo(), hrl4.getServerName(), 0);

      HRegionLocation hrl5 = null;
      for (HRegionLocation regionLocation : regionsList5) {
         hrl5 = regionLocation;
      }
      TransactionRegionLocation trl5 = new TransactionRegionLocation(hrl5.getRegionInfo(), hrl5.getServerName(), 0);

      TransactionState tx = mapTransactionStates.get(transactionId);
      try {
         testRegisterRegion(tx, trl);
         testRegisterRegion(tx, trl2);
         testRegisterRegion(tx, trl3);
         testRegisterRegion(tx, trl4);
         testRegisterRegion(tx, trl5);
         testRegisterRegion(tx, trl);
         testRegisterRegion(tx, trl);
      } catch (IOException e) {
         System.out.println("exception in testRegisterRegion call, txId: " + transactionId +
              " retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException " + e);
         System.exit(1);
      }

//      testPut();
//      testGet();
//     testCommitRequest();
//      testCommit();

//      transactionId++; //Should be 2

//      testBeginTransaction(transactionId);
//      testDelete();
//      testGet();
//      testCommitRequest();
//      testCommit();

//      transactionId++; //Should be 3

//      testBeginTransaction(transactionId);
//      testGet();
//      testCommitIfPossible();

//      transactionId++; //Should be 4

//      testBeginTransaction(transactionId);
///      testCheckAndPut();
//     testGet();
//      testCommitRequest();
//      testCommit();

//      transactionId++; //Should be 5

//      testBeginTransaction(transactionId);
//      testGet();
//      testCommitIfPossible();

//      transactionId++; //Should be 6

//      testBeginTransaction(transactionId);
//      testCheckAndDelete();
//      testGet();
//      testCommitRequest();
//      testCommit();

//      transactionId++; //Should be 7

//      testBeginTransaction(transactionId);
//      testGet();
//      testCommitIfPossible();

    } catch (IOException e) {
      System.out.println("TestTableClient threw IOException");
      System.out.println(e.toString());
    } catch (Throwable t) {
      System.out.println("TestTableClient threw throwable exception");
      System.out.println(t.toString());
    }

    System.out.println("Finished TestTableClient");
  }

}
