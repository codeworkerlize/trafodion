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

// TrxTableClient3.java
  
package org.apache.hadoop.hbase.coprocessor.transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

public class TrxTableClient3 {

  static String regionname = "RegionName";
  static Connection connection;
  static Table t = null;
  static long id = 1L;
  static long startIdVal = 1L;
  static long commitIdVal = 1L;
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
  static int regionCount = 0;
  static Scan scan = null;
  static Pair<byte[][], byte[][]> startEndKeys = null;

  private static IdTm idServer;
  private static final int ID_TM_SERVER_TIMEOUT = 100000;
  private static final String TABLE_NAME = "table3";

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
  private static final byte [] VALUE4 = Bytes.toBytes(4);

  private static Admin admin;

 // Initialize and set up tables 
    public static void initialize() throws Exception {
 
     Configuration config = HBaseConfiguration.create();

     HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
     desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionEndpoint");
     desc.addFamily(new HColumnDescriptor(FAMILY));
     connection = ConnectionFactory.createConnection(config);
     admin  = connection.getAdmin();
     
     System.out.println("TrxTableClient3 start ");
     String preload = System.getenv("LD_PRELOAD");
     if (preload == null){
        System.out.println("\n*** LD_PRELOAD not configured.  Should \"export LD_PRELOAD=${JAVA_HOME}/jre/lib/${JRE_LIB_DIR}/libjsig.so:${TRAF_HOME}/export/lib${SQ_MBTYPE}/libseabasesig.so\" ***\n");
        System.exit(1);
     }
     else{
        System.out.println("\n*** LD_PRELOAD configured: " + preload + " ***\n");
     }

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
       System.out.println ("  Creating the table " + TABLE_NAME);
       admin.createTable(desc);
     }
     catch (TableExistsException e) {
       System.out.println("  Table " + TABLE_NAME + " already exists");
     }

     t = connection.getTable(desc.getTableName());
     try {
       startKeys = connection.getRegionLocator(t.getName()).getStartKeys();
       startRow = startKeys[startPos];
       System.out.println("  Table " + TABLE_NAME + " startRow is " + startRow);
     } catch (IOException e) {
       System.out.println("  Table " + TABLE_NAME + " unable to get start keys " + e);
     }
     for (int i = 0; i < startKeys.length; i++){
     String regionLocation = connection.getRegionLocator(t.getName()).getRegionLocation(startKeys[i]).
        getHostname();
       System.out.println("  Table " + TABLE_NAME + " region location " + regionLocation + ", startKey is " + startKeys[i]);
     }

     try {
        startEndKeys = connection.getRegionLocator(t.getName()).getStartEndKeys();
        for (int i = 0; i < startEndKeys.getFirst().length; i++) {
          System.out.println(" First key: " + startEndKeys.getFirst()[i] +  ", Second key: "  + startEndKeys.getSecond()[i]);
        }
     } catch (Exception e) {
       System.out.println("  Table " + TABLE_NAME + " unable to get start and endkeys " + e);
     }

     regionsList = connection.getRegionLocator(t.getName()).getAllRegionLocations();
     System.out.println("  Table " + TABLE_NAME + " has " + regionsList.size() + " regons");

     int first = 0;
     for (HRegionLocation regionLocation : regionsList) {
        HRegionInfo region = regionLocation.getRegionInfo();
        if (first == 0) {
          regionname = region.getRegionNameAsString();
          first++;
        }
          
        System.out.println("\t\t" + region.getRegionNameAsString());
     }
     try {
         idServer = new IdTm(false);
         System.out.println("  New IdTm created");
      }
      catch (Exception e){
         System.out.println("Exception creating new IdTm: " + e);
      }
      IdTmId startId;
      try {
        System.out.println("initialize() getting new startId");
        startId = new IdTmId();
        System.out.println("initialize() created new IdTmId");
        idServer.id(ID_TM_SERVER_TIMEOUT, startId);
        System.out.println("initialize() idServer.id returned: " + startId.val);
      } catch (Throwable exc) {
         System.out.println("initialize()  : IdTm threw exception " + exc);
         throw new IdTmException("initialize() : IdTm threw exception " + exc);
      }
      startIdVal = startId.val;

  }

    static public long getIdTmVal() throws Exception {
       IdTmId LvId;
       try {
          LvId = new IdTmId();
          System.out.println("getIdTmVal getting new Id");
          idServer.id(ID_TM_SERVER_TIMEOUT, LvId);
          System.out.println("getIdTmVal idServer.id returned: " + LvId.val);
       	  return LvId.val;
        } catch (IdTmException exc) {
           System.out.println("getIdTmVal : IdTm threw exception " + exc);
           throw new IdTmException("getIdTmVal : IdTm threw exception " + exc);
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
        builder.setTransactionId(id);
        builder.setParticipantNum(1);
        builder.setDropTableRecorded(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.abortTransaction(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], AbortTransactionResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, callable);
      } catch (Throwable e) {
         System.out.println("  AbortTransactionResponse exception " + e );
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

  static public void testBeginTransaction() throws IOException {

    System.out.println("\nStarting testBeginTransaction with Id " + id);

    try{
    	startIdVal = getIdTmVal();
    }
    catch (Exception e){
        System.out.println("getIdTmVal threw exception " + e);
        e.printStackTrace();
    }

    Batch.Call<TrxRegionService, BeginTransactionResponse> callable = 
        new Batch.Call<TrxRegionService, BeginTransactionResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<BeginTransactionResponse> rpcCallback = 
        new BlockingRpcCallback<BeginTransactionResponse>();         

      @Override
      public BeginTransactionResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.BeginTransactionRequest.Builder builder = BeginTransactionRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.beginTransaction(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], BeginTransactionResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testBeginTransaction exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testBeginTransaction throwable " + t );         
      }

    System.out.println("Finished testBeginTransaction ");
    return;
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
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
      } catch (Exception e) {
          System.out.println("  testCheckAndDelete exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCheckAndDelete throwable " + t );         
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

  static public void testCheckAndDeleteE() throws IOException {

    System.out.println("Starting testCheckAndDeleteE");
    final byte[] emptyVal = new byte[] {};

    Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndDeleteResponse>();         

      @Override
      public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(emptyVal));
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
      } catch (Exception e) {
          System.out.println("  testCheckAndDeleteE exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCheckAndDeleteE throwable " + t );         
      }

      for (CheckAndDeleteResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndDeleteEResponse exception " + exception );
        else
          System.out.println("  testCheckAndDeleteEResponse result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndDeleteE");
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW2));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_B));
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
      } catch (Exception e) {
          System.out.println("  testCheckAndDelete2 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCheckAndDelete2 throwable " + t );         
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

    System.out.println("Starting testCheckAndDelete4");

    Batch.Call<TrxRegionService, CheckAndDeleteResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndDeleteResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndDeleteResponse>();         

      @Override
      public CheckAndDeleteResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW4));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_B));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE4));
        Delete d = new Delete(ROW4);
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
      } catch (Exception e) {
          System.out.println("  testCheckAndDelete4 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCheckAndDelete4 throwable " + t );         
      }

      for (CheckAndDeleteResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndDelete4Response exception " + exception );
        else
          System.out.println("  testCheckAndDelete4Response result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndDelete4");
    return;
  } 

  static public void testCheckAndPutE() throws IOException {

    System.out.println("Starting testCheckAndPutE");
    final byte[] emptyVal = new byte[] {};

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable = 
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback = 
        new BlockingRpcCallback<CheckAndPutResponse>();         

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(emptyVal));
        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, emptyVal);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPutE exception " + e );     
          e.printStackTrace();
      } catch (Throwable t) {
         System.out.println("  testCheckAndPutE throwable " + t );     
         t.printStackTrace();
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPutEResponse exception " + exception );
        else
          System.out.println("  testCheckAndPutEResponse result is  " + checkResult);
      }

    System.out.println("Finished testCheckAndPutE");
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW1));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(emptyVal));
        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPut exception " + e );     
          e.printStackTrace();
      } catch (Throwable t) {
         System.out.println("  testCheckAndPut throwable " + t );     
         t.printStackTrace();
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

    System.out.println("Finished testCheckAndPut " + result);
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW2));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE1));
        Put p = new Put(ROW2).addColumn(FAMILY, QUAL_B, VALUE2);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPut2 exception " + e );     
          e.printStackTrace();
      } catch (Throwable t) {
         System.out.println("  testCheckAndPut2 throwable " + t );     
         t.printStackTrace();
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW3));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_A));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE1));
        Put p = new Put(ROW3).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPut3 exception " + e );
      } catch (Throwable t) {
          System.out.println("  testCheckAndPut3 throwable " + t );
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW4));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_B));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE2));
        Put p = new Put(ROW4).addColumn(FAMILY, QUAL_B, Bytes.toBytes(4));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CheckAndPutResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPut4 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCheckAndPut4 throwable " + t );         
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

  static public void testCheckAndPut5() throws IOException {

    System.out.println("Starting testCheckAndPut5");

    Batch.Call<TrxRegionService, CheckAndPutResponse> callable =
        new Batch.Call<TrxRegionService, CheckAndPutResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CheckAndPutResponse> rpcCallback =
        new BlockingRpcCallback<CheckAndPutResponse>();

      @Override
      public CheckAndPutResponse call(TrxRegionService instance) throws IOException {
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipCheck(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setRow(HBaseZeroCopyByteString.wrap(ROW5));
        builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(QUAL_B));
        builder.setValue(HBaseZeroCopyByteString.wrap(VALUE2));
        Put p = new Put(ROW5).addColumn(FAMILY, QUAL_B, Bytes.toBytes(4));
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);

        instance.checkAndPut(controller, builder.build(), rpcCallback);
        return rpcCallback.get();
      }
    };

      Map<byte[], CheckAndPutResponse> result = null;
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCheckAndPut5 exception " + e );
      } catch (Throwable t) {
          System.out.println("  testCheckAndPut5 throwable " + t );
      }
      for (CheckAndPutResponse cresponse : result.values())
      {
        checkResult = cresponse.getResult();
        String exception = cresponse.getException();
        boolean hasException = cresponse.getHasException();
        if (hasException)
          System.out.println("  testCheckAndPut5Response exception " + exception );
        else
          System.out.println("  testCheckAndPut5Response result is  " + checkResult);
      }
	    System.out.println("Finished testCheckAndPut5");
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
        builder.setTransactionId(id);
        builder.setScannerId(scannerId);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        instance.closeScanner(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], CloseScannerResponse> result = null;   

    try {
      result = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Exception e) {
        System.out.println("  testCloseScanner exception " + e );         
    } catch (Throwable t) {
        System.out.println("  testCloseScanner throwable " + t );         
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

    System.out.println("Starting testCommit for transid " + id);

    try{
      commitIdVal = getIdTmVal();
    }
    catch (Exception e){
      System.out.println("getIdTmVal threw exception " + e);
      e.printStackTrace();
    }

    Batch.Call<TrxRegionService, CommitResponse> callable = 
        new Batch.Call<TrxRegionService, CommitResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitResponse> rpcCallback = 
        new BlockingRpcCallback<CommitResponse>();         

      @Override
      public CommitResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequest.Builder builder = CommitRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setParticipantNum(1);
        builder.setTmTableCDCAttr(0);
        builder.setCommitId(commitIdVal);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.commit(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCommit exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCommit throwable " + t );         
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

  static public int testCommitRequest() throws IOException {

    System.out.println("Starting testCommitRequest for transid " + id);

    boolean sendAgain = true;
    int returnValue = -1;
    while (sendAgain){
      Batch.Call<TrxRegionService, CommitRequestResponse> callable = 
        new Batch.Call<TrxRegionService, CommitRequestResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitRequestResponse> rpcCallback = 
        new BlockingRpcCallback<CommitRequestResponse>();         

      @Override
      public CommitRequestResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitRequestRequest.Builder builder = CommitRequestRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setSavepointId(-1);
        builder.setStartEpoch(startIdVal);
        builder.setDropTableRecorded(false);
        builder.setSkipConflictDetection(false);

        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setParticipantNum(1);
        
        instance.commitRequest(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitRequestResponse> result = null;   
      try {
        System.out.println("  testCommitRequest sending request " );         
        result = t.coprocessorService(TrxRegionService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, callable);
        System.out.println("  testCommitRequest request sent; result " + result );         
      } catch (Exception e) {
          System.out.println("  testCommitRequest exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCommitRequest exception " + t );         
      }

      for (CommitRequestResponse cresponse : result.values())
      {
        System.out.println("  testCommitRequest getting value " );         
        int value = cresponse.getResult();
        System.out.println("  CommitRequestResponse value " + value);
        if (value == 9){ // PREPARE_REFRESH
           System.out.println("  CommitRequestResponse received PREPARE_REFRESH; sending again ");
        }
        else {
            System.out.println("  CommitRequestResponse response complete ");      
            sendAgain = false;
        }
        returnValue = value;
      }
    }

    System.out.println("Finished testCommitRequest returning " + returnValue);
    return returnValue;
  } 

  static public void testCommitIfPossible() throws IOException {

    System.out.println("Starting testCommitIfPossible");

    try{
    	commitIdVal = getIdTmVal();
    }
    catch (Exception e){
        System.out.println("getIdTmVal threw exception " + e);
        e.printStackTrace();
    }

    Batch.Call<TrxRegionService, CommitIfPossibleResponse> callable = 
        new Batch.Call<TrxRegionService, CommitIfPossibleResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<CommitIfPossibleResponse> rpcCallback = 
        new BlockingRpcCallback<CommitIfPossibleResponse>();         

      @Override
      public CommitIfPossibleResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.CommitIfPossibleRequest.Builder builder = CommitIfPossibleRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartEpoch(startIdVal);
        builder.setCommitId(commitIdVal);
        builder.setParticipantNum(1);
        builder.setTmTableCDCAttr(0);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.commitIfPossible(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], CommitIfPossibleResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testCommitIfPossible exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testCommitIfPossible throwable " + t );         
      }

      for (CommitIfPossibleResponse response : result.values())
      {
        if (response.hasException()){
           System.out.println("CommitIfPossibleResponse exception: " + response.getException());
        }
        else {
            System.out.println("CommitIfPossibleResponse: success");        	
        }
      }
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Delete d = new Delete(ROW1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.DELETE, d);
        builder.setDelete(m1);
        
        instance.delete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], DeleteTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testDelete exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testDelete throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
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
      } catch (Exception e) {
          System.out.println("  testDeleteMultiple exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testDeleteMultiple throwable " + t );         
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
        Get get = new Get(ROW1).addColumn(FAMILY, QUAL_A);
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipScanConflict(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.get(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], GetTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testGetTransactional exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testGetTransactional throwable " + t );         
      }

      for (GetTransactionalResponse gresponse : result.values())
      {
        Result resultFromGet = ProtobufUtil.toResult(gresponse.getResult());
        System.out.println("GetTransactionalResponse Get result before action is committed:" + resultFromGet.size() + ":" + Bytes.toStringBinary(resultFromGet.getRow()));
      }

    System.out.println("Finished testGet");
    return;
  } 

  static public void testGetMultiple() throws IOException {

    System.out.println("Starting testGetMultiple");

    Batch.Call<TrxRegionService, GetMultipleTransactionalResponse> callable =
        new Batch.Call<TrxRegionService, GetMultipleTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<GetMultipleTransactionalResponse> rpcCallback =
        new BlockingRpcCallback<GetMultipleTransactionalResponse>();

      @Override
      public GetMultipleTransactionalResponse call(TrxRegionService instance) throws IOException {
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetMultipleTransactionalRequest.Builder builder = GetMultipleTransactionalRequest.newBuilder();
        Get get1 = new Get(ROW1).addColumn(FAMILY, QUAL_A);
        Get get2 = new Get(ROW3).addColumn(FAMILY, QUAL_A);
        Get get3 = new Get(ROW5).addColumn(FAMILY, QUAL_A);
        ArrayList<Get> list = new ArrayList<Get>();
        list.add(get1);
        list.add(get2);
        list.add(get3);

        for (Get get : list){
          builder.addGet(ProtobufUtil.toGet(get));
        }

//        builder.setGetList(ProtobufUtil.toGet(list));
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setSkipScanConflict(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        instance.getMultiple(controller, builder.build(), rpcCallback);
        return rpcCallback.get();
      }
    };

      Map<byte[], GetMultipleTransactionalResponse> result = null;
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testGetMultipleTransactional exception " + e );
      } catch (Throwable t) {
          System.out.println("  testGetMultipleTransactional throwable " + t );
      }
      System.out.println("GetMultipleTransactionalResponse GetMultiple returned result: " + result.size() + ":" + result);

      int i = 0;
      for (GetMultipleTransactionalResponse gresponse : result.values())
      {
        for (int n = 0; n < gresponse.getCount(); n++){
           Result resultFromGet = ProtobufUtil.toResult(gresponse.getResult(n));
           System.out.println("GetMultipleTransactionalResponse Get(" + n + ") from region(" + i + ") result before action is committed: " + resultFromGet.size() + ":" + resultFromGet);
        }
        i++;
      }

    System.out.println("Finished testGetMultiple");
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut1 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut1 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW2).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut2 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut2 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW3).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut3 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut3 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW4).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut4 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut4 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW5).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut5 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut5 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSavepointId(-1);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW6).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPut6 exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPut6 throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILYBAD, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.setPut(m1);
        
        instance.put(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPutException exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPutException throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Put p = new Put(ROW1).addColumn(FAMILY, QUAL_A, VALUE1);
        MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);
        builder.addPut(m1);
        
        instance.putMultiple(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], PutMultipleTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testPutMultiple exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testPutMultiple throwable " + t );         
      }

    System.out.println("Finished testPutMultiple " + result);
    return;
  } 

  static public void testPerformScan(final int nextCallSeq) throws IOException {

    System.out.println("Starting testPerformScan nextSeq " + nextCallSeq);

    Batch.Call<TrxRegionService, PerformScanResponse> callable = 
        new Batch.Call<TrxRegionService, PerformScanResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<PerformScanResponse> rpcCallback = 
        new BlockingRpcCallback<PerformScanResponse>();         

      @Override
      public PerformScanResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PerformScanRequest.Builder builder = PerformScanRequest.newBuilder();        
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        builder.setScannerId(scannerId);
        builder.setNumberOfRows(3);
        builder.setCloseScanner(false);
        builder.setNextCallSeq(nextCallSeq);
        builder.setAutoOpen(false);
        builder.setSkipScanConflict(false);

        Scan scan = new Scan();
        scan.addColumn(FAMILY, QUAL_A);
        builder.setScan(ProtobufUtil.toScan(scan));

        instance.performScan(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], PerformScanResponse> presult = null;   
    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result[]
    results = null;


    try {
      presult = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Exception e) {
        System.out.println("  testPerformScan exception " + e );         
    } catch (Throwable t) {
        System.out.println("  testPerformScan throwable " + t );         
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
        builder.setSkipScanConflict(false);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));

        Scan scan = new Scan();
        scan.addColumn(FAMILY, QUAL_A);

        builder.setScan(ProtobufUtil.toScan(scan));
        
        instance.openScanner(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], OpenScannerResponse> result = null;   

    try {
      result = t.coprocessorService(TrxRegionService.class, null, null, callable);
    } catch (Exception e) {
        System.out.println("  testOpenScannerResponse exception " + e );         
    } catch (Throwable t) {
        System.out.println("  testOpenScannerResponse throwable " + t );         
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
        rbuilder.setTransactionId(id);
        rbuilder.setRegionName(ByteString.copyFromUtf8(regionname));
        rbuilder.setTmId(7);
        
        instance.recoveryRequest(controller, rbuilder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], RecoveryRequestResponse> rresult = null;   
      try {
        rresult = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Exception e) {
          System.out.println("  testRecoveryResponse exception " + e );         
      } catch (Throwable t) {
          System.out.println("  testRecoveryResponse throwable " + t );         
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

  static public void main(String[] args) {
    
    System.out.println("Starting TrxTableClient3");

    try {
      initialize();
      int prepareResult = -1;
      //Should be transaction id 1

      testBeginTransaction();
      testCheckAndPut(); // should return true
      // Can't use testCommitIfPossible on the 1st request because
      // we expect a PREPARE_REFRESH response
      prepareResult = testCommitRequest();
      if (prepareResult == 9) /* PREPARE_REFRESH */ {
         prepareResult = testCommitRequest();
      }
      if (prepareResult == 1){
         testCommit();
      }

//      id++; //Should be 2 
//      testBeginTransaction();
//      System.out.println("TestOpenScanner open a new scanner");
//      testOpenScanner();
//      System.out.println("TestPerformScan get rows 1 through 3");
//      testPerformScan(0);  // Get the first three
//      prepareResult = testCommitRequest();
//      if (prepareResult == 1){
//         testCommit();
//      }

//      id++; //Should be 3 
//      testBeginTransaction();
//      testCheckAndPut(); // should return false
//      testCommitIfPossible();

//      id++; //Should be 4
//      testBeginTransaction();
//      testCheckAndPut2(); // should return true 
//      testCommitIfPossible();

//      id++; //Should be 5
//      testBeginTransaction();
//      testCheckAndPut3(); // should return true
//      testCommitIfPossible();

//      id++; //Should be 6
//      testBeginTransaction();
//      testGet();             // Should have a row
//      testCommitIfPossible();

      id++; //Should be 7
      testBeginTransaction();
      testCheckAndDelete();  // Should return true
      testCommitIfPossible();

//      id++; //Should be 8
//      testBeginTransaction();
//      testGet();             // Should not have a row
//      testCommitIfPossible();

//      id++; //Should be 9
//      testBeginTransaction();
//      testDeleteMultiple();
//      prepareResult = testCommitRequest();
//      if (prepareResult == 1){
//         testCommit();
//      }

//      id++; //Should be 10
//      testBeginTransaction();
//      testGet();             // Should not have a row
//      testCommitIfPossible();

      id++; //Should be 11
      testBeginTransaction();
      testGet();            // should NOT show row1
      testCheckAndPut();    // should return true
      testGet();            // should show row1
      System.out.println("testGet complete for id " + id);
//      try {
//          Thread.sleep(60000);          ///1000 milliseconds is one second.
//      } catch(InterruptedException ex) {
//            Thread.currentThread().interrupt();
//      }
      testCheckAndDelete(); // should return true
      testGet();            // should NOT show row1
      testCommitIfPossible();

      id++; //Should be 12
      testBeginTransaction();
      testGet();              // should have no rows
      testCommitIfPossible();
  
      id++; //Should be 13
      testBeginTransaction();
      testCheckAndPut();      // should return true
      testCheckAndPut2();     // should return true
      testCheckAndPut4();     // should return true
      testGet();
      testCheckAndDelete4();  // should return true
      testCommitIfPossible();

      id++; //Should be 14
      testBeginTransaction();
      testGet();              // should have no rows
      testCommitIfPossible();
  
      id++; //Should be 15
      testBeginTransaction();
      testDeleteMultiple();
      prepareResult = testCommitRequest();
      if (prepareResult == 1){
         testCommit();
      }

      id++; //Should be 16  Should show zero rows, clear table
      testBeginTransaction();
      testGet();
      testCommitIfPossible();

      id++; //Should be 17, leaves no rows in the table
      testBeginTransaction();
      testPut();
      testDelete();
      prepareResult = testCommitRequest();
      if (prepareResult == 1){
         testCommit();
      }

      // Confirm there are no records in 'table3'
      id++; //Should be 18
      testBeginTransaction();
      System.out.println("Should show no matching rows from the Get");
      testGet();
      testCommitIfPossible();

      // Additional tests from HBase book

      id++; //Should be 19
      testBeginTransaction();
      testCheckAndPutE(); // should return true
      testCheckAndPut();  // should return true
      testCheckAndPut();  // should return false
      testCheckAndPut2(); // should return true
      testCheckAndPut3(); // should return Exception
      testCommitIfPossible();

      id++; //Should be 20
      testBeginTransaction();
      System.out.println("Should show a matching row from the Get");
      testGet();
      testCommitIfPossible();

      id++; //Should be 21
      testBeginTransaction();
      testDeleteMultiple();
      prepareResult = testCommitRequest();
      if (prepareResult == 1){
         testCommit();
      }

      id++; //Should be 22
      testBeginTransaction();
      testCheckAndPutE();     // should return true
      testCheckAndPut();      // should return true
      testCheckAndPutE();     // should return false
      testCheckAndDeleteE();  // should be false
      testCheckAndPut2();     // should return true
      testCheckAndDelete2();  // should be true
      testCheckAndDeleteE();  // should be false
      testPut();              // should return true
      testPut2();             // should return true
      testPut3();             // should return true
      testPut4();             // should return true
      testPut5();             // should return true
      testCommitIfPossible();

      id++; //Should be 23  Try to create a conflict.
      testBeginTransaction();
      System.out.println("Should show 1 row from the Get");
      testGet();
      // Don't commit this transaction yet.
      
      id++; //Should be 24
      testBeginTransaction();
      testDelete();
      testCommitIfPossible();
      id--; // Should be 23
      testCommitIfPossible(); // Should abort.
      
      id+=2; //Should be 25
      testBeginTransaction();
      System.out.println("Should show 3 rows from the GetMultiple");
      testGetMultiple();
      testCommitIfPossible();

    } catch (IOException e) {
      System.out.println("TrxTableClient3 threw IOException");
      System.out.println(e.toString());
    } catch (Throwable t) {
      System.out.println("TrxTableClient3 threw throwable exception");
      System.out.println(t.toString());
    }

    System.out.println("Finished TrxTableClient3");
  }
}

