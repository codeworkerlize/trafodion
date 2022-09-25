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

package org.apache.hadoop.hbase.coprocessor.transactional;

import java.io.IOException;
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
import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

// Please see instructions at the end of this file for
// creating and populating the TRAFODION.PERF.TEST2 table.
// The instructions should create a four partition/regioned table.
//
// To confirm the output number of rows is the correct number
// Execute in the hbase shell: COUNT 'TRAF_1500000:TRAFODION.PERF.TEST2' 

public class TrxTableClient2 {

  static String regionname = "RegionName";
  static String [] regionNames = null;
  static Connection connection;
  static Table t = null;
  static long id = 1L;
  static long startIdVal = 1L;
  static long commitIdVal = 1L;
  static long scannerId = 0L;
  static boolean hasMore = false;
  static long totalRows = 0L;
  static boolean continuePerform = true;
  static byte [][] startKeys = null;
  static int startPos = 0;
  static byte [] startRow = null;
  static byte [] lastRow = null;
  private static Result lastResult = null;
  static List<HRegionLocation> regionsList = null;
  static int regionCount = 0;
  static Scan scan = null;
  static int regionSize = 0;
  
  private static IdTm idServer;
  private static final int ID_TM_SERVER_TIMEOUT = 1000;

  private static final String TABLE_NAME = "TRAF_1500000:TRAFODION.PERF.TEST2";
  private static final byte[] FAMILY = Bytes.toBytes("#1");
  private static final byte[] FAMILYBAD = Bytes.toBytes("familybad");
  private static final byte[] QUAL_A = Bytes.toBytes(1);

  private static final byte[] ROW1 = Bytes.toBytes("\000\000\000\000\200\000\017\307");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final byte[] ROW4 = Bytes.toBytes("row4");
  private static final byte[] ROW5 = Bytes.toBytes("row5");
  private static final byte[] ROW6 = Bytes.toBytes("row6");
  private static final byte [] VALUE1 = Bytes.toBytes(1000);

  private static Admin admin;
  static Pair<byte[][], byte[][]> startEndKeys = null;


 // Initialize and set up tables 
    public static void initialize() throws Exception {
 
        Configuration config = HBaseConfiguration.create();

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        desc.addFamily(new HColumnDescriptor(FAMILY));
        connection = ConnectionFactory.createConnection(config);
        admin  = connection.getAdmin();
        System.out.println("TrxTableClient2 start ");
        String preload = System.getenv("LD_PRELOAD");
        if (preload == null){
           System.out.println("\n*** LD_PRELOAD not configured.  Should \"export LD_PRELOAD=$JAVA_HOME/jre/lib/amd64/libjsig.so:$TRAF_HOME/export/lib64d/libseabasesig.so\" ***\n");
        }
        else{
           System.out.println("\n*** LD_PRELOAD configured: " + preload + " ***\n");
        }

/*
  
     try {
       System.out.println ("  Cleaning up the table " + TABLE_NAME);
       admin.disableTable(TABLE_NAME);
       admin.deleteTable(TABLE_NAME);
     }
     catch (TableNotFoundException e) {
       System.out.println("  Table " + TABLE_NAME + " was not found");
     }
     catch (TableNotEnabledException n) {
       System.out.println("  Table " + TABLE_NAME + " is not enabled");
     }
  
*/

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
       System.out.println("  Table " + TABLE_NAME + " unable to get start keys" + e);
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
       System.out.println("  Table " + TABLE_NAME + " unable to get start and endkeys" + e);
     }

     regionsList = connection.getRegionLocator(t.getName()).getAllRegionLocations();

     regionSize = regionsList.size();
     System.out.println("\t\t regionSize " + regionSize);

     regionNames = new String[regionSize];

     int count = 0;
     for (HRegionLocation regionLocation : regionsList) {
        HRegionInfo region = regionLocation.getRegionInfo();
        regionNames[count] = region.getRegionNameAsString();
        System.out.println("\t\t getRegionName " + region.getRegionNameAsString());
        System.out.println("\t\t regionNames " + regionNames[count]);
        count++;
     }
     regionname = regionNames[0];
     System.out.println("\t\t regionName " + regionNames[0]);
     try {
        idServer = new IdTm(false);
     }
     catch (Exception e){
        System.out.println("Exception creating new IdTm: " + e);
     }
     IdTmId startId;
     try {
       startId = new IdTmId();
       System.out.println("initialize() getting new startId");
       idServer.id(ID_TM_SERVER_TIMEOUT, startId);
       System.out.println("initialize() idServer.id returned: " + startId.val);
     } catch (IdTmException exc) {
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
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.abortTransaction(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], AbortTransactionResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        System.out.println("  testAbortTransaction exception " + e );
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

  static public void testBeginTransaction(final Get get, final Scan scan) throws IOException {

    System.out.println("\nStarting testBeginTransaction with Id " + id);

    try{
       startIdVal = getIdTmVal();
    }
    catch (Exception e){
       System.out.println("getIdTmVal threw exception " + e);
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
        if (get != null){
          System.out.println("testBeginTransaction using get " + get);
          result = t.coprocessorService(TrxRegionService.class, get.getRow(), get.getRow(), callable);
        }else{
          System.out.println("testBeginTransaction using scan " + scan);
          result = t.coprocessorService(TrxRegionService.class, scan.getStartRow(), scan.getStopRow(), callable);
        }
      } catch (Throwable e) {
        System.out.println("  testBeginTransaction exception " + e );
        e.printStackTrace();
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
        System.out.println("  testCheckAndDelete exception " + e );
        e.printStackTrace();
      }

    System.out.println("Finished testCheckAndDelete " + result);
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
          System.out.println("  testCheckAndPut exception " + e );
          e.printStackTrace();
      }

    System.out.println("Finished testCheckAndPut " + result);
    return;
  } 

  static public void testCloseScanner(Scan scan) throws IOException {

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
      result = t.coprocessorService(TrxRegionService.class, scan.getStartRow(), scan.getStopRow(), callable);
    } catch (Throwable e) {
      System.out.println("  testCloseScanner exception " + e );
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
      } catch (Throwable e) {
        System.out.println("  testCommit exception " + e );
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
        result = t.coprocessorService(TrxRegionService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, callable);
      } catch (Exception e) {
          System.out.println("  testCommitRequest exception " + e );
      } catch (Throwable t) {
          System.out.println("  testCommitRequest throwable " + t );
      }

      for (CommitRequestResponse cresponse : result.values())
      {
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

  static public void testCommitIfPossible(final Get get, final Scan scan) throws IOException {

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
        if (get != null)
          result = t.coprocessorService(TrxRegionService.class, get.getRow(), get.getRow(), callable);
        else
          result = t.coprocessorService(TrxRegionService.class, scan.getStartRow(), scan.getStopRow(), callable);
      } catch (Throwable e) {
        System.out.println("  testCommitIfPossible exception " + e );     
        e.printStackTrace();
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
      } catch (Throwable e) {
        System.out.println("  testDelete exception " + e );     
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
      } catch (Throwable e) {
        System.out.println("  testDeleteMultiple exception " + e );     
        e.printStackTrace();
      }

    System.out.println("Finished testDeleteMultiple " + result);
    return;
  } 

  static public void testGet(final Get get) throws IOException {

    System.out.println("Starting testGet");

    Batch.Call<TrxRegionService, GetTransactionalResponse> callable = 
        new Batch.Call<TrxRegionService, GetTransactionalResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<GetTransactionalResponse> rpcCallback = 
        new BlockingRpcCallback<GetTransactionalResponse>();         

      @Override
      public GetTransactionalResponse call(TrxRegionService instance) throws IOException {        
        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.GetTransactionalRequest.Builder builder = GetTransactionalRequest.newBuilder();        
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setTransactionId(id);
        builder.setRegionName(ByteString.copyFromUtf8(regionname));
        
        instance.get(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
      Map<byte[], GetTransactionalResponse> result = null;   
      try {
        result = t.coprocessorService(TrxRegionService.class, get.getRow(), get.getRow(), callable);
      } catch (Throwable e) {
        System.out.println("  testGet exception " + e );
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
        builder.setTransactionId(id);
        builder.setStartId(startIdVal);
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
        System.out.println("  testPut exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut1 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut2 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut3 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut4 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut5 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPut6 exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPutException exception " + e );
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
        builder.setTransactionId(id);
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
        System.out.println("  testPutMultiple exception " + e );
        e.printStackTrace();
      }

    System.out.println("Finished testPutMultiple " + result);
    return;
  } 

  static public void testPerformScan(Scan scan, final int nextCallSeq) throws IOException {

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
        builder.setNumberOfRows(10000);
        builder.setCloseScanner(false);
        builder.setNextCallSeq(nextCallSeq);

        instance.performScan(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], PerformScanResponse> presult = null;   
    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result[] results = null;

    try {
      presult = t.coprocessorService(TrxRegionService.class, scan.getStartRow(), scan.getStopRow(), callable);
    } catch (Throwable e) {
        System.out.println("  testPerformScan exception " + e );     
    }

      int count = 0;
      boolean hasMore = false;

      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result result = null;
            
      for (PerformScanResponse presponse : presult.values())
      {
        if (presponse.getHasException())
        {
          String exception = presponse.getException();
          System.out.println("  testPerformScanResponse exception " + exception );
          continuePerform = false;
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
            //System.out.println("  testPerformScan response count " + count + ", hasMore is " + hasMore + ", result " + results[i] );
            if (!hasMore) {
              continuePerform = false;
              lastResult = ProtobufUtil.toResult(results[i]);
              lastRow = lastResult.getRow();
              //lastResult = results[i];
            }
          }
        }
      } 

    totalRows = totalRows + count;

    System.out.println("Finished testPerformScan rows scanned " + totalRows);
    return;
  } 

  static public void testOpenScanner(final Scan scan) throws IOException {

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

//        scan = new Scan(startEndKeys.getFirst()[regionCount],startEndKeys.getSecond()[regionCount]);
        scan.addFamily(FAMILY);

        builder.setScan(ProtobufUtil.toScan(scan));
        
        instance.openScanner(controller, builder.build(), rpcCallback);
        return rpcCallback.get();        
      }
    };
 
    Map<byte[], OpenScannerResponse> result = null;   

    try {
      result = t.coprocessorService(TrxRegionService.class,
        scan.getStartRow(),
        scan.getStopRow(),
        callable);
    } catch (Throwable e) {
      System.out.println("  testOpenScan exception " + e );
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
      } catch (Throwable e) {
        System.out.println("  testRecovery exception " + e );
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

  static public void main(String[] args) {
    
    System.out.println("Starting TrxTableClient2");

    try {
      initialize();

      System.out.println("Start scanner test");

      for (regionCount = 0; regionCount < regionSize; regionCount++) {
        Scan scan = new Scan(startEndKeys.getFirst()[regionCount],startEndKeys.getSecond()[regionCount]);
        regionname = regionNames[regionCount];
        scan.addFamily(FAMILY);
        System.out.println("TestOpenScanner calling open scanner number " + regionCount + ", startKey " + startEndKeys.getFirst()[regionCount] + ", stopKey " + startEndKeys.getSecond()[regionCount] + ", regionname " + regionname);
        testBeginTransaction(null, scan);
        testOpenScanner(scan);
        continuePerform = true;
        int scanCall = 0;
        while (continuePerform) {
          System.out.println("TestPerformScan get 10000 rows in call number " + scanCall);
          testPerformScan(scan, scanCall);
          scanCall++;
        }
        
        testCommitIfPossible(null, scan);
        testCloseScanner(scan);

        System.out.println("TestPerformScan ended with " + totalRows + " rows" + ", regionSize " + regionSize);
      }

    } catch (java.lang.ArrayIndexOutOfBoundsException a) {
      // ignore
    } catch (IOException e) {
      System.out.println("TrxTableClient2 threw IOException " + e);
    } catch (Throwable t) {
      System.out.println("TrxTableClient2 threw throwable exception " + t);
    }

    System.out.println("Finished TrxTableClient2");
  } 

}

/*  To create and populate the TRAFODION.PERF.TEST2 table.
 *
 *  Select the number of rows to insert based on the
 *  number of upsert statements executed.
 *
 *  Execute the following instructions under sqlci:
 *
 *  set schema TRAFODION.PERF;
 *
 *  create table perf.test2 (
 *  col1 int not null primary key,
 *  col2 largeint not null,
 *  col3 largeint not null,
 *  col4 largeint not null,
 *  col5 largeint not null,
 *  col6 largeint not null,
 *  col7 largeint not null,
 *  col8 largeint not null,
 *  col9 largeint not null,
 *  col10 char(182) not null)
 *  salt using 4 partitions on (col1);
 *
 *  --disable 'TRAFODION.PERF.TEST2'
 *  --alter 'TRAFODION.PERF.TEST2', METHOD => 'table_att', MAX_FILESIZE => '10000000000'
 *  --alter 'TRAFODION.PERF.TEST2', METHOD => 'table_att', SPLIT_POLICY => 'ConstantSizeRegionSplitPolicy'
 *  --enable 'TRAFODION.PERF.TEST2'
 *
 *  set schema perf ;
 *
 *  insert into perf.test2 values (1,2,3,4,5,6,7,8,9,'abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890ab');
 *
 *
 *  upsert using load into test2 select col1+1, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+2, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+4, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+8, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+16, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+32, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+64, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+128, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+256, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+512, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+1024, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+2048, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+4096, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+8192, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+16384, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+32768, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+65536, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+131072, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+262144, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+524288, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+1048576, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+2097152, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *  upsert using load into test2 select col1+4194304, col2, col3, col4, col5, col6, col7, col8, col9, col10 from test2 ;
 *
 */
