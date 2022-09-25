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

// TrxTableClient.java
  
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

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.hadoop.hbase.client.transactional.TransactionState;

public class TrxTableClient5 {

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

  private static final String TABLE_NAME = "table1";
  private static IdTm idServer;
  private static final int ID_TM_SERVER_TIMEOUT = 1000;

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

  private static Admin admin;

 // Initialize and set up tables 
    public static void initialize() throws Exception {
 
     Configuration config = HBaseConfiguration.create();

     HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
     desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionEndpoint");
     desc.addFamily(new HColumnDescriptor(FAMILY));
     connection = ConnectionFactory.createConnection(config);
     admin  = connection.getAdmin();

     System.out.println("TrxTableClient start ");
     String preload = System.getenv("LD_PRELOAD");
     if (preload == null){
        System.out.println("\n*** LD_PRELOAD not configured.  Should \"export LD_PRELOAD=$JAVA_HOME/jre/lib/amd64/libjsig.so:$TRAF_HOME/export/lib64d/libseabasesig.so\" ***\n");
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
       System.out.println("  Table " + TABLE_NAME + " unable to get start keys" + e);
     }
     for (int i = 0; i < startKeys.length; i++){
        String regionLocation = connection.getRegionLocator(t.getName()).getRegionLocation(startKeys[i]).
        getHostname();
        System.out.println("  Table " + TABLE_NAME + " region location" + regionLocation + ", startKey is " + startKeys[i]);
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

  static public void testParseTransaction() throws IOException {

    System.out.println("\nStarting testParseTransaction with Id " + id);

    try{
    	startIdVal = getIdTmVal();
    }
    catch (Exception e){
        System.out.println("getIdTmVal threw exception " + e);
        e.printStackTrace();
    }
    
    System.out.println("testParseTransaction for value " + id + ", hex " + Long.toHexString(id) +", ClusterId " + TransactionState.getClusterId(id)
    + ", InstanceId " + TransactionState.getInstanceId(id) + ", NodeId " + TransactionState.getNodeId(id) + ", Sequence " + TransactionState.getTransSeqNum(id));

    return;
  }

  static public void main(String[] args) {
    
    System.out.println("Starting TrxTableClient5");

    try {
      initialize();
      int prepareResult = -1;

      //Should be transaction id 1

      testParseTransaction();

      // Should be clusterId = 4, instance 0, nodeId = 2
      id = 1125908525286806L;
      testParseTransaction();

      id = 1688849860263937L; //Should be clusterId = 6, instanceId = 0, nodeId = 0.
      testParseTransaction();

      id = 145804037936119837L; //Should be clusterId = 6, instanceId = 2, nodeId = 0
      testParseTransaction();

    } catch (IOException e) {
      System.out.println("TrxTableClient5 threw IOException " + e);
    } catch (Throwable t) {
      System.out.println("TrxTableClient5 threw throwable exception " + t);
    }

    System.out.println("Finished TrxTableClient5");
  }

}
