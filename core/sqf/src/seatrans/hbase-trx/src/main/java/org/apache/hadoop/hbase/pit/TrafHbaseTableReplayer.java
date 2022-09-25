/* @@@ START COPYRIGHT @@@
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

package org.apache.hadoop.hbase.pit;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.ATRConfig;
import org.apache.hadoop.hbase.client.transactional.AsyncXDCWorker.BinlogFetcher;
import org.apache.hadoop.hbase.client.transactional.SQLInterface;
import org.apache.hadoop.hbase.client.transactional.AsyncXDCWorker.ContextDependentTask;
import org.apache.hadoop.hbase.client.transactional.AsyncXDCWorker.FailableTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.util.ByteArrayKey;


public class TrafHbaseTableReplayer {
   static final Log LOG = LogFactory.getLog(TrafHbaseTableReplayer.class);
   public static final String SEQ_META_TABLE = "TRAF_RSRVD_1:TRAFODION._MD_.SEQ_GEN";
   
   public static void setupConf() {
      String confFile = System.getProperty("trafodion.log4j.configFile");
      if (confFile == null) {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         System.setProperty("trafodion.atrxdc.log", System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.hbasereplayer.log");
         confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
      }
      PropertyConfigurator.configure(confFile);
   }

   static class seqReplayer extends Thread{
      private String tableName;
      private Connection     masterConnection;
      private Connection     localConnection;
      private Table          localTable;
      private Table          remoteTable;
      private int interval;
      private long lastTs;
      private ConcurrentHashMap<ByteArrayKey, Long> cacheSizeMap = new ConcurrentHashMap<ByteArrayKey, Long>();;

      seqReplayer(String tableNm, Connection master, Connection local) {
         this.tableName = tableNm;
         this.masterConnection = master;
         this.localConnection = local;
         this.interval = 500; //default 500 ms
         this.lastTs = 0;
      }

      private void updateCacheSize(byte[] rk, byte[] v)
      {
        ByteArrayKey rowInHex = new ByteArrayKey(rk);
        long cs = Bytes.toLong(v);
        cacheSizeMap.put(rowInHex, cs);
      }

      private long getCacheSize(byte[] rk) {
        ByteArrayKey rowInHex = new ByteArrayKey(rk);
        try {
          if(cacheSizeMap.containsKey(rowInHex))
            return cacheSizeMap.get(rowInHex);
          else //read from table
          {
            Get g = new Get(rk);
            Result rs = remoteTable.get(g); 
            if(rs != null ) {
              if(rs.list() != null ) {
               for (Cell cell : rs.rawCells()) {
                  byte[] qc = CellUtil.cloneQualifier(cell);
                  if(qc[0] == 0x09) 
                  {
                    long cs = Bytes.toLong(CellUtil.cloneValue(cell));
                    cacheSizeMap.put(rowInHex, cs);
                    return cs;
                  }
               }
             }
            }
          }
        }
        catch(IOException e) { return -1L; }
        return -1L;
      }

      private void processSequenceData(Cell cell ) throws IOException
      {
        boolean isNextValue = false;
        byte[] rowkey = cell.getRow();
        byte[] qc = CellUtil.cloneQualifier(cell);
        if(qc[0] == 0x0A) //next value
          isNextValue = true;
        if(qc[0] == 0x09) //cacheSize
          updateCacheSize(rowkey, CellUtil.cloneValue(cell) );

        //update the read starting time to current
        if(lastTs < cell.getTimestamp() ) lastTs = cell.getTimestamp();

        Put localPut = new Put(rowkey);
        if( isNextValue == true ) //most common cases
        {
          long cacheSize = getCacheSize(rowkey);
          long nextVal = Bytes.toLong(CellUtil.cloneValue(cell));
          if(cacheSize >0) 
            nextVal += cacheSize;
          localPut.add(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), Bytes.toBytes(nextVal));
          localTable.put(localPut);
        }
        else {
          localPut.add(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
          localTable.put(localPut);
        }
      }

      private void processData(Cell cell) throws IOException
      {
        byte[] rowkey = cell.getRow();

        //update the read starting time to current
        if(lastTs < cell.getTimestamp() ) lastTs = cell.getTimestamp();

        Put localPut = new Put(rowkey);
        localPut.add(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
        localTable.put(localPut);
      }
	  
      @Override
	  public void run() {
            try {
              localTable = localConnection.getTable(TableName.valueOf(tableName)); //target write into
              remoteTable = masterConnection.getTable(TableName.valueOf(tableName)); //src to read from
            }
            catch(Exception e) {
              LOG.error("TrafHbaseTableReplayer startup error : " , e);
            }
            Scan masterScan = new Scan();
            ResultScanner ss = null;
            byte[] rowkey;
            while(true) {
              try{
                long readToTs = System.currentTimeMillis(); 
                masterScan.setTimeRange(lastTs+1, readToTs);
                ss = remoteTable.getScanner(masterScan);
                 
                for (Result r : ss) {
                  for (Cell cell : r.rawCells()) {
                    if(tableName.equals(SEQ_META_TABLE))
                      processSequenceData(cell);
                    else
                      processData(cell);
                  }
                }
                Thread.sleep(interval); 
              }
              catch(Exception e) {
                LOG.error("TrafHbaseTableReplayer runtime error : " , e);
              }
            }//endless loop
	  }
   }
   
   public static void main(String[] args) {
      setupConf();
      try {
        
        Configuration local_conf = HBaseConfiguration.create();
        Connection localConnection = ConnectionFactory.createConnection(local_conf); 
        STRConfig str_conf = STRConfig.getInstance(local_conf);
        Connection masterConnection = ConnectionFactory.createConnection( str_conf.getPeerConfiguration(str_conf.getFirstRemotePeerId())); 
        TrafHbaseTableReplayer mainThread = new TrafHbaseTableReplayer();
        String syncTable = SEQ_META_TABLE;
        if(args.length > 0) //given a table name
          syncTable = args[0];
        seqReplayer seqPlayer = new seqReplayer(syncTable , masterConnection, localConnection);
        seqPlayer.start();
      } 
      catch(Exception e) {
        LOG.error("native hbase replayer startup failed", e);
      }
   }

}
