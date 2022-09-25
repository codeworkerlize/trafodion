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
import org.apache.hadoop.hbase.util.Bytes;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


public class HBaseBinlogReplayer implements FailableTask {
   static final Log    LOG                       = LogFactory.getLog(HBaseBinlogReplayer.class);
   static final String DDL_LOG_TABLE_DUP         = HBaseBinlog.DDL_LOG_TABLE + "_DUP";
   static final String MAP_DUMP_TABLE            = HBaseBinlog.BINLOG_READER_TABLE_NAME;
   static final byte[] MAP_DUMP_ROWKEY           = Bytes.toBytes("BINLOGMAPKEY");
   static final int    MAX_BLOCKING_QUEUE_SIZE   = 10000;
   static final int    MIN_TRANSPACKET_HEAP_SIZE = 1000;
   static final int    FLUSH_WAITING_SECONDS     = 2;
   static final int    CHECK_POINT_TRX_DISTANCE  = 10000;
   static final int    MUTATOR_WRITE_BUFFER_SIZE = 64 * 1024 * 1024;

   private Connection                              hbaseConnection;
   private TreeMap<String, BufferedMutator>        tablePools;
   private TransactionPacketsPriorityBlockingQueue transPacketsQueue;
   private ZooKeeperWatcher                        zkw;
   private SQLInterface                            sqlci;
   private boolean                                 markStop;
   private boolean                                 stopReplay;
   private CheckPoint                              checkPoint;
   private TransactionPacket                       lastReplayedTxp;
   private long                                    replayedTransCount;


   public static void setupConf() {
      String confFile = System.getProperty("trafodion.log4j.configFile");
      if (confFile == null) {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         System.setProperty("trafodion.atrxdc.log", System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.replayer.log");
         confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
      }
      PropertyConfigurator.configure(confFile);
   }

   public static class ReplayUnitMark implements Serializable {
      private static final long serialVersionUID = 1L;
      int salt;
      long writeID;
      long commitID;

      public ReplayUnitMark(int s, long wid, long cid) {
         this.salt = s;
         this.writeID = wid;
         this.commitID = cid;
      }
   }

   public static class ReplayUnit {
      ReplayUnitMark replayUnitMark;
      String tableName;
      String DDLText;
      List<Mutation> mutations;

      public ReplayUnit(ReplayUnitMark rum, String tableName) {
         this.replayUnitMark = rum;
         this.tableName = tableName;
         this.mutations = new LinkedList<>();
      }

      public void parseMsg(TransactionMutationMsg tmm) throws IOException {
         List<Boolean> putOrDel = tmm.getPutOrDelList();
         List<MutationProto> putProtos = tmm.getPutList();
         List<MutationProto> deleteProtos = tmm.getDeleteList();
         int putIndex = 0;
         int deleteIndex = 0;

         for(Boolean isPut : putOrDel) {
            if (isPut) {
               Put put = ProtobufUtil.toPut(putProtos.get(putIndex++));
               LOG.trace("HBaseBinlogReplayer put: " + put);
               this.mutations.add(put);
            }
            else {
               Delete del = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
               LOG.trace("HBaseBinlogReplayer delete: " + del);
               this.mutations.add(del);
            }
         }
      }
   }

   private static class TransactionPacket {
      short totalNum;
      long commitID;
      String tableName;
      List<ReplayUnit> replayUnits;

      public String toString() {
        String ret = "commitid: ";
        ret += commitID;
        ret += " tableName: " + tableName;
        return ret;
      }

      public TransactionPacket(long commitID) {
         this.commitID = commitID;
         this.replayUnits = new ArrayList<>();
      }

      public ReplayUnit getReplayUnit(String tableName) {
         for (ReplayUnit ru : replayUnits) {
            if (ru.tableName.equals(tableName)) {
               return ru;
            }
         }
         return null;
      }

      public void parseMsg(HBaseBinlog.RowKey rowkey, TransactionMutationMsg tmm, TransactionPacketPacker packer) throws IOException {
         ReplayUnit ru = getReplayUnit(tmm.getTableName());
         tableName = tmm.getTableName();
         if (ru == null) {
            ru = new ReplayUnit(new ReplayUnitMark((int)rowkey.getMapedSalt(), rowkey.getWriteID(), tmm.getCommitId()), tmm.getTableName());
            replayUnits.add(ru);
         }
         totalNum = rowkey.getTotalNum();
         // TODO: total num of DDL log is not correct, ignore total num check for now.
         if (tmm.getTableName().equals(HBaseBinlog.DDL_LOG_TABLE)) {
            LOG.trace("ddl rowkey: " + rowkey);
            ru.DDLText = packer.parseDDLText(tmm);
            totalNum = rowkey.getTotalNum();
         }
         else {
            ru.parseMsg(tmm);
         }
      }
   }

   public static class CheckPoint {
      private ZooKeeperWatcher zkw;
      private TreeMap<Integer, ReplayUnitMark> checkpointMarks;
      public CheckPoint(ZooKeeperWatcher zkw) throws IOException {
         this.zkw = zkw;
         checkpointMarks = new TreeMap<>();
         try {
            if (ZKUtil.checkExists(zkw, ATRConfig.getReplayerCheckPointPath()) == -1) {
               ZKUtil.createSetData(zkw, ATRConfig.getReplayerCheckPointPath(), null);
            }
            byte[] data = ZKUtil.getData(zkw, ATRConfig.getReplayerCheckPointPath());
            if (data != null) {
               ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
               checkpointMarks = (TreeMap<Integer, ReplayUnitMark>)ois.readObject();
            }
         }
         catch (Exception e) {
            throw new IOException("CheckPoint initialize error", e);
         }
      }

      public ReplayUnitMark getReplayUnitMark(int salt) {
         return checkpointMarks.get(salt);
      }

      public void write() throws IOException {
         try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(checkpointMarks);
            ZKUtil.setData(zkw, ATRConfig.getReplayerCheckPointPath(), bos.toByteArray());
         } catch (IOException | KeeperException e) {
            throw new IOException("CheckPoint write", e);
         }
      }

      public String toString() {
         StringBuilder retBuilder = new StringBuilder();
         for (Entry<Integer, ReplayUnitMark> e : checkpointMarks.entrySet()) {
            retBuilder.append('[');
            retBuilder.append(e.getKey());
            retBuilder.append(',');
            retBuilder.append(e.getValue().writeID);
            retBuilder.append(',');
            retBuilder.append(e.getValue().commitID);
            retBuilder.append("]\n");
         }
         return retBuilder.toString();
      }

      public static void resetCheckPoint(ZooKeeperWatcher zkw) throws KeeperException {
         ZKUtil.deleteNode(zkw, ATRConfig.getReplayerCheckPointPath());
      }
   }

   public static class CommitIDSortComparator implements Comparator<TransactionPacket> {
      @Override
      public int compare(TransactionPacket arg0, TransactionPacket arg1) {
         return arg0.commitID > arg1.commitID ? 1 : -1;
      }
   }

   private static class TransactionPacketsPriorityBlockingQueue {
      boolean flushable;
      long replayedCommitID;
      int bufferSize;
      int maxSize;
      PriorityBlockingQueue<TransactionPacket> priorityBlockingQueueImpl;
      boolean isReady;
      Object readyLock;
      boolean isFull;
      Object fullLock;
      long waitTimes;

      public TransactionPacketsPriorityBlockingQueue(int bufferSize, int maxSize) {
         flushable = false;
         replayedCommitID = 0;
         this.bufferSize = bufferSize;
         this.maxSize = maxSize;
         priorityBlockingQueueImpl = new PriorityBlockingQueue<>(bufferSize, new HBaseBinlogReplayer.CommitIDSortComparator());
         isReady = false;
         readyLock = new Object();
         isFull = false;
         fullLock = new Object();
         waitTimes = 0;
      }

      public TransactionPacket take() throws InterruptedException {
         while (true) {
            if (priorityBlockingQueueImpl.size() >= bufferSize || flushable) {
               TransactionPacket ret = priorityBlockingQueueImpl.poll(FLUSH_WAITING_SECONDS, TimeUnit.SECONDS);
               if (isFull == true) {
                  synchronized(fullLock) {
                     isFull = false;
                     fullLock.notifyAll();
                  }
               }
               return ret;
            }
            else {
               synchronized(readyLock) {
                  isReady = false;
                  readyLock.wait();
               }
            }
         }
      }

      public void put(TransactionPacket e) throws InterruptedException {
         flushable = false;
         while (true) {
            if (priorityBlockingQueueImpl.size() < maxSize) {
               priorityBlockingQueueImpl.put(e);
               if (isReady == false && priorityBlockingQueueImpl.size() >= bufferSize) {
                  synchronized (readyLock) {
                     isReady = true;
                     readyLock.notifyAll();
                  }
               }
               return ;
            }
            else {
               synchronized(fullLock) {
                  isFull = true;
                  ++waitTimes;
                  fullLock.wait();
               }
               if (waitTimes % 1000 == 0) {
                  LOG.info("HBaseBinlogReplayer transaction packet buffer full wait times: " + waitTimes);
               }
            }
         }
      }

      public void setFlushable(boolean flushable) {
         this.flushable = flushable;
         if (flushable) {
            synchronized (readyLock) {
               isReady = true;
               readyLock.notifyAll();
            }
         }
      }

      // TODO: filter duplicate binlog.
      public void setReplayedCommitID(long cid) {
         this.replayedCommitID = cid;
      }

      public long getReplayedCommitID() {
         return replayedCommitID;
      }
   }

   /**
    * take binlog records from binlogCellBuffer and check transaction records completeness and output to transPacketsBuffer
    *
    */
   public static class TransactionPacketPacker implements FailableTask {
      private BlockingQueue<Cell> binlogCellsBuffer;
      private TransactionPacketsPriorityBlockingQueue transPacketsBuffer;
      private SQLInterface sqlci;
      private Connection conn;
      private Table ddlDupTable;
      private Table dumpTable;
      private boolean markStop;

      public TransactionPacketPacker(Configuration conf, BlockingQueue<Cell> binlogCellsBuffer, TransactionPacketsPriorityBlockingQueue transPacketsBuffer) throws IOException {
         this.binlogCellsBuffer = binlogCellsBuffer;
         this.transPacketsBuffer = transPacketsBuffer;
         conn = ConnectionFactory.createConnection(conf);
         ddlDupTable = conn.getTable(TableName.valueOf(DDL_LOG_TABLE_DUP));
         dumpTable = conn.getTable(TableName.valueOf(MAP_DUMP_TABLE));
         sqlci = new SQLInterface();
         sqlci.executeQuery("set parserflags 131072");
         markStop = false;
      }

      public byte[] haveDumpedMap() {
        try{
          Get g = new Get(MAP_DUMP_ROWKEY);
          Result rs = null;
          rs = dumpTable.get(g);
          List<Cell> cells = rs.listCells();
          if(null != cells && !cells.isEmpty()){
            for(Cell ce:cells){  //should only be one for now
              return CellUtil.cloneValue(ce);
            }
          }
        }
        catch(Exception e) {
          LOG.error("HBaseBinlogReplayer, check map dump failed : " ,e);
        }
        return null;
      }

      @Override
      public void run() throws Exception {
         Map<Long, TransactionPacket> map ;
         byte[] dumpdata = haveDumpedMap();
         if( dumpdata != null)
         {
           try{
             //restore from dump
             ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(dumpdata));
             map = (Map<Long, TransactionPacket>)ois.readObject();
             LOG.info("HBaseBinlogReplayer, get map from dump success. map size is " + map.size());
           }
           catch(Exception e) {
             LOG.fatal("HBaseBinlogReplayer, get map from dump failed: " , e);
             map = new HashMap<Long, TransactionPacket>();
           }
         }
         else
         {
           map = new HashMap<Long, TransactionPacket>();
           LOG.info("HBaseBinlogReplayer, create a new map");
         }
         int mapnotnormal = 0;
         while (!markStop) {
            Cell cell = binlogCellsBuffer.poll(FLUSH_WAITING_SECONDS, TimeUnit.SECONDS);
            if (cell != null) {
               HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(CellUtil.cloneRow(cell));
               TransactionMutationMsg tmm = TransactionMutationMsg.parseFrom(CellUtil.cloneValue(cell));
               LOG.debug("HBaseBinlog transaction packet packer take binlog rowkey: " + rowkey + ", commitid: " + tmm.getCommitId() + ", totalNum: " + tmm.getTotalNum());

               if (map.containsKey(tmm.getCommitId())) {
                  TransactionPacket txp = map.get(tmm.getCommitId());
                  txp.parseMsg(rowkey, tmm, this);

                  if (tmm.getIsMsgComplete()) {
                     txp.totalNum += 1;
                  }

                  if (txp.totalNum == rowkey.getTotalNum()) {
                     transPacketsBuffer.put(txp);
                     map.remove(tmm.getCommitId());
                     LOG.debug(String.format("%s log transaction is complete, commit id: %s", rowkey.getTotalNum(), tmm.getCommitId()));
                  }
               }
               else {
                  TransactionPacket txp = new TransactionPacket(tmm.getCommitId());
                  if (tmm.getIsMsgComplete()) {
                     txp.totalNum = 1;
                  }

                  txp.parseMsg(rowkey, tmm, this);

                  // TODO: some log total num is zero?
                  if (txp.totalNum >= tmm.getTotalNum()) {
                     transPacketsBuffer.put(txp);
                  }
                  else {
                     map.put(tmm.getCommitId(), txp);
                  }
               }
            }
            else {
               if (map.size() == 0) {
                  transPacketsBuffer.setFlushable(true);
                  LOG.debug("HBaseBinlog transaction packet packer no new data fetched and map size is zero, enable flush");
                  mapnotnormal = 0;
               }
               else {
                  mapnotnormal++;
                  if(mapnotnormal> 100)
                  {
                    mapnotnormal = 0; //reset
                    LOG.warn("HBaseBinlog , replayer cannot get all logs for some transactions, please check hte map content");
                  }
                  LOG.info("HBaseBinlog transaction packet packer no new data fetched and map size is " + map.size());
                  //print top 10 map size info
                  int printc = 0;
                  for(TransactionPacket tp1 : map.values() ) {
                      LOG.info("HBaseBinlog map content, "+ tp1.toString() );
                      printc++;
                      if(printc > 10) break; //print up to 10 should be enough
                  }
               } // if(map.size() == 0)
            }
         } //while
         //it is going to quit, dump map
         dumpMap(map);
      }
      @Override
      public void stop() {
         // TODO Auto-generated method stub
         markStop = true;
      }

      public void dumpMap(Map<Long, TransactionPacket> map) {
        if(map.size() > 0)
        {
          try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(map);
            Put p = new Put(MAP_DUMP_ROWKEY);
            p.addColumn(Bytes.toBytes("mt_") , Bytes.toBytes("map"), bos.toByteArray() );
            dumpTable.put(p);
          } catch ( Exception e) {
            LOG.fatal("HBaseBinlogReplayer fatal error: Map write exception: ", e);
          }
          LOG.info("HBaseBinlogReplayer dump map before exit replayer, dump size " + map.size());
        }
        else
          LOG.info("HBaseBinlogReplayer no need to dump map before exit replayer, dump size " + map.size());

      }

      // TODO: using a better performance implementation.
      public String parseDDLText(TransactionMutationMsg tmm) throws IOException {
         truncateDDLDupTable();
         if (replayToDDLDupTable(tmm) > 0) {
            final String trafTableName = "TRAFODION.\"_XDC_MD_\".XDC_DDL_DUP";
            final String querySchema = new String("select distinct schema_name from " + trafTableName);
            SQLInterface.ResultSet res = sqlci.executeQueryAndFetchResultSet(querySchema);
            if (res == null) {
               throw new IOException("HBaseBinlogReplayer parse DDL failed due to no result for query: " + querySchema);
            }
            String ddlText = String.format("set schema %s;", res.getData(1, 0));
            final String queryDDL = new String("select ddl_text from " + trafTableName + " order by seq_num");
            res = sqlci.executeQueryAndFetchResultSet(queryDDL);
            if (res == null) {
               throw new IOException("HBaseBinlogReplayer parse DDL failed due to no result for query: " + queryDDL);
            }
            for (int i = 1; i < res.getRowCount(); ++i) {
               ddlText += res.getData(i, 0) + " ";
            }
            LOG.trace("HBaseBinlogReplayer parsed ddl text: " + ddlText);
            return ddlText;
         }
         return "";
      }

      private void truncateDDLDupTable() throws IOException {
         Scan scan = new Scan();
         scan.setFilter(new FirstKeyOnlyFilter());
         try(ResultScanner scanner = ddlDupTable.getScanner(scan)) {
            for (final Result result : scanner) {
               final List<Cell> cells = result.listCells();
               for (final Cell cell : cells) {
                  ddlDupTable.delete(new Delete(CellUtil.cloneRow(cell)));
               }
            }
         }
      }

      /**
       * replay to XDC_DDL_DUP table and return put count
       * @return put count
       */
      private int replayToDDLDupTable(TransactionMutationMsg tmm) throws IOException {
         LOG.trace("HBaseBinlogReplayer replay to table ddl dup.");

         List<Boolean> putOrDel = tmm.getPutOrDelList();
         List<MutationProto> putProtos = tmm.getPutList();
         List<MutationProto> delProtos = tmm.getDeleteList();
         LOG.trace("replay do ddl dup table, putOrDel size: %s" + putOrDel.size());

         int putidx = 0;
         int delidx = 0;
         for (Boolean isPut : putOrDel) {
            if (isPut) {
               Put put = ProtobufUtil.toPut(putProtos.get(putidx++));
               if (LOG.isTraceEnabled()) {
                  LOG.trace(String.format("table: %s, commitID: %s, put: %s\n", tmm.getTableName(), tmm.getCommitId(), put));
               }

               Put newPut = new Put(put.getRow());
               NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
               for (Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                  for (Iterator<Cell> iterator = entry.getValue().iterator(); iterator.hasNext();) {
                     Cell cell = iterator.next();
                     byte[] family = CellUtil.cloneFamily(cell);
                     byte[] qualifier = CellUtil.cloneQualifier(cell);
                     byte[] value = CellUtil.cloneValue(cell);
                     newPut.addColumn(family,qualifier,value);
                  }
               }
               ddlDupTable.put(newPut);
            }
            else {
               LOG.warn(String.format("ignore to replay log(commitID:%s, table:%s) to XDC_DDL_DUP table due to encunter delete: %s",
                                      tmm.getCommitId(), tmm.getTableName(), ProtobufUtil.toDelete(delProtos.get(delidx++))));
            }
         }
         return putProtos.size();
      }
   }

   public HBaseBinlogReplayer(Configuration replayConf, TransactionPacketsPriorityBlockingQueue transPacketsQueue) throws IOException, KeeperException {
      this.hbaseConnection = ConnectionFactory.createConnection(replayConf);
      this.transPacketsQueue = transPacketsQueue;
      zkw = new ZooKeeperWatcher(replayConf, "BinlogReplayer", null);
      checkPoint = new CheckPoint(zkw);
      LOG.debug("HBaseBinlogReplayer initialize with check point: " + checkPoint);
      tablePools = new TreeMap<>();
      sqlci = new SQLInterface();
      sqlci.executeQuery("set parserflags 131072");
      replayedTransCount = 0;
      markStop = false;
      stopReplay = false;

      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
         try {
           stopReplay = true;
           writeCheckPoint();
           LOG.info("HBaseBinlogReader Shutdown Hook is running, last checkpoint " + getCheckPoint());
         } catch (Exception ie) {
           LOG.warn("HBaseBinlogReader Shutdown Hook get exception" + ie.getMessage(), ie);
         };
        }
      });
   }

   public void close() throws IOException {
      for (Map.Entry<String, BufferedMutator> e : tablePools.entrySet()) {
         e.getValue().close();
      }

      if (this.hbaseConnection != null) {
         this.hbaseConnection.close();
      }

      if (zkw != null) {
         zkw.close();
      }
   }

   public void replay(TransactionPacket txp) throws IOException {
      LOG.debug("HBaseBinlogReplayer replay transaction commitId: " + txp.commitID);
      updateCheckPoint(txp);
      try {
         for (ReplayUnit ru : txp.replayUnits) {
            if (ru.DDLText != null) {
               LOG.info(String.format("HBaseBinlogReplayer replay DDL: %s, from transaction commitId: %s", ru.DDLText, txp.commitID));
               //before do DDL, flush the tablePools first, since the table may be deleted
               try {
                 for (Entry<String, BufferedMutator> e : tablePools.entrySet()) {
                    e.getValue().flush();
                 }
               }
               catch (Exception e) {
                 LOG.error("HBaseBinlogReplayer flush before run DDL error: ", e);
               }
               if (!ru.DDLText.isEmpty())
                  sqlci.executeQuery(ru.DDLText);
               writeCheckPointOnly();
               //for DDL, it may be very slow, so add finish log to help trouble shooting
               LOG.info(String.format("HBaseBinlogReplayer replay DDL: %s, from transaction commitId: %s finished", ru.DDLText, txp.commitID));
            }
            else {
               BufferedMutator table = getTableOrThrows(ru.tableName);
               table.mutate(ru.mutations);
            }
         }
      }
      catch (IOException e) {
         LOG.error("HBaseBinlogReplayer replay transaction packet error for commitID: " + txp.commitID, e);
         throw new IOException("HBaseBinlogReplayer replay transaction packet error", e);
      }
      if (++replayedTransCount % CHECK_POINT_TRX_DISTANCE == 0) {
         writeCheckPoint();
      };
   }

   public CheckPoint getCheckPoint() {
      return checkPoint;
   }

   /**
    * just update memory
    *
    */
   public void updateCheckPoint(TransactionPacket txp) {
      for (ReplayUnit ru : txp.replayUnits) {
         checkPoint.checkpointMarks.put(ru.replayUnitMark.salt, ru.replayUnitMark);
      }
   }

   /**
    * persistence
    *
    */
   public void writeCheckPoint() {
      try {
         for (Entry<String, BufferedMutator> e : tablePools.entrySet()) {
            e.getValue().flush();
         }
         checkPoint.write();
      }
      catch (Exception e) {
         LOG.error("HBaseBinlogReplayer write check point error: ", e);
      }
      LOG.trace("HBaseBinlogReplayer write check point: " + checkPoint);
   }

   public void writeCheckPointOnly() {
      try {
         checkPoint.write();
      }
      catch (Exception e) {
         LOG.error("HBaseBinlogReplayer write check point only error: ", e);
      }
      LOG.trace("HBaseBinlogReplayer write check point only: " + checkPoint);
   }

   private BufferedMutator getTableOrThrows(final String tableName) throws IOException {
      BufferedMutator table = tablePools.get(tableName);
      if (table == null) {
         TableName hbaseTableName = TableName.valueOf(tableName);
         try (Admin admin = hbaseConnection.getAdmin()) {
            if (!admin.isTableAvailable(hbaseTableName)) {
               LOG.error(String.format("The table %s not available, did you manually create this table with same DDL as primary cluster on secondary cluster?", tableName));
               throw new IOException("The table " + tableName + " not available");
            }
         }

         BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener(){
               @Override
               public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator arg1)
                  throws RetriesExhaustedWithDetailsException {
                  for (int i = 0; i < e.getNumExceptions(); ++i) {
                     LOG.error("HBaseBinlogReplayer put error: " + e.getRow(i));
                  }
               }
            };
         BufferedMutatorParams params = new BufferedMutatorParams(hbaseTableName).listener(listener);
         params.writeBufferSize(MUTATOR_WRITE_BUFFER_SIZE);
         table = hbaseConnection.getBufferedMutator(params);
         tablePools.put(tableName, table);
      }
      return table;
   }

   public static HBaseBinlog.RowKey currentHighestRowKey(int salt, int partNum, CheckPoint cp) {
      ReplayUnitMark rum = cp.getReplayUnitMark((int)HBaseBinlog.RowKey.computeMapedSalt(salt, partNum));
      long wid = rum == null ? 0 : rum.writeID;
      return new HBaseBinlog.RowKey((int)HBaseBinlog.RowKey.computeMapedSalt(salt, partNum), wid, 0, (short)0);
   }

   public static void runPlayer(String connStr) {
      Configuration binlogConf = HBaseConfiguration.create();
      if (connStr != null) {
         String[] hostAndPort = connStr.split(":");
         binlogConf.set("hbase.zookeeper.quorum", hostAndPort[0]);
         if (hostAndPort.length > 1) {
            binlogConf.setInt("hbase.zookeeper.property.clientPort", Integer.parseInt(hostAndPort[1]));
         }
      }
      binlogConf.setInt("hbase.client.retries.number", 3);
      binlogConf.setInt("hbase.client.keyvalue.maxsize", 0);

      BlockingQueue<Cell> binlogCellsBuffer = new LinkedBlockingDeque<>(MAX_BLOCKING_QUEUE_SIZE);
      TransactionPacketsPriorityBlockingQueue commitIDSortTransPacketsBuffer = new TransactionPacketsPriorityBlockingQueue(MIN_TRANSPACKET_HEAP_SIZE, MAX_BLOCKING_QUEUE_SIZE);
      try(ATRConfig master_atrconf = ATRConfig.newInstance(binlogConf)) {
         int binlog_partition_num = Integer.parseInt(master_atrconf.getBinlogPartNum());
         ContextDependentTask.TaskDependentContext depContext = new ContextDependentTask.TaskDependentContext();
         Configuration local_conf = HBaseConfiguration.create();
         local_conf.setInt("hbase.client.keyvalue.maxsize", 0);

         HBaseBinlogReplayer replayer = new HBaseBinlogReplayer(local_conf, commitIDSortTransPacketsBuffer);
         for (int i = 0; i < binlog_partition_num; ++i) {
            new ContextDependentTask(depContext,
                                     new BinlogFetcher(binlogConf,
                                                       currentHighestRowKey(i, binlog_partition_num, replayer.getCheckPoint()),
                                                       binlog_partition_num, binlogCellsBuffer));
         }

         new ContextDependentTask(depContext, new TransactionPacketPacker(local_conf, binlogCellsBuffer, commitIDSortTransPacketsBuffer));
         new ContextDependentTask(depContext, replayer);

         final ExecutorService pool = Executors.newFixedThreadPool(depContext.dependentTasks.size());
         for (ContextDependentTask task : depContext.dependentTasks) {
            pool.submit(task);
         }
         depContext.waitContext();
         pool.shutdown();
      }
      catch (Exception e) {
         LOG.error("HBaseBinlogReplayer got error exit:", e);
      }
      LOG.info("HBaseBinlogReplayer exit.");
   }

   public static void printCheckPoint() {
      try (ZooKeeperWatcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "printCheckPoint", null)) {
         CheckPoint cp = new CheckPoint(zkw);
         System.out.println(cp);
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void setCheckPoint(String checkMarkString) {
      try (ZooKeeperWatcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "setCheckPoint", null)) {
         CheckPoint cp = new CheckPoint(zkw);
         String[] rmStrs = checkMarkString.split(",");
         if (rmStrs.length != 3) {
            System.err.println("checkpoint format error");
            return ;
         }
         int salt = Integer.parseInt(rmStrs[0]);
         long wid = Long.parseLong(rmStrs[1]);
         long cid = Long.parseLong(rmStrs[2]);
         ReplayUnitMark rum = new ReplayUnitMark(salt, wid, cid);
         cp.checkpointMarks.put(salt, rum);
         cp.write();
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void resetCheckPoint() {
      try (ZooKeeperWatcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "resetCheckPoint", null)) {
         CheckPoint.resetCheckPoint(zkw);
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void main(String[] args) {
      setupConf();
      if (args.length == 0) {
         System.out.println("no command");
         return ;
      }

      switch (args[0]) {
      case "replay":
         runPlayer(args.length > 1 ? args[1] : null);
         break;
      case "printcheckpoint":
         printCheckPoint();
         break;
      case "setcheckpoint":
         setCheckPoint(args[1]);
         break;
      case "resetcheckpoint":
         resetCheckPoint();
         break;
      default:
         System.err.println("unsupported command: " + args[0]);
         break;
      }
   }

   @Override
   public void run() throws Exception {
      while (!markStop) {
         if(stopReplay == false) {
           TransactionPacket txp = transPacketsQueue.take();
           if (txp == null) {
             if (lastReplayedTxp != null) {
               LOG.info("HBaseBinlogReplayer transaction packer queue empty, flush data.");
               writeCheckPoint();
             }
           }
           else {
            replay(txp);
           }
           lastReplayedTxp = txp;
         }
      }
   }

   @Override
   public void stop() {
      markStop = true;
   }
}
