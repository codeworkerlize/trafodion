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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.ATRConfig;
import org.apache.hadoop.hbase.client.transactional.utils.BashScript;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;


public class HBaseBinlog implements AutoCloseable, ATRConfig.ReInitializable {
   public Object initSaltLock = new Object();
   private static Boolean enableStrictBinlogAntiDup = false;

   public static void setupConf() {
      String confFile = System.getProperty("trafodion.log4j.configFile");
      if (confFile == null) {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         System.setProperty("trafodion.atrxdc.log",
               System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.log");
         confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
      }
      PropertyConfigurator.configure(confFile);
   }

   private static class BinlogLocalSingleton {
      private static HBaseBinlog instance;
      static {
         instance = new HBaseBinlog(BINLOG_INSTANCE_ID_LOCAL);
      }
   }

   private static class BinlogRemoteSingleton {
      private static HBaseBinlog instance;
      static {
         instance = new HBaseBinlog(BINLOG_INSTANCE_ID_REMOTE);
      }
   }

   public static HBaseBinlog local_instance() {
      return BinlogLocalSingleton.instance;
   }

   public static HBaseBinlog remote_instance() {
      return BinlogRemoteSingleton.instance;

   }

   public static boolean hasThirdBinlogConf() {
     try {
       String thirdConnSt = ATRConfig.instance().getThirdBinlogConnectionString();
       if( thirdConnSt == null || thirdConnSt.equals("") ) //no conf
         return false;
       //validate connst todo
       return true;
     }
     catch(Exception e) {
       LOG.error("HBaseBinlog: try to check if has third Binlog conf get exception: ", e);
       return false;
     }
   }

   public static Configuration getThirdBinlogConf() throws IOException {
     try {
       String thirdConnSt = ATRConfig.instance().getThirdBinlogConnectionString();
       if( thirdConnSt == null || thirdConnSt.equals("") ) //no conf
         return null;
       Configuration conf = HBaseConfiguration.create();
       String[] hostAndPort = thirdConnSt.split(":");
       conf.set("hbase.zookeeper.quorum", hostAndPort[0]);
       if (hostAndPort.length > 1) {
         conf.setInt("hbase.zookeeper.property.clientPort", Integer.parseInt(hostAndPort[1]));
       }
       return conf;
     }
     catch(Exception e) {
       LOG.error("HBaseBinlog: try to get third Binlog conf meet exception: ", e);
       return null;
     }
   }

   public Configuration getBinlogTableConf() throws KeeperException, InterruptedException, IOException {
      Configuration retconf = null;
      if (instanceId == BINLOG_INSTANCE_ID_REMOTE) {
         if (ATRConfig.instance().getBinlogConnectionString().isEmpty())
            throw new IOException("Binlog remote connection string is empty.");
         Configuration conf = HBaseConfiguration.create();
         String[] hostAndPort = ATRConfig.instance().getBinlogConnectionString().split(":");
         conf.set("hbase.zookeeper.quorum", hostAndPort[0]);
         if (hostAndPort.length > 1) {
            conf.setInt("hbase.zookeeper.property.clientPort", Integer.parseInt(hostAndPort[1]));
         }
         retconf = conf;
      }
      else {
         retconf = localConfig;
      }

      String retryNumStr = System.getenv("BINLOG_PUT_RETRY_TIMES");
      if(retryNumStr != null)
        BINLOG_CLIENT_RETRY_TIMES = Integer.parseInt(retryNumStr.trim());

      String rpcTimeoutStr = System.getenv("BINLOG_PUT_RPC_TIMEOUT");
      if(rpcTimeoutStr != null)
        BINLOG_CLIENT_RPC_TIMEOUT = Integer.parseInt(rpcTimeoutStr.trim());

      String retryPauseStr = System.getenv("BINLOG_PUT_RETRY_PAUSE");
      if(retryPauseStr != null)
        BINLOG_CLIENT_RETRY_PAUSE = Integer.parseInt(retryPauseStr.trim());

      retconf.setInt("hbase.client.retries.number", BINLOG_CLIENT_RETRY_TIMES);
      retconf.setInt("hbase.client.pause", BINLOG_CLIENT_RETRY_PAUSE);
      retconf.setInt("hbase.rpc.timeout", BINLOG_CLIENT_RPC_TIMEOUT);
      retconf.setInt("hbase.client.keyvalue.maxsize", 0);
      return retconf;
   }

   private static class PathSort implements Comparable<PathSort> {
      String sortName;
      Path filePath;

      public PathSort(String name, Path path) {
         this.sortName = name;
         this.filePath = path;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj instanceof PathSort) {
            return sortName.equals(((PathSort)obj).getSortName());
         }
         return false;
      }

      @Override
      public int compareTo(PathSort other) {
         return sortName.compareTo(other.getSortName());
      }

      public String getSortName() {
         return sortName;
      }

      public Path getFilePath() {
         return filePath;
      }
   }

   public interface BinlogSate {
      public boolean checkAvailable();
      public boolean append(LongWritable writeID, Put put, String sync, boolean doFlowControl);
      public void reinitialize() throws IOException;
      public HBaseBinlog.RowKey getHighestRowKey();
      public boolean close();
   }

   private static class BinlogNormalState implements BinlogSate {
      private HBaseBinlog     binlog;
      private Connection      connection;
      private Connection      localconnection;
      private Connection      thirdConnection;
      private Admin           admin;
      private Table           binlogReaderTable;
      private BufferedMutator binlogMutator;
      public  List<Put>       safeBuffer;
      public Object safeBufferLock = new Object();
      private int             versions;
      private boolean         disableBlockCache;
      private boolean         available;

      public BinlogNormalState(HBaseBinlog binlog) {
         this.binlog = binlog;
         reinitializeAndTryAppend();
      }

      @Override
      public void reinitialize() throws IOException {
         LOG.info(binlog.logPrefix() + "normal state reinitialize.");
         try {
            if(connection == null || connection.isClosed())
               connection = ConnectionFactory.createConnection(binlog.getBinlogTableConf());
            if(localconnection == null || localconnection.isClosed())
               localconnection = ConnectionFactory.createConnection(binlog.localConfig);
            versions = 10;

            final HColumnDescriptor hcol = new HColumnDescriptor(BINLOG_FAMILY);
            if (disableBlockCache) {
               hcol.setBlockCacheEnabled(false);
            }
            hcol.setMaxVersions(versions);
            admin = connection.getAdmin();
            final boolean binlogTableExists = admin.isTableAvailable(TableName.valueOf(BINLOG_TABLE_NAME));
            if(LOG.isTraceEnabled()) {
               LOG.trace(binlog.logPrefix() + "Binlog Table " + BINLOG_TABLE_NAME + (binlogTableExists? " exists" : " does not exist" ));
            }
            final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(BINLOG_TABLE_NAME));
//            desc.addFamily(hcol);

            if (binlogTableExists == false) {
               try {
                  LOG.info(binlog.logPrefix() + "try new HTable: " + BINLOG_TABLE_NAME);
                  // TODO: create table with presplit regions according to partion number.
                  createBinlog(binlog.getBinlogTableConf(), admin.getClusterStatus().getServersSize());
               }
               catch(final TableExistsException tee){
                  int retryCount = 0;
                  boolean binlogTableEnabled = false;
                  while (retryCount < 3) {
                     retryCount++;
                     binlogTableEnabled = admin.isTableAvailable(TableName.valueOf(BINLOG_TABLE_NAME));
                     if (! binlogTableEnabled) {
                        try {
                           Thread.sleep(2000); // sleep two seconds or until interrupted
                        }
                        catch (final InterruptedException e) {
                           // Ignore the interruption and keep going
                        }
                     }
                     else {
                        break;
                     }
                  }
                  if (retryCount == 3){
                     LOG.error(binlog.logPrefix() + "Exception while enabling " + BINLOG_TABLE_NAME);
                     throw new IOException("HBaseBinlog Exception while enabling " + BINLOG_TABLE_NAME);
                  }
               }
               catch(final Exception e){
                  LOG.error(binlog.logPrefix() + "Exception while creating " + BINLOG_TABLE_NAME + ": " , e);
                  throw new IOException("HBaseBinlog Exception while creating " + BINLOG_TABLE_NAME + ": " + e);
               }
            }


            binlogReaderTable = localconnection.getTable(TableName.valueOf(BINLOG_READER_TABLE_NAME));

            BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener(){
                  @Override
                  public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator m)
                     throws RetriesExhaustedWithDetailsException {
                     for (int i = 0; i < e.getNumExceptions(); ++i) {
                        LOG.error(binlog.logPrefix() + "failed append put: " + e.getRow(i));
                     }
                  }
               };
            BufferedMutatorParams params = new BufferedMutatorParams(desc.getTableName()).listener(listener);
            params.writeBufferSize(Long.parseLong(ATRConfig.instance().getBinlogMutatorWriteBufferSize()));
            binlogMutator = connection.getBufferedMutator(params);
            if (safeBuffer == null) {
               safeBuffer = new ArrayList<>();
            }

         }
         catch (Exception e) {
            LOG.error(binlog.logPrefix() + "reinitialize error", e);
            throw new IOException("HBaseBinlog reinitialize error", e);
         }
      }

      public List<Put> getSafeBuffer() {
        return safeBuffer;
      }

      //Table in HBase is light-weight, so make it per thread
      public Table getBinlogTable() throws IOException
      {
         Table table = connection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));
         //if use max perf mode set binlogTable to autoFlush mode and set the write bufer
         if(ATRConfig.instance().getSyncMode().equals(ATRConfig.SYNC_MODE_MAX_PERFORMANCE)){
            try {
               table.setWriteBufferSize(Long.parseLong(ATRConfig.instance().getBinlogMutatorWriteBufferSize()));
            } catch (Exception e) {
               LOG.warn("MAX_PERFORMANCE mode set table's writeBufferSize failed due to :" , e);
            }
         }
         return table;
      }

      //This table is used to record binlog when the remote binlog is no longer reached
      //to keep the replication number at least as 2
      public Table getThirdBinlogTable() throws IOException
      {
         return thirdConnection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));
      }

      public void clearSafeBuffer() { safeBuffer.clear(); }
      public void flushSafeBuffer() throws IOException
      {
         try(Table binlogTable = getBinlogTable()) {
            binlogTable.put(safeBuffer);
         }
      }

      @Override
      public boolean checkAvailable() {
         if (!available) {
            binlog.startNormalStateWatcher();
         }
         return available;
      };

      @Override
      public boolean append(LongWritable writeID, Put put, String sync, boolean doFlowControl) {
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "normal state append");
         }
         try {
            if (sync.equals(ATRConfig.SYNC_MODE_MAX_PERFORMANCE) ||
                sync.equals(ATRConfig.SYNC_MODE_S_RELIABILITY) ){

               int limiter = ATRConfig.instance().getSafebufferLimiter();
               if(safeBuffer.size() > limiter && doFlowControl == true) {
                  if (LOG.isWarnEnabled()) {
                     LOG.warn("flow control safebuffer is too large, over " + limiter + ", sleep 10ms.");
                  }
                  try{
                     Thread.sleep(10);
                  } catch (Exception e) {}
               }

               synchronized(safeBufferLock) {
                  safeBuffer.add(put);
               }
               if(safeBuffer.size() > 500)
                  LOG.warn(binlog.logPrefix() + "safebuffer is large " + safeBuffer.size());
            }
            else {
               if(LOG.isTraceEnabled()) {
                  LOG.trace(binlog.logPrefix() + "hbase.zookeeper.quorum " + connection.getConfiguration().get("hbase.zookeeper.quorum"));
                  LOG.trace(binlog.logPrefix() + "hbase.zookeeper.property.clientPort " + connection.getConfiguration().get("hbase.zookeeper.property.clientPort"));
               }
               try(Table binlogTable = getBinlogTable()) {
                  binlogTable.put(put);
               }
            }
         }
         catch (Exception e) {
            available = false;
            LOG.warn(binlog.logPrefix() + "normal state append failed.", e);
            switch (sync) {
               case ATRConfig.SYNC_MODE_MAX_RELIABILITY:
                  synchronized (changeStateLock) {
                     if (binlog.state instanceof BinlogNormalState) // still in normal state
                        binlog.state = binlog.changeState();
                     return binlog.state.append(writeID, put, sync, doFlowControl);
                  }
               case ATRConfig.SYNC_MODE_MAX_PERFORMANCE:
                  List<Put> tmpList = new ArrayList<>();
                  synchronized (safeBufferLock) {
                     tmpList.addAll(safeBuffer);
                     safeBuffer.clear();
                  }
                  // TODO: do some retry before state change.
                  synchronized (changeStateLock) {
                     if (binlog.state instanceof BinlogNormalState) // still in normal state
                        binlog.state = binlog.changeState();
                     for (Put p : tmpList) {
                        byte[] rk = p.getRow();
                        HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(rk);
                        long fid = rowkey.getWriteID();
                        binlog.state.append(new LongWritable(fid), p, sync, doFlowControl);
                        //is there a chance to lost the current writeID?
                        //now this is a code branch that will never be reached, since when sync is false
                        //append() only invokes safeBuffer.add(), which should not throw exception
                        //but we want to do real HBase IO when safeBuffer is large enough in the future when we make this more clear
                        //now, HBaseBinlog only flush the safeBuffer periodicly in a chore thread
                     }
                  }
                  return true;
               case ATRConfig.SYNC_MODE_MAX_PROTECTION:
                  return false;
               default:
                  return false;
            }
         }
         return true;
      }

      @Override
      public HBaseBinlog.RowKey getHighestRowKey() {
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "normal state get highest rowkey.");
         }
         try(Table binlogTable = getBinlogTable()){
            BinlogStats stats = BinlogStats.getBinlogStats(binlogTable, binlog.saltNum, binlog.partNum);
            return stats.getHighestRowKey();
         }
         catch (Exception e) {
            LOG.warn(binlog.logPrefix() + "normal state get highest rowkey failed.", e);
            synchronized(changeStateLock) {
               if(binlog.state instanceof BinlogNormalState) // still in normal state
                 binlog.state = binlog.changeState();
              return binlog.state.getHighestRowKey();
            }
         }
      }

      @Override
      public boolean close() {
         // TODO: close resource.
         return true;
      }

      public boolean reinitializeAndTryAppend() {
         try {
            LOG.info(binlog.logPrefix() + "reinitializeAndTryAppend entry");
            reinitialize();
            RowKey testRowKey = new RowKey(binlog.mapedSalt, 0L, 0, (short)0);
            Put testPut = new Put(testRowKey.getRowArray());
            testPut.addColumn(BINLOG_FAMILY, BINLOG_QUAL, Bytes.toBytes(binlog.instanceId + " available test"));
            LOG.info(binlog.logPrefix() + "try available test to binlog table.");
            try(Table binlogTable = getBinlogTable()) {
               binlogTable.put(testPut);
            }
            available = true;
            LOG.info(binlog.logPrefix() + "try reinitialize and append success to mapedsalt:" + binlog.mapedSalt);
         }
         catch (IOException e) {
            LOG.info(binlog.logPrefix() + "try reinitialize and append failed: ", e);
            available = false;
         }
         return available;
      }
   }

   private static class BinlogCacheState implements BinlogSate {
      private HBaseBinlog binlog;
      private boolean ready;
      private boolean reinitializeOnNotReady;
      private FileSystem fs;
      private Path currentCacheFilePath;
      private SequenceFile.Writer binlogCacheFileWriter;
      private List<PathSort> cachedFiles;
      private String curFileName;
      int     maxRowCount = 2000000;
      long    cacheRowCount;

      public BinlogCacheState(HBaseBinlog binlog) {
         this.binlog = binlog;
         this.ready = false;
         this.reinitializeOnNotReady = false;
         this.cacheRowCount = 0L;
         this.cachedFiles = Collections.synchronizedList(new ArrayList<PathSort>());
         this.curFileName = null;

         String envVar = System.getenv("HDFS_FILE_CACEHED_MAX_BINLOG_SIZE");
         if (envVar != null) {
            envVar = envVar.trim();
            maxRowCount = envVar.length() > 0 ? Integer.parseInt(envVar) : 2000000;
         }
         LOG.info("HBaseBinlog cacheState max binlog row count in one file: " + maxRowCount);

         StringBuilder pathSB = new StringBuilder();
         pathSB.append(binlog.localConfig.get("fs.defaultFS"))
                 .append(HBaseBinlog.CACHE_FILE_ROOT)
                 .append("/");
         Path cacheFileRoot = new Path(pathSB.toString());
         String cacheFilePrefix = binlog.localConfig.get("fs.defaultFS") + HBaseBinlog.CACHE_FILE_ROOT + "/" + binlog.instanceId + "/" + binlog.saltNum + "-";
         try {
            if (fs == null) fs = FileSystem.get(binlog.localConfig);
            FileStatus[] fileStatuses = fs.listStatus(cacheFileRoot);
            for (FileStatus fileStatus : fileStatuses) {
               String pathSTR = fileStatus.getPath().toString();
               if (fileStatus.isFile() && pathSTR.startsWith(binlog.localConfig.get("fs.defaultFS")+ HBaseBinlog.CACHE_FILE_ROOT + "/" + binlog.instanceId + "/" + binlog.saltNum + "-")) {
                  String suffix = pathSTR.substring(pathSTR.lastIndexOf("-"));
                  if (fileStatus.getLen() <= 0) {
                     LOG.warn("HBaseBinlog checkCachedFiles found zero-length file and delete it. file name:" + pathSTR);
                     fs.delete(fileStatus.getPath());
                     continue;
                  }
                  if (LOG.isInfoEnabled()) {
                     LOG.info("HBaseBinlog cacheState found remained file:" + pathSTR);
                  }
                  //binlog.syncCacheFile2SafeBuffer(fs, fileStatus.getPath(), binlog.localConfig);
                  cachedFiles.add(new PathSort(suffix, fileStatus.getPath()));
               }
            }
            if (cachedFiles.size() > 0)
               Collections.sort(cachedFiles);
         } catch (FileNotFoundException ffe) {
            LOG.info("No cache file remained.");
         } catch (IOException e) {
            LOG.error("HBaseBinlog cache state close remain cache file failed: ", e);
         }
      }

      public List<PathSort> getCachedFiles() {
         return cachedFiles;
      }

      public void setCachedFiles(List<PathSort> newList) {
         cachedFiles.addAll(newList);
      }

      @Override
      public boolean checkAvailable() {
         if (LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "cache state check available");
         }
         if (!ready && reinitializeOnNotReady) {
            try {
               LOG.info(binlog.logPrefix() + "cache state prepare");
               reinitialize();
               reinitializeOnNotReady = false;
            }
            catch (Exception e) {
               LOG.warn(binlog.logPrefix() + "normal state prepare failed.", e);
               return false;
            }
         }
         return ready;
      };

      private void checkCachedFiles() {
         if (cachedFiles.size() > 0)
            return;

         StringBuilder pathSB = new StringBuilder();
         pathSB.append(binlog.localConfig.get("fs.defaultFS"))
                 .append(HBaseBinlog.CACHE_FILE_ROOT)
                 .append("/");
         Path cacheFileRoot = new Path(pathSB.toString());
         try {
            if (fs == null) fs = FileSystem.get(binlog.localConfig);
            FileStatus[] fileStatuses = fs.listStatus(cacheFileRoot);
            for (FileStatus fileStatus : fileStatuses) {
               String pathSTR = fileStatus.getPath().toString();
               if (fileStatus.isFile() && pathSTR.startsWith(binlog.localConfig.get("fs.defaultFS") + HBaseBinlog.CACHE_FILE_ROOT+ "/" + binlog.instanceId  + "/" + binlog.saltNum + "-")) {
                  String suffix = pathSTR.substring(pathSTR.lastIndexOf("-") + 1);
                  if (LOG.isInfoEnabled()) {
                     LOG.info("HBaseBinlog checkCachedFiles found remained file:" + pathSTR);
                  }
                  if (suffix.equals(curFileName))
                     continue;
                  if (fileStatus.getLen() <= 0) {
                     LOG.warn("HBaseBinlog checkCachedFiles found zero-length file and delete it. file name:" + pathSTR);
                     fs.delete(fileStatus.getPath());
                     continue;
                  }
                  cachedFiles.add(new PathSort(suffix, fileStatus.getPath()));
               }
            }
            if (cachedFiles.size() > 0)
               Collections.sort(cachedFiles);
         } catch (FileNotFoundException ffe) {
            if (LOG.isInfoEnabled()) {
               LOG.info("HBaseBinlog checkCachedFiles: no cache file remained.");
            }
         } catch (Exception e) {
            LOG.error("HBaseBinlog checkCachedFiles: remain cache file failed: ", e);
         }
      }

      private boolean hasCachedFiles() {
         StringBuilder pathSB = new StringBuilder();
         pathSB.append(binlog.localConfig.get("fs.defaultFS"))
                 .append(HBaseBinlog.CACHE_FILE_ROOT)
                 .append("/");
         Path cacheFileRoot = new Path(pathSB.toString());
         try {
            if (fs == null) fs = FileSystem.get(binlog.localConfig);
            FileStatus[] fileStatuses = fs.listStatus(cacheFileRoot);
            return (fileStatuses.length > 0);
         } catch (Exception e) {
            LOG.error("HBaseBinlog hasCachedFiles failed", e);
         }
         return false;
      }

      @Override
      public boolean append(LongWritable writeID, Put put, String sync, boolean doFlowControl) {
         boolean noException = true;
         if (LOG.isDebugEnabled()) {
            LOG.debug(binlog.logPrefix() + "HBaseBinlog cache state append:" + writeID);
         }
         try {
            if (cachedFiles.size() <= 0 && binlog.normalState.checkAvailable()) {
               checkCachedFiles();
               if (cachedFiles.size() <= 0 && cacheRowCount < 1000) {
                  synchronized(changeStateLock) {
                     if(binlog.state instanceof BinlogCacheState ) {// still in cache state
                        ready = false;
                        binlog.changeState();
                     }
                     if (binlog.state instanceof BinlogCacheState == false) {
                        binlog.state.append(writeID, put, sync, doFlowControl);
                        return true;
                     }
                  }
               } else if (cachedFiles.size() <= 0 && binlog.state instanceof BinlogCacheState) {
                  reinitialize();
                  checkCachedFiles();
               }
            }
         } catch (Exception e) {
            noException = false;
         }
         try {
            if (noException || binlog.state instanceof BinlogCacheState) {
               if (this.cacheRowCount >= maxRowCount) {
                  reinitialize();
               }
               this.cacheRowCount++;
               if (LOG.isDebugEnabled()) {
                  LOG.debug(binlog.logPrefix() + "binlogCacheFileWriter append:" + writeID);
               }
               binlogCacheFileWriter.append(writeID, new BytesWritable(ProtobufUtil.toMutation(MutationType.PUT, put).toByteArray()));
               if (sync.equals(ATRConfig.SYNC_MODE_MAX_PERFORMANCE)) {
                  binlogCacheFileWriter.sync();
               }
            }
         } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
               LOG.warn(binlog.logPrefix() + "HBaseBinlog cache state append failed with exception:", e);
            }
            ready = false;
            synchronized(changeStateLock) {
               if(binlog.state instanceof BinlogCacheState) //still in cache state
                  binlog.changeState();
               if (cachedFiles.size() > 0 && binlog.state instanceof BinlogCacheState == false) {
                  LOG.error(binlog.logPrefix() + "HBaseBinlog cache state: failed to append binlog in " + cachedFiles.size() + " cached files");
               }
               return binlog.state.append(writeID, put, sync, doFlowControl);
            }
         }
         return true;
      }

      @Override
      synchronized public void reinitialize() throws IOException {
         LOG.info(binlog.logPrefix() + "cache state reinitialize");
         fs = FileSystem.get(binlog.localConfig);
         if (binlogCacheFileWriter != null) {
            binlogCacheFileWriter.close();
            curFileName = null;
         }
         currentCacheFilePath = getNewCacheFilePath();
         binlogCacheFileWriter = SequenceFile.createWriter(fs, binlog.localConfig, currentCacheFilePath, LongWritable.class, BytesWritable.class);
         cacheRowCount = 0;
         ready = true;
      }

      @Override
      public HBaseBinlog.RowKey getHighestRowKey() {
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "cache state get highest rowkey");
         }
         return new HBaseBinlog.RowKey(binlog.saltNum, binlog.nextWriteID.get() - 1, 0, (short)0);
      }

      @Override
      public boolean close() {
         if (LOG.isInfoEnabled()) {
            LOG.info(binlog.logPrefix() + "cacheState close info: cache in " + currentCacheFilePath + " close with rowcount " + cacheRowCount );
         }
         boolean ret = true;
         if (binlogCacheFileWriter != null) {
            try {
               ready = false;
               binlogCacheFileWriter.close();
               binlogCacheFileWriter = null;
               curFileName = null;
               cacheRowCount = 0;
               ret = binlog.syncCacheFile2SafeBuffer(fs, currentCacheFilePath, binlog.localConfig);
            } catch (IOException e) {
               LOG.error(binlog.logPrefix() + "HBaseBinlog cache state close binlog cache file writer failed: ", e);
               return false;
            }
         }
         return ret;
      }

      /**
       * make cahce state invalid and wait for change state.
       *
       */
      public void retire() {
         ready = false;
         reinitializeOnNotReady = false;
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "cache state retired");
         }
      }

      private Path getNewCacheFilePath() {
         String hdfsRoot = binlog.localConfig.get("fs.defaultFS");
         String pathSTR = hdfsRoot + HBaseBinlog.CACHE_FILE_ROOT + "/" + binlog.instanceId + "/" + binlog.saltNum + "-" + System.currentTimeMillis();
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "fs.defaultFS: " + hdfsRoot);
         }
         Path path = new Path(pathSTR);
         curFileName = pathSTR.substring(pathSTR.lastIndexOf("-") + 1);
         if (LOG.isInfoEnabled()) {
            LOG.info(binlog.logPrefix() + "HBaseBinlog new cache file path: " + path);
         }
         return path;
      }
   }

   private static class BinlogFatalState implements BinlogSate {
      private HBaseBinlog binlog;
      public int flushCount = 0;

      public BinlogFatalState(HBaseBinlog binlog) {
         this.binlog = binlog;
      }

      @Override
      public boolean checkAvailable() {
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "fatal state prepare");
         }
         return true;
      };

      @Override
      public boolean append(LongWritable writeID, Put put, String sync, boolean doFlowControl) {
         HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(put.getRow());
         LOG.error(binlog.logPrefix() + "fatal state append, binlog record lost: " + rowkey);
         return false;
      }

      @Override
      public void reinitialize() {
         LOG.info(binlog.logPrefix() + "fatal state reinitialize");
      }

      @Override
      public HBaseBinlog.RowKey getHighestRowKey() {
         if(LOG.isTraceEnabled()) {
            LOG.trace(binlog.logPrefix() + "fatal state get highest rowkey");
         }
         return new HBaseBinlog.RowKey(binlog.saltNum, binlog.nextWriteID.get() - 1, 0, (short)0);
      }

      @Override
      public boolean close() {
         if (LOG.isInfoEnabled()) {
           LOG.info(binlog.logPrefix() + "fatal state close.");
         }
         return true;
      }
   }

   private static class NormalStateHealthWatcher implements Runnable {
      private BinlogNormalState state;

      public NormalStateHealthWatcher(BinlogNormalState state) {
         this.state = state;
      }

      @Override
      public void run() {
		  if (!Thread.currentThread().getName().startsWith("N")) {
			  Thread.currentThread().setName("NormalStateHealthWatcher-" + Thread.currentThread().getName());
		  }
        LOG.info(state.binlog.logPrefix() + "NormalStateHealthWatcher started!");

         while (true) {
            try {
               // TODO: make this configable
               Thread.sleep(5000);
               if (state.reinitializeAndTryAppend()) {
                  break;
               }
            } catch (InterruptedException e) {
               LOG.info(state.binlog.logPrefix() + "normal state health watcher interrupted:", e);
               break;
            }
         }
         state.binlog.watchingNormalState = false;
         LOG.info(state.binlog.logPrefix() + "normal state health watcher exit.");
      }
   }

   private static class BinlogFlusher implements Runnable {
      private int interval;
      private HBaseBinlog inst;

      public BinlogFlusher(int invl, HBaseBinlog obj) {
         interval = invl;
         inst = obj;
      }

      @Override
      public void run() {
         Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

         if (!Thread.currentThread().getName().startsWith("b")) {
            Thread.currentThread().setName("binlogFlusher-" + Thread.currentThread().getName());
         }

         long preTime = System.currentTimeMillis();
         boolean sleep = false;
         while(true) {
            try  {
               long stamp1 = System.currentTimeMillis();
               long stamp2 = 0, stamp3 = 0;
               preTime = inst.flush(preTime, sleep);

               if ((inst.state == inst.normalState && inst.normalState.safeBuffer.size() <= 0)
                    || (inst.state == inst.cacheState && (inst.cacheState.getCachedFiles().size() <=0 || inst.normalState.checkAvailable() == false))
                    || inst.state == inst.fatalState) {
                  stamp2 = System.currentTimeMillis();
                  Thread.sleep(interval);
                  sleep = true;
               } else {
                  sleep = false;
               }
               stamp3 = System.currentTimeMillis();
               if(stamp3 - stamp1 > (ATRConfig.instance().getLogTimeLimiter() + (sleep ? interval : 0))) {
                  LOG.warn("binlogFlusher phaseFlush " + (sleep ? stamp2 - stamp1 : stamp3 - stamp1) +
                          " phaseSleep " + (sleep ? stamp3 - stamp2 : 0) +
                          " phaseSize " + (stamp3 - stamp1));
               }
            }
            catch (Exception e) {
               LOG.error("binlogFlusher failed: ", e);
            }
         }//while(true)
      }
   }

   private static class CachedFileSyncer implements Runnable {
      private FileSystem fs;
      private Configuration conf;
      private Path cacheFilePath;
      private Table cacheSyncBinlogTable; //used by sync thread which will be created and terminated on-demand, so the table object should be temp object
      private Connection connection;

      public CachedFileSyncer(FileSystem fs, Configuration conf, Path cacheFilePath, Connection connection) {
         this.fs = fs;
         this.conf = conf;
         this.cacheFilePath = cacheFilePath;
         this.connection = connection;
      }

      @Override
      public void run() {
		  if (!Thread.currentThread().getName().startsWith("C")) {
			  Thread.currentThread().setName("CachedFileSyncer-" + Thread.currentThread().getName());
		  }

         try (SequenceFile.Reader cacheFileReader = new SequenceFile.Reader(fs, cacheFilePath, conf);
              Connection conn = ConnectionFactory.createConnection(conf);
              Table binlogTable = conn.getTable(getTableName())) {
            LongWritable key = new LongWritable();
            BytesWritable value = new BytesWritable();
            long position = cacheFileReader.getPosition();
            int restoredrows = 0;
            long startwid = 0L;
            long towid = 0L;
            //create Table at this point, since HBase is avaiable again
            cacheSyncBinlogTable = connection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));
            while (cacheFileReader.next(key, value)) {
               String syncSeen = cacheFileReader.syncSeen() ? "*" : "";
               if(restoredrows == 0) startwid = key.get();
               if(LOG.isTraceEnabled()) {
                  LOG.trace(String.format("HBaseBinlog cache file reader [%s%s]\t read writeid%s\n", position, syncSeen, key));
               }
               Put readPut = ProtobufUtil.toPut(MutationProto.parseFrom(value.copyBytes()));
               cacheSyncBinlogTable.put(readPut);
               restoredrows++;
               position = cacheFileReader.getPosition(); // beginning of next record
            }
            cacheFileReader.close();
            fs.delete(cacheFilePath);
            cacheSyncBinlogTable.close();
            LOG.info("HBaseBinlog sync cache into binlog hbase table stats: read and put " + restoredrows + " from " + startwid + " to " + key.get() + " cachefile " + cacheFilePath);
         }
         catch (IOException e) {
            LOG.error(String.format("HBaseBinlog CachedFileSyncer sync path: %s failed: ", cacheFilePath), e);
         }
      }
   }

   static final Log LOG = LogFactory.getLog(HBaseBinlog.class);
   public static final String CACHE_FILE_ROOT = "/user/trafodion/binlogcache";
   public static final String DDL_LOG_TABLE = "TRAF_RSRVD_1:TRAFODION._XDC_MD_.XDC_DDL";
   public static final String BINLOG_NAMESPACE = "TRAF_RSRVD_5";
   public static final String BINLOG_TABLE_NAME = BINLOG_NAMESPACE + ":TRAFODION._DTM_.TRAFODION_BINLOG";
   public static Object changeStateLock = new Object();
   public static Object initializeLock = new Object();
   public static Object appendLock = new Object();

   public static String BINLOG_READER_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER";
   private static final byte[] BINLOG_FAMILY = Bytes.toBytes("mf");
   private static final byte[] BINLOG_META_FAMILY = Bytes.toBytes("mt_");
   private static final byte[] BINLOG_QUAL = Bytes.toBytes("mq");
   private static final byte[] BINLOG_KEYS_QUAL = Bytes.toBytes("key");
   private static int BINLOG_CLIENT_RETRY_TIMES = 3;
   private static int BINLOG_CLIENT_RPC_TIMEOUT = 10*1000; //10 seconds
   private static int BINLOG_CLIENT_RETRY_PAUSE = 10;
   private static int MAX_FLUSH_INTERVAL = 100000;  //100 seconds is the max valid flush interval
   private static int DEFAULT_FLUSH_INTERVAL = 100;
   private static int MAX_ALLOWED_SIZE_OF_BUFFER = 10000 ;//10K in binlog buffer means we need to let commit slower
   private static int BINLOG_TABLE_TIME_TO_LIVE = 259200;
   private static int BINLOG_TABLE_MAX_VERSION = 1000;
   private static int BINLOG_INSTANCE_ID_LOCAL  = 1;
   private static int BINLOG_INSTANCE_ID_REMOTE = 2;
   private static long MAX_FILE_SIZE = 9223372036854775807l;
   private static int BINLOG_WRITE_BATCH_SIZE = 50;

   public static final int BINLOG_FLUSH_OK   =   0;
   public static final int BINLOG_FLUSH_WAIT =   1;
   public static final int BINLOG_WRITE_ERROR = -1;

   private Configuration localConfig;
   private ZooKeeperWatcher watcher;
   private AtomicLong nextWriteID;
   private long nextWriteIDJump;
   private long prevCommitID;
   private long lastCommitTS = 0l;
   private long lastFlushWidTS = 0l;
   private long lastFlushedWid = 0l;
   private long prevLastFlushedWid = 0l;
   private int partNum;
   private int saltNum = -1;
   private int instanceId;
   private int mapedSalt = 0;
   private boolean shouldReinitialize;
   private boolean shouldReinitializeSalt = true;
   private BinlogSate state;
   private BinlogNormalState normalState;
   private BinlogCacheState  cacheState;
   private BinlogFatalState  fatalState;
   private boolean watchingNormalState;
   private boolean isFirstTimeInit = true;
   private ThreadPoolExecutor daemonThreadPool;

   public static class RowKey {
      private final byte[] row_bytes;
      private final ByteBuffer bb;

      private static final int ROWKEY_LENGTH     = 18;
      private static final int MAPED_SALT_OFFSET = 0;
      private static final int WRITE_ID_OFFSET   = 4;
      private static final int REGIONINFO_OFFSET = 12;
      private static final int TOTAL_NUM_OFFSET  = 16;

      public RowKey(final byte[] row) {
         row_bytes = row;
         bb = ByteBuffer.wrap(row);
      }

      public RowKey(final int salt, final long writeID, final int regionInfo, final short totalNum) {
         row_bytes = new byte[ROWKEY_LENGTH];
         bb = ByteBuffer.wrap(row_bytes);
         bb.putInt(salt);
         bb.putLong(writeID);
         bb.putInt(regionInfo);
         bb.putShort(totalNum);
      }

      public RowKey clone() {
         byte[] bytes = row_bytes.clone();
         return new RowKey(bytes);
      }

      public byte[] getRowArray() {
         return row_bytes;
      }

      public void setMapedSalt(final int salt) {
         bb.putInt(MAPED_SALT_OFFSET, salt);
      }

      public int getMapedSalt() {
         return bb.getInt(MAPED_SALT_OFFSET);
      }

      public void setSalt(final int salt, final int partNum) {
         setMapedSalt((int)computeMapedSalt(salt, partNum));
      }

      public long getSalt(final int partNum) {
         return (getMapedSalt() >>> 20);
      }

      public void setWriteID(final long commitID) {
         bb.putLong(WRITE_ID_OFFSET, commitID);
      }

      public long getWriteID() {
         return bb.getLong(WRITE_ID_OFFSET);
      }

      public void setRegionInfo(final int regionInfo) {
         bb.putInt(REGIONINFO_OFFSET, regionInfo);
      }

      public int getRegionInfo() {
         return bb.getInt(REGIONINFO_OFFSET);
      }

      public void setTotalNum(final short totalNum) {
         bb.putShort(TOTAL_NUM_OFFSET, totalNum);
      }

      public short getTotalNum() {
         return bb.getShort(TOTAL_NUM_OFFSET);
      }

      public String toString() {
         return "salt:" + getMapedSalt() + ",wid:" + getWriteID() + ",ri:" + getRegionInfo()
               + ",tn:" + getTotalNum();
      }

      public static long computeMapedSaltDistance(int partnum) {
         return 0xffffffffL / partnum;
      }

      public static long computeMapedSalt(int salt, int partnum) {
         return salt << 20;
      }
   }

   public static class BinlogStats {
      public int    salt;
      public int    partNum;
      public Table  binlog_table;
      public RowKey highest_rowkey;
      public long   latest_commit_id;
      public long   latest_write_time;
      public String latest_mutate_table = "";

      private BinlogStats(Table binlog, int salt, int partNum) throws IOException {
         binlog_table = binlog;
         this.salt = salt;
         this.partNum = partNum;

         int mappedSalt = (int)RowKey.computeMapedSalt(salt, partNum);
         RowKey reverseStartRow = new RowKey(mappedSalt + 1, 0, 0, (short)0);
         RowKey reverseStopRow = new RowKey(mappedSalt, 0, 0, (short)0);
         Scan scan = new Scan();
         scan.addColumn(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());
         if(LOG.isTraceEnabled()) {
            LOG.trace("reverse start row: " + Bytes.toHex(reverseStartRow.row_bytes));
            LOG.trace("reverse stop row: " + Bytes.toHex(reverseStopRow.row_bytes));
         }
         scan.setStartRow(reverseStartRow.getRowArray());
         scan.setStopRow(reverseStopRow.getRowArray());
         scan.setReversed(true);
         ResultScanner scanner = binlog_table.getScanner(scan);
         Result result = scanner.next();

         if (result != null) {
            Cell latest_cell = result.getColumnLatestCell(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());
            highest_rowkey = new RowKey(CellUtil.cloneRow(latest_cell));
            latest_write_time = latest_cell.getTimestamp();
            byte[] input = CellUtil.cloneValue(latest_cell);
            TransactionMutationMsg tmm = TransactionMutationMsg.parseFrom(input);
            latest_commit_id = tmm.getCommitId();
            latest_mutate_table = tmm.getTableName();
            LOG.info("HBaseBinlog get highest row key from existed binlog table: " + highest_rowkey);
         } else {
            highest_rowkey = reverseStopRow;
            LOG.info("HBaseBinlog no record in binlog table, set highest rowkey to: " + highest_rowkey);
         }
      }

      public static BinlogStats getBinlogStats(Table binlogTable, int salt, int partNum) throws IOException {
         return new BinlogStats(binlogTable, salt, partNum);
      }

      public HBaseBinlog.RowKey getHighestRowKey() {
         return highest_rowkey;
      }
   }

   private HBaseBinlog (int id) {
      /**
       * to support online configuration and no error reported during initialization, all stuff have been move to realReinitialize.
       */
      instanceId = id;
      shouldReinitialize = true;
      Runtime.getRuntime().addShutdownHook( new Thread(new Runnable(){
         @Override
         public void run() {
            LOG.info(logPrefix() + " JVM shutdown hook is running");
            try {
               if (cacheState != null && cacheState.binlogCacheFileWriter != null) {
                  LOG.info(logPrefix() + " JVMShutdownHook start to close cachefile.");
                  cacheState.binlogCacheFileWriter.close();
               }
            } catch (Exception e) {
               LOG.warn(logPrefix() + " JVMShutdownHook failed due to ", e);
            }
         }
      }));
   }

   public BinlogSate changeState() {
      if (state != null && state.checkAvailable()) { // check for concurrent change state.
         if (LOG.isTraceEnabled()) {
            LOG.trace(logPrefix() + " current state is available, no need to change.");
         }
         return state;
      }

      if (state == normalState) {
         state = cacheState;
         cacheState.reinitializeOnNotReady = true;
         startNormalStateWatcher();
         if (cacheState.checkAvailable()) {
            LOG.info(logPrefix() + "changeState from normal to cache");
            return state;
         }
      }
      else if (state == cacheState) {
         //close can fail, in case of fail to close, state cannot change
         if (cacheState.close() == false) { //fail to close
            if (false == cacheState.checkAvailable()) {
               LOG.warn("HBaseBinlog: changeState to fatal");
               state = fatalState;
            }
            return state;
         }
         state = normalState;
         if (state.checkAvailable()) {
            if (LOG.isInfoEnabled()) {
               LOG.info(logPrefix() + "changeState from cache to normal");
            }
            return state;
         }
         else {
            return changeState();
         }
      }
      else if (state == null) {
         state = cacheState;
         return changeState();
      }

      state = fatalState;
      LOG.warn(logPrefix() + "changeState to fatal");
      return state;
   }

   public void startNormalStateWatcher() {
      // TODO: during environment shutting down, no need to watch.
      if (!watchingNormalState) {
         LOG.info(logPrefix() + "start normal state watcher.");
         Thread watcherThread = new Thread(new NormalStateHealthWatcher(normalState));
         watcherThread.start();
         watchingNormalState = true;
      }
   }

   public void startCacheFileWatcher(FileSystem fs, Path cacheFilePath) {
      // TODO: use other hbase connection for thread safe.
      daemonThreadPool.submit(new CachedFileSyncer(fs, localConfig, cacheFilePath, normalState.connection));
   }

   public void startBinlogFlusher(int invl) {
      LOG.info(logPrefix() + "start binlogfluster with interval " + invl);
      daemonThreadPool.submit(new BinlogFlusher(invl, this));
   }

   private void sync2SafeBufferOneByOne(List<Put> tmpBuffer, Table binlogTable) throws Exception {
      for (Put nextPut : tmpBuffer) {
         binlogTable.put(nextPut);
      }
   }

   public boolean syncCacheFile2SafeBuffer(FileSystem fs, Path cacheFilePath, Configuration conf) {
      if (LOG.isInfoEnabled()) {
         LOG.info(logPrefix() + "Start to sync cache file into safeBuffer.");
      }
      try (SequenceFile.Reader cacheFileReader = new SequenceFile.Reader(fs, cacheFilePath, conf);
           Table binlogTable = normalState.connection.getTable(getTableName())) {
         LongWritable key = new LongWritable();
         BytesWritable value = new BytesWritable();
         long position = cacheFileReader.getPosition();
         int restoredrows = 0;
         long startwid = 0L;
         long towid = 0L;
         List<Put> tmpBuffer = new ArrayList<>();
         boolean putError = false;
         while (cacheFileReader.next(key, value)) {
            putError = false;
            String syncSeen = cacheFileReader.syncSeen() ? "*" : "";
            if(restoredrows == 0) startwid = key.get();
            if(LOG.isTraceEnabled()) {
               LOG.trace(String.format("HBaseBinlog rewriteCacheFile reader [%s%s]\t read writeid%s\n", position, syncSeen, key));
            }
            Put readPut = ProtobufUtil.toPut(MutationProto.parseFrom(value.copyBytes()));
            tmpBuffer.add(readPut);
            if( (restoredrows % BINLOG_WRITE_BATCH_SIZE) == 0 && restoredrows > 0) {
              try {
                 binlogTable.put(tmpBuffer);
              } catch (Exception binEx) {
                 LOG.warn("normal syncCacheFile2SafeBuffer: failed to flush in batch mode, single mode will be used", binEx);
                 putError = true;
              }
              if (putError) {
                 try {
                    sync2SafeBufferOneByOne(tmpBuffer, binlogTable);
                 } catch (Exception binEx) {
                    normalState.available = false;
                    cacheFileReader.close();
                    throw binEx;
                 }
              }
              tmpBuffer.clear();
            }
            restoredrows++;
            position = cacheFileReader.getPosition(); // beginning of next record
         }
         cacheFileReader.close();
         putError = false;
         if(tmpBuffer.size() > 0) {
            try {
               binlogTable.put(tmpBuffer);
            } catch (Exception binEx) {
               LOG.warn("normal syncCacheFile2SafeBuffer: failed to flush in batch mode, single mode will be used", binEx);
               putError = true;
            }
            if (putError) {
               try {
                  sync2SafeBufferOneByOne(tmpBuffer, binlogTable);
               } catch (Exception binEx) {
                  normalState.available = false;
                  throw binEx;
               }
            }
         }
         fs.delete(cacheFilePath);
         if (LOG.isInfoEnabled()) {
            LOG.info(logPrefix() + "HBaseBinlog rewriteCacheFile into binlog safeBuffer stats: read and put " + restoredrows + " from " + startwid + " to " + key.get() + " cachefile " + cacheFilePath);
         }
      } catch (FileNotFoundException ffe) {
         LOG.error(String.format("HBaseBinlog  path: %s not found: ", cacheFilePath), ffe);
      } catch (Exception e) {
         LOG.error(String.format("HBaseBinlog rewriteCacheFile path: %s failed: ", cacheFilePath), e);
         return false;
      }
      return true;
   }

   void initializeNextWriteID() throws IOException {
      this.nextWriteIDJump = ATRConfig.instance().getNextWriteIDJump(saltNum);
      //get current timestamp use this to initialize the start write ID, no need to record and read jump id
      long  nowtime = System.currentTimeMillis();
      nowtime = nowtime << 11;
      this.nextWriteID = new AtomicLong(nowtime);
      //we allow the hole in writeId for a new round of life time
      //HBaseBinlog.RowKey highestRowkey = state.getHighestRowKey();
      //nextWriteID.set(highestRowkey.getWriteID() + 1);
      // TODO: initialize prevCommitID from exists binlog?
      prevCommitID = 0L;
      LOG.info(logPrefix() + "nextWriteID initialized with " + nextWriteID);
   }

   byte[] genBinlogKey(final long wid, final long cid, final String regionInfo, final short totalNum)
   {
      final RowKey rowkey = new RowKey(mapedSalt, wid, regionInfo.hashCode(), totalNum);
      if (LOG.isDebugEnabled()) {
         LOG.debug(logPrefix() + "generated rowkey: " + rowkey + ",on region: " + regionInfo);
      }
      return rowkey.getRowArray();
   }

  private String normalizeTableName(String t){
    String tbns[] = t.split("\\.");
    int size = tbns.length;
    if(size < 2) return null;
    //last two parts schema and table name
    //String ret =  tbns[size-2] + "." + tbns[size-1];
    String schemaName = tbns[size-2].startsWith("\"") ? tbns[size-2].replaceAll("\"", "") : tbns[size-2];
    String tableName = tbns[size-1].startsWith("\"") ? tbns[size-1].replaceAll("\"", "") : tbns[size-1];
    String ret = schemaName +"."+tableName;
    return ret;
  }

   public boolean tableHasPrimaryKey(String tbl, Configuration conf) {
      tbl = normalizeTableName(tbl);
      if (state == normalState) {
         try {
            if(shouldReinitialize) {
               realReinitialize(conf);
            }
            Get g = new Get(Bytes.toBytes(tbl));
            g.addColumn(BINLOG_META_FAMILY, BINLOG_KEYS_QUAL);
            Result rs = normalState.binlogReaderTable.get(g);
            String v = Bytes.toString(rs.getValue(BINLOG_META_FAMILY, BINLOG_KEYS_QUAL));
            if(v == null) return true;
            if(v.contains("SIGNED LARGEINT-0-0-0-SYSKEY") || v.equals("syskey"))
              return false;
         }
         catch (Exception e) {
            return true;
         }
      }
      return true;
   }

   public long getCurrentWriteID(Configuration conf) {
      if (shouldReinitialize) {
         try {
            realReinitialize(conf);
         } catch (IOException e) {
            LOG.error("HBaseBinlog get current writeID failed due to :" , e);
             return 0l;
         }
     }
      long writeID = nextWriteID.getAndIncrement();
/* no need to mantain the nextWriteIDJump since we use current timestamp as next init ID
      if (writeID >= nextWriteIDJump) {
         try {
            nextWriteIDJump = writeID + ATRConfig.instance().getWriteIDFlushInterval();
            ATRConfig.instance().setNextWriteIDJump(saltNum, nextWriteIDJump);
         }
         catch (Exception e) {
            LOG.error(logPrefix() + "update next writeID jump error.", e);
         }
      }
*/
      return writeID;
   }


   //just return the current writeID
   public long getWriteIDnoInc() {
     if( nextWriteID != null)
     return nextWriteID.get();
     else
     return -2L;
   }

   public boolean append(final long writeID, Configuration conf, final TransactionMutationMsg.Builder tmBuilder, final String regionInfo, final String sync, boolean freshwid) {
      if(LOG.isTraceEnabled()) {
         LOG.trace(logPrefix() + "appends the log of table " + tmBuilder.getTableName() + " with mode " + sync);
      }

      try {
         if (instanceId == BINLOG_INSTANCE_ID_REMOTE && ATRConfig.instance().getBinlogConnectionString().isEmpty()) {
            LOG.warn(logPrefix() + "remote instance append failed due to bcstr not configured.");
            return false;
         }

         if (shouldReinitialize) {
            realReinitialize(conf);
         }

         synchronized (appendLock) {
            long currentCommitID = tmBuilder.getCommitId();
            final Put p = new Put(genBinlogKey(writeID, currentCommitID, regionInfo, (short)tmBuilder.getTotalNum()));
            p.addColumn(BINLOG_FAMILY, BINLOG_QUAL, tmBuilder.build().toByteArray());
            if (ATRConfig.instance().isBinlogSkipWAL()) {
               p.setDurability(Durability.SKIP_WAL);
            }
            prevCommitID = currentCommitID;
            //update my nextwriteID
            if(freshwid)
              nextWriteID.set(writeID);

            if(sync.equals(ATRConfig.SYNC_MODE_MAX_PERFORMANCE))
               synchronized(changeStateLock) { //if some other thread try to change the state, wait until it is done, then do append
                  return state.append(new LongWritable(writeID), p, sync, true);
               }
            else {
               return state.append(new LongWritable(writeID), p, sync, true);
            }
         }
      }
      catch (Exception e) {
         StringBuilder sb = new StringBuilder();
         sb.append("writeId: ").append(writeID)
                 .append(";currentCommitID:").append(tmBuilder.getCommitId())
                 .append(";regionInfo:").append(regionInfo)
                 .append(";isSkipWAL:").append(ATRConfig.instance().isBinlogSkipWAL())
                 .append(".HBaseBinlog atrxdc appending failed due to error: ");
         LOG.error(logPrefix() + sb.toString(), e);
      }
      return false;
   }

   /**
    * append mutation to binlog.
    * @return true on success, false on fail
    */
   public boolean append(Configuration conf, final TransactionMutationMsg.Builder tmBuilder, final String regionInfo, final String sync) {
      if(LOG.isTraceEnabled()) {
         LOG.trace(logPrefix() + "appends the log of table " + tmBuilder.getTableName() + " with mode " + sync);
      }

      try {
         if (instanceId == BINLOG_INSTANCE_ID_REMOTE && ATRConfig.instance().getBinlogConnectionString().isEmpty()) {
            LOG.warn(logPrefix() + "remote instance append failed due to bcstr not configured.");
            return false;
         }

         if (shouldReinitialize) {
            realReinitialize(conf);
         }

         synchronized (appendLock) {
            long currentCommitID = tmBuilder.getCommitId();
            long writeID = nextWriteID.getAndIncrement();
/*
            if (writeID >= nextWriteIDJump) {
               try {
                  nextWriteIDJump = writeID + ATRConfig.instance().getWriteIDFlushInterval();
                  ATRConfig.instance().setNextWriteIDJump(saltNum, nextWriteIDJump);
               }
               catch (Exception e) {
                  LOG.error(logPrefix() + "update next writeID jump error.", e);
               }
            }
*/
            final Put p = new Put(genBinlogKey(writeID, currentCommitID, regionInfo, (short)tmBuilder.getTotalNum()));
            p.addColumn(BINLOG_FAMILY, BINLOG_QUAL, tmBuilder.build().toByteArray());
            if (ATRConfig.instance().isBinlogSkipWAL()) {
               p.setDurability(Durability.SKIP_WAL);
            }
            prevCommitID = currentCommitID;

            if(sync.equals(ATRConfig.SYNC_MODE_MAX_PERFORMANCE))
               synchronized(changeStateLock) { //if some other thread try to change the state, wait until it is done, then do append
                  return state.append(new LongWritable(writeID), p, sync, true);
               }
            else {
               return state.append(new LongWritable(writeID), p, sync, true);
            }
         }
      }
      catch (Exception e) {
         StringBuilder sb = new StringBuilder();
         sb.append("writeId: ").append(nextWriteID == null? "null" : nextWriteID.getAndIncrement())
                 .append(";currentCommitID:").append(tmBuilder.getCommitId())
                 .append(";regionInfo:").append(regionInfo)
                 .append(";isSkipWAL:").append(ATRConfig.instance().isBinlogSkipWAL())
                 .append(".HBaseBinlog atrxdc appending failed due to error: ");
         LOG.error(logPrefix() + sb.toString(), e);
      }
      return false;
   }
   public BinlogSate getState() { return state; }

   //this method should be synchronized
   //flush will be invoked per region at present, so it is highly possible to run concurrently
   //normally, all flush need to flush the safeBuffer, which will be protected by safeBufferLock
   //so it is better to sync in function level
   //it will also prevent the concurrent issue of state change, which now protected by changeStateLock, but seems still not correct
   //thread A and B do flush at same time and HBase run into issue
   //A first get exception and changeState, then B
   //B will exception as well since when B start, state is still normal
   //B will be blocked by changeSateLock until A finish, now state is cache
   //B will do cache state append, so no problem , but B has no need to run into exception
   public long flush(long preTime, boolean sleep) throws IOException {
      if (state instanceof BinlogNormalState && ((BinlogNormalState) state).binlogMutator != null) {
         long time;
         List<Put> tmpBuffer = new ArrayList<>();
         long lv_lastFlushedWid = 0l;
         synchronized(normalState.safeBufferLock) {
            tmpBuffer.addAll( normalState.getSafeBuffer());
            normalState.clearSafeBuffer();
            if(enableStrictBinlogAntiDup == true && tmpBuffer.size() > 0 )
            {
               //update the lastCommitTS
               lastCommitTS = getWriteIDnoInc();
               LOG.debug("HBaseBinlog: antidup flush lastCommitTS is " + lastCommitTS);
            }
            lv_lastFlushedWid = getWriteIDnoInc();
         }
         time = System.currentTimeMillis();

         try {
            if(tmpBuffer.size() > 0)
            {
               try(Table binlogTable = normalState.getBinlogTable()) {
                  binlogTable.put(tmpBuffer);
               }

               Long deltaTime = System.currentTimeMillis() - preTime;
               Long putTime = System.currentTimeMillis() - time;
               if (deltaTime > ATRConfig.instance().getLogTimeLimiter()) {
                  LOG.warn("normal flush: deltaTime " + deltaTime + " putTime " + putTime + " sleep " + sleep +
                          " bufferSize " + tmpBuffer.size());
               }

               if(LOG.isTraceEnabled()) {
                 LOG.trace("HBaseBinlog flushed binlog size: " + tmpBuffer.size());
              }
              tmpBuffer.clear();
              tmpBuffer = null;
              lastFlushedWid = lv_lastFlushedWid;
            }
            preTime = time;
            if(enableStrictBinlogAntiDup == false ) { //old way
              if ((lastCommitTS + 10*1000) < System.currentTimeMillis()) {
               lastCommitTS = System.currentTimeMillis();
               try {
                  ATRConfig.instance().setLastCommitTS(this.saltNum, lastCommitTS);
               } catch (Exception e) {
                  LOG.warn("Failed to log last commitTS in ZK.", e);
               }
              }
            }

            else
            {
              if ((lastFlushWidTS+ 1*1000) < System.currentTimeMillis()) {
               lastFlushWidTS = System.currentTimeMillis();
               //get the last wid this flush will put
               if(tmpBuffer != null && tmpBuffer.size() > 0)  {
                try {
                  ATRConfig.instance().setLastCommitTS(this.saltNum, lastCommitTS);
                  LOG.debug("HBaseBinlog: antidup flush set lastCommitTS as " + lastCommitTS);
                } catch (Exception e) {
                  LOG.warn("Failed to log last commitTS in ZK.", e);
                }
               }
              }//if lastFlushWidTS
            }
         }
         catch (Exception e) {
            LOG.warn("HBaseBinlog flush binlog table error : ", e);
            synchronized(changeStateLock) {
             if(state instanceof BinlogNormalState ) //still in normal state
             {
               normalState.available = false;
               state = changeState();
             }
             int flushStatwid = 0;
             int flushStatCount = 0;
             long fid = 0;
             synchronized (normalState.safeBufferLock) {
               tmpBuffer.addAll(normalState.getSafeBuffer());
               normalState.clearSafeBuffer();
             }
             for(Put p: tmpBuffer) {
              byte[] rk = p.getRow();
              HBaseBinlog.RowKey rowkey = new HBaseBinlog.RowKey(rk);
              fid = rowkey.getWriteID();
              if(flushStatCount == 0) // the first recored , make a log
                LOG.info("HBaseBinlog: flush error and move safebuffer into cache file from wid " + fid );
              state.append(new LongWritable(fid), p, ATRConfig.SYNC_MODE_MAX_PERFORMANCE, false);
              flushStatCount++;
             }
             LOG.info("HBaseBinlog: flush error and move safebuffer into cache file end with count" + flushStatCount + " and end wid is " + fid);
           }
         }
      } else if (state instanceof BinlogCacheState) {
         List<PathSort> files = cacheState.getCachedFiles();
         String tmpName = null;
         boolean isFileSynced = false;

         while (files.size() > 0) {
            PathSort path = files.get(0);
            if (path == null) {
               files.remove(0);
               continue;
            }
            if (tmpName != null && tmpName.compareTo(path.getSortName()) >= 0) {
               LOG.error("HBaseBinlog name is out of order: " + tmpName + " " + path.getSortName());
            }
            tmpName = path.getSortName();
            if (normalState.checkAvailable() == false)
               break;

            isFileSynced = true;
            if (syncCacheFile2SafeBuffer(cacheState.fs, path.getFilePath(), localConfig)) {
               files.remove(0);
            } else {
               break;
            }
         }

         if (files.size() > 0 && isFileSynced) {
            String logMSG = "HBaseBinlog failed to sync cached file to binlog table. please check the first file, and all file names: ";
            for (int idx = 0; idx < files.size(); idx++) {
               PathSort filePath = files.get(idx);
               logMSG = logMSG + (idx == 0 ? "" : " & ") + filePath.getFilePath().toString();
            }
            LOG.error(logMSG);
         }

         synchronized(changeStateLock) {
            if (cacheState.binlogCacheFileWriter != null)
               cacheState.binlogCacheFileWriter.hflush();
         }
         if (normalState.checkAvailable() && (isFileSynced || cacheState.hasCachedFiles())) {
            RowKey testRowKey = new RowKey(mapedSalt, 0L, 0, (short)0);
            Put testPut = new Put(testRowKey.getRowArray());
            testPut.addColumn(BINLOG_META_FAMILY, BINLOG_QUAL, Bytes.toBytes("available test"));
            testPut.setDurability(Durability.SKIP_WAL);
            synchronized(changeStateLock) {
               //try to drive the state change
               state.append(new LongWritable(0L), testPut, ATRConfig.SYNC_MODE_MAX_PERFORMANCE, false);
            }
         }
         preTime = System.currentTimeMillis();
      } else {
         if (state instanceof BinlogFatalState) {
            ((BinlogFatalState) state).flushCount ++ ;
            if(((BinlogFatalState) state).flushCount % 10000 == 0) LOG.warn("Binlog is not flushed. BinlogState is " + state.getClass().getName());
         }
      }
      return preTime;
   }

   // check current state, if in cache mode, check avaiable of normal state and swith back
   public void flushCache() throws IOException {
      if (state instanceof BinlogCacheState ) { //check if we are now in cache state
          synchronized(changeStateLock) {
            if (state instanceof BinlogCacheState ) { //check if we are still in cache state
             //if normal state is ok, trigger a state change
             if (normalState.available == true )
               state = changeState();
            }
         }
      }
   }

   @Override
   public void close() throws Exception {
      if (watcher != null) {
         watcher.close();
      }
      // TODO: close state
   }
   public int getInstanceID() {
      return instanceId;
   }

   public String logPrefix() {
      return "HBaseBinlog instance " + instanceId + ": ";
   }

   public long getMaxCommitedID() {
      return 0;
   }

   public static TableName getTableName() {
      return TableName.valueOf(BINLOG_TABLE_NAME);
   }

   public static byte[] getColumnFamily() {
      return BINLOG_FAMILY;
   }

   public static byte[] getQualifier() {
      return BINLOG_QUAL;
   }

   public static List<String> getPrintableStats(boolean remote) throws Exception {
      List<String> stats = new ArrayList<>();

      Configuration conf = HBaseConfiguration.create();
      if (remote) {
         conf.set("hbase.zookeeper.quorum", ATRConfig.instance().getBinlogConnectionString());
      }
      stats.add("binlog zk quorum: " + conf.get("hbase.zookeeper.quorum"));
      Connection conn = ConnectionFactory.createConnection(conf);
      Table binlogTable = conn.getTable(getTableName());

      SimpleDateFormat fmt = new SimpleDateFormat("MM/dd HH:mm:ss.sss");
      String format = "| %-3s| %-12s| %-17s| %-3s| %-19s| %-35s|";
      stats.add(" ====================================================================================================");
      stats.add(String.format(format, "S", "latest WID", "latest CID", "TN",  "  write time", "    latest mutate table"));
      stats.add(" ====================================================================================================");
      int partNum = getBinlogPartionNum();
      for (int s = 0; s < partNum; ++s) {
         BinlogStats bs = BinlogStats.getBinlogStats(binlogTable, s, partNum);
         stats.add(String.format(format,
                                 bs.highest_rowkey.getSalt(partNum),
                                 bs.highest_rowkey.getWriteID(),
                                 bs.latest_commit_id,
                                 bs.highest_rowkey.getTotalNum(),
                                 fmt.format(new Date(bs.latest_write_time)),
                                 bs.latest_mutate_table.replaceFirst("TRAF_RSRVD_\\d+:", "")));
      }
      stats.add(" ====================================================================================================");
      return stats;
   }

   public static int getBinlogPartionNum() throws IOException {
      try {
         // TODO: binlog partial number depending on local ATRConfig is a good idea?
         return Integer.parseInt(ATRConfig.instance().getBinlogPartNum());
      }
      catch (KeeperException | NumberFormatException | InterruptedException e) {
         LOG.error("get binlog partion number error", e);
         throw new IOException("get binlog partion number error");
      }
   }

   public static int getBinlogFlushInterval() {
      try {
         return Integer.parseInt(ATRConfig.instance().getBinlogFlushInterval());
      }
      catch (Exception e) {
         LOG.error("get binlog flush interval error", e);
      }
      return -1;
   }

   private static String getShortHostName() throws UnknownHostException {
      InetAddress address = InetAddress.getLocalHost();
      String shortHostName = address.getHostName();
      int idot = shortHostName.indexOf(".");
      if (idot > 0) {
         shortHostName = shortHostName.substring(0, idot);
      }
      return shortHostName;
   }

   public static void setSaltMap(final ZooKeeperWatcher watcher) throws Exception {
      BashScript bashCmd = new BashScript("edb_pdsh $(trafconf -wname) trafconf -mynidforbinlog");
      bashCmd.execute();
      ATRConfig.instance().setSaltMap(bashCmd.stdout());
   }

   public static void resetSaltMap(final ZooKeeperWatcher watcher) throws Exception {
      Configuration conf = HBaseConfiguration.create();
      try (Connection conn = ConnectionFactory.createConnection(conf);
           Admin admin = conn.getAdmin()) {
         ClusterStatus cs = admin.getClusterStatus();
         Collection<ServerName> serverNames = cs.getServers();
         Collection<ServerName> deadServerNames = cs.getDeadServerNames();

         TreeSet<String> serverSet = new TreeSet<String>();
         for(ServerName serverName: serverNames) {
            String hostname = serverName.getHostname();
            int idot = hostname.indexOf(".");
            if (idot > 0) {
               hostname = hostname.substring(0, idot);
            }
            serverSet.add(hostname);
         }
         for(ServerName serverName : deadServerNames) {
            String hostname = serverName.getHostname();
            int idot = hostname.indexOf(".");
            if (idot > 0) {
               hostname = hostname.substring(0, idot);
            }
            serverSet.add(hostname);
         }

         StringBuilder saltMap = new StringBuilder();
         int i = 0;
         for(String server : serverSet) {
            saltMap.append(server)
                    .append(":")
                    .append(i)
                    .append("\n");
            i++;
         }
         ATRConfig.instance().setSaltMap(saltMap.toString());
      }
   }

   public static String getSaltMap(final ZooKeeperWatcher watcher) throws Exception {
      return ATRConfig.instance().getSaltMap();
   }

   public void initializeMySalt(Configuration conf) throws Exception {
      String saltMap = "";
      try (ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, "BinlogInitialization", null)) {
         saltMap = getSaltMap(watcher);
      } catch (IOException ioe) {
         LOG.warn("failed to get salt map from zookeeper." + ioe.getMessage());
         if (ioe.getMessage().contains("not exist")) {
            try (ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, "BinlogInitialization", null)) {
               resetSaltMap(watcher);
               saltMap = getSaltMap(watcher);
            }
         } else {
            throw ioe;
         }
      }

      String shortHostName = getShortHostName();
      LOG.info("HBaseBinlog initializeMySalt for host: " + shortHostName + ", salt map is: " + saltMap);
      String[] lines = saltMap.split("\n");
      for (String line : lines) {
         String[] hostAndSalt = line.split(":");
         if (hostAndSalt[0].trim().equalsIgnoreCase(shortHostName)) {
            saltNum = Integer.parseInt(hostAndSalt[1].trim());
            mapedSalt = (int)RowKey.computeMapedSalt(saltNum, partNum);
            break;
         }
      }
      if (saltNum == -1) {
         LOG.error("HBaseBinlog no match salt for host: " + shortHostName + ", salt map is: " + saltMap);
         throw new IOException("No match salt for host: " + shortHostName);
      }
      //initialize finished
      shouldReinitializeSalt = false;
   }

   public int getMySalt(Configuration conf) {
      try {
         if(shouldReinitializeSalt) {
           synchronized(initSaltLock) {
             if(shouldReinitializeSalt)
               initializeMySalt(conf);
           }
         }
      } catch (Exception e) {
         LOG.warn("Error in getMySalt while re-initialize.", e);
      }
      return saltNum;
   }

   public int isThisWidFlushed(long wid, long tid) {
     synchronized(changeStateLock) {
        if (state instanceof BinlogCacheState ) { //check if we are now in cache state
          return BINLOG_WRITE_ERROR;
        }
     }
     if(wid <= lastFlushedWid )
       return BINLOG_FLUSH_OK;
     else
       return BINLOG_FLUSH_WAIT;
   }

   @Override
   public void reInitialize() {
      // real reinitialize in worker thread when appendding binlog.
      LOG.info("HBaseBinlog get reinitialize signal.");
      shouldReinitialize = true;
   }

   synchronized public void initializeBinlog(Configuration conf) {
      if (ATRConfig.instance().isATRXDCEnabled() == false)
         return;
      try {
         if(shouldReinitialize) {
            realReinitialize(conf);
         }
      } catch (Exception e) {
         LOG.warn("Error in getMySalt while re-initialize.", e);
      }
   }

   private void realReinitialize(Configuration conf) throws IOException {
      // TODO: some potential resource leak?
      try {
         synchronized(initializeLock) {
            if (shouldReinitialize) {
               LOG.info("HBaseBinlog real reinitialization for instance: " + instanceId);
               this.localConfig = conf;
               this.partNum = getBinlogPartionNum();
               initializeMySalt(conf);
               LOG.info("HBaseBinlog construct for salt: " + saltNum);

               String envEnableStrictBinlogAntiDup = System.getenv("ENABLE_STRICT_BINLOG_ANTI_DUP");
               if (envEnableStrictBinlogAntiDup!= null)
                 enableStrictBinlogAntiDup = (Integer.parseInt(envEnableStrictBinlogAntiDup.trim()) == 0) ? false : true;

               if (daemonThreadPool == null)
                  daemonThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

               state = null;
               if (normalState == null) normalState = new BinlogNormalState(this);
               if (cacheState == null) cacheState  = new BinlogCacheState(this);
               if (fatalState == null) fatalState  = new BinlogFatalState(this);

               if (cacheState.getCachedFiles().size() > 0) {
                  state = cacheState;
                  normalState.checkAvailable();
                  cacheState.reinitializeOnNotReady = true;
                  if (cacheState.checkAvailable() == false) {
                     state = fatalState;
                     LOG.error("HBaseBinlog fatal state is used when real reinitializeation");
                  } else {
                     LOG.info("HBaseBinlog cache state is used when real reinitializeation");
                  }
               } else if (normalState.checkAvailable()) {
                  state = normalState;
                  LOG.info("HBaseBinlog normal state is used when real reinitializeation");
               } else if (cacheState.checkAvailable()) {
                  state = cacheState;
                  LOG.info("HBaseBinlog cache state is used when real reinitializeation");
               } else {
                  state = fatalState;
                  LOG.error("HBaseBinlog fatal state is used when real reinitializeation");
               }

               int intv = getBinlogFlushInterval();
               if( ATRConfig.instance().getSyncMode().equals(ATRConfig.SYNC_MODE_MAX_RELIABILITY)) {
                  intv = 1;
               }
               if (isFirstTimeInit) {
                  if(intv <= 0 || intv > MAX_FLUSH_INTERVAL) //something wrong use default
                     startBinlogFlusher(DEFAULT_FLUSH_INTERVAL);
                  else
                     startBinlogFlusher(intv);
                  isFirstTimeInit = false;
               }

               initializeNextWriteID();
               ATRConfig.instance().registerReinitializeOnConfigChange(this);
               shouldReinitialize = false;
            }
         }
      }
      catch (Exception e) {
         LOG.error("HBaseBinlog re-initialize failed.", e);
         throw new IOException("HBaseBinlog re-initialize failed.", e);
      }
   }

   public static void clearBinlog(Configuration conf, long timeStamp) throws IOException {
      try(Connection conn = ConnectionFactory.createConnection(conf);
          Table binlog_table = conn.getTable(getTableName());
          BufferedMutator binlog_mutator = conn.getBufferedMutator(getTableName())) {
         final Scan scan = new Scan();
         scan.setTimeRange(0, timeStamp);
         scan.addColumn(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());

         try(ResultScanner scanner = binlog_table.getScanner(scan)) {
            for (final Result result : scanner) {
               final List<Cell> cells = result.listCells();
               for (final Cell cell : cells) {
                  final HBaseBinlog.RowKey rowKey = new HBaseBinlog.RowKey(CellUtil.cloneRow(cell));
                  Delete delete = new Delete(CellUtil.cloneRow(cell));
                  binlog_mutator.mutate(delete);
                  if(rowKey.getWriteID() != 0){
                    TransactionMutationMsg tmm = TransactionMutationMsg.parseFrom(CellUtil.cloneValue(cell));
                    if(LOG.isTraceEnabled()) {
                       LOG.trace(String.format("deleted --> %s, commitID:%s", rowKey, tmm.getCommitId()));
                    }
                  }
               }
            }
         }
         binlog_mutator.flush();
      }
      return;
   }

   public static void createBinlog(Configuration conf, int nodeNum) throws IOException{
      try(Connection conn = ConnectionFactory.createConnection(conf);
         Admin admin = conn.getAdmin()) {
         TableName binlogTable = TableName.valueOf(BINLOG_TABLE_NAME);
         if(admin.isTableAvailable(binlogTable)) {
            System.out.println("disable '" + BINLOG_TABLE_NAME + "'");
            admin.disableTable(binlogTable);
            System.out.println("drop '" + BINLOG_TABLE_NAME + "'");
            admin.deleteTable(binlogTable);
         }
         else {
            try {
               NamespaceDescriptor nsDescriptor =  NamespaceDescriptor.create(BINLOG_NAMESPACE).build();
               admin.createNamespace(nsDescriptor);
               LOG.info("HBaseBinlog namespace created: " + BINLOG_NAMESPACE);
            }
            catch(NamespaceExistException e) {
               // just ignore
            }
         }

         HTableDescriptor tableDescriptor = new HTableDescriptor(binlogTable);
         HColumnDescriptor mf_column = new HColumnDescriptor(BINLOG_FAMILY);
         mf_column.setTimeToLive(BINLOG_TABLE_TIME_TO_LIVE);
         HColumnDescriptor mt_column = new HColumnDescriptor(BINLOG_META_FAMILY);
         mt_column.setMaxVersions(BINLOG_TABLE_MAX_VERSION);

         tableDescriptor.addFamily(mf_column);
         tableDescriptor.addFamily(mt_column);

         if(nodeNum > 1) {
            byte[][] splitKeys = new byte[nodeNum - 1][];
            for (int i = 0; i < nodeNum-1; i ++) {
               splitKeys[i] = Bytes.toBytes((i+1) << 20);
            }
            tableDescriptor.setMaxFileSize(MAX_FILE_SIZE);
            System.out.println("create " + tableDescriptor.toString());
            admin.createTable(tableDescriptor,splitKeys);
         } else {
            System.out.println("create " + tableDescriptor.toString());
            admin.createTable(tableDescriptor);
         }
      }
   }

   public static void rebuildBinlog() throws Exception {
      // TODO: implement this, temporary rebuild in shell.
   }

   public static void scanBinlog(Configuration conf, int termWidth) throws Exception {
      try(Connection conn = ConnectionFactory.createConnection(conf);
          Table binlog_table = conn.getTable(getTableName())) {

         KeyValuePrinter kvp = new KeyValuePrinter(termWidth, 28);

         System.out.println("Scan connection: " + conf.get("hbase.zookeeper.quorum"));
         kvp.print("Readable RowKey", "Value");

         final Scan scan = new Scan();
         scan.addColumn(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());

         long rowCount = 0;
         long startTime = System.currentTimeMillis();
         try(ResultScanner scanner = binlog_table.getScanner(scan)) {
            for (final Result result : scanner) {
               final List<Cell> cells = result.listCells();
               for (final Cell cell : cells) {
                  final HBaseBinlog.RowKey rowKey = new HBaseBinlog.RowKey(CellUtil.cloneRow(cell));
                  kvp.print(rowKey.toString(), Bytes.toStringBinary(CellUtil.cloneValue(cell)));
                  ++rowCount;
               }
            }
         }
         long endTime = System.currentTimeMillis();
         System.out.format("\n%d row(s) in %.3f seconds\n", rowCount, (endTime - startTime) / 1000.0);
      }
   }

   public static void scanBinlogReader(Configuration conf, int termWidth) throws Exception {
      try(Connection conn = ConnectionFactory.createConnection(conf);
          Table binlog_table = conn.getTable(TableName.valueOf(BINLOG_READER_TABLE_NAME))) {

         KeyValuePrinter kvp = new KeyValuePrinter(termWidth, 25);
         System.out.println("Scan connection: " + conf.get("hbase.zookeeper.quorum"));
         kvp.print("ROW", "Value");

         final Scan scan = new Scan();
         long rowCount = 0;
         long startTime = System.currentTimeMillis();
         try(ResultScanner scanner = binlog_table.getScanner(scan)) {
            for (final Result result : scanner) {
               final List<Cell> cells = result.listCells();
               for (final Cell cell : cells) {
                  kvp.print(Bytes.toStringBinary(CellUtil.cloneRow(cell)), Bytes.toStringBinary(CellUtil.cloneValue(cell)));
                  ++rowCount;
               }
            }
         }
         long endTime = System.currentTimeMillis();
         System.out.format("\n%d row(s) in %.3f seconds\n", rowCount, (endTime - startTime) / 1000.0);
      }
   }

   public static void print_usage() {
      System.out.println("Usage: prog {commands}");
      System.out.println("   commands:");
      System.out.println("             clearbinlog                clear binlog contents");
      System.out.println("             scanbinlog [remote]        scan binlog contents");
      System.out.println("             scanbinlogreader [remote]  scan binlog reader contents");
      System.out.println("             stats [remote]             print highest rowkey of salt");
      System.out.println("             partnum                    print binlog partition num");
      System.out.println("             resetsm                    print highest rowkey of salt");
      System.out.println("             -rb                        rebuild binlog.");
      System.out.println("             -h                         print this message.");
   }

   public static void main(final String[] args) {
      setupConf();
      final Configuration conf = HBaseConfiguration.create();

      try {
         if (args[0].compareTo("-rb") == 0) {
            rebuildBinlog();
         }
         else if (args[0].compareTo("stats") == 0) {
            List<String> lines = getPrintableStats(args.length != 1);
            for (String line : lines) {
               System.out.println(line);
            }
         }
         else if (args[0].compareTo("partnum") == 0) {
            if (args.length == 2 && args[1].compareTo("remote") == 0) {
               try {
                  String partnumZkPath = ATRConfig.instance().getBinlogPartNumZKPath();
                  conf.set("hbase.zookeeper.quorum", ATRConfig.instance().getBinlogConnectionString());
                  ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "BinlogCLIZK", null);
                  String remotePartNumStr = new String(ZKUtil.getData(zkw, partnumZkPath));
                  System.out.println(remotePartNumStr);
               } catch (KeeperException e) {
                  if (e.code().equals(KeeperException.Code.NONODE)) {
                     // using local binlog partition number for pure hbase cluster.
                     System.out.println(getBinlogPartionNum());
                  } else {
                     throw e;
                  }
               }
            }
            else {
               System.out.println(getBinlogPartionNum());
            }
         }
         else if (args[0].compareTo("resetsm") == 0) {
            try (ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, "BinlogCLIZK", null)) {
               setSaltMap(watcher);
               System.out.println("reset salt map success, current salt map is:");
               System.out.println(getSaltMap(watcher));
            }
         }
         else if (args[0].compareTo("clearbinlog") == 0) {
            clearBinlog(conf, Long.parseLong(args[1]) * 1000);
         }
         else if (args[0].compareTo("scanbinlog") == 0) {
            int termWidth = Integer.parseInt(args[1]);
            if (args.length == 3 && args[2].compareTo("remote") == 0) {
               conf.set("hbase.zookeeper.quorum", ATRConfig.instance().getBinlogConnectionString());
            }
            scanBinlog(conf, termWidth);
         }
         else if (args[0].compareTo("scanbinlogreader") == 0) {
            int termWidth = Integer.parseInt(args[1]);
            if (args.length == 3 && args[2].compareTo("remote") == 0) {
               conf.set("hbase.zookeeper.quorum", ATRConfig.instance().getBinlogConnectionString());
            }
            scanBinlogReader(conf, termWidth);
         }
         else if (args[0].compareTo("createbinlog") == 0) {
            if (args.length == 3 && args[2].compareTo("remote") == 0) {
               String remote_quorum = ATRConfig.instance().getBinlogConnectionString();
               if (remote_quorum.isEmpty()) {
                  System.err.println("***ERROR: can't rebuild remote binlog due to the remote zookeeper quorum was not set, to set it, run 'atrxdc -set bcstr'.");
                  System.exit(-1);
               }
               conf.set("hbase.zookeeper.quorum", remote_quorum);
            }
            createBinlog(conf, Integer.parseInt(args[1]));
         }
         else if (args[0].compareTo("getttl") == 0) {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            HColumnDescriptor[] colFamilies  =admin.getTableDescriptor(TableName.valueOf(BINLOG_TABLE_NAME)).getColumnFamilies();
            for(HColumnDescriptor colFamily:colFamilies) {
               if(colFamily.getNameAsString().equals("mf"))
                  System.out.println(colFamily.getTimeToLive() + " seconds ");
            }

         }
         else {
            System.out.println("unsupported command: " + args[0]);
            print_usage();
         }
      }
      catch (Exception e) {
         e.printStackTrace(System.err);
         System.exit(-1);
      }
   }

   private static class KeyValuePrinter {
      private int termWidth;
      private int keyWidth;
      private int spliterWidth = 2;

      public KeyValuePrinter(int termWidth, int keyWidth) {
         this.termWidth = termWidth;
         this.keyWidth  = keyWidth;
      }

      public void print(String key, String value) {
         StringBuilder sb = new StringBuilder();
         String keyRegexp = String.format("(?<=\\G.{%d})", keyWidth);
         String valueRegexp = String.format("(?<=\\G.{%d})", termWidth - keyWidth - spliterWidth);
         String[] keySplits = key.split(keyRegexp);
         String[] valuesSplits = value.split(valueRegexp);

         int i = 0;
         while (i < keySplits.length || i < valuesSplits.length) {
            int nkspace = keyWidth + spliterWidth;
            if (i < keySplits.length) {
               sb.append(keySplits[i]);
               nkspace -= keySplits[i].length();
            }
            for (int ns = 0; ns < nkspace; ++ns) {
               sb.append(' ');
            }
            if (i < valuesSplits.length) {
               sb.append(valuesSplits[i]);
            }
            sb.append("\n");
            ++i;
         }

         System.out.print(sb.toString());
      }
   }

   private class JVMShutdownHook extends Thread {
      public void run() {
         LOG.info("JVM shutdown hook is running");
         try {
            if (cacheState != null && cacheState.binlogCacheFileWriter != null) {
               LOG.info("HBaseBinlog JVMShutdownHook start to close cachefile.");
               cacheState.binlogCacheFileWriter.close();
            }
         } catch (Exception e) {
            LOG.warn("HBaseBinlog JVMShutdownHook failed due to ", e);
         }
      }
   }
}

