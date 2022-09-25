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

import com.esgyn.producer.BinlogProducer;
import java.sql.SQLException;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Map;
import java.util.Arrays;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.transactional.RMInterface;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockMode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.apache.hadoop.hbase.util.TrxEnvironmentEdgeManager;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.snapshot.SnapshotExistsException;
//import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
//import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType; 
//import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType ;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.client.Durability;
import org.trafodion.sql.HTableClient;
import org.trafodion.sql.TrafConfiguration;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.DtmConst;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.CompressionTest;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor.Builder;

import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafEstimateRowCountRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafEstimateRowCountResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;


import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafEstimateRowCountRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafEstimateRowCountResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;
import java.lang.management.*;

public class HBaseClient {

    static Logger logger = Logger.getLogger(HBaseClient.class.getName());
    static int sv_peer_id = 0;
    private static Configuration config = null;
    private static STRConfig pSTRConfig = null;
    final long megaByte = 1024L * 1024L;

    // variables used for getRegionStats() and getClusterStats()
    private int regionStatsEntries = 0;
    private int clusterStatsState = 1;
    private TrafRegionStats rsc;
    private byte[][] regionInfo = null;
    private int currRegion = 0;
    private static final int MAX_REGION_INFO_ROWS = 100;

    // this set of constants MUST be kept in sync with the C++ enum in
    // ExpHbaseDefs.h
    public static final int HBASE_NAME = 0;
    public static final int HBASE_MAX_VERSIONS = 1;
    public static final int HBASE_MIN_VERSIONS = 2;
    public static final int HBASE_TTL = 3;
    public static final int HBASE_BLOCKCACHE = 4;
    public static final int HBASE_IN_MEMORY = 5;
    public static final int HBASE_COMPRESSION = 6;
    public static final int HBASE_BLOOMFILTER = 7;
    public static final int HBASE_BLOCKSIZE = 8;
    public static final int HBASE_DATA_BLOCK_ENCODING = 9;
    public static final int HBASE_CACHE_BLOOMS_ON_WRITE = 10;
    public static final int HBASE_CACHE_DATA_ON_WRITE = 11;
    public static final int HBASE_CACHE_INDEXES_ON_WRITE = 12;
    public static final int HBASE_COMPACT_COMPRESSION = 13;
    public static final int HBASE_PREFIX_LENGTH_KEY = 14;
    public static final int HBASE_EVICT_BLOCKS_ON_CLOSE = 15;
    public static final int HBASE_KEEP_DELETED_CELLS = 16;
    public static final int HBASE_REPLICATION_SCOPE = 17;
    public static final int HBASE_MAX_FILESIZE = 18;
    public static final int HBASE_COMPACT = 19;
    public static final int HBASE_DURABILITY = 20;
    public static final int HBASE_MEMSTORE_FLUSH_SIZE = 21;
    public static final int HBASE_SPLIT_POLICY = 22;
    public static final int HBASE_ENCRYPTION = 23;
    public static final int HBASE_CACHE_DATA_IN_L1 = 24;
    public static final int HBASE_PREFETCH_BLOCKS_ON_OPEN = 25;
    public static final int HBASE_HDFS_STORAGE_POLICY= 26;
    public static final int HBASE_REGION_REPLICATION = 27;

    public static final String BINLOG_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER";
    public static String BINLOG_META_CF_NAME = "mt_";
    public static final byte[] BINLOG_META_COL_QUAL = Bytes.toBytes("col");
    public static final byte[] BINLOG_META_KEY_QUAL = Bytes.toBytes("key");
 

    private static String instanceId;
    private static String projectId;
    private static volatile Connection hbaseConnection;
    private static volatile Connection bigtableConnection;
    private static volatile Connection hbaseAggConnection;
    private static boolean enableRowLevelLock = false;

    public HBaseClient() {
    }

    static {
        System.setProperty("hostName", System.getenv("HOSTNAME"));
        String trafLog = System.getProperty("TRAF_LOG"); 
        if (trafLog == null) {  
            System.setProperty("TRAF_LOG", System.getenv("TRAF_LOG"));  
        } 
    	String confFile = System.getProperty("trafodion.log4j.configFile");
        System.setProperty("trafodion.root", System.getenv("TRAF_HOME"));
    	if (confFile == null) {
           confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
    	}
    	PropertyConfigurator.configure(confFile);
        Configuration my_config  = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        MDC.put("PID", pid);
	String sv_peer_id_string = System.getenv("CONNECT_TO_XDC_PEER_ID");
        String envEnableRowLevelLock = System.getenv("ENABLE_ROW_LEVEL_LOCK");
        if (envEnableRowLevelLock != null)
            try {
                enableRowLevelLock = (Integer.parseInt(envEnableRowLevelLock.trim()) == 0) ? false : true;
            } catch (Exception e) {}
	if ( sv_peer_id_string == null) {
	    config = my_config;
	}
	else {
	    sv_peer_id = Integer.parseInt(sv_peer_id_string);
	    try {
		pSTRConfig = STRConfig.getInstance(my_config);
		Configuration peer_config = pSTRConfig.getPeerConfiguration(sv_peer_id, true);
		if (peer_config == null) {
		    System.out.println("XDC Peer ID: " + sv_peer_id 
				       + " does not exist OR it has not been configured for synchronization."
				       + " Will connect with the local HBase cluster."
				       );
		    config = my_config;
		}
		else {
		    System.out.println("Connecting to the XDC Peer ID: " + sv_peer_id);
		    config = peer_config;
		}
	    }
	    catch (Exception e) {
		System.out.println("Unable to get information for" 
				   + " Peer ID: " + sv_peer_id 
				   + " Connecting to the local cluster"
				   );
		pSTRConfig = null;
		config = my_config;
	    }

	}

    }

    public static Connection getHbaseConnection() {
           return hbaseConnection;
    }

    public static Configuration getConfiguration() 
    {
       return config;
    }

    public static Connection getConnection() throws IOException 
    {
       return getConnection(false);
    }

    public static Connection getAggConnection(Configuration configuration) throws IOException
    {
        Connection connection = null;
        if (hbaseAggConnection == null) {
            synchronized (HBaseClient.class) {
                if (hbaseAggConnection == null) {
                    hbaseAggConnection = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        connection = hbaseAggConnection;
        return connection;
    }
    
    public static Connection getConnection(boolean bigtable) throws IOException 
    {
        Connection connection = null;
        if (bigtable) {
            if (bigtableConnection == null) {
                synchronized (HBaseClient.class) {
                    if (bigtableConnection == null) {
                        bigtableConnection = BigtableConfiguration.connect(projectId, instanceId);
                    }
                }
            }
            connection = bigtableConnection;
        }
        else {
            if (hbaseConnection == null) {
                synchronized (HBaseClient.class) {
                    if (hbaseConnection == null) {
                        hbaseConnection = ConnectionFactory.createConnection(config);
                    }
                }
            }
            connection = hbaseConnection;
        }
        return connection;
    }

    public static boolean reconnect() throws IOException
    {
        Connection connection;
        logger.info("HBaseClient.reconnect()");
        if (bigtableConnection != null) {
            synchronized (HBaseClient.class) {
                if (bigtableConnection != null) {
                    bigtableConnection.close();
                    bigtableConnection = null;
                }
            }
            connection =  getConnection(true);
        }
        else {
            if (hbaseConnection != null) {
                synchronized (HBaseClient.class) {
                    if (hbaseConnection != null) {
                        try {
                            hbaseConnection.close();
                            hbaseConnection = null;
                        } catch (IOException e) {
                        }
                    }
                }
            }
            if (hbaseAggConnection != null) {
                synchronized (HBaseClient.class) {
                    if (hbaseAggConnection != null) {
                        try {
                            hbaseAggConnection.close();
                            hbaseAggConnection = null;
                        } catch (IOException e) {
                        }
                    }
                }
            }
            connection =  getConnection(false);
        }
        return (connection != null);
    }

    public boolean init(String connectParam1, String connectParam2, boolean lc_bigtable)
	throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException
    {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.init(" + connectParam1 + ", " + connectParam2
                         + ") called.");
        
        
        if (lc_bigtable) {
           projectId = connectParam1;
           instanceId = connectParam2;
        }
        getConnection(lc_bigtable);
        return true;
    }

    private void addCoprocessor(HTableDescriptor desc) throws IOException {
        String[] coprocessors = config.getStrings("hbase.coprocessor.region.classes");
        if (coprocessors != null) {
           for (int i = 0; i < coprocessors.length ; i++) {
              desc.addCoprocessor(coprocessors[i].trim());
           }
        }
    }

    public boolean create(String tblName, Object[]  colFamNameList,
                          boolean isMVCC) 
        throws IOException, MasterNotRunningException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.create(" + tblName + ") called, and MVCC is " + isMVCC + ".");
            HTableDescriptor desc = new HTableDescriptor(tblName);
            addCoprocessor(desc);
            for (int i = 0; i < colFamNameList.length ; i++) {
		String  colFam = (String)colFamNameList[i];
                HColumnDescriptor colDesc = new HColumnDescriptor(colFam);
                if (isMVCC)
                  colDesc.setMaxVersions(DtmConst.MVCC_MAX_VERSION);
                else
                  colDesc.setMaxVersions(DtmConst.SSCC_MAX_VERSION);
                desc.addFamily(colDesc);
            }
            Admin admin = getConnection().getAdmin();
            admin.createTable(desc);
            admin.close();
            return true;
   } 

   // used for returning two flags from setDescriptors method

   private class ChangeFlags {
       boolean tableDescriptorChanged;
       boolean columnDescriptorChanged;
       boolean storagePolicyChanged;

       ChangeFlags() {
           tableDescriptorChanged = false;
           columnDescriptorChanged = false;
           storagePolicyChanged = false;
       }

       void setTableDescriptorChanged() {
           tableDescriptorChanged = true;
       }

       void setColumnDescriptorChanged() {
           columnDescriptorChanged = true;
       }

       boolean tableDescriptorChanged() {
           return tableDescriptorChanged;
       }

       boolean columnDescriptorChanged() {
           return columnDescriptorChanged;
       }

       void setStoragePolicyChanged(String str) {
           storagePolicy_ = str;
           storagePolicyChanged = true;
       }

       boolean storagePolicyChanged()    {
           return storagePolicyChanged;
       }

       String storagePolicy_;


   }

   private ChangeFlags setDescriptors(Object[] tableOptions,
                                      HTableDescriptor desc,
                                      HColumnDescriptor colDesc,
                                      int defaultVersionsValue) 
       throws IOException {
       ChangeFlags returnStatus = new ChangeFlags();
       String trueStr = "TRUE";
       if (logger.isDebugEnabled()) logger.debug("setDesc, option length: " + tableOptions.length);

       for (int i = 0; i < tableOptions.length; i++) {
           if (i == HBASE_NAME)	
               continue ;
           String tableOption = (String)tableOptions[i];
           if ((i != HBASE_MAX_VERSIONS) && (tableOption.isEmpty()))
               continue ;
           switch (i) {
           case HBASE_MAX_VERSIONS:
               if (tableOption.isEmpty()) {
                   if (colDesc.getMaxVersions() != defaultVersionsValue) {
                       colDesc.setMaxVersions(defaultVersionsValue);
                       returnStatus.setColumnDescriptorChanged();
                   }
               }
               else {
                   colDesc.setMaxVersions
                       (Integer.parseInt(tableOption));
                   returnStatus.setColumnDescriptorChanged();
               }
               break ;
           case HBASE_MIN_VERSIONS:
               colDesc.setMinVersions
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_TTL:
               colDesc.setTimeToLive
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKCACHE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setBlockCacheEnabled(true);
               else
                   colDesc.setBlockCacheEnabled(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_IN_MEMORY:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setInMemory(true);
               else
                   colDesc.setInMemory(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ"))
	       {    // throws IOException
		   CompressionTest.testCompression(Algorithm.GZ);
                   colDesc.setCompressionType(Algorithm.GZ);
	       }
               else if (tableOption.equalsIgnoreCase("LZ4"))
	       {   // throws IOException
		   CompressionTest.testCompression(Algorithm.LZ4);
                   colDesc.setCompressionType(Algorithm.LZ4);
	       }
               else if (tableOption.equalsIgnoreCase("LZO"))
	       {   // throws IOException
		   CompressionTest.testCompression(Algorithm.LZO);
                   colDesc.setCompressionType(Algorithm.LZO);
	       }
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY"))
	       {   // throws IOException
		   CompressionTest.testCompression(Algorithm.SNAPPY);
                   colDesc.setCompressionType(Algorithm.SNAPPY); 
	       }
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOOMFILTER:
               if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setBloomFilterType(BloomType.NONE);
               else if (tableOption.equalsIgnoreCase("ROW"))
                   colDesc.setBloomFilterType(BloomType.ROW);
               else if (tableOption.equalsIgnoreCase("ROWCOL"))
                   colDesc.setBloomFilterType(BloomType.ROWCOL); 
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKSIZE:
               if (logger.isDebugEnabled()) 
                   logger.debug("setDescriptor, blockSize: " + Integer.parseInt(tableOption));
               colDesc.setBlocksize
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_ENCRYPTION:
               if (tableOption.equalsIgnoreCase("AES"))
                   colDesc.setEncryptionType("AES");
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_DATA_BLOCK_ENCODING:
               if (tableOption.equalsIgnoreCase("DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
               else if (tableOption.equalsIgnoreCase("FAST_DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
               else if (tableOption.equalsIgnoreCase("PREFIX"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
               else if (tableOption.equalsIgnoreCase("PREFIX_TREE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_BLOOMS_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheBloomsOnWrite(true);
               else
                   colDesc.setCacheBloomsOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_DATA_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheDataOnWrite(true);
               else
                   colDesc.setCacheDataOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_INDEXES_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheIndexesOnWrite(true);
               else
                   colDesc.setCacheIndexesOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPACT_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ")) {
		   // throws IOException
		   CompressionTest.testCompression(Algorithm.GZ);
                   colDesc.setCompactionCompressionType(Algorithm.GZ); 
	       }
               else if (tableOption.equalsIgnoreCase("LZ4")) {
		   // throws IOException
		   CompressionTest.testCompression(Algorithm.LZ4);
                   colDesc.setCompactionCompressionType(Algorithm.LZ4);
	       }
               else if (tableOption.equalsIgnoreCase("LZO")) {
		   // throws IOException
		   CompressionTest.testCompression(Algorithm.LZO);
                   colDesc.setCompactionCompressionType(Algorithm.LZO);
	       }
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompactionCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY")) {
		   // throws IOException
		   CompressionTest.testCompression(Algorithm.SNAPPY);
                   colDesc.setCompactionCompressionType(Algorithm.SNAPPY); 
	       }
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_PREFIX_LENGTH_KEY:
               desc.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY,
                             tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_EVICT_BLOCKS_ON_CLOSE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setEvictBlocksOnClose(true);
               else
                   colDesc.setEvictBlocksOnClose(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_KEEP_DELETED_CELLS:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setKeepDeletedCells(true);
               else
                   colDesc.setKeepDeletedCells(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_REPLICATION_SCOPE:
               colDesc.setScope
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_MAX_FILESIZE:
               desc.setMaxFileSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_COMPACT:
              if (tableOption.equalsIgnoreCase(trueStr))
                   desc.setCompactionEnabled(true);
               else
                   desc.setCompactionEnabled(false); 
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_DURABILITY:
               if (tableOption.equalsIgnoreCase("ASYNC_WAL"))
                   desc.setDurability(Durability.ASYNC_WAL);
               else if (tableOption.equalsIgnoreCase("FSYNC_WAL"))
                   desc.setDurability(Durability.FSYNC_WAL);
               else if (tableOption.equalsIgnoreCase("SKIP_WAL"))
                   desc.setDurability(Durability.SKIP_WAL);
               else if (tableOption.equalsIgnoreCase("SYNC_WAL"))
                   desc.setDurability(Durability.SYNC_WAL);
               else if (tableOption.equalsIgnoreCase("USE_DEFAULT"))
                   desc.setDurability(Durability.USE_DEFAULT);
               returnStatus.setTableDescriptorChanged(); 
               break ;
           case HBASE_MEMSTORE_FLUSH_SIZE:
               desc.setMemStoreFlushSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
	   case HBASE_CACHE_DATA_IN_L1:
	       if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheDataInL1(true);
               else
                   colDesc.setCacheDataInL1(false); 
               returnStatus.setColumnDescriptorChanged();
               break ;
	   case HBASE_PREFETCH_BLOCKS_ON_OPEN:
              if (tableOption.equalsIgnoreCase(trueStr))
		  colDesc.setPrefetchBlocksOnOpen(true);
	      else
		  colDesc.setPrefetchBlocksOnOpen(false); 
	      returnStatus.setColumnDescriptorChanged();
	      break ;
           case HBASE_HDFS_STORAGE_POLICY:
               //TODO HBase 2.0 support this
               //So when come to HBase 2.0, no need to do this via HDFS, just set here
             returnStatus.setStoragePolicyChanged(tableOption);
             break ;
           case HBASE_SPLIT_POLICY:
               // This method not yet available in earlier versions
               // desc.setRegionSplitPolicyClassName(tableOption));
               desc.setValue(desc.SPLIT_POLICY, tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_REGION_REPLICATION:
               desc.setRegionReplication
                   (Integer.parseInt(tableOption));
               returnStatus.setTableDescriptorChanged();
               break;
           default:
               break;
           }
       }

       return returnStatus;
   }
   

   // incrBackupEnabled: this table is enabled for incremental backup.
   public boolean createk(String tblName, Object[] tableOptions,
                          Object[]  beginEndKeys, long transID, 
                          int numSplits, int keyLength,
                          boolean isMVCC, boolean incrBackupEnabled)
       throws IOException, MasterNotRunningException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.createk(" + tblName + ") called for transID " + transID);
            String trueStr = "TRUE";
            ChangeFlags setDescRet = null;
            HTableDescriptor desc = new HTableDescriptor(tblName);
            addCoprocessor(desc);
            int defaultVersionsValue = 0;
            if (isMVCC)
                defaultVersionsValue = DtmConst.MVCC_MAX_VERSION;
            else
                defaultVersionsValue = DtmConst.SSCC_MAX_VERSION;

            // column family names are space delimited list of names.
            // extract all family names and add to table descriptor.
            // All other default and specified options remain the same for all families.
            String colFamsStr = (String)tableOptions[HBASE_NAME];
            String[] colFamsArr = colFamsStr.split("\\s+"); 

            for (int i = 0; i < colFamsArr.length; i++){            
                String colFam = colFamsArr[i];

                HColumnDescriptor colDesc = new HColumnDescriptor(colFam);

                // change the descriptors based on the tableOptions; 
                setDescRet = setDescriptors(tableOptions,desc /*out*/,colDesc /*out*/, defaultVersionsValue);
                
                if (logger.isDebugEnabled()) 
                   logger.debug("After addFamily."
                                + ",table: " + tblName 
                                + ", transID: " + transID
                                + ", table desc: " + desc
                                + ", col desc: " + colDesc
                                );
                desc.addFamily(colDesc);
            }

            Admin admin = getConnection().getAdmin();
               if (beginEndKeys != null && beginEndKeys.length > 0)
               {
                  byte[][] keys = new byte[beginEndKeys.length][];
                  for (int i = 0; i < beginEndKeys.length; i++){
                     keys[i] = (byte[])beginEndKeys[i]; 
                     if (logger.isDebugEnabled()) logger.debug("HBaseClient.createk key #" + i + "value" + keys[i] + ") called.");
                  }
                  if (transID != 0 && (hbaseConnection != null)) {
                     RMInterface table = new RMInterface(HBaseClient.getConnection());
                     table.createTable(desc, keys, numSplits, keyLength, transID, incrBackupEnabled);
                     if (logger.isDebugEnabled()) logger.debug("HBaseClient.createk beginEndKeys(" + beginEndKeys + ") called.");
                  } else {
                     admin.createTable(desc, keys);
                  }
               }
               else {
                  if (transID != 0 && (hbaseConnection != null)) {
                     RMInterface table = new RMInterface(HBaseClient.getConnection());
                     table.createTable(desc, null, numSplits, keyLength, transID, incrBackupEnabled);
                  } else {
                     admin.createTable(desc);
                  }
               }

            if(setDescRet!= null)
            {
              if(setDescRet.storagePolicyChanged())
              {
                 Object tableOptionsStoragePolicy[] = new Object[HBASE_HDFS_STORAGE_POLICY+1];
                 for(int i=0; i<HBASE_HDFS_STORAGE_POLICY; i++)
                   tableOptionsStoragePolicy[i]="";
                 tableOptionsStoragePolicy[HBASE_HDFS_STORAGE_POLICY]=(String)setDescRet.storagePolicy_ ;
                 tableOptionsStoragePolicy[HBASE_NAME]=(String)tblName;
                 alter(tblName,tableOptionsStoragePolicy,transID);
              }
            }
            else
              admin.close();
        return true;
    }

    public boolean registerTruncateOnAbort(String tblName, long transID)
        throws MasterNotRunningException, IOException {

           if (transID != 0 && (hbaseConnection != null)) {
              RMInterface table = new RMInterface(HBaseClient.getConnection());
              table.truncateTableOnAbort(tblName, transID);
           }
        return true;
    }

    private void waitForCompletion(String tblName,Admin admin) 
        throws IOException {
        // poll for completion of an asynchronous operation
        boolean keepPolling = true;
        while (keepPolling) {
            // status.getFirst() returns the number of regions yet to be updated
            // status.getSecond() returns the total number of regions
            Pair<Integer,Integer> status = admin.getAlterStatus(tblName.getBytes());

            keepPolling = (status.getFirst() > 0) && (status.getSecond() > 0);
            if (keepPolling) {
                try {
                    Thread.sleep(2000);  // sleep two seconds or until interrupted
                }
                catch (InterruptedException e) {
                    // ignore the interruption and keep going
                }    
            }
        }
    }

    public boolean alter(String tblName, Object[] tableOptions, long transID)
        throws IOException, MasterNotRunningException {

        if (logger.isDebugEnabled()) logger.debug("HBaseClient.alter(" + tblName + ") called.");
        Admin admin = getConnection().getAdmin();
        HTableDescriptor htblDesc = admin.getTableDescriptor(TableName.valueOf(tblName));       
        HColumnDescriptor[] families = htblDesc.getColumnFamilies();
        String colFam = (String)tableOptions[HBASE_NAME];
        if (colFam == null)
            return true; // must have col fam name

        // if the only option specified is col fam name and this family doesnt already
        // exist, then add it.
        boolean onlyColFamOptionSpecified = true;
        for (int i = 0; (onlyColFamOptionSpecified && (i < tableOptions.length)); i++) {
            if (i == HBASE_NAME)	
                continue ;

            if (((String)tableOptions[i]).length() != 0)
                {
                    onlyColFamOptionSpecified = false;
                }
        }

        HColumnDescriptor colDesc = htblDesc.getFamily(colFam.getBytes());

        ChangeFlags status = new ChangeFlags();
        if (onlyColFamOptionSpecified) {
            if (colDesc == null) {
                colDesc = new HColumnDescriptor(colFam);
                
                htblDesc.addFamily(colDesc);
                
                status.setTableDescriptorChanged();
            } else
                return true; // col fam already exists
        }
        else {
            if (colDesc == null )
            {
               if( (String)tableOptions[HBASE_HDFS_STORAGE_POLICY] == null || 
                   (String)tableOptions[HBASE_HDFS_STORAGE_POLICY]=="") 
                return true; // colDesc must exist
            }
            else {

              int defaultVersionsValue = colDesc.getMaxVersions(); 

              status = 
                setDescriptors(tableOptions,htblDesc /*out*/,colDesc /*out*/, defaultVersionsValue);
           }
        }

            if (transID != 0 && (hbaseConnection != null)) {
                // Transactional alter support
                RMInterface table = new RMInterface(HBaseClient.getConnection());
                table.alter(tblName, tableOptions, transID);
                if (logger.isDebugEnabled()) logger.debug("HBaseClient.alter(" + 
                          tblName + ") called with object length: " + 
                          java.lang.reflect.Array.getLength(tableOptions));
            }
            else {
                // the modifyTable and modifyColumn operations are asynchronous,
                // so we have to have additional code to poll for their completion
                // (I hear that synchronous versions will be available in HBase 1.x)
                TableName tableName = TableName.valueOf(tblName);
                boolean tableIsEnabled = admin.isTableEnabled(tableName); 

                // Some alter requests (e.g. HBASE_REGION_REPLICATION) require the
                // hbase table disabled, do that now.
                if (status.tableDescriptorChanged()) {
                    if( (String)tableOptions[HBASE_REGION_REPLICATION] != null ) {
                        if (tableIsEnabled)
                            admin.disableTable(tableName);
                    }
                    admin.modifyTable(tableName,htblDesc);
                    waitForCompletion(tblName,admin);
                    if (tableIsEnabled)
                        admin.enableTable(tableName);
                }
                else if (status.columnDescriptorChanged()) {
                    admin.modifyColumn(tableName,colDesc);                  
                    waitForCompletion(tblName,admin);
                }
                admin.close();
            }
        return true;
    }

    public boolean truncate(String tblName, boolean preserveSplits, long transID)
             throws MasterNotRunningException, IOException {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.truncate(" + tblName + ") called.");
        Admin admin = getConnection().getAdmin();
        try {
           TableName tableName = TableName.valueOf(tblName);
           if (admin.isTableEnabled(tableName))
              admin.disableTable(tableName);
           admin.truncateTable(tableName, preserveSplits);
        } finally {
           admin.close();
        }
        if(transID != 0 ) //this truncate is done inside a transaction
        {
           RMInterface table = new RMInterface(HBaseClient.getConnection());
           long epoch = TrxEnvironmentEdgeManager.currentTime();
           //it should be safe to set back epoch by 10 mins
           epoch = epoch - 10*60*1000000;
           table.pushRegionEpoch(tblName,transID,epoch,config, getConnection());
           table.truncateTableOnAbort(tblName,transID);
        }
        return true;
    }

    public boolean drop(String tblName, long transID)
             throws MasterNotRunningException, IOException {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.drop(" + tblName + ") called.");
        Admin admin = getConnection().getAdmin();
        try {
           if(transID != 0 && (hbaseConnection != null)) {
              RMInterface table = new RMInterface(HBaseClient.getConnection());
              table.dropTable(tblName, transID);
           }
           else {
               TableName tableName = TableName.valueOf(tblName);
               if (admin.isTableEnabled(tableName))
                   admin.disableTable(tableName);
              admin.deleteTable(tableName);
           }
        } finally {
           admin.close();
        }
        return true;
    }

    public boolean dropAll(String pattern, long transID) 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.dropAll(" + pattern + ") called.");
            Admin admin = getConnection().getAdmin();
	    HTableDescriptor[] htdl = admin.listTables();
	    if (htdl == null)
		return true;

            IOException ioExc = null;  
	    for (HTableDescriptor htd : htdl) {
		String tblName = htd.getNameAsString();

                // Traf tables must start with "TRAF_" (namespace) or
                // "TRAFODION." (catalog name).
                int idx1 = tblName.indexOf("TRAF_");
                int idx2 = tblName.indexOf("TRAFODION.");
                if (! ((idx1 == 0) || (idx2 == 0)))
                    continue;

                // do not drop objects in namespace TRAF_RSRVD_5.
                // This is reserved for DTM objects.
                int idx = tblName.indexOf("TRAF_RSRVD_5:");
                if (idx >= 0)
                    continue;

                // do not drop DTM log files which have the format: 
                //        TRAFODION._DTM_.*
                idx = tblName.indexOf("TRAFODION._DTM_");
                if (idx >= 0)
                    continue;

                // do not drop objects in namespace TRAF_RSRVD_7.
                // This is reserved for manageability opentsdb stuff.
                // do not drop DTM log files which have the format: 
                //        TRAFODION._DTM_.*
                idx = tblName.indexOf("TRAF_RSRVD_7:");
                if (idx >= 0)
                    continue;

                try {
                    if(transID != 0 && (hbaseConnection != null)) {
                        RMInterface table = new RMInterface(HBaseClient.getConnection());
                        table.dropTable(tblName, transID);
                    }
                    else {
                        TableName tableName = TableName.valueOf(tblName);
                        if (! admin.isTableEnabled(tableName))
                            admin.enableTable(tableName);
                        
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                    }
                }
                
                catch (IOException e) {
                    if (ioExc == null) {
                        ioExc = new IOException("Not all tables are dropped, For details get suppressed exceptions");
                        ioExc.addSuppressed(e);
                     }
                     else 
                        ioExc.addSuppressed(e);
                    if (logger.isDebugEnabled()) logger.debug("HbaseClient.dropAll  error" + e);
                }
            }

            admin.close();
            if (ioExc != null)
                throw ioExc;
            return true;
    }

    public String[] listAll(String pattern) 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.listAll(" + pattern + ") called.");
            Admin admin = getConnection().getAdmin();

	    HTableDescriptor[] htdl = 
                (pattern.isEmpty() ? admin.listTables() : admin.listTables(pattern));
            if (htdl.length == 0) {
               admin.close(); 
               return null;
            }
            String[] hbaseTables = new String[htdl.length];
            int i=0;
	    for (HTableDescriptor htd : htdl) {
                hbaseTables[i++] = htd.getNameAsString();
	    }
            admin.close();
            
            return hbaseTables;
    }

    public byte[][]  getClusterStats() 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.getClusterStats called.");

            while (true) {
                switch (clusterStatsState) {
                case 1: // open
                    {
                        rsc = new TrafRegionStats(getConnection());
                        rsc.Open();

                        regionInfo = new byte[MAX_REGION_INFO_ROWS][];

                        currRegion = 0;

                        clusterStatsState = 2;
                    }
                    break;
                    
                case 3: // close
                    {
                        rsc.Close();
                        
                        clusterStatsState = 1;

                        return null;
                    }
                    
                case 2: // fetch
                    {
                        if (currRegion >= MAX_REGION_INFO_ROWS) {

                            regionStatsEntries = currRegion;

                            currRegion = 0;
                            return regionInfo;
                        }
                            
                        if (! rsc.GetNextRegion()) {
                            if (currRegion > 0) {
                                clusterStatsState = 3;

                                regionStatsEntries = currRegion;

                                currRegion = 0;
                                return Arrays.copyOf(regionInfo, regionStatsEntries);
                            }

                            clusterStatsState = 3;
                            break;
                        }
                     
                        SizeInfo regionSizeInfo  = rsc.getCurrRegionSizeInfo();

                        String serverName = regionSizeInfo.serverName;
                        String regionName = regionSizeInfo.regionName;
                        String tableName  = regionSizeInfo.tableName;
                        
                        int  numStores           = regionSizeInfo.numStores;
                        int  numStoreFiles       = regionSizeInfo.numStoreFiles;
                        long storeUncompSize     = regionSizeInfo.storeUncompSize;
                        long storeFileSize       = regionSizeInfo.storeFileSize;
                        long memStoreSize        = regionSizeInfo.memStoreSize;
                        long readRequestsCount   = regionSizeInfo.readRequestsCount;
                        long writeRequestsCount   = regionSizeInfo.writeRequestsCount;
                        
                        String oneRegion = "";
                        oneRegion += serverName + "|";
                        oneRegion += regionName + "|";
                        oneRegion += tableName  + "|";
                        oneRegion += String.valueOf(numStores) + "|";
                        oneRegion += String.valueOf(numStoreFiles) + "|";
                        oneRegion += String.valueOf(storeUncompSize) + "|";
                        oneRegion += String.valueOf(storeFileSize) + "|";
                        oneRegion += String.valueOf(memStoreSize) + "|";
                        oneRegion += String.valueOf(readRequestsCount) + "|";
                        oneRegion += String.valueOf(writeRequestsCount) + "|";
                        
                        regionInfo[currRegion++] = oneRegion.getBytes();

                    }

                } // switch
            }

    }

    public byte[][]  getRegionStats(String tableName) 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.getRegionStats(" + tableName + ") called.");

            Admin admin = getConnection().getAdmin();
            if (tableName == null) //tableName.isEmpty())
                return getClusterStats();

            HTable htbl = new HTable(config, tableName);
            HRegionInfo hregInfo = null;
            byte[][] regionInfo = null;
            try {
                TrafRegionStats rsc = new TrafRegionStats(htbl, admin);
                
                NavigableMap<HRegionInfo, ServerName> locations
                    = htbl.getRegionLocations();
                regionInfo = new byte[locations.size()][]; 
                regionStatsEntries = 0; 
 
                for (Map.Entry<HRegionInfo, ServerName> entry: 
                         locations.entrySet()) {
                
                    hregInfo = entry.getKey();                    
                    ServerName serverName = entry.getValue();
                    byte[] regionName = hregInfo.getRegionName();
                    String encodedRegionName = hregInfo.getEncodedName();
                    String ppRegionName = HRegionInfo.prettyPrint(encodedRegionName);
                    SizeInfo regionSizeInfo  = rsc.getRegionSizeInfo(regionName);
                    String serverNameStr     = "";
                    int  numStores           = 0;
                    int  numStoreFiles       = 0;
                    long storeUncompSize     = 0;
                    long storeFileSize       = 0;
                    long memStoreSize        = 0;
                    long readRequestsCount   = 0;
                    long writeRequestsCount  = 0;
                    String ppTableName = "";
                    if (regionSizeInfo != null) {
                       serverNameStr       = regionSizeInfo.serverName;
                       numStores           = regionSizeInfo.numStores;
                       numStoreFiles       = regionSizeInfo.numStoreFiles;
                       storeUncompSize     = regionSizeInfo.storeUncompSize;
                       storeFileSize       = regionSizeInfo.storeFileSize;
                       memStoreSize        = regionSizeInfo.memStoreSize;
                       readRequestsCount   = regionSizeInfo.readRequestsCount;
                       writeRequestsCount  = regionSizeInfo.writeRequestsCount;
                       ppTableName = regionSizeInfo.tableName;
                       ppRegionName = regionSizeInfo.regionName;
                    }
                    String oneRegion;
                    oneRegion = serverNameStr + "|";
                    oneRegion += ppTableName + "/" + ppRegionName + "|";
                    oneRegion += String.valueOf(numStores) + "|";
                    oneRegion += String.valueOf(numStoreFiles) + "|";
                    oneRegion += String.valueOf(storeUncompSize) + "|";
                    oneRegion += String.valueOf(storeFileSize) + "|";
                    oneRegion += String.valueOf(memStoreSize) + "|";
                    oneRegion += String.valueOf(readRequestsCount) + "|";
                    oneRegion += String.valueOf(writeRequestsCount) + "|";
                    
                    regionInfo[regionStatsEntries++] = oneRegion.getBytes();
                }
            }
            finally {
                admin.close();
            }

            return regionInfo;
    }

    public boolean copy(String srcTblName, String tgtTblName, boolean force)
	throws MasterNotRunningException, IOException, SnapshotCreationException, InterruptedException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.copy(" + srcTblName + tgtTblName + ") called.");
            Admin admin = getConnection().getAdmin();
	    
            // snapshot name cannot contain ":".
            // If the src name is prefixed by "namespace:", replace
            // ":" by "_"
            String snapshotName = srcTblName + "_SNAPSHOT";
            snapshotName = snapshotName.replace(":", "_");
            TableName tgtTableName = TableName.valueOf(tgtTblName);
            TableName srcTableName = TableName. valueOf(srcTblName);

            if ((force == true) &&
                (admin.tableExists(tgtTableName))) {
                admin.disableTable(tgtTableName);
                admin.deleteTable(tgtTableName);
            }
                
	    if (! admin.isTableDisabled(srcTableName))
	       admin.disableTable(srcTableName);
	    createSnapshot(srcTblName, snapshotName, true);
	    admin.cloneSnapshot(snapshotName, tgtTableName);
	    deleteSnapshot(snapshotName, false);
	    admin.enableTable(srcTableName);
	    admin.close();
	    return true;
    }

    public boolean exists(String tblName, long transID)  
           throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("HBaseClient.exists(" + tblName + ") called.");
            Admin admin = getConnection().getAdmin();
            boolean result = admin.tableExists(TableName.valueOf(tblName));
            admin.close();
            return result;
    }

    public HTableClient getHTableClient(long jniObject, String tblName, 
					boolean useTRex, boolean bSynchronized,
                                        boolean incrBackup) throws IOException 
    {
       if (logger.isDebugEnabled()) logger.debug("HBaseClient.getHTableClient(" + tblName
						 + (useTRex ? ", use TRX" : ", no TRX") 
						 + (bSynchronized ? ", use STR" : ", no STR") 
						 + ") called.");

       HTableClient htable = null;
       htable = new HTableClient(bigtableConnection != null);
       if (htable.init(tblName, useTRex, bSynchronized, incrBackup) == false) {
          if (logger.isDebugEnabled()) logger.debug("  ==> Error in init(), returning empty.");
            return null;
       }
       htable.setJniObject(jniObject);
       return htable;
    }

    public void releaseHTableClient(HTableClient htable) 
                    throws IOException {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.releaseHTableClient("
            + (htable == null ? " htable is null " : htable.getTableName()) + "). ");
        if (htable == null)
            return;
	                
        boolean cleanJniObject = false;
        if (bigtableConnection != null) {
           htable.close();
           return;
        }
        else
           htable.release(cleanJniObject);
    }

    public boolean grant(byte[] user, byte[] tblName,
                         Object[] actionCodes) throws IOException {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.grant(" + new String(user) + ", "
                     + new String(tblName) + ") called.");
		byte[] colFamily = null;

		Permission.Action[] assigned = new Permission.Action[actionCodes.length];
		for (int i = 0 ; i < actionCodes.length; i++) {
			String actionCode = (String)actionCodes[i];
			assigned[i] = Permission.Action.valueOf(actionCode);
		}

	    //HB98
	    TableName htblName = TableName.valueOf(new String(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME)
						   ,new String(tblName));
            UserPermission userPerm = new UserPermission(user, htblName,
                                                         colFamily, assigned);

            AccessController accessController = new AccessController();
	    //HB98 The grant() method is very different in HB98 (commenting out for now)
            //accessController.grant(userPerm);
        return true;
    }

   public boolean revoke(byte[] user, byte[] tblName,
                          Object[] actionCodes) 
                     throws IOException {
        if (logger.isDebugEnabled()) logger.debug("HBaseClient.revoke(" + new String(user) + ", "
                     + new String(tblName) + ") called.");
        byte[] colFamily = null;

        Permission.Action[] assigned = new Permission.Action[actionCodes.length];
        for (int i = 0 ; i < actionCodes.length; i++) {
            String actionCode = (String)actionCodes[i];
            assigned[i] = Permission.Action.valueOf(actionCode);
        }

	    //HB98
	    TableName htblName = TableName.valueOf(new String(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME)
						   ,new String(tblName));
            UserPermission userPerm = new UserPermission(user, htblName,
                                                         colFamily, assigned);

            AccessController accessController = new AccessController();
	    
	    //HB98 The revoke() method is very different in HB98 (commenting out for now)
            //accessController.revoke(userPerm);
        return true;
    }

    // Debugging method to display initial set of KeyValues and sequence
    // of column qualifiers.
    private void printQualifiers(HFile.Reader reader, int maxKeys) 
                 throws IOException {
      String qualifiers = new String();
      HFileScanner scanner = reader.getScanner(false, false, false);
      scanner.seekTo();
      int kvCount = 0;
      int nonPuts = 0;
      do {
        Cell kv = scanner.getKeyValue();
        //System.out.println(kv.toString());
        if (kv.getTypeByte() == KeyValue.Type.Put.getCode())
          qualifiers = qualifiers + kv.getQualifier()[0] + " ";
        else
          nonPuts++;
      } while (++kvCount < maxKeys && scanner.next());
      System.out.println("First " + kvCount + " column qualifiers: " + qualifiers);
      if (nonPuts > 0)
        System.out.println("Encountered " + nonPuts + " non-PUT KeyValue types.");
    }

    // Estimates the number of rows still in the MemStores of the regions
    // associated with the passed table name. The number of bytes in the
    // MemStores is divided by the passed row size in bytes, which is
    // derived by comparing the row count for an HFile (which in turn is
    // derived by the number of KeyValues in the file and the number of
    // columns in the table) to the size of the HFile.
    private long estimateMemStoreRows(String tblName, int rowSize)
                 throws MasterNotRunningException, IOException {
      if (logger.isDebugEnabled()) logger.debug("estimateMemStoreRows called for " + tblName + " with row size " + rowSize);

      if (rowSize == 0)
        return 0;

      Admin admin = getConnection().getAdmin();
      HTable htbl = new HTable(config, tblName);
      long totalMemStoreBytes = 0;
      try {
        // Get a set of all the regions for the table.
        Set<HRegionInfo> tableRegionInfos = htbl.getRegionLocations().keySet();
        Set tableRegions = new TreeSet(Bytes.BYTES_COMPARATOR);
        for (HRegionInfo regionInfo : tableRegionInfos) {
          tableRegions.add(regionInfo.getRegionName());
        }
     
        // Get collection of all servers in the cluster.
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        final long bytesPerMeg = 1024L * 1024L;
     
        // For each server, look at each region it contains and see if 
        // it is in the set of regions for the table. If so, add the
        // size of its the running total.
        for (ServerName serverName : servers) {
          ServerLoad serverLoad = clusterStatus.getLoad(serverName);
          for (RegionLoad regionLoad: serverLoad.getRegionsLoad().values()) {
            byte[] regionId = regionLoad.getName();
            if (tableRegions.contains(regionId)) {
              long regionMemStoreBytes = bytesPerMeg * regionLoad.getMemStoreSizeMB();
              if (logger.isDebugEnabled()) logger.debug("Region " + regionLoad.getNameAsString()
                           + " has MemStore size " + regionMemStoreBytes);
              totalMemStoreBytes += regionMemStoreBytes;
            }
          }
        }
      }
      catch (IOException e) {
        if (logger.isDebugEnabled()) logger.debug("IOException caught in estimateMemStoreRows: " + e);
      }
      catch (Throwable e) {
        if (logger.isDebugEnabled()) logger.debug("Throwable caught in estimateMemStoreRows: " + e);
      }
      finally {
        admin.close();
      }

      // Divide the total MemStore size by the size of a single row.
      if (logger.isDebugEnabled()) logger.debug("Estimating " + (totalMemStoreBytes / rowSize)
                   + " rows in MemStores of table's regions.");
      return totalMemStoreBytes / rowSize;
    }


    public float getBlockCacheFraction()
    {
        float defCacheFraction = 0.4f;
        return config.getFloat("hfile.block.cache.size",defCacheFraction);
    }

    // if we make the method below public later, should think about whether this is the
    // right class to host this method

    // compares two qualifiers as unsigned, lexicographically ordered byte strings
    static private boolean isQualifierLessThanOrEqual(Cell nextKv,
                                                      Cell currKv)
    {
       int currLength = currKv.getQualifierLength(); 
       int currOffset = currKv.getQualifierOffset();
       byte [] currQual = currKv.getQualifierArray();
       int nextLength = nextKv.getQualifierLength(); 
       int nextOffset = nextKv.getQualifierOffset();
       byte [] nextQual = nextKv.getQualifierArray();   

       // If we later decide we need a performance-critical version of this method,
       // we should just use a native method that calls C memcmp.

       int minLength = nextLength;
       if (currLength < nextLength)
         minLength = currLength;

       for (int i = 0; i < minLength; i++) {
         // ugh... have to do some gymnastics to make this an
         // unsigned comparison
         int nextQualI = nextQual[i+nextOffset];
         if (nextQualI < 0)
           nextQualI = nextQualI + 256;
         int currQualI = currQual[i+currOffset];
         if (currQualI < 0)
           currQualI = currQualI + 256;

         if (nextQualI < currQualI)
           return true;
         else if (nextQualI > currQualI)
           return false;
         // else equal, move on to next byte
       }

       // the first minLength bytes are the same; the shorter array
       // is regarded as less

       boolean rc = (nextLength <= currLength);      

       return rc;
    }


    // Estimates row count for tblName. Has a retry loop for the 
    // case of java.io.FileNotFoundException, which can happen if
    // compactions are in flight when we call estimateRowCountBody.
    // We try again with geometrically higher timeouts in hopes that
    // the compaction will go away. But after 4 minutes plus of retries
    // we'll give up and invite the user to try later.
    public boolean estimateRowCount(String tblName, int partialRowSize,
                                    int numCols, int retryLimitMilliSeconds, long[] rc)
                   throws MasterNotRunningException, IOException, ClassNotFoundException, URISyntaxException {
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.estimateRowCount(" + tblName + ") called."); 
      boolean retcode = false;  // assume failure
      int retryWait = 2000;     // initial sleep before retry interval is 2 seconds
      int cumulativeSleepTime = 0;
      while (retryWait > 0) {
        try {
          retcode = estimateRowCountBody(tblName,partialRowSize,numCols,rc);
          retryWait = 0;  // for normal loop exit
        }
        catch (FileNotFoundException fne) {

          if (cumulativeSleepTime < retryLimitMilliSeconds) {   // stop retrying if we've exceeded limit
            if (logger.isDebugEnabled()) logger.debug("FileNotFoundException encountered (" + fne.getMessage()
                                                      + ") retrying in " + Integer.toString(retryWait/1000) + " seconds." );
            try {
              Thread.sleep(retryWait);  // sleep for a while or until interrupted
              cumulativeSleepTime += retryWait;
            }
            catch (InterruptedException e) {
              // ignore the interruption and keep going
            }  
            retryWait = 2 * retryWait;
            if (retryWait > 30000)
              retryWait = 30000;  // max out the retry wait at 30 seconds
          }
          else {
            // we've retried enough; just re-throw
            if (logger.isDebugEnabled()) logger.debug("FileNotFoundException encountered (" + fne.getMessage()
                                                      + "); not retrying." );
            throw fne;
          }      
        }
      }
 
      return retcode;
    }
        
    

    // Estimates row count for tblName by iterating over the HFiles for
    // the table, extracting the KeyValue entry count from the file's
    // trailer block, summing the counts, and dividing by the number of
    // columns in the table. An adjustment is made for the estimated
    // number of missing values by sampling the first several
    // hundred KeyValues to see how many are missing.
    private boolean estimateRowCountBody(String tblName, int partialRowSize,
                                    int numCols, long[] rc)
                   throws MasterNotRunningException, IOException, ClassNotFoundException, URISyntaxException {
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.estimateRowCountBody(" + tblName + ") called.");

      final String REGION_NAME_PATTERN = "[0-9a-f]*";
      final String HFILE_NAME_PATTERN  = "[0-9a-f]*";

      // To estimate incidence of nulls, read the first 500 rows worth
      // of KeyValues. For aligned format (numCols == 1), the whole row 
      // is in one cell so we don't need to look for missing cells.
      final int ROWS_TO_SAMPLE = ((numCols > 1) ? 500 : 0);  // don't bother sampling for aligned format
      int putKVsSampled = 0;
      int nonPutKVsSampled = 0;
      int missingKVsCount = 0;
      int sampleRowCount = 0;
      long totalEntries = 0;   // KeyValues in all HFiles for table
      long totalSizeBytes = 0; // Size of all HFiles for table 
      boolean more = true;

      // Make sure the config doesn't specify HBase bucket cache. If it does,
      // then the CacheConfig constructor may fail with a Java OutOfMemory 
      // exception because our JVM isn't configured with large enough memory.

      String ioEngine = config.get(HConstants.BUCKET_CACHE_IOENGINE_KEY,null);
      if (ioEngine != null) {
          config.unset(HConstants.BUCKET_CACHE_IOENGINE_KEY); // delete the property
      }

      // Access the file system to go directly to the table's HFiles.
      // Create a reader for the file to access the entry count stored
      // in the trailer block, and a scanner to iterate over a few
      // hundred KeyValues to estimate the incidence of missing 
      // KeyValues. KeyValues may be missing because the column has
      // a null value, or because the column has a default value that
      // has not been materialized.
      long nano1, nano2;
      nano1 = System.nanoTime();
      FileSystem fileSystem = FileSystem.get(config);
      nano2 = System.nanoTime();
      if (logger.isDebugEnabled()) logger.debug("FileSystem.get() took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      CacheConfig cacheConf = new CacheConfig(config);
      String hbaseRootPath = config.get(HConstants.HBASE_DIR).trim();
      if (hbaseRootPath.charAt(0) != '/')
        hbaseRootPath = new URI(hbaseRootPath).getPath();
      if (logger.isDebugEnabled()) logger.debug("hbaseRootPath = " + hbaseRootPath);

      // extract namespace from tblName, if present
      int colonPos = tblName.indexOf(':');
      String nameSpace = null;
      String tblNameWithoutNameSpace = null;
      if (colonPos >= 0) {
          nameSpace = tblName.substring(0, colonPos);
          tblNameWithoutNameSpace = tblName.substring(colonPos+1);
      }

      String pathName = hbaseRootPath + "/data/" +
          (nameSpace == null ? "default" : nameSpace) + "/" +
          (nameSpace == null ? tblName : tblNameWithoutNameSpace) + 
          "/" + REGION_NAME_PATTERN + "/#1/" + HFILE_NAME_PATTERN;
      //System.out.println("pathName = " + pathName);
      FileStatus[] fsArr = fileSystem.globStatus(new Path (pathName));
      for (FileStatus fs : fsArr) {
        if (logger.isDebugEnabled()) logger.debug("Estimate row count is processing file " + fs.getPath());
        // Make sure the file name conforms to HFile name pattern.
        if (!StoreFileInfo.isHFile(fs.getPath())) {
          if (logger.isDebugEnabled()) logger.debug("Skipped file " + fs.getPath() + " -- not a valid HFile name.");
          continue;
        }
        HFile.Reader reader = HFile.createReader(fileSystem, fs.getPath(), cacheConf, config);
        try {
          totalEntries += reader.getEntries();
          totalSizeBytes += reader.length();
          //printQualifiers(reader, 100);
          if (ROWS_TO_SAMPLE > 0 &&
              totalEntries == reader.getEntries()) {  // first file only

            // Trafodion column qualifiers are ordinal numbers, but are represented
            // as varying length unsigned little-endian integers in lexicographical
            // order. So, for example, in a table with 260 columns, the column
            // qualifiers (if present) will be read in this order: 
            // 1 (x'01'), 257 (x'0101'), 2 (x'02'), 258 (x'0201'), 3 (x'03'),
            // 259 (x'0301'), 4 (x'04'), 260 (x'0401'), 5 (x'05'), 6 (x'06'), 
            // 7 (x'07'), ...
            // We have crossed the boundary to the next row if and only if the
            // next qualifier read is less than or equal to the previous, 
            // compared unsigned, lexicographically.

            HFileScanner scanner = reader.getScanner(false, false, false);
            scanner.seekTo();  //position at beginning of first data block

            // the next line should succeed, as we know the HFile is non-empty
            Cell currKv = scanner.getKeyValue();
            while ((more) && (currKv.getTypeByte() != KeyValue.Type.Put.getCode())) {
              nonPutKVsSampled++;
              more = scanner.next();
              currKv = scanner.getKeyValue();
            }
            if (more) {
              // now we have the first KeyValue in the HFile

              int putKVsThisRow = 1;
              putKVsSampled++;
              sampleRowCount++;  // we have at least one row
              more = scanner.next();
    
              while ((more) && (sampleRowCount <= ROWS_TO_SAMPLE)) {
                Cell nextKv = scanner.getKeyValue();
                if (nextKv.getTypeByte() == KeyValue.Type.Put.getCode()) {
                  if (isQualifierLessThanOrEqual(nextKv,currKv)) {
                    // we have crossed a row boundary
                    sampleRowCount++;
                    missingKVsCount += (numCols - putKVsThisRow);
                    putKVsThisRow = 1;
                  } else {
                    putKVsThisRow++;
                  }
                  currKv = nextKv;
                  putKVsSampled++;
                } else {
                  nonPutKVsSampled++;  // don't count these toward the number
                } 
              more = scanner.next();
              }
            }   
  
            if (sampleRowCount > ROWS_TO_SAMPLE) {
              // we read one KeyValue beyond the ROWS_TO_SAMPLE-eth row, so
              // adjust counts for that
              putKVsSampled--;
              sampleRowCount--;
            }

            if (logger.isDebugEnabled())
              logger.debug("Sampled " + missingKVsCount + " missing values.");
          }  // code for first file
        } finally {
          reader.close(false);
        }
      } // for

      long estimatedEntries = (ROWS_TO_SAMPLE > 0
                                 ? 0               // get from sample data, below
                                 : totalEntries);  // no sampling, use stored value
      if ((putKVsSampled > 0) && // avoid div by 0 if no Put KVs in sample
          (putKVsSampled >= ROWS_TO_SAMPLE/10)) { // avoid really small samples
        // Formerly, we would multiply this by a factor of 
        // putKVsSampled / (putKVsSampled + nonPutKVsSampled).
        // If non-put records are evenly distributed among the cells, then
        // that would give a better estimate. However, we find that often
        // (e.g. time-ordered data that is being aged out), the non-put cells
        // clump up in one place -- they might even take a whole HFile!
        // There is no real way to compensate other than reading the entire
        // table. So, we don't try to scale down the number of rows based 
        // on the proportion of non-Put cells. That means the value below
        // will sometimes over-estimate, but it is much better to over-
        // estimate than to under-estimate when it comes to row counts.
        estimatedEntries = ((putKVsSampled + missingKVsCount) * totalEntries)
                                   / putKVsSampled;
      } else { // few or no Puts found
        // The first file might have been full of deletes, which can happen
        // when time-ordered data ages out. We don't want to infer that the
        // table as a whole is all deletes (it almost certainly isn't in the
        // time-ordered data age-out case). We could just keep reading HFiles
        // until we find one with a decent sample of rows but that might take
        // awhile. Instead, we'll just punt and use totalEntries for our
        // estimate. This will over-estimate, but it is far better to do that
        // than to under-estimate.
        estimatedEntries = totalEntries;
      }

      // Calculate estimate of rows in all HFiles of table.
      rc[0] = (estimatedEntries + (numCols/2)) / numCols; // round instead of truncate

      // Estimate # of rows in MemStores of all regions of table. Pass
      // a value to divide the size of the MemStore by. Base this on the
      // ratio of bytes-to-rows in the HFiles, or the actual row size if
      // the HFiles were empty.
      int rowSize;
      if (rc[0] > 0)
        rowSize = (int)(totalSizeBytes / rc[0]);
      else {
        // From Traf metadata we have calculated and passed in part of the row
        // size, including size of column qualifiers (col names), which are not
        // known to HBase.  Add to this the length of the fixed part of the
        // KeyValue format, times the number of columns.
        int fixedSizePartOfKV = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE // key len + value len
                              + KeyValue.KEY_INFRASTRUCTURE_SIZE;     // rowkey & col family len, timestamp, key type
        rowSize = partialRowSize   // for all cols: row key + col qualifiers + values
                      + (fixedSizePartOfKV * numCols);

        // Trafodion tables have a single col family at present, so we only look
        // at the first family name, and multiply its length times the number of
        // columns. Even if more than one family is used in the future, presumably
        // they will all be the same short size.
        Table htbl = getConnection().getTable(TableName.valueOf(tblName));
        //HTable htbl = new HTable(config, tblName);
        HTableDescriptor htblDesc = htbl.getTableDescriptor();
        HColumnDescriptor[] families = htblDesc.getColumnFamilies();
        rowSize += (families[0].getName().length * numCols);
      }

      // Get the estimate of MemStore rows. Add to total after logging
      // of individual sums below.
      long memStoreRows = estimateMemStoreRows(tblName, rowSize);

      if (logger.isDebugEnabled()) logger.debug(tblName + " contains a total of " + totalEntries + " KeyValues in all HFiles.");
      if (putKVsSampled + missingKVsCount > 0)
        if (logger.isDebugEnabled()) logger.debug("Sampling indicates a null incidence of " + 
                     (missingKVsCount * 100)/(putKVsSampled + missingKVsCount) +
                     " percent.");
      if (logger.isDebugEnabled()) logger.debug("Estimated number of actual values (including nulls) is " + estimatedEntries);
      if (logger.isDebugEnabled()) logger.debug("Estimated row count in HFiles = " + estimatedEntries +
                   " / " + numCols + " (# columns) = " + rc[0]);
      if (logger.isDebugEnabled()) logger.debug("Estimated row count from MemStores = " + memStoreRows);

      rc[0] += memStoreRows;  // Add memstore estimate to total
      if (logger.isDebugEnabled()) logger.debug("Total estimated row count for " + tblName + " = " + rc[0]);
      return true;
    }

    // Similar to estimateRowCount, except that the implementation
    // uses a coprocessor. This is necessary when HBase encryption is
    // in use, because the Trafodion ID does not have the proper 
    // authorization to the KeyStore file used by HBase.
    public boolean estimateRowCountViaCoprocessor(String tblName, int partialRowSize,
                                    int numCols, int retryLimitMilliSeconds, long[] rc)
                   throws ServiceException, IOException {
      if (logger.isDebugEnabled()) {
        logger.debug("HBaseClient.estimateRowCountViaCoprocessor(" + tblName + ") called.");
        logger.debug("numCols = " + numCols + ", partialRowSize = " + partialRowSize);
      }

      boolean retcode = true; 
      rc[0] = 0;

      Table table = getConnection().getTable(TableName.valueOf(tblName));

      int putKVsSampled = 0;
      int nonPutKVsSampled = 0;
      int missingKVsCount = 0;
      long totalEntries = 0;   // KeyValues in all HFiles for table
      long totalSizeBytes = 0; // Size of all HFiles for table 

      final int finalNumCols = numCols;

      Batch.Call<TrxRegionService, TrafEstimateRowCountResponse> callable = 
        new Batch.Call<TrxRegionService, TrafEstimateRowCountResponse>() {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<TrafEstimateRowCountResponse> rpcCallback = 
            new BlockingRpcCallback<TrafEstimateRowCountResponse>();         

          @Override
          public TrafEstimateRowCountResponse call(TrxRegionService instance) throws IOException {    
            if (logger.isDebugEnabled()) logger.debug("call method for TrxRegionService was called");
            
            // one of these God-awful long type identifiers common in Java/Maven environments...
            org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrafEstimateRowCountRequest.Builder
              builder = TrafEstimateRowCountRequest.newBuilder();        
            builder.setNumCols(finalNumCols);
        
            instance.trafEstimateRowCount(controller, builder.build(), rpcCallback);
            TrafEstimateRowCountResponse response = rpcCallback.get();
            if (logger.isDebugEnabled()) {
              if (response == null)
                logger.debug("response was null");
              else
                logger.debug("response was non-null");
              if (controller.failed())
                logger.debug("controller.failed() is true");
              else
                logger.debug("controller.failed() is false");
              if (controller.errorText() != null)
                logger.debug("controller.errorText() is " + controller.errorText());
              else
                logger.debug("controller.errorText() is null");
              IOException ioe = controller.getFailedOn();
              if (ioe != null)
                logger.debug("controller.getFailedOn() returned " + ioe.getMessage());
              else
                logger.debug("controller.getFailedOn() returned null");
            }
            return response;        
          }
      };
    
      Map<byte[], TrafEstimateRowCountResponse> result = null;
      try {
        result = table.coprocessorService(TrxRegionService.class, null, null, callable);
      } catch (Throwable e) {
        throw new IOException("Exception from coprocessorService caught in estimateRowCountViaCoprocessor",e);
      }      

      for (TrafEstimateRowCountResponse response : result.values()) {
        boolean hasException = response.getHasException();
        String exception = response.getException();
        if (hasException) {
          if (logger.isDebugEnabled()) logger.debug("HBaseClient.estimateRowCountViaCoprocessor exception " + exception);
          throw new IOException(exception);
        }
        totalEntries = totalEntries + response.getTotalEntries();
        totalSizeBytes = totalSizeBytes + response.getTotalSizeBytes();
        putKVsSampled = putKVsSampled + response.getPutKVsSampled();
        nonPutKVsSampled = nonPutKVsSampled + response.getNonPutKVsSampled();
        missingKVsCount = missingKVsCount + response.getMissingKVsCount();
      }

      if (logger.isDebugEnabled()) { 
        logger.debug("The coprocessor service for estimating row count returned " + result.size() + " messages.");
        logger.debug("totalEntries = " + totalEntries + ", totalSizeBytes = " + totalSizeBytes);
        logger.debug("putKVsSampled = " + putKVsSampled + ", nonPutKVsSampled = " + nonPutKVsSampled +
                     ", missingKVsCount = " + missingKVsCount);
      }

      final int ROWS_TO_SAMPLE = 500;
      long estimatedEntries = ((ROWS_TO_SAMPLE > 0) && (numCols > 1)
                                 ? 0               // get from sample data, below
                                 : totalEntries);  // no sampling, use stored value

      if ((putKVsSampled > 0) && // avoid div by 0 if no Put KVs in sample
          (putKVsSampled >= ROWS_TO_SAMPLE/10)) { // avoid really small samples
        // Formerly, we would multiply this by a factor of 
        // putKVsSampled / (putKVsSampled + nonPutKVsSampled).
        // If non-put records are evenly distributed among the cells, then
        // that would give a better estimate. However, we find that often
        // (e.g. time-ordered data that is being aged out), the non-put cells
        // clump up in one place -- they might even take a whole HFile!
        // There is no real way to compensate other than reading the entire
        // table. So, we don't try to scale down the number of rows based 
        // on the proportion of non-Put cells. That means the value below
        // will sometimes over-estimate, but it is much better to over-
        // estimate than to under-estimate when it comes to row counts.
        estimatedEntries = ((putKVsSampled + missingKVsCount) * totalEntries)
                                   / putKVsSampled;
      } else { // few or no Puts found
        // The first file might have been full of deletes, which can happen
        // when time-ordered data ages out. We don't want to infer that the
        // table as a whole is all deletes (it almost certainly isn't in the
        // time-ordered data age-out case). We could just keep reading HFiles
        // until we find one with a decent sample of rows but that might take
        // awhile. Instead, we'll just punt and use totalEntries for our
        // estimate. This will over-estimate, but it is far better to do that
        // than to under-estimate.
        estimatedEntries = totalEntries;
      }

      if (logger.isDebugEnabled()) { 
        logger.debug("estimatedEntries = " + estimatedEntries + ", numCols = " + numCols);
      } 

      // Calculate estimate of rows in all HFiles of table.
      rc[0] = (estimatedEntries + (numCols/2)) / numCols; // round instead of truncate

      if (logger.isDebugEnabled()) { 
        logger.debug("rc[0] = " + rc[0]);
      }       

      // Estimate # of rows in MemStores of all regions of table. Pass
      // a value to divide the size of the MemStore by. Base this on the
      // ratio of bytes-to-rows in the HFiles, or the actual row size if
      // the HFiles were empty.
      int rowSize;

      if (rc[0] > 0)
        rowSize = (int)(totalSizeBytes / rc[0]);
      else {
        // From Traf metadata we have calculated and passed in part of the row
        // size, including size of column qualifiers (col names), which are not
        // known to HBase.  Add to this the length of the fixed part of the
        // KeyValue format, times the number of columns.
        int fixedSizePartOfKV = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE // key len + value len
                              + KeyValue.KEY_INFRASTRUCTURE_SIZE;     // rowkey & col family len, timestamp, key type
        rowSize = partialRowSize   // for all cols: row key + col qualifiers + values
                      + (fixedSizePartOfKV * numCols);


        // Trafodion tables have a single col family at present, so we only look
        // at the first family name, and multiply its length times the number of
        // columns. Even if more than one family is used in the future, presumably
        // they will all be the same short size.
        HTableDescriptor htblDesc = table.getTableDescriptor();
        HColumnDescriptor[] families = htblDesc.getColumnFamilies();
        rowSize += (families[0].getName().length * numCols);
      }

      // Get the estimate of MemStore rows
      long memStoreRows = estimateMemStoreRows(tblName, rowSize);

      if (logger.isDebugEnabled()) {
        logger.debug("Estimated row count from HFiles = " + rc[0]);
        logger.debug("Estimated row count from MemStores = " + memStoreRows);
      }

      rc[0] += memStoreRows;  // Add memstore estimate to total
      if (logger.isDebugEnabled()) logger.debug("Total estimated row count for " + tblName + " = " + rc[0]);

      return retcode;
    }


    /**
    This method returns node names where Hbase Table regions reside
    **/
    public boolean getRegionsNodeName(String tblName, String[] nodeNames)
                   throws IOException
    {
      if (logger.isDebugEnabled()) 
        logger.debug("HBaseClient.getRegionsNodeName(" + tblName + ") called.");

      HRegionInfo regInfo = null;


      HTable htbl = new HTable(config, tblName);
      if (logger.isDebugEnabled())
         logger.debug("after HTable call in getRegionsNodeName");

        NavigableMap<HRegionInfo, ServerName> locations = htbl.getRegionLocations();
        if (logger.isDebugEnabled())
           logger.debug("after htable.getRegionLocations call in getRegionsNodeName");

      
        String hostName;
        int regCount = 0;

        for (Map.Entry<HRegionInfo, ServerName> entry: locations.entrySet()) {
          if (logger.isDebugEnabled()) logger.debug("Entered for loop in getRegionsNodeName");
          regInfo = entry.getKey();
          hostName = entry.getValue().getHostname();
          nodeNames[regCount] = hostName;
          if (logger.isDebugEnabled()) logger.debug("Hostname for region " + regCount + " is " + hostName);
          regCount++;
        }

      return true;
    }



    /**
    This method returns index levels and block size of Hbase Table.
    Index level is read from  Hfiles trailer block. Randomly selects one region and iterates through all Hfiles
    in the chosen region and gets the maximum index level.
    Block size is read from HColumnDescriptor.
    **/
    public boolean getHbaseTableInfo(String tblName, int[] tblInfo)
                   throws MasterNotRunningException, IOException, ClassNotFoundException, URISyntaxException {

      if (logger.isDebugEnabled()) logger.debug("HBaseClient.getHbaseTableInfo(" + tblName + ") called.");
      final String REGION_NAME_PATTERN = "[0-9a-f]*";
      final String HFILE_NAME_PATTERN  = "[0-9a-f]*";

      // initialize 
      int indexLevel = 0;
      int currIndLevel = 0;
      int blockSize = 0;
      tblInfo[0] = indexLevel;
      tblInfo[1] = blockSize;
      Table htbl = null;
      HTableDescriptor htblDesc = null;

      // get block size
      try
      {
         htbl = getConnection().getTable(TableName.valueOf(tblName));
         htblDesc = htbl.getTableDescriptor();
      }
      catch (TableNotFoundException te) 
      {
         return false;
      }
      HColumnDescriptor[] families = htblDesc.getColumnFamilies();
      blockSize = families[0].getBlocksize();
      tblInfo[1] = blockSize;

      // Access the file system to go directly to the table's HFiles.
      long nano1 = 0, nano2 = 0;
      if (logger.isDebugEnabled())
        nano1 = System.nanoTime();
      FileSystem fileSystem = FileSystem.get(config);

      if (logger.isDebugEnabled()) {
        nano2 = System.nanoTime();
        logger.debug("FileSystem.get() took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      }

      // Make sure the config doesn't specify HBase bucket cache. If it does,
      // then the CacheConfig constructor may fail with a Java OutOfMemory 
      // exception because our JVM isn't configured with large enough memory.
      String ioEngine = config.get(HConstants.BUCKET_CACHE_IOENGINE_KEY,null);
      if (ioEngine != null) {
          config.unset(HConstants.BUCKET_CACHE_IOENGINE_KEY); // delete the property
      }
      CacheConfig cacheConf = new CacheConfig(config);
      String hbaseRootPath = config.get(HConstants.HBASE_DIR).trim();
      if (hbaseRootPath.charAt(0) != '/')
        hbaseRootPath = new URI(hbaseRootPath).getPath();
      if (logger.isDebugEnabled()) logger.debug("hbaseRootPath = " + hbaseRootPath);

      // extract namespace from tblName, if present
      int colonPos = tblName.indexOf(':');
      String nameSpace = null;
      String tblNameWithoutNameSpace = null;
      if (colonPos >= 0) {
          nameSpace = tblName.substring(0, colonPos);
          tblNameWithoutNameSpace = tblName.substring(colonPos+1);
      }
      else
          tblNameWithoutNameSpace = tblName;

      String regDir = hbaseRootPath + "/data/" +
          (nameSpace == null ? "default" : nameSpace) + "/" +
          (nameSpace == null ? tblName : tblNameWithoutNameSpace) + 
          "/" + REGION_NAME_PATTERN + "/#1/";

      if (logger.isDebugEnabled()) logger.debug("region dir = " + regDir);

      //get random region from the list of regions and look at all Hfiles in that region
      FileStatus[] regArr;
      try {
        regArr = fileSystem.globStatus(new Path(regDir));
      } catch (IOException ioe) {
        if (logger.isDebugEnabled()) logger.debug("fs.globStatus on region throws IOException");
        return false; // return index level = 0; and  block size
      }
      
      // logging
      if (logger.isDebugEnabled()) {
        for (int i =0; i < regArr.length; i++) 
          logger.debug("Region Path is " + regArr[i].getPath());
      }

      if (regArr.length == 0)
         return true;
      // get random region from the region array
      int regInd = 0;
      regInd = Math.abs(tblNameWithoutNameSpace.hashCode()) % regArr.length;
      Path regName = regArr[regInd].getPath();
      // extract MD5 hash name of random region from its path including colFam name. 
      // we just need part2 and looks something like /c8fe2d575de62d5d5ffc530bda497bca/#1
      String strRegPath = regName.toString();
      String parts[] = strRegPath.split(tblNameWithoutNameSpace);
      String part2 = parts[1];
      // now remove regular expression from the region path.
      // would look something like /hbase/data/default/<cat.sch.tab>/[0-9a-f]*/#1
      int j = regDir.indexOf("/[");
      String regPrefix = regDir.substring(0,j);
      if (logger.isDebugEnabled()) logger.debug("Region Path prefix = " + regPrefix);
      String hfilePath = regPrefix + part2 + "/" + HFILE_NAME_PATTERN;
      if (logger.isDebugEnabled()) logger.debug("Random = " + regInd + ", region is " + regName);
      if (logger.isDebugEnabled()) logger.debug("Hfile path = " + hfilePath);

      FileStatus[] fsArr;
      try {
        fsArr = fileSystem.globStatus(new Path(hfilePath));
      } catch (IOException ioe) {
        if (logger.isDebugEnabled()) logger.debug("fs.globStatus on Hfile throws IOException");
        return false; // return index level = 0; and  block size
      }

      if (logger.isDebugEnabled()) {
        for (int i =0; i < fsArr.length; i++)
          logger.debug("Hfile Path is " + fsArr[i].getPath());
      }
     
      // no Hfiles return from here
      if (fsArr.length == 0)
        return true; // return index level = 0; and  block size

      // get maximum index level going through all Hfiles of randomly chosen region
      if (logger.isDebugEnabled())
        nano1 = System.nanoTime();
      for (FileStatus fs : fsArr) {
        // Make sure the file name conforms to HFile name pattern.
        if (!StoreFileInfo.isHFile(fs.getPath())) {
          if (logger.isDebugEnabled()) logger.debug("Skipped file " + fs.getPath() + " -- not a valid HFile name.");
          continue;
        }

        // Create a reader for the file to access the index levels stored
        // in the trailer block
        HFile.Reader reader = HFile.createReader(fileSystem, fs.getPath(), cacheConf, config);
        try {
          FixedFileTrailer trailer = reader.getTrailer();
          currIndLevel = trailer.getNumDataIndexLevels();
          // index levels also include data block, should be excluded.
          if (currIndLevel > 0)
            currIndLevel = currIndLevel - 1;
          if (logger.isDebugEnabled()) 
            logger.debug("currIndLevel = " + currIndLevel+ ", indexLevel = " + indexLevel);
          if (currIndLevel > indexLevel)
            indexLevel = currIndLevel;
       } finally {
         reader.close(false);
       }
      } // for

      if (logger.isDebugEnabled()) {
        nano2 = System.nanoTime();
        logger.debug("get index level took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      }

      tblInfo[0] = indexLevel;
      if (logger.isDebugEnabled()) {
        logger.debug("Index Levels for " + tblName + " = " + tblInfo[0]);
        logger.debug("Block Size for " + tblName + " = " + tblInfo[1]);
      }
      
      return true;
    }

    void printCell(KeyValue kv) {
        String rowID = new String(kv.getRow());
        String colFamily = new String(kv.getFamily());
        String colName = new String(kv.getQualifier());
        String colValue = new String(kv.getValue());
        String row = rowID + ", " + colFamily + ", " + colName + ", "
            + colValue + ", " + kv.getTimestamp();
        System.out.println(row);
    }

    
  public  HBulkLoadClient getHBulkLoadClient() throws IOException 
  {
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.getHBulkLoadClient() called.");
    HBulkLoadClient hblc = null;

    hblc = new HBulkLoadClient( config);
    
    return hblc;
    
  }
  public void releaseHBulkLoadClient(HBulkLoadClient hblc) 
      throws IOException 
  {
     if (hblc == null)
       return;
          
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.releaseHBulkLoadClient().");
      hblc.release();
   }
  
  public  org.apache.hadoop.hbase.pit.BackupRestoreClient getBackupRestoreClient() throws IOException 
  {
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.getBackupRestoreClient() called.");
    org.apache.hadoop.hbase.pit.BackupRestoreClient brc = null;
    brc = new org.apache.hadoop.hbase.pit.BackupRestoreClient( config);
    return brc;
    
  }
  public void releaseBackupRestoreClient(org.apache.hadoop.hbase.pit.BackupRestoreClient brc) 
      throws IOException 
  {
     if (brc == null)
       return;
          
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.releaseBackupRestoreClient().");
      brc.release();
   }
  
  //returns the latest snapshot name for a table. returns null if table has no snapshots
  //associated with it
  public static String getLatestSnapshot(String tabName) throws IOException
  {
    Admin admin = getConnection().getAdmin();
    List<SnapshotDescription> snapDescs = admin.listSnapshots();
    long maxTimeStamp = 0;
    String latestsnpName = null;
    for (SnapshotDescription snp :snapDescs )
    {
      if (snp.getTable().compareTo(tabName) == 0 && 
          snp.getCreationTime() > maxTimeStamp)
      {
        latestsnpName= snp.getName();
        maxTimeStamp = snp.getCreationTime();
      }
      
    }
    admin.close();
    return latestsnpName;
  }
  public boolean cleanSnpScanTmpLocation(String pathStr) throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("HbaseClient.cleanSnpScanTmpLocation() - start - Path: " + pathStr);

      Path delPath = new Path(pathStr );
      delPath = delPath.makeQualified(delPath.toUri(), null);
      FileSystem fs = FileSystem.get(delPath.toUri(),config);
      fs.delete(delPath, true);
    
    return true;
  }
  private boolean updatePermissionForEntries(FileStatus[] entries, String hbaseUser, FileSystem fs) throws IOException 
  {
    if (entries == null) {
      return true;
    }
    
    for (FileStatus child : entries) {
      Path path = child.getPath();
      List<AclEntry> lacl = AclEntry.parseAclSpec("user:" + hbaseUser + ":rwx", true) ;
      try 
      {
        fs.modifyAclEntries(path, lacl);
      }
      catch (IOException e)
      {
        //if failure just log exception and continue
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.updatePermissionForEntries() exception. " + e);
      }
      if (child.isDir()) 
      {
        FileStatus[] files = FSUtils.listStatus(fs,path);
        updatePermissionForEntries(files,hbaseUser, fs);
      } 
    }
    return true;
  }
  
  public boolean setArchivePermissions( String tabName) throws IOException,ServiceException
  {
    if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.setArchivePermissions() called. ");
    Path rootDir = FSUtils.getRootDir(config);
    FileSystem myfs = FileSystem.get(rootDir.toUri(),config);
    FileStatus fstatus = myfs.getFileStatus(rootDir);
    String hbaseUser = fstatus.getOwner(); 
    assert (hbaseUser != null && hbaseUser.length() != 0);
    Path tabArcPath = HFileArchiveUtil.getTableArchivePath(config,  TableName.valueOf(tabName));
    if (tabArcPath == null)
      return true;
    List<AclEntry> lacl = AclEntry.parseAclSpec("user:" + hbaseUser + ":rwx", true) ;
    try
    {
      myfs.modifyAclEntries(tabArcPath, lacl);
    }
    catch (IOException e)
    {
      //if failure just log exception and continue
      if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.setArchivePermissions() exception. " + e);
    }
    FileStatus[] files = FSUtils.listStatus(myfs,tabArcPath);
    updatePermissionForEntries(files,  hbaseUser, myfs); 
    return true;
  }

    public int startGet(long jniObject, String tblName, 
                        boolean useTRex, boolean bSynchronize, int lockMode, boolean skipReadConflict,
                        long transID, long savepointID, long pSavepointId, int isolationLevel, byte[] rowID,
                        Object[] columns, long timestamp,
                        String hbaseAuths, int replicaId, boolean waitOnSelectForUpdate, boolean firstReadBypassTm, String queryContext)
                        throws IOException {
      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, false);
      return htc.startGet(transID, savepointID, pSavepointId, isolationLevel, rowID, columns, timestamp, lockMode, skipReadConflict,
                          hbaseAuths, replicaId, waitOnSelectForUpdate, firstReadBypassTm, queryContext);
  }

  public int startGet(long jniObject, String tblName,
                      boolean useTRex, boolean bSynchronize, int lockMode, boolean skipReadConflict, boolean skipTransaction,
                      long transID, long savepointID, long pSavepointId, int isolationLevel, Object[] rowIDs,
                      Object[] columns, long timestamp,
                      String hbaseAuths, int replicaId, String queryContext)
                        throws IOException {
      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, false);
      return htc.startGet(transID, savepointID, pSavepointId, isolationLevel, rowIDs, columns, timestamp, lockMode, skipReadConflict, skipTransaction,
                          hbaseAuths, replicaId, queryContext);
  }

  public int startGet(long jniObject, String tblName, 
                      boolean useTRex, boolean bSynchronize, int lockMode, boolean skipReadConflict, boolean skipTransaction,
                      long transID, long savepointID, long pSavepointId, int isolationLevel, short rowIDLen, Object rowIDs,
                      Object[] columns,
                      String hbaseAuths , int replicaId, String queryContext)
                        throws IOException {
      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, false);
      return htc.getRows(transID, savepointID, pSavepointId, isolationLevel, rowIDLen, rowIDs, columns, lockMode,  skipReadConflict, skipTransaction, hbaseAuths, replicaId, queryContext);
  }

  private String getOutOfMemoryErrorMsg()
  {
      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
      MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
      long maxMemory = heapUsage.getMax() / megaByte;
      long usedMemory = heapUsage.getUsed() / megaByte;
      String msg = "JVM OutOfMemory: Current memory usage :"+usedMemory+"M/"+maxMemory+"M";
      return msg;
  }

  public boolean insertRow(long jniObject, String tblName,
                           long transID, long savepointID, long pSavepointId, byte[] rowID,
                           Object row,
                           long timestamp,
                           boolean checkAndPut,
			   short colIndexToCheck,
                           int flags, int nodeId, String queryContext)
      throws IOException, InterruptedException, ExecutionException {

      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      //System.out.println("tblName = " + tblName + ", transID = " + transID + ", bSynchronize = " + bSynchronize);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = false;
      try{
        ret = htc.putRow(transID, savepointID, pSavepointId, rowID, row, null, null, timestamp, colIndexToCheck,
                         checkAndPut, 
                         flags, nodeId, queryContext);
      }
      catch (java.lang.OutOfMemoryError e) {
        throw new IOException(getOutOfMemoryErrorMsg());
      }
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean checkAndUpdateRow(long jniObject, 
				   String tblName, 
				   long transID, 
                                   long savepointID,
                                   long pSavepointId,
				   byte[] rowID,
				   Object columnsToUpdate,
				   byte[] columnToCheck,
				   byte[] columnValToCheck,
				   long timestamp,
                                   int flags, int nodeId, String queryContext)
      throws IOException, InterruptedException, ExecutionException {
      boolean checkAndPut = true;
      short colIndexToCheck = 0; // is overridden by columnToCheck

      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = htc.putRow(transID, savepointID, pSavepointId,
                               rowID, columnsToUpdate, 
                               columnToCheck, columnValToCheck,
                               timestamp, colIndexToCheck, checkAndPut,
                               flags, nodeId, queryContext);
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean insertRows(long jniObject, String tblName,
                            long transID, long savepointID, long pSavepointId,
                            short rowIDLen,
                            Object rowIDs,
                            Object rows,
                            long timestamp,
                            int flags, String queryContext) throws IOException, InterruptedException, ExecutionException {

      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = htc.putRows(transID, savepointID, pSavepointId, rowIDLen, rowIDs, rows, 
                                timestamp, flags, queryContext);
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean lockRequired(String tableName, long transactionId, long savepointId, long pSavepointId, int lockMode, boolean registerRegion, String queryContext) throws IOException {
    HTableClient htc = new HTableClient(false);
    htc.init(tableName, false, true, false);
    boolean ret = htc.lockRequired(transactionId, savepointId, pSavepointId, tableName, lockMode, registerRegion, queryContext);
    releaseHTableClient(htc);
    return ret;
  }
  
  public boolean updateVisibility(long jniObject, String tblName, boolean useTRex, 
                            long transID, byte[] rowID,
                            Object row, String queryContext) throws IOException, InterruptedException, ExecutionException {

      HTableClient htc = getHTableClient(jniObject, tblName, useTRex, true, false);
      boolean ret = htc.updateVisibility(transID, rowID, row, queryContext);
      releaseHTableClient(htc);
      return ret;
  }

  public boolean deleteRow(long jniObject, 
			   String tblName,
			   long transID, 
                           long savepointID,
                           long pSavepointId,
			   byte[] rowID,
			   Object[] columns,
			   long timestamp,
                           String hbaseAuths,
                           int flags, String queryContext) throws IOException {
      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = htc.deleteRow(transID, savepointID, pSavepointId, rowID, columns, timestamp, 
                                  hbaseAuths, flags, queryContext);
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean deleteRows(long jniObject, 
			    String tblName,
			    long transID, 
                            long savepointID,
                            long pSavepointId,
			    short rowIDLen, 
			    Object rowIDs,
                            Object[] columns,
			    long timestamp, 
                            String hbaseAuths,
                            int flags, String queryContext)
      throws IOException, InterruptedException, ExecutionException {

      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = htc.deleteRowsInt(transID, savepointID, pSavepointId, rowIDLen, rowIDs,
                                      columns, timestamp, 
                                      hbaseAuths, flags, queryContext);
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean checkAndDeleteRow(long jniObject,
				   String tblName,
				   long transID, 
                                   long savepointID,
                                   long pSavepointId,
				   byte[] rowID,
                                   Object[] columns,
				   byte[] columnToCheck,
				   byte[] colValToCheck,
				   long timestamp,
                                   String hbaseAuths,
                                   int flags, String queryContext) throws IOException {

      boolean useTRex = TrafExtStorageUtils.useTRex(flags);
      boolean bSynchronize = TrafExtStorageUtils.syncRepl(flags);
      boolean asyncOperation = TrafExtStorageUtils.asyncOper(flags);
      boolean incrBackup = TrafExtStorageUtils.incrementalBackup(flags);

      HTableClient htc = getHTableClient(jniObject, tblName, 
                                         useTRex, bSynchronize, incrBackup);
      boolean ret = htc.checkAndDeleteRow(transID, savepointID, pSavepointId, rowID, columns,
                                          columnToCheck, colValToCheck, 
                                          timestamp, hbaseAuths, flags, queryContext);
      if (asyncOperation == true)
         htc.setJavaObject(jniObject);
      else
         releaseHTableClient(htc);
      return ret;
  }

  public boolean  createCounterTable(String tabName,  String famName) throws IOException, MasterNotRunningException
  {
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.createCounterTable() - start");
    Admin admin = getConnection().getAdmin();
    TableName tableName =  TableName.valueOf (tabName);
    if (admin.tableExists(tableName)) {
        admin.close();
        return true;
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDesc = new HColumnDescriptor(famName);
    // A counter table is non-DTM-transactional.
    // Use the default maximum versions for MVCC.
    colDesc.setMaxVersions(DtmConst.MVCC_MAX_VERSION);
    desc.addFamily(colDesc);
    admin.createTable(desc);
    admin.close();
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.createCounterTable() - end");
    return true;
  }

  public long incrCounter(String tabName, String rowId, String famName, String qualName, long incrVal) throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.incrCounter() - start");

    HTable myHTable = new HTable(config, tabName);
    long count = myHTable.incrementColumnValue(Bytes.toBytes(rowId), Bytes.toBytes(famName), Bytes.toBytes(qualName), incrVal);
    myHTable.close();
    return count;
  }
 
  public byte[][] getStartKeys(String tblName, boolean useTRex) throws IOException 
  {
    byte[][] startKeys;
    HTableClient htc = getHTableClient(0, tblName, useTRex, false, false);
    startKeys = htc.getStartKeys();
    releaseHTableClient(htc);
    return startKeys;
  }

  public byte[][] getEndKeys(String tblName, boolean useTRex) throws IOException 
  {
    byte[][] endKeys;
    HTableClient htc = getHTableClient(0, tblName, useTRex, false, false);
    endKeys = htc.getEndKeys();
    if (logger.isDebugEnabled()) logger.debug("getEndKeys for " + tblName + " returned " + endKeys.length + " bytes " + endKeys);
    releaseHTableClient(htc);
    return endKeys;
  }

  //add directive path to a hdfs cache pool, a table may composed of multiple directives.
  public void addDirectiveToHDFSCache(DistributedFileSystem dfs, String directivePath, String poolName) throws IOException
  {
     CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder().
                                        setPath(new Path(directivePath)).
                                        setReplication((short)1).
                                        setPool(poolName).build();     
     dfs.addCacheDirective(directive);
     if (logger.isDebugEnabled()) logger.debug("[Hdfs Cache]Add " + directivePath + 
                                               "to pool " + poolName);
  }
  
  //remove a directive from a hdfs cache pool, return number of directives removed.
  public int removeDirectiveFromHDFSCache(DistributedFileSystem dfs, String directivePath, String poolName) throws IOException
  {
     int directiveCounter = 0;
     CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder().
                                        setPath(new Path(directivePath)).
                                        setPool(poolName).build();

     RemoteIterator<CacheDirectiveEntry> iter = 
          dfs.listCacheDirectives(directive);

    //get id of the directive and remove by id.
    //a directive can be cached multiple times,
    //so there could be multiple entries for a directive,
    //they have different ids.
     while(iter.hasNext())
     {
       long id = iter.next().getInfo().getId();
       dfs.removeCacheDirective(id);
       directiveCounter++;
       if (logger.isDebugEnabled()) logger.debug("[Hdfs Cache]Remove " + directivePath + 
                                                 "from pool " + poolName);
     }
     return directiveCounter;
  }

  //get all cached paths from a pool, specified by poolName,
  //return false if the pool doesn't exist in hdfs,
  //outPathsInPool should be allocated by caller.
  boolean getPathsFromPool(DistributedFileSystem dfs, String poolName, 
                           Set<String> outPathsInPool //out,all paths in a pool 
                           ) throws IOException
  {
       RemoteIterator<CachePoolEntry> poolIter = dfs.listCachePools();
       boolean isPoolExist = false;
       while (poolIter.hasNext()) {
            CachePoolEntry entry = poolIter.next();
            CachePoolInfo info = entry.getInfo();    
            if(info.getPoolName().compareTo(poolName) == 0){
                isPoolExist = true;
		break;
            }
       }
	
       if(isPoolExist)
       {
           CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder().setPool(poolName).build();
           RemoteIterator<CacheDirectiveEntry> dirIter = dfs.listCacheDirectives(directive);
           while(dirIter.hasNext()) {
               CacheDirectiveEntry en = dirIter.next();
               CacheDirectiveInfo cdInfo = en.getInfo();
               outPathsInPool.add(cdInfo.getPath().toUri().getPath().toString());
           }
           return true;
      }
      else
           return false;//pool does not exist
  }
  
  //find all directives belonging to a table and add them to hdfs cache pool.
  //same directives are not added twice.
  //return -1 if pool doesn't exist
  public int addTablesToHDFSCache(Object[] qualifiedTableNames, String poolName) throws IOException
  {
      Set<String> cachedDirectives = new TreeSet<String>();
      FileSystem fs = FileSystem.get(config);
      if(!getPathsFromPool((DistributedFileSystem)fs, poolName, cachedDirectives))
          return -1;

      for (int i = 0 ; i < qualifiedTableNames.length; i++) {
          String hbaseTableName = (String)qualifiedTableNames[i];
		  
          HTable table = new HTable(config, hbaseTableName);
		  
          Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(config), table.getName());
		  
          NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
          for (Map.Entry<HRegionInfo, ServerName> entry: locations.entrySet()) {
              FileStatus[] fds = FSUtils.listStatus(fs, new Path(tableDir, entry.getKey().getEncodedName()), 
                                                                            new FSUtils.FamilyDirFilter(fs));
              if(fds == null) 
                  continue;
              for (FileStatus status: fds) {
                  if(!cachedDirectives.contains(status.getPath().toUri().getPath().toString()))
          		 addDirectiveToHDFSCache((DistributedFileSystem)fs, status.getPath().toUri().getPath().toString(), poolName);
              }
          }
          //add next table in list to hdfs cache.
      }
      return 0;
  }
  //remove all directives belonging to a table in hdfs cache pool
  //return -1 if pool doesn't exist
  public int removeTablesFromHDFSCache(Object[] qualifiedTableNames, String poolName) throws IOException
  {
      Set<String> cachedDirectives = new TreeSet<String>();
      Set<String> poolNames = new TreeSet<String>();
      FileSystem fs = FileSystem.get(config);
      if(poolName.length() == 0)
      {//if the pool name is not specified remove from all pool
          RemoteIterator<CachePoolEntry> poolIter = ((DistributedFileSystem)fs).listCachePools();
          while (poolIter.hasNext()) {
              CachePoolEntry entry = poolIter.next();
              CachePoolInfo info = entry.getInfo();	
              poolNames.add(info.getPoolName());
         }
      }
      else if(getPathsFromPool((DistributedFileSystem)fs, poolName, cachedDirectives)) {
          poolNames.add(poolName);
      }
      else
         return -1;

      if(poolNames.size()==0)
          return -1;

      for (int i = 0 ; i < qualifiedTableNames.length; i++) {
          String hbaseTableName = (String)qualifiedTableNames[i];
          HTable table = new HTable(config, hbaseTableName);
		  
          Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(config), table.getName());
		  
          NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
          for (Map.Entry<HRegionInfo, ServerName> entry: locations.entrySet()) {
              FileStatus[] fds = FSUtils.listStatus(fs, new Path(tableDir, entry.getKey().getEncodedName()), 
                                                                            new FSUtils.FamilyDirFilter(fs));
              if(fds == null) 
                  continue;
              for (FileStatus status: fds) {
                  for(String pool: poolNames)
                      removeDirectiveFromHDFSCache((DistributedFileSystem)fs, status.getPath().toUri().getPath().toString(), pool);
              }
          }
          //remove next table in list to hdfs cache.
      }
      return 0;
  }
  
  public byte[][] showTablesHDFSCache(Object[] qualifiedTableNames) throws IOException
  {
    FileSystem fs = FileSystem.get(config);
    byte[][] rows = null;
    int COLNUM = 9;
    int [] colWidth = new int[COLNUM] ;
    for(int i = 0; i < COLNUM; i++) colWidth[i] = 0;
    int j = 0;
	
    for (int i = 0 ; i < qualifiedTableNames.length; i++) {
    	String hbaseTableName = (String)qualifiedTableNames[i];
    	HTable table = new HTable(config, hbaseTableName);
    	Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(config), table.getName());
    	
    	NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
 
        rows = new byte[locations.size()+1][]; 
           	
    	for (Map.Entry<HRegionInfo, ServerName> entry: locations.entrySet()) {
    		FileStatus[] fds = FSUtils.listStatus(fs, new Path(tableDir, (entry.getKey()).getEncodedName()), 
    									new FSUtils.FamilyDirFilter(fs));
    		if(fds == null)  continue;
    		for (FileStatus fstatus: fds) {
    			CacheDirectiveInfo directive = new CacheDirectiveInfo
    										  .Builder()
    										  .setPath(new Path(fstatus.getPath().toUri().getPath().toString()))
    										  .build();
    			RemoteIterator<CacheDirectiveEntry> iter = ((DistributedFileSystem)fs).listCacheDirectives(directive);
    			while(iter.hasNext()) {
    				CacheDirectiveEntry en = iter.next();
    				CacheDirectiveInfo cdInfo = en.getInfo();
    				CacheDirectiveStats stats = en.getStats();
    	
    				String oneRow;
    				oneRow = cdInfo.getId().toString() + "|";
    				oneRow += cdInfo.getPool() + "|";
    				oneRow += cdInfo.getReplication().toString() + "|";
    				// This is effectively never, round for nice printing
    				String expire;
    				if (cdInfo.getExpiration().getMillis() >
    					CacheDirectiveInfo.Expiration.MAX_RELATIVE_EXPIRY_MS / 2) {
    					expire = "never";
    				} else {
    					expire = cdInfo.getExpiration().toString();
    				}
				oneRow += expire + "|";
    				oneRow += cdInfo.getPath().toUri().getPath() + "|";
    				oneRow += String.valueOf(stats.getBytesNeeded()) + "|";
    				oneRow += String.valueOf(stats.getBytesCached()) + "|";
    				oneRow += String.valueOf(stats.getFilesNeeded()) + "|";
    				oneRow += String.valueOf(stats.getFilesCached());
    				rows[j++] = oneRow.getBytes();	
			        colWidth[0]=cdInfo.getId().toString().length() > colWidth[0]?cdInfo.getId().toString().length():colWidth[0];
				colWidth[1]=cdInfo.getPool().length() > colWidth[1]?cdInfo.getPool().length():colWidth[1];
			        colWidth[2]=cdInfo.getReplication().toString().length() > colWidth[2]?cdInfo.getReplication().toString().length():colWidth[2];
				colWidth[3]=expire.length() > colWidth[3]?expire.length():colWidth[3];
			        colWidth[4]=cdInfo.getPath().toUri().getPath().toString().length() > colWidth[4]?cdInfo.getPath().toUri().getPath().toString().length():colWidth[4];
				colWidth[5]=String.valueOf(stats.getBytesNeeded()).length() > colWidth[5]?String.valueOf(stats.getBytesNeeded()).length():colWidth[5];
			        colWidth[6]=String.valueOf(stats.getBytesCached()).length() > colWidth[6]?String.valueOf(stats.getBytesCached()).length():colWidth[6];
				colWidth[7]=String.valueOf(stats.getFilesNeeded()).length() > colWidth[7]?String.valueOf(stats.getFilesNeeded()).length():colWidth[7];	
				colWidth[8]=String.valueOf(stats.getFilesCached()).length() > colWidth[8]?String.valueOf(stats.getFilesCached()).length():colWidth[8];
    			}//while			   
    		}//for
    	}
    }
	
    String widthInfo = "";
    for(int i = 0; i < COLNUM; i++) { 
        widthInfo += String.valueOf(colWidth[i]) + "|"; 
    }
    rows[j] = widthInfo.getBytes();

    return rows;
  }

  public boolean createSnapshot( String tableName, String snapshotName)
      throws IOException
  {
      Admin admin = getConnection().getAdmin();
      admin.snapshot(snapshotName, TableName.valueOf(tableName));
      admin.close();
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.createSnapshot() - Snapshot created: " + snapshotName);
      return true;
  }

  public boolean restoreSnapshot( String snapshotName, String tableName)
      throws IOException
  {
    Admin admin = null;
    try
    {
      admin = getConnection().getAdmin();
      TableName table = TableName.valueOf(tableName);
      if (! admin.isTableDisabled(table))
          admin.disableTable(table);
      
      admin.restoreSnapshot(snapshotName);
      admin.enableTable(table);
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.restoreSnapshot() - Snapshot restored: " + snapshotName);
    }
    finally
    {
       admin.close();
    }
    return true;
  }

  public boolean verifySnapshot( String tableName, String snapshotName)
      throws IOException
  {
     Admin admin = getConnection().getAdmin();
     List<SnapshotDescription>  lstSnaps = admin.listSnapshots();
     try 
     {
        for (SnapshotDescription snpd : lstSnaps) {
           if (snpd.getName().compareTo(snapshotName) == 0 && 
                snpd.getTable().compareTo(tableName) == 0) {
              if (logger.isDebugEnabled()) 
                 logger.debug("HBaseClient.verifySnapshot() - Snapshot verified: " + snapshotName);
              return true;
           }
        }
      } finally {
        admin.close();
      }
      return false;
  }
 
  public boolean deleteSnapshot( String snapshotName)
      throws IOException 
  {
      Admin admin = getConnection().getAdmin();
      admin.deleteSnapshot(snapshotName);
      admin.close();
      if (logger.isDebugEnabled()) logger.debug("HBaseClient.deleteSnapshot() - Snapshot deleted: " + snapshotName);
      return true;
  }

    public  String[] namespaceOperation(int oper, String nameSpace,
                                        Object[] keyArray, Object[] valArray)
        throws IOException 
    {
        Admin admin = getConnection().getAdmin();

        NamespaceDescriptor[] listNSD = null;
        TableName[] listTableName = null;
        String[] namespaceObjects = null;
        if (oper == 1) // create
            {
                NamespaceDescriptor.Builder builder = 
                    NamespaceDescriptor.create(nameSpace);
                NamespaceDescriptor nsd = builder.build();

                if (keyArray != null && valArray != null &&
                    keyArray.length > 0 && 
                    (keyArray.length == valArray.length)) {
                    
                    for (int i = 0; i < keyArray.length; i++) {
                        String key = (String)keyArray[i];
                        key = key.trim();
                        String val = (String)valArray[i];
                        val = val.trim();

                        if (key.isEmpty() || val.isEmpty()) {
                            continue;
                        }

                        nsd.setConfiguration(key, val);
                    } // for
                } // if

                admin.createNamespace(nsd);
            }
        else if (oper == 2) // drop
            admin.deleteNamespace(nameSpace);
        else if (oper == 3) // list namespaces
            {
                listNSD = admin.listNamespaceDescriptors();
                if (listNSD.length > 0) {
                    int i = 0;
                    namespaceObjects = new String[listNSD.length];
                    
                    for (NamespaceDescriptor nsd : listNSD) {
                        namespaceObjects[i++] = nsd.getName();
                    }
                }
            }
        else if (oper == 4) // list tablesnames in nameSpace
            {
                listTableName = admin.listTableNamesByNamespace(nameSpace);
                int i = 0;
                if (listTableName.length > 0) {
                    namespaceObjects = new String[listTableName.length];
                    
                    for (TableName tn : listTableName) {
                        namespaceObjects[i++] = tn.getNameAsString();
                    }
                }
            }
        else if (oper == 5) // check if namespace exists
            {
                // exception will be throws if nameSpace doesnt exist
                listTableName = admin.listTableNamesByNamespace(nameSpace);
            }
        else if (oper == 6) // alter
            {
                if (keyArray != null && valArray != null &&
                    keyArray.length > 0 && 
                    (keyArray.length == valArray.length)) {

                    NamespaceDescriptor nsd = 
                        admin.getNamespaceDescriptor(nameSpace);
                    
                    for (int i = 0; i < keyArray.length; i++) {
                        String key = (String)keyArray[i];
                        key = key.trim();
                        String val = (String)valArray[i];
                        val = val.trim();

                        if (key.isEmpty()) {
                            continue;
                        }

                        if (val.isEmpty())
                            nsd.removeConfiguration(key);
                        else
                            nsd.setConfiguration(key, val);
                    } // for

                    admin.modifyNamespace(nsd);
                } // if
            }
        else if (oper == 7) // get namespace config
            {
                NamespaceDescriptor nsd = 
                    admin.getNamespaceDescriptor(nameSpace);
                
                Map<String, String> nsdConfig = nsd.getConfiguration();
                
                int i = 0;
                namespaceObjects = new String[nsdConfig.entrySet().size()];
                
                for (Map.Entry<String, String> e : nsdConfig.entrySet()) {
                    StringBuilder s = new StringBuilder();
                    String key = e.getKey();
                    String value = e.getValue();
                    if (key == null) {
                        continue;
                    }
                    s.append("'");
                    s.append(key);
                    s.append("' => '");
                    s.append(value);
                    s.append("'");
                    
                    namespaceObjects[i++] = s.toString();
                }
            }

        admin.close();
        if (logger.isDebugEnabled()) 
            logger.debug("HBaseClient.namespaceOperation()");
        return namespaceObjects;
    }

  public boolean savepointCommitOrRollback(long transID,
                                             long savepointID,
                                             long pSavepointID,
                                             boolean isCommit) throws IOException {
        short savepointResult = 0;
        if ((transID == -1 ) ||
            (savepointID <= 0)) {
            //            System.out.println("transID = -1");
            if (logger.isInfoEnabled()) logger.info("Entering savepointCommitOrRollback with -1 transID; returning false");
            return false;
        }
        if (isCommit)
            {
                //                System.out.println("commit savepoint");
                if (logger.isInfoEnabled()) logger.info("Entering savepointCommitOrRollback commit for transaction "
                        + transID + " savepointID " + savepointID);
                RMInterface table = new RMInterface(HBaseClient.getConnection());
                savepointResult = table.doCommitSavepoint(transID, savepointID, pSavepointID, config, getConnection());
                if (logger.isInfoEnabled()) logger.info("savepointCommitOrRollback commit for transaction "
                       + transID + " savepointID " + savepointID + " returning " + (savepointResult == 0));

 //               System.out.println("savepointCommitOrRollback commit for transaction "
  //                                 + transID + " savepointID " + savepointID + " returning savepointResult = " + savepointResult );

            }
        else
            {
                //                System.out.println("savepointID = " + savepointID);
                //                System.out.println("rollback savepoint");
                if (logger.isInfoEnabled()) logger.info("Entering savepointCommitOrRollback abort for transaction "
                       + transID + " savepointID " + savepointID);
                RMInterface table = new RMInterface(HBaseClient.getConnection());
                savepointResult = table.doAbortSavepoint(transID, savepointID, pSavepointID, config, getConnection());
                if (logger.isInfoEnabled()) logger.info("savepointCommitOrRollback abort for transaction "
                       + transID + " savepointID " + savepointID + " returning " + (savepointResult == 0));
            }

        return (savepointResult == 0);

    }

  public long getNextValue(String tabName, String rowId, String famName,
                           String qualName, long incrBy, boolean skipWAL) throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("HBaseClient.getNextValue() - Enter");

    Table osgTable = getConnection().getTable(TableName.valueOf(tabName));
    long nextValue = 0;
    if (skipWAL)
      nextValue = osgTable.incrementColumnValue(Bytes.toBytes(rowId),
                                            Bytes.toBytes(famName),
                                            Bytes.toBytes(qualName),
                                            incrBy,
                                            Durability.SKIP_WAL);
    else
      nextValue = osgTable.incrementColumnValue(Bytes.toBytes(rowId),
                                            Bytes.toBytes(famName),
                                            Bytes.toBytes(qualName),
                                            incrBy);
    osgTable.close();
    if (logger.isDebugEnabled()) logger.debug("tabName : " + tabName + ", rowId : " + rowId
                                              + ", famName : " + famName + " , qualName : "
                                              + ", incrBy: " + incrBy + ", skipWAL : "
                                              + skipWAL + ", nextValue : " + nextValue);
    return nextValue;
  }

  public long deleteSeqRow(String tabName, String rowId) throws IOException
  {
    Table osgTable = getConnection().getTable(TableName.valueOf(tabName));
    Delete todelete = new Delete(Bytes.toBytes(rowId));
    osgTable.delete(todelete);
    return 0;
  }

  public String [] getTableDefForBinlog(String t) throws IOException
  {
    Connection connection = getConnection();
    Table btable = connection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));

    Get g = new Get(Bytes.toBytes(t));
    Result rs = btable.get(g);
    g.addFamily(Bytes.toBytes(BINLOG_META_CF_NAME));
    String cols = Bytes.toString(rs.getValue(Bytes.toBytes(BINLOG_META_CF_NAME) , BINLOG_META_COL_QUAL));
    String keys = Bytes.toString(rs.getValue( Bytes.toBytes(BINLOG_META_CF_NAME) , BINLOG_META_KEY_QUAL ));

    String[] retV = new String[2];
    retV[0] = cols;
    retV[1] = keys;
    return retV;
  }

  public long updateTableDefForBinlog(String tabName, String cols, String keys, long ts)
  {
    try {
      Connection connection = getConnection();
      Table btable = connection.getTable(TableName.valueOf(BINLOG_TABLE_NAME));
      Put p = new Put(Bytes.toBytes(tabName));
      //System.out.println("input ts is " + ts + " curr " + System.currentTimeMillis() );
      //record the current timestamp as ts for put object
      if (ts == 0) 
        ts = System.currentTimeMillis();
      p.addColumn(Bytes.toBytes(BINLOG_META_CF_NAME) , BINLOG_META_COL_QUAL, ts, Bytes.toBytes(cols) );
      p.addColumn(Bytes.toBytes(BINLOG_META_CF_NAME) , BINLOG_META_KEY_QUAL, ts, Bytes.toBytes(keys) );
      btable.put(p);
    }
    catch (IOException e) {
      logger.error("update binlog info error, table name: " + tabName
                   + "; cols: " + cols + "; keys: " + keys);
    }

    return 0;
  }
    
  public static boolean createSnapshot( String tableName, String snapshotName, boolean deleteIfExists)
      throws IOException
  {
    Admin admin = null;
    try 
    {
      admin = getConnection().getAdmin();
      try {
         admin.snapshot(snapshotName, TableName.valueOf(tableName));
      } catch (SnapshotExistsException e) {
         try {
           if (! deleteIfExists)
              throw e;
           admin.deleteSnapshot(snapshotName);
         } catch (IOException ioe) {}
         admin.snapshot(snapshotName, TableName.valueOf(tableName));
      }
    }
    finally
    {
      admin.close();
    }
    return true;
  }
  
  public static boolean deleteSnapshot( String snapshotName, boolean throwError)
      throws IOException
  {
    
    Admin admin = null;
    try
    {
      admin = getConnection().getAdmin();
      try {
        admin.deleteSnapshot(snapshotName);
      } catch (IOException ioe) {
          if (throwError)
             throw ioe;
      }
    }
    finally 
    {
       admin.close();
    }
    return true;
  }

  public static boolean cancelOperation(long transactionID)
  {
      if (logger.isInfoEnabled()) 
	  logger.info("cancel the transaction " + transactionID + " start.");

      Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
      TransactionState ts = mapTransactionStates.get(transactionID);
      if (ts != null) {
          ts.setCancelOperation(true);
      }

      if (logger.isInfoEnabled()) 
	  logger.info("transaction " + transactionID + " is canceled.");

    return true;
  }

  public boolean putData(long commitId, String query, int eventType, String schemaName, byte[] params){
    try {
            BinlogProducer binlogProducer = BinlogProducer.getInstance();
            return binlogProducer.putData(commitId, query, eventType, schemaName, params);
    } catch (Exception e) {
            logger.error("hbase client putdata failed");
    }
    return false;
  }

}

