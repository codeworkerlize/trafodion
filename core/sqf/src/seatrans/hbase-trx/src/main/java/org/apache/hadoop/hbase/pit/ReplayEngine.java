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


package org.apache.hadoop.hbase.pit;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import org.apache.hadoop.hdfs.client.HdfsAdmin;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.HConnectionManager;
//import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;

import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.*;

public class ReplayEngine {
    //static final Log LOG = LogFactory.getLog("ReplayEngine.class");
    static Logger LOG = Logger.getLogger(ReplayEngine.class);

    private static Configuration config;
    private Connection readConnection;
    private Connection writeConnection;
    private Configuration adminReadConf;
    private Connection adminReadConnection;
    private FileSystem fileSystem = null;
  //  private int       my_cluster_id     = 0; 
    private long timeStamp = 0;
    private long startTime = 0;
    private int xdc_startTime_check = 1; // default is to always do xdc start time check

    // These are here to not have to pass them around
    //private HTable table;
    private Admin admin;
    private HdfsAdmin hdfsAdmin;
    int pit_thread = 1;

    public static final int XDC_UP          = 1;                     // 00001
    public static final int XDC_DOWN        = 2;                // 00010
    public static final int SYNCHRONIZED    = 4;           // 00100
    public static final int SKIP_CONFLICT   = 8;            // 01000
    public static final int SKIP_REMOTE_CK  = 16;      // 10000
    public static final int INCREMENTALBR  = 32;      // 100000
    public static final int TABLE_ATTR_SET  = 64;  // 1000000
    public static final int SKIP_SDN_CDC  = 128;  // 10000000
    public static final int PIT_ALL  = 256;                  // 100000000


  /**
   * threadPool - pool of thread for asynchronous requests
   */
    private ExecutorService threadPool_table;
    private ExecutorService threadPool_file;

    void setupLog4j() {
	System.setProperty("hostName", System.getenv("HOSTNAME"));
        System.setProperty("trafodion.logdir", System.getenv("TRAF_LOG"));
        String confFile = System.getenv("TRAF_CONF")
            + "/log4j.dtm.config";
        PropertyConfigurator.configure(confFile);
   }

    /**
     * ReplayEngineCallable  :  inner class for creating asynchronous requests
     */
    private abstract class ReplayEngineCallable implements Callable<Integer>  {

        ReplayEngineCallable(){}

        public Integer doReplay(List<MutationMetaRecord> mList,
                                Table mtable,
                                Configuration mconfig,
                                String msnapshotPath,
                                int pit_batch,
                                boolean pv_skip_snapshot,
                                boolean pv_verbose) throws IOException {
            long threadId = Thread.currentThread().getId();
            if (LOG.isTraceEnabled()) LOG.trace("ENTRY doReplay from thread " + threadId + " path " + msnapshotPath);

            if (!pv_skip_snapshot) {
                admin.restoreSnapshot(msnapshotPath);

                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine got path restored " + msnapshotPath);
            }

            for (int i = 0; i < mList.size(); i++) {

                if (LOG.isTraceEnabled()) LOG.trace("doReplay" + ", thread: " + threadId
                        + ", path " + msnapshotPath + ", file#: " + i);
                MutationMetaRecord mutationRecord = mList.get(i);
                String mutationPathString = mutationRecord.getMutationPath();
                try {
                    mutationPathString = new URI(mutationPathString).getPath();
                } catch (Exception e) {
                    LOG.warn("MutationPath URI error: " + mutationPathString);
                }
                Path mutationPath = new Path(mutationPathString);

                // read in mutation file, parse it and replay operations
                if (pit_batch >= 1)
                    mutationReaderFile2(mutationPath, mconfig, mtable, pit_batch, pv_verbose, false);
                else
                    mutationReaderFile(mutationPath, mconfig, mtable, pv_verbose, false);
            }

            if (LOG.isTraceEnabled()) LOG.trace("EXIT doReplay from thread " + threadId + " path " + msnapshotPath);
            return 0;
        }

        public Integer doReplayParallel(Map<String,List<MutationMetaRecord>> mapByRegion,
                    List<LobMetaRecord> lobList,
                    final Table mtable,
					final Configuration mconfig, 
					String msnapshotPath,
					final int pit_batch,
					boolean pv_skip_snapshot,
					final boolean pv_verbose,
                                        final boolean xdc_replay) throws Exception  
	{
	    long threadId = Thread.currentThread().getId();
	    if (pv_verbose) {
		    System.out.println("doReplayParallel ENTRY"
				   + ", thread: " + threadId 
				   + ", path: " + msnapshotPath);
	    }
	    if (LOG.isInfoEnabled()) LOG.info("doReplayParallel ENTRY" + ", thread: " + threadId + ", path: " + msnapshotPath);

	    if (! pv_skip_snapshot) {
		    admin.restoreSnapshot(msnapshotPath);
		    if (pv_verbose)
		        System.out.println("doReplayParallel, ReplayEngine restored path: " + msnapshotPath);
		    if (LOG.isInfoEnabled())
                LOG.info("doReplayParallel, ReplayEngine restored path: " + msnapshotPath);
	    }

        String hdfsRoot = config.get("fs.default.name");
        Path hdfsRootPath = new Path(hdfsRoot);
        hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);

	    for (int i = 0; i < lobList.size(); i++) {
		   if (pv_verbose) {
              System.out.println("doReplayParallel"
                       + ", thread: " + threadId
                       + ", path: " + msnapshotPath
                       + ", lobSnapshot#: " + i);
		   }
           LobMetaRecord lobRecord = lobList.get(i);
		   String lobPathString = lobRecord.getHdfsPath();
		   final Path lobPath = new Path (lobPathString);

	    }

	    CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_file);

        for (final Map.Entry<String, List<MutationMetaRecord>> entry : mapByRegion.entrySet()) {
            final List<MutationMetaRecord> mutationMetaRecordList = entry.getValue();
            final List<Path> pathList = getSortedMutationList(mutationMetaRecordList);

		    // Send mutation work to a thread for work
		    compPool.submit(new ReplayEngineCallable() {
			    public Integer call() throws IOException {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("doReplayParallelByRegion table  " + mtable.getName().getNameAsString() +" region " +
                                entry.getKey() + " mutationSize " + pathList.size());
                    }
			        Table lv_table = writeConnection.getTable(mtable.getName());
			        int res = 0;
                    for (Path path : pathList) {
                        try {
                            res += doReplayFile(path,
                                    lv_table,
                                    mconfig,
                                    pit_batch,
                                    pv_verbose,
                                    xdc_replay);
                        } catch (Throwable e) {
                            LOG.error("doReplayFile error: path " + path.toString(), e);
                            throw new IOException(e);
                        }
                    }
                    return res;
                }
		    });
	    }

	    try {
		    for (int i = 0; i < mapByRegion.size(); i++) {
		        int returnValue = compPool.take().get();
		        if (pv_verbose) {
			        System.out.println("doReplayParallel, check return code"
					   + ", loopIndex: " + i
					   + ", returnValue: " + returnValue
					   );
		        }
            }
	    }catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            if (pv_verbose) System.out.println("doReplayParallel, exception retrieving replies : " + sw.toString());
        	LOG.error("doReplayParallel, exception retrieving replies : " , e);
		    throw e;
	    }

	    if (pv_verbose) {
		System.out.println("doReplayParallel: EXIT"
				   + ", thread: " + threadId 
				   + ", path: " + msnapshotPath);
	    }
	    if (LOG.isInfoEnabled()) 
		    LOG.info("doReplayParallel: EXIT" + ", thread: " + threadId + ", path: " + msnapshotPath);

	    return 0;
	}

        public Integer doReplayFile(Path pv_mutation_path,
				    Table mtable,
				    Configuration mconfig, 
				    int pit_batch,
				    boolean pv_verbose,
                                    boolean xdc_replay) throws IOException  
	{
	    long threadId = Thread.currentThread().getId();

	    if (LOG.isTraceEnabled()) LOG.trace("doReplayFile ENTRY"
						+ ", thread: " + threadId 
						+ ", path: " + pv_mutation_path
						);
	  
	    if (pit_batch >= 1)
		    mutationReaderFile2(pv_mutation_path, mconfig, mtable, pit_batch, pv_verbose, xdc_replay);
	    else 
		    mutationReaderFile(pv_mutation_path, mconfig, mtable, pv_verbose, xdc_replay);
	  
	    if (LOG.isTraceEnabled())
		LOG.trace("doReplayFile EXIT" 
			  + ", thread: " + threadId 
			  + ", path: " + pv_mutation_path);

	    return 0;
	}
  }

    public void startThreadPools(int pv_num_table_threads,
				 int pv_num_file_threads)
    {
	if (LOG.isTraceEnabled()) LOG.trace("startThreadPools ENTRY" 
					    + ", num_table_threads: " + pv_num_table_threads 
					    + ", num_file_threads: " + pv_num_file_threads
					    );
        threadPool_table = Executors.newFixedThreadPool(pv_num_table_threads);
        threadPool_file = Executors.newFixedThreadPool(pv_num_file_threads);
    }

    public void shutdownThreadPools(boolean pv_now) {
	if (LOG.isTraceEnabled()) LOG.trace("shutdownThreadPools ENTRY" 
					    + ", shutdownNow?: " + pv_now
					    );
	if (pv_now) {
	    threadPool_table.shutdownNow();
	    threadPool_file.shutdownNow();
	}
	else {
	    threadPool_table.shutdown();
	    threadPool_file.shutdown();
	}
    }

    public ReplayEngine(String tag,
            long restoreTS,
			int pit_thread,
			int pit_parallel,
			int pv_peer_id,
			final boolean pv_skip_snapshot,
			final boolean pv_verbose) throws Exception
    {
        //setupLog4j();
	if (LOG.isInfoEnabled())
	    LOG.info("ReplayEngine (tag) constructor ENTRY"
	         + ", restoreTS: " + restoreTS
             + ", tag: " + tag
		     + ", parallel threads: " + pit_thread + " new "
		     + ", pit_parallel: " + pit_parallel
		     + ", peer_id: " + pv_peer_id
		     + ", skip_snapshot: " + pv_skip_snapshot
		     + ", verbose: " + pv_verbose
		     );

        int pit_batch = 0;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
          pit_batch = Integer.parseInt(pitBatch);
          if(LOG.isDebugEnabled()) LOG.debug("PIT mutation batch setting: " + pit_batch);
        }
        if (LOG.isInfoEnabled()) LOG.info("PIT mutation batch setting: " + pit_batch);
        final int pit_batch1 = pit_batch;

	startThreadPools(pit_thread, pit_thread);

        config = new Configuration();
        Configuration peer_config = null;

	if (pv_peer_id == 0) {
	    peer_config = config;
	}
	else {
	    try {
		STRConfig pSTRConfig = STRConfig.getInstance(config);
		peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
		if (peer_config == null) {
		    System.exit(1);
		}
//        connection = pSTRConfig.getPeerConnection(pv_peer_id);
	    } catch (IOException ioe) {
		    System.exit(1);
	    }
	}
        readConnection = ConnectionFactory.createConnection(config);
        writeConnection = ConnectionFactory.createConnection(peer_config);
        fileSystem = FileSystem.get(config);

        admin = readConnection.getAdmin();

        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_table);
        try {
            ArrayList<String> tables = new ArrayList<String>();
            ArrayList<TableRecoveryGroup> trgs = new ArrayList<TableRecoveryGroup>();

            RecoveryRecord recoveryRecord = new RecoveryRecord(config,
                                                               tag,
                                                               "REPLAY",
                                                               /* adjust */ true,
                                                               /* force */ false,
                                                               /* restoreTS */ restoreTS,
                                                               /* showOnly */ false);
            timeStamp = recoveryRecord.getEndTime();
            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

            int table_num = 0;
            int table_restored = 0;
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()) {
                  tables.add(table_num, tableEntry.getKey());
                  trgs.add(table_num, tableEntry.getValue());
                  table_num++;
            }

            int table_remain = table_num;
            int table_idx = 0;
            int mini_batch = pit_thread;

            if (LOG.isInfoEnabled()) LOG.info("ReplayEngine: (tag) tables remaining " + table_num + " tables restored " + table_restored);

            while ((mini_batch > 0) && (table_remain > 0)) { // submit the 1st batch (pit_thread) of table restore

                String tableName = tables.get(table_idx);
                TableRecoveryGroup tableRecoveryGroup = trgs.get(table_idx);

                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine (tag) working on table " + table_idx + " Name " + tableName);
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                final String snapshotPath = tableMeta.getSnapshotPath();
                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine (tag) snapshot path:" + snapshotPath);
                // these need to be final due to the inner class access
                final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine (tag) : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                // Send mutation work to a thread for work
                compPool.submit(new ReplayEngineCallable() {
                    public Integer call() throws IOException {
                        return doReplay(mutationList,
					table,
					config,
					snapshotPath,
					pit_batch1,
					pv_skip_snapshot,
					pv_verbose);
                   }
                 });

                 table_idx++;
                 table_remain--;
                 mini_batch--;
            }

            while (table_num > table_restored) { // there are still tables to be restored

                int returnValue = compPool.take().get();

                table_restored++;
                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine (tag) restored table " + table_restored + " with return code " + returnValue);

                // TBD: handle restore/CompletionService exception

                if (table_idx < table_num) { // there are still tables to be submitted

                     String tableName = tables.get(table_idx);
                     TableRecoveryGroup tableRecoveryGroup = trgs.get(table_idx);

                     if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine (tag) working on table " + table_idx + " Name " + tableName);
                     if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine (tag) got TableRecoveryGroup");
                     SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                     final String snapshotPath = tableMeta.getSnapshotPath();
                     if (LOG.isInfoEnabled()) LOG.info("ReplayEngine (tag) snapshot path:" + snapshotPath);
                     // these need to be final due to the inner class access
                     final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                     final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                     String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                     if (LOG.isInfoEnabled()) LOG.info("ReplayEngine (tag) : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                     // Send mutation work to a thread for work
                     compPool.submit(new ReplayEngineCallable() {
                         public Integer call() throws IOException {
                             return doReplay(mutationList,
					     table,
					     config,
					     snapshotPath,
					     pit_batch1,
					     pv_skip_snapshot,
					     pv_verbose);
                        }
                      });

                      table_idx++;
                      table_remain--;
                }
            } // while loop

        }catch (Exception e) {
            LOG.error("ReplayEngine (tag) exception : ", e.getCause());
	    shutdownThreadPools(true);
            throw e;
       }

	shutdownThreadPools(false);
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine (tag) constructor EXIT");
    }

    public ReplayEngine(long timestamp,
			int pit_thread, 
			int pit_parallel, 
			int pv_peer_id,
			final boolean pv_skip_snapshot,
			final boolean pv_verbose) throws Exception 
    {
        //setupLog4j();
	if (LOG.isInfoEnabled())
	    LOG.info("ReplayEngine constructor ENTRY"
             + ", timestamp: " + timestamp
		     + ", parallel threads: " + pit_thread + " new "
		     + ", pit_parallel: " + pit_parallel
		     + ", peer_id: " + pv_peer_id
		     + ", skip_snapshot: " + pv_skip_snapshot
		     + ", verbose: " + pv_verbose
		     ); 

        int pit_batch = 0;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
    	  pit_batch = Integer.parseInt(pitBatch);
    	  if(LOG.isDebugEnabled()) LOG.debug("PIT mutation batch setting: " + pit_batch);
        }
        if (LOG.isInfoEnabled()) LOG.info("PIT mutation batch setting: " + pit_batch);
        final int pit_batch1 = pit_batch;

        startThreadPools(pit_thread, pit_thread);

        timeStamp = timestamp;

        config = new Configuration();
        Configuration peer_config = null;

    if (pv_peer_id == 0) {
	    peer_config = config;
	}
	else {
	    try {
		STRConfig pSTRConfig = STRConfig.getInstance(config);
		peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
		if (peer_config == null) {
		    System.exit(1);
		}
//        connection = pSTRConfig.getPeerConnection(pv_peer_id);
	    } catch (IOException ioe) {
           System.exit(1);
	    }
	}
        readConnection = ConnectionFactory.createConnection(config);
        writeConnection = ConnectionFactory.createConnection(peer_config);
        fileSystem = FileSystem.get(config);

        admin = readConnection.getAdmin();

        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_table);
        try {
            ArrayList<String> tables = new ArrayList<String>();
            ArrayList<TableRecoveryGroup> trgs = new ArrayList<TableRecoveryGroup>();

            RecoveryRecord recoveryRecord = new RecoveryRecord(timeStamp);
            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

            int table_num = 0;            
            int table_restored = 0;
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet()) {       
                  tables.add(table_num, tableEntry.getKey());
                  trgs.add(table_num, tableEntry.getValue());
                  table_num++;
            }            
    
            int table_remain = table_num;
            int table_idx = 0;
            int mini_batch = pit_thread;

            if (LOG.isInfoEnabled()) LOG.info("ReplayEngine: tables remaining " + table_num + " tables restored " + table_restored);
            while ((mini_batch > 0) && (table_remain > 0)) { // submit the 1st batch (pit_thread) of table restore

                String tableName = tables.get(table_idx);
                TableRecoveryGroup tableRecoveryGroup = trgs.get(table_idx);

                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine working on table " + table_idx + " Name " + tableName);
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                final String snapshotPath = tableMeta.getSnapshotPath();
                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine snapshot path:" + snapshotPath);              
                // these need to be final due to the inner class access
                final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                // Send mutation work to a thread for work
                compPool.submit(new ReplayEngineCallable() {
                    public Integer call() throws IOException {
                        return doReplay(mutationList, 
					table, 
					config, 
					snapshotPath,
					pit_batch1, 
					pv_skip_snapshot,
					pv_verbose);
                   }
                 });

                 table_idx++;
                 table_remain--;
                 mini_batch--;
            }

            while (table_num > table_restored) { // there are still tables to be restored

                int returnValue = compPool.take().get();

                table_restored++;
                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine restored table " + table_restored + " with return code " + returnValue);

                // TBD: handle restore/CompletionService exception

                if (table_idx < table_num) { // there are still tables to be submitted

                     String tableName = tables.get(table_idx);
                     TableRecoveryGroup tableRecoveryGroup = trgs.get(table_idx);

                     if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine working on table " + table_idx + " Name " + tableName);
		             if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine got TableRecoveryGroup"); 
                     SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                     final String snapshotPath = tableMeta.getSnapshotPath();
                     if (LOG.isInfoEnabled()) LOG.info("ReplayEngine snapshot path:" + snapshotPath);              
                     // these need to be final due to the inner class access
                     final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                     final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                     String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                     if (LOG.isInfoEnabled()) LOG.info("ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                     // Send mutation work to a thread for work
                     compPool.submit(new ReplayEngineCallable() {
                         public Integer call() throws IOException {
                             return doReplay(mutationList,
					     table,
					     config, 
					     snapshotPath,
					     pit_batch1,
					     pv_skip_snapshot,
					     pv_verbose);
                        }
                      });

                      table_idx++;
                      table_remain--;
                }
            } // while loop

        }catch (Exception e) {
            LOG.error("Replay Engine exception : ", e.getCause());
	        shutdownThreadPools(true);
            throw e;
       }

	    shutdownThreadPools(false);
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor EXIT");
    }

    public ReplayEngine(long timestamp, 
			int pit_thread,
			int pv_peer_id,
			final boolean pv_skip_snapshot, 
			final boolean pv_verbose) throws Exception {
        //setupLog4j();
	if (LOG.isInfoEnabled())
	    LOG.info("ReplayEngine constructor ENTRY"
             + ", timestamp: " + timestamp
		     + ", parallel threads: " + pit_thread + " now "
		     + ", peer_id: " + pv_peer_id
		     + ", skip_snapshot: " + pv_skip_snapshot
		     + ", verbose: " + pv_verbose
		     ); 

        int pit_batch = 200;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
    	  pit_batch = Integer.parseInt(pitBatch);
          if (pit_batch <= 0) pit_batch = 0;
    	  if(LOG.isDebugEnabled()) LOG.debug("PIT mutation batch setting: " + pit_batch);
        }
        final int pit_batch1 = pit_batch;
        if (LOG.isInfoEnabled()) LOG.info("PIT mutation batch setting: " + pit_batch);

	startThreadPools(pit_thread, pit_thread);

        timeStamp = timestamp;

        config = HBaseConfiguration.create();
        Configuration peer_config = null;
        
	if (pv_peer_id == 0) {
	    peer_config = config;
	}
	else {
	    try {
		STRConfig pSTRConfig = STRConfig.getInstance(config);
		peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
		if (peer_config == null) {
           System.exit(1);
		}
	    } catch (IOException ioe) {
           System.exit(1);
	    }
	}

        readConnection = ConnectionFactory.createConnection(config);
        writeConnection = ConnectionFactory.createConnection(peer_config);
        fileSystem = FileSystem.get(config);

        admin = readConnection.getAdmin();
        String hdfsRoot = config.get("fs.default.name");
        Path hdfsRootPath = new Path(hdfsRoot);
        hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);

        int loopCount = 0;
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_table);
        try {
            RecoveryRecord recoveryRecord = new RecoveryRecord(timeStamp);
        
            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();
 
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
            {            
                String tableName = tableEntry.getKey();
                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine working on table " + tableName);

                TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine got TableRecoveryGroup"); 
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                final String snapshotPath = tableMeta.getSnapshotPath();

                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine snapshot path:" + snapshotPath);

                final List<LobMetaRecord> lobList = tableRecoveryGroup.getLobList();
                
                // these need to be final due to the inner class access
                final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                final Map<String,List<MutationMetaRecord>> groupByRegion = mapByRegion(mutationList);
                // Send mutation work to a thread for work
                compPool.submit(new ReplayEngineCallable() {
                    public Integer call() throws Exception {
                        return doReplayParallel(groupByRegion,
						lobList,
						table, 
						config, 
						snapshotPath,
						pit_batch1,
						pv_skip_snapshot, 
						pv_verbose,
                                                false);
                   }
                 });

                loopCount++;
            } 
        }catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            LOG.error("Replay Engine exception in retrieving/replaying mutations : ", e);
	    shutdownThreadPools(true);
            throw e;
        }

        try {
          // simply to make sure they all complete, no return codes necessary at the moment
          for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
            int returnValue = compPool.take().get();
	    //            if ((loopIndex % 10) == 1) System.out.println("..... ReplayEngine: table restored " + (loopIndex*100)/loopCount + " .....");
          }
        }catch (Exception e) {
            LOG.error("Replay Engine exception retrieving replies : ", e);
	        shutdownThreadPools(true);
            throw e;
       }

	    shutdownThreadPools(false);
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor EXIT");
    }


// for xdc catchup specific

    public ReplayEngine(long timestamp, 
                        int pit_thread,
                        int pv_peer_id,
                        final boolean pv_skip_snapshot, 
                        final boolean pv_verbose,
                        ArrayList<String> tableList) throws Exception {
        //setupLog4j();
        if (LOG.isInfoEnabled())
            LOG.info("xdc ReplayEngine constructor ENTRY"
             + ", timestamp: " + timestamp
                     + ", parallel threads: " + pit_thread + " now "
                     + ", peer_id: " + pv_peer_id
                     + ", skip_snapshot: " + pv_skip_snapshot
                     + ", verbose: " + pv_verbose
                     ); 
        if (pv_verbose) 
            System.out.println("xdc ReplayEngine constructor ENTRY"
                   + ", timestamp: " + timestamp
                               + ", parallel threads: " + pit_thread + " now "
                               + ", peer_id: " + pv_peer_id
                               + ", skip_snapshot: " + pv_skip_snapshot
                               + ", verbose: " + pv_verbose
                           ); 

        int pit_batch = 200;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
          pit_batch = Integer.parseInt(pitBatch);
          if (pit_batch <= 0) pit_batch = 0;
          if(LOG.isDebugEnabled()) LOG.debug("xdc PIT mutation batch setting: " + pit_batch);
        }
        final int pit_batch1 = pit_batch;
        if (LOG.isInfoEnabled()) LOG.info("xdc PIT mutation batch setting: " + pit_batch);

        String sv_xdc_startTime_check = System.getenv("XDC_START_TIME_CHECK");
        if (sv_xdc_startTime_check != null)
              xdc_startTime_check = Integer.parseInt(sv_xdc_startTime_check);
        if (LOG.isInfoEnabled()) LOG.info("XDC start time check: " + xdc_startTime_check);

        startThreadPools(pit_thread, pit_thread);

        timeStamp = timestamp;

        Set<String> tableSet = new HashSet<>(tableList);

        config = HBaseConfiguration.create();
        Configuration peer_config = null;
        
        if (pv_peer_id == 0) {
            peer_config = config;
        }
        else {
            try {
                STRConfig pSTRConfig = STRConfig.getInstance(config);
                peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
                if (peer_config == null) {
                    System.out.println("Peer ID: " + pv_peer_id + " does not exist OR it has not been configured for synchronization.");
                    System.exit(1);
                }
            } catch (IOException ioe) {
                if (pv_verbose) System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
                System.exit(1);
            }
        }

        readConnection = ConnectionFactory.createConnection(config);
        writeConnection = ConnectionFactory.createConnection(peer_config);
        fileSystem = FileSystem.get(config);

        admin = readConnection.getAdmin();
        String hdfsRoot = config.get("fs.default.name");
        Path hdfsRootPath = new Path(hdfsRoot);
        hdfsAdmin = new HdfsAdmin ( hdfsRootPath.toUri(), config);
        SnapshotMeta replaySM = new SnapshotMeta(config);

        int loopCount = 0;
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_table);
        try {
            RecoveryRecord recoveryRecord = null;
            try{
               recoveryRecord = new RecoveryRecord(timeStamp, true /* xdcReplay */, tableList);
            }
            catch (Exception re){
               if (LOG.isInfoEnabled()) LOG.info("xdc ReplayEngine Exception creating RecoveryRecord ", re);
               throw re;
            }
            finally{
               // To get here we must have already acquired the SM lock and now we have the RecoveryRecord,
               // so we can unlock and allow additional backups to proceed
               if (LOG.isInfoEnabled()) LOG.info("xdc ReplayEngine releasing xDC lock");
               replaySM.unlock("XDC_PEER_UP");
            }

            startTime = recoveryRecord.getSnapshotStartRecord().getKey(); // start cid of xdc peer down record

            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

            if (pv_verbose) {
                System.out.println("xdc ReplayEngine: size of recoveryTableMap: " + recoveryTableMap.size());
            }
 
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
            {            
                String tableName = tableEntry.getKey();

                if (LOG.isTraceEnabled()) LOG.trace("xdc ReplayEngine working on table " + tableName);

                if (! tableSet.contains(tableName)) {
                     System.out.println("xdc ReplayEngine skip on table " + tableName + " not synchronous ");
                     continue;
                }
                else {
                     if (pv_verbose) {
                         System.out.println("xdc ReplayEngine working on table " + tableName);
                     }
                }

                TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

                if (LOG.isTraceEnabled()) LOG.trace("xdc ReplayEngine got TableRecoveryGroup"); 
                if (pv_verbose) {
                    System.out.println("xdc ReplayEngine got TableRecoveryGroup " + tableRecoveryGroup);
                }
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                final String snapshotPath = tableMeta.getSnapshotPath();

                if (pv_verbose) {
                    System.out.println("xdc ReplayEngine got path: " + snapshotPath);
                }
                if (LOG.isInfoEnabled()) LOG.info("xdc ReplayEngine snapshot path:" + snapshotPath);

                final List<LobMetaRecord> lobList = tableRecoveryGroup.getLobList();
                if (pv_verbose) System.out.println("xdc ReplayEngine : " + lobList.size() + " lob files for table: " + tableName);
                
                // these need to be final due to the inner class access
                final Table table = writeConnection.getTable(TableName.valueOf(tableName));
                final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                if (pv_verbose) System.out.println("xdc ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);
                if (LOG.isInfoEnabled()) LOG.info("xdc ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                final Map<String,List<MutationMetaRecord>> groupByRegion = mapByRegion(mutationList);
                // Send mutation work to a thread for work
                compPool.submit(new ReplayEngineCallable() {
                    public Integer call() throws Exception {
                        return doReplayParallel(groupByRegion,
                                                lobList,
                                                table, 
                                                config, 
                                                snapshotPath,
                                                pit_batch1,
                                                pv_skip_snapshot, 
                                                pv_verbose,
                                                true /* xdc replay */);
                   }
                 });

                loopCount++;
            } 
        }catch (Exception e) {
            if (pv_verbose) System.out.println("xdc Replay Engine exception in retrieving/replaying mutations :" + e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            if (pv_verbose) System.out.println(sStackTrace);
            LOG.error("xdc Replay Engine exception in retrieving/replaying mutations : ", e);
            shutdownThreadPools(true);
            throw e;
        }

        try {
            if (pv_verbose) {
                System.out.println("xdc ReplayEngine, after compPool.submit" 
                                   + ", loopCount: " + loopCount
                                   );
            }
          // simply to make sure they all complete, no return codes necessary at the moment
          for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
            int returnValue = compPool.take().get();
            if (pv_verbose) {
                System.out.println("xdc ReplayEngine, check return code" 
                                   + ", loopIndex: " + loopIndex
                                   + ", returnValue: " + returnValue
                                   );
            }
            //            if ((loopIndex % 10) == 1) System.out.println("..... ReplayEngine: table restored " + (loopIndex*100)/loopCount + " .....");
          }
        }catch (Exception e) {
            if (pv_verbose) System.out.println("xdc Replay Engine exception retrieving replies : " + e);
            LOG.error("xdc Replay Engine exception retrieving replies : ", e);
            shutdownThreadPools(true);
            throw e;
       }

        shutdownThreadPools(false);
        if (LOG.isTraceEnabled()) LOG.trace("xdc ReplayEngine constructor EXIT");
    }

    public ReplayEngine(long timestamp,
                        int pv_peer_id,
                        boolean pv_skip_snapshot,
                        boolean pv_verbose) throws Exception {
        Table mtable;
        if (LOG.isInfoEnabled())
            LOG.info("ReplayEngine constructor ENTRY"
                    + ", timestamp: " + timestamp
                    + ", peer_id: " + pv_peer_id
                    + ", skip_snapshot: " + pv_skip_snapshot
                    + ", verbose: " + pv_verbose
            );

        timeStamp = timestamp;

        int pit_batch = 200;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
            pit_batch = Integer.parseInt(pitBatch);
            if (pit_batch < 0) pit_batch = 0;
            if (LOG.isDebugEnabled()) LOG.debug("PIT mutation batch setting: " + pit_batch);
        }
        if (LOG.isInfoEnabled()) LOG.info("PIT mutation batch setting: " + pit_batch);

        config = HBaseConfiguration.create();
        Configuration peer_config = null;

        if (pv_peer_id == 0) {
            peer_config = config;
        } else {
            try {
                STRConfig pSTRConfig = STRConfig.getInstance(config);
                peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
                if (peer_config == null) {
                    System.exit(1);
                }
//        connection = pSTRConfig.getPeerConnection(pv_peer_id);
            } catch (IOException ioe) {
                System.exit(1);
            }
        }

        readConnection = ConnectionFactory.createConnection(config);
        writeConnection = ConnectionFactory.createConnection(peer_config);
        fileSystem = FileSystem.get(config);

        admin = readConnection.getAdmin();

        try {
            RecoveryRecord recoveryRecord = new RecoveryRecord(timeStamp);

            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

            for (Map.Entry<String, TableRecoveryGroup> tableEntry : recoveryTableMap.entrySet()) {
                String tableName = tableEntry.getKey();

                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine working on table " + tableName);

                TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                String snapshotPath = tableMeta.getSnapshotPath();

                if (LOG.isInfoEnabled()) LOG.info("ReplayEngine restoring snapshot :" + snapshotPath);

                if (!pv_skip_snapshot) {
                    admin.restoreSnapshot(snapshotPath);
                }

                mtable = writeConnection.getTable(TableName.valueOf(tableName));

                // Now go through mutations files one by one for now
                List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();
                String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
                LOG.info("ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);

                for (int i = 0; i < mutationList.size(); i++) {
                    MutationMetaRecord mutationRecord = mutationList.get(i);
                    String mutationPathString = mutationRecord.getMutationPath();
                    Path mutationPath = new Path(new URI(mutationPathString).getPath());

                    // read in mutation file, parse it and replay operations
                    if (pit_batch >= 1)
                        mutationReaderFile2(mutationPath, config, mtable, pit_batch, pv_verbose, false);
                    else
                        mutationReaderFile(mutationPath, config, mtable, pv_verbose, false);
                }
            }
        } catch (Exception e) {
            LOG.error("ReplayEngine Exception occurred during Replay ", e);
            throw e;
        }

    }

    public ReplayEngine(final List<MutationMetaRecord> mutationList,
            final List<LobMetaRecord> lobList,
            String tableName,
            long timestamp,
            int numParallel,
            int pv_peer_id,
            boolean pv_verbose) throws Exception
    {
	    if (LOG.isInfoEnabled()){
          LOG.info("ReplayEngine (mutationList) constructor ENTRY"
             + ", mutationList size: " + mutationList.size()
             + ", lobList size: " + lobList.size()
             + ", tableName: " + tableName
             + ", timestamp: " + timestamp
             + ", numParallel" + numParallel
             + ", peer_id: " + pv_peer_id
		     + ", verbose: " + pv_verbose
		     );
	    }

        long threadId = Thread.currentThread().getId();
        if (LOG.isInfoEnabled()) LOG.info("ENTRY ReplayEngine thread " + threadId);
        timeStamp = timestamp;
        pit_thread = numParallel > 0 ? numParallel : 1;
        
        int pit_batch = 200;
        String pitBatch = System.getenv("TM_PIT_MUTATION_BATCH");
        if (pitBatch != null) {
          pit_batch = Integer.parseInt(pitBatch);
          if (pit_batch < 0) pit_batch = 0;
          if(LOG.isDebugEnabled()) LOG.debug("PIT mutation batch setting: " + pit_batch);
        }
        if (LOG.isInfoEnabled()) LOG.info("PIT mutation paralle threads:" + pit_thread + " batch setting: " + pit_batch);
        final int pitb = pit_batch; 

        config = HBaseConfiguration.create();
        Configuration peer_config = null;

        if (pv_peer_id == 0) {
           peer_config = config;
        }
        else {
	       try {
		      STRConfig pSTRConfig = STRConfig.getInstance(config);
              peer_config = pSTRConfig.getPeerConfiguration(pv_peer_id, true);
              if (peer_config == null) {
                 System.exit(1);
              }
//        connection = pSTRConfig.getPeerConnection(pv_peer_id);
           } catch (IOException ioe) {
              System.exit(1);
           }
       }

       readConnection = ConnectionFactory.createConnection(config);
       writeConnection = ConnectionFactory.createConnection(peer_config);
       fileSystem = FileSystem.get(config);

       admin = readConnection.getAdmin();

        try {
           if (LOG.isInfoEnabled()) LOG.info("ReplayEngine working on table " + tableName);

           final Table mtable = writeConnection.getTable(TableName.valueOf(tableName));

           // Now go through mutations files one by one for now
           String host = writeConnection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(HConstants.EMPTY_START_ROW, false).getHostname();
           
           if (LOG.isInfoEnabled()) LOG.info("ReplayEngine : " + mutationList.size() + " mutation files for server " + host + " and table: " + tableName);
           final Map<String,List<MutationMetaRecord>> groupByRegion = mapByRegion(mutationList);
           pit_thread = Math.min(groupByRegion.size(), pit_thread);

           startThreadPools(1, pit_thread); //table pool is one.
           CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool_table);

           compPool.submit(new ReplayEngineCallable() {
             public Integer call() throws Exception {
                 return doReplayParallel(groupByRegion,
                                         lobList,
                                         mtable, 
                                         config, 
                                         "" /*snapshotPath*/,
                                         pitb,
                                         true /*pv_skip_snapshot*/, 
                                         false /*pv_verbose*/,
                                         false
                                         );
             }
           });
           
           int returnValue = compPool.take().get();
           if (LOG.isInfoEnabled()) LOG.info("EXIT ReplayEngine thread " + threadId);
        }
        catch (Exception e) {
            LOG.error("ReplayEngine (mutationList) Exception occurred during Replay ", e);
            throw e;
        }
        finally {
          shutdownThreadPools(true);
        }
    }

    public void mutationReaderFile(Path readPath,
				   Configuration config,
				   Table table,
				   boolean pv_verbose,
                                   boolean xdc_replay) throws IOException 
    {
 
    // this method is invoked by the replay engine after a mutation file is included in the replay file set

      long sid;
      long cid;
      int iKV = 0;
      long iTxnProto = 0;
      int tableAttr = 0;
      boolean xdc_catchup = false;

      long lv_totalPuts = 0;
      long lv_totalDeletes = 0;

      HFile.Reader reader = null;
      try { 
	    
          if (LOG.isInfoEnabled()) LOG.info("PIT mutationReaderFile " + readPath + " start ");
	  
          reader = HFile.createReader(fileSystem, readPath, new CacheConfig(config), config);
          HFileScanner scanner = reader.getScanner(true, false);
          boolean hasKVs = scanner.seekTo(); // get to the beginning position
	  
	  while (hasKVs) {
              //KeyValue firstVal = scanner.getKeyValue();
              Cell tmVal = scanner.getKeyValue();
	      iKV++;
	      if (LOG.isTraceEnabled()) LOG.trace("PIT mutationReaderFile: read txn proto, path " + readPath + " KV " + iKV);

	      ByteArrayInputStream input = new ByteArrayInputStream(CellUtil.cloneValue(tmVal));
              TransactionMutationMsg tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
	      while (tmm != null) {
		  // if tsm.getCommitId() is NOT within the desired range, then skip this record
		  // Note. the commitID will not be in a monotonic increasing order inside the mutation files due to concurrency/delay
		  // the smallest commitId within a file will be logged into mutation meta to filter out unnecessary mutation files
                  if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: transaction id " + tmm.getTxId());
		  sid = tmm.getStartId();
		  cid = tmm.getCommitId();
                  tableAttr = tmm.getTableCDCAttr();
		  iTxnProto++;

                  if (LOG.isTraceEnabled()) LOG.trace("PIT mutation replay: transaction id " + tmm.getTxId()
                                       + " commit id " +cid + " restore time " + timeStamp + " MC tabe name " + tmm.getTableName());

                  if (! table.getName().getNameAsString().equals(tmm.getTableName())) {
                      LOG.error("PIT ZZZ error table name mismatch between replay engine and CDC record " +
                                                        table.getName().getNameAsString() + " vs " + tmm.getTableName());
                  }

                  if (xdc_replay) {
                       xdc_catchup =  (((tableAttr & SYNCHRONIZED) == SYNCHRONIZED)
                                           && ((tableAttr & XDC_DOWN) == XDC_DOWN)  );
                       if (LOG.isDebugEnabled()) LOG.debug("xdc replay: mutation info: tid " +  tmm.getTxId() + " cid " + cid +
                                            " table name " + tmm.getTableName() + " tabel attr " + tableAttr + " xdc_catchup " + xdc_catchup); 
                       if (!xdc_catchup) {
                          // Get the next mutation and continue
                          tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
                          continue;
                       }
                  }

                  if ( cid <= timeStamp )
                  {
                    List<Boolean> putOrDel = tmm.getPutOrDelList();
                    List<MutationProto> putProtos = tmm.getPutList();
                    List<MutationProto> deleteProtos = tmm.getDeleteList();
		    // the two lists are to contain the mutations to be replayed in the table level
		    // Note. Only committed mutations will be in the mutation file.
		    List<Put> puts = Collections.synchronizedList(new ArrayList<Put>());
                    List<Delete> deletes = Collections.synchronizedList(new ArrayList<Delete>());

                    int putIndex = 0;
                    int deleteIndex = 0;
		    // Note. the put/del we get from mutation proto should contain correct time order (serialized when they are added
		    //           into ts.writeOrdering, so we should be able to do a puts and then a dels
		    //           this is somewhat equivalent to how the recovery plays
 		    for (Boolean put : putOrDel) {
                        if (put) {
                          Put writePut = ProtobufUtil.toPut(putProtos.get(putIndex++));
                          puts.add(writePut);
                        }
                        else {
                          Delete writeDelete = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
                          deletes.add(writeDelete);
                        }
                        if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- "
							    + ", txnId: " + tmm.getTxId() 
							    + ", #puts: " + putIndex 
							    + ", #deletes: " + deleteIndex
							    );

			lv_totalPuts += putIndex;
			lv_totalDeletes += deleteIndex;

                        // complete current transaction's mutations    
		        // TRK-Not here mutationReplay(puts, deletes); // here is replay transaction by transaction (may just use HBase client API)
      
                    }
  		    mutationReplay(puts, deletes, table); // here is replay transaction by transaction (may just use HBase client API)
                    // print this txn's mutation context 
                    if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " has " + putIndex + " put " + deleteIndex + " delete ");   
                }
		  
                  // complete current txnMutationProto    

                 tmm  = TransactionMutationMsg.parseDelimitedFrom(input); 
	      } // more than one tsm inside a KV

                if (LOG.isTraceEnabled()) {
                        LOG.trace("PIT mutationReaderFile2,"
                                           + readPath + " complete Txn Proto with current KV "
                                           + " #total puts: " + lv_totalPuts 
                                           + " #total deletes: " + lv_totalDeletes
                                           );   
                 }

	      // has processed on KV, can invoke client put/delete call to replay
	      
              hasKVs = scanner.next();
	  } // still has KVs not processed in current reader
      } catch(CorruptHFileException ce) {
        //This exception can be expected if region server goes down
        //abruptly. However, since region server recovers, it creates a
        //new mutation Reader file. So this corrupt file can be ignored.
        LOG.warn("mutationReaderFile CorruptHFileException : " + ce );
        
      } catch(IOException e) {
          LOG.error("mutationReaderFile Exception : " + e );
          throw e;
      } finally {
          if (reader != null) {
             reader.close();
          }
      }
      if (LOG.isInfoEnabled()) LOG.info("PIT mutationReaderFile " 
					  + readPath + " complete with "
					  + " #transaction_mutations: " + iTxnProto
					  + " #total_puts: " + lv_totalPuts 
					  + " #total_deletes: " + lv_totalDeletes
					  );
    }

    public void mutationReaderFile2(Path readPath, 
				    Configuration config, 
				    Table table,
				    int batchSize,
				    boolean pv_verbose,
                                    boolean xdc_replay) throws IOException 
    {
 
    // this method is invoked by the replay engine after a mutation file is included in the replay file set

      long sid;
      long cid;
      int iKV = 0;
      long iTxnProto = 0;
      int tableAttr = 0;
      boolean xdc_catchup = false;

      long lv_totalPuts = 0;
      long lv_totalDeletes = 0;

	  HFile.Reader reader = null;
      try { 
	    
          if (LOG.isInfoEnabled()) LOG.info("PIT mutationReaderFile2 " + readPath + " start ");

      try{
          reader = HFile.createReader(fileSystem, readPath, new CacheConfig(config), config);
      }
      catch (FileNotFoundException fnfe){
         if (LOG.isInfoEnabled()) LOG.info("PIT mutationReaderFile2 file not found, ignoring ");
         return;
      }
          HFileScanner scanner = reader.getScanner(true, false);
          boolean hasKVs = scanner.seekTo(); // get to the beginning position
	  
	  while (hasKVs) {
              //KeyValue firstVal = scanner.getKeyValue();
              Cell tmVal = scanner.getKeyValue();
	      iKV++;
	      if (LOG.isTraceEnabled()) LOG.trace("PIT mutationReaderFile2: read txn proto, path " + readPath + " KV " + iKV);

	      ByteArrayInputStream input = new ByteArrayInputStream(CellUtil.cloneValue(tmVal));
              TransactionMutationMsg tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
	      while (tmm != null) {
		  // if tsm.getCommitId() is NOT within the desired range, then skip this record
		  // Note. the commitID will not be in a monotonic increasing order inside the mutation files due to concurrency/delay
		  // the smallest commitId within a file will be logged into mutation meta to filter out unnecessary mutation files
                  if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: transaction id " + tmm.getTxId()
                                                                  + " RE table name " + table.getName().getNameAsString());
		  sid = tmm.getStartId();
		  cid = tmm.getCommitId();
                  tableAttr = tmm.getTableCDCAttr();
		  iTxnProto++;

                  if (LOG.isTraceEnabled()) LOG.trace("PIT mutation replay: transaction id " + tmm.getTxId()
                                       + " commit id " +cid + " restore time " + timeStamp + " MC tabe name " + tmm.getTableName());

                  if (! table.getName().getNameAsString().equals(tmm.getTableName())) {
                      LOG.error("PIT ZZZ error table name mismatch between replay engine and CDC record " +
                                                        table.getName().getNameAsString() + " vs " + tmm.getTableName());
                  }

                  if (xdc_replay) {
                       xdc_catchup =  (((tableAttr & SYNCHRONIZED) == SYNCHRONIZED)
                                           && ((tableAttr & XDC_DOWN) == XDC_DOWN)  );
                       if (LOG.isDebugEnabled()) LOG.debug("xdc replay: mutation info: tid " +  tmm.getTxId() + " cid " + cid +
                                            " table name " + tmm.getTableName() + " tabel attr " + tableAttr + " xdc_catchup " + xdc_catchup); 
                       if (!xdc_catchup) {
                          // Get the next mutation and continue
                          tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
                          continue;
                       }
                       if (   (xdc_catchup) && (xdc_startTime_check == 1)   ) { // if this is xdc replay and do the xdc start cid range check
                          if (cid < startTime) {
                             if (LOG.isDebugEnabled()) LOG.debug("xdc replay: bypass old mutation: tid " +  tmm.getTxId() + " cid " + cid +
                                            " table name " + tmm.getTableName() + " tabel attr " + tableAttr + " xdc_catchup " + xdc_catchup +
                                            " replay range " + startTime + " to " + timeStamp + " , range check mode " + xdc_startTime_check); 
                             tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
                             continue;               
                         } // cid < replay range start time
                       }
                  }

                  if ( cid <= timeStamp )
                  {
                    List<Boolean> putOrDel = tmm.getPutOrDelList();
                    List<MutationProto> putProtos = tmm.getPutList();
                    List<MutationProto> deleteProtos = tmm.getDeleteList();
		    // the two lists are to contain the mutations to be replayed in the table level
		    // Note. Only committed mutations will be in the mutation file.
		    List<Put> puts = Collections.synchronizedList(new ArrayList<Put>());
                    List<Delete> deletes = Collections.synchronizedList(new ArrayList<Delete>());

                    int putIndex = 0;
                    int deleteIndex = 0;
                    int mIndex = 0;
                    int pIndex = 0;
                    int iBatch = 0;
		    // Note. the put/del we get from mutation proto should contain correct time order (serialized when they are added
		    //           into ts.writeOrdering, so we should be able to do a puts and then a dels
		    //           this is somewhat equivalent to how the recovery plays
 		    for (Boolean put : putOrDel) {
                        pIndex++;
                        mIndex++;
                        if (put) {
                          Put writePut = ProtobufUtil.toPut(putProtos.get(putIndex++));
                          puts.add(writePut);
                        }
                        else {
                          Delete writeDelete = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
                          deletes.add(writeDelete);
                        }

                        // complete current transaction's mutations    
		        // TRK-Not here mutationReplay(puts, deletes); // here is replay transaction by transaction (may just use HBase client API)
      
                        if (mIndex >= batchSize) { // do a client puts/deletes every batchSize mutations, defined in ms.env
  		             mutationReplay(puts, deletes, table); // here is replay transaction by transaction (may just use HBase client API)
                             // print this txn's mutation context 
                            iBatch++;
                            mIndex = 0;
                            puts.clear();
                            deletes.clear();
                            if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " is doing batch " + iBatch + " mutation " + pIndex);   
                        }  
                    } // for loop
                    
                    if ((!puts.isEmpty()) || (!deletes.isEmpty())) { // last batch
  		             mutationReplay(puts, deletes, table); // here is replay transaction by transaction (may just use HBase client API)
                             // print this txn's mutation context 
                            if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " is doing batch " + iBatch + " mutation " + pIndex);   

                    }

                    if (LOG.isTraceEnabled())
			LOG.trace("PIT mutationReaderFile2,"
				  + " TxId: " + tmm.getTxId()
				  + " startId: " + sid 
				  + " commitId: " + cid 
				  + " #puts: " + putIndex 
				  + " #deletes: " + deleteIndex
				  );   
		    lv_totalPuts += putIndex;
		    lv_totalDeletes += deleteIndex;
                  }  // complete current txnMutationProto
                  else {
                  }

                 tmm  = TransactionMutationMsg.parseDelimitedFrom(input); 
	      } // more than one tsm inside a KV

                 if (LOG.isTraceEnabled()) {
                        LOG.trace("PIT mutationReaderFile2,"
                                           + readPath + " complete Txn Proto with current KV "
                                           + " #total puts: " + lv_totalPuts 
                                           + " #total deletes: " + lv_totalDeletes
                                           );   
                 }

	      // has processed on KV, can invoke client put/delete call to replay
	      
              hasKVs = scanner.next();
	  } // still has KVs not processed in current reader
	  } catch(CorruptHFileException ce) {
	    //This exception can be expected if region server goes down
	    //abruptly. However, since region server recovers, it creates a
	    //new mutation Reader file. So this corrupt file can be ignored.
        LOG.warn("mutationReaderFile2 CorruptHFileException : " + ce );
      } catch(IOException e) {
          LOG.error("mutationReaderFile2 Exception : ", e );
          throw e;
      } finally {
          if (reader != null) {
             reader.close();
          }
      }
      if (LOG.isInfoEnabled()) LOG.info("PIT mutationReaderFile2 " 
					  + readPath + " complete with "
					  + " #transaction_mutations: " + iTxnProto
					  + " #total_puts: " + lv_totalPuts 
					  + " #total_deletes: " + lv_totalDeletes
					  );
    }
   
  public void mutationReplay(List<Put> puts, List<Delete> deletes,  Table mtable) throws IOException {
      
      // only for local instance
      // peer may require a "connection" or "peerId" argument
      // Use peer connection to get peer table (Interface Table)
      // use primitive HBase Table interface to replay committed puts or deletes in a batch way


      // peerTable is from 
      // Connection connection = ConnectionFactory.createConnection(peerConfig);or use xdc Config peer's connection
      // Table peerTable = connection.getTable(TableName from regionInfo);
      // can directly invoke table.put or table.delete with list of put or delete here to peer (i.e. the catching-up side)

      if (LOG.isTraceEnabled()) LOG.trace("MutationReplay for table " + mtable.getName().getNameAsString()
              + " #puts: " + puts.size()
			  + " #deletes: " + deletes.size()
			  );

      try {
        
       mtable.put(puts);
       mtable.delete(deletes);

      } catch(IOException e) {
          LOG.error("MutationReplay Exception : ", e);
          throw e;
      }      
  }

  public Map<String,List<MutationMetaRecord>> mapByRegion(List<MutationMetaRecord> recordList) {
      Map<String,List<MutationMetaRecord>> map = new HashMap<>();
      for (MutationMetaRecord record : recordList) {
          String region = record.getRegionName();
          List<MutationMetaRecord> list = map.get(region);
          if(null == list) {
              list = new ArrayList<>();
          }
          list.add(record);
          map.put(region, list);
      }
      return map;
  }

  public List<Path> getSortedMutationList(List<MutationMetaRecord> mutationMetaRecordList) {
      List<Path> pathList = new ArrayList<>(mutationMetaRecordList.size());
      Collections.sort(mutationMetaRecordList, new Comparator<MutationMetaRecord>() {
          @Override
          public int compare(MutationMetaRecord o1, MutationMetaRecord o2) {
              // Mutation Name Example: TRAFODION.JAVABENCH.OE_STOCK_INDEX_100-snapshot-1722382509927818-e-efd1a2a67d2d009601fdafff4d8f11fe-1642604535715
              // the sort field is '1642604535715'
              Long t1 = Long.parseLong(o1.getMutationPath().substring(o1.getMutationPath().lastIndexOf("-") + 1));
              Long t2 = Long.parseLong(o2.getMutationPath().substring(o2.getMutationPath().lastIndexOf("-") + 1));
              return Long.compare(t1, t2);
          }
      });
      for (MutationMetaRecord record : mutationMetaRecordList) {
          try {
              pathList.add(new Path(new URI(record.getMutationPath()).getPath()));
          } catch (Exception e) {
              LOG.warn("MutationPath URI error: " + record.getMutationPath());
          }
      }
      return pathList;
  }
}
