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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.hbase.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import java.util.*;

import org.apache.hadoop.hbase.pit.meta.AbstractSnapshotRecordMeta;
import org.apache.hadoop.hbase.pit.meta.SnapshotTableIndexRecord;
import org.apache.hadoop.hbase.pit.meta.SnapshotTagIndexRecord;

/**
 * This class is responsible for maintaining all metadata writes, puts or deletes, to the Trafodion snapshot table.
 *
 * @see
 * <ul>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
 * </li>
 * <li> MutationMeta
 * {@link MutationMeta}
 * </li>
 * <li> TableRecoveryGroup
 * {@link TableRecoveryGroup}
 * </li>
 * <li> RecoveryRecord
 * {@link RecoveryRecord}
 * </li>
 * </ul>
 * 
 */
public class SnapshotMeta {

   static final Log LOG = LogFactory.getLog(SnapshotMeta.class);
   private static Admin admin;
   private Configuration config;
   
   //TRAF_RESERVED_NAMESPACE5 namespace name in sql/common/ComSmallDefs.h.
   private static String SNAPSHOT_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.SNAPSHOT";
   private static final byte[] SNAPSHOT_FAMILY = Bytes.toBytes("sf");
   private static final byte[] SNAPSHOT_QUAL = Bytes.toBytes("sq");
   private static final long SNAPSHOT_LOCK_KEY = 0;
   private static final long SNAPSHOT_MUTATION_FLUSH_KEY = 1;
   private static int MetaRetryCount = 100;
   private static int MetaRetryDelay = 5000; // 5 seconds
   private static Table table;
   private Connection connection;
   private static Object adminConnLock = new Object();

   private boolean disableBlockCache;

   private int SnapshotRetryDelay;
   private int SnapshotRetryCount;

   ZooKeeperWatcher zooKeeper;
   private HBasePitZK pitZK;

   /**
    * SnapshotMeta
    * @throws Exception
    */
   public SnapshotMeta (Configuration config) throws IOException  {
      if (LOG.isTraceEnabled()) LOG.trace("Enter SnapshotMeta constructor");
      this.config = config;

      String hbaseRpcTimeout = System.getenv("HAX_BR_HBASE_RPC_TIMEOUT");
      int hbaseRpcTimeoutInt = 600000;
      if (hbaseRpcTimeout != null) {
          synchronized(adminConnLock) {
            if (hbaseRpcTimeout != null) {
                hbaseRpcTimeoutInt = Integer.parseInt(hbaseRpcTimeout.trim());
            }
          } // sync
      }

      String value = this.config.getTrimmed("hbase.rpc.timeout");
      this.config.set("hbase.rpc.timeout", Integer.toString(hbaseRpcTimeoutInt));
      if (LOG.isInfoEnabled())
         LOG.info("SnapshotMeta HAX: BackupRestore HBASE RPC Timeout, revise hbase.rpc.timeout from "
              + value + " to " + hbaseRpcTimeoutInt);

      connection = ConnectionFactory.createConnection(this.config);
      admin = connection.getAdmin();
      disableBlockCache = true;

      pitZK = new HBasePitZK(this.config);

      SnapshotRetryDelay = 5000; // 3 seconds
      SnapshotRetryCount = 60;

      int versions = 10;
      try {
         String maxVersions = System.getenv("TM_MAX_SNAPSHOT_VERSIONS");
         if (maxVersions != null){
            versions = (Integer.parseInt(maxVersions) > versions ? Integer.parseInt(maxVersions) : versions);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_MAX_SNAPSHOT_VERSIONS is not in ms.env");
      }

      HColumnDescriptor hcol = new HColumnDescriptor(SNAPSHOT_FAMILY);
      hcol.setMaxVersions(versions);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }

      int retryCount = 0;
      boolean snapshotTableExists = false;
      while (retryCount < 3) {
         try{
            snapshotTableExists = admin.isTableAvailable(TableName.valueOf(SNAPSHOT_TABLE_NAME));
            break;
         }
         catch(Exception ke){
            if (retryCount < 3){
               retryCount++;
               if (LOG.isInfoEnabled()) LOG.info("SnapshotMeta... Exception in initialization, retrying ", ke);
               try {
                  Thread.sleep(1000); // sleep one seconds or until interrupted
               }
               catch (InterruptedException e) {
                  // ignore the interruption and keep going
               }
            }
            else{
               if (LOG.isErrorEnabled()) LOG.error("SnapshotMeta... Exception in initialization ", ke);
               throw new IOException(ke);
            }
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("Snapshot Table " + SNAPSHOT_TABLE_NAME + (snapshotTableExists? " exists" : " does not exist" ));
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SNAPSHOT_TABLE_NAME));
      desc.addFamily(hcol);
      table = connection.getTable(desc.getTableName());

      if (snapshotTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + SNAPSHOT_TABLE_NAME);
            admin.createTable(desc);
         }
         catch(TableExistsException tee){
            retryCount = 0;
            boolean snapshotTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               snapshotTableEnabled = admin.isTableAvailable(TableName.valueOf(SNAPSHOT_TABLE_NAME));
               if (! snapshotTableEnabled) {
                  try {
                     Thread.sleep(2000); // sleep two seconds or until interrupted
                  }
                  catch (InterruptedException e) {
                     // ignore the interruption and keep going
                  }
               }
               else {
                  break;
               }
            }
            if (retryCount == 3){
               LOG.error("SnapshotMeta Exception while enabling " + SNAPSHOT_TABLE_NAME);
               throw new IOException("SnapshotMeta Exception while enabling " + SNAPSHOT_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("SnapshotMeta Exception while creating " + SNAPSHOT_TABLE_NAME + ": " , e);
            throw new IOException(e);
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMeta constructor()");
      return;
   }

   /**
    * initializeSnapshot
    * @param long key
    * @param String tag
    * @throws Exception
    */
   public void initializeSnapshot(final long key, final String tag, final boolean incrementalBackup,
                                  final String extendedAttributes, final String backupType, final long systemTime) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot start for key "
           + key + " tag " + tag + " extendedAttributes " + extendedAttributes + " incrementalBackup " + incrementalBackup);
      String keyString = new String(String.valueOf(key));
      int recordType = SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue();
      boolean snapshotComplete = false;
      boolean markedForDeletion = false;
      boolean firstIncremental = false;
      boolean fuzzyBackup = false;
      long completionTime = 0;

	  SnapshotMetaStartRecord tmpSmsr = new SnapshotMetaStartRecord (key, tag, snapshotComplete,
               completionTime, incrementalBackup, extendedAttributes, backupType, systemTime);

      this.putRecord(tmpSmsr);
      if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot exit for tag " + tag);
   }

   /**
    * putRecord
    * @param SnapshotMetaStartRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaStartRecord record) throws Exception {

      if (record == null){
	    IOException ioe = new IOException("putRecord SnapshotMetaStartRecord input record is null ");
	    if (LOG.isTraceEnabled())
	        LOG.trace("Error: ", ioe);
	    throw ioe;
	  }

      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for snapshot START; tag "
                       + record.getUserTag() + " record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue()){
         IOException ioe = new IOException("putRecord SnapshotMetaStartRecord found record with incorrect type " + record);
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
    		  Bytes.toBytes(String.valueOf(String.valueOf(key) + ","
                       + String.valueOf(record.getVersion()) + ","
                       + record.getRecordType()) + ","
                       + record.getUserTag() + ","
                       + String.valueOf(record.getSnapshotComplete()) + ","
                       + String.valueOf(record.getCompletionTime()) + ","
                       + String.valueOf(record.getFuzzyBackup()) + ","
                       + String.valueOf(record.getMarkedForDeletion()) + ","
                       + String.valueOf(record.getIncrementalBackup()) + ","
                       + String.valueOf(record.getFirstIncremental()) + ","
                       + record.getExtendedAttributes() + ","
                       + record.getBackupType() + ","
                       + String.valueOf(record.getStartTime()) + ","
                       + String.valueOf(record.getCompletionWallTime())));

      int retries = 0;
      boolean complete = false;
      do {     
         retries++;
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try table.put, " + p );
            table.put(p);
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord (start record) for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString
                  + " tag " + record.getUserTag() + " due to Exception " , e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try{
               locator.getRegionLocation(p.getRow(), true);
            }
            catch(IOException ioe){
               LOG.error("putRecord caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord (start record) aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * putRecord
    * @param SnapshotMetaIncrementalRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaIncrementalRecord record) throws Exception {

      if (record == null){
        IOException ioe = new IOException("putRecord SnapshotMetaIncrementalRecord input record is null ");
        if (LOG.isTraceEnabled())
           LOG.trace("Error: ", ioe);
        throw ioe;
      }
      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for INCREMENTAL snapshot with tag "
               + record.getUserTag() + " record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()){
          IOException ioe = new IOException("putRecord SnapshotMetaIncrementalRecord found record with incorrect type " + record);
          if (LOG.isTraceEnabled())
             LOG.trace("Error: ", ioe);
          throw ioe;
      }

      StringBuilder lobStringBuilder = new StringBuilder();
      List<String> lobStringList = new ArrayList<String>();
      Set<Long> lobSnaps = record.getLobSnapshots();
      Iterator<Long> lobIt = lobSnaps.iterator();
      int current = 0;
      int count = record.getNumLobs();

      while (lobIt.hasNext() && current < count) {
        Long ls = lobIt.next();
        if (current != 0){
          lobStringBuilder.append(",");
        }
        lobStringList.add(String.valueOf(ls));
        lobStringBuilder.append(String.valueOf(ls));
        current++;
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
                Bytes.toBytes(String.valueOf(key) + ","
                       + String.valueOf(record.getVersion()) + ","
                       + String.valueOf(record.getRecordType()) + ","
                       + String.valueOf(record.getRootSnapshotId()) + ","
                       + String.valueOf(record.getParentSnapshotId()) + ","
                       + String.valueOf(record.getChildSnapshotId()) + ","
                       + String.valueOf(record.getPriorMutationStartTime()) + ","
                       + record.getTableName() + ","
                       + record.getUserTag() + ","
                       + record.getSnapshotName() + ","
                       + record.getSnapshotPath() + ","
                       + String.valueOf(record.getInLocalFS()) + ","
                       + String.valueOf(record.getExcluded()) + ","
                       + String.valueOf(record.getArchived()) + ","
                       + record.getArchivePath() + ","
                       + record.getMetaStatements() + ","
                       + record.getNumLobs() + ","
                       + lobStringBuilder.toString()));

      int retries = 0;
      boolean complete = false;

      // insert a index record used to speed up query table record
      SnapshotTableIndexRecord tableIndexRecord = getSnapshotTableIndex(record.getTableName());
      if (tableIndexRecord != null) {
         if (LOG.isTraceEnabled())
            LOG.trace("SnapshotMetaIncrementalRecord-put cache:"+tableIndexRecord);
         tableIndexRecord.add(key, SnapshotTableIndexRecord.INCR_TYPE, record.getUserTag());
      } else {
         tableIndexRecord = new SnapshotTableIndexRecord(
                 record.getTableName(),
                 record.getVersion(),
                 null,
                 String.valueOf(key)
         );
         if (LOG.isTraceEnabled())
            LOG.trace("SnapshotMetaIncrementalRecord-put new:"+tableIndexRecord);

      }
      Put indexPut = getRecordMetaPut(tableIndexRecord);

      do {
         retries++;
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try table.put, " + p );
            if(null == indexPut){
               table.put(p);
            } else {
               List<Put> puts = new ArrayList<>(2);
               puts.add(p);
               puts.add(indexPut);
               table.put(puts);
            }
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord for key: " + keyString);
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString + " due to Exception " , e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try{
               locator.getRegionLocation(p.getRow(), true);
            }
            catch(IOException ioe){
               LOG.error("putRecord caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * putRecord
    * @param SnapshotMetaRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaRecord record) throws Exception {

      if (record == null){
        IOException ioe = new IOException("putRecord SnapshotMetaRecord input record is null ");
        if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
        throw ioe;
      }

      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()){
          IOException ioe = new IOException("putRecord SnapshotMetaRecord found record with incorrect type " + record);
          if (LOG.isTraceEnabled())
             LOG.trace("Error: ", ioe);
          throw ioe;
      }

      StringBuilder lobStringBuilder = new StringBuilder();
      List<String> lobStringList = new ArrayList<String>();
      Set<Long> lobSnaps = record.getLobSnapshots();
      Iterator<Long> lobIt = lobSnaps.iterator();
      int current = 0;
      int count = record.getNumLobs();

      while (lobIt.hasNext() && current < count) {
        Long ls = lobIt.next();
        if (current != 0){
          lobStringBuilder.append(",");
        }
        lobStringList.add(String.valueOf(ls));
        lobStringBuilder.append(String.valueOf(ls));
        current++;
      }

      StringBuilder snapshotStringBuilder = new StringBuilder();
      List<String> snapshotStringList = new ArrayList<String>();
      Set<Long> dependentSnaps = record.getDependentSnapshots();
      Iterator<Long> it = dependentSnaps.iterator();
      boolean first = true;
      while (it.hasNext()) {
         Long ss = it.next();
//         if (LOG.isTraceEnabled()) LOG.trace("putRecord dependentSnapshot " + ss + " for tag "
//                  + record.getUserTag());
         if (first){
            first = false;
         }
         else{
            snapshotStringBuilder.append(",");
         }
         snapshotStringList.add(String.valueOf(ss));
         snapshotStringBuilder.append(String.valueOf(ss));
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
    		  Bytes.toBytes(String.valueOf(key) + ","
                       + String.valueOf(record.getVersion()) + ","
                       + String.valueOf(record.getRecordType()) + ","
                       + String.valueOf(record.getSupersedingSnapshotId()) + ","
                       + String.valueOf(record.getRestoredSnapshot()) + ","
                       + String.valueOf(record.getRestoreStartTime()) + ","
                       + String.valueOf(record.getHiatusTime()) + ","
                       + record.getTableName() + ","
                       + record.getUserTag() + ","
                       + record.getSnapshotName() + ","
                       + record.getSnapshotPath() + ","
                       + String.valueOf(record.getUserGenerated()) + ","
                       + String.valueOf(record.getIncrementalTable()) + ","
                       + String.valueOf(record.getSqlMetaData()) + ","
                       + String.valueOf(record.getInLocalFS()) + ","
                       + String.valueOf(record.getExcluded()) + ","
                       + String.valueOf(record.getMutationStartTime()) + ","
                       + record.getArchivePath() + ","
                       + record.getMetaStatements() + ","
                       + record.getNumLobs() + ","
                       + lobStringBuilder.toString() + ","
                       + snapshotStringBuilder.toString()));

      int retries = 0;
      boolean complete = false;
      // insert a index record used to speed up query table record
      SnapshotTableIndexRecord tableIndexRecord = getSnapshotTableIndex(record.getTableName());
      if (tableIndexRecord != null) {
         if (LOG.isTraceEnabled())
            LOG.trace("SnapshotMetaRecord-put cache:"+tableIndexRecord);

         tableIndexRecord.add(key, SnapshotTableIndexRecord.NORMAL_TYPE, record.getUserTag());
      } else {
         tableIndexRecord = new SnapshotTableIndexRecord(
                 record.getTableName(),
                 record.getVersion(),
                 String.valueOf(key)
                 ,null

         );
         if (LOG.isTraceEnabled())
            LOG.trace("SnapshotMetaRecord-put new:"+tableIndexRecord);
      }
      Put indexPut = getRecordMetaPut(tableIndexRecord);

      do {     
         retries++;
         try {
            if(null == indexPut){
               table.put(p);
            } else {
               List<Put> puts = new ArrayList<>(2);
               puts.add(p);
               puts.add(indexPut);
               table.put(puts);
            }
            complete = true;

            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString + " due to Exception " , e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try{
               locator.getRegionLocation(p.getRow(), true);
            }
            catch(IOException ioe){
               LOG.error("putRecord caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * putRecord
    * @param SnapshotMetaLockRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaLockRecord record) throws Exception {

      if (record == null){
	    IOException ioe = new IOException("putRecord SnapshotMetaLockRecord input record is null ");
	    if (LOG.isTraceEnabled())
	        LOG.trace("Error: ", ioe);
	    throw ioe;
	  }
      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for snapshot LOCK; tag "
                       + record.getUserTag() + " record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()){
         IOException ioe = new IOException("putRecord SnapshotMetaLockRecord found record with incorrect type " + record);
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
              Bytes.toBytes(String.valueOf(String.valueOf(key) + ","
                       + String.valueOf(record.getVersion()) + ","
                       + record.getRecordType()) + ","
                       + record.getUserTag() + ","
                       + String.valueOf(record.getLockHeld())));

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         try {
            table.put(p);
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord (lock record) for key: " + keyString);
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString
                  + " tag " + record.getUserTag() + " due to Exception " , e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try{
               locator.getRegionLocation(p.getRow(), true);
            }
            catch(IOException ioe){
               LOG.error("putRecord caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord (lock record) aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * getLockRecord
    * @return SnapshotMetaLockRecord
    * @throws Exception
    */
   public SnapshotMetaLockRecord getLockRecord() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getLockRecord start ");
      SnapshotMetaLockRecord record;

      try {
         Get g = new Get(Bytes.toBytes(SNAPSHOT_LOCK_KEY));
         try {
            Result r = table.get(g);
            if (r == null){
               LOG.warn("getLockRecord unable to retrieve SnapshotMetaLockRecord for key " + SNAPSHOT_LOCK_KEY);
               return null;
            }
            if (r.isEmpty()){
               LOG.warn("getLockRecord retrieved empty Result for key " + SNAPSHOT_LOCK_KEY);
               return null;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            // skip the key string
            st.nextToken();
            String versionString = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getLockRecord() Exception ", ioe);
               throw ioe;
            }

            String recordTypeString  = st.nextToken();
            if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()){
               LOG.warn("getLockRecord did not find a SnapshotMetaLockRecord. Type found " + recordTypeString);
               return null;
            }
            // We found a full snapshot
            String userTagString     = st.nextToken();
            String lockHeldString    = st.nextToken();
            record = new SnapshotMetaLockRecord(SNAPSHOT_LOCK_KEY, Integer.parseInt(versionString), userTagString,
                             lockHeldString.contains("true"));
         }
         catch (Exception e1){
            LOG.error("getLockRecord() Exception " , e1);
            throw e1;
         }
      }
      catch (Exception e2) {
         LOG.error("getLockRecord() Exception2 " , e2);
         throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getLockRecord end; returning " + record);
      return record;
   }

   /**
    * putRecord
    * @param SnapshotMutationFlushRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMutationFlushRecord record) throws Exception {

      if (record == null){
	    IOException ioe = new IOException("putRecord SnapshotMutationFlushRecord input record is null ");
	    if (LOG.isTraceEnabled())
	        LOG.trace("Error: ", ioe);
	    throw ioe;
	  }
      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for snapshot MUTATION FLUSH record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_MUTATION_FLUSH_RECORD.getValue()){
         IOException ioe = new IOException("putRecord SnapshotMutationFlushRecord found record with incorrect type " + record);
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      StringBuilder builder = new StringBuilder();
      Iterator<String> it = record.getTableList().iterator();
      while (it.hasNext()){
         String currTab = it.next();
         builder.append(",");
         builder.append(currTab);
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
              Bytes.toBytes(String.valueOf(String.valueOf(key) + ","
                       + String.valueOf(record.getVersion()) + ","
                       + String.valueOf(record.getRecordType()) + ","
                       + String.valueOf(record.getInterval()) + ","
                       + String.valueOf(record.getCommitId())
                       + builder.toString())));

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         try {
            table.put(p);
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord (mutation flush record) for key: " + keyString);
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord (mutation flush record) for key: " + keyString
                  + " due to Exception " , e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try{
               locator.getRegionLocation(p.getRow(), true);
            }
            catch(IOException ioe){
               LOG.error("putRecord (mutation flush record) caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord (mutation flush record) aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * getFlushRecord
    * @return SnapshotMutationFlushRecord
    * @throws Exception
    */
   public SnapshotMutationFlushRecord getFlushRecord() throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getFlushRecord start ");
      SnapshotMutationFlushRecord record = null;;

      try {
         Get g = new Get(Bytes.toBytes(SNAPSHOT_MUTATION_FLUSH_KEY));
         try {
            Result r = table.get(g);
            if (r == null){
               LOG.warn("getFlushRecord unable to retrieve SnapshotMutationFlushRecord for key " + SNAPSHOT_MUTATION_FLUSH_KEY);
               return null;
            }
            if (r.isEmpty()){
               LOG.warn("getFlushRecord retrieved empty Result for key " + SNAPSHOT_MUTATION_FLUSH_KEY);
               return null;
            }
            List<Cell> list = r.getColumnCells(SNAPSHOT_FAMILY, SNAPSHOT_QUAL);  // returns all versions of this column
            int i = 0;
            for (Cell cell : list) {
               i++;
               StringTokenizer st =
                         new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");

               if (st.hasMoreElements()) {
                  // skip the key string
                  st.nextToken();
                  String versionString = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                           + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getFlushRecord() Exception ", ioe);
                     throw ioe;
                  }
                  String recordTypeString  = st.nextToken();
                  if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_MUTATION_FLUSH_RECORD.getValue()){
                     LOG.warn("getFlushRecord did not find a SnapshotMutationFlushRecord. Type found " + recordTypeString);
                     return null;
                  }
                  String intervalString     = st.nextToken();
                  String commitIdString     = st.nextToken();
                  ArrayList<String> tableList = new ArrayList<String>();
                  while (st.hasMoreElements()) {
                     String tableNameToken = st.nextElement().toString();
                     tableList.add(tableNameToken);
                  }
                  if (LOG.isTraceEnabled()) LOG.trace("Parsing record for interval (" + intervalString + ")");
                  record = new SnapshotMutationFlushRecord(SNAPSHOT_MUTATION_FLUSH_KEY, Integer.parseInt(versionString),
                            Integer.parseInt(intervalString), Long.parseLong(commitIdString), tableList);

               }
               else {
                  if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for " + i);
               }
            }// for (Cell cell : list)
         }
         catch (Exception e1){
            LOG.error("getFlushRecord() Exception " , e1);
            throw e1;
         }
      }
      catch (IOException e2) {
         LOG.error("getFlushRecord() Exception2 " , e2);
         throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getFlushRecord end; returning " + record);
      return record;
   }

   /**
    * getNthFlushRecord
    * @return SnapshotMutationFlushRecord
    * @throws Exception
    */
   public SnapshotMutationFlushRecord getNthFlushRecord(int n) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("getNthFlushRecord start for n " + n);
      SnapshotMutationFlushRecord record = null;

      try {
         Get g = new Get(Bytes.toBytes(SNAPSHOT_MUTATION_FLUSH_KEY));
         g.setMaxVersions(n + 1);  // will return last n+1 versions of row just in case
         try {
            Result r = table.get(g);
            if (r == null){
               LOG.warn("getNthFlushRecord unable to retrieve SnapshotMutationFlushRecord for key " + SNAPSHOT_MUTATION_FLUSH_KEY);
               return null;
            }
            if (r.isEmpty()){
               LOG.warn("getNthFlushRecord retrieved empty Result for key " + SNAPSHOT_MUTATION_FLUSH_KEY);
               return null;
            }
            List<Cell> list = r.getColumnCells(SNAPSHOT_FAMILY, SNAPSHOT_QUAL);  // returns all versions of this column
            int i = 0;
            for (Cell cell : list) {
               i++;
               StringTokenizer st =
                        new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");

               if (st.hasMoreElements()) {
                   // skip the key string
                   st.nextToken();
                   String versionString = st.nextToken();
                   int version = Integer.parseInt(versionString);
                   if (version != SnapshotMetaRecordType.getVersion()){
                      IOException ioe = new IOException("Unexpected record version found: "
                          + version + " expected: " + SnapshotMetaRecordType.getVersion());
                      LOG.error("getNthFlushRecord() Exception ", ioe);
                      throw ioe;
                   }
                   String recordTypeString  = st.nextToken();
                   if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_MUTATION_FLUSH_RECORD.getValue()){
                      LOG.warn("getNthFlushRecord did not find a SnapshotMutationFlushRecord. Type found " + recordTypeString);
                      return null;
                   }
                   String intervalString     = st.nextToken();
                   String commitIdString     = st.nextToken();
                   ArrayList<String> tableList = new ArrayList<String>();
                   while (st.hasMoreElements()) {
                      String tableNameToken = st.nextElement().toString();
                      tableList.add(tableNameToken);
                   }
                   if (LOG.isTraceEnabled()) LOG.trace("Parsing record for interval (" + intervalString + ")");
                   if ( i < n ){
                      if (LOG.isTraceEnabled()) LOG.trace("Skipping record " + i + " of " + n + " for flush record");
                      continue;
                   }
                   record = new SnapshotMutationFlushRecord(SNAPSHOT_MUTATION_FLUSH_KEY, Integer.parseInt(versionString),
                           Integer.parseInt(intervalString), Long.parseLong(commitIdString), tableList);

                }
                else {
                   if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for " + i);
                }
            }// for (Cell cell : list)
         }
         catch (Exception e1){
            LOG.error("getNthFlushRecord() Exception " , e1);
            throw new IOException(e1);
         }
      }
      catch (Exception e2) {
         LOG.error("getNthFlushRecord() Exception2 " , e2);
         throw new IOException(e2);
      }

      if (LOG.isTraceEnabled()) LOG.trace("getNthFlushRecord end; returning " + (record == null ? "null" : record));
      return record;
   }

   /**
    * isDropable
    * @return boolean
    * @throws Exception
    */
   public boolean isDropable(String tag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("isDropable start for tag " + tag);
      boolean dropable = true;
      List<SnapshotMetaRecord> snapshotList = null;
      StringBuilder backupType = new StringBuilder ();

      snapshotList = getAdjustedSnapshotSet(tag, backupType);
      for (SnapshotMetaRecord mr :  snapshotList) {
         if (mr.getUserTag().equals(tag)){
            SnapshotMetaRecord currRec = getCurrentSnapshotRecord(mr.getTableName());
            if (mr.getKey() == currRec.getKey()){
               if (LOG.isTraceEnabled()) LOG.trace("isDropable tag " + tag
                     + " is not droppable ");
               dropable = false;
            }
         }
      }
      if (LOG.isTraceEnabled()) LOG.trace("isDropable end; returning " + dropable);
      return dropable;
   }

   /**
    * lock
    * @param userTag
    * @throws Exception
    */
   public boolean lock(final String userTag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("lock start for userTag " + userTag);
      byte[] lock = Bytes.toBytes("LOCKED");
      SnapshotMetaLockRecord record;
      boolean locked = false;
      int retries = -1;

      do {
         try {
            retries++;
            pitZK.createSnapshotMetaLockNode(lock);
            locked = true;
         }
         catch (Exception e) {
            if (LOG.isInfoEnabled()) LOG.info("lock() Exception creating SnapshotMetaLockNode ", e);
            try {
               Thread.sleep(MetaRetryDelay); // 5 second default
            } catch (InterruptedException ie) {
            }
         }
      } while (! locked && retries < MetaRetryCount);
      if (retries == MetaRetryCount){
         IOException ioe = new IOException("Unable to create SnapshotMetaLockNode");
         LOG.error("lockRecord failed due to excessive retries on SnapshotMetaLockNode for tag: "
                 + userTag + " due to ", ioe);
         throw ioe;
      }

      retries = -1;
      locked = false;
      do {
         try {
            retries++;
            record = getLockRecord();
         }
         catch (Exception e) {
            LOG.error("lock() Exception " , e);
            pitZK.deleteSnapshotMetaLockNode();
            throw e;
         }
         if (record == null){
            record = new SnapshotMetaLockRecord(SNAPSHOT_LOCK_KEY, SnapshotMetaRecordType.getVersion(),
                        userTag, true /* lockHeld */);
            try{
               if (LOG.isTraceEnabled()) LOG.trace("lockRecord not found; writing initial record " + record);
               putRecord(record);
            }
            catch(Exception e2){
               LOG.error("lock() Exception from putRecord" , e2);
               pitZK.deleteSnapshotMetaLockNode();
               throw e2;
            }
            locked = true;
            break;
         }
         if (record.getLockHeld()){
            LOG.error("lockRecord found existing lock for tag " + record.getUserTag());
            pitZK.deleteSnapshotMetaLockNode();
            IOException ioe = new IOException("Unable to write to Snapshot table");
            throw ioe;
         }
         else {
            // The lock is not currently held, let's lock it
            record.setLockHeld(true);
            record.setUserTag(userTag);
            if (LOG.isTraceEnabled()) LOG.trace("lockRecord locking record " + record);
            try{
               putRecord(record);
               locked = true;
            }
            catch(Exception e3){
                LOG.error("lock() Exception from putRecord after setting locked " , e3);
                pitZK.deleteSnapshotMetaLockNode();
                throw e3;
            }
         }
      } while (! locked && retries < MetaRetryCount);

      if (LOG.isTraceEnabled()) LOG.trace("lock end; returning " + locked);
      return locked;
   }

   /**
    * unlock
    * @param userTag
    * @throws Exception
    */
   public boolean unlock(final String userTag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("unlock start for userTag " + userTag);
      SnapshotMetaLockRecord record;
      boolean unlocked = false;
      int retries = -1;

      do {
         try {
            retries++;
            record = getLockRecord();
         }
         catch (Exception e) {
            LOG.error("unlock() Exception " , e);
            throw e;
         }
         if (record == null){
            record = new SnapshotMetaLockRecord(SNAPSHOT_LOCK_KEY, SnapshotMetaRecordType.getVersion(),
                      userTag, false /* lockHeld */);
            try{
               if (LOG.isTraceEnabled()) LOG.trace("unlock - lockRecord not found; writing initial record " + record);
               putRecord(record);
            }
            catch(Exception e2){
               LOG.error("unlock() Exception from putRecord" , e2);
               throw e2;
            }
            unlocked = true;
            break;
         }
         else if (record.getLockHeld()){
            // The lock is currently held
            if (record.getUserTag().compareTo(userTag) != 0){
               // Not our lock
               String msg = new String("unlock() locked record tag " + record.getUserTag() + " is not userTag " + userTag);
               LOG.error(msg);
               throw new IOException("msg");
            }
            // Our lock; let's unlock it
            record.setLockHeld(false);
            record.setUserTag(userTag);
            if (LOG.isTraceEnabled()) LOG.trace("unlocking record " + record);
            try{
               putRecord(record);
               unlocked = true;
            }
            catch(Exception e3){
               LOG.error("unlock() Exception from putRecord after setting unlocked " , e3);
               throw e3;
            }
         }
         else {
            if (LOG.isTraceEnabled()) LOG.trace("unlockRecord found existing unlocked record for tag "
                     + record.getUserTag());
            unlocked = true;
         }
      } while (! unlocked && retries < MetaRetryCount);

      if (LOG.isTraceEnabled()) LOG.trace("unlock end; returning " + unlocked);

      if (unlocked) {
         MutationMeta.mergeShadowRecords();
         pitZK.deleteSnapshotMetaLockNode();
      }

      return unlocked;
   }

   /**
    * cleanupLock
    * @param userTag
    * @throws Exception
    */
   public boolean cleanupLock(final String userTag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("cleanupLock start for userTag " + userTag);
      SnapshotMetaLockRecord record;
      boolean unlocked = false;
      int retries = -1;

      do {
         try {
            retries++;
            record = getLockRecord();
         }
         catch (Exception e) {
            LOG.error("cleanupLock() Exception " , e);
            throw e;
         }
         if (record == null){
            record = new SnapshotMetaLockRecord(SNAPSHOT_LOCK_KEY, SnapshotMetaRecordType.getVersion(),
                      userTag, false /* lockHeld */);
            try{
               if (LOG.isTraceEnabled()) LOG.trace("cleanupLock - lockRecord not found; writing initial record " + record);
               putRecord(record);
            }
            catch(Exception e2){
               LOG.error("cleanupLock() Exception from putRecord" , e2);
               throw e2;
            }
            unlocked = true;
            break;
         }
         else if (record.getLockHeld()){
            // The lock is currently held
            if (record.getUserTag().compareTo(userTag) != 0){
               // Not our lock
               String msg = new String("cleanupLock() locked record tag " + record.getUserTag() + " is not userTag " + userTag);
               LOG.error(msg);
               throw new IOException("msg");
            }
            // Our lock; let's unlock it
            record.setLockHeld(false);
            record.setUserTag(userTag);
            if (LOG.isTraceEnabled()) LOG.trace("cleanupLock record " + record);
            try{
               putRecord(record);
               unlocked = true;
            }
            catch(Exception e3){
               LOG.error("cleanupLock() Exception from putRecord after setting unlocked " , e3);
            }
         }
         else {
            if (LOG.isTraceEnabled()) LOG.trace("cleanupLock found existing unlocked record for tag "
                     + record.getUserTag());
            unlocked = true;
         }
      } while (! unlocked && retries < MetaRetryCount);

      if (unlocked) {
         MutationMeta.mergeShadowRecords();
         pitZK.cleanupSnapshotMetaLockNode();
      }
      if (LOG.isTraceEnabled()) LOG.trace("cleanupLock end; returning " + unlocked);

      return unlocked;
   }

   /**
    * getSnapshotRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaRecord getSnapshotRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotRecord start for key " + key);
      SnapshotMetaRecord record;

      try {
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            if (r == null){
               LOG.error("getSnapshotRecord unable to retrieve SnapshotMetaRecord for key " + key);
               return null;
            }
            if (r.isEmpty()){
               LOG.error("getSnapshotRecord retrieved empty Result for key " + key);
               return null;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            // skip the key string
            st.nextToken();
            String versionString = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getSnapshotRecord exception ", ioe);
               throw ioe;
            }

            String recordTypeString  = st.nextToken();
            if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()){
               LOG.error("getSnapshotRecord did not find a SnapshotMetaRecord for key "
                   + key + " Type found " + recordTypeString);
               return null;
            }
            String supersedingSnapshotString = st.nextToken();
            String restoredSnapshotString = st.nextToken();
            String restoreStartTimeString = st.nextToken();
            String hiatusTimeString = st.nextToken();
            String tableNameString    = st.nextToken();
            String userTagString      = st.nextToken();
            String snapshotNameString = st.nextToken();
            String snapshotPathString = st.nextToken();
            String userGeneratedString = st.nextToken();
            String incrementalTableString = st.nextToken();
            String sqlMetaDataString  = st.nextToken();
            String inLocalFsString    = st.nextToken();
            String excludedString     = st.nextToken();
            String mutationStartTimeString = st.nextToken();
            long mutationStartTime;
            // This is to cover for metadata migration for an unused parameter.
            if (mutationStartTimeString.contains("false")){
               mutationStartTime = 0L;
            }
            else{
               mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
            }
            String archivePathString  = st.nextToken();
            String mutationStatements = st.nextToken();
            String numLobsString      = st.nextToken();
            int numLobs = Integer.parseInt(numLobsString);
            Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
            String lobSnapshotString = null;
            try {
              for (int i = 0; i < numLobs; i++) {
                 lobSnapshotString = st.nextElement().toString();
                 lobSnapshots.add(Long.parseLong(lobSnapshotString));
              }
            } catch (Exception e) {
               LOG.error("getSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e );
            }

            Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
            String dependentSnapshotString = null;
            try {
               while (st.hasMoreElements()) {
                  dependentSnapshotString = st.nextElement().toString();
                  depSnapshots.add(Long.parseLong(dependentSnapshotString));
               }
            } catch (Exception e) {
               LOG.error("getSnapshotRecord exception parsing dependent snapshots " + dependentSnapshotString, e );
            }

            long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
            long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
            long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
            long hiatusTime = Long.parseLong(hiatusTimeString, 10);

//            if (LOG.isTraceEnabled()) LOG.trace("snapshotKey: " + key
//                    + " versionString: " + versionString
//                    + " recordTypeString: " + recordTypeString
//                    + " supersedingSnapshot: " + supersedingSnapshotString
//                    + " restoredSnapshotString: " + restoredSnapshotString
//                    + " restoreStartTimeString: " + restoreStartTimeString
//                    + " hiatusTimeString: " + hiatusTimeString
//            		+ " tableName: " + tableNameString
//            		+ " userTag: " + userTagString
//                    + " snapshotName: " + snapshotNameString
//            		+ " snapshotPath: " + snapshotPathString
//                  + " userGenerated: " + userGeneratedString
//                  + " incrementalTable: " + incrementalTableString
//                  + " sqlMetaData: " + sqlMetaDataString
//                    + " inLocalFS: " + inLocalFsString
//                   + " excluded: " + excludedString
//            		+ " mutationStartTime: " + mutationStartTime
//                    + " archivePath: " + archivePathString
//                    + " mutationStatements: " + mutationStatements
//                    + " numLobs: " + numLobs
//                    + " lobSnapshots: " + lobSnapshots);
//                    + " dependentSnapshots: " + depSnapshots);

            record = new SnapshotMetaRecord(key, Integer.parseInt(versionString), supersedingSnapshot,
                    restoredSnapshot, restoreStartTime, hiatusTime, tableNameString,
                    userTagString, snapshotNameString, snapshotPathString,
                    userGeneratedString.contains("true"),
                    incrementalTableString.contains("true"),
                    sqlMetaDataString.contains("true"),
                    inLocalFsString.contains("true"),
                    excludedString.contains("true"),
                    mutationStartTime,
                    archivePathString, mutationStatements,
                    numLobs, lobSnapshots, depSnapshots);
         }
         catch (Exception e1){
             LOG.error("getSnapshotRecord Exception " , e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getSnapshotRecord Exception2 " , e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotRecord end; returning " + record);
      return record;
   }

   /**
    * createSnapshotRecords
    * @param long key
    * @throws Exception
    */
   public ArrayList<SnapshotMetaRecord> createSnapshotRecords(final long key, final ArrayList<String> tableList, final String tag) throws IOException{
      if (LOG.isTraceEnabled()) LOG.trace("createSnapshotRecords start for " + tableList.size() + " tables and tag " + tag);
      ArrayList<SnapshotMetaRecord> records = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record;
      Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
      Set<Long> dependentSnapshots = Collections.synchronizedSet(new HashSet<Long>());
      dependentSnapshots.add(-1L);

      for (String tableName : tableList){
         String snapshotName = new String(tableName.replace(":", "_") + "_snapshot");
         String snapshotPath = SnapshotMetaRecord.createPath(snapshotName);

         record = new SnapshotMetaRecord(key, SnapshotMetaRecordType.getVersion(),
                  -1L /* supercedingSnapshot */,
                  -1L /* restoredSnapshot */,
                  -1L /* restoreStartTime */,
                  -1L /* hiatusTime */,
                  tableName,
                  tag,
                  snapshotName, snapshotPath,
                  false /* userGenerated */,
                  false /* incrementalTable */,
                  false /* sqlMetaData */,
                  true /* inLocalFs */,
                  false /* excluded */,
                  -1L /* mutationStartTime */,
                  "DefaultPath" /* archivePathString */,
                  "EmptyStatements" /* Meta Statements */,
                  lobSnapshots.size() /*numLobs */,
                  lobSnapshots,
                  dependentSnapshots);

         records.add(record);
      }

      if (LOG.isTraceEnabled()) LOG.trace("createSnapshotRecords end; returning " + records.size() + " records");
      return records;
   }

   /**
    * getIncrementalSnapshotRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaIncrementalRecord getIncrementalSnapshotRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getIncrementalSnapshotRecord start for key " + key);
      SnapshotMetaIncrementalRecord record;

      try {
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            if (r == null){
               IOException ioe = new IOException("StackTrace");
               LOG.error("getIncrementalSnapshotRecord unable to retrieve SnapshotMetaIncrementalRecord for key " + key + " ", ioe);
               return null;
            }
            if (r.isEmpty()){
               IOException ioe2 = new IOException("StackTrace");
               LOG.error("getIncrementalSnapshotRecord retrieved empty Result for key " + key + " ", ioe2);
               return null;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            // skip the key string
            st.nextToken();
            String versionString = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getIncrementalSnapshotRecord exception ", ioe);
               throw ioe;
            }

            String recordTypeString  = st.nextToken();
            if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()){
               LOG.error("getIncrementalSnapshotRecord did not find a SnapshotMetaRecord for key "
                   + key + " Type found " + recordTypeString);
               return null;
            }

            String rootSnapshotString = st.nextToken();
            long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
            String parentSnapshotString = st.nextToken();
            String childSnapshotString = st.nextToken();
            String priorMutationStartTimeString = st.nextToken();
            String tableNameString    = st.nextToken();
            String userTagString      = st.nextToken();
            String snapshotNameString = st.nextToken();
            String snapshotPathString = st.nextToken();
            String inLocalFsString    = st.nextToken();
            String excludedString    = st.nextToken();
            String archivedString     = st.nextToken();
            String archivePathString  = st.nextToken();
            String mutationStatements = st.nextToken();
            String numLobsString = st.nextToken();
            int numLobs = Integer.parseInt(numLobsString);
            Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
            String lobSnapshotString = null;
            try {
              for (int i = 0; i < numLobs; i++) {
                lobSnapshotString = st.nextElement().toString();
                lobSnapshots.add(Long.parseLong(lobSnapshotString));
              }
            } catch (Exception e) {
               LOG.error("getIncrementalSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e );
            }

            long parentSnapshot = Long.parseLong(parentSnapshotString, 10);
            long childSnapshot = Long.parseLong(childSnapshotString, 10);
            long priorMutationStartTime = Long.parseLong(priorMutationStartTimeString, 10);

            record = new SnapshotMetaIncrementalRecord(key,
                     version, rootSnapshot, parentSnapshot,
                     childSnapshot, priorMutationStartTime,
                     tableNameString, userTagString,
                     snapshotNameString, snapshotPathString,
                     inLocalFsString.contains("true"),
                     excludedString.contains("true"),
                     archivedString.contains("true"),
                     archivePathString, mutationStatements,
                     numLobs, lobSnapshots);

         }
         catch (Exception e1){
             LOG.error("getIncrementalSnapshotRecord Exception " , e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getIncrementalSnapshotRecord Exception2 " , e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getIncrementalSnapshotRecord end; returning " + record);
      return record;
   }

   /**
    * getSnapshotStartRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getSnapshotStartRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotStartRecord start for key " + key);
      SnapshotMetaStartRecord record;

      try {
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            if (r == null){
               LOG.error("getSnapshotStartRecord unable to retrieve SnapshotMetaStartRecord for key " + key);
               return null;
            }
            if (r.isEmpty()){
               LOG.error("getSnapshotStartRecord retrieved empty Result for key " + key);
               return null;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            // skip the key string
            st.nextToken();
            String versionString = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getSnapshotStartRecord exception ", ioe);
               throw ioe;
            }

            String recordTypeString  = st.nextToken();
            if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue()){
               LOG.error("getSnapshotStartRecord did not find a SnapshotMetaStartRecord for key "
                   + key + " Type found " + recordTypeString);
               return null;
            }
            // We found a full snapshot
            String userTagString           = st.nextToken();
            String snapshotCompleteString  = st.nextToken();
            String completionTimeString    = st.nextToken();
            String fuzzyBackupString       = st.nextToken();
            String markedForDeletionString = st.nextToken();
            String incrementalBackupString = st.nextToken();
            String firstIncrementalString  = st.nextToken();
            String extendedAttributes      = st.nextToken();
            String backupType              = st.nextToken();
            String timeString              = st.nextToken();
            String completionWallTimeString = null;
            if(st.hasMoreTokens() == true)
              completionWallTimeString = st.nextToken();

            record = new SnapshotMetaStartRecord(key, Integer.parseInt(versionString), userTagString,
                             snapshotCompleteString.contains("true"),
                             Long.parseLong(completionTimeString, 10),
                             fuzzyBackupString.contains("true"),
                             markedForDeletionString.contains("true"),
                             incrementalBackupString.contains("true"),
                             firstIncrementalString.contains("true"),
                             extendedAttributes,
                             backupType,
                             Long.parseLong(timeString, 10));
           if( completionWallTimeString != null )
              record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );

         }
         catch (Exception e1){
             LOG.error("getSnapshotStartRecord Exception " , e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getSnapshotStartRecord Exception2 " , e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotStartRecord end; returning " + record);
      return record;
   }

   /**
    * getPriorStartRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getPriorStartRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorStartRecord start for key " + key);
      SnapshotMetaStartRecord record = null;

      try {
         //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups
         SubstringComparator comp1 = new SubstringComparator("_SAVE_");
         SubstringComparator comp2 = new SubstringComparator("_BRC_");
         SingleColumnValueFilter filter1 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp1);
         SingleColumnValueFilter filter2 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp2);
         FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
         filterList.addFilter(filter1);
         filterList.addFilter(filter2);

         // Strategy is to do a reversed scan starting from key
         Scan s = new Scan(longToByteArray(key));
         s.setFilter(filterList);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         s.setReversed(true);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               if (record != null) {
                  break;
               }
               long currKey = Bytes.toLong(r.getRow());
               if (currKey >= key) {
                  if (LOG.isTraceEnabled()) LOG.trace("getPriorStartRecord(key) currKey " + currKey
                          + " is not less than key " + key + ";  continuing");
                  continue;
               }
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (st.hasMoreElements()) {
                     // Skip the key
                     st.nextToken();
                     String versionString = st.nextToken();
                     int version = Integer.parseInt(versionString);
                     if (version != SnapshotMetaRecordType.getVersion()) {
                        IOException ioe = new IOException("Unexpected record version found: "
                                + version + " expected: " + SnapshotMetaRecordType.getVersion());
                        LOG.error("getPriorStartRecord(key) exception ", ioe);
                        throw ioe;
                     }
                     String typeString = st.nextToken();
                     int recordType = Integer.parseInt(typeString);
                     if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                        // We found a full snapshot
                        String userTagString = st.nextToken();
                        //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups
                        if (userTagString.contains("_SAVE_") ||
                                userTagString.contains("_BRC_")) {
                           continue;
                        }
                        String snapshotCompleteString = st.nextToken();
                        String completionTimeString = st.nextToken();
                        String fuzzyBackupString = st.nextToken();
                        String markedForDeletionString = st.nextToken();
                        String incrementalBackupString = st.nextToken();
                        String firstIncrementalString = st.nextToken();
                        String extendedAttributes = st.nextToken();
                        String backupType = st.nextToken();
                        String timeString = st.nextToken();
                        String completionWallTimeString = null;
                        if (st.hasMoreTokens() == true)
                           completionWallTimeString = st.nextToken();

                        if (snapshotCompleteString.contains("false")) {
                           continue;
                        }
                        record = new SnapshotMetaStartRecord(currKey, Integer.parseInt(versionString), userTagString,
                                snapshotCompleteString.contains("true"),
                                Long.parseLong(completionTimeString, 10),
                                fuzzyBackupString.contains("true"),
                                markedForDeletionString.contains("true"),
                                incrementalBackupString.contains("true"),
                                firstIncrementalString.contains("true"),
                                extendedAttributes,
                                backupType,
                                Long.parseLong(timeString, 10));
                        if (completionWallTimeString != null)
                           record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10));
                     }
                  }
               }
            }
         } catch (Exception e) {
            LOG.error("getPriorStartRecord(key)  Exception getting results ", e);
            throw new IOException(e);
         } finally {
            ss.close();
         }
      } catch (Exception e) {
         LOG.error("getPriorStartRecord(key)  Exception setting up scanner ", e);
         throw new IOException(e);
      }
      if (record == null) {
         LOG.warn("Exception in getPriorStartRecord(key).  Record " + key + " not found");
      }
      return record;
   }

   /**
    * getPeerDownStartRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getPeerDownStartRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPeerDownStartRecord start for key " + key);
      SnapshotMetaStartRecord record = null;

      try {
          SubstringComparator comp = new SubstringComparator("SNAPSHOT_PEER_");
          SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
          // Strategy is to do a reversed scan starting from key
          Scan s = new Scan(longToByteArray(key));
          s.setFilter(filter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          s.setReversed(true);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                if (record != null) {
                   break;
                }
                long currKey = Bytes.toLong(r.getRow());
                if (currKey >= key){
                   if (LOG.isTraceEnabled()) LOG.trace("getPeerDownStartRecord(key) currKey " + currKey
                                     + " is not less than key " + key + ";  continuing");
                   continue;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getPeerDownStartRecord(key) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         // We found a full snapshot
                         String userTagString           = st.nextToken();
                         if (! userTagString.contains("SNAPSHOT_PEER_")){
                            if (LOG.isTraceEnabled()) LOG.trace("getPeerDownStartRecord(key) ignoring start record for tag " + userTagString);
                            continue;
                         }
                         String snapshotCompleteString  = st.nextToken();
                         String completionTimeString    = st.nextToken();
                         String fuzzyBackupString       = st.nextToken();
                         String markedForDeletionString = st.nextToken();
                         String incrementalBackupString = st.nextToken();
                         String firstIncrementalString  = st.nextToken();
                         String extendedAttributes      = st.nextToken();
                         String backupType              = st.nextToken();
                         String timeString              = st.nextToken();
                         String completionWallTimeString = null;
                         if(st.hasMoreTokens() == true)
                           completionWallTimeString = st.nextToken();

                         if (snapshotCompleteString.contains("false")) {
                            continue;
                         }
                         record = new SnapshotMetaStartRecord(currKey, Integer.parseInt(versionString), userTagString,
                                          snapshotCompleteString.contains("true"),
                                          Long.parseLong(completionTimeString, 10),
                                          fuzzyBackupString.contains("true"),
                                          markedForDeletionString.contains("true"),
                                          incrementalBackupString.contains("true"),
                                          firstIncrementalString.contains("true"),
                                          extendedAttributes,
                                          backupType,
                                          Long.parseLong(timeString, 10));
                         if( completionWallTimeString != null )
                            record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPeerDownStartRecord(key)  Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPeerDownStartRecord(key)  Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (record == null) {
         throw new Exception("Exception in getPeerDownStartRecord(key).  Record " + key + " not found");
      }
      return record;
   }

   /**
    * getFollowingStartRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getFollowingStartRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getFollowingStartRecord start for key " + key);
      SnapshotMetaStartRecord record = null;

      try {
         //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups
         SubstringComparator comp1 = new SubstringComparator("_SAVE_");
         SubstringComparator comp2 = new SubstringComparator("_BRC_");
         SingleColumnValueFilter filter1 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp1);
         SingleColumnValueFilter filter2 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp2);

         FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
         filterList.addFilter(filter1);
         filterList.addFilter(filter2);
         // Strategy is to do a forward scan starting from key
         Scan s = new Scan(longToByteArray(key));
         s.setFilter(filterList);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               if (record != null) {
                  break;
               }
               long currKey = Bytes.toLong(r.getRow());
               if (currKey <= key) {
                  if (LOG.isTraceEnabled()) LOG.trace("getFollowingStartRecord(key) currKey " + currKey
                          + " is not greater than key " + key + ";  continuing");
                  continue;
               }
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (st.hasMoreElements()) {
                     // Skip the key
                     st.nextToken();
                     String versionString = st.nextToken();
                     int version = Integer.parseInt(versionString);
                     if (version != SnapshotMetaRecordType.getVersion()) {
                        IOException ioe = new IOException("Unexpected record version found: "
                                + version + " expected: " + SnapshotMetaRecordType.getVersion());
                        LOG.error("getFollowingStartRecord(key) exception ", ioe);
                        throw ioe;
                     }
                     String typeString = st.nextToken();
                     int recordType = Integer.parseInt(typeString);
                     if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                        // We found a full snapshot
                        String userTagString = st.nextToken();
                        //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups
                        if (userTagString.contains("_SAVE_") ||
                                userTagString.contains("_BRC_")) {
                           continue;
                        }
                        String snapshotCompleteString = st.nextToken();
                        String completionTimeString = st.nextToken();
                        String fuzzyBackupString = st.nextToken();
                        String markedForDeletionString = st.nextToken();
                        String incrementalBackupString = st.nextToken();
                        String firstIncrementalString = st.nextToken();
                        String extendedAttributes = st.nextToken();
                        String backupType = st.nextToken();
                        String timeString = st.nextToken();
                        String completionWallTimeString = null;
                        if (st.hasMoreTokens() == true)
                           completionWallTimeString = st.nextToken();

                        if (snapshotCompleteString.contains("false")) {
                           continue;
                        }
                        record = new SnapshotMetaStartRecord(currKey, Integer.parseInt(versionString), userTagString,
                                snapshotCompleteString.contains("true"),
                                Long.parseLong(completionTimeString, 10),
                                fuzzyBackupString.contains("true"),
                                markedForDeletionString.contains("true"),
                                incrementalBackupString.contains("true"),
                                firstIncrementalString.contains("true"),
                                extendedAttributes,
                                backupType,
                                Long.parseLong(timeString, 10));
                        if (completionWallTimeString != null)
                           record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10));
                     }
                  }
               }
            }
         } catch (Exception e) {
            LOG.error("getFollowingStartRecord(key)  Exception getting results ", e);
            throw new IOException(e);
         } finally {
            ss.close();
         }
      } catch (Exception e) {
         LOG.error("getFollowingStartRecord(key)  Exception setting up scanner ", e);
         throw new IOException(e);
      }
      if (record == null) {
         LOG.warn("Exception in getFollowingStartRecord(key).  Record " + key + " not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getFollowingStartRecord(key) returning " + record);
      return record;
   }

   /**
    * getCurrentStartRecord
    * @return SnapshotMetaStartRecord rec
    * @throws Exception
    */
   public SnapshotMetaStartRecord getCurrentStartRecord() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentStartRecord start");
      SnapshotMetaStartRecord record = null;

      try {
         // Strategy is to do a reversed scan starting from the end of the table
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         s.setReversed(true);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               if (record != null) {
                  break;
               }
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (st.hasMoreTokens()) {
                     // Skip the key
                     st.nextToken();
                     String versionString = st.nextToken();
                     int version = Integer.parseInt(versionString);
                     if (version != SnapshotMetaRecordType.getVersion()){
                        IOException ioe = new IOException("Unexpected record version found: "
                            + version + " expected: " + SnapshotMetaRecordType.getVersion());
                        LOG.error("getCurrentStartRecord exception ", ioe);
                        throw ioe;
                     }
                     String typeString = st.nextToken();
                     int recordType = Integer.parseInt(typeString);
                     if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                        // We found a full snapshot
                        String userTagString           = st.nextToken();
                        String snapshotCompleteString  = st.nextToken();
                        String completionTimeString    = st.nextToken();
                        String fuzzyBackupString       = st.nextToken();
                        String markedForDeletionString = st.nextToken();
                        String incrementalBackupString = st.nextToken();
                        String firstIncrementalString  = st.nextToken();
                        String extendedAttributes      = st.nextToken();
                        String backupType              = st.nextToken();
                        String timeString              = st.nextToken();
                         String completionWallTimeString = null;
                         if(st.hasMoreTokens() == true)
                            completionWallTimeString = st.nextToken();

                        record = new SnapshotMetaStartRecord(currKey, Integer.parseInt(versionString), userTagString,
                                          snapshotCompleteString.contains("true"),
                                          Long.parseLong(completionTimeString, 10),
                                          fuzzyBackupString.contains("true"),
                                          markedForDeletionString.contains("true"),
                                          incrementalBackupString.contains("true"),
                                          firstIncrementalString.contains("true"),
                                          extendedAttributes,
                                          backupType,
                                          Long.parseLong(timeString, 10));
                         if( completionWallTimeString != null )
                            record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );
                     }
                  }
               }
            }
         }
         catch(Exception e){
            LOG.error("getCurrentStartRecord()  Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getCurrentStartRecord() Exception setting up scanner " , e);
         throw new IOException(e);
      }
      if (record == null) {
         throw new Exception("getCurrentStartRecord()  current record not found");
      }
      return record;
   }

   /**
    * getCurrentStartRecordId
    * @return long Id
    * @throws Exception
    */
   public long getCurrentStartRecordId() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentStartRecordId start");
      SnapshotMetaStartRecord record = null;

      record = getCurrentStartRecord();
      return record.getKey();
   }

   /**
    * getCurrentSnapshotId
    * @param String tableName
    * @return long Id
    * @throws Exception
    */
   public long getCurrentSnapshotId(final String tableName) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentSnapshotId start for tableName " + tableName);
      SnapshotMetaRecord record = null;
      record = getCurrentSnapshotRecord(tableName);

      if (record == null) {
         throw new Exception("getCurrentSnapshotId current record not found");
      }
      return record.getKey();
   }

   /**
    * getCurrentSnapshotRecord
    * @param String tableName
    * @return SnapshotMetaRecord
    * @throws Exception
    */
   public SnapshotMetaRecord getCurrentSnapshotRecord(final String tableName) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentSnapshotRecord start for tableName " + tableName);
      SnapshotMetaRecord record = null;

      SubstringComparator comp = new SubstringComparator(tableName);
      SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
      // Strategy is to do a reverse scan starting at the end of the snapshot table

      // using index record to speed up query table record
      SnapshotTableIndexRecord tableIndexRecord = getSnapshotTableIndex(tableName);
      if (tableIndexRecord != null && !tableIndexRecord.getSnapShots().isEmpty()) {
         LinkedList<Long> list = tableIndexRecord.getSnapShots();
         while (!list.isEmpty()) {
            long metaRowKey = list.pollLast();
            Scan s = new Scan(new Get(Bytes.toBytes(metaRowKey)));
            record = getCurrentSnapshotRecord(s, tableName);
            if (null != record) {
               if (LOG.isTraceEnabled())
                  LOG.trace("getCurrentSnapshotRecord--index cache:" + record);
               break;
            }
         }
      }
      if (record == null) {
         IOException ioe = new IOException("getCurrentSnapshotRecord current record not found");
         if (LOG.isTraceEnabled()) LOG.trace("Exception getting current SnapshotMetaRecord for table "
                 + tableName + " ", ioe);
      }
      return record;
   }

   public SnapshotMetaRecord getCurrentSnapshotRecord(Scan s, final String tableName) {
      SnapshotMetaRecord record = null;
      long start = System.currentTimeMillis();
      try (ResultScanner ss = table.getScanner(s)) {
         for (Result r : ss) {
            if (record != null) {
               break;
            }

            long currKey = Bytes.toLong(r.getRow());
            for (Cell cell : r.rawCells()) {
               StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
               if (st.hasMoreTokens()) {
                  // Skip the key
                  st.nextToken();
                  String versionString = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()) {
                     IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getCurrentSnapshotRecord exception ", ioe);
                     throw ioe;
                  }

                  String typeString = st.nextToken();
                  int recordType = Integer.parseInt(typeString);
                  if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                     // We found a partial snapshot
                     String supersedingSnapshotString = st.nextToken();
                     String restoredSnapshotString = st.nextToken();
                     String restoreStartTimeString = st.nextToken();
                     String hiatusTimeString = st.nextToken();
                     String tableNameString = st.nextToken();
                     if (!tableNameString.equals(tableName)) {
                        continue;
                     }
                     String userTagString = st.nextToken();
                     String snapshotNameString = st.nextToken();
                     String snapshotPathString = st.nextToken();
                     String userGeneratedString = st.nextToken();
                     String incrementalTableString = st.nextToken();
                     String sqlMetaDataString = st.nextToken();
                     String inLocalFsString = st.nextToken();
                     String excludedString = st.nextToken();
                     if (excludedString.contains("true")) {
                        continue;
                     }
                     String mutationStartTimeString = st.nextToken();
                     long mutationStartTime;
                     // This is to cover for metadata migration for an unused parameter.
                     if (mutationStartTimeString.contains("false")) {
                        mutationStartTime = 0L;
                     } else {
                        mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                     }
                     String archivePathString = st.nextToken();
                     String mutationStatements = st.nextToken();
                     String numLobsString = st.nextToken();
                     int numLobs = Integer.parseInt(numLobsString);
                     Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                     String lobSnapshotString = null;
                     try {
                        for (int i = 0; i < numLobs; i++) {
                           lobSnapshotString = st.nextElement().toString();
                           lobSnapshots.add(Long.parseLong(lobSnapshotString));
                        }
                     } catch (Exception e) {
                        LOG.error("getCurrentSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e);
                     }

                     Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                     String dependentSnapshotString = null;
                     try {
                        while (st.hasMoreElements()) {
                           dependentSnapshotString = st.nextElement().toString();
                           depSnapshots.add(Long.parseLong(dependentSnapshotString));
                        }
                     } catch (Exception e) {
                        LOG.error("getCurrentSnapshotRecord exception parsing dependent snapshots " + dependentSnapshotString, e);
                     }

                     long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                     long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                     long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                     long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                     record = new SnapshotMetaRecord(currKey, supersedingSnapshot,
                             restoredSnapshot, restoreStartTime, hiatusTime,
                             tableNameString, userTagString, snapshotNameString,
                             snapshotPathString, userGeneratedString.contains("true"),
                             incrementalTableString.contains("true"),
                             sqlMetaDataString.contains("true"),
                             inLocalFsString.contains("true"),
                             excludedString.contains("true"),
                             mutationStartTime,
                             archivePathString, mutationStatements,
                             numLobs, lobSnapshots, depSnapshots);
                  } else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {
                     // This is a SnapshotIncrementalMetaRecord, if the table name matches
                     // we can use it to get the key of the snapshot record instead of scanning.

                     String rootSnapshotString = st.nextToken();
                     long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
                     String parentSnapshotString = st.nextToken();
                     String childSnapshotString = st.nextToken();
                     String priorMutationStartTimeString = st.nextToken();
                     String tableNameString = st.nextToken();
                     if (!tableNameString.equals(tableName)) {
                        continue;
                     }

                     if (LOG.isTraceEnabled())
                        LOG.trace("getCurrentSnapshotRecord found incremental record with root key " + rootSnapshot);

                     record = getSnapshotRecord(rootSnapshot);

                     if (LOG.isTraceEnabled()) LOG.trace("getCurrentSnapshotRecord returning record : " + record);
                  }
               }
            }
         }
      } catch (Exception e) {
         LOG.error("getCurrentSnapshotRecord(table)  Exception getting results ", e);
         throw new RuntimeException(e);
      }
      return record;
   }

   /**
    * This method can retrieve SnapshotMetaRecords relative to the key passed in.
    * The key should be the key of some other SnapshotMeta record.
    * The boolean 'before' indicates whether the request wants a record before or
    * after the specified key.  True means before, false, assumes after.
    * If we are asked to provide a record before the specified key, we will return
    * the last record we have prior to the key.  If we are asked to provide a
    * record after the specified key, we will return the first record that comes after.
    * getRelativeSnapshotRecord
    * @param String tableName
    * @param long key
    * @param boolean before
    * @return SnapshotMetaRecord
    * @throws Exception
    */
   public SnapshotMetaRecord getRelativeSnapshotRecord(final String tableName,
            final long key, final boolean before) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getRelativeSnapshotRecord start for tableName " + tableName
          + " key " + key + " before " + before);
      SnapshotMetaRecord record = null;
      try {
         SubstringComparator comp = new SubstringComparator(tableName);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
         // Strategy is to start at the passed in key and do a forward or
         // reverse scan depending on the value of before
         Scan s = null;
         // using table index record to speed up table snapshot query
         SnapshotTableIndexRecord tableIndexRecord = getSnapshotTableIndex(tableName);
         if (tableIndexRecord != null && !tableIndexRecord.getSnapShots().isEmpty()) {
            LinkedList<Long> list = tableIndexRecord.getSnapShots();

            long firstTimestamp = list.getFirst();
            long lastTimestamp = list.getLast();
            // if the key is within the range of index then use Get rather than Scan
            if (key > firstTimestamp && key < lastTimestamp) {
               /*
                * Dequeue order: from large to small
                * scan reverse: dequeue order is consistent
                * scan forward: dequeue order is opposite
                */
               for (long timestamp : list) {
                  // the one contained in the queue which is smaller than 
                  // the target timestamp while is what we needed while scan reverse.
                  if (before && timestamp < key) {
                     s = new Scan(new Get(Bytes.toBytes(timestamp)));
                     break;
                  }
                  // the one contained in the queue which is smaller than 
                  // the target timestamp while is what we needed while scan forward.
                  if (!before && timestamp > key) {
                     s = new Scan(new Get(Bytes.toBytes(timestamp)));
                  }
               }
               record = getRelativeSnapshotRecord(s, tableName, key);
            }
         }
      }
      catch(Exception e){
         LOG.error("getRelativeSnapshotRecord Exception setting up scanner " , e);
         throw new IOException(e);
      }
      if (record == null) {
         IOException ioe = new IOException("getRelativeSnapshotRecord current record not found");
         if (LOG.isTraceEnabled()) LOG.trace("Exception getting current SnapshotMetaRecord for table "
             + tableName + " ", ioe);
      }
      return record;
   }

   public SnapshotMetaRecord getRelativeSnapshotRecord(Scan s, final String tableName, final long key) {
      SnapshotMetaRecord record = null;
      try(ResultScanner ss = table.getScanner(s)) {
         for (Result r : ss) {
            long currKey = Bytes.toLong(r.getRow());
            if (currKey == key){
               // this is the starting record.  Skip it and take the next one.
               continue;
            }
            if (record != null){
               break;
            }
            for (Cell cell : r.rawCells()) {
               StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
               if (st.hasMoreElements()) {
                  // Skip the key
                  st.nextToken();
                  String versionString = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getRelativeSnapshotRecord exception ", ioe);
                     throw ioe;
                  }

                  String typeString = st.nextToken();
                  int recordType = Integer.parseInt(typeString);
                  if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                     // We found a partial snapshot
                     String supersedingSnapshotString = st.nextToken();
                     String restoredSnapshotString = st.nextToken();
                     String restoreStartTimeString = st.nextToken();
                     String hiatusTimeString = st.nextToken();
                     String tableNameString    = st.nextToken();
                     if (! tableNameString.equals(tableName)) {
                        continue;
                     }
                     String userTagString             = st.nextToken();
                     String snapshotNameString        = st.nextToken();
                     String snapshotPathString        = st.nextToken();
                     String userGeneratedString       = st.nextToken();
                     String incrementalTableString    = st.nextToken();
                     String sqlMetaDataString         = st.nextToken();
                     String inLocalFsString           = st.nextToken();
                     String excludedString            = st.nextToken();
                     String mutationStartTimeString   = st.nextToken();
                     long mutationStartTime;
                     // This is to cover for metadata migration for an unused parameter.
                     if (mutationStartTimeString.contains("false")){
                        mutationStartTime = 0L;
                     }
                     else{
                        mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                     }
                     String archivePathString         = st.nextToken();
                     String mutationStatements        = st.nextToken();
                     String numLobsString             = st.nextToken();

                     int numLobs = Integer.parseInt(numLobsString);
                     Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                     String lobSnapshotString = null;
                     try {
                        for (int i = 0; i < numLobs; i++) {
                           lobSnapshotString = st.nextElement().toString();
                           lobSnapshots.add(Long.parseLong(lobSnapshotString));
                        }
                     } catch (Exception e) {
                        LOG.error("getRelativeSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e );
                     }

                     Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                     String dependentSnapshotString = null;
                     try {
                        while (st.hasMoreElements()) {
                           dependentSnapshotString = st.nextElement().toString();
                           depSnapshots.add(Long.parseLong(dependentSnapshotString));
                        }
                     } catch (Exception e) {
                        LOG.error("getRelativeSnapshotRecord exception parsing dependent snapshots " + dependentSnapshotString, e );
                     }

                     long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                     long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                     long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                     long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                     record = new SnapshotMetaRecord(currKey, supersedingSnapshot,
                             restoredSnapshot, restoreStartTime, hiatusTime,
                             tableNameString, userTagString, snapshotNameString,
                             snapshotPathString, userGeneratedString.contains("true"),
                             incrementalTableString.contains("true"),
                             sqlMetaDataString.contains("true"),
                             inLocalFsString.contains("true"),
                             excludedString.contains("true"),
                             mutationStartTime,
                             archivePathString, mutationStatements,
                             numLobs, lobSnapshots, depSnapshots);

                  }
               }
            }
         }
      }
      catch(Exception e){
         LOG.error("getRelativeSnapshotRecord  Exception getting results " , e);
         throw new RuntimeException(e);
      }
      return record;
   }

   /**
    * listSnapshotStartRecords
    * @return ArrayList<SnapshotMetaStartRecord> set
    * @throws Exception
    *
    * This method takes no parameters and retrieves a set of all SnapshotMetaStartRecords
    * possibly as part of a 'sqlci list backups' command
    */
   public ArrayList<SnapshotMetaStartRecord> listSnapshotStartRecords() throws Exception {

      long start = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) LOG.trace("listSnapshotStartRecords()");
      ArrayList<SnapshotMetaStartRecord> returnList = new ArrayList<SnapshotMetaStartRecord>();
      SnapshotMetaStartRecord record = null;
      long snapshotStopId = 0;

      try {
         //  1 rowkey prefix scan all tag index
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         s.setRowPrefixFilter(Bytes.toBytes(SnapshotTagIndexRecord.TAG_PREFIX));
         ResultScanner ss = table.getScanner(s);

         try {
            List<Get> gets = new ArrayList<>();
            for (Result r : ss) {
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (st.hasMoreElements()) {
                     // Skip the key
                     st.nextToken();
                     st.nextToken();
                     String typeString = st.nextToken();
                     int recordType = Integer.parseInt(typeString);
                     if (recordType == (SnapshotMetaRecordType.SNAPSHOT_TAG_INDEX_RECORD.getValue())) {
                        String columnValue = Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
                        SnapshotTagIndexRecord tagIndexRecord = new SnapshotTagIndexRecord(columnValue);
                        gets.add(new Get(Bytes.toBytes(tagIndexRecord.getStartKey())));
                     }
                     else {
                        // This is a SnapshotMetaRecord, so ignore it
                        continue;
                     }
                  }
               }
            }

            //use tag index gets snapshot start data
            if(!gets.isEmpty()){
               Result [] startResults = table.get(gets);
               for (Result r : startResults) {
                  if (r.isEmpty()){
                     continue;
                  }
                  long currKey = Bytes.toLong(r.getRow());
                  for (Cell cell : r.rawCells()) {
                     StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                     if (st.hasMoreElements()) {
                        // Skip the key
                        st.nextToken();
                        String versionString = st.nextToken();
                        int version = Integer.parseInt(versionString);
                        if (version != SnapshotMetaRecordType.getVersion()){
                           IOException ioe = new IOException("Unexpected record version found: "
                                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
                           LOG.error("listSnapshotStartRecords exception ", ioe);
                           throw ioe;
                        }
                        String typeString = st.nextToken();
                        int recordType = Integer.parseInt(typeString);
                        if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                           // We found a snapshot start
                           String userTagString            = st.nextToken();
                           String snapshotCompleteString   = st.nextToken();
                           String completionTimeString     = st.nextToken();
                           String fuzzyBackupString        = st.nextToken();
                           String markedForDeletionString  = st.nextToken();
                           String incrementalBackupString  = st.nextToken();
                           String firstIncrementalString   = st.nextToken();
                           String extendedAttributes       = st.nextToken();
                           String backupType;
                           try {
                              backupType               = st.nextToken();
                           }
                           catch(Exception e){
                              LOG.error("listSnapshotStartRecords() Exception getting backup type for currKey " + currKey
                                      + " type " + recordType + "tag " + userTagString + " incrementalBackup " + incrementalBackupString, e);
                              throw new IOException(e);
                           }
                           String timeString          = st.nextToken();
                           String completionWallTimeString = null;
                           if(st.hasMoreTokens() == true)
                              completionWallTimeString = st.nextToken();

                           snapshotStopId = Long.parseLong(completionTimeString, 10);
                           record = new SnapshotMetaStartRecord(currKey, version, userTagString,
                                   snapshotCompleteString.contains("true"),
                                   snapshotStopId,
                                   fuzzyBackupString.contains("true"),
                                   markedForDeletionString.contains("true"),
                                   incrementalBackupString.contains("true"),
                                   firstIncrementalString.contains("true"),
                                   extendedAttributes,
                                   backupType,
                                   Long.parseLong(timeString, 10));

                           if( completionWallTimeString != null )
                              record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );

                           returnList.add(record);
                        }
                        else {
                           // This is a SnapshotMetaRecord, so ignore it
                           continue;
                        }
                     }
                  }
               }
            }

         }
         catch(Exception e){
            LOG.error("listSnapshotStartRecords() Exception getting results ", e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("listSnapshotStartRecords() Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (returnList.isEmpty()) {
         throw new Exception("listSnapshotStartRecords() Prior record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("listSnapshotStartRecords(): returning " + returnList.size() + " records");

      long end = System.currentTimeMillis();
      if (LOG.isInfoEnabled())
         LOG.info("listSnapshotStartRecords total time : " + (end -start) +"  size:  " + returnList.size());
      return returnList;
   }

   /**
    * getPriorSnapshotSet
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    * 
    * This method takes no parameters and retrieves a snapshot set for the latest completed
    * full snapshot list as part of a restore operation
    */
   public ArrayList<SnapshotMetaRecord> getPriorSnapshotSet() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet()");
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;
      SnapshotMetaStartRecord startRec = null;
      long snapshotStopId = 0;
      boolean ignoreCurrRecord = false;
      // Strategy is to get the current start record, which does a reverse table scan.
      // Then starting at that record we do a forward scan to get the remaining records.
      startRec = getCurrentStartRecord();
      try {
          Scan s = new Scan(longToByteArray(startRec.getKey()));
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);
          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                ignoreCurrRecord = false;
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getPriorSnapshotSet() exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         // Found a start record.  Skip the user tag
                         st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // must ignore the following snapshsots until we find another
                            // snapshot that completed successfully.
                            ignoreCurrRecord = true;
                            continue;
                         }

                         // We found a snapshot start that was completed, so anything already in the
                         // returnList is invalid.  We need to empty the returnList and start
                         // building it from here.
                     	 returnList.clear();

                     	 // Note that the current record we are reading is a SnapshotMetaStartRecord, not a SnapshotMetaRecord,
                         // so we skip it rather than add it into the list, but we do record the completionTime
                         // so we know when to stop including snapshots in the returnList.
                         String completionTimeString  = st.nextToken();
                         snapshotStopId = Long.parseLong(completionTimeString, 10);
                         ignoreCurrRecord = false;
                         continue;
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         // This is a SnapshotMetaRecord, but if the key is greater than the 
                         // snapshotStopId we ignore it rather than include it in the returnList
                         if (currKey > snapshotStopId) {
                            ignoreCurrRecord = true;
                         }
                         if (ignoreCurrRecord) {
                        	 continue;
                         }
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString = st.nextToken();
                         String sqlMetaDataString  = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString     = st.nextToken();
                         String mutationStartTimeString = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();

                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet exception parsing dependent snapshots " + dependentSnapshotString, e );
                         }

                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey, supersedingSnapshot,
                                          restoredSnapshot, restoreStartTime,
                                          hiatusTime,
                                          tableNameString, userTagString,
                                          snapshotNameString, snapshotPathString,
                                          userGeneratedString.contains("true"),
                                          incrementalTableString.contains("true"),
                                          sqlMetaDataString.contains("true"),
                                          inLocalFsString.contains("true"),
                                          excludedString.contains("true"),
                                          mutationStartTime,
                                          archivePathString, mutationStatements,
                                          numLobs, lobSnapshots, depSnapshots);

                         returnList.add(record);
                      }
                      else {
                          // SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()
                          // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorSnapshotSet() Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorSnapshotSet() Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (returnList.isEmpty()) {
         throw new Exception("Exception in getPriorSnapshotSet().  Record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(): returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * excludeSnapshotFromTable
    * @return void
    * @throws Exception
    *
    */
   public void excludeSnapshotFromTable(HashSet<String> tableNameSet, String tag, long excludePoint) throws IOException {
      if (LOG.isInfoEnabled()) LOG.info("excludeSnapshotFromTable start for table "
              + tableNameSet.size() + " tag " + tag + " and excludePoint " + excludePoint);
      SnapshotMetaRecord record = null;
      int count = 0, batch = 100;
      try {
         Scan s = new Scan(longToByteArray(excludePoint));
         s.setCaching(1000);
         s.setCacheBlocks(false);
         s.setBatch(1000);
         ResultScanner ss = table.getScanner(s);

         List<SnapshotMetaRecord> batchPutList = new ArrayList<>();
          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (currKey <= excludePoint){
                   // This record is prior to the excludePoint
                   continue;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // ignore the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("excludeSnapshotFromTable() exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         // Found a start record, not a SnapshotMeta
                         continue;
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         // This is a SnapshotMetaRecord; include it in the returnList
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString   = st.nextToken();
                         String tableNameString    = st.nextToken();
                         if (!tableNameSet.contains(tableNameString)){
                            // This is for the wrong table
                            continue;
                         }
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString = st.nextToken();
                         String sqlMetaDataString  = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString     = st.nextToken();
                         String mutationStartTimeString = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();

                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("excludeSnapshotFromTable exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("excludeSnapshotFromTable exception parsing dependent snapshots " + dependentSnapshotString, e );
                            throw new IOException("excludeSnapshotFromTable exception parsing dependent snapshots " + dependentSnapshotString, e );
                         }
                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey, supersedingSnapshot,
                                          restoredSnapshot, restoreStartTime,
                                          hiatusTime,
                                          tableNameString, userTagString,
                                          snapshotNameString, snapshotPathString,
                                          userGeneratedString.contains("true"),
                                          incrementalTableString.contains("true"),
                                          sqlMetaDataString.contains("true"),
                                          inLocalFsString.contains("true"),
                                          excludedString.contains("true"),
                                          mutationStartTime,
                                          archivePathString, mutationStatements,
                                          numLobs, lobSnapshots, depSnapshots);

                         if (LOG.isTraceEnabled()) LOG.trace("excludeSnapshotFromTable found record to exclude "
                                 + record);
                         record.setExcluded(true);
                         batchPutList.add(record);
                         if ((++count) % batch == 0) {
                           batchPutSnapshotRecords(batchPutList);
                           batchPutList.clear();
                         }
                      }
                      else {
                          // SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()
                          // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                      }
                   }
                }
            }
            if (!batchPutList.isEmpty()) {
               batchPutSnapshotRecords(batchPutList);
            }
         }
         catch(Exception e){
            LOG.error("excludeSnapshotFromTable() Exception getting results " , e);
            throw new IOException("excludeSnapshotFromTable() Exception getting results " , e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("excludeSnapshotFromTable() Exception setting up scanner " , e);
          throw new IOException("excludeSnapshotFromTable() Exception setting up scanner " , e);
      }
      if (LOG.isInfoEnabled()) LOG.info("excludeSnapshotFromTable() end count " + count);
      return;
   }

   /**
    * deleteSnapshotStartRecord
    * @param String tag
    * @throws Exception
    * 
    * This method takes a String parameter that is the tag associated with the desired snapshot start record
    * and deletes it
    */
   public void deleteSnapshotStartRecord(String tag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("deleteSnapshotStartRecord for tag " + tag);
      boolean found = false;

      try {
          SubstringComparator comp = new SubstringComparator(tag);
          SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
          Scan s = new Scan();
          s.setFilter(filter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("deleteSnapshotStartRecord(tag) exception ", ioe);
                         throw ioe;
                      }

                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {

                         // We found a snapshot start.  Let's see if the tag is the one we want.
                         String userTagString           = st.nextToken();
                         if (userTagString.equals(tag)) {
                        	found = true;
                            deleteRecord(currKey);
                            if (LOG.isTraceEnabled()) LOG.trace("deleteSnapshotStartRecord(tag).  Deleted startRecord for "
                                   + currKey + " tag " + tag);

                         }
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("deleteSnapshotStartRecord(tag) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("deleteSnapshotStartRecord(tag) Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (found == false) {
         throw new Exception("Exception in deleteSnapshotStartRecord(tag).  Record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteSnapshotStartRecord(tag): Exit ");
      return;
   }

   /**
    * getSnapshotStartRecord
    * @param String tag
    * @return SnapshotMetaStartRecord record
    * @throws Exception
    *
    * This method takes a String parameter that is the tag associated with the desired snapshot start record
    * and returns it
    */
   public SnapshotMetaStartRecord getSnapshotStartRecord(String tag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotStartRecord for tag " + tag);
      SnapshotMetaStartRecord record = null;

      try {
         SubstringComparator comp = new SubstringComparator(tag);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
          // Strategy is to do a reversed table scan assuming that the most recent
          // start record is closer to the end of the table than the beginning.
         Scan s = new Scan();
         s.setReversed(true);

         // get tag index
         SnapshotTagIndexRecord tagIndexRecord = getSnapshotTagIndex(tag);
         // if tag index is exists and has vaild endkey then set start and stop key
         if (tagIndexRecord != null) {
            // scan reverse: from endkey to startkey
            s = new Scan();
            s.setStopRow(Bytes.toBytes(tagIndexRecord.getEndKey()));
            s.setStartRow(Bytes.toBytes(tagIndexRecord.getStartKey()));
         }
          s.setFilter(filter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (record != null) {
                   break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getSnapshotStartRecord(tag) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {

                         // We found a snapshot start.  Let's see if the tag is the one we want.
                         String userTagString           = st.nextToken();
                         if (userTagString.equals(tag)) {
                            String snapshotCompleteString  = st.nextToken();
                            String completionTimeString    = st.nextToken();
                            String fuzzyBackupString       = st.nextToken();
                            String markedForDeletionString = st.nextToken();
                            String incrementalBackupString = st.nextToken();
                            String firstIncrementalString  = st.nextToken();
                            String extendedAttributes      = st.nextToken();
                            String backupType              = st.nextToken();
                            String timeString              = st.nextToken();
                            String completionWallTimeString = null;
                            if(st.hasMoreTokens() == true)
                               completionWallTimeString = st.nextToken();

                            record = new SnapshotMetaStartRecord(currKey, version, userTagString,
                                        snapshotCompleteString.contains("true"),
                                        Long.parseLong(completionTimeString, 10),
                                        fuzzyBackupString.contains("true"),
                                        markedForDeletionString.contains("true"),
                                        incrementalBackupString.contains("true"),
                                        firstIncrementalString.contains("true"),
                                        extendedAttributes,
                                        backupType,
                                        Long.parseLong(timeString, 10));

                            if( completionWallTimeString != null )
                               record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );

                            if (LOG.isTraceEnabled()) LOG.trace("getSnapshotStartRecord(tag).  Found startRecord for tag "
                                   + tag + " key " + currKey);
                            break;
                         }
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getSnapshotStartRecord(tag) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getSnapshotStartRecord(tag) Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (record == null) {
         throw new FileNotFoundException("Exception in getSnapshotStartRecord(tag).  Record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotStartRecord(tag): Exit returning record " + record.getKey());
      return record;
   }

   /**
    * getPriorSnapshotSet
    * @param String tag
    * @param StringBuilder backupType
    * @param ArrayList<String> schemaList
    * @param ArrayList<String> tableList
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    * 
    * This method takes a String parameter that is the tag associated with the desired snapshot set
    * and retrieves the snapshot set as a list to be used as part of a restore operation
    */
   public ArrayList<SnapshotMetaRecord> getPriorSnapshotSet(String tag, StringBuilder backupType) throws Exception {
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;
      long snapshotStopId = 0;
      boolean tagFound = false;
      boolean done = false;
      // Strategy is the retrieve the most recent start record using a reverse scan.
      // Then we position a new scan using the start record key and perform a forward
      // scan to retrieve the remaining records in the set.
      try {
         SubstringComparator comp = new SubstringComparator(tag);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);

         Scan s = new Scan();
         // using tag index
         SnapshotTagIndexRecord tagIndexRecord = getSnapshotTagIndex(tag);
         // the tag index is exists and has valid endkey then set scanning range
         if (tagIndexRecord != null) {
            s.setStartRow(Bytes.toBytes(tagIndexRecord.getStartKey()));
            if (tagIndexRecord.getEndKey() > 0) {
               s.setStopRow(Bytes.toBytes(tagIndexRecord.getEndKey()));
            }
         }
          s.setFilter(filter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (done){
                   break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getPriorSnapshotSet(tag) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // must ignore the following snapshots until we find another
                            // snapshot that completed successfully.
                            continue;
                         }

                         // We found a snapshot start that was completed.  Let's see if the tag is the one we want
                         if (userTagString.equals(tag)) {
                            String completionTimeString  = st.nextToken();
                            snapshotStopId = Long.parseLong(completionTimeString, 10);
						    String fuzzyBackupString       = st.nextToken();
                            String markedForDeletionString = st.nextToken();
                            String incrementalBackupString = st.nextToken();
                            String firstIncrementalString  = st.nextToken();
							String extendedAttributes      = st.nextToken();
							String backupTypeString        = st.nextToken();
							String timeString              = st.nextToken();
                            backupType.append(backupTypeString);
                            tagFound = true;
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag) found a full snapshot for tag "
                                      + userTagString + ", incrementalBackup " + incrementalBackupString
                                      + ", firstIncremental " + firstIncrementalString
                                      + ", extendedAttributes " + extendedAttributes + ", markedForDeletion "
                                      + markedForDeletionString + " backupType " + backupTypeString
                                      + " timeString " + timeString);
                            continue;
                         }
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         // This is a SnapshotMetaRecord, but if we haven't found the tag we are
                         // looking for in a start record we ignore it rather than include it in the returnList
                         if (tagFound != true) {
                            continue;
                         }
                         
                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag) Snapshot record key "  + currKey
                                      + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString    = st.nextToken();
                         String sqlMetaDataString = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString    = st.nextToken();
                         String mutationStartTimeString     = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();
                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet(tag) exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet(tag) exception parsing dependent snapshots "
                                      + dependentSnapshotString + " ", e );
                         }

                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey, supersedingSnapshot,
                                  restoredSnapshot, restoreStartTime, hiatusTime,
                                  tableNameString, userTagString,
                                  snapshotNameString, snapshotPathString,
                                  userGeneratedString.contains("true"),
                                  incrementalTableString.contains("true"),
                                  sqlMetaDataString.contains("true"),
                                  inLocalFsString.contains("true"),
                                  excludedString.contains("true"),
                                  mutationStartTime,
                                  archivePathString, mutationStatements,
                                  numLobs, lobSnapshots, depSnapshots);

                         if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag) adding record to returnList, record: " + record);
                         returnList.add(record);
                      }
                      else {
                         // SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()
                         // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorSnapshotSet(tag) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorSnapshotSet(tag) Exception setting up scanner " , e);
          throw new IOException(e);
      }
/*
      // Now we have a superset of all snapshots from the tag.  We will check to see if we need to cull the list
      // based on the schemaList and tableList passed in by the user.
      if ((schemaList.size() != 0)  || (tableList.size() != 0)){
         // We need to make sure the list we return matches one of the entries
         // in the schemaList or tableList
         for (SnapshotMetaRecord tmpRecord : returnList) {
            String  tblName = tmpRecord.getTableName();
            if ((! isTableOnSchemaList(tblName, schemaList)) && (! isTableOnTableList(tblName, tableList))){
               if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag): removing snapshot " + tmpRecord.getKey());
               returnList.remove(tmpRecord.getKey());
            }
         }
      }
*/
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag): returning " + returnList.size()
      + " records for tag " + tag);
      return returnList;
   }

   /**
    * getPriorIncrementalSnapshotSet
    * @param String tag
    * @return ArrayList<SnapshotMetaIncrementalRecord> set
    * @throws Exception
    *
    * This method takes a String parameter that is the tag associated with the desired snapshot set
    * and retrieves the incremental snapshot set as a list to be used as part of a delete backup
    * or export backup operation
    */
   public ArrayList<SnapshotMetaIncrementalRecord> getPriorIncrementalSnapshotSet(String tag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet for tag " + tag);
      ArrayList<SnapshotMetaIncrementalRecord> returnList = new ArrayList<SnapshotMetaIncrementalRecord>();
      SnapshotMetaIncrementalRecord record = null;
      long snapshotStopId = 0;
      boolean tagFound = false;
      boolean done = false;
      // Strategy is the retrieve the most recent start record using a reverse scan.
      // Then we position a new scan using the start record key and perform a forward
      // scan to retrieve the remaining records in the set.
      //startRec = getSnapshotStartRecord(tag);
      try {
         SubstringComparator comp = new SubstringComparator(tag);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
         Scan s = new Scan();
         // using tag index
         SnapshotTagIndexRecord tagIndexRecord = getSnapshotTagIndex(tag);
         // the tag index is exists and has valid endkey then set scanning range 
         if (tagIndexRecord != null) {
            s.setStartRow(Bytes.toBytes(tagIndexRecord.getStartKey()));
            if (tagIndexRecord.getEndKey() > 0) {
               s.setStopRow(Bytes.toBytes(tagIndexRecord.getEndKey()));
            }
         }

         s.setFilter(filter);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (done) {
                   break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getPriorIncrementalSnapshotSet(tag) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // must ignore the following snapshots until we find another
                            // snapshot that completed successfully.
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) found a partial snapshot set for key "
                                                + currKey + " but it never completed; ignoring");
                            continue;
                         }

                         // We found a snapshot start that was completed.  Let's see if the tag is the one we want
                         if (userTagString.equals(tag)) {
                            String completionTimeString  = st.nextToken();
                            snapshotStopId = Long.parseLong(completionTimeString, 10);
                            String fuzzyBackupString       = st.nextToken();
                            String markedForDeletionString = st.nextToken();
                            String incrementalBackupString = st.nextToken();
                            String firstIncrementalString = st.nextToken();
                            if(firstIncrementalString.contains("true")){
                               // We treat the first incremental record after a fuzzy backup as a regular backup
                               if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) found a first incremental set for key "
                                                 + currKey + "; Treating it as a regular backup and ignoring");
                            }
                            tagFound = true;
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) found a full snapshot for tag "
                                      + userTagString + ", incrementalBackup " + incrementalBackupString
                                      + ", markedForDeletion " + markedForDeletionString);
                            continue;
                         }
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {
                         // This is a SnapshotIncrementalMetaRecord, but if we haven't found the tag we are
                         // looking for in a start record we ignore it rather than include it in the returnList
                         if (tagFound != true) {
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag)  Ignoring incremental snapshot record for key "  + currKey);
                            continue;
                         }

                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) Snapshot record key "  + currKey
                                      + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }

                         String rootSnapshotString = st.nextToken();
                         long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
                         String parentSnapshotString = st.nextToken();
                         String childSnapshotString = st.nextToken();
                         String priorMutationStartTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString    = st.nextToken();
                         String archivedString     = st.nextToken();
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString = st.nextToken();
                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getPriorIncrementalSnapshotSet(tag) exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         long parentSnapshot = Long.parseLong(parentSnapshotString, 10);
                         long childSnapshot = Long.parseLong(childSnapshotString, 10);
                         long priorMutationStartTime = Long.parseLong(priorMutationStartTimeString, 10);

                         if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) adding record to returnList, key " + currKey);

                         record = new SnapshotMetaIncrementalRecord(currKey,
                                  version, rootSnapshot, parentSnapshot,
                                  childSnapshot, priorMutationStartTime,
                                  tableNameString, userTagString,
                                  snapshotNameString, snapshotPathString,
                                  inLocalFsString.contains("true"),
                                  excludedString.contains("true"),
                                  archivedString.contains("true"),
                                  archivePathString, mutationStatements,
                                  numLobs, lobSnapshots);

                         if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag) adding record to returnList, record: " + record);
                         returnList.add(record);
                      }
                      else {
                         // SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()
                         // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorIncrementalSnapshotSet(tag) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorIncrementalSnapshotSet(tag) Exception setting up scanner " , e);
          throw new IOException(e);
      }
/*
      // Now we have a superset of all snapshots from the tag.  We will check to see if we need to cull the list
      // based on the schemaList and tableList passed in by the user.
      if ((schemaList.size() != 0)  || (tableList.size() != 0)){
         // We need to make sure the list we return matches one of the entries
         // in the schemaList or tableList
         for (SnapshotMetaRecord tmpRecord : returnList) {
            String  tblName = tmpRecord.getTableName();
            if ((! isTableOnSchemaList(tblName, schemaList)) && (! isTableOnTableList(tblName, tableList))){
               if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag): removing snapshot " + tmpRecord.getKey());
               returnList.remove(tmpRecord.getKey());
            }
         }
      }
*/
      if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotSet(tag): returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getPriorIncrementalSnapshotRecord
    * @param String tableName
    * @param boolean includeExcluded
    * @return SnapshotMetaIncrementalRecord
    * @throws Exception
    *
    * This method takes a String parameter that is the tableName associated with the desired snapshot record
    * and a boolean indicating whether we should return prior excluded snapshots.  It then retrieves the
    * incremental snapshot.
    */
   public SnapshotMetaIncrementalRecord getPriorIncrementalSnapshotRecord(String tableName, boolean includeExcluded) throws Exception {
      if (LOG.isTraceEnabled()) { LOG.trace("getPriorIncrementalSnapshotRecord for tableName " + tableName + " includeExcluded " + includeExcluded); }
      SnapshotMetaIncrementalRecord record = null;

      try {
         SubstringComparator comp = new SubstringComparator(tableName);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
         // Strategy is to perform a reverse scan from the end of the table.

         // TODO using table index
         SnapshotTableIndexRecord tableIndexRecord = getSnapshotTableIndex(tableName);
         if (tableIndexRecord != null && !tableIndexRecord.getIncrSnapshots().isEmpty()) {
            LinkedList<Long> list = tableIndexRecord.getIncrSnapshots();
            while (!list.isEmpty()) {
               long metaRowKey = list.pollLast();
               Scan s = new Scan(new Get(Bytes.toBytes(metaRowKey)));
               record = getPriorIncrementalSnapshotRecord(s, tableName, includeExcluded);
               if (null != record) {
                  if (LOG.isTraceEnabled())
                     LOG.trace("getPriorIncrementalSnapshotRecord--index record:" + record);
                  break;
               }
            }
         }
         if (record == null) {
            LOG.warn("Exception getting current SnapshotMetaRecord for table " + tableName + " not found");
         }
      }
      catch(Exception e){
          LOG.error("getPriorIncrementalSnapshotRecord Exception setting up scanner " , e);
          throw new IOException(e);
      }

      if (LOG.isTraceEnabled()) { LOG.trace("getPriorIncrementalSnapshotRecord: returning " + record); }
      return record;
   }

   public SnapshotMetaIncrementalRecord getPriorIncrementalSnapshotRecord(Scan s, String tableName, boolean includeExcluded) {
      SnapshotMetaIncrementalRecord record = null;

      try(ResultScanner ss = table.getScanner(s)) {
         for (Result r : ss) {
            if (record != null){
               break;
            }
            long currKey = Bytes.toLong(r.getRow());
            for (Cell cell : r.rawCells()) {
               StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
               if (st.hasMoreElements()) {
                  // Skip the key
                  st.nextToken();
                  String versionString = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getPriorIncrementalSnapshotRecord exception ", ioe);
                     throw ioe;
                  }
                  String typeString = st.nextToken();
                  int recordType = Integer.parseInt(typeString);
                  if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                     continue;
                  }
                  else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {

                     if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotRecord.  Found snapshot record for key "  + currKey);

                     String rootSnapshotString = st.nextToken();
                     long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
                     String parentSnapshotString = st.nextToken();
                     String childSnapshotString = st.nextToken();
                     String priorMutationStartTimeString = st.nextToken();
                     String tableNameString    = st.nextToken();
                     if (! tableNameString.equals(tableName)){
                        // This is for the wrong table
                        continue;
                     }
                     String userTagString      = st.nextToken();
                     String snapshotNameString = st.nextToken();
                     String snapshotPathString = st.nextToken();
                     String inLocalFsString    = st.nextToken();
                     String excludedString    = st.nextToken();
                     if ((excludedString.contains("true")) && (! includeExcluded)){
                        if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotRecord incremental record "
                                + currKey + " is excluded");
                        continue;
                     }
                     String archivedString     = st.nextToken();
                     String archivePathString  = st.nextToken();
                     String mutationStatements = st.nextToken();
                     String numLobsString = st.nextToken();
                     int numLobs = Integer.parseInt(numLobsString);
                     Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                     String lobSnapshotString = null;
                     try {
                        for (int i = 0; i < numLobs; i++) {
                           lobSnapshotString = st.nextElement().toString();
                           lobSnapshots.add(Long.parseLong(lobSnapshotString));
                        }
                     } catch (Exception e) {
                        LOG.error("getPriorIncrementalSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e );
                     }

                     long parentSnapshot = Long.parseLong(parentSnapshotString, 10);
                     long childSnapshot = Long.parseLong(childSnapshotString, 10);
                     long priorMutationStartTime = Long.parseLong(priorMutationStartTimeString, 10);

                     record = new SnapshotMetaIncrementalRecord(currKey,
                             version, rootSnapshot, parentSnapshot,
                             childSnapshot, priorMutationStartTime,
                             tableNameString, userTagString,
                             snapshotNameString, snapshotPathString,
                             inLocalFsString.contains("true"),
                             excludedString.contains("true"),
                             archivedString.contains("true"),
                             archivePathString, mutationStatements,
                             numLobs, lobSnapshots);

                     if (LOG.isTraceEnabled()) LOG.trace("getPriorIncrementalSnapshotRecord found record " + record);
                  }
                  else {
                     // SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()
                     // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                  }
               }
            }
         }
      }
      catch(Exception e){
         LOG.error("getPriorIncrementalSnapshotRecord Exception getting results " , e);
         throw new RuntimeException(e);
      }
      return record;
   }

   /**
    * getIncrementalSnapshotSetForRoot
    * @param long rootKey
    * @return ArrayList<SnapshotMetaIncrementalRecord> set
    * @throws Exception
    *
    * This method takes a long parameter that is the key associated with the root snapshot of
    * the desired incremental snapshot set and retrieves records as a list.
    */
   public ArrayList<SnapshotMetaIncrementalRecord> getIncrementalSnapshotSetForRoot(long rootKey) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getIncrementalSnapshotSetForRoot for root " + rootKey);
      ArrayList<SnapshotMetaIncrementalRecord> returnList = new ArrayList<SnapshotMetaIncrementalRecord>();
      SnapshotMetaRecord record = null;
      SnapshotMetaIncrementalRecord incRec = null;
      Set<Long> deps = null;

      try {
         record = getSnapshotRecord(rootKey);
         deps = record.getDependentSnapshots();

         //batch query
         List<SnapshotMetaIncrementalRecord> incrementalRecords = getIncrementalSnapshotRecords(deps);
         Map<Long,SnapshotMetaIncrementalRecord> irsMap = new HashMap<>();
         for (SnapshotMetaIncrementalRecord ir : incrementalRecords) {
            irsMap.put(ir.getKey(),ir);
         }

         Iterator<Long> depsIterator = deps.iterator();
         while (depsIterator.hasNext()) {
            Long currKey = depsIterator.next();
            if (currKey == -1L){
               break;
            }
            incRec = irsMap.get(currKey);
            if(incRec == null){
               // The record doesn't exist, so remove it as a dependency and update the root
               depsIterator.remove();
               record.setDependentSnapshots(deps);
               putRecord(record);
            }
            else{
               returnList.add(incRec);
            }
         }
      }
      catch(Exception e){
         LOG.error("getIncrementalSnapshotSetForRoot Exception " , e);
         throw new IOException(e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getIncrementalSnapshotSetForRoot: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getAdjustedSnapshotSet
    * @param String tag
    * @param StringBuilder backupType
    * @param ArrayList<String> schemaList
    * @param ArrayList<String> tableList
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    *
    * This method takes a String parameter that is the tag associated with the desired snapshot set
    * and retrieves the snapshot set as a list to be used as part of a restore operation.  Any incremental
    * snapshots found in the set associated with the user tag are converted to the real root snapshot
    */
   public ArrayList<SnapshotMetaRecord> getAdjustedSnapshotSet(String tag, StringBuilder backupType) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet for tag " + tag);
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;
      //SnapshotMetaStartRecord startRec = null;
      long snapshotStopId = 0;
      boolean tagFound = false;
      boolean done = false;
      boolean incrementalBackup = false;
      String tableNameString = null;

      try {
         // Strategy is the retrieve the most recent start record using a reverse scan.
         // Then we position a new scan using the start record key and perform a forward
         // scan to retrieve the remaining records in the set.
         //startRec = getSnapshotStartRecord(tag);
         SubstringComparator comp = new SubstringComparator(tag);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
         //Scan s = new Scan(longToByteArray(startRec.getKey()));
         Scan s = new Scan();
         // using tag index
         SnapshotTagIndexRecord tagIndexRecord = getSnapshotTagIndex(tag);
         // the tag index is exists and has valid endkey then set scanning range 
         if (tagIndexRecord != null) {
            s.setStartRow(Bytes.toBytes(tagIndexRecord.getStartKey()));
            if (tagIndexRecord.getEndKey() > 0) {
               s.setStopRow(Bytes.toBytes(tagIndexRecord.getEndKey()));
            }
         }
         s.setFilter(filter);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (done) {
                   break;
                }
                //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet tag " + tag + " currKey is " + currKey);
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getAdjustedSnapshotSet(tag) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) currKey is " + currKey +" record is a SnapshotMetaStartRecord ");
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // must ignore the following snapshots until we find another
                            // snapshot that completed successfully.
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) found a partial snapshot set for key "
                            //                    + currKey + " but it never completed; ignoring");
                            continue;
                         }

                         // We found a snapshot start that was completed.  Let's see if the tag is the one we want
                         if (userTagString.equals(tag)) {
                            String completionTimeString  = st.nextToken();
                            snapshotStopId = Long.parseLong(completionTimeString, 10);
                            String fuzzyBackupString       = st.nextToken();
                            String markedForDeletionString = st.nextToken();
                            String incrementalBackupString = st.nextToken();
                            String firstIncrementalString  = st.nextToken();
                            if(firstIncrementalString.contains("true")){
                            }
                            String extendedAttributes      = st.nextToken();
                            String backupTypeString        = st.nextToken();
                            String timeString              = st.nextToken();
                            backupType.append(backupTypeString);
                            tagFound = true;
                            incrementalBackup = incrementalBackupString.contains("true");
                            if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) found a full snapshot for tag "
                                      + userTagString + ", markedForDeletion " + markedForDeletionString + " extendedAttributes " + extendedAttributes
                                      + " incrementalBackup " + incrementalBackup + " backupType " + backupTypeString
                                      + " timeString " + timeString);
                            continue;
                         }
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) currKey is " + currKey +" record is a SnapshotMetaRecord ");
                         // This is a SnapshotMetaRecord, but if we haven't found the tag we are
                         // looking for in a start record we ignore it rather than include it in the returnList
                         if (tagFound != true) {
                            // if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag)  Ignoring snapshot record for key "  + currKey);
                            continue;
                         }

                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) Snapshot record key "  + currKey
                            //          + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString = st.nextToken();
                         String sqlMetaDataString = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString    = st.nextToken();
                         String mutationStartTimeString     = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();
                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getAdjustedSnapshotSet(tag) exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("getAdjustedSnapshotSet(tag) exception parsing dependent snapshots " + dependentSnapshotString, e );
                         }

                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey,
                                  supersedingSnapshot,
                                  restoredSnapshot, restoreStartTime,
                                  hiatusTime,
                                  tableNameString, userTagString,
                                  snapshotNameString, snapshotPathString,
                                  userGeneratedString.contains("true"),
                                  incrementalTableString.contains("true"),
                                  sqlMetaDataString.contains("true"),
                                  inLocalFsString.contains("true"),
                                  excludedString.contains("true"),
                                  mutationStartTime,
                                  archivePathString, mutationStatements,
                                  numLobs, lobSnapshots, depSnapshots);

                         if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) " + tag
                                + " adding record to returnList, record: " + record);
                         returnList.add(record);
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {

                         String rootSnapshotString  = st.nextToken();
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) found incremental snapshot record "
                         //        + currKey + " with root " + rootSnapshotString);
                         long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
                         if (tagFound != true) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag)  Ignoring incremental "
                            //         + "snapshot record for key "  + currKey);
                           continue;
                         }

                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) incremental Snapshot record key "  + currKey
                            //           + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }

                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) getting root snapshot record with key "
                         //        + rootSnapshot);
                         record = getSnapshotRecord(rootSnapshot);
                         if (record == null){
                            String parentSnapshotString = st.nextToken();
                            String childSnapshotString = st.nextToken();
                            String priorMutationStartTimeString = st.nextToken();
                            tableNameString    = st.nextToken();
                            String userTagString      = st.nextToken();

                            IOException ioe = new IOException ("getAdjustedSnapshotSet could not find record with key " + rootSnapshot
                                    + " for incremental record " + currKey + " on table " + tableNameString + " and tag " + userTagString);
                            LOG.error("Error getAdjustedSnapshotSet ", ioe);
                            //throw ioe;
                         }
                         else {
                            if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(tag) " + tag
                                  + " adding real snapshot record to returnList, record: " + record);
                            returnList.add(record);
                         }
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getAdjustedSnapshotSet(tag) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getAdjustedSnapshotSet(tag) Exception setting up scanner " , e);
          throw new IOException(e);
      }
/*
      // Now we have a superset of all snapshots from the tag.  We will check to see if we need to cull the list
      // based on the schemaList and tableList passed in by the user.
      if ((schemaList.size() != 0)  || (tableList.size() != 0)){
         // We need to make sure the list we return matches one of the entries
         // in the schemaList or tableList
         for (SnapshotMetaRecord tmpRecord : returnList) {
            String  tblName = tmpRecord.getTableName();
            if ((! isTableOnSchemaList(tblName, schemaList)) && (! isTableOnTableList(tblName, tableList))){
               if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(tag): removing snapshot " + tmpRecord.getKey());
               returnList.remove(tmpRecord.getKey());
            }
         }
      }
*/
      if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet tag: " + tag
                 + " returning " + returnList.size() + " records");
      return returnList;
//      throw new IOException("test");
   }

   /**
    * getAdjustedSnapshotSet
    * @param long key
    * @param StringBuilder backupType
    * @param ArrayList<String> schemaList
    * @param ArrayList<String> tableList
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    *
    * This method takes a long parameter that is the key associated with the desired snapshot set
    * and retrieves the snapshot set as a list to be used as part of a restore operation.  Any incremental
    * snapshots found in the set associated with the user tag are converted to the real root snapshot
    */
   public ArrayList<SnapshotMetaRecord> getAdjustedSnapshotSet(long key, StringBuilder backupType) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet for key " + key);
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;
      SnapshotMetaStartRecord startRec = null;
      long snapshotStopId = 0;
      boolean tagFound = false;
      boolean done = false;
      boolean incrementalBackup = false;
      String tableNameString = null;
      String tag = null;

      try {
         // Strategy is the retrieve the most recent start record using a reverse scan.
         // Then we position a new scan using the start record key and perform a forward
         // scan to retrieve the remaining records in the set.
         startRec = getSnapshotStartRecord(key);
         tag = startRec.getUserTag();
         SubstringComparator comp = new SubstringComparator(tag);
         SingleColumnValueFilter filter = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.EQUAL, comp);
         Scan s = new Scan(longToByteArray(key));
         // get tag index
         SnapshotTagIndexRecord tagIndexRecord = getSnapshotTagIndex(tag);
         // if tag index is exists and has vaild endkey then set start and stop key
         if (tagIndexRecord != null) {
            // scan reverse: from endkey to startkey
            s = new Scan();
            s.setStopRow(Bytes.toBytes(tagIndexRecord.getEndKey()));
            s.setStartRow(Bytes.toBytes(tagIndexRecord.getStartKey()));
         }
         s.setFilter(filter);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (done) {
                   break;
                }
                //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet key " + key + " currKey is " + currKey);
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getAdjustedSnapshotSet(key) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) currKey is " + currKey +" record is a SnapshotMetaStartRecord ");
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // must ignore the following snapshots until we find another
                            // snapshot that completed successfully.
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) found a partial snapshot set for key "
                            //                    + currKey + " but it never completed; ignoring");
                            continue;
                         }

                         // We found a snapshot start that was completed.  Let's see if the tag is the one we want
                         if (userTagString.equals(tag)) {
                            String completionTimeString  = st.nextToken();
                            snapshotStopId = Long.parseLong(completionTimeString, 10);
                            String fuzzyBackupString       = st.nextToken();
                            String markedForDeletionString = st.nextToken();
                            String incrementalBackupString = st.nextToken();
                            String firstIncrementalString  = st.nextToken();
                            if(firstIncrementalString.contains("true")){
                            }
                            String extendedAttributes      = st.nextToken();
                            String backupTypeString        = st.nextToken();
                            String timeString              = st.nextToken();
                            backupType.append(backupTypeString);
                            tagFound = true;
                            incrementalBackup = incrementalBackupString.contains("true");
                            if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) found a full snapshot for tag "
                                      + userTagString + ", markedForDeletion " + markedForDeletionString + " extendedAttributes " + extendedAttributes
                                      + " incrementalBackup " + incrementalBackup + " backupType " + backupTypeString
                                      + " timeString " + timeString);
                            continue;
                         }
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) currKey is " + currKey +" record is a SnapshotMetaRecord ");
                         // This is a SnapshotMetaRecord, but if we haven't found the tag we are
                         // looking for in a start record we ignore it rather than include it in the returnList
                         if (tagFound != true) {
                            // if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key)  Ignoring snapshot record for key "  + currKey);
                            continue;
                         }

                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) Snapshot record key "  + currKey
                            //          + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString = st.nextToken();
                         String sqlMetaDataString = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString    = st.nextToken();
                         String mutationStartTimeString     = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();
                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getAdjustedSnapshotSet(key) exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("getAdjustedSnapshotSet(key) exception parsing dependent snapshots " + dependentSnapshotString, e );
                         }

                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey,
                                  supersedingSnapshot,
                                  restoredSnapshot, restoreStartTime,
                                  hiatusTime,
                                  tableNameString, userTagString,
                                  snapshotNameString, snapshotPathString,
                                  userGeneratedString.contains("true"),
                                  incrementalTableString.contains("true"),
                                  sqlMetaDataString.contains("true"),
                                  inLocalFsString.contains("true"),
                                  excludedString.contains("true"),
                                  mutationStartTime,
                                  archivePathString, mutationStatements,
                                  numLobs, lobSnapshots, depSnapshots);

                         if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) " + tag
                                + " adding record to returnList, record: " + record);
                         returnList.add(record);
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {

                         String rootSnapshotString  = st.nextToken();
                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) found incremental snapshot record "
                         //        + currKey + " with root " + rootSnapshotString);
                         long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
                         if (tagFound != true) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key)  Ignoring incremental "
                            //         + "snapshot record for key "  + currKey);
                           continue;
                         }

                         // We have found the tag we are looking for, but now we need to ensure the
                         // current record is not beyond the stopId of the full snapshot
                         if (currKey > snapshotStopId) {
                            //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) incremental Snapshot record key "  + currKey
                            //           + " is greater than the stopId " + snapshotStopId + ".  Set is complete");
                            done = true;
                            break;
                         }

                         //if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) getting root snapshot record with key "
                         //        + rootSnapshot);
                         record = getSnapshotRecord(rootSnapshot);
                         if (record == null){
                            IOException ioe = new IOException ("getAdjustedSnapshotSet could not find record with key " + rootSnapshot);
                            LOG.error("Error getAdjustedSnapshotSet ", ioe);
                            throw ioe;
                         }
                         if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) " + tag
                               + " adding real snapshot record to returnList, record: " + record);
                         returnList.add(record);
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getAdjustedSnapshotSet(key) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getAdjustedSnapshotSet(key) Exception setting up scanner " , e);
          throw new IOException(e);
      }
/*
      // Now we have a superset of all snapshots from the tag.  We will check to see if we need to cull the list
      // based on the schemaList and tableList passed in by the user.
      if ((schemaList.size() != 0)  || (tableList.size() != 0)){
         // We need to make sure the list we return matches one of the entries
         // in the schemaList or tableList
         for (SnapshotMetaRecord tmpRecord : returnList) {
            String  tblName = tmpRecord.getTableName();
            if ((! isTableOnSchemaList(tblName, schemaList)) && (! isTableOnTableList(tblName, tableList))){
               if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(key): removing snapshot " + tmpRecord.getKey());
               returnList.remove(tmpRecord.getKey());
            }
         }
      }
*/
      if (LOG.isTraceEnabled()) LOG.trace("getAdjustedSnapshotSet(key) tag: " + tag
                 + " returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getPriorSnapshotSet
    * @param long key
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    * 
    * This method takes a timeId and retrieves a snapshot set for all snapshots
    * between the prior completed full snapshot and the timeId provided, including
    * additional partial snapshots associated with DDL operations or partial
    * snapshots as part of another full snapshot that has been initiated, but not completed
    */
   public ArrayList<SnapshotMetaRecord> getPriorSnapshotSet(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(key) start for key " + key);
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;
      SnapshotMetaStartRecord startRec = null;

      // Strategy is the retrieve the most recent start record using a reverse scan.
      // Then we position a new scan using the start record key and perform a forward
      // scan to retrieve the remaining records in the set.
      try {
          startRec = getPriorStartRecord(key);
          SubstringComparator comp1 = new SubstringComparator("_SAVE_");
          SubstringComparator comp2 = new SubstringComparator("_BRC_");
          SingleColumnValueFilter filter1 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp1);
          SingleColumnValueFilter filter2 = new SingleColumnValueFilter(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, CompareOp.NOT_EQUAL, comp2);
          FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
          filterList.addFilter(filter1);
          filterList.addFilter(filter2);
          Scan s = new Scan(longToByteArray(startRec.getKey()));
          s.setFilter(filterList);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          s.setReversed(true);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (currKey >= key){
                    break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getPriorSnapshotSet(key) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         String userTagString = st.nextToken();
                         //Eliminate _SAVE_,_BRC_, _RESTORED_, SNAPSHOT_PEER_ backups
                         if (userTagString.contains("_SAVE_") ||
                             userTagString.contains("_BRC_")) {
                            continue;
                         }
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // continue as if this record didn't exist and add additional 
                            // partial snapshots if there are any that fit in our time frame
                            continue;
                         }

                         // We found a snapshot start that was completed, so anything already in the
                         // returnList is invalid.  We need to empty the returnList and start
                         // building it from here.
                     	 //
                         // Note that the current record we are reading is a SnapshotMetaStartRecord, not a SnapshotMetaRecord,
                         // so we skip it rather than add it into the list
                     	 returnList.clear();
                         continue;
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         String supersedingSnapshotString = st.nextToken();
                         long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
                         if (supersedingSnapshot != -1 && supersedingSnapshot < key){
                            // This is a valid snapshot that is less than the key passed in, however
                            // a subsequent snapshot was performed on this table that now supersedes
                            // this snapshot.  We must skip this snapshot and only included the superseding
                            // snapshot in the snapshot set.
                            continue;
                         }
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         String userGeneratedString = st.nextToken();
                         String incrementalTableString = st.nextToken();
                         String sqlMetaDataString = st.nextToken();
                         String inLocalFsString    = st.nextToken();
                         String excludedString    = st.nextToken();
                         String mutationStartTimeString     = st.nextToken();
                         long mutationStartTime;
                         // This is to cover for metadata migration for an unused parameter.
                         if (mutationStartTimeString.contains("false")){
                            mutationStartTime = 0L;
                         }
                         else{
                            mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
                         }
                         String archivePathString  = st.nextToken();
                         String mutationStatements = st.nextToken();
                         String numLobsString      = st.nextToken();
                         int numLobs = Integer.parseInt(numLobsString);
                         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String lobSnapshotString = null;
                         try {
                           for (int i = 0; i < numLobs; i++) {
                             lobSnapshotString = st.nextElement().toString();
                             lobSnapshots.add(Long.parseLong(lobSnapshotString));
                           }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet(key) exception parsing lob snapshots " + lobSnapshotString, e );
                         }

                         Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
                         String dependentSnapshotString = null;
                         try {
                            while (st.hasMoreElements()) {
                               dependentSnapshotString = st.nextElement().toString();
                               depSnapshots.add(Long.parseLong(dependentSnapshotString));
                            }
                         } catch (Exception e) {
                            LOG.error("getPriorSnapshotSet(key) exception parsing dependent snapshots " + dependentSnapshotString, e );
                         }
                         long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
                         long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
                         long hiatusTime = Long.parseLong(hiatusTimeString, 10);

                         record = new SnapshotMetaRecord(currKey,
                                supersedingSnapshot, restoredSnapshot,
                                restoreStartTime, hiatusTime,
                                tableNameString, userTagString,
                                snapshotNameString, snapshotPathString,
                                userGeneratedString.contains("true"),
                                incrementalTableString.contains("true"),
                                sqlMetaDataString.contains("true"),
                                inLocalFsString.contains("true"),
                                excludedString.contains("true"),
                                mutationStartTime,
                                archivePathString, mutationStatements,
                                numLobs, lobSnapshots, depSnapshots);

                         returnList.add(record);
                      }
                      else {
                         // SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()
                         // SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue()
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorSnapshotSet(key)  Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorSnapshotSet(key) Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (returnList.isEmpty()) {
         throw new Exception("getPriorSnapshotSet(key) Prior record for key " + key + " not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet(key) : returning " + returnList.size() + " records");
      return returnList;	   
   }

   /**
    * getAllPriorSnapshotStartRecords
    * @param long key
    * @return ArrayList<SnapshotMetaStartRecord> set
    * @throws Exception
    * 
    * This method takes a timeId and retrieves a snapshot start record set for all snapshots
    * prior to the timeId provided.
    * 
    * NOTE: Snapshots that start before the specified time and are not complete are returned in
    * the result set, but snapshots that start before the specified time and complete after
    * the specified time are not returned.
    */
   public ArrayList<SnapshotMetaStartRecord> getAllPriorSnapshotStartRecords(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getAllPriorSnapshotStartRecords start for key " + key);
      ArrayList<SnapshotMetaStartRecord> returnList = new ArrayList<SnapshotMetaStartRecord>();
      SnapshotMetaStartRecord record = null;

      try {
          // Strategy is to perform a regular scan and stop at the key value
          RowFilter rowFilter = new RowFilter(CompareOp.LESS, new BinaryComparator(longToByteArray(key)));
          Scan s = new Scan();
          s.setFilter(rowFilter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (currKey >= key){
                    if (LOG.isTraceEnabled()) LOG.trace("getAllPriorSnapshotStartRecords (key) currKey " + currKey
                 		   + " is not less than key " + key + ".  Scan complete");
                    break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                	  // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getAllPriorSnapshotStartRecords(key) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         String completionTimeString    = st.nextToken();
                         String fuzzyBackupString       = st.nextToken();
                         String markedForDeletionString = st.nextToken();
                         String incrementalBackupString = st.nextToken();
                         String firstIncrementalString  = st.nextToken();
                         String extendedAttributes      = st.nextToken();
                         String backupType              = st.nextToken();
                         String timeString              = st.nextToken();
                         String completionWallTimeString = null;
                         if(st.hasMoreTokens() == true)
                            completionWallTimeString = st.nextToken();

                         if (snapshotCompleteString.contains("false") ||
                            (snapshotCompleteString.contains("true") && (Long.parseLong(completionTimeString, 10) < key))) {
                            record = new SnapshotMetaStartRecord(currKey, version, userTagString,
                                               snapshotCompleteString.contains("true"),
                                               Long.parseLong(completionTimeString, 10),
                                               fuzzyBackupString.contains("true"),
                                               markedForDeletionString.contains("true"),
                                               incrementalBackupString.contains("true"),
                                               firstIncrementalString.contains("true"),
                                               extendedAttributes,
                                               backupType,
                                               Long.parseLong(timeString, 10));

                         if( completionWallTimeString != null )
                            record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );

                            returnList.add(record);
                         }
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getAllPriorSnapshotStartRecords(key) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getAllPriorSnapshotStartRecords(key) Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (returnList.isEmpty()) {
         throw new Exception("getAllPriorSnapshotStartRecords(key) Prior record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getAllPriorSnapshotStartRecords(key): returning " + returnList.size() + " records");
      return returnList;	   
   }

   public Map<Long,String> getSnapshotMetaList(long startRowKey, long stopRowKey) {
      Scan s = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey));
      if (LOG.isInfoEnabled())
         LOG.info("getSnapshotMetaList, startRowKey:" + startRowKey + "stopRowKey:" + stopRowKey);
      s.setBatch(1000);

      ResultScanner ss;
      try {
         ss = table.getScanner(s);
      } catch (IOException e) {
         LOG.error("getSnapshotNameList, scan error \n" + e);
         return new HashMap<>();
      }

      Map<Long,String> columnMap = new HashMap<>(64);
      for (Result r : ss) {
         String value = Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         SnapshotMetaRecordType type = AbstractSnapshotRecordMeta.getRecordTypeByValue(value);
         if (type == SnapshotMetaRecordType.SNAPSHOT_RECORD || type == SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD) {
            columnMap.put(Bytes.toLong(r.getRow()), value);
         }
      }
      return columnMap;
   }

   /**
    * deleteRecord
    * @param long key
    * @return boolean success
    * @throws Exception
    */
   public static boolean deleteRecord(final long key) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord start for key: " + key);
      try {
         Delete d;
         //create our own hashed key
         d = new Delete(Bytes.toBytes(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteRecord  (" + key + ") ");
         table.delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteRecord Exception " , e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord - exit");
      return true;
   }

   /**
    * isTableOnSchemaList
    * @param String tableName
    * @param ArrayList<String> schemaList
    * @return boolean found
    */
   public static boolean isTableOnSchemaList(final String tableName, final ArrayList<String> schemaList) {
      if (LOG.isTraceEnabled()) LOG.trace("isTableOnSchemaList start for table: " + tableName
            + " schemaList size " + schemaList.size());

      for (String tmpSchema : schemaList){
         if (tableName.startsWith(tmpSchema)){
            if (LOG.isTraceEnabled()) LOG.trace("isTableOnSchemaList table: " + tableName
                       + " is part of schema " + tmpSchema + " returning true");
            return true;
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("isTableOnSchemaList table: " + tableName
                       + " is not part of schemaList returning false");
      return false;
   }

   /**
    * isTableOnTableList
    * @param String tableName
    * @param ArrayList<String> tableList
    * @return boolean found
    */
   public static boolean isTableOnTableList(final String tableName, final ArrayList<String> tableList) {
      if (LOG.isTraceEnabled()) LOG.trace("isTableOnTableList start for table: " + tableName
            + " tableList size " + tableList.size());

      for (String tmpTable : tableList){
         if (tableName.equals(tmpTable)){
            if (LOG.isTraceEnabled()) LOG.trace("isTableOnTableList table: " + tableName
                        + " found in tableList returning true");
            return true;
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("isTableOnTableList table: " + tableName
                        + " is not on tableList returning false");
      return false;
   }

   /**
    * closeConnections
    * @return void
    *
    */
   public void closeConnections() {
     try{
        connection.close();
     }
     catch(Exception e){
        if (LOG.isTraceEnabled()) LOG.trace("closeConnection ignoring caught exception ", e);
     }
   }

   /**
    * translate
    * @param String hash
    * @return String tableName
    * @throws Exception
    *
    * This method takes a String parameter that is the hash associated with the a snapshot record
    * and retrieves the name of the table that is the object of the snapshot.  This is valuable
    * when scanning hdfs directories to see which table is stored in a particular location.
    */
   public String translate(String hash) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("translate for hash " + hash);
      String tableName = "Not Found";
      boolean done = false;

      try {
          // Since there is no MD5 method to translate the hash back to its original
          // value, we must do a table scan and look for the hash in the records.
          Scan s = new Scan();
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (done) {
                   break;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("translate(hash) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         // We are only looking for SnapshotRecords or SnapshotMetaIncrementalRecords
                         continue;
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue())) {

                         String rootSnapshotString = st.nextToken();
                         String parentSnapshotString = st.nextToken();
                         String childSnapshotString = st.nextToken();
                         String priorMutationStartTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         if (snapshotPathString.equals(hash)){
                            tableName = tableNameString;
                            done = true;
                            break;
                         }
                      }
                      else if (recordType == (SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue())) {
                         String supersedingSnapshotString = st.nextToken();
                         String restoredSnapshotString = st.nextToken();
                         String restoreStartTimeString = st.nextToken();
                         String hiatusTimeString = st.nextToken();
                         String tableNameString    = st.nextToken();
                         String userTagString      = st.nextToken();
                         String snapshotNameString = st.nextToken();
                         String snapshotPathString = st.nextToken();
                         if (snapshotPathString.equals(hash)){
                            tableName = tableNameString;
                            done = true;
                            break;
                         }
                      }
                      else {
                         // SNAPSHOT_LOCK_RECORD 
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("translate(hash) Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("translate(hash) Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("translate(hash): returning " + tableName);
      return tableName;
   }
   private byte[] longToByteArray (final long L) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      dos.writeLong(L);
      dos.flush();
      return baos.toByteArray();
   }

   /**
    * getFollowingBRCRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getFollowingBRCRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getFollowingBRCRecord start for key " + key);
      SnapshotMetaStartRecord record = null;

      try {
          // Strategy is to do a forward scan starting from key
          RowFilter rowFilter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(longToByteArray(key)));
          Scan s = new Scan(longToByteArray(key));
          s.setFilter(rowFilter);
          s.setCaching(1000);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                if (record != null) {
                   break;
                }
                long currKey = Bytes.toLong(r.getRow());
                if (currKey <= key){
                   if (LOG.isTraceEnabled()) LOG.trace("getFollowingBRCRecord(key) currKey " + currKey
                                     + " is not greater than key " + key + ";  continuing");
                   continue;
                }
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (st.hasMoreElements()) {
                      // Skip the key
                      st.nextToken();
                      String versionString = st.nextToken();
                      int version = Integer.parseInt(versionString);
                      if (version != SnapshotMetaRecordType.getVersion()){
                         IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                         LOG.error("getFollowingBRCRecord(key) exception ", ioe);
                         throw ioe;
                      }
                      String typeString = st.nextToken();
                      int recordType = Integer.parseInt(typeString);
                      if (recordType == (SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue())) {
                         // We found a full snapshot
                         String userTagString           = st.nextToken();
                         //only need _BRC_ backups
                         if ( ! userTagString.contains("_BRC_")) {
                            continue;
                         }
                         String snapshotCompleteString  = st.nextToken();
                         String completionTimeString    = st.nextToken();
                         String fuzzyBackupString       = st.nextToken();
                         String markedForDeletionString = st.nextToken();
                         String incrementalBackupString = st.nextToken();
                         String firstIncrementalString  = st.nextToken();
                         String extendedAttributes      = st.nextToken();
                         String backupType              = st.nextToken();
                         String timeString              = st.nextToken();
                         String completionWallTimeString = null;
                         if(st.hasMoreTokens() == true)
                            completionWallTimeString = st.nextToken();
                         if (snapshotCompleteString.contains("false")) {
                            continue;
                         }
                         record = new SnapshotMetaStartRecord(currKey, Integer.parseInt(versionString), userTagString,
                                          snapshotCompleteString.contains("true"),
                                          Long.parseLong(completionTimeString, 10),
                                          fuzzyBackupString.contains("true"),
                                          markedForDeletionString.contains("true"),
                                          incrementalBackupString.contains("true"),
                                          firstIncrementalString.contains("true"),
                                          extendedAttributes,
                                          backupType,
                                          Long.parseLong(timeString, 10));

                         if( completionWallTimeString != null )
                            record.setCompletionWallTime(Long.parseLong(completionWallTimeString, 10) );
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getFollowingBRCRecord(key)  Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getFollowingBRCRecord(key)  Exception setting up scanner " , e);
          throw new IOException(e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getFollowingBRCRecord(key) returning " + record);
      return record;
   }

   /**
    * insert data into hbase
    * @param meta meta
    * @throws Exception e
    */
   public void putRecordMeta(AbstractSnapshotRecordMeta meta) throws Exception {
      Put p = getRecordMetaPut(meta);
      if(null == p){
         return;
      }

      int retries = 0;
      boolean complete = false;
      Object keyString = meta.getKey();
      int recordType = meta.getRecordType();
      do {
         retries++;
         try {
            table.put(p);
            complete = true;
            if (retries > 1) {
               if (LOG.isTraceEnabled())
                  LOG.trace("Retry successful in putRecord (" + recordType + ") for key: " + keyString);
            }
         } catch (Exception e2) {
            LOG.error("Retry " + retries + " putRecord for key: " + keyString
                    + " type " + recordType + " due to Exception ", e2);
            RegionLocator locator = connection.getRegionLocator(table.getName());
            try {
               locator.getRegionLocation(p.getRow(), true);
            } catch (IOException ioe) {
               LOG.error("putRecord caught Exception getting region location ", ioe);
               throw ioe;
            }
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount) {
               LOG.error("putRecord " + recordType + " aborting due to excessive retries for key: " + keyString + " due to " +
                       "Exception; aborting ");
               System.exit(1);
            }
         }
      } while (!complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
   }

   /**
    * insert data into hbase
    * @param meta meta
    * @throws Exception e
    */
   private Put getRecordMetaPut(AbstractSnapshotRecordMeta meta) throws Exception {
      // if the key is null then no need to put record
      if (meta.getKey() == null) {
         return null;
      }
      Put p;
      // if the type of key is long then we will transfer to bytes
      if(meta.getKey() instanceof  Long){
         p = new Put(Bytes.toBytes((Long)meta.getKey()));
      }else{
         p = new Put(Bytes.toBytes(String.valueOf(meta.getKey())));
      }
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, Bytes.toBytes(meta.getColumn()));
      if (LOG.isTraceEnabled())
         LOG.trace("putRecordMeta meta: " + meta);
      return p;
   }

   public List<SnapshotTagIndexRecord> getSnapshotTagIndexList(String startTagName, String stopTagName) {
      List<SnapshotTagIndexRecord> indexList = new ArrayList<>(16);

      Scan s = new Scan();
      s.setFilter(new PrefixFilter(Bytes.toBytes(SnapshotTagIndexRecord.TAG_PREFIX)));
      s.setBatch(1000);
      if (AbstractSnapshotRecordMeta.isNotEmpty(startTagName)) {
         String startRowKey = SnapshotTagIndexRecord.getRowKey(startTagName);
         if (startRowKey != null) { s.setStartRow(Bytes.toBytes(startRowKey)); }
      }
      if (AbstractSnapshotRecordMeta.isNotEmpty(stopTagName)) {
         String stopRowKey = SnapshotTagIndexRecord.getRowKey(stopTagName);
         if (stopRowKey != null) { s.setStopRow(Bytes.toBytes(stopRowKey)); }
      }
      ResultScanner ss;
      try {
         ss = table.getScanner(s);
      } catch (IOException e) {
         LOG.error("getSnapshotTagIndexList, scan error \n" + e);
         return new ArrayList<>();
      }

      for (Result r : ss) {
         String value = Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         SnapshotTagIndexRecord record = new SnapshotTagIndexRecord(value);
         indexList.add(record);
      }
      return indexList;
   }

   /**
    * Get the RowKey range of two regular backups according to tagName
    * @param tagName tagName
    * @return RowKey range
    */
   public Pair<byte[],byte[]> getRegularRangeByTagName(String tagName) {
      byte[] startRowKey = HConstants.EMPTY_START_ROW;
      byte[] endRowKey = HConstants.EMPTY_END_ROW;

      SnapshotTagIndexRecord currentTagIndex = getSnapshotTagIndex(tagName);
      if (null == currentTagIndex) {
         return Pair.newPair(startRowKey, endRowKey);
      }
      List<SnapshotTagIndexRecord> indexList = getSnapshotTagIndexList(null, null);
      /*
       * Can be replaced with lambda expressions, but EsgynDB does not support JDK1.8.
       * Sort to determine the tag to be deleted.
       */
      indexList.sort(new Comparator<SnapshotTagIndexRecord>() {
         @Override
         public int compare(SnapshotTagIndexRecord o1, SnapshotTagIndexRecord o2) {
            return Long.compare(o1.getStartKey(), o2.getStartKey());
         }
      });

      try {
         for (SnapshotTagIndexRecord record : indexList) {
            // check tag
            SnapshotMetaStartRecord startRecord = getSnapshotStartRecord(record.getStartKey());
            if (startRecord != null && !startRecord.getIncrementalBackup()) {
               if (record.getStartKey() <= currentTagIndex.getStartKey()) {
                  startRowKey = Bytes.toBytes(record.getStartKey());
               } else {
                  endRowKey = Bytes.toBytes(record.getEndKey());
                  break;
               }
            }
         }
      } catch (Exception e) {
         LOG.error("getRegularRangeByTagName get record failed,", e);
      }

      return Pair.newPair(startRowKey, endRowKey);
   }

   public List<byte[]> getBrcRowKey(byte[] startRowKey, byte[] endRowKey) {
      String snapshotKeyWord = "NON_TX_SNAPSHOT";
      String brcKeyWord = "_BRC_";
      List<byte[]> rowKeyList = new ArrayList<>(64);
      Scan s = new Scan(startRowKey, endRowKey);
      s.setBatch(1000);
      ResultScanner ss;
      try {
         ss = table.getScanner(s);
      } catch (IOException e) {
         LOG.error("getMutationMetaByTag, scan error \n" + e);
         return rowKeyList;
      }
      for (Result r : ss) {
         String value = Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         if (AbstractSnapshotRecordMeta.isEmpty(value)) { continue; }
         if (value.contains(snapshotKeyWord) && value.contains(brcKeyWord)) {
            rowKeyList.add(r.getRow());
         }
      }

      return rowKeyList;
   }
   /**
    * get tag index according tag name
    * @param tagName
    * @return
    */
   public SnapshotTagIndexRecord getSnapshotTagIndex(String tagName) {
      try {
         long start = System.currentTimeMillis();
         String rowKey = SnapshotTagIndexRecord.getRowKey(tagName);
         if (AbstractSnapshotRecordMeta.isEmpty(rowKey)) { return null; }
         Get get = new Get(Bytes.toBytes(rowKey));
         Result result = table.get(get);
         if (result.isEmpty()) {
            rowKey = SnapshotTagIndexRecord.getOldRowKey(tagName);
            if (AbstractSnapshotRecordMeta.isEmpty(rowKey)) { return null; }
            get = new Get(Bytes.toBytes(rowKey));
            result = table.get(get);
         }
         String columnValue = Bytes.toString(result.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         if (AbstractSnapshotRecordMeta.isNotEmpty(columnValue)) {
            if (LOG.isTraceEnabled())
               LOG.trace("getSnapshotTagIndex tagName:" + tagName + " found time: " + (System.currentTimeMillis() - start) + "ms " +
                    "column" + ": " + columnValue);
            return new SnapshotTagIndexRecord(columnValue);
         } else {
            LOG.warn("SnapshotTagIndexRecord tagName:" + tagName + " not found");
         }
      } catch (Exception e) {
         LOG.warn("catch: SnapshotTagIndexRecord tagName:" + tagName + " not found");
      }
      return null;
   }


   /**
    * get table index according table name
    * @param tableName
    * @return
    */
   public SnapshotTableIndexRecord getSnapshotTableIndex(String tableName) {
      try {
         long start = System.currentTimeMillis();
         String rowKey = SnapshotTableIndexRecord.getRowKey(tableName);
         Get get = new Get(Bytes.toBytes(rowKey));
         Result result = table.get(get);
         String columnValue = Bytes.toString(result.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         if (LOG.isTraceEnabled())
            LOG.trace("getSnapshotTableIndex tableName:" + tableName + " found time: " + (System.currentTimeMillis() - start) + " " +
                 "column" + ": " + columnValue);
         return new SnapshotTableIndexRecord(columnValue);
      } catch (Exception e) {
         LOG.warn("SnapshotTableIndex tableName:" + tableName + " not found");
      }
      return null;
   }

   /**
    * batch query SnapshotMetaIncrementalRecord data
    * @param keys
    * @return
    */
   public List<SnapshotMetaIncrementalRecord> getIncrementalSnapshotRecords(Set<Long> keys) {
      long start = System.currentTimeMillis();
      if (keys.isEmpty()) {
         return Collections.emptyList();
      }
      List<SnapshotMetaIncrementalRecord> list = new ArrayList<>(keys.size());
      List<Get> gets = new ArrayList<>(keys.size());
      for (Long key : keys) {
         if (!key.equals(-1L)) {
            gets.add(new Get(Bytes.toBytes(key)));
         }
      }
      if (!gets.isEmpty()) {
         try {
            Result[] results = table.get(gets);
            for (Result result : results) {
               if (result.isEmpty()){
                  continue;
               }
               SnapshotMetaIncrementalRecord incrementalRecord = resultToIncrementalSnapshotRecord(result);
               if (null != incrementalRecord) {
                  list.add(incrementalRecord);
               }
            }
         } catch (IOException e) {
            IOException ioe = new IOException("StackTrace");
            LOG.error("getIncrementalSnapshotRecords unable to retrieve SnapshotMetaIncrementalRecord for keys"
                    + keys, ioe);
         } catch (Exception e) {
            IOException ioe = new IOException("StackTrace");
            LOG.error("getIncrementalSnapshotRecords resultToIncrementalSnapshotRecord error", ioe);
         }
      }
      long end = System.currentTimeMillis();
      if (LOG.isTraceEnabled())
         LOG.trace("getIncrementalSnapshotRecords keys: " + keys + " time " + (end - start));
      return list;
   }

   public SnapshotMetaIncrementalRecord resultToIncrementalSnapshotRecord(Result r) throws Exception {
      SnapshotMetaIncrementalRecord record;
      try {
         if (r == null) {
            IOException ioe = new IOException("StackTrace");
            LOG.warn("getIncrementalSnapshotRecord unable to retrieve SnapshotMetaIncrementalRecord for key", ioe);
            return null;
         }
         if (r.isEmpty()) {
            IOException ioe2 = new IOException("StackTrace");
            LOG.warn("getIncrementalSnapshotRecord retrieved empty Result for key", ioe2);
            return null;
         }
         long key = Bytes.toLong(r.getRow());
         StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
         // skip the key string
         st.nextToken();
         String versionString = st.nextToken();
         int version = Integer.parseInt(versionString);
         if (version != SnapshotMetaRecordType.getVersion()) {
            IOException ioe = new IOException("Unexpected record version found: "
                    + version + " expected: " + SnapshotMetaRecordType.getVersion());
            LOG.error("getIncrementalSnapshotRecord exception ", ioe);
            throw ioe;
         }

         String recordTypeString = st.nextToken();
         if (Integer.parseInt(recordTypeString) != SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()) {
            LOG.warn("getIncrementalSnapshotRecord did not find a SnapshotMetaRecord for key "
                    + key + " Type found " + recordTypeString);
            return null;
         }

         String rootSnapshotString = st.nextToken();
         long rootSnapshot = Long.parseLong(rootSnapshotString, 10);
         String parentSnapshotString = st.nextToken();
         String childSnapshotString = st.nextToken();
         String priorMutationStartTimeString = st.nextToken();
         String tableNameString = st.nextToken();
         String userTagString = st.nextToken();
         String snapshotNameString = st.nextToken();
         String snapshotPathString = st.nextToken();
         String inLocalFsString = st.nextToken();
         String excludedString = st.nextToken();
         String archivedString = st.nextToken();
         String archivePathString = st.nextToken();
         String mutationStatements = st.nextToken();
         String numLobsString = st.nextToken();
         int numLobs = Integer.parseInt(numLobsString);
         Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
         String lobSnapshotString = null;
         try {
            for (int i = 0; i < numLobs; i++) {
               lobSnapshotString = st.nextElement().toString();
               lobSnapshots.add(Long.parseLong(lobSnapshotString));
            }
         } catch (Exception e) {
            LOG.error("getIncrementalSnapshotRecord exception parsing lob snapshots " + lobSnapshotString, e);
         }

         long parentSnapshot = Long.parseLong(parentSnapshotString, 10);
         long childSnapshot = Long.parseLong(childSnapshotString, 10);
         long priorMutationStartTime = Long.parseLong(priorMutationStartTimeString, 10);

         record = new SnapshotMetaIncrementalRecord(key,
                 version, rootSnapshot, parentSnapshot,
                 childSnapshot, priorMutationStartTime,
                 tableNameString, userTagString,
                 snapshotNameString, snapshotPathString,
                 inLocalFsString.contains("true"),
                 excludedString.contains("true"),
                 archivedString.contains("true"),
                 archivePathString, mutationStatements,
                 numLobs, lobSnapshots);

      } catch (Exception e1) {
         LOG.error("getIncrementalSnapshotRecord Exception ", e1);
         throw e1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getIncrementalSnapshotRecord end; returning " + record);
      return record;
   }


   /**
    * delete start record data,end data,tag index
    * @param key
    * @return
    * @throws IOException
    */
   public boolean deleteSanpShotStartRecord(final long key) throws IOException {
      long start = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) LOG.trace("deleteSanpShotStartRecord start for key: " + key);
      try {
         //1?get snapshotStart data
         SnapshotMetaStartRecord startRecord = getSnapshotStartRecord(key);
         if(null != startRecord){
            List<Delete> deletes = new ArrayList<>();
            SnapshotTagIndexRecord tagIndex = getSnapshotTagIndex(startRecord.getUserTag());
            if(null != tagIndex){
               //delete tagIndex
               deletes.add(new Delete(Bytes.toBytes(tagIndex.getKey().toString())));
               //delete snapshot end data
               deletes.add(new Delete(Bytes.toBytes(tagIndex.getEndKey())));
            }
            //delete start key
            deletes.add(new Delete(Bytes.toBytes(key)));
            deleteRecords(deletes);
         }
      }
      catch (Exception e) {
         LOG.error("deleteSanpShotStartRecord Exception " , e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteSanpShotStartRecord - exit");
      long end = System.currentTimeMillis();
      if (LOG.isInfoEnabled())
         LOG.info("deleteSanpShotStartRecord time is "+ (end - start));
      return true;
   }

   /**
    * batch delete recoreds
    * @param deletes
    * @return
    * @throws IOException
    */
   public static boolean deleteRecords(final List<Delete> deletes) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecords start for key: " + deletes.size());
      try {
         if (LOG.isTraceEnabled()) LOG.trace("deleteRecords  (" + deletes.size() + ") ");
         table.delete(deletes);
      }
      catch (Exception e) {
         LOG.error("deleteRecords Exception " , e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecords - exit");
      return true;
   }



   /**
    * @param keys snapshot table keys
    * @return
    * @throws IOException
    */
   public static boolean deleteRecordsKeys(final List<Long> keys) throws IOException {
      List<Delete> deletes = new ArrayList<>(keys.size());
      for (Long key : keys) {
         deletes.add(new Delete(Bytes.toBytes(key)));
      }
      return deleteRecords(deletes);
   }


   /**
    * batch put incremental records,call by RecoveryRecord,do not update index
    * @param records
    * @throws Exception
    */
   public void batchPutIncrementalRecords(final List<SnapshotMetaIncrementalRecord> records) throws Exception {
      List<Put> puts = new ArrayList<>();
      for (SnapshotMetaIncrementalRecord record : records) {
         puts.add(snapshotMetaIncrementalRecord2Put(record));
      }
      putRecords(puts);
   }

   /**
    * SnapshotMetaIncrementalRecord to put
    * @param record
    * @return
    * @throws Exception
    */
   private Put snapshotMetaIncrementalRecord2Put(SnapshotMetaIncrementalRecord record) throws Exception {
      if (record == null) {
         IOException ioe = new IOException("putRecord SnapshotMetaIncrementalRecord input record is null ");
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }
      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for INCREMENTAL snapshot with tag "
              + record.getUserTag() + " record " + record);
      long key = record.getKey();
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue()) {
         IOException ioe = new IOException("putRecord SnapshotMetaIncrementalRecord found record with incorrect type " + record);
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      StringBuilder lobStringBuilder = new StringBuilder();
      List<String> lobStringList = new ArrayList<String>();
      Set<Long> lobSnaps = record.getLobSnapshots();
      Iterator<Long> lobIt = lobSnaps.iterator();
      int current = 0;
      int count = record.getNumLobs();

      while (lobIt.hasNext() && current < count) {
         Long ls = lobIt.next();
         if (current != 0) {
            lobStringBuilder.append(",");
         }
         lobStringList.add(String.valueOf(ls));
         lobStringBuilder.append(String.valueOf(ls));
         current++;
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
              Bytes.toBytes(String.valueOf(key) + ","
                      + String.valueOf(record.getVersion()) + ","
                      + String.valueOf(record.getRecordType()) + ","
                      + String.valueOf(record.getRootSnapshotId()) + ","
                      + String.valueOf(record.getParentSnapshotId()) + ","
                      + String.valueOf(record.getChildSnapshotId()) + ","
                      + String.valueOf(record.getPriorMutationStartTime()) + ","
                      + record.getTableName() + ","
                      + record.getUserTag() + ","
                      + record.getSnapshotName() + ","
                      + record.getSnapshotPath() + ","
                      + String.valueOf(record.getInLocalFS()) + ","
                      + String.valueOf(record.getExcluded()) + ","
                      + String.valueOf(record.getArchived()) + ","
                      + record.getArchivePath() + ","
                      + record.getMetaStatements() + ","
                      + record.getNumLobs() + ","
                      + lobStringBuilder.toString()));
      return p;
   }

   /**
    * batch put snapshot meta records,call by RecoveryRecord,do not update index
    * @param records
    * @throws Exception
    */
   public void batchPutSnapshotRecords(final List<SnapshotMetaRecord> records) throws Exception {
      List<Put> puts = new ArrayList<>();
      for (SnapshotMetaRecord record : records) {
         puts.add(snapshotMetaRecord2Put(record));
      }
      putRecords(puts);
   }

   /**
    * SnapshotMetaRecord to put
    * @param record
    * @return
    * @throws Exception
    */
   private Put snapshotMetaRecord2Put(SnapshotMetaRecord record) throws Exception {
      if (record == null) {
         IOException ioe = new IOException("putRecord SnapshotMetaRecord input record is null ");
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      long key = record.getKey();
      if (record.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()) {
         IOException ioe = new IOException("putRecord SnapshotMetaRecord found record with incorrect type " + record);
         if (LOG.isTraceEnabled())
            LOG.trace("Error: ", ioe);
         throw ioe;
      }

      StringBuilder lobStringBuilder = new StringBuilder();
      List<String> lobStringList = new ArrayList<String>();
      Set<Long> lobSnaps = record.getLobSnapshots();
      Iterator<Long> lobIt = lobSnaps.iterator();
      int current = 0;
      int count = record.getNumLobs();

      while (lobIt.hasNext() && current < count) {
         Long ls = lobIt.next();
         if (current != 0) {
            lobStringBuilder.append(",");
         }
         lobStringList.add(String.valueOf(ls));
         lobStringBuilder.append(String.valueOf(ls));
         current++;
      }

      StringBuilder snapshotStringBuilder = new StringBuilder();
      List<String> snapshotStringList = new ArrayList<String>();
      Set<Long> dependentSnaps = record.getDependentSnapshots();
      Iterator<Long> it = dependentSnaps.iterator();
      boolean first = true;
      while (it.hasNext()) {
         Long ss = it.next();
         if (first) {
            first = false;
         } else {
            snapshotStringBuilder.append(",");
         }
         snapshotStringList.add(String.valueOf(ss));
         snapshotStringBuilder.append(String.valueOf(ss));
      }

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
              Bytes.toBytes(String.valueOf(key) + ","
                      + String.valueOf(record.getVersion()) + ","
                      + String.valueOf(record.getRecordType()) + ","
                      + String.valueOf(record.getSupersedingSnapshotId()) + ","
                      + String.valueOf(record.getRestoredSnapshot()) + ","
                      + String.valueOf(record.getRestoreStartTime()) + ","
                      + String.valueOf(record.getHiatusTime()) + ","
                      + record.getTableName() + ","
                      + record.getUserTag() + ","
                      + record.getSnapshotName() + ","
                      + record.getSnapshotPath() + ","
                      + String.valueOf(record.getUserGenerated()) + ","
                      + String.valueOf(record.getIncrementalTable()) + ","
                      + String.valueOf(record.getSqlMetaData()) + ","
                      + String.valueOf(record.getInLocalFS()) + ","
                      + String.valueOf(record.getExcluded()) + ","
                      + String.valueOf(record.getMutationStartTime()) + ","
                      + record.getArchivePath() + ","
                      + record.getMetaStatements() + ","
                      + record.getNumLobs() + ","
                      + lobStringBuilder.toString() + ","
                      + snapshotStringBuilder.toString()));
      return p;
   }


   /**
    * batch put records
    *
    * @param puts
    * @return
    * @throws IOException
    */
   private boolean putRecords(final List<Put> puts) throws Exception {
      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         try {
            table.put(puts);
            complete = true;
         } catch (Exception e) {
            LOG.error("putRecords error: ", e);
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount) {
               System.exit(1);
            }
         }
      } while (!complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
      return true;
   }

   /**
    *
    * @param startRowKey regular backup SnapshotMetaStartRecord start key
    * @param endRowKey  next regular backup SnapshotMetaStartRecord start key
    * @return
    * @throws IOException
    */
   public List<SnapshotMetaRecord> getBrcSnapshotMetaRecord(byte[] startRowKey, byte[] endRowKey) throws IOException {
      String snapshotKeyWord = "NON_TX_SNAPSHOT";
      String brcKeyWord = "_BRC_";
      List<SnapshotMetaRecord> resultList = new ArrayList<>(64);
      Scan s = new Scan(startRowKey, endRowKey);
      s.setBatch(1000);
      ResultScanner ss;
      try {
         ss = table.getScanner(s);
      } catch (IOException e) {
         LOG.error("getMutationMetaByTag, scan error \n" + e);
         return resultList;
      }
      for (Result r : ss) {
         String value = Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL));
         if (AbstractSnapshotRecordMeta.isEmpty(value)) { continue; }
         if (value.contains(snapshotKeyWord) && value.contains(brcKeyWord)) {
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            // skip the key string
            String key = st.nextToken();
            String versionString = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               continue;
            }

            String recordTypeString  = st.nextToken();
            if (Integer.parseInt(recordTypeString)  != SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue()){
               continue;
            }

            String supersedingSnapshotString = st.nextToken();
            String restoredSnapshotString = st.nextToken();
            String restoreStartTimeString = st.nextToken();
            String hiatusTimeString = st.nextToken();
            String tableNameString    = st.nextToken();
            String userTagString      = st.nextToken();
            String snapshotNameString = st.nextToken();
            String snapshotPathString = st.nextToken();
            String userGeneratedString = st.nextToken();
            String incrementalTableString = st.nextToken();
            String sqlMetaDataString  = st.nextToken();
            String inLocalFsString    = st.nextToken();
            String excludedString     = st.nextToken();
            String mutationStartTimeString = st.nextToken();
            long mutationStartTime;
            // This is to cover for metadata migration for an unused parameter.
            if (mutationStartTimeString.contains("false")){
               mutationStartTime = 0L;
            }
            else{
               mutationStartTime = Long.parseLong(mutationStartTimeString, 10);
            }
            String archivePathString  = st.nextToken();
            String mutationStatements = st.nextToken();
            String numLobsString      = st.nextToken();
            int numLobs = Integer.parseInt(numLobsString);
            Set<Long> lobSnapshots = Collections.synchronizedSet(new HashSet<Long>());
            String lobSnapshotString = null;
            try {
               for (int i = 0; i < numLobs; i++) {
                  lobSnapshotString = st.nextElement().toString();
                  lobSnapshots.add(Long.parseLong(lobSnapshotString));
               }
            } catch (Exception e) {
               LOG.warn("getBrcSnapshotMetaRecord exception parsing lob snapshots " + lobSnapshotString, e );
            }

            Set<Long> depSnapshots = Collections.synchronizedSet(new HashSet<Long>());
            String dependentSnapshotString = null;
            try {
               while (st.hasMoreElements()) {
                  dependentSnapshotString = st.nextElement().toString();
                  depSnapshots.add(Long.parseLong(dependentSnapshotString));
               }
            } catch (Exception e) {
               LOG.warn("getBrcSnapshotMetaRecord exception parsing dependent snapshots " + dependentSnapshotString, e );
            }

            long supersedingSnapshot = Long.parseLong(supersedingSnapshotString, 10);
            long restoredSnapshot = Long.parseLong(restoredSnapshotString, 10);
            long restoreStartTime = Long.parseLong(restoreStartTimeString, 10);
            long hiatusTime = Long.parseLong(hiatusTimeString, 10);
            SnapshotMetaRecord record = new SnapshotMetaRecord(Long.parseLong(key), Integer.parseInt(versionString),
                    supersedingSnapshot,
                    restoredSnapshot, restoreStartTime, hiatusTime, tableNameString,
                    userTagString, snapshotNameString, snapshotPathString,
                    userGeneratedString.contains("true"),
                    incrementalTableString.contains("true"),
                    sqlMetaDataString.contains("true"),
                    inLocalFsString.contains("true"),
                    excludedString.contains("true"),
                    mutationStartTime,
                    archivePathString, mutationStatements,
                    numLobs, lobSnapshots, depSnapshots);
            resultList.add(record);
         }
      }
      return resultList;
   }

   public void close() throws Exception {
      try {
         if (admin != null) {
            admin.close();
            admin = null;
         }
         if (connection != null) {
            connection.close();
            connection = null;
         }
         this.config = null;
      } catch (Exception e) {
         throw new IOException(e);
      }
   }

   /**
    * get previous tag SnapshotMetaStartRecord
    * @param tagName
    * @return
    */
   public SnapshotMetaStartRecord getPrevTag(String tagName) {
      List<SnapshotTagIndexRecord> indexList = getSnapshotTagIndexList(null, null);
      indexList.sort(new Comparator<SnapshotTagIndexRecord>() {
         @Override
         public int compare(SnapshotTagIndexRecord o1, SnapshotTagIndexRecord o2) {
            return Long.compare(o2.getStartKey(), o1.getStartKey());
         }
      });
      SnapshotTagIndexRecord tagIndex = getSnapshotTagIndex(tagName);
      for (SnapshotTagIndexRecord tagIndexRecord : indexList) {
         if (tagIndexRecord.getStartKey() >= tagIndex.getStartKey()) {
            continue;
         }
         try {
            return getSnapshotStartRecord(tagIndexRecord.getStartKey());
         } catch (Exception e) {
            return null;
         }
      }
      return null;
   }
}
