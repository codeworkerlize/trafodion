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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.pit.meta.AbstractSnapshotRecordMeta;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SubstringComparator;

import java.util.*;

import org.apache.hadoop.hbase.pit.MutationMetaRecord;

/**
 * This class is responsible for maintaining all meta data writes, puts or deletes, to the Trafodion mutation table.
 *
 * SEE ALSO:
 * <ul>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> SnapshotMeta
 * {@link SnapshotMeta}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
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
public class MutationMeta {

   static final Log LOG = LogFactory.getLog(MutationMeta.class);
   private static Admin admin;
   private Configuration config;
   
   //TRAF_RESERVED_NAMESPACE5 namespace name in sql/common/ComSmallDefs.h.
   private static String MUTATION_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.MUTATION";
   private static String MUTATION_SHADOW_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.MUTATION_SHADOW";
   private static String BINLOG_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.TRAFODION_BINLOG";
   private static String BINLOG_READER_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER";
   private static final byte[] BINLOG_META_FAMILY = Bytes.toBytes("mt_");
   private static final byte[] MUTATION_FAMILY = Bytes.toBytes("mf");
   private static final byte[] MUTATION_QUAL = Bytes.toBytes("mq");

   private static Table table;
   private static Table shadowTable;
   private static Connection connection;
   private static Object adminConnLock = new Object();
   private HBasePitZK pitZK;

   private static int versions;
   private boolean disableBlockCache;

   private static int MutationRetryDelay;
   private static int MutationRetryCount;

/**
    * MutationMeta
    * @throws Exception
    */
   public MutationMeta (Configuration config) throws Exception  {

      if (LOG.isTraceEnabled()) LOG.trace("Enter MutationMeta(config) constructor");
      this.config = HBaseConfiguration.create();

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
         LOG.info("MutationMeta HAX: BackupRestore HBASE RPC Timeout, revise hbase.rpc.timeout from "
              + value + " to " + hbaseRpcTimeoutInt);

      connection = ConnectionFactory.createConnection(this.config);

      try {
         admin = connection.getAdmin();
      }
      catch (Exception e) {
         if (LOG.isTraceEnabled()) LOG.trace("  Exception creating Admin " , e);
         throw e;
      }
      disableBlockCache = true;

      MutationRetryDelay = 5000; // 5 seconds
      MutationRetryCount = 2;

      versions = 10;
      try {
         String maxVersions = System.getenv("TM_MAX_SNAPSHOT_VERSIONS");
         if (maxVersions != null){
            versions = (Integer.parseInt(maxVersions) > versions ? Integer.parseInt(maxVersions) : versions);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_MAX_SNAPSHOT_VERSIONS is not in ms.env");
      }

      HColumnDescriptor hcol = new HColumnDescriptor(MUTATION_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      hcol.setMaxVersions(versions);

      boolean mutationTableExists = admin.isTableAvailable(TableName.valueOf(MUTATION_TABLE_NAME));
      if (LOG.isTraceEnabled()) LOG.trace("Mutation Table " + MUTATION_TABLE_NAME + (mutationTableExists? " exists" : " does not exist" ));
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(MUTATION_TABLE_NAME));
      desc.addFamily(hcol);
      table = connection.getTable(desc.getTableName());

      if (mutationTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + MUTATION_TABLE_NAME);
            admin.createTable(desc);
         }
         catch(TableExistsException tee){
            int retryCount = 0;
            boolean mutationTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               mutationTableEnabled = admin.isTableAvailable(TableName.valueOf(MUTATION_TABLE_NAME));
               if (! mutationTableEnabled) {
                  try {
                     Thread.sleep(2000); // sleep two seconds or until interrupted
                  }
                  catch (InterruptedException e) {
                     // Ignore the interruption and keep going
                  }
               }
               else {
                  break;
               }
            }
            if (retryCount == 3){
               LOG.error("MutationMeta Exception while enabling " + MUTATION_TABLE_NAME);
               throw new IOException("MutationMeta Exception while enabling " + MUTATION_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("MutationMeta Exception while creating " + MUTATION_TABLE_NAME + ": " , e);
            throw new IOException("MutationMeta Exception while creating " + MUTATION_TABLE_NAME + ": " + e);
         }
      }

      HColumnDescriptor mhcol = new HColumnDescriptor(BINLOG_META_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      mhcol.setMaxVersions(1000);

      boolean binlogReaderTableExists = admin.isTableAvailable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
      if (LOG.isTraceEnabled()) LOG.trace("binlog reader Table " + BINLOG_READER_TABLE_NAME + (binlogReaderTableExists? " exists" : " does not exist" ));
      HTableDescriptor mbindesc = new HTableDescriptor(TableName.valueOf(BINLOG_READER_TABLE_NAME));
      mbindesc.addFamily(mhcol);
      mbindesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.binlog.BinlogReaderObserver");

      if (binlogReaderTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + BINLOG_READER_TABLE_NAME);
            admin.createTable(mbindesc);
         }
         catch(TableExistsException tee){
            int retryCount = 0;
            boolean binlogReaderTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               binlogReaderTableEnabled = admin.isTableAvailable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
               if (! binlogReaderTableEnabled) {
                  try {
                     Thread.sleep(2000); // sleep two seconds or until interrupted
                  }
                  catch (InterruptedException e) {
                     // Ignore the interruption and keep going
                  }
               }
               else {
                  break;
               }
            }
            if (retryCount == 3){
               LOG.error("MutationMeta Exception while enabling " + BINLOG_READER_TABLE_NAME);
               throw new IOException("MutationMeta Exception while enabling " + BINLOG_READER_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("MutationMeta Exception while creating " + BINLOG_READER_TABLE_NAME + ": " , e);
            throw new IOException("MutationMeta Exception while creating " + BINLOG_TABLE_NAME + ": " + e);
         }
      }

      boolean shadowTableExists = admin.isTableAvailable(TableName.valueOf(MUTATION_SHADOW_TABLE_NAME));
      if (LOG.isTraceEnabled()) LOG.trace("Mutation Shadow Table " + MUTATION_SHADOW_TABLE_NAME + (shadowTableExists? " exists" : " does not exist" ));
      HTableDescriptor shadowDesc = new HTableDescriptor(TableName.valueOf(MUTATION_SHADOW_TABLE_NAME));
      shadowDesc.addFamily(hcol);
      shadowTable = connection.getTable(shadowDesc.getTableName());

      if (shadowTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + MUTATION_SHADOW_TABLE_NAME);
            admin.createTable(shadowDesc);
         }
         catch(TableExistsException tee){
            int retryCount = 0;
            boolean shadowTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               shadowTableEnabled = admin.isTableAvailable(TableName.valueOf(MUTATION_SHADOW_TABLE_NAME));
               if (! shadowTableEnabled) {
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
               LOG.error("MutationMeta Exception while enabling " + MUTATION_SHADOW_TABLE_NAME);
               throw new IOException("MutationMeta Exception while enabling " + MUTATION_SHADOW_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("MutationMeta Exception while creating " + MUTATION_SHADOW_TABLE_NAME + ": " , e);
            throw new IOException("MutationMeta Exception while creating " + MUTATION_SHADOW_TABLE_NAME + ": " + e);
         }
      }
      pitZK = new HBasePitZK(this.config);
      if (LOG.isTraceEnabled()) LOG.trace("Exit MutationMeta constructor()");
      return;
   }

   /**
    * putMutationRecord
    * @param MutationMetaRecord record
    * @param boolean blindWrite
    * @throws Exception
    * @return long key of the row put or 0 if failure
    */
   public String putMutationRecord(final MutationMetaRecord record, final boolean blindWrite) throws Exception {

      if (LOG.isDebugEnabled()) LOG.debug("putMutationRecord start blindWrite "
           + blindWrite + " for record " + record);

      if (! blindWrite){
         String retVal = checkAndPutMutationRecord(record);
         return retVal;
      }
      String key = record.getRowKey();
      String keyString = new String(String.valueOf(key));

      // Create the Put
      Put p = new Put(getMutationRowKey(key));
      p.addColumn(MUTATION_FAMILY, MUTATION_QUAL,
                Bytes.toBytes(String.valueOf(key) + "$"
                        + String.valueOf(record.getVersion()) + "$"
                        + record.getTableName() + "$"
                        + String.valueOf(record.getRootSnapshot()) + "$"
                        + String.valueOf(record.getPriorIncrementalSnapshot()) + "$"
                        + String.valueOf(record.getBackupSnapshot()) + "$"
                        + String.valueOf(record.getSupersedingFullSnapshot()) + "$"
                        + String.valueOf(record.getSmallestCommitId()) + "$"
                        + String.valueOf(record.getLargestCommitId()) + "$"
                        + String.valueOf(record.getFileSize()) + "$"
                        + record.getUserTag() + "$"
                        + record.getRegionName() + "$"
                        + record.getMutationPath() + "$"
                        + String.valueOf(record.getInLocalFS()) + "$"
                        + String.valueOf(record.getArchived()) + "$"
                        + record.getArchivePath()));

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         boolean locked = pitZK.isSnapshotMetaLocked();
         try {
            if (locked){
               if (LOG.isDebugEnabled()) LOG.debug("putMutationRecord try shadowTable.put metaLocked " + locked + ", " + p );
               shadowTable.put(p);
            }
            else {
               if (LOG.isDebugEnabled()) LOG.debug("putMutationRecord try table.put metaLocked "
                    + locked + ", " + p );
               table.put(p);
            }
            complete = true;
            if (retries > 1){
               if (LOG.isDebugEnabled()) LOG.debug("Retry successful in putMutationRecord for key: " + keyString);
            }
         }
         catch (Exception e2){
            if (retries < MutationRetryCount){
               LOG.error("Retry " + retries + " putMutationRecord for key: " + keyString + " due to Exception " , e2);
               connection.getRegionLocator(TableName.valueOf(MUTATION_TABLE_NAME)).getRegionLocation(p.getRow(), true);
               Thread.sleep(MutationRetryDelay); // 5 second default
            }
            else{
               LOG.error("putMutationRecord unsuccessful after retry for key: " + keyString + " due to Exception ", e2);
               throw e2;
            }
         }
      } while (! complete && retries < MutationRetryCount);  // default give up after 5 minutes

      if (LOG.isDebugEnabled()) LOG.debug("putMutationRecord exit returning " + key);
      return key;
   }

   /**
    * putShadowRecord
    * @param record MutationMetaRecord
    * @return rowKey
    * @throws Exception e2
    */
   public String putShadowRecord(final MutationMetaRecord record) throws Exception {
      String key = record.getRowKey();
      String keyString = String.valueOf(key);
      Put p = new Put(getMutationRowKey(key));
      p.addColumn(MUTATION_FAMILY, MUTATION_QUAL,
              Bytes.toBytes(key + "$"
                      + record.getVersion() + "$"
                      + record.getTableName() + "$"
                      + record.getRootSnapshot() + "$"
                      + record.getPriorIncrementalSnapshot() + "$"
                      + record.getBackupSnapshot() + "$"
                      + record.getSupersedingFullSnapshot() + "$"
                      + record.getSmallestCommitId() + "$"
                      + record.getLargestCommitId() + "$"
                      + record.getFileSize() + "$"
                      + record.getUserTag() + "$"
                      + record.getRegionName() + "$"
                      + record.getMutationPath() + "$"
                      + record.getInLocalFS() + "$"
                      + record.getArchived() + "$"
                      + record.getArchivePath()));

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         try {
            shadowTable.put(p);
            complete = true;
            if (retries > 1){
               if (LOG.isDebugEnabled()) { LOG.debug("Retry successful in putShadowRecord for key: " + keyString); }
            }
         } catch (Exception e2) {
            if (retries < MutationRetryCount){
               LOG.error("Retry " + retries + " putShadowRecord for key: " + keyString + " due to Exception " , e2);
               connection.getRegionLocator(TableName.valueOf(MUTATION_TABLE_NAME)).getRegionLocation(p.getRow(), true);
               Thread.sleep(MutationRetryDelay); // 5 second default
            } else{
               LOG.error("putShadowRecord unsuccessful after retry for key: " + keyString + " due to Exception ", e2);
               throw e2;
            }
         }
         // default give up after 5 minutes
      } while (! complete && retries < MutationRetryCount);

      if (LOG.isDebugEnabled()) { LOG.debug("putShadowRecord exit returning " + key); }
      return key;
   }

   public String checkAndPutMutationRecord(final MutationMetaRecord record) throws Exception {

      if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord start for record " + record);
      String key = record.getRowKey();
      String keyString = new String(String.valueOf(key));

      // Create the Put
      Put p = new Put(getMutationRowKey(key));
      p.addColumn(MUTATION_FAMILY, MUTATION_QUAL,
                Bytes.toBytes(String.valueOf(key) + "$"
                        + String.valueOf(record.getVersion()) + "$"
                        + record.getTableName() + "$"
                        + String.valueOf(record.getRootSnapshot()) + "$"
                        + String.valueOf(record.getPriorIncrementalSnapshot()) + "$"
                        + String.valueOf(record.getBackupSnapshot()) + "$"
                        + String.valueOf(record.getSupersedingFullSnapshot()) + "$"
                        + String.valueOf(record.getSmallestCommitId()) + "$"
                        + String.valueOf(record.getLargestCommitId()) + "$"
                        + String.valueOf(record.getFileSize()) + "$"
                        + record.getUserTag() + "$"
                        + record.getRegionName() + "$"
                        + record.getMutationPath() + "$"
                        + String.valueOf(record.getInLocalFS()) + "$"
                        + String.valueOf(record.getArchived()) + "$"
                        + record.getArchivePath()));

      int retries = 0;
      boolean added = false;
      boolean complete = false;
      do {
         retries++;
         boolean locked = pitZK.isSnapshotMetaLocked();
         try {
            if (locked){
               // Here we have detected the SnapshotMetaLock.  We must guard against
               // the possibility that a row is inserted into the Mutation table
               // and we don't detect it.  So we first check the mutation table,
               // then checkAndPut in the shadow
               MutationMetaRecord lvRec = getMutationRecord(key);
               if (lvRec != null){
                  // The record is already in the table, so we can't continue
                  if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord check found record already in mutation file for key " + key);
                  complete = true;
                  break;
               }
               added = shadowTable.checkAndPut(getMutationRowKey(key), MUTATION_FAMILY, MUTATION_QUAL, null, p);
               if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord try shadowTable.checkAndPut, result is "
                        + added + " for key " + key + " metaLocked " + locked );
            }
            else {
               added = table.checkAndPut(getMutationRowKey(key), MUTATION_FAMILY, MUTATION_QUAL, null, p);
               if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord try table.checkAndPut, result is "
                       + added + " for key " + key + " metaLocked " + locked );
            }
            complete = true;
         }
         catch (Exception e2){
            if (retries < MutationRetryCount){
               LOG.error("checkAndPutMutationRecord retrying for key: " + keyString +  " metaLocked " + locked
                           + " due to Exception " , e2);
               connection.getRegionLocator(TableName.valueOf(MUTATION_TABLE_NAME)).getRegionLocation(p.getRow(), true);
               Thread.sleep(MutationRetryDelay); // 5 second default
            }
            else{
               LOG.error("checkAndPutMutationRecord unsuccessful after retry for key: " + keyString
                             + " metaLocked " + locked  + " due to Exception ", e2);
               throw e2;
            }
         }
      } while (! complete && retries < MutationRetryCount);  // default give up after 5 minutes

      if (added) {
         if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord for key: " + keyString + " was successful");
         return key;
      }
      else {
         if (LOG.isDebugEnabled()) LOG.debug("checkAndPutMutationRecord for key: " + keyString + " was unsuccessful");
         return null;
      }
   }

   /**
    * mergeShadowRecords
    * @throws Exception
    * @return void
    *
    * This procedure is called during the SnapshotMeta.unlock() procedure
    * in order to merge the Mutation table and the MutationShadow table.
    * Each row from the shadow table must be inserted into the mutation
    * table prior to deleting the row from the shadow table.  It's
    * important to pass the ignoreLock parameter to writeMutationRecord
    * so that writes go to the mutation table and not the shadow table
    * because the mutation lock is still held at this point
    */
   public static void mergeShadowRecords() throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords start");

      MutationMetaRecord record = null;
      MutationMetaRecord shadowRecord = null;

      try {
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = shadowTable.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords currKey is " + currKey);
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("mergeShadowRecords exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  String largestCommitIdString     = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int mutationVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords MutationKey: " + keyString
                            + " version: " + versionString
                            + " tableName: " + tableNameString
                            + " rootSnapshotString: " + rootSnapshotString
                            + " incrementalSnapshotString: " + incrementalSnapshotString
                            + " backupSnapshotString: " + backupSnapshotString
                            + " supersedingSnapshotString: " + supersedingSnapshotString
                            + " smallestCommitId: " + smallestCommitIdString
                            + " largestCommitId: " + largestCommitIdString
                            + " fileSize: " + fileSizeString
                            + " userTag: " + userTagString
                            + " regionName: " + regionNameString
                            + " mutationPath: " + mutationPathString
                            + " inLocalFS: " + inLocalFsString
                            + " archived: " + archivedString
                            + " archivePath: " + archivePathString);

                  shadowRecord = new MutationMetaRecord(keyString, mutationVersion, tableNameString,
                                  Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                  Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                  Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString),
                                  Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                  inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);

                  record = getMutationRecord(shadowRecord.getRowKey(), false /* readFromShadow */);
                  if (record != null){
                     if (LOG.isDebugEnabled()) LOG.debug("mergeShadowRecords merging shadowRecord: " + shadowRecord + " with record: " + record);
                     if (record.getSmallestCommitId() != shadowRecord.getSmallestCommitId()){
                        shadowRecord.setSmallestCommitId(Math.max(record.getSmallestCommitId(), shadowRecord.getSmallestCommitId()));
                     }
                     if (record.getLargestCommitId() != shadowRecord.getLargestCommitId()){
                        shadowRecord.setLargestCommitId(Math.max(record.getLargestCommitId(), shadowRecord.getLargestCommitId()));
                     }
                  }

                  // Shadow now has most updated information
                  record = shadowRecord;
                  int retries = 0;
                  boolean complete = false;
                  do {
                     retries++;
                     try {
                        // The SnapshotMeta lock is still held, so we need to write while ignoring the lock
                        if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords putting record " + record);
                        Put lvPut = new Put(r.getRow());
                        lvPut.addColumn(MUTATION_FAMILY, MUTATION_QUAL,
                                Bytes.toBytes(String.valueOf(record.getRowKey()) + "$"
                                        + String.valueOf(record.getVersion()) + "$"
                                        + record.getTableName() + "$"
                                        + String.valueOf(record.getRootSnapshot()) + "$"
                                        + String.valueOf(record.getPriorIncrementalSnapshot()) + "$"
                                        + String.valueOf(record.getBackupSnapshot()) + "$"
                                        + String.valueOf(record.getSupersedingFullSnapshot()) + "$"
                                        + String.valueOf(record.getSmallestCommitId()) + "$"
                                        + String.valueOf(record.getLargestCommitId()) + "$"
                                        + String.valueOf(record.getFileSize()) + "$"
                                        + record.getUserTag() + "$"
                                        + record.getRegionName() + "$"
                                        + record.getMutationPath() + "$"
                                        + String.valueOf(record.getInLocalFS()) + "$"
                                        + String.valueOf(record.getArchived()) + "$"
                                        + record.getArchivePath()));

                        table.put(lvPut);

                        // Now we can delete from the shadow table
                        if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords deleting record from the shadow table" +
                                " " + record.getRowKey());
                        Delete lvDel = new Delete(r.getRow());
                        shadowTable.delete(lvDel);
                        complete = true;
                        if (retries > 1){
                           if (LOG.isTraceEnabled()) LOG.trace("Retry successful in mergeShadowRecords for key: " + keyString);
                        }
                     }
                     catch (Exception e2){
                        if (retries < MutationRetryCount){
                           LOG.error("Retry " + retries + " mergeShadowRecords for key: " + keyString + " due to Exception " , e2);
                           connection.getRegionLocator(TableName.valueOf(MUTATION_TABLE_NAME)).getRegionLocation(r.getRow(), true);
                           connection.getRegionLocator(TableName.valueOf(MUTATION_SHADOW_TABLE_NAME)).getRegionLocation(r.getRow(), true);
                           Thread.sleep(MutationRetryDelay); // 5 second default
                        }
                        else{
                           LOG.error("mergeShadowRecords unsuccessful after retry for key: " + keyString + " due to Exception ", e2);
                           throw e2;
                        }
                     }
                  } while (! complete && retries < MutationRetryCount);  // default give up after 5 minutes
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("mergeShadowRecords  Exception getting results " , e);
            throw new IOException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("mergeShadowRecords  Exception setting up scanner " , e);
         throw new IOException(e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords end ");
   }

   /**
    * getMutationRecord
    * @param String key
    * @return MutationMetaRecord
    * @throws Exception
    *
    */
   public MutationMetaRecord getMutationRecord(final String key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord called for key " + key + " without specifying which table");
      return getMutationRecord(key, false /* readShadow */);
   }

   /**
    * getMutationRecord
    * @param long key
    * @return MutationMetaRecord
    * @throws Exception
    *
    */
   public static MutationMetaRecord getMutationRecord(final String key, final boolean readShadow) throws Exception {

	  if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord start for key " + key + " readShadow " + readShadow);
      MutationMetaRecord record = null;
      
      try {
         Get g = new Get(getMutationRowKey(key));
         try {
            Result r = null;
            if (readShadow){
               r = shadowTable.get(g);
            }
            else{
               r = table.get(g);
            }
            if (r == null || r.size() == 0) {
               if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord key " + key + " not found in "
                            + (readShadow ? "meta shadow table" : "meta base table"));
               return record;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(MUTATION_FAMILY, MUTATION_QUAL)), "$");
            String keyString                 = st.nextToken();
            String versionString             = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getMutationRecord (key) exception ", ioe);
               throw ioe;
            }

            String tableNameString           = st.nextToken();
            String rootSnapshotString        = st.nextToken();
            String incrementalSnapshotString = st.nextToken();
            String backupSnapshotString      = st.nextToken();
            String supersedingSnapshotString = st.nextToken();
            String smallestCommitIdString    = st.nextToken();
            String largestCommitIdString     = st.nextToken();
            String fileSizeString            = st.nextToken();
            String userTagString             = st.nextToken();
            String regionNameString          = st.nextToken();
            String mutationPathString        = st.nextToken();
            String inLocalFsString           = st.nextToken();
            String archivedString            = st.nextToken();
            String archivePathString         = st.nextToken();
            int mutationVersion = Integer.parseInt(versionString, 10);

            if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord(key) MutationKey: " + Bytes.toLong(r.getRow())
                    + " key: " + keyString
                    + " version: " + versionString
                    + " tableName: " + tableNameString
                    + " rootSnapshotString: " + rootSnapshotString
                    + " incrementalSnapshotString: " + incrementalSnapshotString
                    + " backupSnapshotString: " + backupSnapshotString
                    + " supersedingSnapshotString " + supersedingSnapshotString
            		+ " smallestCommitId: " + smallestCommitIdString
                    + " largestCommitId: " + largestCommitIdString
            		+ " fileSize: " + fileSizeString
                    + " userTag: " + userTagString
            		+ " regionName: " + regionNameString
            		+ " mutationPath: " + mutationPathString
                    + " inLocalFS: " + inLocalFsString
            		+ " archived: " + archivedString
            		+ " archivePath: " + archivePathString);

            record = new MutationMetaRecord(key, mutationVersion, tableNameString, Long.parseLong(rootSnapshotString),
                                        Long.parseLong(incrementalSnapshotString), Long.parseLong(backupSnapshotString),
                                        Long.parseLong(supersedingSnapshotString),
                                        Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString), Long.parseLong(fileSizeString),
                                        userTagString, regionNameString, mutationPathString,
                                        inLocalFsString.contains("true"), archivedString.contains("true"),
                                        archivePathString);
         }
         catch (Exception e1){
             LOG.error("getMutationRecord Exception " , e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getMutationRecord Exception2 " , e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord end; returning " + record);
      return record;
   }
   
   /**
    * getPriorMutations
    * @param long timeId
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<MutationMetaRecord> getPriorMutations(final long timeId) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations start for timeId " + timeId);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         RowFilter rowFilter = new RowFilter(CompareOp.LESS, new BinaryComparator(longToByteArray(timeId)));
         Scan s = new Scan();
         s.setFilter(rowFilter);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (currKey >= timeId){
                   if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations(timeId) currKey " + currKey
	                 		   + " is not less than timeId " + timeId + ".  Scan complete");
                   break;
               }
               if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations(timeId) currKey is " + currKey);
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getPriorMutations(timeId) exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  String largestCommitIdString     = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int mutationVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations(timeId) MutationKey: " + keyString
                            + " version: " + versionString
                            + " tableName: " + tableNameString
                            + " rootSnapshotString: " + rootSnapshotString
                            + " incrementalSnapshotString: " + incrementalSnapshotString
                            + " backupSnapshotString: " + backupSnapshotString
                            + " supersedingSnapshotString: " + supersedingSnapshotString
                     		+ " smallestCommitId: " + smallestCommitIdString
                            + " largestCommitId: " + largestCommitIdString
                     		+ " fileSize: " + fileSizeString
                            + " userTag: " + userTagString
                     		+ " regionName: " + regionNameString
                     		+ " mutationPath: " + mutationPathString
                            + " inLocalFS: " + inLocalFsString
                     		+ " archived: " + archivedString
                     		+ " archivePath: " + archivePathString);
                     
                  record = new MutationMetaRecord(keyString, mutationVersion, tableNameString,
                                  Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                  Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                  Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString),
                                  Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                  inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);

                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getPriorMutations(timeId)  Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getPriorMutations(timeId)  Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         throw new Exception("getPriorMutations(timeId) record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations(timeId): returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * endRange
    * @param long startRange
    * @param long endRange
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<MutationMetaRecord> getMutationsFromRange(final long limitKey, final long startRange,
		                                                      final long endRange) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange start with limitKey "
		      + limitKey + " for startRange " + startRange + " endRange " + endRange);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getMutationsFromRange exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  if (Long.parseLong(smallestCommitIdString) > endRange){
                     if (LOG.isTraceEnabled()) LOG.trace("smallestCommitId " + Long.parseLong(smallestCommitIdString)
                            + " in Mutation " + keyString + " is greater than endRange " + endRange + "; ignoring ");
                     continue;
                  }
                  String largestCommitIdString     = st.nextToken();

                  // We try to ensure that we have a valid largestCommitId and smallestCommitId, but if for some reason the
                  // smallestCommitId is uninitialized, we will include the mutation file and rely on the ReplayEngine to filter
                  if ((Long.parseLong(largestCommitIdString) < startRange) && (Long.parseLong(largestCommitIdString) != 0L)){
                     if (LOG.isTraceEnabled()) LOG.trace("largestCommitIdString " + Long.parseLong(largestCommitIdString)
                            + " in Mutation " + keyString + " is less than startRange " + startRange + "; ignoring ");
                     continue;
                  }
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int mutationVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange MutationKey: " + Bytes.toLong(r.getRow())
                            + " version: " + versionString
                            + " tableName: " + tableNameString
                            + " rootSnapshotString: " + rootSnapshotString
                            + " incrementalSnapshotString: " + incrementalSnapshotString
                            + " backupSnapshotString: " + backupSnapshotString
                            + " supersedingSnapshotString: " + supersedingSnapshotString
                     		+ " smallestCommitId: " + smallestCommitIdString
                            + " largestCommitId: " + largestCommitIdString
                     		+ " fileSize: " + fileSizeString
                            + " userTag: " + userTagString
                     		+ " regionName: " + regionNameString
                     		+ " mutationPath: " + mutationPathString
                            + " inLocalFS: " + inLocalFsString
                     		+ " archived: " + archivedString
                     		+ " archivePath: " + archivePathString);
                     
                  record = new MutationMetaRecord(keyString, mutationVersion, tableNameString,
                                      Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                      Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                      Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString),
                                      Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                      inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromRange Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }

         // Now we need to scan the shadow table to pick up and additional records.
         s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ss = shadowTable.getScanner(s);

         if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange start on shadowTable with limitKey "
             + limitKey + " for startRange " + startRange + " endRange " + endRange);
         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record in version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getMutationsFromRange shadowTable exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  if (Long.parseLong(smallestCommitIdString) > endRange){
                     if (LOG.isTraceEnabled()) LOG.trace("smallestCommitId in shadowTable " + Long.parseLong(smallestCommitIdString)
                             + " in Mutation " + keyString + " is greater than endRange " + endRange + "; ignoring ");
                     continue;
                  }
                  String largestCommitIdString     = st.nextToken();

                  // We try to ensure that we have a valid largestCommitId and smallestCommitId, but if for some reason the
                  // smallestCommitId is uninitialized, we will include the mutation file and rely on the ReplayEngine to filter
                  if ((Long.parseLong(largestCommitIdString) < startRange) && (Long.parseLong(largestCommitIdString) != 0L)){
                     if (LOG.isTraceEnabled()) LOG.trace("largestCommitIdString in shadowTable " + Long.parseLong(largestCommitIdString)
                             + " in Mutation " + keyString + " is less than startRange " + startRange + "; ignoring ");
                     continue;
                  }
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int mutationVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange shadowTable MutationKey: " + Bytes.toLong(r.getRow())
                             + " version: " + versionString
                             + " tableName: " + tableNameString
                             + " rootSnapshotString: " + rootSnapshotString
                             + " incrementalSnapshotString: " + incrementalSnapshotString
                             + " backupSnapshotString: " + backupSnapshotString
                             + " supersedingSnapshotString: " + supersedingSnapshotString
                             + " smallestCommitId: " + smallestCommitIdString
                             + " largestCommitId: " + largestCommitIdString
                             + " fileSize: " + fileSizeString
                             + " userTag: " + userTagString
                             + " regionName: " + regionNameString
                             + " mutationPath: " + mutationPathString
                             + " inLocalFS: " + inLocalFsString
                             + " archived: " + archivedString
                             + " archivePath: " + archivePathString);

                  record = new MutationMetaRecord(keyString, mutationVersion, tableNameString,
                                       Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                       Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                       Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString),
                                       Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                       inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromRange shadowTable Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getMutationsFromRange Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
    	  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange found no mutation records in range");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<MutationMetaRecord> getUnneededMutations() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getUnneededMutations START ");
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;
      try {
         Scan s = new Scan();
         s.setReversed(true);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getUnneededMutations exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  long rootSnapshot = Long.parseLong(rootSnapshotString);
                  if (rootSnapshot != 999){
                     continue;
                  }
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  String largestCommitIdString     = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int mutationVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getUnneededMutations found MutationKey: " + Bytes.toLong(r.getRow())
                            + " version: " + versionString
                            + " tableName: " + tableNameString
                            + " rootSnapshotString: " + rootSnapshotString
                            + " incrementalSnapshotString: " + incrementalSnapshotString
                            + " backupSnapshotString: " + backupSnapshotString
                            + " supersedingSnapshotString: " + supersedingSnapshotString
                            + " smallestCommitId: " + smallestCommitIdString
                            + " largestCommitId: " + largestCommitIdString
                            + " fileSize: " + fileSizeString
                            + " userTag: " + userTagString
                            + " regionName: " + regionNameString
                            + " mutationPath: " + mutationPathString
                            + " inLocalFS: " + inLocalFsString
                            + " archived: " + archivedString
                            + " archivePath: " + archivePathString);

                  record = new MutationMetaRecord(keyString, mutationVersion, tableNameString,
                                      Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                      Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                      Long.parseLong(smallestCommitIdString), Long.parseLong(largestCommitIdString),
                                      Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                      inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getUnneededMutations Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getUnneededMutations Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         if (LOG.isTraceEnabled()) LOG.trace("getUnneededMutations found no mutation records in range");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getUnneededMutations: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    *
    * compatibility long key
    * fist use RowPrefixFilter query, if empty remove RowPrefixFilter
    *
    * @param snapshotKey
    * @param mutationStartTime
    * @param tableName
    * @param skipPending
    * @param isIncrementalBackup
    * @param endTime
    * @return
    * @throws Exception
    */
   public ArrayList<MutationMetaRecord> getMutationsFromSnapshot(final long snapshotKey, final long mutationStartTime,
                                                            final String tableName, boolean skipPending,
                                                                  boolean isIncrementalBackup, long endTime, boolean regularExport) throws Exception {
      ArrayList<MutationMetaRecord> list = getMutationsFromSnapshot(snapshotKey, mutationStartTime, tableName,
              skipPending, isIncrementalBackup, endTime,true, regularExport);
      return list;
   }

   /**
    * getMutationsFromSnapshot
    * @param long startKey
    * @param boolean skipPending
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<MutationMetaRecord> getMutationsFromSnapshot(final long snapshotKey, final long mutationStartTime, final String tableName,
                                                                 boolean skipPending,
                                                                 boolean isIncrementalBackup,
                                                                 long endTime,
                                                                 boolean prefixQuery, boolean regularExport) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot start for snapshotKey " + snapshotKey
          + " tableName " + tableName + " mutationStartTime " + mutationStartTime + " endTime " + endTime + " skipPending " + skipPending +
              " prefixQuery " + prefixQuery + " regularExport " + regularExport);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      // Even though we get the start key passed in we will start
      // a little behind that for safety.
      Scan s = null;
      try {
         s = new Scan();
         SingleColumnValueFilter filter1 = null;
         SubstringComparator comp1 = null;
         SingleColumnValueFilter filter2 = null;
         SubstringComparator comp2 = null;
         FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
         if (tableName != null && !tableName.isEmpty()) {
            comp1 = new SubstringComparator(tableName);
            filter1 = new SingleColumnValueFilter(MUTATION_FAMILY, MUTATION_QUAL, CompareOp.EQUAL, comp1);
            filterList.addFilter(filter1);
         }

         if (skipPending) {
            comp2 = new SubstringComparator("PENDING");
            filter2 = new SingleColumnValueFilter(MUTATION_FAMILY, MUTATION_QUAL, CompareOp.NOT_EQUAL, comp2);
            filterList.addFilter(filter2);
         }
         if(prefixQuery){
            s.setRowPrefixFilter(Bytes.toBytes(tableName));
         }
         s.setCaching(1000);
         s.setCacheBlocks(false);
         s.setFilter(filterList);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getMutationsFromSnapshot(key) exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String rootSnapshotString        = st.nextToken();
                  //if regular backup and export operation,not compare rootsnapshotId
                  if (!regularExport && Long.parseLong(rootSnapshotString) != snapshotKey){
                     continue;
                  }
                  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot found mutation for snapshot " + rootSnapshotString);
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  long smallestCommitId            = Long.parseLong(st.nextToken());
                  long largestCommitId             = Long.parseLong(st.nextToken());
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  if (skipPending && userTagString.equals("PENDING")){
                     continue;
                  }

                  //if regular export include  previous tag start - current tag end mutations
                  if (regularExport) {
                     if (largestCommitId < mutationStartTime || smallestCommitId > endTime) {
                        continue;
                     }
                     if(!tableNameString.equals(tableName)) {
                        continue;
                     }
                  }

                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot found mutationPath "
                           + mutationPathString + " for snapshot " + rootSnapshotString);
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();

                  record = new MutationMetaRecord(keyString, version, tableNameString,
                                      Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                                      Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                                      smallestCommitId, largestCommitId,
                                      Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                                      inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);
                  if (returnList.contains(record.getRowKey())){
                     IOException ioe = new IOException ("duplicate mutation");
                     LOG.error("Exception in getMutationsFromSnapshot for key " + record.getRowKey() + " ", ioe);
                     throw ioe;
                  }
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromSnapshot Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getMutationsFromSnapshot Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getMutationsFromUserTag
    * @param String userTag
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<MutationMetaRecord> getMutationsFromUserTag(final String userTable, final String userTag) {
        return getMutationsFromUserTag(userTable, userTag, false);
   }

   /**
    * getMutationsFromUserTag
    * @param userTable userTable
    * @param userTag userTag
    * @param table table
    * @return MutationMetaRecordList
    */
   public ArrayList<MutationMetaRecord> getMutationsFromUserTag(final String userTable, final String userTag,
                                                                Table table, boolean prefixQuery) {
      if (LOG.isInfoEnabled())
         LOG.info("getMutationsFromUserTag start for table " + userTable + " userTag " + userTag + " prefixQuery " + prefixQuery);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         SubstringComparator comp1 = new SubstringComparator(userTable);
         SubstringComparator comp2 = new SubstringComparator(userTag);
         SingleColumnValueFilter filter1 = new SingleColumnValueFilter(MUTATION_FAMILY, MUTATION_QUAL, CompareOp.EQUAL, comp1);
         SingleColumnValueFilter filter2 = new SingleColumnValueFilter(MUTATION_FAMILY, MUTATION_QUAL, CompareOp.EQUAL, comp2);
         FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
         filterList.addFilter(filter1);
         filterList.addFilter(filter2);
         Scan s = new Scan();
         if(prefixQuery){
            s.setRowPrefixFilter(Bytes.toBytes(userTable));
         }
         s.setFilter(filterList);
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                             + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getMutationsFromUserTag exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  if (! (tableNameString.equals(userTable))){
                     continue;
                  }
                  String rootSnapshotString        = st.nextToken();
                  String incrementalSnapshotString = st.nextToken();
                  String backupSnapshotString      = st.nextToken();
                  String supersedingSnapshotString = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  String largestCommitIdString     = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  if (! (userTagString.equals(userTag))){
                     continue;
                  }
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();

                  record = new MutationMetaRecord(keyString, version, tableNameString,
                          Long.parseLong(rootSnapshotString), Long.parseLong(incrementalSnapshotString),
                          Long.parseLong(backupSnapshotString), Long.parseLong(supersedingSnapshotString),
                          Long.parseLong(smallestCommitIdString),
                          Long.parseLong(largestCommitIdString),
                          Long.parseLong(fileSizeString), userTagString, regionNameString, mutationPathString,
                          inLocalFsString.contains("true"), archivedString.contains("true"), archivePathString);
                  if (returnList.contains(record.getRowKey())){
                     IOException ioe = new IOException ("duplicate mutation");
                     LOG.error("Exception in getMutationsFromUserTag for userTag " + record.getUserTag() + " ", ioe);
                     throw ioe;
                  }
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromUserTag Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getMutationsFromUserTag Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromUserTag found no mutation records for userTag " + userTag);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromUserTag: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * compatibility long key
    *
    * fist use RowPrefixFilter query, if empty remove RowPrefixFilter
    *
    * getMutationsFromUserTag
    * @param userTable userTable
    * @param userTag userTag
    * @param isReadShadow isReadShadow
    * @return MutationMetaRecordList
    */
   public ArrayList<MutationMetaRecord> getMutationsFromUserTag(final String userTable, final String userTag,
                                                                final boolean isReadShadow) {
      Table queryTable = isReadShadow ? shadowTable : table;
      ArrayList<MutationMetaRecord> list = getMutationsFromUserTag(userTable, userTag, queryTable, true);
      return list;
   }

   /**
    * getAllMutationsFromUserTag
    * @param userTable userTable
    * @param userTag userTag
    * @return all MutationMetaRecord
    */
   public ArrayList<MutationMetaRecord> getAllMutationsFromUserTag(final String userTable, final String userTag) {
      ArrayList<MutationMetaRecord> list = getMutationsFromUserTag(userTable, userTag, true);
      list.addAll(getMutationsFromUserTag(userTable, userTag, false));
      return list;
   }

   public Map<byte[],String> getMutationMetaByTag(List<String> tagNameList) {
      Scan s = new Scan();
      s.setBatch(1000);
      ResultScanner ss;
      try {
         ss = table.getScanner(s);
      } catch (IOException e) {
         LOG.error("getMutationMetaByTag, scan error \n" + e);
         return new HashMap<>(16);
      }

      Map<byte[],String> columnMap = new HashMap<>(64);
      for (Result r : ss) {
         String value = Bytes.toString(r.getValue(MUTATION_FAMILY, MUTATION_QUAL));
         if (AbstractSnapshotRecordMeta.isEmpty(value)) { continue; }
         String[] fieldArray = value.split("\\$");
         if (fieldArray.length < 13) { continue; }
         String tagName = fieldArray[10];
         String filePath = fieldArray[12];
         if (tagNameList.contains(tagName)) {
            columnMap.put(r.getRow(), filePath);
         }
      }
      return columnMap;
   }

   public static boolean deleteRecords(final List<Delete> deletes) {
      if (LOG.isTraceEnabled()) {
         LOG.trace("deleteRecords start for key: " + deletes.size());
      }
      try {
         if (LOG.isTraceEnabled()) {
            LOG.trace("deleteRecords  (" + deletes.size() + ") ");
         }
         table.delete(deletes);
      } catch (Exception e) {
         LOG.error("deleteRecords Exception " , e );
         return false;
      }
      if (LOG.isTraceEnabled()) {
         LOG.trace("deleteRecords - exit");
      }
      return true;
   }
   /**
    * deleteMutationRecord
    * @param long key
    * @return boolean success
    * @throws IOException
    * 
    */
   public static boolean deleteMutationRecord(final String key) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord start for key: " + key);
      try {
         Delete d = new Delete(getMutationRowKey(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord  (" + key + ") ");
         table.delete(d);
         if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord  (" + key + ") from shadow table");
         shadowTable.delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteMutationRecord Exception " , e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord - exit");
      return true;
   }

   private byte[] longToByteArray (final long L) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      dos.writeLong(L);
      dos.flush();
      return baos.toByteArray();
   }

   /**
    * compatibility long rowkKey,if numeric first to long then toBytes
    * @param key
    * @return
    */
   public static byte [] getMutationRowKey(String key){
      byte [] rowKey;
      if(AbstractSnapshotRecordMeta.isNumeric(key)){
         rowKey = Bytes.toBytes(Long.parseLong(key));
      } else {
         rowKey = Bytes.toBytes(key);
      }
      return rowKey;
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
    * batchPut mutation meta data
    * @param record MutationMetaRecord
    * @return rowKey
    * @throws Exception e2
    */
   public void batchPut(List<MutationMetaRecord> records, boolean shadow) throws Exception {
      Table putTable = shadow ? shadowTable : table;
      List<Put> puts = new ArrayList<>(records.size());
      for (MutationMetaRecord record : records) {
         String key = record.getRowKey();
         Put p = new Put(getMutationRowKey(key));
         p.addColumn(MUTATION_FAMILY, MUTATION_QUAL,
                 Bytes.toBytes(key + "$"
                         + record.getVersion() + "$"
                         + record.getTableName() + "$"
                         + record.getRootSnapshot() + "$"
                         + record.getPriorIncrementalSnapshot() + "$"
                         + record.getBackupSnapshot() + "$"
                         + record.getSupersedingFullSnapshot() + "$"
                         + record.getSmallestCommitId() + "$"
                         + record.getLargestCommitId() + "$"
                         + record.getFileSize() + "$"
                         + record.getUserTag() + "$"
                         + record.getRegionName() + "$"
                         + record.getMutationPath() + "$"
                         + record.getInLocalFS() + "$"
                         + record.getArchived() + "$"
                         + record.getArchivePath()));
         puts.add(p);
      }

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         try {
            putTable.put(puts);
            complete = true;
            if (retries > 1){
               if (LOG.isDebugEnabled()) { LOG.debug("Retry successful in batchPut size: " + puts.size()); }
            }
         } catch (Exception e2) {
            if (retries < MutationRetryCount){
               LOG.error("Retry " + retries + " batchPut size: " + puts.size() + " due to Exception " , e2);
               Thread.sleep(MutationRetryDelay); // 5 second default
            } else{
               LOG.error("batchPut unsuccessful after retry for key: " + puts.size() + " due to Exception ", e2);
               throw e2;
            }
         }
         // default give up after 5 minutes
      } while (! complete && retries < MutationRetryCount);

      if (LOG.isDebugEnabled()) { LOG.debug("batchPut exit"); }
   }
}
