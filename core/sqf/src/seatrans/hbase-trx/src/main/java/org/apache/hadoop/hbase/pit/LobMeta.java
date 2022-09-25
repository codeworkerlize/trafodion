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
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.pit.LobMetaRecord;

/**
 * This class is responsible for maintaining all meta data writes, puts or deletes, to the Trafodion Lob Meta table.
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
 * <li> LobMetaRecord
 * {@link LobMetaRecord}
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
public class LobMeta {

   static final Log LOG = LogFactory.getLog(LobMeta.class);
   private static Admin admin;
   private Configuration config;
   
   //TRAF_RESERVED_NAMESPACE5 namespace name in sql/common/ComSmallDefs.h.
   private static String LOB_META_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.LOB_META";
   private static String LOB_META_SHADOW_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.LOB_META_SHADOW";
   private static final byte[] LOB_META_FAMILY = Bytes.toBytes("hf");
   private static final byte[] LOB_META_QUAL = Bytes.toBytes("hq");

   private static Table table;
   private static Table shadowTable;
   private static Connection connection;
   private static Configuration adminConf = null;
   private static Connection adminConnection = null;
   private static Object adminConnLock = new Object();
   private HBasePitZK pitZK;

   private static int versions;
   private boolean disableBlockCache;

   private static int LobRetryDelay;
   private static int LobRetryCount;

/**
    * LobMeta
    * @throws Exception
    */
   public LobMeta (Configuration config) throws Exception  {

      if (LOG.isTraceEnabled()) LOG.trace("Enter LobMeta(config) constructor");
      this.config = HBaseConfiguration.create();
      connection = ConnectionFactory.createConnection(config);

      String rpcTimeout = System.getenv("HAX_ADMIN_RPC_TIMEOUT");
      if (rpcTimeout != null){
           synchronized(adminConnLock) {
              if (adminConnection == null) {
                 adminConf = new Configuration(config);
                 int rpcTimeoutInt = Integer.parseInt(rpcTimeout.trim());
                 String value = adminConf.getTrimmed("hbase.rpc.timeout");
                 adminConf.set("hbase.rpc.timeout", Integer.toString(rpcTimeoutInt));
                 String value2 = adminConf.getTrimmed("hbase.rpc.timeout");
                 LOG.info("HAX: ADMIN RPC Timeout, revise hbase.rpc.timeout from " + value + " to " + value2);
                 adminConnection = ConnectionFactory.createConnection(adminConf);
              }
           } // sync
      }
      else {
           adminConnection = connection;
      }

      try {
         admin = connection.getAdmin();
      }
      catch (Exception e) {
         if (LOG.isTraceEnabled()) LOG.trace("  Exception creating Admin " , e);
         throw e;
      }
      disableBlockCache = true;

      LobRetryDelay = 5000; // 5 seconds
      LobRetryCount = 2;

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

      HColumnDescriptor hcol = new HColumnDescriptor(LOB_META_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      hcol.setMaxVersions(versions);

      boolean lobTableExists = admin.isTableAvailable(TableName.valueOf(LOB_META_TABLE_NAME));
      if (LOG.isTraceEnabled()) LOG.trace("Lob Meta Table " + LOB_META_TABLE_NAME + (lobTableExists? " exists" : " does not exist" ));
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(LOB_META_TABLE_NAME));
      desc.addFamily(hcol);
      table = connection.getTable(desc.getTableName());

      if (lobTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + LOB_META_TABLE_NAME);
            admin.createTable(desc);
         }
         catch(TableExistsException tee){
            int retryCount = 0;
            boolean lobTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               lobTableEnabled = admin.isTableAvailable(TableName.valueOf(LOB_META_TABLE_NAME));
               if (! lobTableEnabled) {
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
               LOG.error("LobMeta Exception while enabling " + LOB_META_TABLE_NAME);
               throw new IOException("LobMeta Exception while enabling " + LOB_META_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("LobMeta Exception while creating " + LOB_META_TABLE_NAME + ": " , e);
            throw new IOException("LobMeta Exception while creating " + LOB_META_TABLE_NAME + ": " + e);
         }
      }

      boolean shadowTableExists = admin.isTableAvailable(TableName.valueOf(LOB_META_SHADOW_TABLE_NAME));
      if (LOG.isTraceEnabled()) LOG.trace("Lob Meta Shadow Table " + LOB_META_SHADOW_TABLE_NAME + (shadowTableExists? " exists" : " does not exist" ));
      HTableDescriptor shadowDesc = new HTableDescriptor(TableName.valueOf(LOB_META_SHADOW_TABLE_NAME));
      shadowDesc.addFamily(hcol);
      shadowTable = connection.getTable(shadowDesc.getTableName());

      if (shadowTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + LOB_META_SHADOW_TABLE_NAME);
            admin.createTable(shadowDesc);
         }
         catch(TableExistsException tee){
            int retryCount = 0;
            boolean shadowTableEnabled = false;
            while (retryCount < 3) {
               retryCount++;
               shadowTableEnabled = admin.isTableAvailable(TableName.valueOf(LOB_META_SHADOW_TABLE_NAME));
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
               LOG.error("LobMeta Exception while enabling " + LOB_META_SHADOW_TABLE_NAME);
               throw new IOException("LobMeta Exception while enabling " + LOB_META_SHADOW_TABLE_NAME);
            }
         }
         catch(Exception e){
            LOG.error("LobMeta Exception while creating " + LOB_META_SHADOW_TABLE_NAME + ": " , e);
            throw new IOException("LobMeta Exception while creating " + LOB_META_SHADOW_TABLE_NAME + ": " + e);
         }
      }
      pitZK = new HBasePitZK(this.config);
      if (LOG.isTraceEnabled()) LOG.trace("Exit LobMeta constructor()");
      return;
   }

   /**
    * putLobMetaRecord
    * @param LobMetaRecord record
    * @param boolean blindWrite
    * @throws Exception
    * @return long key of the row put or 0 if failure
    */
   public long putLobMetaRecord(final LobMetaRecord record, final boolean blindWrite) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("putLobMetaRecord start blindWrite "
           + blindWrite + " for record " + record);

      if (! blindWrite){
         long retVal = checkAndPutLobMetaRecord(record);
         return retVal;
      }
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(LOB_META_FAMILY, LOB_META_QUAL,
                Bytes.toBytes(String.valueOf(key) + "$"
                        + String.valueOf(record.getVersion()) + "$"
                        + record.getTableName() + "$"
                        + String.valueOf(record.getSnapshotName()) + "$"
                        + String.valueOf(record.getFileSize()) + "$"
                        + record.getUserTag() + "$"
                        + record.getHdfsPath() + "$"
                        + String.valueOf(record.getSourceDirectory()) + "$"
                        + String.valueOf(record.getInLocalFS()) + "$"
                        + String.valueOf(record.getArchived()) + "$"
                        + record.getArchivePath()));

      int retries = 0;
      boolean complete = false;
      do {
         retries++;
         boolean locked = pitZK.isSnapshotMetaLocked();
         try {
//            if (locked){
//               if (LOG.isTraceEnabled()) LOG.trace("putLobMetaRecord try table.put metaLocked " + locked + ", " + p );
//               shadowTable.put(p);
//            }
//            else {
               if (LOG.isTraceEnabled()) LOG.trace("putLobMetaRecord try table.put metaLocked "
                    + locked + ", " + p );
               table.put(p);
//            }
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putLobMetaRecord for key: " + keyString);
            }
         }
         catch (Exception e2){
            if (retries < LobRetryCount){
               LOG.error("Retry " + retries + " putLobMetaRecord for key: " + keyString + " due to Exception " , e2);
               connection.getRegionLocator(TableName.valueOf(LOB_META_TABLE_NAME)).getRegionLocation(p.getRow(), true);
               Thread.sleep(LobRetryDelay); // 5 second default
            }
            else{
               LOG.error("putLobMetaRecord unsuccessful after retry for key: " + keyString + " due to Exception ", e2);
               throw e2;
            }
         }
      } while (! complete && retries < LobRetryCount);  // default give up after 5 minutes

      if (LOG.isTraceEnabled()) LOG.trace("putLobMetaRecord exit returning " + key);
      return key;
   }

   public long checkAndPutLobMetaRecord(final LobMetaRecord record) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("checkAndPutLobMetaRecord start for record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.addColumn(LOB_META_FAMILY, LOB_META_QUAL,
            Bytes.toBytes(String.valueOf(key) + "$"
                      + String.valueOf(record.getVersion()) + "$"
                      + record.getTableName() + "$"
                      + String.valueOf(record.getSnapshotName()) + "$"
                      + String.valueOf(record.getFileSize()) + "$"
                      + record.getUserTag() + "$"
                      + record.getHdfsPath() + "$"
                      + String.valueOf(record.getSourceDirectory()) + "$"
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
            added = table.checkAndPut(Bytes.toBytes(key), LOB_META_FAMILY, LOB_META_QUAL, null, p);
            if (LOG.isTraceEnabled()) LOG.trace("checkAndPutLobMetaRecord try table.checkAndPut, result is "
                    + added + " for key " + key + " metaLocked " + locked );
            complete = true;
         }
         catch (Exception e2){
            if (retries < LobRetryCount){
               LOG.error("checkAndPutLobMetaRecord retrying for key: " + keyString +  " metaLocked " + locked
                           + " due to Exception " , e2);
               connection.getRegionLocator(TableName.valueOf(LOB_META_TABLE_NAME)).getRegionLocation(p.getRow(), true);
               Thread.sleep(LobRetryDelay); // 5 second default
            }
            else{
               LOG.error("checkAndPutLobMetaRecord unsuccessful after retry for key: " + keyString
                             + " metaLocked " + locked  + " due to Exception ", e2);
               throw e2;
            }
         }
      } while (! complete && retries < LobRetryCount);  // default give up after 5 minutes

      if (added) {
         if (LOG.isTraceEnabled()) LOG.trace("checkAndPutLobMetaRecord for key: " + keyString + " was successful");
         return key;
      }
      else {
         if (LOG.isTraceEnabled()) LOG.trace("checkAndPutLobMetaRecord for key: " + keyString + " was unsuccessful");
         return 0;
      }
   }

   /**
    * mergeShadowRecords
    * @throws Exception
    * @return void
    *
    * This procedure is called during the SnapshotMeta.unlock() procedure
    * in order to merge the Lob Meta table and the Lob Meta Shadow table.
    * Each row from the shadow table must be inserted into the Lob Meta
    * table prior to deleting the row from the shadow table.  It's
    * important to pass the ignoreLock parameter to writeLobMetaRecord
    * so that writes go to the Lob Meta table and not the shadow table
    * because the hdfs lock is still held at this point
    */
   public static void mergeShadowRecords() throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords start");

      LobMetaRecord record = null;

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
                  String snapshotNameString        = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String hdfsPathString            = st.nextToken();
                  String sourceDirectoryString     = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int lobVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords LobMetaKey: " + keyString
                            + " version: " + versionString
                            + " tableName: " + tableNameString
                            + " snapshotNameString: " + snapshotNameString
                            + " fileSize: " + fileSizeString
                            + " userTag: " + userTagString
                            + " hdfsPath: " + hdfsPathString
                            + " sourceDirectoryString: " + sourceDirectoryString
                            + " inLocalFS: " + inLocalFsString
                            + " archived: " + archivedString
                            + " archivePath: " + archivePathString);

                  record = new LobMetaRecord(Bytes.toLong(r.getRow()), lobVersion, tableNameString,
                                  snapshotNameString, Long.parseLong(fileSizeString), userTagString,
                                  hdfsPathString, sourceDirectoryString, inLocalFsString.contains("true"),
                                  archivedString.contains("true"), archivePathString);

                  int retries = 0;
                  boolean complete = false;
                  do {
                     retries++;
                     try {
                        // The SnapshotMeta lock is still held, so we need to write while ignoring the lock
                        if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords merging record " + record);
                        Put lvPut = new Put(r.getRow());
                        long key = Bytes.toLong(r.getRow());
                        lvPut.addColumn(LOB_META_FAMILY, LOB_META_QUAL,
                                  Bytes.toBytes(String.valueOf(key) + "$"
                                        + String.valueOf(record.getVersion()) + "$"
                                        + record.getTableName() + "$"
                                        + String.valueOf(record.getSnapshotName()) + "$"
                                        + String.valueOf(record.getFileSize()) + "$"
                                        + record.getUserTag() + "$"
                                        + record.getHdfsPath() + "$"
                                        + String.valueOf(record.getSourceDirectory()) + "$"
                                        + String.valueOf(record.getInLocalFS()) + "$"
                                        + String.valueOf(record.getArchived()) + "$"
                                        + record.getArchivePath()));

                        table.put(lvPut);

                        // Now we can delete from the shadow table
                        if (LOG.isTraceEnabled()) LOG.trace("mergeShadowRecords deleting record from the shadow table " + record.getKey());
                        Delete lvDel = new Delete(r.getRow());
                        shadowTable.delete(lvDel);
                        complete = true;
                        if (retries > 1){
                           if (LOG.isTraceEnabled()) LOG.trace("Retry successful in mergeShadowRecords for key: " + keyString);
                        }
                     }
                     catch (Exception e2){
                        if (retries < LobRetryCount){
                           LOG.error("Retry " + retries + " mergeShadowRecords for key: " + keyString + " due to Exception " , e2);
                           connection.getRegionLocator(TableName.valueOf(LOB_META_TABLE_NAME)).getRegionLocation(r.getRow(), true);
                           connection.getRegionLocator(TableName.valueOf(LOB_META_SHADOW_TABLE_NAME)).getRegionLocation(r.getRow(), true);
                           Thread.sleep(LobRetryDelay); // 5 second default
                        }
                        else{
                           LOG.error("mergeShadowRecords unsuccessful after retry for key: " + keyString + " due to Exception ", e2);
                           throw e2;
                        }
                     }
                  } while (! complete && retries < LobRetryCount);  // default give up after 5 minutes
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
    * getLobMetaRecord
    * @param long key
    * @return LobMetaRecord
    * @throws Exception
    * 
    */
   public LobMetaRecord getLobMetaRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getLobMetaRecord start for key " + key);
      LobMetaRecord record = null;
      
      try {
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            if (r == null || r.size() == 0) {
               IOException ioe = new IOException("Place Holder ");
               if (LOG.isTraceEnabled()) LOG.trace("getLobMetaRecord key " + key + " not found ", ioe);
               return record;
            }
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(LOB_META_FAMILY, LOB_META_QUAL)), "$");
            String keyString                 = st.nextToken();
            String versionString             = st.nextToken();
            int version = Integer.parseInt(versionString);
            if (version != SnapshotMetaRecordType.getVersion()){
               IOException ioe = new IOException("Unexpected record version found: "
                   + version + " expected: " + SnapshotMetaRecordType.getVersion());
               LOG.error("getLobMetaRecord (key) exception ", ioe);
               throw ioe;
            }

            String tableNameString           = st.nextToken();
            String snapshotNameString        = st.nextToken();
            String fileSizeString            = st.nextToken();
            String userTagString             = st.nextToken();
            String hdfsPathString            = st.nextToken();
            String sourceDirectoryString     = st.nextToken();
            String inLocalFsString           = st.nextToken();
            String archivedString            = st.nextToken();
            String archivePathString         = st.nextToken();
            int lobVersion = Integer.parseInt(versionString, 10);

            if (LOG.isTraceEnabled()) LOG.trace("getLobMetaRecord(key) LobRecordKey: " + Bytes.toLong(r.getRow())
                    + " key: " + keyString
                    + " version: " + versionString
                    + " tableName: " + tableNameString
                    + " snapshotNameString: " + snapshotNameString
                    + " fileSize: " + fileSizeString
                    + " userTag: " + userTagString
                    + " hdfsPath: " + hdfsPathString
                    + " sourceDirectoryString: " + sourceDirectoryString
                    + " inLocalFS: " + inLocalFsString
                    + " archived: " + archivedString
                    + " archivePath: " + archivePathString);

            record = new LobMetaRecord(key, lobVersion, tableNameString,
                    snapshotNameString, Long.parseLong(fileSizeString), userTagString,
                    hdfsPathString, sourceDirectoryString, inLocalFsString.contains("true"),
                    archivedString.contains("true"), archivePathString);
         }
         catch (Exception e1){
             LOG.error("getLobMetaRecord Exception " , e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getLobMetaRecord Exception2 " , e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getLobMetaRecord end; returning " + record);
      return record;
   }

   /**
    * getPriorLobRecords
    * @param Set<Long> ids
    * @return ArrayList<LobMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<LobMetaRecord> getLobRecordsFromList(final Set<Long> ids) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromList start for ids " + ids);

      ArrayList<LobMetaRecord> returnList = new ArrayList<LobMetaRecord>();
      for (Long id : ids){
         try{
            LobMetaRecord tmpRec = getLobMetaRecord(id.longValue());
            if (tmpRec == null){
               if (LOG.isErrorEnabled()) LOG.error("getLobRecordsFromList unable to get record for id " + id);
            }
            returnList.add(tmpRec);
         }
         catch( Exception e){
            if (LOG.isErrorEnabled()) LOG.error("getLobRecordsFromList exception getting record for ds " + id + " ", e);
            throw new IOException(e);
         }
      }
      return returnList;
   }

   /**
    * getPriorLobRecords
    * @param long timeId
    * @return ArrayList<LobMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<LobMetaRecord> getPriorLobRecords(final long timeId) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorLobRecords start for timeId " + timeId);
      ArrayList<LobMetaRecord> returnList = new ArrayList<LobMetaRecord>();
      LobMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (currKey >= timeId){
                   if (LOG.isTraceEnabled()) LOG.trace("getPriorLobRecords(timeId) currKey " + currKey
	                 		   + " is not less than timeId " + timeId + ".  Scan complete");
                   break;
               }
               if (LOG.isTraceEnabled()) LOG.trace("getPriorLobRecords(timeId) currKey is " + currKey);
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getPriorLobRecords(timeId) exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String snapshotNameString        = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String hdfsPathString            = st.nextToken();
                  String sourceDirectoryString     = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int lobVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getPriorLobRecords(timeId) LobMetaKey: " + keyString
                          + " version: " + versionString
                          + " tableName: " + tableNameString
                          + " snapshotNameString: " + snapshotNameString
                          + " fileSize: " + fileSizeString
                          + " userTag: " + userTagString
                          + " hdfsPath: " + hdfsPathString
                          + " sourceDirectoryString: " + sourceDirectoryString
                          + " inLocalFS: " + inLocalFsString
                          + " archived: " + archivedString
                          + " archivePath: " + archivePathString);
                     
                  record = new LobMetaRecord(Bytes.toLong(r.getRow()), lobVersion, tableNameString,
                          snapshotNameString, Long.parseLong(fileSizeString), userTagString,
                          hdfsPathString, sourceDirectoryString, inLocalFsString.contains("true"),
                          archivedString.contains("true"), archivePathString);

                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getPriorLobRecords(timeId)  Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getPriorLobRecords(timeId)  Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         throw new Exception("getPriorLobRecords(timeId) record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getPriorLobRecords(timeId): returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getMetaVersion
    * @return long
    *
    */
   public int getMetaVersion()  {
      int version = SnapshotMetaRecordType.getVersion();
      if (LOG.isTraceEnabled()) LOG.trace("getMetaVersion returning " + version);

      return version;
   }

   /**
    * getLobRecordsFromRange
    * @param long startKey
    * @param long endKey
    * @return ArrayList<LobMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<LobMetaRecord> getLobRecordsFromRange(final long startKey,
		                                                      final long endKey) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromRange start for startKey "
                                  + startKey + " endkey " + endKey);
      ArrayList<LobMetaRecord> returnList = new ArrayList<LobMetaRecord>();
      LobMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(1000);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (currKey < startKey){
                   if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromRange currKey (" + currKey + ") is less than startKey(" + startKey + ") ignoring record");
                   continue;
               }
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String versionString             = st.nextToken();
                  int version = Integer.parseInt(versionString);
                  if (version != SnapshotMetaRecordType.getVersion()){
                     IOException ioe = new IOException("Unexpected record version found: "
                         + version + " expected: " + SnapshotMetaRecordType.getVersion());
                     LOG.error("getLobRecordsFromRange exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  String snapshotNameString        = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  String hdfsPathString            = st.nextToken();
                  String sourceDirectoryString     = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  int lobVersion = Integer.parseInt(versionString, 10);

                  if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromRange LobMetaKey: " + Bytes.toLong(r.getRow())
                              + " version: " + versionString
                              + " tableName: " + tableNameString
                              + " snapshotNameString: " + snapshotNameString
                              + " fileSize: " + fileSizeString
                              + " userTag: " + userTagString
                              + " hdfsPath: " + hdfsPathString
                              + " sourceDirectoryString: " + sourceDirectoryString
                              + " inLocalFS: " + inLocalFsString
                              + " archived: " + archivedString
                              + " archivePath: " + archivePathString);
                     
                  record = new LobMetaRecord(Bytes.toLong(r.getRow()), lobVersion, tableNameString,
                          snapshotNameString, Long.parseLong(fileSizeString), userTagString,
                          hdfsPathString, sourceDirectoryString, inLocalFsString.contains("true"),
                          archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getLobRecordsFromRange Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getLobRecordsFromRange Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
    	  if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromRange found no Lob records in range");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromRange: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getLobRecordsFromUserTag
    * @param String userTag
    * @return ArrayList<LobMetaRecord>
    * @throws Exception
    *
    */
   public ArrayList<LobMetaRecord> getLobRecordsFromUserTag(final String userTable, final String userTag) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromUserTag start for table " + userTable + " userTag " + userTag);
      ArrayList<LobMetaRecord> returnList = new ArrayList<LobMetaRecord>();
      LobMetaRecord record = null;

      try {
         Scan s = new Scan();
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
                     LOG.error("getLobRecordsFromUserTag exception ", ioe);
                     throw ioe;
                  }
                  String tableNameString           = st.nextToken();
                  if (! (tableNameString.equals(userTable))){
                     continue;
                  }
                  String snapshotNameString        = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String userTagString             = st.nextToken();
                  if (! (userTagString.equals(userTag))){
                     continue;
                  }
                  String hdfsPathString            = st.nextToken();
                  String sourceDirectoryString     = st.nextToken();
                  String inLocalFsString           = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();

                  record = new LobMetaRecord(Bytes.toLong(r.getRow()), version, tableNameString,
                          snapshotNameString, Long.parseLong(fileSizeString), userTagString,
                          hdfsPathString, sourceDirectoryString, inLocalFsString.contains("true"),
                          archivedString.contains("true"), archivePathString);
                  if (returnList.contains(record.getKey())){
                     IOException ioe = new IOException ("duplicate record");
                     LOG.error("Exception in getLobRecordsFromUserTag for userTag " + record.getUserTag() + " ", ioe);
                     throw ioe;
                  }
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getLobRecordsFromUserTag Exception getting results " , e);
            throw new RuntimeException(e);
         }
         finally {
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getLobRecordsFromUserTag Exception setting up scanner " , e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromUserTag found no Lob records for userTag " + userTag);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getLobRecordsFromUserTag: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * deleteLobMetaRecord
    * @param long key
    * @return boolean success
    * @throws IOException
    * 
    */
   public static boolean deleteLobMetaRecord(final long key) throws IOException {
      IOException ioe = new IOException("place holder");
      if (LOG.isTraceEnabled()) LOG.trace("deleteLobMetaRecord start for key: " + key + " ", ioe);
      try {
         Delete d;
         //create our own hashed key
         d = new Delete(Bytes.toBytes(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteLobMetaRecord  (" + key + ") ");
         table.delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteLobMetaRecord Exception " , e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteLobMetaRecord - exit");
      return true;
   }

   public void close() throws Exception {
      try {
         if (admin != null) {
            admin.close();
            admin = null;
         }

         if (adminConnection != null) {
            adminConnection.close();
            adminConnection = null;
         }

         if (connection != null) {
            connection.close();
            connection = null;
         }
         this.config = null;
         this.adminConf = null;
      } catch (Exception e) {
         throw new IOException(e);
      }
   }
}
