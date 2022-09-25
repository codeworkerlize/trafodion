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
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.Serializable;
/**
 * This class is one of three used for writing rows into the SnapshotMeta class
 * and Table.  This class is specific to writing a record indicating the start
 * of a partial snapshot operation.
 * 
 * SEE ALSO:
 * <ul>
 * <li> SnapshotMetaStartRecord
 * {@link SnapshotMetaStartRecord}
 * </li>
 * <li> SnapshotMetaIncrementalRecord
 * {@link SnapshotMetaIncrementalRecord}
 * </li>
 * </ul>
 * 
 */
public class SnapshotMetaRecord implements Serializable{


static final Log LOG = LogFactory.getLog(SnapshotMetaRecord.class);

   /**
	 *
	 */
   private static final long serialVersionUID = 6658455501457753411L;

	// These are the components of a record entry into the SnapshotMeta table
   private long key;
   private int version;
   private int recordType;
   private long supersedingSnapshot;
   private long restoredSnapshot;
   private long restoreStartTime;
   private long hiatusTime;
   private String tableName;
   private String userTag;
   private String snapshotName;
   private String snapshotPath;
   private boolean userGenerated;
   private boolean incrementalTable;
   private boolean sqlMetaData;
   private boolean inLocalFS;
   private boolean excluded;
   private long mutationStartTime;
   private String archivePath;
   private String metaStatements;
   private int numLobs;
   private Set<Long> lobSnapshots;
   private Set<Long> dependentSnapshots;

   private List<SnapshotMetaRecord> brcSnapshotList;
   
   /**
    * SnapshotMetaRecord
    * @param long key
    * @param int version
    * @param String tableName
    * @param String userTag
    * @param String snapshotName
    * @param String snapshotPath
    * @param boolean inLocalFS
    * @param boolean excluded
    * @param long mutationStartTime
    * @param String archivePath
    * @param Set<Long> dependentSnapshots
    * @throws IOException
    */
   public SnapshotMetaRecord (final long key,
        final int vers, final long supersedingSnapshot,
        final long restoredSnapshot, final long restoreStartTime,
        final long hiatusTime,
		final String tableName, final String userTag, final String snapshotName,
		final String snapshotPath, final boolean userGenerated,
        final boolean incrementalTable,
        final boolean sqlMetaData, final boolean inLocalFS, final boolean excluded,
        final long mutationStartTime, final String archivePath, final String metaStatements,
        final int numLobs, final Set<Long> lobSnapshots, final Set<Long> dependentSnapshots) throws IOException{

	  if (vers != SnapshotMetaRecordType.getVersion()){
	     IllegalArgumentException iae = new IllegalArgumentException("Error SnapshotMetaRecord constructor() incompatible version "
	                   + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
	     LOG.error("Error SnapshotMetaRecord ", iae);
	     throw iae;
	  }

      this.key = key;
      this.version = vers;
      this.recordType = SnapshotMetaRecordType.SNAPSHOT_RECORD.getValue();
      this.supersedingSnapshot = supersedingSnapshot;
      this.restoredSnapshot = restoredSnapshot;
      this.restoreStartTime = restoreStartTime;
      this.hiatusTime = hiatusTime;
      this.tableName = new String(tableName);
      this.userTag = new String(userTag);
      this.snapshotName = snapshotName;
      this.snapshotPath = new String(snapshotPath);
      this.userGenerated = userGenerated;
      this.incrementalTable = incrementalTable;
      this.sqlMetaData = sqlMetaData;
      this.inLocalFS = inLocalFS;
      this.excluded = excluded;
      this.mutationStartTime = mutationStartTime;
      this.archivePath = new String(archivePath);
      this.metaStatements = new String(metaStatements);
      for (Long ss : dependentSnapshots){
        if (ss.longValue() == key) {
           // Can't have a dependency on ourselves.
           IOException ioe = new IOException (" where ");
           LOG.error(" Error setting dependentSnapshots.  Cannot have a self dependency ", ioe);
           throw ioe;
        }
      }
      this.numLobs = numLobs;
      this.lobSnapshots = Collections.synchronizedSet(new TreeSet<Long>(lobSnapshots));
      this.dependentSnapshots = Collections.synchronizedSet(new TreeSet<Long>(dependentSnapshots));

//      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaRecord constructor() " + this.toString());
      return;
   }

   /**
    * SnapshotMetaRecord
    * @param long key
    * @param int version
    * @param String tableName
    * @param String userTag
    * @param String snapshotName
    * @param String snapshotPath
    * @param boolean inLocalFS
    * @param boolean excluded
    * @param long mutationStartTime
    * @param String archivePath
    * @param int numLobs
    * @param Set<Long> lobSnapshots
    * @param Set<Long> dependentSnapshots
    * @throws IOException
    */
   public SnapshotMetaRecord (final long key,
        final long supersedingSnapshot,
        final long restoredSnapshot, final long restoreStartTime,
        final long hiatusTime,
		final String tableName, final String userTag, final String snapshotName,
		final String snapshotPath, final boolean userGenerated,
        final boolean incrementalTable,
        final boolean sqlMetaData, final boolean inLocalFS, final boolean excluded,
        final long mutationStartTime, final String archivePath, final String metaStatements,
        final int numLobs, final Set<Long> lobSnapshots,
        final Set<Long> dependentSnapshots) throws IOException{

	   this(key, SnapshotMetaRecordType.getVersion(), supersedingSnapshot, restoredSnapshot, restoreStartTime,
		hiatusTime, tableName, userTag, snapshotName, snapshotPath, userGenerated, incrementalTable, sqlMetaData,
        inLocalFS, excluded, mutationStartTime, archivePath, metaStatements, numLobs, lobSnapshots, dependentSnapshots);
   }

   /**
    * SnapshotMetaRecord
    * @param long key
    * @param String tableName
    * @param String userTag
    * @param String snapshotName
    * @param String snapshotPath
    */
   public SnapshotMetaRecord (final long key, final long supersedingSnapshot,
       final long restoredSnapshot, final long restoreStartTime,
       final long hiatusTime, final String tableName,
	   final String userTag, final String snapshotName, final String snapshotPath,
       final long mutationStartTime, final int numLobs, final Set<Long> lobSnapshots,
       final Set<Long>dependentSnapshots) throws Exception  {
      this(key, supersedingSnapshot,
              restoredSnapshot,
              restoreStartTime,
              hiatusTime,
              tableName, userTag,
              snapshotName,
              snapshotPath,
              true /* userGenerated */,
              false /* incrementalTable */,
              true /* sqlMetaData */,
              true /* inLocalFS */,
              false /* excluded */,
              mutationStartTime,
              "DefaultPath" /* archivePath */,
              "EmptyStatements" /* metaStatements */,
              numLobs,
              lobSnapshots,
              dependentSnapshots);
   }

   /**
    * getKey
    * @return long
    *
    * This is the key retried from the IdTm server
    */
   public long getKey() {
       return this.key;
   }

   /**
    * getVersion
    * @return int
    *
    * This is the version of this record for migration
    * and compatibility
    */
   public int getVersion() {
       return this.version;
   }

   public void setVersion(int v) {
       this.version = v;
       return;
   }

   /**
    * getRecordType
    * @return int
    *
    * This method is called to determine the type of record the row retrieved
    * from the SnapshotMeta is.
    */
   public int getRecordType() {
      return this.recordType;
   }

   /**
    * setRecordType
    * @param int
    * 
    * This method is called to set the type of record the row retrieved
    * from the SnapshotMeta is.
    */
   public void setRecordType(int type) {
      this.recordType = type;
   }

   /**
    * getSupersedingSnapshotId
    * @return long
    *
    * This method is called to return the supersedingSnapshot from the SnapshotMetaRecord
    */
   public long getSupersedingSnapshotId() {
      return this.supersedingSnapshot;
   }

   /**
    * setSupersedingSnapshotId
    * @return void
    *
    * This method is called to return the supersedingSnapshot from the SnapshotMetaRecord
    */
   public void setSupersedingSnapshotId(long snapshotId) {
      this.supersedingSnapshot = snapshotId;
   }

   /**
    * getRestoreStartTime
    * @return long
    *
    * This method is called to return the restoreStartTime from the SnapshotMetaRecord
    */
   public long getRestoreStartTime() {
      return this.restoreStartTime;
   }

   /**
    * setRestoreStartTime
    * @return void
    *
    * This method is called to return the restoreStartTime from the SnapshotMetaRecord
    */
   public void setRestoreStartTime(long startTime) {
      this.restoreStartTime = startTime;
   }

   /**
    * getHiatusTime
    * @return long
    *
    * This method is called to return the hiatusTime from the SnapshotMetaRecord
    * indicating this Snapshot should no longer allow incremental updates
    */
   public long getHiatusTime() {
      return this.hiatusTime;
   }

   /**
    * setHiatusTime
    * @return void
    *
    * This method is called to set the hiatusTime from the SnapshotMetaRecord
    * indicating this Snapshot should no longer allow incremental updates
    */
   public void setHiatusTime(long hiatusTime) {
      this.hiatusTime = hiatusTime;
   }

   /**
    * getRestoredSnapshot
    * @return long
    *
    * This method is called to return the restoredSnapshot from the SnapshotMetaRecord
    */
   public long getRestoredSnapshot() {
      return this.restoredSnapshot;
   }

   /**
    * setRestoredSnapshot
    * @return void
    *
    * This method is called to return the restoredSnapshot from the SnapshotMetaRecord
    */
   public void setRestoredSnapshot(long snapshotId) {
      this.restoredSnapshot = snapshotId;
   }

   /**
    * getTableName
    * @return String
    * 
    * This method is called to retrieve the Table associated with this snapshot record.
    */
   public String getTableName() {
       return this.tableName;
   }

   /**
    * setTableName
    * @param String
    * 
    * This method is called to set the table name associated with this snapshot record.
    */
   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
   }

   /**
    * getUserTag
    * @return String
    *
    * This method is called to retrieve the tag associated with this snapshot.
    */
   public String getUserTag() {
       return this.userTag;
   }

   /**
    * setUserTag
    * @param String
    *
    * This method is called to set the tag associated with this snapshot.
    */
   public void setUserTag(final String userTag) {
       this.userTag = new String(userTag);
   }

   /**
    * getSnapshotName
    * @return String
    *
    * This method is called to retrieve the name to the associated snapshot.
    */
   public String getSnapshotName() {
       return this.snapshotName;
   }

   /**
    * setSnapshotName
    * @param String
    *
    * This method is called to set the name to the associated snapshot.
    */
   public void setSnapshotName(final String snapshotName) {
       this.snapshotName = new String(snapshotName);
       return;
   }

   /**
    * getSnapshotPath
    * @return String
    * 
    * This method is called to retrieve the path to the associated snapshot.
    */
   public String getSnapshotPath() {
       return this.snapshotPath;
   }
   
   /**
    * setSnapshotPath
    * @param String
    * 
    * This method is called to set the path to the associated snapshot.
    */
   public void setSnapshotPath(final String snapshotPath) {
       this.snapshotPath = new String(snapshotPath);
       return;
   }

   /**
    * getUserGenerated
    * @return boolean
    *
    * This method is called to check whether this snapshot was created as part of a user operation.
    */
   public boolean getUserGenerated() {
       return this.userGenerated;
   }

   /**
    * setUserGenerated
    * @param boolean
    *
    * This method is called to indicate whether this snapshot is part of a user initiated operation.
    */
   public void setUserGenerated(final boolean userGenerated) {
       this.userGenerated = userGenerated;
   }

   /**
    * getIncrementalTable
    * @return boolean
    *
    * This method is called to check whether this snapshot was created for an incremental table.
    */
   public boolean getIncrementalTable() {
       return this.incrementalTable;
   }

   /**
    * setIncrementalTable
    * @param boolean
    *
    * This method is called to indicate whether this snapshot was created for an incremental table.
    */
   public void setIncrementalTable(final boolean incrementalTable) {
       this.incrementalTable = incrementalTable;
   }

   /**
    * getSqlMetaData
    * @return boolean
    *
    * This method is called to check whether this snapshot was created for a SQL Metadata object.
    */
   public boolean getSqlMetaData() {
       return this.sqlMetaData;
   }

   /**
    * setSqlMetaData
    * @param boolean
    *
    * This method is called to indicate whether this snapshot was created for a SQL Metadata object.
    */
   public void setSqlMetaData(final boolean sqlMetaData) {
       this.sqlMetaData = sqlMetaData;
   }

   /**
    * getInLocalFS
    * @return boolean
    *
    * This method is called to check whether this snapshot is stored locally.
    */
   public boolean getInLocalFS() {
       return this.inLocalFS;
   }

   /**
    * setInLocalFS
    * @param boolean
    *
    * This method is called to indicate whether this snapshot is stored locally.
    */
   public void setInLocalFS(final boolean inLocalFS) {
       this.inLocalFS = inLocalFS;
   }

   /**
    * getExcluded
    * @return boolean
    *
    * This method is called to check whether this snapshot has been excluded from the recovered database.
    */
   public boolean getExcluded() {
       return this.excluded;
   }

   /**
    * setExcluded
    * @param boolean
    *
    * This method is called to indicate whether this snapshot has been excluded from the recovered database.
    */
   public void setExcluded(final boolean excluded) {
       this.excluded = excluded;
   }

   /**
    * getMutationStartTime
    * @return long
    * 
    * This method is called to retrieve the system time after which mutations would start.
    */
   public long getMutationStartTime() {
       return this.mutationStartTime;
   }

   /**
    * setMutationStartTime
    * @param long
    * 
    * This method is called to set the system time after which mutations would start.
    */
   public void setMutationStartTime(final long startTime) {
       this.mutationStartTime = startTime;
   }

   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to retrieve the path of the archived snapshot.
    */
   public String getArchivePath() {
       return this.archivePath;
   }

   /**
    * setMetaStatements
    * @param String
    * 
    * This method is called to set the list of metaStatements needed to make the snapshot valid in Trafodion MD tables.
    */
   public void setMetaStatements(final String metaStatements) {
       this.metaStatements = new String(metaStatements);
   }

   /**
    * getMetaStatements
    * @return String
    *
    * This method is called to get the list of metaStatements needed to make the snapshot valid in Trafodion MD tables.
    */
   public String getMetaStatements() {
       return this.metaStatements;
   }

   /**
    * createPath
    * @return String
    *
    * This method is called to create an appropriate path given a snapshot name.
    */
   public static String createPath(final String snapshotName) {
       return MD5Hash.getMD5AsHex(Bytes.toBytes(snapshotName));
   }

   /**
    * getNumLobs
    * @return int
    *
    * This method is called to return the number of lobSnapshots (LOB columns) for this table
    */
   public int getNumLobs() {
      return this.numLobs;
   }

   /**
    * setNumLobs
    * @return void
    *
    * This method is called to set the number of lobs in the SnapshotMetaRecord
    */
   public void setNumLobs(int num) {
      this.numLobs = num;
   }

   /**
    * setLobSnapshots
    * @param Set<long>
    *
    * This method is called to set the list of lobSnapshots that refer to lob columns of this table.
    */
   public void setLobSnapshots(final Set<Long> lobSnapshots) throws IOException{
	   for (Long ss : lobSnapshots){
	     if (ss.longValue() == this.key) {
	        // Can't have a dependency on ourselves.
	        IOException ioe = new IOException (" where ");
	        LOG.error(" Error setLobSnapshots setting lobSnapshots.  Cannot have a self dependency ", ioe);
	        throw ioe;
	     }
	   }
       this.lobSnapshots = lobSnapshots;
   }

   /**
    * getLobSnapshots
    * @return Set<Long>
    *
    * This method is called to get the list of lobSnapshots that maintain the lob columns of this table.
    */
   public Set<Long> getLobSnapshots() {
       return this.lobSnapshots;
   }

   /**
    * setDependentSnapshots
    * @param Set<long>
    *
    * This method is called to set the list of dependentSnapshots that depend on this snapshot.  As long
    * as there are dependent snapshots we cannot delete this record.
    */
   public void setDependentSnapshots(final Set<Long> dependentSnapshots) throws IOException{
	   for (Long ss : dependentSnapshots){
	     if (ss.longValue() == this.key) {
	           // Can't have a dependency on ourselves.
	        IOException ioe = new IOException (" where ");
	        LOG.error(" Error setDependentSnapshots setting dependentSnapshots.  Cannot have a self dependency ", ioe);
	        throw ioe;
	     }
	   }
       this.dependentSnapshots = dependentSnapshots;
   }

   /**
    * getDependentSnapshots
    * @return Set<Long>
    *
    * This method is called to get the list of dependentSnapshots that depend on this snapshot.  As long
    * as there are dependent snapshots we cannot delete this record.
    */
   public Set<Long> getDependentSnapshots() {
       return this.dependentSnapshots;
   }

   /**
    * toString
    * @return String
    */
   @Override
   public String toString() {
       return "SnaphotKey: " + key
            + ", version: " + version
            + ", recordType: " + recordType
            + ", supersedingSnapshot: " + supersedingSnapshot
            + ", restoredSnapshot: " + restoredSnapshot
            + ", restoreStartTime: " + restoreStartTime
            + ", hiatusTime: " + hiatusTime
            + ", tableName: " + tableName
            + ", userTag: " + userTag
            + ", snapshotName: " + snapshotName
            + ", snapshotPath: " + snapshotPath
            + ", userGenerated: " + userGenerated
            + ", incrementalTable: " + incrementalTable
            + ", sqlMetaData: " + sqlMetaData
            + ", inLocalFS: " + inLocalFS
            + ", excluded: " + excluded
            + ", mutationStartTime: " + mutationStartTime
            + ", archivePath: " + archivePath
            + ", metaStatements: " + metaStatements
            + ", numLobs: " + numLobs
            + ", lobSnapshots: " + lobSnapshots
            + ", dependentSnapshots: " + dependentSnapshots;
   }


   public boolean isSkipTag(){
      //if userGenerated is false, skip is true
      //use by BackupRestoreClient.backupObjects MutationCapture2.doHFileCreate MutationCapture2.doHFileClose
      boolean skip = !this.userGenerated;
      return skip;
   }

   public List<SnapshotMetaRecord> getBrcSnapshotList() {
      return brcSnapshotList;
   }

   public void setBrcSnapshotList(List<SnapshotMetaRecord> brcSnapshotList) {
      this.brcSnapshotList = brcSnapshotList;
   }
}

class SnapshotMetaRecordCompare implements Comparator<SnapshotMetaRecord>
{
    // Used for sorting in ascending order of
    // roll number
    public int compare(SnapshotMetaRecord a, SnapshotMetaRecord b)
    {
        return (a.getTableName().compareTo(b.getTableName()));
    }
}

class SnapshotMetaRecordReverseKeyCompare implements Comparator<SnapshotMetaRecord>
{
    // Used for sorting in descending key order so that newer
    // snapshots are listed first.
    public int compare(SnapshotMetaRecord a, SnapshotMetaRecord b)
    {
       if (b.getKey() > a.getKey()){
          return 1;
       }
       else if (b.getKey() < a.getKey()){
          return -1;
       }
       return 0;
    }
}
