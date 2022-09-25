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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

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
public class SnapshotMetaIncrementalRecord implements Serializable{

   static final Log LOG = LogFactory.getLog(SnapshotMetaIncrementalRecord.class);

   /**
	 *
	 */
   private static final long serialVersionUID = -2255721580861915265L;

   // These are the components of a record entry into the SnapshotMeta table
   private long key;
   private int version;
   private int recordType;
   private long rootSnapshot;
   private long parentSnapshot;
   private long childSnapshot;
   private boolean excluded;
   private String tableName;
   private String userTag;
   private long priorMutationStartTime;
   private String snapshotName;
   private String snapshotPath;
   private boolean inLocalFS;
   private boolean archived;
   private String archivePath;
   private String metaStatements;
   private int numLobs;
   private Set<Long> lobSnapshots;
   
   /**
    * SnapshotMetaIncrementalRecord
    * @param long key
    * @param int vers
    * @param String tableName
    * @param String userTag
    * @param String snapshotName
    * @param String snapshotPath
    * @param boolean inLocalFS
    * @param boolean archived
    * @param String archivePath
    * @throws IOException 
    */
   public SnapshotMetaIncrementalRecord (final long key, final int vers, final long rootSnapshot,
        final long parentSnapshot, final long childSnapshot, final long priorMutationStartTime,
        final String tableName, final String userTag, final String snapshotName,
		final String snapshotPath, final boolean inLocalFS, final boolean excluded,
        final boolean archived, final String archivePath, final String metaStatements,
        final int numLobs, final Set<Long> lobSnapshots) throws IOException{
 
      if (LOG.isTraceEnabled()) LOG.trace("Enter SnapshotMetaIncrementalRecord constructor for key: " + key
              + " version: " + vers + " tableName: " + tableName + " userTag: " + userTag
              + " rootSnapshot: " + rootSnapshot + " parentSnapshot: " + parentSnapshot
              + " childSnapshot: " + childSnapshot + " priorMutationStartTime: " + priorMutationStartTime
              + " snapshotPath: " + snapshotPath + " inLocalFS: " + String.valueOf(inLocalFS)
              + " excluded " + excluded + " archived: " + String.valueOf(archived)
              + " archivePath: " + archivePath + " metaStatements: " + metaStatements
              + " numLobs " + numLobs + " lobSnapshots " + lobSnapshots);

      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error SnapshotMetaIncrementalRecord constructor() incompatible version "
                  + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error SnapshotMetaIncrementalRecord ", iae);
         throw iae;
      }

      this.key = key;
      this.version = vers;
      this.recordType = SnapshotMetaRecordType.SNAPSHOT_INCREMENTAL_RECORD.getValue();
      this.rootSnapshot = rootSnapshot;
      this.parentSnapshot = parentSnapshot;
      this.childSnapshot = childSnapshot;
      this.priorMutationStartTime = priorMutationStartTime;
      this.tableName = new String(tableName);
      this.userTag = new String(userTag);
      this.snapshotName = snapshotName;
      this.snapshotPath = new String(snapshotPath);
      this.inLocalFS = inLocalFS;
      this.excluded = excluded;
      this.archived = archived;
      this.archivePath = new String(archivePath);
      this.metaStatements = new String(metaStatements);
      this.numLobs = numLobs;
      this.lobSnapshots = lobSnapshots;
      
      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaIncrementalRecord constructor() " + this.toString());
      return;
   }

   /**
    * SnapshotMetaIncrementalRecord
    * @param long key
    * @param String tableName
    * @param String userTag
    * @param String snapshotName
    * @param String snapshotPath
    */
   public SnapshotMetaIncrementalRecord (final long key, final long rootSnapshot, final long parentSnapshot,
       final long childSnapshot, final long priorMutationStartTime, final String tableName,
	   final String userTag, final String snapshotName, final String snapshotPath,
	   final int numLobs, final Set<Long> lobSnapshots) throws Exception  {
      this(key, SnapshotMetaRecordType.getVersion(),
              rootSnapshot, parentSnapshot,
              childSnapshot,
              priorMutationStartTime,
              tableName, userTag,
              snapshotName,
              snapshotPath,
              true /* inLocalFS */,
              false /* excluded */,
              false /* archived */,
              "DefaultPath" /* archivePath */,
              "EmptyStatements" /* metaStatements */,
              numLobs,
              lobSnapshots);
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
    * setRootSnapshotId
    * @return void
    *
    * This method is called to set the rootSnapshot from the SnapshotMetaIncrementalRecord
    */
   public void setRootSnapshotId(long snapshotId) {
      this.rootSnapshot = snapshotId;
   }

   /**
    * getRootSnapshotId
    * @return long
    *
    * This method is called to return the rootSnapshot from the SnapshotMetaIncrementalRecord
    */

   public long getRootSnapshotId() {
      return this.rootSnapshot;
   }

   /**
    * setParentSnapshotId
    * @return void
    *
    * This method is called to set the parentSnapshot from the SnapshotMetaIncrementalRecord
    */
   public void setParentSnapshotId(long snapshotId) {
      this.parentSnapshot = snapshotId;
   }

   /**
    * getParentSnapshotId
    * @return long
    *
    * This method is called to return the parentSnapshot from the SnapshotMetaIncrementalRecord
    */

   public long getParentSnapshotId() {
      return this.parentSnapshot;
   }

   /**
    * setChildSnapshotId
    * @return void
    *
    * This method is called to set the childSnapshotId from the SnapshotMetaIncrementalRecord
    */
   public void setChildSnapshotId(long snapshotId) {
      this.childSnapshot = snapshotId;
   }

   /**
    * getChildSnapshotId
    * @return long
    *
    * This method is called to return the childSnapshot from the SnapshotMetaIncrementalRecord
    */
   public long getChildSnapshotId() {
      return this.childSnapshot;
   }

   /**
    * setPriorMutationStartTime
    * @return void
    *
    * This method is called to set the priorMutationStartTime from the SnapshotMetaIncrementalRecord
    */
   public void setPriorMutationStartTime(long startId) {
      this.priorMutationStartTime = startId;
   }

   /**
    * getPriorMutationStartTime
    * @return long
    *
    * This method is called to return the priorMutationStartTime from the SnapshotMetaIncrementalRecord
    */
   public long getPriorMutationStartTime() {
      return this.priorMutationStartTime;
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
    * getArchived
    * @return boolean
    * 
    * This method is called to check whether this snaphot has been archived off platform.
    */
   public boolean getArchived() {
       return this.archived;
   }

   /**
    * setArchived
    * @param boolean
    * 
    * This method is called to indicate whether this snapshot has been archived off platform.
    */
   public void setArchived(final boolean archived) {
       this.archived = archived;
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
    * This method is called to set the number of lob snapshots associated with this incremental record
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
    * createPath
    * @return String
    *
    * This method is called to create an appropriate path given a snapshot name.
    */
   public static String createPath(final String snapshotName) {
       return MD5Hash.getMD5AsHex(Bytes.toBytes(snapshotName));
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
            + ", rootSnapshot: " + rootSnapshot
            + ", parentSnapshot: " + parentSnapshot
            + ", childSnapshot: " + childSnapshot
            + ", priorMutationStartTime: " + priorMutationStartTime
            + ", tableName: " + tableName
            + ", userTag: " + userTag
            + ", snapshotName: " + snapshotName
            + ", snapshotPath: " + snapshotPath
            + ", inLocalFS: " + inLocalFS
            + ", excluded " + excluded
            + ", archived: " + archived
            + ", archivePath: " + archivePath
            + ", metaStatements: " + metaStatements;
   }
}

class SnapshotMetaIncrementalRecordCompare implements Comparator<SnapshotMetaIncrementalRecord>
{
    // Used for sorting in ascending order of
    // roll number
    public int compare(SnapshotMetaIncrementalRecord a, SnapshotMetaIncrementalRecord b)
    {
        return (a.getTableName().compareTo(b.getTableName()));
    }
}
