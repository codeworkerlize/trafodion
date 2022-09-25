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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.Serializable;
import java.lang.IllegalArgumentException;
public class MutationMetaRecord implements Serializable{

   static final Log LOG = LogFactory.getLog(MutationMetaRecord.class);

   /**
     *
     */
   private static final long serialVersionUID = -4254066283686295171L;

   // These are the components of a record entry into the MutationMeta table
   private long key;
   private int version;
   private String tableName;
   private long rootSnapshot;
   private long priorIncrementalSnapshot;
   private long backupSnapshot;
   private long supersedingFullSnapshot;
   private long smallestCommitId;
   private long largestCommitId;
   private long fileSize;
   private String userTag;
   private String regionName;
   // mutationPath is created in MutationCapture2::doHFileCreate
   private String mutationPath;
   private boolean inLocalFS;
   private boolean archived;
   private String archivePath;
   //replace key
   private String rowKey;


   /**
    * MutationMetaRecord
    * @param String key
    * @param int version
    * @param String tableName
    * @param long rootSnapshot
    * @param long incrementalSnapshot
    * @param long backupSnapshot
    * @param long supersedingFullSnapshot
    * @param long smallestCommitId
    * @param long largestCommitId
    * @param long fileSize
    * @param String userTag
    * @param String regionName
    * @param String mutationPath
    * @param boolean inLocalFS
    * @param boolean archived
    * @param String archivePath
    */
   public MutationMetaRecord (final String key, final int vers, final String tableName, final long rootSnapshot,
           final long priorIncrementalSnapshot, final long backupSnapshot, final long supersedingFullSnapshot,
           final long smallestCommitId, final long largestCommitId,
		   final long fileSize, final String userTag, final String regionName, final String mutationPath,
		   final boolean inLocalFS, final boolean archived, final String archivePath) throws IllegalArgumentException{
 
      if (LOG.isDebugEnabled()) LOG.debug("Enter MutationMetaRecord constructor");

      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error MutationMetaRecord constructor() incompatible version "
                 + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error MutationMetaRecord ", iae);
         throw iae;
      }
      if (priorIncrementalSnapshot == 0){
         IllegalArgumentException iae2 = new IllegalArgumentException("incorrect incremental mutation");
         LOG.error("Exception in MutationMetaRecord ", iae2);
         throw iae2;
      }
      this.rowKey = key;
      this.version = vers;
      this.tableName = new String(tableName);
      this.rootSnapshot = rootSnapshot;
      this.priorIncrementalSnapshot = priorIncrementalSnapshot;
      this.backupSnapshot = backupSnapshot;
      this.supersedingFullSnapshot = supersedingFullSnapshot;
      this.smallestCommitId = smallestCommitId;
      this.largestCommitId = largestCommitId;
      this.fileSize = fileSize;
      this.userTag = new String(userTag);
      this.regionName = new String(regionName);
      this.mutationPath = new String(mutationPath);
      this.inLocalFS = inLocalFS;
      this.archived = archived;
      this.archivePath = new String(archivePath);

      if (LOG.isDebugEnabled()) LOG.debug("Exit MutationMetaRecord constructor() " + this.toString());
      return;
   }

   /**
    * MutationMetaRecord
    * @param String key
    * @param String tableName
    * @param long rootSnapshot
    * @param long priorIncrementalSnapshot
    * @param long backupSnapshot
    * @param long smallestCommitId
    * @param long largestCommitId
    * @param long fileSize
    * @param String userTag
    * @param String regionName
    * @param String mutationPath
    * @param boolean inLocalFS
    * @param boolean archived
    * @param String archivePath
    */
   public MutationMetaRecord (final String key, final String tableName, final long rootSnapshot,
           final long priorIncrementalSnapshot, final long backupSnapshot, final long supersedingFullSnapshot,
           final long smallestCommitId, final long largestCommitId,
		   final long fileSize, final String userTag, final String regionName, final String mutationPath,
		   final boolean inLocalFS, final boolean archived, final String archivePath) throws IllegalArgumentException{

	   // Add the version
	   this(key, SnapshotMetaRecordType.getVersion(), tableName, rootSnapshot,
           priorIncrementalSnapshot, backupSnapshot, supersedingFullSnapshot,
           smallestCommitId, largestCommitId,
		   fileSize, userTag, regionName, mutationPath,
		   inLocalFS, archived, archivePath);
   }

   /**
    * MutationMetaRecord
    * @param long key
    * @param String tableName
    * @param long rootSnapshot
    * @param long priorIncrementalSnapshot
    * @param long smallestCommitId
    * @param long largestCommitId
    * @param long fileSize
    * @param String userTag
    * @param String regionName
    * @param String mutationPath
    */
   public MutationMetaRecord (final String key, final String tableName, final long rootSnapshot,
           final long priorIncrementalSnapshot, final long backupSnapshot, final long supersedingFullSnapshot,
           final long smallestCommitId, final long largestCommitId,
		   final long fileSize, final String userTag, final String regionName, final String mutationPath) {
      this(key, tableName, rootSnapshot, priorIncrementalSnapshot, backupSnapshot,
                  supersedingFullSnapshot, smallestCommitId, largestCommitId, fileSize,
                  userTag, regionName, mutationPath,
                  true /* inLocalFS */, false /* archived */, null /* archive location */);
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

   public void setKey(long newKey) {
       this.key = newKey;
       return;
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
    * getTableName
    * @return String
    * 
    * This method is called to retrieve the Table associated with this mutation file.
    */
   public String getTableName() {
       return this.tableName;
   }

   /**
    * setTableName
    * @param String
    * 
    * This method is called to set the table name associated with this mutation file.
    */
   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
   }

   /**
    * getRootSnapshot
    * @return long
    *
    * This method is called to get the root snapshot record for this mutation file
    */
   public long getRootSnapshot() {
      return this.rootSnapshot;
   }

   /**
    * setRootSnapshot
    * @param long
    *
    * This method is called to set the association between this mutation file and a particular
    * root snapshot record.
    */
   public void setRootSnapshot(final long rootSnapshot) {
      this.rootSnapshot = rootSnapshot;
   }

   /**
    * getPriorIncrementalSnapshot
    * @return long
    *
    * This method is called to get the incremental snapshot record for this mutation file
    */
   public long getPriorIncrementalSnapshot() {
      return this.priorIncrementalSnapshot;
   }

   /**
    * setPriorIncrementalSnapshot
    * @param long
    *
    * This method is called to set the incremental snapshot association between this mutation file and a particular
    * incremental snapshot record.
    */
   public void setPriorIncrementalSnapshot(final long incrementalSnapshot) {
      this.priorIncrementalSnapshot = incrementalSnapshot;
   }

   /**
    * getSupersedingFullSnapshot
    * @return long
    *
    * This method is called to get the superseding snapshot record for this mutation file
    */
   public long getSupersedingFullSnapshot() {
      return this.supersedingFullSnapshot;
   }

   /**
    * setSupersedingFullSnapshot
    * @param long
    *
    * This method is called to set the superseding snapshot association between this mutation file and a particular
    * FULL snapshot record.
    */
   public void setSupersedingFullSnapshot(final long fullSnapshot) {
      this.supersedingFullSnapshot = fullSnapshot;
   }

   /**
    * getBackupSnapshot
    * @return long
    *
    * This method is called to get the backup snapshot record for this mutation file
    */
   public long getBackupSnapshot() {
      return this.backupSnapshot;
   }

   /**
    * setBackupSnapshot
    * @param long
    *
    * This method is called to set the backup snapshot association between this mutation file and a particular
    * incremental backup record.
    */
   public void setBackupSnapshot(final long Snapshot) {
      this.backupSnapshot = Snapshot;
   }

   /**
    * getSmallestCommitId
    * @return long
    * 
    * This method is called to get the smallest commitId from the mutations contained in this file.
    * This provides an optimization to the replay engine when determining which files need to
    * be replayed in order to recover to a particular point-in-time.
    */
   public long getSmallestCommitId() {
      return this.smallestCommitId;
   }

   /**
    * setSmallestCommitId
    * @param long
    * 
    * This method is called to set the smallest commitId for the mutations contained in this file.
    */
   public void setSmallestCommitId(final long commitId) {
      this.smallestCommitId = commitId;
   }

   /**
    * getLargestCommitId
    * @return long
    *
    * This method is called to get the largest commitId from the mutations contained in this file.
    * This provides an optimization to the replay engine when determining which files need to
    * be replayed in order to recover to a particular point-in-time.
    */
   public long getLargestCommitId() {
      return this.largestCommitId;
   }

   /**
    * setLargestCommitId
    * @param long
    *
    * This method is called to set the largest commitId for the mutations contained in this file.
    */
   public void setLargestCommitId(final long commitId) {
      this.largestCommitId = commitId;
   }

   /**
    * getFileSize
    * @return long
    * 
    * This method is called to get the file size of the particular mutaion file.
    */
   public long getFileSize() {
      return this.fileSize;
   }

   /**
    * setFileSize
    * @param long
    * 
    * This method is called to set the file size for the mutation file.
    */
   public void setFileSize(final long fileSize) {
      this.fileSize = fileSize;
   }

   /**
    * getUserTag
    * @return String
    *
    * This method is called to get the user tag from a backup associated with this mutation file.
    */
   public String getUserTag() {
      return this.userTag;
   }

   /**
    * setUserTag
    * @param String
    *
    * This method is called to set the user tag from a backup  associated with this mutation file.
    */
   public void setUserTag(final String userTag) {
      this.userTag = new String(userTag);
   }

   /**
    * getRegionName
    * @return String
    * 
    * This method is called to get the region name associated with this mutation file.
    */
   public String getRegionName() {
      return this.regionName;
   }

   /**
    * setRegionName
    * @param String
    * 
    * This method is called to set the region name associated with this mutation file.
    */
   public void setRegionName(final String regionName) {
      this.regionName = new String(regionName);
   }

   /**
    * getMutationPath
    * @return String
    * 
    * This method is called to get the path to this mutation file.
    * This method is used by MutationFileList sorting in ReplayEngine
    */
   public String getMutationPath() {
      return this.mutationPath;
   }
   
   /**
    * setMutationPath
    * @param String
    * 
    * This method is called to set the path to this mutation file.
    */
   public void setMutationPath(final String mutationPath) {
      this.mutationPath = new String(mutationPath);
      return;
   }

   /**
    * getInLocalFS
    * @return boolean
    *
    * This method is called to determine whether this mutation file is stored locally.
    */
   public boolean getInLocalFS() {
      return this.inLocalFS;
   }

   /**
    * setInLocalFS
    * @param boolean
    *
    * This method is called to indicate whether this mutation file is stored locally.
    */
   public void setInLocalFS(final boolean inLocalFS) {
      this.inLocalFS = inLocalFS;
   }

   /**
    * getArchived
    * @return boolean
    * 
    * This method is called to determine whether this mutation file has been archived.
    */
   public boolean getArchived() {
      return this.archived;
   }

   /**
    * setArchived
    * @param boolean
    * 
    * This method is called to indicate whether this mutation file has been archived.
    */
   public void setArchived(final boolean archived) {
      this.archived = archived;
   }

   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to get the path to this archived mutation file.
    */
   public String getArchivePath() {
      return this.archivePath;
   }
   
   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to set the path to this archived mutation file.
    */
   public void setArchivePath(final String archivePath) {
      this.archivePath = new String(archivePath);
      return;
   }

   /*
   * This method is called to update hdfs url after import before restore.
   */
   public void updateHdfsUrl(final String hdfsUrl) {
      int idx= this.mutationPath.indexOf("/user/trafodion/PIT");
      this.mutationPath = this.mutationPath.substring(idx);
      this.mutationPath = hdfsUrl + this.mutationPath;
      return;
   }

   /**
    * toString
    * @return String
    */
   @Override
   public String toString() {
      return "Mutationkey: " + key + " version " + version + " tag " + getUserTag() + " tableName: " + tableName
             + " rootSnapshot: " + rootSnapshot + " priorIncrementalSnapshot: " + priorIncrementalSnapshot
             + " backupSnapshot: " + backupSnapshot + " supersedingSnapshot " + getSupersedingFullSnapshot()
             + " smallestCommitId: " + smallestCommitId + " largestCommitId: " + largestCommitId
             + " fileSize: " + fileSize + " userTag " + userTag + " regionName: " + regionName
             + " mutationPath: " + mutationPath + " inLocalFS: " + inLocalFS
             + " archived: " + archived + " archivePath: " + archivePath + " rowKey " + rowKey;
   }

   /**
    * compatibility long key,if rowKey is null(import old export tag),use key for rowKey
    * @return
    */
   public String getRowKey() {
      if(null != rowKey && rowKey.length() > 0){
         return rowKey;
      }else {
         return String.valueOf(key);
      }
   }

   public void setRowKey(String rowKey) {
      this.rowKey = rowKey;
   }
}
