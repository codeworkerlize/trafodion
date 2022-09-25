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
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.Serializable;
/**
 * This class is one of three used for writing rows into the SnapshotMeta class
 * and Table.  This class is specific to writing a record indicating the start
 * of a full snapshot operation.  Such records are not written for partial
 * snapshots.
 * 
 * @see org.apache.hadoop.hbase.client.transactional.SnapshotMetaRecord#SnapshotMetaRecord(final long key, final short recordType, final String tableName, final String userTag, 
		     final String snapshotPath)
 */
public class SnapshotMetaStartRecord implements Serializable{


static final Log LOG = LogFactory.getLog(SnapshotMetaStartRecord.class);

   /**
	 *
	 */
   private static final long serialVersionUID = -2255721580861915255L;

   // These are the components of a SnapshotMetaStartRecord entry into the SnapshotMeta table
   private long key;
   private int version;
   private int recordType;
   private String userTag;
   private boolean snapshotComplete;
   private long completionTime;
   private long completionWallTime;
   private boolean fuzzyBackup;
   private boolean markedForDeletion;
   private boolean incrementalBackup;
   private boolean firstIncremental;
   private String extendedAttributes;
   private String backupType;
   private long startTime;
   
   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final String userTag, final boolean incrementalBackup,
        final String extendedAttributes, final String backupType, final long startTime) throws IOException{
      this(key, SnapshotMetaRecordType.getVersion(), userTag, /* snapshotComplete */ false, /* completionTime */ 0, /* incrementalBackup */ false,
                                incrementalBackup, extendedAttributes, backupType, startTime);
   }

   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @param boolean snapshotComplete
    * @param long completionTime
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final String userTag, final boolean snapshotComplete,
		                           final long completionTime, final boolean incrementalBackup,
		                           final String extendedAttributes, final String backupType,
		                           final long startTime) throws IOException{
      this(key, SnapshotMetaRecordType.getVersion(), userTag, snapshotComplete, completionTime, /* markedForDeletion */ false,
                                incrementalBackup, extendedAttributes, backupType, startTime);
   }

   public SnapshotMetaStartRecord (final long key, final int vers, final String userTag, final boolean snapshotComplete,
           final long completionTime, final boolean markedForDeletion, final boolean incrementalBackup,
           final String extendedAttributes, final String backupType, final long startTime) throws IOException{

      this(key, vers, userTag, snapshotComplete, completionTime, /* fuzzyBackup */ false,
	            markedForDeletion, incrementalBackup, /* firstIncremental */ false, extendedAttributes,
	            backupType, startTime);
   }

   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @param boolean snapshotComplete
    * @param long completionTime
    * @param boolean markedForDeletion
    * @param boolean incrementalBackup
    * @param String extendedAttributes
    * @param String backupType
    * @param long startTime
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final int vers, final String userTag, final boolean snapshotComplete,
		              final long completionTime, final boolean fuzzyBackup, final boolean markedForDeletion, final boolean incrementalBackup,
                      final boolean firstIncremental, final String extendedAttributes, final String backupType, final long startTime) throws IOException{
 
      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error SnapshotMetaStartRecord constructor() incompatible version "
               + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error SnapshotMetaStartRecord ", iae);
         throw iae;
      }

      this.key = key;
      this.version = vers;
      this.recordType = SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getValue();
      this.userTag = new String(userTag);
      this.snapshotComplete = snapshotComplete;
      this.completionTime = completionTime;
      this.completionWallTime = 0L;
      this.fuzzyBackup = fuzzyBackup;
      this.markedForDeletion = markedForDeletion;
      this.incrementalBackup = incrementalBackup;
      this.firstIncremental = firstIncremental;
      this.setExtendedAttributes(extendedAttributes);
      this.backupType = new String(backupType);
      this.startTime = startTime;

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaStartRecord constructor() " + this.toString());
      return;
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
    * getSnapshotComplete
    * @return boolean
    * 
    * This method is called to determine whether this full snapshot completed or not.
    */
   public boolean getSnapshotComplete() {
       return this.snapshotComplete;
   }

   /**
    * setSnapshotComplete
    * @param boolean
    * 
    * This method is called to indicate whether this full snapshot completed or not.
    */
   public void setSnapshotComplete(final boolean snapshotComplete) {
       this.snapshotComplete = snapshotComplete;
   }

   /**
    * getCompletionTime
    * @return long
    * 
    * This method is called to retrieve the snapshot completion time.
    */
   public long getCompletionTime() {
       return this.completionTime;
   }

   public long getCompletionWallTime() {
       return this.completionWallTime;
   }

   /**
    * setCompletionTime
    * @param long completionTime
    * 
    * This method is called to set the snapshot completion time.
    */
   public void setCompletionTime(final long completionTime) {
       this.completionTime = completionTime;
   }
   public void setCompletionWallTime(final long completionTime) {
       this.completionWallTime = completionTime;
   }

   /**
    * getFuzzyBackup
    * @return boolean
    *
    * This method is called to determine whether this is a fuzzy backup.
    */
   public boolean getFuzzyBackup() {
       return this.fuzzyBackup;
   }

   /**
    * setFuzzyBackup
    * @param boolean
    *
    * This method is called to set whether this is a fuzzy backup.
    */
   public void setFuzzyBackup(final boolean value) {
       this.fuzzyBackup = value;
   }

   /**
    * getMarkedForDeletion
    * @return boolean
    * 
    * This method is called to determine whether this full snapshot is intended to be deleted.
    */
   public boolean getMarkedForDeletion() {
       return this.markedForDeletion;
   }

   /**
    * setMarkedForDeletion
    * @param boolean
    * 
    * This method is called to set whether this full snapshot should be deleted.
    */
   public void setMarkedForDeletion(final boolean markedForDeletion) {
       this.markedForDeletion = markedForDeletion;
   }
   
   /**
    * getIncrementalBackup
    * @return boolean
    *
    * This method is called to determine whether this is an incremental backup or not.
    */
   public boolean getIncrementalBackup() {
       return this.incrementalBackup;
   }

   /**
    * setIncrementalBackup
    * @param boolean
    *
    * This method is called to set whether this backup is incremental or not.
    */
   public void setIncrementalBackup(final boolean incrementalBackup) {
       this.incrementalBackup = incrementalBackup;
   }

   /**
    * getFirstIncremental
    * @return boolean
    *
    * This method is called to determine whether this is the first incremental backup
    * following a regular fuzzy backup or not.
    */
   public boolean getFirstIncremental() {
       return this.firstIncremental;
   }

   /**
    * setIncrementalBackup
    * @param boolean
    *
    * This method is called to set whether this is the first incremental backup
    * following a regular fuzzy backup or not.
    */
   public void setFirstIncremental(final boolean value) {
       this.firstIncremental = value;
   }

   /**
    * getExtendedAttributes
    * @return String
    *
    * This method is called to retrieve the SQL attributes string from the backup operation.
    */
   public String getExtendedAttributes() {
       return this.extendedAttributes;
   }

   /**
    * setExtendedAttributes
    * @param String
    *
    * This method is called to set the SQL attributes string from the backup operation.
    */
   public void setExtendedAttributes(String attributes) {
       this.extendedAttributes = new String(attributes);
   }

   /**
    * getBackupType
    * @return String
    * 
    * This method is called to determine whether this snapshot set is full/schema/meta.
    */
   public String getBackupType() {
       return this.backupType;
   }

   /**
    * getStartTime
    * @return long
    *
    * This method is called to retrieve the system time associated with this start record.
    */
   public long getStartTime() {
      return this.startTime;
   }

   /**
    * setStartTime
    * @param long
    *
    * This method is called to set the system time associated with this start record.
    */
   public void setStartTime(final long startTime) {
      this.startTime = startTime;
   }


   /**
    * toString
    * @return String this
    */
   @Override
   public String toString() {
       return "SnaphotStartKey: " + key
               + ", version: " + version
               + ", recordType: " + recordType
               + ", userTag: " + userTag + ", snapshotComplete: " + snapshotComplete
               + ", completionTime: " + completionTime
               + ", fuzzyBackup: " + fuzzyBackup + ", markedForDeletion: " + markedForDeletion
               + ", incrementalBackup: " + incrementalBackup + ", firstIncremental: " + firstIncremental
               + ", extendedAttributes: " + extendedAttributes + ", backupType: " + backupType
               + ", startTime: " + startTime;
   }

}

class SnapshotMetaStartRecordReverseKeyCompare implements Comparator<SnapshotMetaStartRecord>
{
    // Used for sorting in descending key order so that newer
    // snapshots are listed first.
    public int compare(SnapshotMetaStartRecord a, SnapshotMetaStartRecord b)
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
