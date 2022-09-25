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
public class SnapshotMetaLockRecord implements Serializable{


static final Log LOG = LogFactory.getLog(SnapshotMetaLockRecord.class);

   /**
	 *
	 */
   private static final long serialVersionUID = -2255721570861915255L;

   // These are the components of a SnapshotMetaLockRecord entry into the SnapshotMeta table
   private long key;
   private int version;
   private int recordType;
   private String userTag;
   private boolean lockHeld;
   
   /**
    * SnapshotMetaLockRecord
    * @param long key
    * @param int vers
    * @param String userTag
    * @param boolean lockHeld
    * @throws IOException 
    */
   public SnapshotMetaLockRecord (final long key, final int vers, final String userTag, final boolean lockHeld) throws IOException{
 
      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error SnapshotMetaLockRecord constructor() incompatible version "
               + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error SnapshotMetaLockRecord ", iae);
         throw iae;
      }

      this.key = key;
      this.version = vers;
      this.recordType = SnapshotMetaRecordType.SNAPSHOT_LOCK_RECORD.getValue();
      this.userTag = new String(userTag);
      this.lockHeld = lockHeld;

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaLockRecord constructor() " + this.toString());
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
    * getLockHeld
    * @return boolean
    * 
    * This method is called to determine whether a backup/restore operation is in progress or not.
    */
   public boolean getLockHeld() {
       return this.lockHeld;
   }

   /**
    * setLockHeld
    * @param boolean
    * 
    * This method is called to indicate whether a backup/restore operation is in progress or not.
    */
   public void setLockHeld(final boolean lockHeld) {
       this.lockHeld = lockHeld;
   }

   /**
    * toString
    * @return String this
    */
   @Override
   public String toString() {
       return "SnaphotLockKey: " + key
               + ", version: " + version
               + ", recordType: " + recordType
    		   + ", userTag: " + userTag + " lockHeld " + lockHeld;
   }

}
