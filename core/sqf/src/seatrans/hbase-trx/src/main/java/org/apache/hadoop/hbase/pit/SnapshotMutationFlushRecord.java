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
import java.util.Iterator;

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
public class SnapshotMutationFlushRecord implements Serializable{


static final Log LOG = LogFactory.getLog(SnapshotMutationFlushRecord.class);

   /**
	 *
	 */
   private static final long serialVersionUID = -2255721570861915335L;

   // These are the components of a SnapshotMutationFlushRecord entry into the SnapshotMeta table
   private long key;
   private int version;
   private int recordType;
   private int interval;
   private long commitId;
   private ArrayList<String> tableList;

   /**
    * SnapshotMetaLockRecord
    * @param long key
    * @param int vers
    * @param int interval
    * @param long commitId
    * @throws IOException 
    */
   public SnapshotMutationFlushRecord (final long key, final int vers, final int interval, final long commitId,
                 final ArrayList<String> tableList) throws IOException{
 
      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error SnapshotMutationFlushRecord constructor() incompatible version "
               + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error SnapshotMutationFlushRecord ", iae);
         throw iae;
      }

      this.key = key;
      this.version = vers;
      this.recordType = SnapshotMetaRecordType.SNAPSHOT_MUTATION_FLUSH_RECORD.getValue();
      this.interval = interval;
      this.commitId = commitId;
      this.tableList = tableList;

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMutationFlushRecord constructor() " + this.toString());
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
    * getInterval
    * @return int
    * 
    * This method is called to retrieve the interval associated with this record.
    */
   public int getInterval() {
       return this.interval;
   }

   /**
    * setInterval
    * @param int
    * 
    * This method is called to set the interval associated with this record.
    */
   public void setInterval(final int param) {
       this.interval = param;
   }

   /**
    * getCommitId
    * @return long
    * 
    * This method is called to retrieve the commitId from this flush interval.
    */
   public long getCommitId() {
       return this.commitId;
   }

   /**
    * setCommitId
    * @param long
    * 
    * This method is called to set a commitId from a flush interval.
    */
   public void setCommitId(final long param) {
       this.commitId = param;
   }

   /**
    * setTableList
    * @param ArrayList<String>
    *
    * This method is called to set the list of tables for a flush interval.
    */
   public void setTableList(final ArrayList<String> tables) {
       this.tableList = tables;
   }

   /**
    * getTableList
    * @return ArrayList<String>
    *
    * This method is called to retrieve the list of tables for a flush interval.
    */
   public ArrayList<String> getTableList() {
	   if (this.tableList != null){
          return this.tableList;
	   }
	   else{
          return new ArrayList<String>();
	   }
   }


   /**
    * toString
    * @return String this
    */
   @Override
   public String toString() {
       StringBuilder tableString = new StringBuilder();
       Iterator<String> li  = this.getTableList().iterator();
       while (li.hasNext()){
          String currTab = li.next();
          tableString.append(", ");
          tableString.append(currTab);          
       }
       return "SnapshotMutationFlushKey: " + key
               + ", version: " + version
               + ", recordType: " + recordType
    		   + ", interval: " + interval + " commitId " + commitId
    	       + tableString.toString();
   }

}
