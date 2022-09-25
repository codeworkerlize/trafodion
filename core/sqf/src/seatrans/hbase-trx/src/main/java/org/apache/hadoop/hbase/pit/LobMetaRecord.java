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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.Serializable;
import java.lang.IllegalArgumentException;
public class LobMetaRecord implements Serializable{

   static final Log LOG = LogFactory.getLog(LobMetaRecord.class);

   /**
     *
     */
   private static final long serialVersionUID = -4254026283686295171L;

   // These are the components of a record entry into the LobMeta table
   private long key;
   private int version;
   private String tableName;
   private String snapshotName;
   private long fileSize;
   private String userTag;
   private String hdfsPath;
   private String sourceDirectory;
   private boolean inLocalFS;
   private boolean archived;
   private String archivePath;

   /**
    * LobMetaRecord
    * @param long key
    * @param int version
    * @param String tableName
    * @param String snapshotName
    * @param long fileSize
    * @param String userTag
    * @param String hdfsPath
    * @param String sourceDirectory
    * @param boolean inLocalFS
    * @param boolean archived
    * @param String archivePath
    */
   public LobMetaRecord (final long key, final int vers, final String tableName, final String snapshotName,
           final long fileSize, final String userTag, final String hdfsPath, final String sourceDirectory,
		   final boolean inLocalFS, final boolean archived, final String archivePath) throws IllegalArgumentException{

      if (LOG.isTraceEnabled()) LOG.trace("Enter LobMetaRecord constructor");

      if (vers != SnapshotMetaRecordType.getVersion()){
         IllegalArgumentException iae = new IllegalArgumentException("Error LobMetaRecord constructor() incompatible version "
                 + vers + ", expected version " + SnapshotMetaRecordType.getVersion());
         LOG.error("Error LobMetaRecord ", iae);
         throw iae;
      }

      this.key = key;
      this.version = vers;
      this.tableName = new String(tableName);
      this.snapshotName = new String(snapshotName);
      this.fileSize = fileSize;
      this.userTag = new String(userTag);
      this.hdfsPath = new String(hdfsPath);
      this.sourceDirectory = new String(sourceDirectory);
      this.inLocalFS = inLocalFS;
      this.archived = archived;
      this.archivePath = new String(archivePath);

      if (LOG.isTraceEnabled()) LOG.trace("Exit LobMetaRecord constructor() " + this.toString());
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
    * This method is called to retrieve the Table associated with this Lob file.
    */
   public String getTableName() {
       return this.tableName;
   }

   /**
    * setTableName
    * @param String
    * 
    * This method is called to set the table name associated with this Lob file.
    */
   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
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
    * This method is called to set the file size for the Lob file.
    */
   public void setFileSize(final long fileSize) {
      this.fileSize = fileSize;
   }

   /**
    * getUserTag
    * @return String
    *
    * This method is called to get the user tag from a backup associated with this Lob file.
    */
   public String getUserTag() {
      return this.userTag;
   }

   /**
    * setUserTag
    * @param String
    *
    * This method is called to set the user tag from a backup  associated with this Lob file.
    */
   public void setUserTag(final String userTag) {
      this.userTag = new String(userTag);
   }

   /**
    * getHdfsPath
    * @return String
    * 
    * This method is called to get the path to this Lob file.
    */
   public String getHdfsPath() {
      return this.hdfsPath;
   }

   /**
    * setHdfsPath
    * @param String
    * 
    * This method is called to set the path to this Lob file.
    */
   public void setHdfsPath(final String hdfsPath) {
      this.hdfsPath = new String(hdfsPath);
      return;
   }

   /**
    * getSnapshotName
    * @return String
    *
    * This method is called to get the snapshot name.
    */
   public String getSnapshotName() {
      return this.snapshotName;
   }

   /**
    * setSnapshotName
    * @param String
    *
    * This method is called to get the snapshot name.
    */
   public void setSnapshotName(final String name) {
      this.snapshotName = new String(name);
      return;
   }

   /**
    * getSourceDirectory
    * @return String
    *
    * This method is called to get the source directory to snapshot came from.
    */
   public String getSourceDirectory() {
      return this.sourceDirectory;
   }

   /**
    * setSourceDirectory
    * @param String
    *
    * This method is called to set the source directory to snapshot came from.
    */
   public void setSourceDirectory(final String sourcePath) {
      this.sourceDirectory = new String(sourcePath);
      return;
   }

   /**
    * getInLocalFS
    * @return boolean
    *
    * This method is called to determine whether this Lob file is stored locally.
    */
   public boolean getInLocalFS() {
      return this.inLocalFS;
   }

   /**
    * setInLocalFS
    * @param boolean
    *
    * This method is called to indicate whether this Lob file is stored locally.
    */
   public void setInLocalFS(final boolean inLocalFS) {
      this.inLocalFS = inLocalFS;
   }

   /**
    * getArchived
    * @return boolean
    * 
    * This method is called to determine whether this Lob file has been archived.
    */
   public boolean getArchived() {
      return this.archived;
   }

   /**
    * setArchived
    * @param boolean
    * 
    * This method is called to indicate whether this Lob file has been archived.
    */
   public void setArchived(final boolean archived) {
      this.archived = archived;
   }

   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to get the path to this archived Lob file.
    */
   public String getArchivePath() {
      return this.archivePath;
   }
   
   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to set the path to this archived Lob file.
    */
   public void setArchivePath(final String archivePath) {
      this.archivePath = new String(archivePath);
      return;
   }
   
   /*
   * This method is called to update hdfs url after import before restore.
   */
   public void updateHdfsUrl(final String hdfsUrl) {
      int idx= this.hdfsPath.indexOf("/user/trafodion/lobs");
      this.hdfsPath = this.hdfsPath.substring(idx);
      this.hdfsPath = hdfsUrl + this.hdfsPath;
      return;
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
      return "LobRecordkey: " + getKey() + " version " + getVersion() + " tag " + getUserTag()
             + " tableName: " + getTableName()
             + " snapshotName: " + getSnapshotName() + " sourceDirectory: " + getSourceDirectory()
             + " fileSize: " + getFileSize() + " userTag " + getUserTag()
             + " hdfsPath: " + getHdfsPath() + " inLocalFS: " + getInLocalFS()
             + " archived: " + getArchived() + " archivePath: " + getArchivePath();
   }

}
