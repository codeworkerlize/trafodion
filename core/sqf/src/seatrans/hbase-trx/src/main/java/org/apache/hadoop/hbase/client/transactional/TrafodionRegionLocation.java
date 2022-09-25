/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/

package org.apache.hadoop.hbase.client.transactional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.transactional.TrafodionKeyRange;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public class TrafodionRegionLocation implements Externalizable{

/**
  * 
  */
private static final long serialVersionUID = -1255621405442819901L;

static final Log LOG = LogFactory.getLog(TrafodionRegionLocation.class);

  public boolean tableRecordedDropped;
  public boolean generateCatchupMutations;
  public boolean mustBroadcast;
  public int peerId;
  public TableName tableName;
  public TrafodionKeyRange tkr;
  
  public TrafodionRegionLocation()
  {
     // Default constructor used for Externalizable interface
  }

  /*
   public TrafodionRegionLocation(HRegionInfo regionInfo, final String hostname, final int port) {
     //ServerName
     ServerName sn = new ServerName(hostname, port, 0);
     //regionInfo, hostname, port);
   }
   */
    public TrafodionRegionLocation(TableName tableName, byte[] startKey, byte[] endKey)
    {
       this(tableName, startKey, endKey, 0);
    }

    public TrafodionRegionLocation(TableName tableName, byte[] startKey, byte[] endKey, int pv_peerId) {
       peerId = pv_peerId;
       tableRecordedDropped = false;
       setGenerateCatchupMutations(false);
       setMustBroadcast(false);
       this.tableName = tableName;
       this.tkr = new TrafodionKeyRange(startKey, endKey);
    }
    
    public void setTableRecordedDropped()
    {
    	tableRecordedDropped = true;
        if (LOG.isTraceEnabled()) LOG.trace("Table recorded dropped for region:" + this.toString());
    }
    public boolean isTableRecodedDropped()
    {
    	return tableRecordedDropped;
    }

    public void setMustBroadcast(final boolean value) {
        this.mustBroadcast = value;
    }

    public boolean getMustBroadcast() {
        return this.mustBroadcast;
    }

    public void setPeerId(int pvPeerId)
    {
       peerId = pvPeerId;
    }
    public int getPeerId()
    {
       return peerId;
    }

    public void setGenerateCatchupMutations(boolean value)
    {
       generateCatchupMutations = value;
    }
    public boolean getGenerateCatchupMutations()
    {
       return generateCatchupMutations;
    }

    public TableName getTableName()
    {
       return tableName;
    }

    public byte[] getStartKey()
    {
       return tkr.getStartKey();
    }

    public byte[] getEndKey()
    {
       return tkr.getEndKey();
    }

    public TrafodionKeyRange getKeyRange()
    {
       return tkr;
    }
    
    public HRegionInfo getRegionInfo()
    {
       return new HRegionInfo(tableName, getStartKey(), getEndKey());
    }

    public int compareTo(HRegionLocation o) {
       IOException ioe = new IOException("Use TrafodionRegionLocation");
       if (LOG.isErrorEnabled()) LOG.error("CompareTo HRegionLocation is not supported. ", ioe);
       return 1;
    }

    public int compareTo(TrafodionRegionLocation o) {
      if (o == null) {
        if (LOG.isDebugEnabled()) LOG.debug("CompareTo TrafodionRegionLocation object is null");
        return 1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("compareTo ENTRY: " + o);
      if (this.getPeerId() != o.getPeerId()){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo peerIds differ: mine: " + this.getPeerId()
                + " object's: " + o.getPeerId());
         return 1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("CompareTo TrafodionRegionLocation Entry:  TableNames :\n      mine: "
              + this.getTableName().getNameAsString() + "\n object's : " + o.getTableName().getNameAsString());

      // Make sure this is the same table
      int result = this.getTableName().compareTo(o.getTableName());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo TrafodionRegionLocation TableNames are different: result is " + result);
         return result;
      }

      result = this.getKeyRange().compareTo(o.getKeyRange());
      if (LOG.isDebugEnabled()) LOG.debug("Key comparison returned result " + result
              + " for table " + this.getTableName().getNameAsString());

      return result;
   }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (LOG.isDebugEnabled()) LOG.debug("equals ENTRY: this: " + this + " o: " + o);
    if (this == o) {
      if (LOG.isDebugEnabled()) LOG.debug("equals same object: " + o);
      return true;
    }
    if (o == null) {
      if (LOG.isDebugEnabled()) LOG.debug("equals o is null");
      return false;
    }
    if (LOG.isDebugEnabled()) LOG.debug("equals o comparing TRL: " + o);
    return this.compareTo((TrafodionRegionLocation)o) == 0;
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	 tableRecordedDropped = in.readBoolean();
     generateCatchupMutations = in.readBoolean();
     mustBroadcast = in.readBoolean();
	 peerId = in.readInt();
     tableName = (TableName)in.readObject();
	 tkr = (TrafodionKeyRange)in.readObject();
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeBoolean(tableRecordedDropped);
    out.writeBoolean(generateCatchupMutations);
    out.writeBoolean(mustBroadcast);
    out.writeInt(peerId);
    out.writeObject(tableName);
    out.writeObject(tkr);
  }

  /**
   * toString
   * @return String this
   *
   */
  @Override
  public String toString() {
    HRegionInfo regionInfo = this.getRegionInfo(); 
    return super.toString() + " table " + getTableName().getNameAsString()
    		+ " encodedName " + regionInfo.getEncodedName()
            + " start key: " + ((regionInfo.getStartKey() != null) ?
                    (Bytes.equals(regionInfo.getStartKey(), HConstants.EMPTY_START_ROW) ?
                          "INFINITE" : Hex.encodeHexString(regionInfo.getStartKey())) : "NULL")
            + " end key: " + ((regionInfo.getEndKey() != null) ?
                    (Bytes.equals(regionInfo.getEndKey(), HConstants.EMPTY_END_ROW) ?
                          "INFINITE" : Hex.encodeHexString(regionInfo.getEndKey())) : "NULL")
            + " peerId " + this.peerId + " tableRecodedDropped " + isTableRecodedDropped()
            + " generateCatchupMutations " + getGenerateCatchupMutations()
            + " mustBroadcast " + getMustBroadcast();
  }
}
