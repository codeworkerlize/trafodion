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
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;

public class TransactionRegionLocation extends HRegionLocation {

  static final Log LOG = LogFactory.getLog(TransactionRegionLocation.class);

  public boolean tableRecordedDropped;
  public boolean generateCatchupMutations;
  public boolean mustBroadcast;
  public boolean readOnly;
  public boolean addTotalNum;
  public long binlogWid;
  /*
   public TransactionRegionLocation(HRegionInfo regionInfo, final String hostname, final int port) {
     //ServerName
     ServerName sn = new ServerName(hostname, port, 0);
     //regionInfo, hostname, port);
   }
   */
    public TransactionRegionLocation(HRegionInfo regionInfo, ServerName servName)
    {
       this(regionInfo, servName,0);
    }

    public int peerId;

    public TransactionRegionLocation(HRegionInfo regionInfo, ServerName servName, int pv_peerId) {
	super(regionInfo, servName);
	peerId = pv_peerId;
	tableRecordedDropped = false;
	setGenerateCatchupMutations(false);
	setMustBroadcast(false);
	setReadOnly(false);
        addTotalNum = false;
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


    public void setBinlogWid(long w) {
        this.binlogWid = w;
    }

    public long getBinlogWid() {
        return this.binlogWid;
    }

    public boolean getReadOnly() {
        return this.readOnly;
    }

    public void setReadOnly(final boolean value) {
        this.readOnly = value;
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
       if (LOG.isTraceEnabled()) LOG.trace("setGenerateCatchupMutations: " + generateCatchupMutations
               + " for region:" + this.toString());
    }
    public boolean getGenerateCatchupMutations()
    {
       return generateCatchupMutations;
    }

    public int compareTo(HRegionLocation o) {
       IOException ioe = new IOException("Use TransactionRegionLocation");
       if (LOG.isErrorEnabled()) LOG.error("CompareTo HRegionLocation is not supported. ", ioe);
       return 1;
    }

    public int compareTo(TransactionRegionLocation o) {
      if (o == null) {
        if (LOG.isDebugEnabled()) LOG.debug("CompareTo TransactionRegionLocation object is null");
        return 1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("compareTo ENTRY: " + o);
      if (this.getPeerId() != o.getPeerId()){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo peerIds differ: mine: " + this.getPeerId()
                + " object's: " + o.getPeerId());
         return 1;
      }

      int result = super.getHostname().compareTo(o.getHostname());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("CompareTo TransactionRegionLocation Hostnames don't match for table : " + this.getRegionInfo().getTable().getNameAsString()
            + "\n      mine: " + this.getHostname()
            + "\n object's : " + o.getHostname());
         return result;
      }

      result = super.getPort() - o.getPort();
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("CompareTo TransactionRegionLocation Ports don't match for table : " + this.getRegionInfo().getTable().getNameAsString()
            + "\n      mine: " + this.getPort()
            + "\n object's : " + o.getPort());
         return result;
      }

      // Make sure this is the same table
      result = this.getRegionInfo().getTable().compareTo(o.getRegionInfo().getTable());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo TransactionRegionLocation TableNames are different: result is " + result);
         return result;
      }

      if (LOG.isDebugEnabled()) LOG.debug("Tables and peerIds match - comparing keys for "
              + this.getRegionInfo().getTable().getNameAsString()
              + "\n This start key    : " + Hex.encodeHexString(this.getRegionInfo().getStartKey())
              + "\n Object's start key: " + Hex.encodeHexString(o.getRegionInfo().getStartKey())
              + "\n This end key    : " + Hex.encodeHexString(this.getRegionInfo().getEndKey())
              + "\n Object's end key: " + Hex.encodeHexString(o.getRegionInfo().getEndKey()));

      // Here we are going to compare the keys as a range we can return 0
      // For these comparisons it's important to remember that 'this' is the object that is being added
      // and that 'object' is an object already added in the participationRegions set.
      //
      // We are trying to limit the registration of daughter regions after a region split.
      // So if a location is already added whose startKey is less than ours and whose end
      // key is greater than ours, we will return 0 so that 'this' does not get added into
      // the participatingRegions list.

      // firstKeyInRange will be true if object's startKey is less than ours.
      int startKeyResult = Bytes.compareTo(this.getRegionInfo().getStartKey(), o.getRegionInfo().getStartKey());
      boolean firstKeyInRange = startKeyResult >= 0;
      boolean objLastKeyInfinite = Bytes.equals(o.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW);
      boolean thisLastKeyInfinite = Bytes.equals(this.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW);
      int endKeyResult = Bytes.compareTo(this.getRegionInfo().getEndKey(), o.getRegionInfo().getEndKey());

      // lastKey is in range if the existing object has an infinite end key, no matter what this end key is.
      boolean lastKeyInRange =  objLastKeyInfinite || (! thisLastKeyInfinite && endKeyResult <= 0);
      if (LOG.isTraceEnabled()) LOG.trace("firstKeyInRange " + firstKeyInRange + " lastKeyInRange " + lastKeyInRange);

      if (firstKeyInRange && lastKeyInRange) {
         if (LOG.isDebugEnabled()) LOG.debug("Object's region contains this region's start and end keys.  Regions match for "
                                   + o.getRegionInfo().getTable().getNameAsString());
         return 0;
      }

      if (startKeyResult != 0){
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TransactionRegionLocation startKeys don't match: result is " + startKeyResult);
         return startKeyResult;
      }

      if (objLastKeyInfinite) {
         if (LOG.isInfoEnabled()) LOG.info("Object's region contains this region's end keys for "
                  + o.getRegionInfo().getTable().getNameAsString()
                  + "\n This start key    : " + Hex.encodeHexString(this.getRegionInfo().getStartKey())
                  + "\n Object's start key: " + Hex.encodeHexString(o.getRegionInfo().getStartKey())
                  + "\n This end key    : " + Hex.encodeHexString(this.getRegionInfo().getEndKey())
                  + "\n Object's end key: " + Hex.encodeHexString(o.getRegionInfo().getEndKey()));
      }

      if (this.getRegionInfo().getStartKey().length != 0 && this.getRegionInfo().getEndKey().length == 0) {
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TransactionRegionLocation \"this\" is the last region: result is 1");
         return 1; // this is last region
      }
      if (o.getRegionInfo().getStartKey().length != 0 && o.getRegionInfo().getEndKey().length == 0) {
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TransactionRegionLocation \"object\" is the last region: result is -1");
         return -1; // o is the last region
      }
      if (LOG.isDebugEnabled()) LOG.debug("compareTo TransactionRegionLocation endKeys comparison: result is " + endKeyResult);
      return endKeyResult;
   }

    public int compareToWithLogging(TransactionRegionLocation o) {
      if (o == null) {
         if (LOG.isDebugEnabled()) LOG.debug("CompareTo TransactionRegionLocation object is null");
         return 1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("compareTo ENTRY: " + o);
      if (this.getPeerId() != o.getPeerId()){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo peerIds differ: mine: " + this.getPeerId()
                  + " object's: " + o.getPeerId());
         return 1;
      }

      if (LOG.isTraceEnabled()) LOG.trace("CompareTo TransactionRegionLocation Entry:  TableNames :\n      mine: "
              + this.getRegionInfo().getTable().getNameAsString() + "\n object's : " + o.getRegionInfo().getTable().getNameAsString());

      // Make sure this is the same table
      int result = this.getRegionInfo().getTable().compareTo(o.getRegionInfo().getTable());
      if (result != 0){
         if (LOG.isTraceEnabled()) LOG.trace("compareTo TransactionRegionLocation TableNames are different: result is " + result);
         return result;
      }

      if (LOG.isDebugEnabled()) LOG.debug("Tables and peerIds match - comparing keys for "
                + this.getRegionInfo().getTable().getNameAsString()
                + "\n This start key    : " + Hex.encodeHexString(this.getRegionInfo().getStartKey())
                + "\n Object's start key: " + Hex.encodeHexString(o.getRegionInfo().getStartKey())
                + "\n This end key    : " + Hex.encodeHexString(this.getRegionInfo().getEndKey())
                + "\n Object's end key: " + Hex.encodeHexString(o.getRegionInfo().getEndKey()));

        // Here we are going to compare the keys as a range we can return 0
        // For these comparisons it's important to remember that 'this' is the object that is being added
        // and that 'object' is an object already added in the participationRegions set.
        //
        // We are trying to limit the registration of daughter regions after a region split.
        // So if a location is already added whose startKey is less than ours and whose end
        // key is greater than ours, we will return 0 so that 'this' does not get added into
        // the participatingRegions list.

        // firstKeyInRange will be true if object's startKey is less than ours.
        int startKeyResult = Bytes.compareTo(this.getRegionInfo().getStartKey(), o.getRegionInfo().getStartKey());
        boolean firstKeyInRange = startKeyResult >= 0;
        boolean objLastKeyInfinite = Bytes.equals(o.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW);
        boolean thisLastKeyInfinite = Bytes.equals(this.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW);
        int endKeyResult = Bytes.compareTo(this.getRegionInfo().getEndKey(), o.getRegionInfo().getEndKey());

        // lastKey is in range if the existing object has an infinite end key, no matter what this end key is.
        boolean lastKeyInRange =  objLastKeyInfinite || (! thisLastKeyInfinite && endKeyResult <= 0);
        LOG.warn("firstKeyInRange " + firstKeyInRange + " lastKeyInRange " + lastKeyInRange);

        if (firstKeyInRange && lastKeyInRange) {
           if (LOG.isDebugEnabled()) LOG.debug("Object's region contains this region's start and end keys.  Regions match for "
                                     + o.getRegionInfo().getTable().getNameAsString());
           return 0;
        }

        if (startKeyResult != 0){
           LOG.warn("compareTo TransactionRegionLocation startKeys don't match: result is " + startKeyResult);
           return startKeyResult;
        }

        if (objLastKeyInfinite) {
           LOG.warn("Object's region contains this region's end keys for "
                    + o.getRegionInfo().getTable().getNameAsString()
                    + "\n This start key    : " + Hex.encodeHexString(this.getRegionInfo().getStartKey())
                    + "\n Object's start key: " + Hex.encodeHexString(o.getRegionInfo().getStartKey())
                    + "\n This end key    : " + Hex.encodeHexString(this.getRegionInfo().getEndKey())
                    + "\n Object's end key: " + Hex.encodeHexString(o.getRegionInfo().getEndKey()));
        }

        if (this.getRegionInfo().getStartKey().length != 0 && this.getRegionInfo().getEndKey().length == 0) {
           LOG.warn("compareTo TransactionRegionLocation \"this\" is the last region: result is 1");
           return 1; // this is last region
        }
        if (o.getRegionInfo().getStartKey().length != 0 && o.getRegionInfo().getEndKey().length == 0) {
           LOG.warn("compareTo TransactionRegionLocation \"object\" is the last region: result is -1");
           return -1; // o is the last region
        }
        LOG.warn("compareTo TransactionRegionLocation endKeys comparison: result is " + endKeyResult);
        return endKeyResult;
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
    if (!(o instanceof HRegionLocation)) {
      IOException ioe = new IOException("Use TransactionRegionLocation");
      if (LOG.isErrorEnabled()) LOG.error("equals o is not an instance of HRL: " + o + " ", ioe);
      return false;
    }
    if (o instanceof TransactionRegionLocation) {
        if (LOG.isDebugEnabled()) LOG.debug("equals o comparing TRL: " + o);
        return this.compareTo((TransactionRegionLocation)o) == 0;
    }
    else {
       IOException ioe = new IOException("Use TransactionRegionLocation");
       if (LOG.isErrorEnabled()) LOG.error("equals o comparing HRL is not supported: ", ioe);
       return this.compareTo((HRegionLocation)o) == 0;
    }
  }

  /**
   * toString
   * @return String this
   *
   */
  @Override
  public String toString() {
    return super.toString() + " encodedName " + super.getRegionInfo().getEncodedName()
            + " start key: " + ((super.getRegionInfo().getStartKey() != null) ?
                    (Bytes.equals(super.getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW) ?
                          "INFINITE" : Hex.encodeHexString(super.getRegionInfo().getStartKey())) : "NULL")
            + " end key: " + ((super.getRegionInfo().getEndKey() != null) ?
                    (Bytes.equals(super.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW) ?
                          "INFINITE" : Hex.encodeHexString(super.getRegionInfo().getEndKey())) : "NULL")
            + " peerId " + this.peerId + " tableRecodedDropped " + isTableRecodedDropped()
            + " generateCatchupMutations " + getGenerateCatchupMutations()
            + " mustBroadcast " + getMustBroadcast() + " readOnly " + getReadOnly();
  }
}
