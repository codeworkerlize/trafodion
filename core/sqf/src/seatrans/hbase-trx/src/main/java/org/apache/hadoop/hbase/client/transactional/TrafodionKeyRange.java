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

import java.io.IOException;

public class TrafodionKeyRange {

  static final Log LOG = LogFactory.getLog(TrafodionKeyRange.class);

  public byte[] startKey;
  public byte[] endKey;
  
  public TrafodionKeyRange(byte[] startKey, byte[] endKey) {
     this.startKey = startKey;
     this.endKey = endKey;
  }
    
  public byte[] getStartKey(){
    return startKey;
  }

  public byte[] getEndKey(){
    return endKey;
  }

  public int compareTo(TrafodionKeyRange o) {
    if (o == null) {
      if (LOG.isDebugEnabled()) LOG.debug("CompareTo TrafodionKeyRange object is null");
      return 1;
    }

    if (LOG.isTraceEnabled()) LOG.trace("compareTo ENTRY: - comparing keys for "
              + "\n This start key    : " + Hex.encodeHexString(this.getStartKey())
              + "\n Object's start key: " + Hex.encodeHexString(o.getStartKey())
              + "\n This end key    : " + Hex.encodeHexString(this.getEndKey())
              + "\n Object's end key: " + Hex.encodeHexString(o.getEndKey()));

      // Here we are going to compare the keys as a range we can return 0
      // For these comparisons it's important to remember that 'this' is the object that is being added
      // and that 'object' is an object already added to a container.
      //
      // We are trying to limit the registration of daughter regions after a region split.
      // So if a location is already added whose startKey is less than ours and whose end
      // key is greater than ours, we will return 0 so that 'this' does not get added into
      // the participatingRegions list.

      // firstKeyInRange will be true if object's startKey is less than ours.
      int startKeyResult = Bytes.compareTo(this.getStartKey(), o.getStartKey());
      boolean firstKeyInRange = startKeyResult >= 0;
      boolean objLastKeyInfinite = Bytes.equals(o.getEndKey(), HConstants.EMPTY_END_ROW);
      boolean thisLastKeyInfinite = Bytes.equals(this.getEndKey(), HConstants.EMPTY_END_ROW);
      int endKeyResult = Bytes.compareTo(this.getEndKey(), o.getEndKey());

      // lastKey is in range if the existing object has an infinite end key, no matter what this end key is.
      boolean lastKeyInRange =  objLastKeyInfinite || (! thisLastKeyInfinite && endKeyResult <= 0);
      if (LOG.isTraceEnabled()) LOG.trace("firstKeyInRange " + firstKeyInRange + " lastKeyInRange " + lastKeyInRange);

      if (firstKeyInRange && lastKeyInRange) {
         if (LOG.isDebugEnabled()) LOG.debug("Object's region contains this region's start and end keys.  Keys match ");
         return 0;
      }

      if (startKeyResult != 0){
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TrafodionKeyRange startKeys don't match: result is " + startKeyResult);
         return startKeyResult;
      }

      if (objLastKeyInfinite) {
         if (LOG.isInfoEnabled()) LOG.info("Object's region contains this region's end keys for "
                  + "\n This start key    : " + Hex.encodeHexString(this.getStartKey())
                  + "\n Object's start key: " + Hex.encodeHexString(o.getStartKey())
                  + "\n This end key    : " + Hex.encodeHexString(this.getEndKey())
                  + "\n Object's end key: " + Hex.encodeHexString(o.getEndKey()));
      }

      if (this.getStartKey().length != 0 && this.getEndKey().length == 0) {
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TrafodionKeyRange \"this\" is the last region: result is 1");
         return 1; // this is last region
      }
      if (o.getStartKey().length != 0 && o.getEndKey().length == 0) {
         if (LOG.isDebugEnabled()) LOG.debug("compareTo TrafodionKeyRange \"object\" is the last region: result is -1");
         return -1; // o is the last region
      }
      if (LOG.isDebugEnabled()) LOG.debug("compareTo TrafodionKeyRange endKeys comparison: result is " + endKeyResult);
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
    if (LOG.isDebugEnabled()) LOG.debug("equals o comparing TKR: " + o);
    return this.compareTo((TrafodionKeyRange)o) == 0;
  }

  /**
   * toString
   * @return String this
   *
   */
  @Override
  public String toString() {
    return super.toString()
            + " start key: " + ((getStartKey() != null) ?
                    (Bytes.equals(getStartKey(), HConstants.EMPTY_START_ROW) ?
                          "INFINITE" : Hex.encodeHexString(getStartKey())) : "NULL")
            + " end key: " + ((getEndKey() != null) ?
                    (Bytes.equals(getEndKey(), HConstants.EMPTY_END_ROW) ?
                          "INFINITE" : Hex.encodeHexString(getEndKey())) : "NULL");
  }
}
