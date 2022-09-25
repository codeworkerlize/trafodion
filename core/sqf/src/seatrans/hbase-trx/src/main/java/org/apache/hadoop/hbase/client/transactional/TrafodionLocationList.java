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
import org.apache.hadoop.hbase.util.ByteArrayKey;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import java.lang.management.ManagementFactory;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
//import java.util.TreeMap;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.transactional.TrafodionKeyRange;
import java.io.IOException;

public class TrafodionLocationList {

  static final Log LOG = LogFactory.getLog(TrafodionLocationList.class);

  private ArrayList<HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>>> list;
  private static STRConfig pSTRConfig        = null;
 
  public TrafodionLocationList() throws IOException {
    list = new ArrayList<HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>>>();
    pSTRConfig = STRConfig.getInstance(null);
  }

  public synchronized boolean add(TransactionRegionLocation trl) {
     if (LOG.isDebugEnabled()) LOG.debug("add Entry:  trl: " + trl);
     boolean added = false;
     String tableString = trl.getRegionInfo().getTable().getNameAsString();
     if (LOG.isDebugEnabled()) LOG.debug("Retrieving locations map for table: " + tableString);

     ByteArrayKey startKey =  new ByteArrayKey(trl.getRegionInfo().getStartKey());
     int peerId = trl.getPeerId();
     HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>> peerRegionsMap = null; 
     if (peerId < list.size())
        peerRegionsMap = list.get(peerId);
     HashMap<ByteArrayKey, TransactionRegionLocation> locations = null;
     if (peerRegionsMap != null) 
        locations = peerRegionsMap.get(tableString);
     if (locations != null) {
        // TableName already in the map.  Add the location to the set
        if (! locations.containsKey(startKey)) {
           added = true;
           locations.put(startKey,trl);
        }
        else
        {
          if( trl.addTotalNum == true) {
             //check previous status of totalNum
             TransactionRegionLocation oldTrl = locations.get(startKey);
             if(oldTrl.addTotalNum == false) {
               if (LOG.isDebugEnabled()) LOG.debug("do register again for readonly region trl: " + trl);
               added = true;
               locations.put(startKey,trl); //update the trl attribution, so next time will not trigger a new register
             }
          }
        }
     }
     else {
        // TableName not in the Map.  We can add it immediately.
        locations = new HashMap<ByteArrayKey,TransactionRegionLocation>();
        locations.put(startKey,trl);
        if (peerRegionsMap == null)
            peerRegionsMap = new HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>>();
        peerRegionsMap.put(tableString, locations);
        if (peerId > list.size()) {
           for (int i = list.size(); i < peerId; i++)
              list.add(null);        
        }   
        list.add(peerRegionsMap);
        added = true;
        if (LOG.isDebugEnabled()) LOG.debug("created locations map and added entry for keyString: " + startKey);
     }
     return added;
  }

  public HashMap<String, HashMap<ByteArrayKey,TransactionRegionLocation>> getList(int peerId) {
     if (LOG.isTraceEnabled()) LOG.trace("getList Entry");
     return list.get(peerId);
  }

  public int tableCount(int peerId) {
     return list.get(peerId).size();
  }

  public int regionCount() {
     int count = 0;
     Set<Integer> peers = pSTRConfig.getPeerIds();
     for (Integer peerId : peers) {
         count += regionCount(peerId);
     }
     return count;
  }

  public int getTotalNum() {
     int tn= 0;
     HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>> peerRegionsMap = list.get(0); 
     if( peerRegionsMap != null ) {
       for(  HashMap<ByteArrayKey, TransactionRegionLocation> tableMap :  peerRegionsMap.values() ) {
         for( TransactionRegionLocation trl : tableMap.values() ) {
           if(trl.addTotalNum == true) tn++;
         } 
       }
     }

     return tn;
  }

  public int regionCount(int peerId) {
     int count = 0;
     
     if (peerId < 0)
        return 0;
     if (peerId >= list.size())
        return 0;
     if (list.get(peerId) == null)
        return 0;
 
     for (Map.Entry<String, HashMap<ByteArrayKey, TransactionRegionLocation>> tableMap : list.get(peerId).entrySet())
        count +=  tableMap.getValue().size();
      return count;
  }


  public synchronized void clear() {
     if (LOG.isTraceEnabled()) LOG.trace("clear Entry");
     Set<Integer> peers = pSTRConfig.getPeerIds();
     for (Integer peerId : peers) {
        if (peerId < list.size())
           list.set(peerId, null);
     }
     return;
  }

  public boolean isReadOnlyLocation(TransactionRegionLocation trl) {
     String tableString = trl.getRegionInfo().getTable().getNameAsString();

     ByteArrayKey startKey =  new ByteArrayKey(trl.getRegionInfo().getStartKey());
     int peerId = trl.getPeerId();
     HashMap<String, HashMap<ByteArrayKey, TransactionRegionLocation>> peerRegionsMap = null;
     if (peerId < list.size())
        peerRegionsMap = list.get(peerId);
     HashMap<ByteArrayKey, TransactionRegionLocation> locations = null;
     if (peerRegionsMap != null)
     {
        locations = peerRegionsMap.get(tableString);
     }
     if (locations != null) {
        // TableName already in the map.  Add the location to the set
        if (! locations.containsKey(startKey)) {
           return true;
        }
        else
        {
             //check previous status of totalNum
             TransactionRegionLocation oldTrl = locations.get(startKey);
             if(oldTrl.addTotalNum == false) {
                return true;
             }
             else
                return false;
        }
     }
     else
       return true;
  }

  public int size()
  {
     return list.size();
  }
  /**
   * toString
   * @return String this
   *
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    Set<Integer> peers = pSTRConfig.getPeerIds();
    for (Integer peerId : peers) {
       if (peerId >= list.size())
          continue;
       if (list.get(peerId) == null)
          continue;
 
       for (Map.Entry<String, HashMap<ByteArrayKey,TransactionRegionLocation>> tableMap : list.get(peerId).entrySet()) {
          builder.append( "table " + tableMap.getKey() + "\n");
          for (TransactionRegionLocation loc : tableMap.getValue().values()) {
            builder.append("   start key: " + ((loc.getRegionInfo().getStartKey() != null) ?
                    (Bytes.equals(loc.getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW) ?
                          "INFINITE" : Hex.encodeHexString(loc.getRegionInfo().getStartKey())) : "NULL")
            + "   end key: " + ((loc.getRegionInfo().getEndKey() != null) ?
                    (Bytes.equals(loc.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW) ?
                          "INFINITE" : Hex.encodeHexString(loc.getRegionInfo().getEndKey())) : "NULL")
            + "\n");
          }
       }
    }
    return builder.toString();
  }
}
