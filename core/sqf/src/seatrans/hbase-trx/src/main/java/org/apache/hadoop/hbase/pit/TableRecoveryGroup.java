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
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
/**
 * Simple wrapper for an partial SnapshotMetaRecord and an optional list of n MutationMetaRecords.
 * This class represents the value portion of the recoveryTableMap in the RecoveryRecord class.
 * 
 * SEE ALSO:
 * <ul>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> SnapshotMeta
 * {@link SnapshotMeta}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
 * </li>
 * <li> MutationMeta
 * {@link MutationMeta}
 * </li>
 * <li> RecoveryRecord
 * {@link RecoveryRecord}
 * </li>
 * </ul>
 * 
 */
  public class TableRecoveryGroup implements Serializable{
	  
     static final Log LOG = LogFactory.getLog(TableRecoveryGroup.class);

     private SnapshotMetaRecord smr;
     private ArrayList<SnapshotMetaIncrementalRecord> snapshotIncrementalList = null;
     private ArrayList<MutationMetaRecord> mutationList;
     private ArrayList<LobMetaRecord> lobList;

     public TableRecoveryGroup(final SnapshotMetaRecord smr,
           final ArrayList<SnapshotMetaIncrementalRecord> incrementalList,
           final ArrayList<LobMetaRecord> lobList, final ArrayList<MutationMetaRecord> mutationList) {

        this.smr = smr;
        this.snapshotIncrementalList = incrementalList;
        this.lobList = lobList;
        this.mutationList = mutationList;
     }

     public boolean snapshotRecordIsNull() {
        return (smr == null);
     }

     public SnapshotMetaRecord getSnapshotRecord() {
        return smr;
     }

     public List<MutationMetaRecord> getMutationList() {
        return mutationList;
     }

     public List<LobMetaRecord> getLobList() {
        return lobList;
     }

     /**
      * getSnapshotIncrementalList
      * @return List<SnapshotMetaIncrementalRecord> snapshotIncrementalList
      *
      */
     public ArrayList<SnapshotMetaIncrementalRecord> getSnapshotIncrementalList() {
       return snapshotIncrementalList;
     }

     @Override
     public String toString() {
        return "Snapshot: " + smr + " snapshotIncrementalList: " + snapshotIncrementalList
               + " lobList " + lobList + " MutationList: " + mutationList;
     }
   }  // Class TableRecoveryGroup

  class TableRecoveryGroupCompare implements Comparator<TableRecoveryGroup>
  {
      // Used for sorting in ascending order of
      // roll number
      public int compare(TableRecoveryGroup a, TableRecoveryGroup b)
      {
        return (a.getSnapshotRecord().getTableName().compareTo(b.getSnapshotRecord().getTableName()));
      }
  }