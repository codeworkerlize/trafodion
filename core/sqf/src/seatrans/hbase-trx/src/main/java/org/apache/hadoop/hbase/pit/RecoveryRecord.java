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
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;


/**
 * This is the main class utilized for point-in-time recovery.  When desiring to
 * recover to timeId(x) one would instantiate a new RecoveryRecord(timeId x) and
 * in the constructor a Map of tableNames and TableRecoveryGroups are created that
 * is sufficient to restore each of the tableNames in the database to the specified time.
 * 
 * SEE ALSO:
 * <ul>
 * <li> TableRecoveryGroup
 * {@link TableRecoveryGroup}
 * </li>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
 * </li>
 * </ul>
 */
public class RecoveryRecord implements Serializable{

   static final Log LOG = LogFactory.getLog(RecoveryRecord.class);

   /**
    *
    */
   private static final long serialVersionUID = -4254066283676295171L;

   // These are the components of a RecoveryRecord from which the database can be recovered.
   private List<SnapshotMetaIncrementalRecord> snapshotIncrementalList = null;
   private Map<String, TableRecoveryGroup> recoveryTableMap = new ConcurrentHashMap<String, TableRecoveryGroup>();
   private List<MutationMetaRecord> orphanedMutations = new ArrayList<MutationMetaRecord>();
   private SnapshotMetaStartRecord smsr = null;
   private String bkpType;
   private String opType;
   private String tag;
   private long endTime;
   private long endWallTime;
   private long startTime;
   private boolean incremental;
   private static Object RecoveryLock = new Object();
   private static boolean useFuzzyIBR = true;

   static {
      String useFuzzyIbrString = System.getenv("DTM_FUZZY_BACKUP");
      if (useFuzzyIbrString != null){
         useFuzzyIBR = (Integer.parseInt(useFuzzyIbrString.trim()) == 1) ? true : false;
      }
   }

   /**
    * RecoveryRecord
    * @param String tag
    * @param boolean adjust
    * @param boolean showOnly
    * @throws Exception
    *
    * The tag input to the constructor is the tag associated with the snapshot
    * the user wants to restore.
    * The adjust parameter indicates whether this recovery record should
    * include the real snapshots for tables when an incremental snapshot
    * is encountered.  For recovery operations, we do want to include the
    * real table snapshots that are listed as parent snapshots in the
    * incremental snapshot record.  But for 'delete backup' operations,
    * we want to delete the objects created as part of the backup.
    * So these operations should include the incremental records and not
    * the original snapshots.
    * The showOnly parameter is used to indicate if the RecoveryRecord is
    * being generated as part of a restore command with 'show objects',
    * which is essentially a read-only operation and should not make
    * any modifications to the underlying meta tables for the restore.
    * restoreTS parameter is the limit time from the IdTm for transactions
    * to be applied.  A zero value indicated this is just a simple restore
    * to the tag supplied.
    */
   public RecoveryRecord (Configuration config, String tag, String operation, boolean adjust,
          boolean force, long restoreTS, boolean showOnly) throws Exception {

     if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord constructor for tag " + tag
             + " opType " + operation + " adjust " + adjust + " force " + force
             + " restoreTS " + restoreTS + " showOnly " + showOnly);

     SnapshotMeta sm;
     List<SnapshotMetaRecord> snapshotList = null;
     ArrayList<SnapshotMetaIncrementalRecord> superSetIncrSnapshotList = null;
     LobMeta lm;
     MutationMeta mm;
     ArrayList<MutationMetaRecord> mutationList = new ArrayList<MutationMetaRecord>();
     ArrayList<String> schemaList = new ArrayList<String>();
     ArrayList<String> tableList = new ArrayList<String>();
     long preTagStartKey = 0;
     boolean regularExport = false;
     orphanedMutations = new ArrayList<MutationMetaRecord>();
     Map<String,List<SnapshotMetaRecord>> allBrcListMap = new HashMap<>();
     this.opType = operation;
     this.tag = tag;

     //all need update SnapshotMetaRecord data
     List<SnapshotMetaRecord> updateMetaRecords = new ArrayList<>();
      //all need update SnapshotMetaIncrementalRecord data
     List<SnapshotMetaIncrementalRecord> updateIncrementRecords = new ArrayList<>();
     //SnapshotMetaIncrementalRecord keys,DROP operation targetRec.getKey
     List<Long> deleteKeys = new ArrayList<>();

     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta " , e);
       throw e;
     }

     try {
        if (LOG.isTraceEnabled()) LOG.trace("  Creating LobMeta object ");
        lm = new LobMeta(config);
     }
     catch (Exception e) {
        if (LOG.isTraceEnabled()) LOG.trace("  Exception creating LobMeta ", e);
        throw e;
     }

     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta " , e);
       throw e;
     }
/*
     try {
        sm.lock(tag);
     }
     catch(Exception e){
        LOG.error("Exception acquiring SnapshotMeta lock " , e);
        throw e;
     }
*/
     try{
       // getPriorSnapshotSet for the associated tag
       StringBuilder backupType = new StringBuilder ();

       // If we have a restoreTS we can get the startRecord directly
       // without a scan, which is more efficient, although
       // we should get the same record.
       /*
       if (restoreTS != 0L){
          smsr = sm.getPriorStartRecord(restoreTS);
       }
       else{
          smsr = sm.getSnapshotStartRecord(tag);
       }
       */
       smsr = sm.getSnapshotStartRecord(tag);
       endTime = smsr.getCompletionTime();
       endWallTime = smsr.getCompletionWallTime();
       startTime = smsr.getKey();
       this.incremental = smsr.getIncrementalBackup();
       if (LOG.isTraceEnabled()) LOG.trace(" RecoveryRecord tag " + tag + " incremental " + getIncrementalBackup()
              + " operation " + getOpType());

       if (adjust){
          snapshotList = sm.getAdjustedSnapshotSet(smsr.getKey(), backupType);
       }
       else {
          snapshotList = sm.getPriorSnapshotSet(tag, backupType);
       }

       snapshotIncrementalList = sm.getPriorIncrementalSnapshotSet(tag);
       bkpType = backupType.toString();

       if (operation.equals("EXPORT") && !incremental) {
          SnapshotMetaStartRecord preTag = sm.getPrevTag(smsr.getUserTag());
          LOG.trace("preTag " + preTag);
          if (null != preTag) {
             regularExport = true;
             preTagStartKey = preTag.getKey();
          }
       }

       //query all brc snapshot between current regular and next regular backup
        Pair<byte[],byte[]> backupPair = sm.getRegularRangeByTagName(tag);
        List<SnapshotMetaRecord> brcSnapshotList = sm.getBrcSnapshotMetaRecord(backupPair.getFirst(),
                backupPair.getSecond());
        for (SnapshotMetaRecord brcRecord : brcSnapshotList) {
           List<SnapshotMetaRecord> groupByBrcSnapshotList = allBrcListMap.getOrDefault(brcRecord.getTableName(),
                   new ArrayList<SnapshotMetaRecord>());
           groupByBrcSnapshotList.add(brcRecord);
           allBrcListMap.put(brcRecord.getTableName(),groupByBrcSnapshotList);
        }
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for tag " + tag + " " , e);
       //sm.unlock(tag);
       throw e;
     }

     if (operation.equals("DROP")){
        // Before generating anything, let's go through each of the snapshots and make sure
        // it is eligible to be dropped.
        for (SnapshotMetaRecord mr :  snapshotList) {
           if (mr.getUserTag().equals(tag)){
              // The tag for the drop is the same as this snapshot tag,
              // so this snapshot is a target of the drop.  Let's make sure that
              // it doesn't have dependent incremental backups
              Set<Long> dependentSnapshots = mr.getDependentSnapshots();
              long firstSS = -1;
              String firstTag = "NONE";
              List<SnapshotMetaIncrementalRecord> irs = sm.getIncrementalSnapshotRecords(dependentSnapshots);
              boolean dependencyChange = false;
              if(!irs.isEmpty()){
                 Map<Long,SnapshotMetaIncrementalRecord> irsMap = new HashMap<>();
                 for (SnapshotMetaIncrementalRecord ir : irs) {
                    irsMap.put(ir.getKey(),ir);
                 }

                 for (Long ss : dependentSnapshots){
                    if (ss.longValue() != -1L){
                       // We found a dependent snapshot, so this snapshot can't be deleted.
                       // First, let's make sure the snapshot actually exists
                       SnapshotMetaIncrementalRecord ir = irsMap.get(ss.longValue());
                       if (ir == null){
                          // We have a dependency listed, but that record is not in the SNAPSHOT table.
                          // Let's log a warning event and allow the delete
                          if (LOG.isErrorEnabled()) LOG.error("RecoveryRecord drop tag: " + tag
                                  + " has a table " + mr.getTableName() + " with dependent backup for record " + ss.longValue()
                                  + ", but that record is not found; continuing ");
                          dependentSnapshots.remove(ss);
                          if(dependentSnapshots.isEmpty()){
                             dependentSnapshots.add(-1L);
                          }
                          dependencyChange = true;
                          continue;
                       }

                       if ((ss.longValue() < firstSS) || (firstSS == -1)){
                          firstSS = ss.longValue();
                          firstTag = ir.getUserTag();
                       }
                       if (LOG.isInfoEnabled()) LOG.info("RecoveryRecord drop tag " + tag + " record " + mr
                               + " dependent ss " + ir);
                    }
                 }
              }
              if (dependencyChange) {
                 mr.setDependentSnapshots(dependentSnapshots);
                 updateMetaRecords.add(mr);
              }

              if (firstSS != -1) {
                 // We found a dependent snapshot, so this snapshot can't be dropped.
                 Exception e = new Exception("deleteBackup tag: " + tag + " has a table "
                      + mr.getTableName() + " with dependent backup " + firstTag
                      + ".  Total dependencies: " + dependentSnapshots.size());
                    LOG.error("Exception in deleteBackup ", e);
                    //sm.unlock(tag);
                    throw e;
              }

              if (! force){
                 // If this is not a forced operation, we need to ensure we have a following
                 // regular snapshot, i.e. make sure this snapshot is not current.
                 // Without a more recent snapshot, we cannot associate mutations and a new
                 // restore operation for this table would become impossible.
                 // So we protect the user from this unless they specify a force option
                 SnapshotMetaRecord currRec = sm.getCurrentSnapshotRecord(mr.getTableName());
              }
           }
        }

        // Next check to see if 

     }

     synchronized(RecoveryLock){

     // This is recovery record for an operation to the given tag.  If a real snapshot was performed
     // for a table, then there are no mutations to apply.  However, if a table was configured for incremental
     // backup, then this list could contain a prior snapshot as a starting point and then we apply a subset
     // of all incremental snapshots and associated mutations up to the time of the tag.
     //
     // First let's deal with the list of real snapshots
     ListIterator<SnapshotMetaRecord> snapshotIter;
     for (snapshotIter = snapshotList.listIterator(); snapshotIter.hasNext();) {
        SnapshotMetaRecord rootRecord = snapshotIter.next();
        Map<Long, SnapshotMetaIncrementalRecord> incrementalMap = new HashMap<Long, SnapshotMetaIncrementalRecord>();
        if (rootRecord.getRecordType() != SnapshotMetaRecordType.SNAPSHOT_RECORD.getShort()) {
           // Somehow we retrieved an incorrect record type
           //sm.unlock(tag);
           if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord Error:  incorrect record type found in snapshotList "
                              + String.valueOf(rootRecord.getKey()) + " record type " + rootRecord.getRecordType());
           throw new Exception("RecoveryRecord Error:  incorrect record type found in snapshotList "
                              + String.valueOf(rootRecord.getKey()) + " record type " + rootRecord.getRecordType());
        }
        long rootSnapshotKey = rootRecord.getKey();
        String rootTable = rootRecord.getTableName();
        superSetIncrSnapshotList = new ArrayList<SnapshotMetaIncrementalRecord>() ;
        TableRecoveryGroup recGroup = recoveryTableMap.get(rootTable);
        if (recGroup != null){
           // We found an existing TableRecoveryGroup for the table associated with the current snapshot.
           // This can happen if there was a DDL operation and a partial snapshot of the table
           // after the last full snapshot was completed.  We can throw away the prior TableRecoveryGroup
           // and create a new one associated with this snapshot.
           if (LOG.isTraceEnabled()) LOG.trace(" Deleting existing TableRecoveryGroup for rootTable: "+ rootTable);
           recoveryTableMap.remove(rootTable);
        }
        try{
           // If this snapshot record has dependent snapshots, then we need to get the superset of
           // incremental snapshot records and determine which are included in the recovery
           Set<Long> dependentSS = rootRecord.getDependentSnapshots();
           if (dependentSS.contains(-1L) == false){
              if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord rootRecord for key " + rootSnapshotKey
                       + " has dependentSS " + dependentSS + " on table "+ rootTable);

              superSetIncrSnapshotList = sm.getIncrementalSnapshotSetForRoot(rootSnapshotKey);
              if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord getIncrementalSnapshotSetForRoot for " + rootSnapshotKey
                          + " returned " + superSetIncrSnapshotList.size() + " records in superSetIncrSnapshotList: "+ superSetIncrSnapshotList);
              // We need to patch up the relationship of all incremental snapshots.
              // We build a map and if it's a restore, we start by first marking
              // all incremental records associated with this snapshot as excluded
              ListIterator<SnapshotMetaIncrementalRecord> listIterator = superSetIncrSnapshotList.listIterator();
              SnapshotMetaIncrementalRecord targetRec = null;
              while (listIterator.hasNext()){
                 SnapshotMetaIncrementalRecord ssmir = listIterator.next();
                 if(ssmir != null){
                    if (operation.equals("RESTORE")){
                       ssmir.setExcluded(true);
                    }
                    incrementalMap.put(Long.valueOf(ssmir.getKey()), ssmir);
                    if (ssmir.getUserTag().equals(tag)){
                       // This record is the target incremental record.
                       // we are either dropping or recovering to this record.
                       targetRec = ssmir;
                    }
                 }
              }

              if (targetRec != null){

                 if (operation.equals("DROP")){
                    // Patch up a parent/child relationship between our parent and our child
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord establishing new parent/child relationships for root "
                            + rootRecord + " target " + targetRec);
                    long child = targetRec.getChildSnapshotId();
                    long parent = targetRec.getParentSnapshotId();
                    long root = targetRec.getRootSnapshotId();
                    SnapshotMetaIncrementalRecord parentRec = null;
                    SnapshotMetaIncrementalRecord childRec = null;
                    if (parent != root){
                       // We have a parent
                       parentRec = incrementalMap.get(Long.valueOf(parent));
                       parentRec.setChildSnapshotId(targetRec.getChildSnapshotId());
                    }
                    if (child != -1L) {
                       // We have a child
                       childRec = incrementalMap.get(Long.valueOf(child));
                       if (childRec != null){
                          childRec.setParentSnapshotId(targetRec.getParentSnapshotId());
                       }
                       else{
                          if (LOG.isWarnEnabled()) LOG.warn("RecoveryRecord tag " + tag
                                   + " operation: " + operation
                                   + " did not find child record " + child + "in reverse chain for "
                                   + targetRec);
                       }
                    }

                    // Remove the target from the dependent snapshot list
                    dependentSS.remove(targetRec.getKey());
                    if (dependentSS.isEmpty()){
                       // Need to reinitialize it
                       dependentSS.add(-1L);
                    }
                    rootRecord.setDependentSnapshots(dependentSS);
                    deleteKeys.add(targetRec.getKey());
                    incrementalMap.get(Long.valueOf(targetRec.getKey()));
                 }
                 else {
                    // NOT a DROP operation
                    // Now we need to start a reverse chain of incremental records starting from the
                    // incremental record associated with this recovery record and including all the parents
                    // We use this as a starting point to create the reverse chain in the super
                    // set of incremental records for this table that have the same root snapshot.
                    targetRec.setExcluded(false);
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord tag " + tag
                           + " operation: " + operation
                           + " found incremental record.  Starting reverse chain for "
                           + targetRec);
                    long current = targetRec.getKey();
                    long previous = targetRec.getParentSnapshotId();
                    SnapshotMetaIncrementalRecord previousRec = null;
                    boolean done = false;
                    while(! done){
                       try {
                          previousRec = incrementalMap.get(Long.valueOf(previous));
                          if (previousRec == null){
                             done = true;
                          }
                          else {
                             previousRec.setExcluded(false);
                             previousRec.setChildSnapshotId(current);
                             current = previousRec.getKey();
                             previous = previousRec.getParentSnapshotId();
                             if (LOG.isTraceEnabled()) LOG.trace("Found previous active record for tag "
                                  + tag + " record " + previousRec);
                             if(previous == rootRecord.getKey()){
                                done = true;
                             }
                          }
                       }
                       catch (NullPointerException npe){
                          if (LOG.isTraceEnabled()) LOG.trace("Caught NullPointerException chaining records; chainedRec "
                              + targetRec + " previous " + previous
                                + tag + " record " + previousRec + " incrementalMap " + incrementalMap);
                          //sm.unlock(tag);
                          throw npe;
                       }
                    }
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord built chained list");

                    rootRecord.setRestoredSnapshot(targetRec.getKey());
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord updating root to include restoredRecord " + rootRecord);
                 }

                 // Now if we are not generating this RecoveryRecord just for show, we should
                 // write the newly altered snapshot records back into the SNAPSHOT table
                 if (! showOnly) {
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord updating snapshot meta for root record for recovery tag " + tag
                         + " operation " + getOpType() + " record " + rootRecord);
                    if (operation.equals("RESTORE") && (rootRecord.getHiatusTime() != -1L)){
                       if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord found snapshot meta with a hiatus record " + rootRecord.getHiatusTime()
                              + " greater than endTime " + endTime);
                       rootRecord.setHiatusTime(-1L);
                    }
                    updateMetaRecords.add(rootRecord);

                    for(Map.Entry<Long, SnapshotMetaIncrementalRecord> entry : incrementalMap.entrySet()){
                       SnapshotMetaIncrementalRecord rec = entry.getValue();
                       updateIncrementRecords.add(rec);
                       if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord updated SnapshotMetaIncrementalRecord " + rec);
                    }
                 }
              } // if (targetRec != null){
              else {
                 // The target should be the root
                 if (! rootRecord.getUserTag().equals(tag)){
                    //check if there are following BRC record, created by DDL
                    SnapshotMetaStartRecord smsr = sm.getFollowingBRCRecord( restoreTS );
                   if(smsr != null )
                   {
                     long stopKey = smsr.getKey();
                     ListIterator<SnapshotMetaIncrementalRecord> superSSListIterator = superSetIncrSnapshotList.listIterator();
                     while (superSSListIterator.hasNext()){
                        SnapshotMetaIncrementalRecord ssmir = superSSListIterator.next();
                        if(ssmir.getKey() <= stopKey) 
                          ssmir.setExcluded(false);
                     }
                    }
                    else {
                      // Something's wrong, can't find the target
                      IOException ioe = new IOException("Can't find target for user tag " + tag);
                      LOG.error("Error finding target for operation "
                            + opType + " for root " + rootRecord + " ", ioe);
                      throw ioe;
                    }
                 }
                 else {
                    if(opType.equals("RESTORE")){
                       listIterator = superSetIncrSnapshotList.listIterator();
                       while (listIterator.hasNext()){
                          SnapshotMetaIncrementalRecord ssmir = listIterator.next();
                          updateIncrementRecords.add(ssmir);
                          if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord updated excluded incremental record " + ssmir);
                       }
                    }
                 }
              }
           } // if (dependentSS.contains(-1L) == false){
           else {
              if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord rootRecord has no dependent snapshots " + tag
                       + " operation " + getOpType() + " record " + rootRecord);
           }

           // boolean skipPending = operation.equals("EXPORT");
           // skipPending falseThis set of mutations excludes any 'PENDING' mutations not already assigned to an
           // skipPending true This set of mutations is the super set of all mutations pointing to the root snapshot.
           long startTime = rootRecord.getMutationStartTime();
            if (preTagStartKey > 0) {
                startTime = preTagStartKey;
            }
           mutationList = mm.getMutationsFromSnapshot(rootSnapshotKey, startTime, rootTable, false /* skipPending */,
                   getIncrementalBackup(), endTime, regularExport);
           // If this operation is a DROP, then we only return mutations specific to:
           //  1) the root snapshot if the root is being dropped
           //  2) the range of mutations associated with an incremental backup that
           //     is now EXCLUDED and has no child, i.e. it is the last in the branch
           //
           // If the drop is for an incremental backup that is active, then we re-associate the
           // mutations to the next incremental backup (child) if there is one.  Or we
           // mark them as "PENDING" if there isn't a child incremental snapshot.
           if (operation.equals("DROP")){
              if (rootRecord.getUserTag().equals(tag)){
                 // This is a drop of the root record.
                 // We should not filter and return all mutations
                 if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord DROP root record for tag " + tag + " root " + rootSnapshotKey
                          + " mutations " + mutationList + ", and table " + rootTable);
              }
              else{
                 if (superSetIncrSnapshotList != null){
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord DROP for tag " + tag + " getIncrementalSnapshotSetForRoot found "
                        + superSetIncrSnapshotList.size() + " snapshots for root " + rootSnapshotKey
                        + " " + mutationList.size() + " mutations, and table " + rootTable);

                    // Look through the incremental snapshot list for for the snapshot associated with this DROP
                    SnapshotMetaIncrementalRecord targetIR = null;
                    ListIterator<SnapshotMetaIncrementalRecord> superSSListIterator = superSetIncrSnapshotList.listIterator();
                    while (superSSListIterator.hasNext()){
                       SnapshotMetaIncrementalRecord ssmir = superSSListIterator.next();
                       if ((ssmir != null) && (ssmir.getUserTag().equals(tag))) {
                          targetIR = ssmir;
                          break;
                       }
                    }
                    if (targetIR == null) {
                       //sm.unlock(tag);
                       //fix-m18749 if root incr record exist in snapshotList,but not exist in superSetIncrSnapshotList
                       //targetIR is null does not affect the delete action
                       LOG.warn("Unable to locate target of drop for table " + rootTable
                            + " and tag " + tag);
                       continue;
                    }

                    SnapshotMetaIncrementalRecord childIR = null;
                    if (targetIR.getChildSnapshotId() != -1L) {
                       childIR = incrementalMap.get(targetIR.getChildSnapshotId());
                    }

                    // We found the target and child incremental records.
                    // Now iterate through the mutations looking for ones for the target
                    ListIterator<MutationMetaRecord> mutationListIterator = mutationList.listIterator();
                    ArrayList<MutationMetaRecord> deleteMutationList = new ArrayList<MutationMetaRecord>();
                    ListIterator<MutationMetaRecord> deleteMutationIterator = deleteMutationList.listIterator();
                    while (mutationListIterator.hasNext()){
                       MutationMetaRecord mmRec = mutationListIterator.next();

                       // It's possible the mutation records have changed underneath us, so
                       // we must check the shadow table for the latest version of the record
                       MutationMetaRecord mmRecShadow = mm.getMutationRecord(mmRec.getRowKey(), true /* readShadow */);
                       if (mmRecShadow != null){
                          if (mmRec.getSmallestCommitId() != mmRecShadow.getSmallestCommitId()){
                             mmRecShadow.setSmallestCommitId(Math.max(mmRec.getSmallestCommitId(), mmRecShadow.getSmallestCommitId()));
                          }
                          if (mmRec.getLargestCommitId() != mmRecShadow.getLargestCommitId()){
                             mmRecShadow.setLargestCommitId(Math.max(mmRec.getLargestCommitId(), mmRecShadow.getLargestCommitId()));
                          }
                          mmRec = mmRecShadow;
                          mm.putMutationRecord(mmRec, true /* blind write */);
                          if (LOG.isDebugEnabled())LOG.debug("RecoveryRecord DROP mmRec updated from shadow table " + mmRec);
                       }

                       if (targetIR.getUserTag().equals(mmRec.getUserTag())){
                          if(targetIR.getExcluded() && (targetIR.getChildSnapshotId() == -1L)){
                             // Need to return just these mutations
                             deleteMutationIterator.add(mmRec);
                          }
                          else {
                             // Need to reassign these mutations
                             mmRec.setPriorIncrementalSnapshot(targetIR.getParentSnapshotId());
                             if (childIR == null){
                                // These mutations are all pending and will be included
                                // in the next incremental backup for this table.
                                mmRec.setBackupSnapshot(-1L);
                                mmRec.setUserTag("PENDING");
                             }
                             else {
                                // The new target of these mutations is the child of the
                                // one we are deleting.
                                mmRec.setBackupSnapshot(childIR.getKey());
                                mmRec.setUserTag(childIR.getUserTag());
                             }
                             if (! showOnly){
                                // Make the changes persistent
                                mm.putMutationRecord(mmRec, true /* blind write */);
                             }
                          }
                       } // if (targetIR.getUserTag().equals(mmRec.getUserTag()))
                       // Always remove mutations from the mutationList for a DROP operation.
                       mutationListIterator.remove();
                    }
                    // Now we've iterated the superset of mutations and removed them from
                    // mutationList.  If the target is excluded we will return the deletedMutationList
                    mutationList = deleteMutationList;
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord mutation size " + mutationList.size());
                 }
              } // if (superSetIncrSnapshotList != null)
           } // if (operation.equals("DROP"))
           else{
              // This is not for a DROP.
              // If there are any mutations associated with incremental snapshots that have been
              // excluded from this RecoveryRecord, then we must remove them.
              if (superSetIncrSnapshotList != null){
                 if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord tag " + tag
                      + " getIncrementalSnapshotSetForRoot found "
                      + superSetIncrSnapshotList.size() + " snapshots for root " + rootSnapshotKey
                      + " " + mutationList.size() + " mutations, and table " + rootTable);

                 // Look through the incremental snapshot list for any excluded snapshots and remove the mutations
                 ListIterator<SnapshotMetaIncrementalRecord> superSSListIterator = superSetIncrSnapshotList.listIterator();
                 while (superSSListIterator.hasNext()){
                    SnapshotMetaIncrementalRecord ssmir = superSSListIterator.next();
                    if((ssmir != null) && (ssmir.getExcluded())){

                       // We found an excluded snapshot.  We trim these mutations from the returned
                       // list so they won't get applied, but we do not actually delete the mutations
                       // because a user might later restore to the excluded incremental snapshot and
                       // we will need them.
                       if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord operation " + operation + " tag " + tag
                           + " found excluded incremental snapshot; removing associated mutations for userTag "
                           + ssmir.getUserTag() + " record: " + ssmir);

                       ListIterator<MutationMetaRecord> mutationListIterator = mutationList.listIterator();
                       while (mutationListIterator.hasNext()){
                          MutationMetaRecord mmRec = mutationListIterator.next();

                          if (ssmir.getUserTag().equals(mmRec.getUserTag())){
                             // This mutation is from an excluded incremental snapshot
                             if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord removing excluded mutation " + mmRec);
                              //M-24506,if this is a regular backup and commitId less than end time, do not remove
                              if (!incremental && mmRec.getSmallestCommitId() > 0 && mmRec.getSmallestCommitId() < endTime) {
                                  continue;
                              }
                             mutationListIterator.remove();
                          }
                          else {
                             if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord keeping mutation " + mmRec);
                          }
                          if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord mutation size " + mutationList.size());

                       } // while (mListIterator.hasNext()){
                    } //if(ssmir.getExcluded()){
                    else{
                       if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord incremental snapshot included for "
                             + ssmir);
                    }
                    if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord mutations remaining " + mutationList.size());

                 } // while (superSSListIterator.hasNext()){
              } // if (superSetIncrSnapshotList != null){
           } // This is not for a DROP.

/*           // If this is a restore operation, we delete any still 'PENDING' mutations that
           // are associated with this table because they will never be reachable again.
           if (opType.equals("RESTORE")){
              if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord "
                    + " operation " + getOpType()
                    + " Now looking for orphans for table " + rootTable);

              List<MutationMetaRecord>pMutationList = mm.getMutationsFromUserTag(rootTable, "PENDING");
              ListIterator<MutationMetaRecord> pendingListIterator = pMutationList.listIterator();
              ListIterator<MutationMetaRecord> orphanedListIterator = getOrphanedMutations().listIterator();
              ListIterator<MutationMetaRecord> mListIterator = mutationList.listIterator();
              while (pendingListIterator.hasNext()){
                 MutationMetaRecord pmRec = pendingListIterator.next();
                 // This is an orphaned mutation
                 if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord adding mutation to orphanedMutations " + pmRec);
                 orphanedListIterator.add(pmRec);
                 while (mListIterator.hasNext()){
                    MutationMetaRecord mmRec = mListIterator.next();
                    if (mmRec.getKey() == pmRec.getKey()){
                       if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord found orphaned mutation in mutationList "
                                    + pmRec + " " + mutationList.size() + " mutations remain");
                       mListIterator.remove();
                    }
                 }
              } // while (pendingListIterator.hasNext()){

              // Clear out the orphaned mutations if we are not only for show objects
              if ((getOrphanedMutations().size() > 0) && (! showOnly)) {
                 if (LOG.isTraceEnabled()) LOG.trace("RecoveryRecord found "
                    + getOrphanedMutations().size() + " orphaned mutations that need deleting");
                 orphanedListIterator = getOrphanedMutations().listIterator();

                 FileSystem fs = FileSystem.get(config);
                 while (orphanedListIterator.hasNext()){
                    MutationMetaRecord mutationRecord = orphanedListIterator.next();
                    String mutationPathString = mutationRecord.getMutationPath();
                    Path mutationPath = new Path (mutationPathString);

                    // Delete mutation file
                    if (LOG.isDebugEnabled()) LOG.debug("RecoveryRecord deleting orphaned mutation file at " + mutationPath);
                    fs.delete(mutationPath, false);

                    // Delete mutation record
                    if (LOG.isDebugEnabled()) LOG.debug("restoreObjects deleting orphaned mutationMetaRecord " + mutationRecord);
                    MutationMeta.deleteMutationRecord(mutationRecord.getKey());
                 }
              }
           } // if (opType.equals("RESTORE"))
*/
        }
        catch (Exception e){
          //sm.unlock(tag);
          if (LOG.isTraceEnabled()) LOG.trace("Exception getting the range of mutations for snapshot " + rootSnapshotKey + " " , e);
          throw e;
        }
        int numSnapshots = (superSetIncrSnapshotList == null ? 0 : superSetIncrSnapshotList.size());
//        if (LOG.isTraceEnabled()) LOG.trace("Creating new TableRecoveryGroup for rootRecord: " + rootRecord
//                       + " with " + numSnapshots + " incremental snapshots and "
//                       + mutationList.size() + " mutations");

        Set<Long> lobIds = Collections.synchronizedSet(new HashSet<Long>());
        if (numSnapshots == 0){
           // There are no incremental snapshot records, so get the lobs from the root
           lobIds = rootRecord.getLobSnapshots();
        }
        else {
           // Get the last record
           SnapshotMetaIncrementalRecord ir = superSetIncrSnapshotList.get(superSetIncrSnapshotList.size() - 1);
           lobIds = ir.getLobSnapshots();
        }
        ArrayList<LobMetaRecord> lobRecs = new ArrayList<LobMetaRecord>();
        for (Long id : lobIds){
           LobMetaRecord lr = lm.getLobMetaRecord(id.longValue());
           lobRecs.add(lr);
        }
        //set brcSnapshotLsit
        rootRecord.setBrcSnapshotList(allBrcListMap.getOrDefault(rootTable, new ArrayList<SnapshotMetaRecord>()));
        recGroup = new TableRecoveryGroup(rootRecord, superSetIncrSnapshotList, lobRecs, mutationList);
        if (LOG.isTraceEnabled()) LOG.trace("Adding new entry into RecoveryTableMap from snapshot for rootTable: "+ rootTable);
        recoveryTableMap.put(rootTable, recGroup);
     } // for

        //batch put increments
        if(updateIncrementRecords.size() > 0){
           sm.batchPutIncrementalRecords(updateIncrementRecords);
        }

        //batch put snapshot meta
        if(updateMetaRecords.size() > 0){
           sm.batchPutSnapshotRecords(updateMetaRecords);
        }

        //batch delete
        if(deleteKeys.size() > 0){
           SnapshotMeta.deleteRecordsKeys(deleteKeys);
        }

     } // synchronized(RecoveryLock){
/*
     try {
        sm.unlock(tag);
     }
     catch(Exception e){
        LOG.error("Exception unlocking SnapshotMetat " , e);
        throw e;
     }
*/
     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor tag " + tag + " " + this.toString());
     return;
   }


   /**
    * RecoveryRecord
    * @param long timeId
    * @throws Exception
    * 
    * The timeId provided is the time a user has selected for a point-in-time recovery operation
    */
   public RecoveryRecord (final long timeId) throws Exception {
     if (LOG.isTraceEnabled()) LOG.trace("Enter RecoveryRecord constructor for time: " + timeId);

     Configuration config = HBaseConfiguration.create();
     SnapshotMeta sm;
     List<SnapshotMetaRecord> snapshotList = null;
     LobMeta lm;
     MutationMeta mm;
     ArrayList<MutationMetaRecord> mutationList = null;
     endTime = timeId;
     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta ", e);
       throw e;
     }

     try {
        if (LOG.isTraceEnabled()) LOG.trace("  Creating LobMeta object ");
        lm = new LobMeta(config);
     }
     catch (Exception e) {
        if (LOG.isTraceEnabled()) LOG.trace("  Exception creating LobMeta ", e);
        throw e;
     }

     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta ", e);
       throw e;
     }

     try{
       //  Get a snapshot for every table in the database.  These may not all be associated
       //  with a full snapshot.  A partial snapshot completed after the last full snapshot
       //  but prior to the timeId will also be returned in the list.
       snapshotList = sm.getPriorSnapshotSet(timeId);
       bkpType = new String("TIMEBASED");
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for time: "+ timeId + " ", e);
       throw e;
     }

     startTime = 0;
     try{
        // This is the start time of the full snapshot record
        startTime = sm.getPriorStartRecord(timeId).getKey();
     }
     catch(Exception e){
        if (LOG.isTraceEnabled()) LOG.trace("Exception getting the prior start record for time: "+ timeId + " ", e);
        throw e; 
     }

     ListIterator<SnapshotMetaRecord> snapshotIter;
     for (snapshotIter = snapshotList.listIterator(); snapshotIter.hasNext();) {
        SnapshotMetaRecord tmpRecord = snapshotIter.next();
        if (tmpRecord.getRecordType() == SnapshotMetaRecordType.SNAPSHOT_START_RECORD.getShort()) {
           // Somehow we retrieved a SnapshotMetaStartRecord
           if (LOG.isTraceEnabled()) LOG.trace("Error:  SnapshotMetaStartRecord found in snapshotList "
                              + String.valueOf(tmpRecord.getKey()));
           throw new Exception("Error:  SnapshotMetaStartRecord found in snapshotList "
                              + String.valueOf(tmpRecord.getKey()));
        }
        long tmpSnapshotKey = tmpRecord.getKey();
        String tmpTable = tmpRecord.getTableName();
        TableRecoveryGroup recGroup = recoveryTableMap.get(tmpTable);
        ArrayList<SnapshotMetaIncrementalRecord> superSetIncrSnapshotList =
                sm.getIncrementalSnapshotSetForRoot(tmpRecord.getKey());
        if (recGroup != null){
           // We found an existing TableRecoveryGroup for the table associated with the current snapshot.
           // This can happen if there was a DDL operation and a partial snapshot of the table
           // after the last full snapshot was completed.  We can throw away the prior TableRecoveryGroup
           // and create a new one associated with this snapshot.
           if (LOG.isTraceEnabled()) LOG.trace(" Deleting existing TableRecoveryGroup for tmpRecord: " + tmpRecord);
           recoveryTableMap.remove(tmpTable);
        }
        try{
            mutationList = mm.getMutationsFromSnapshot(tmpSnapshotKey, tmpRecord.getMutationStartTime(), tmpTable,
                    false /* skipPending */, getIncrementalBackup(), getEndWallTime(), false);
        }
        catch (Exception e){
          if (LOG.isTraceEnabled()) LOG.trace("Exception getting the range of mutations for snapshot " + tmpSnapshotKey + " ", e);
          throw e;
        }
        if (LOG.isTraceEnabled()) LOG.trace("Creating new TableRecoveryGroup for tmpRecord: " + tmpRecord
                + " with " + superSetIncrSnapshotList.size() + " incremental snapshots and "
                + mutationList.size() + " mutations");


        Set<Long> lobIds = Collections.synchronizedSet(new HashSet<Long>());
        int numSnapshots = (superSetIncrSnapshotList == null ? 0 : superSetIncrSnapshotList.size());
        if (numSnapshots == 0){
           // There are no incremental snapshot records, so get the lobs from the root
           lobIds = tmpRecord.getLobSnapshots();
        }
        else {
           // Get the last record
           SnapshotMetaIncrementalRecord ir = superSetIncrSnapshotList.get(superSetIncrSnapshotList.size() - 1);
           lobIds = ir.getLobSnapshots();
        }
        ArrayList<LobMetaRecord> lobRecs = new ArrayList<LobMetaRecord>();
        for (Long id : lobIds){
           LobMetaRecord lr = lm.getLobMetaRecord(id.longValue());
           lobRecs.add(lr);
        }

        recGroup = new TableRecoveryGroup(tmpRecord, superSetIncrSnapshotList, lobRecs, mutationList);
        if (LOG.isTraceEnabled()) LOG.trace("Adding new entry into RecoveryTableMap from snapshot for tmpTable: "+ tmpTable);
        recoveryTableMap.put(tmpTable, recGroup);
     }
     if (LOG.isTraceEnabled()) LOG.trace("TableRecoveryMap has : "+ recoveryTableMap.size() + " entries");

     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor for time " + timeId
              + "; snapshot start time " + startTime + " " + this.toString());
     return;
   }

   /**
    * RecoveryRecord
    * @param long timeId
    * @throws Exception
    *
    * The timeId provided is the time a user has selected for a point-in-time recovery operation
    */
   public RecoveryRecord (final long timeId, final boolean xdc_replay, final ArrayList<String> tableList) throws Exception {
     if (LOG.isTraceEnabled()) LOG.trace("Enter RecoveryRecord constructor for time: "
          + timeId + " xdc_replay: " + xdc_replay);

     Configuration config = HBaseConfiguration.create();
     SnapshotMeta sm;
     List<SnapshotMetaRecord> snapshotList = null;
     LobMeta lm;
     MutationMeta mm;
     ArrayList<MutationMetaRecord> mutationList = null;
     endTime = timeId;
     opType = "XDC";
     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta ", e);
       throw e;
     }

     try {
        if (LOG.isTraceEnabled()) LOG.trace("  Creating LobMeta object ");
        lm = new LobMeta(config);
     }
     catch (Exception e) {
        if (LOG.isTraceEnabled()) LOG.trace("  Exception creating LobMeta ", e);
        throw e;
     }

     try {
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta ", e);
       throw e;
     }

     startTime = 0;
     long systemStartTime = 0;
     if (! xdc_replay){
       if (LOG.isErrorEnabled()) LOG.error("RecoveryRecord constructor for xdc replay called incorrectly; xdc_replay is false ");
       throw new IOException("RecoveryRecord constructor for xdc replay called incorrectly; xdc_replay is false ");
     }

     try{
        // This is the start time of the xDC peer down operation
        smsr = sm.getPeerDownStartRecord(timeId);
        startTime = smsr.getKey();
        systemStartTime = smsr.getStartTime();
        if (LOG.isInfoEnabled()) LOG.info("RecoveryRecord constructor for xdc replay, startTime "
                + startTime + " endTime: " + timeId + " systemStartTime: " + systemStartTime);

     }
     catch(Exception e){
        if (LOG.isTraceEnabled()) LOG.trace("Exception getting the prior start record for time: "+ timeId + " ", e);
        throw e;
     }

     this.incremental = smsr.getIncrementalBackup();
     this.tag = smsr.getUserTag();
     bkpType = new String("TIMEBASED");
     try{
       // This is an xDC replay.  We don't care about snapshots, so
       // we will create a dummy snapshot record for the TableRecoveryGroup
       snapshotList = sm.createSnapshotRecords(startTime, tableList, tag);
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for time: "+ timeId + " ", e);
       throw e;
     }

     // This is an xDC operation
     // First populate a list of TRGs
     ListIterator<SnapshotMetaRecord> snapshotIter;
     for (snapshotIter = snapshotList.listIterator(); snapshotIter.hasNext();) {
        SnapshotMetaRecord tmpRecord = snapshotIter.next();
        String tmpTable = tmpRecord.getTableName();
        ArrayList<LobMetaRecord> lobRecs = new ArrayList<LobMetaRecord>();
        ArrayList<SnapshotMetaIncrementalRecord> superSetIncrSnapshotList =
              new ArrayList<SnapshotMetaIncrementalRecord>();
        mutationList = new ArrayList<MutationMetaRecord>();
        if (LOG.isTraceEnabled()) LOG.trace("Creating new TableRecoveryGroup for tmpRecord: " + tmpRecord
               + " with " + superSetIncrSnapshotList.size() + " incremental snapshots "
               + lobRecs.size() + " lobs, and " + mutationList.size() + " mutations");


        int numSnapshots = (superSetIncrSnapshotList == null ? 0 : superSetIncrSnapshotList.size());
        TableRecoveryGroup recGroup = new TableRecoveryGroup(tmpRecord, superSetIncrSnapshotList, lobRecs, mutationList);
        if (LOG.isTraceEnabled()) LOG.trace("Adding new entry into RecoveryTableMap from snapshot for tmpTable: "+ tmpTable);
        recoveryTableMap.put(tmpTable, recGroup);
     }

     // Now we have the TRG, so we need to add the mutations
     mutationList = mm.getMutationsFromRange(systemStartTime, startTime, endTime);
     ListIterator<MutationMetaRecord> mutationIter;
     for (mutationIter = mutationList.listIterator(); mutationIter.hasNext();) {
        MutationMetaRecord tmpRecord = mutationIter.next();
        String tmpTable = tmpRecord.getTableName();
        TableRecoveryGroup recGroup = recoveryTableMap.get(tmpTable);

        // If the TableRecoveryGroup is null, that means the table was not in the list of user tables
        // we created snapshot records for and so a TableRecoveryGroup was not created.
        // Likely we included this mutation because the smallestCommitId was 0 due to a problem in
        // merging shadow records, but since the table doesn't exist, we can skip this mutation
        if (recGroup == null){
           if (LOG.isErrorEnabled()) LOG.error("Mutation record found for table: " + tmpTable + ", but the table is dropped; ignoring.");
           continue;
        }

        // Add the mutation file to the list
        if (LOG.isTraceEnabled()) LOG.trace("Adding mutation into tableRecoveryGroup for tmpTable: "+ tmpTable);
        recGroup.getMutationList().add(tmpRecord);
        if (LOG.isTraceEnabled()) LOG.trace("TRG for table: " + tmpTable + " now has " + recGroup.getMutationList().size() + " mutations ");
     }

     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor for time " + timeId
              + "; snapshot start time " + startTime + " " + this.toString());
     return;
   }

   /**
    * importMeta
    * @throws Exception
    *
    * This method takes a RecoveryRecord and re-establishes the
    * SnapshotMeta and MutationMeta records in the database
    * so that a restore command can be issued.  Usually this is needed
    * after an export snapshot and a drop snapshot have been performed.
    */
   public void importMeta(Configuration config) throws Exception {

     if (LOG.isTraceEnabled()) LOG.trace("importMeta start ");

     SnapshotMeta sm;
     LobMeta lm;
     MutationMeta mm;

     try {
       if (LOG.isTraceEnabled()) LOG.trace("importMeta ceating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("importMeta exception creating SnapshotMeta ", e);
       throw e;
     }

     try {
        if (LOG.isTraceEnabled()) LOG.trace("  Creating LobMeta object ");
        lm = new LobMeta(config);
     }
     catch (Exception e) {
        if (LOG.isTraceEnabled()) LOG.trace("  Exception creating LobMeta ", e);
        throw e;
     }

     try {
       if (LOG.isTraceEnabled()) LOG.trace("importMeta creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("importMeta exception creating MutationMeta ", e);
       throw e;
     }

     // Import requires a tag and an associated SnapshotMetaStartRecord, so
     // if the record in this RecoveryRecord doesn't have a start record
     // we throw an exception.
     if (this.getSnapshotStartRecord() == null){
        IOException ioe = new IOException("Snapshot start record is null");
        LOG.error("Exception in importMeta ", ioe);
        throw ioe;
     }
     // First look at the incremental snapshot records included in this
     // RecoveryRecord to make sure the parent snapshot records are present
     // in the SNAPSHOT table on this cluster.  If not, throw an exception.
     // While we loop through the SnapshotMetaIncrementalRecord we
     // build a map from table names to make lookup easier.
     Map<String, SnapshotMetaIncrementalRecord> incrementalMap =  new HashMap<String, SnapshotMetaIncrementalRecord>();
     Map<String, TableRecoveryGroup> tableMap =  this.getRecoveryTableMap();
//     List<SnapshotMetaIncrementalRecord> smil = this.getSnapshotIncrementalList();
//     for (SnapshotMetaIncrementalRecord smir : smil){
        // These are incremental snapshot records.  Make sure the root exists
//        long rootId = smir.getRootSnapshotId();
     for (Map.Entry<String, TableRecoveryGroup> tableEntry : tableMap.entrySet()){
        String tableName = tableEntry.getKey();
        TableRecoveryGroup tableGroup = tableEntry.getValue();
        long rootId = tableGroup.getSnapshotRecord().getKey();
        if (LOG.isTraceEnabled()) LOG.trace("importMeta verifying root record exists for " + tableName);
        SnapshotMetaRecord smrec = sm.getSnapshotRecord(rootId);
        if (smrec == null) {
           if (LOG.isTraceEnabled()) LOG.trace("Root snapshot " + rootId + " not found in existing SNAPSHOT table");
           smrec = tableGroup.getSnapshotRecord();

           // This record should be the missing parent
           if (smrec.getKey() != rootId){
              LOG.error("Root snapshot " + rootId + " not found smrec " + smrec);
           }
           else {
              if (LOG.isTraceEnabled()) LOG.trace("Root snapshot " + rootId + " found in smrec; adding to SNAPSHOT table");
              sm.putRecord(smrec);
           }
        }

        // We need to make sure we update existing parent/child relationships based
        // on the imported SnapshotMetaIncrementalRecord and ensure from our child we
        // chain back to the root, so we will patch up our child and parent records
        ArrayList<SnapshotMetaIncrementalRecord> snapshotIncrementalList = tableGroup.getSnapshotIncrementalList();
        for (SnapshotMetaIncrementalRecord smir : snapshotIncrementalList){
           if (LOG.isTraceEnabled()) LOG.trace("importMeta working on record " + smir);
           if (smir.getChildSnapshotId() != -1L){
              //  We have a child.  Make ourselves the child's parent
              SnapshotMetaIncrementalRecord child = sm.getIncrementalSnapshotRecord(smir.getChildSnapshotId());
              child.setParentSnapshotId(smir.getKey());
              sm.putRecord(child);
              sm.putRecord(smir);

              if (smir.getParentSnapshotId() != smir.getRootSnapshotId()){
                 // If our parent isn't the root, then it's also an incremental snapshot record
                 // and we need to make we are its child
                 SnapshotMetaIncrementalRecord parent = sm.getIncrementalSnapshotRecord(smir.getParentSnapshotId());
                 parent.setChildSnapshotId(smir.getKey());
                 sm.putRecord(parent);
                 if (LOG.isTraceEnabled()) LOG.trace("importMeta updated our parent incremental record " + parent);
              }
           }
           else {
              LOG.error("importMeta might have orphaned mutations for record " + smir);
              // TODO We don't have a child, so we need to cleanup any orphaned mutations
           }

           if (LOG.isTraceEnabled()) LOG.trace("importMeta adding incremental snapshot record " + smir);
           sm.putRecord(smir);
           incrementalMap.put(smir.getTableName(), smir);
        }
     }

     // Start disassembling the recoveryTableMap.  Some of these records will have
     // been deleted from the SNAPSHOT table, so the records will not be found.
     // Others may be there, but have slightly different data that needs to be merged.
     for (Map.Entry<String, TableRecoveryGroup> tableEntry :  tableMap.entrySet()) {
        String tableName = tableEntry.getKey();
        TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();
        SnapshotMetaRecord smRecToImport = tableRecoveryGroup.getSnapshotRecord();
        SnapshotMetaRecord smRecInTable = sm.getSnapshotRecord(smRecToImport.getKey());
        Set<Long> depSnapshotsInTable = null;
        if (smRecInTable ==  null){
           if (LOG.isTraceEnabled()) LOG.trace("importMeta inserting new record for for " + smRecToImport);
           sm.putRecord(smRecToImport);
           depSnapshotsInTable = smRecToImport.getDependentSnapshots();
           smRecInTable = sm.getSnapshotRecord(smRecToImport.getKey());
        }
        else {
           depSnapshotsInTable = smRecInTable.getDependentSnapshots();
        }

        // Not all snapshot records in this list will have dependent snapshots to validate
        if (LOG.isTraceEnabled()) LOG.trace("importMeta dependent snapshots " + depSnapshotsInTable);
        if (depSnapshotsInTable.size() != 1 && (depSnapshotsInTable.contains(-1L) == false)){
           SnapshotMetaIncrementalRecord smir = incrementalMap.get(smRecToImport.getTableName());
           if (depSnapshotsInTable.contains(smir.getKey()) == false){
              // Need to add the incremental record dependency in the
              // parent SnapshotMetaRecord and write it to the table.
              // Then we need to add the incremental snapshot
              //  record to the table too.
              depSnapshotsInTable.add(smir.getKey());
              smRecInTable.setDependentSnapshots(depSnapshotsInTable);
              if (LOG.isTraceEnabled()) LOG.trace("importMeta updated dependent snapshots for " + smRecInTable);
              sm.putRecord(smRecInTable);
           }
           // Always need to add the incremental record
           if (LOG.isTraceEnabled()) LOG.trace("importMeta importing incremental snapshot " + smir);
           sm.putRecord(smir);
        }

        // Now work on the mutations.
        List<MutationMetaRecord> mutationsToImport = tableRecoveryGroup.getMutationList();
        for (MutationMetaRecord mmRec : mutationsToImport ){
           // check to see if the record is in the table
           MutationMetaRecord existMM = mm.getMutationRecord(mmRec.getRowKey());
           if (existMM == null){
              // Record not found.  Need to add it.
              if (LOG.isTraceEnabled()) LOG.trace("importMeta importing mutation record " + mmRec);
              mm.putMutationRecord(mmRec, /* blindWrite */ true);
           }
           else{
              if (LOG.isInfoEnabled()) LOG.info("importMeta importing mutation record\n" + mmRec
                   + " found existing record\n" + existMM);
           }
        }
     } // for TableRecoveryGroup

     // Finally we need to import the SnapshotMetaStartRecord.
     SnapshotMetaStartRecord smsr = this.getSnapshotStartRecord();
     if (sm.getSnapshotStartRecord(smsr.getKey()) == null ){
        if (LOG.isTraceEnabled()) LOG.trace("importMeta importing SnapshotStartRecord " + smsr);
        sm.putRecord(smsr);
     }

     if (LOG.isTraceEnabled()) LOG.trace("Exit importMeta ");
     return;
   }

   /**
    * removeOrphanedMutations
    * @return void
    *
    * This method is called during a restore operation and looks
    * for any mutations that have a 'PENDING" tag, indicating
    * they are not yet part of an incremental backup.
    *
    * If this restore is part of a subset restore of an
    * incremental backup, then it is important that the
    * removeTable method be called first to remove excluded
    * tables from the recovery record.  Otherwise, mutations
    * will be deleted for active tables and a subsequent
    * incremental backup/restore operation will lose data
    */
   public void removeOrphanedMutations (Configuration config) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("removeOrphanedMutations start for tag " + tag);
      try{
         MutationMeta mm = new MutationMeta(config);
         FileSystem fs = FileSystem.get(config);

         // We delete any still 'PENDING' mutations that are associated with
         // each table because they will never be reachable again.
         Map<String, TableRecoveryGroup> tableMap =  this.getRecoveryTableMap();
         for (Map.Entry<String, TableRecoveryGroup> tableEntry : tableMap.entrySet()){
            String tableName = tableEntry.getKey();
            TableRecoveryGroup tableGroup = tableEntry.getValue();

            if (LOG.isTraceEnabled()) LOG.trace("removeOrphanedMutations "
                  + " operation " + getOpType()
                  + " Now looking for orphans for table " + tableName);
            List<MutationMetaRecord>pMutationList = mm.getMutationsFromUserTag(tableName, "PENDING");
            ListIterator<MutationMetaRecord> pendingListIterator = pMutationList.listIterator();
            ListIterator<MutationMetaRecord> orphanedListIterator = getOrphanedMutations().listIterator();
            List<MutationMetaRecord> mutationList = tableGroup.getMutationList();
            ListIterator<MutationMetaRecord> mListIterator = mutationList.listIterator();
            while (pendingListIterator.hasNext()){
               MutationMetaRecord pmRec = pendingListIterator.next();
               // This is an orphaned mutation
               if (LOG.isTraceEnabled()) LOG.trace("removeOrphanedMutations  found orphaned mutation " + pmRec);
               orphanedListIterator.add(pmRec);
               while (mListIterator.hasNext()){
                  MutationMetaRecord mmRec = mListIterator.next();
                  if (mmRec.getRowKey().equalsIgnoreCase(pmRec.getRowKey())){
                     if (LOG.isTraceEnabled()) LOG.trace("removeOrphanedMutations found orphaned mutation in mutationList "
                               + pmRec + " " + mutationList.size() + " mutations remain");
                     mListIterator.remove();
                  }
               }
            } // while (pendingListIterator.hasNext()){

            // Clear out the orphaned mutations if there are any for this table
            if (getOrphanedMutations().size() > 0) {
               if (LOG.isTraceEnabled()) LOG.trace("removeOrphanedMutations found "
                  + getOrphanedMutations().size() + " orphaned mutations that need deleting");
               orphanedListIterator = getOrphanedMutations().listIterator();

               while (orphanedListIterator.hasNext()){
                  MutationMetaRecord mutationRecord = orphanedListIterator.next();
                  String mutationPathString = mutationRecord.getMutationPath();
                  //M-19585 wrong fs problem, use mutation relative path
                  mutationPathString = new URI(mutationPathString).getPath();

                  Path mutationPath = new Path (mutationPathString);

                  // Delete mutation file
                  if (LOG.isDebugEnabled()) LOG.debug("removeOrphanedMutations deleting orphaned mutation file at " + mutationPath);
                  if (fs.exists(mutationPath))
                    fs.delete(mutationPath, false);
                  else
                    if (LOG.isWarnEnabled()) LOG.warn("mutation file: " + mutationPath + " not exists.");

                  // Delete mutation record
                  if (LOG.isDebugEnabled()) LOG.debug("removeOrphanedMutations deleting orphaned mutationMetaRecord " + mutationRecord);
                  MutationMeta.deleteMutationRecord(mutationRecord.getRowKey());
                  orphanedListIterator.remove();
               }
            } // if (getOrphanedMutations().size() > 0)
         } // for (Map.Entry<String, TableRecoveryGroup> tableEntry : tableMap.entrySet()){
      }
      catch (Exception e){
         LOG.error("removeOrphanedMutations Exception ", e);
         throw new IOException("removeOrphanedMutations Exception" + e);
      }
   }

   /**
    * writeMutationRecords
    * @return void
    *
    */
   public void writeMutationRecords (Configuration config) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("writeMutationRecords start ");
      MutationMeta lvMM;
      try {
         if (LOG.isTraceEnabled()) LOG.trace("writeMutationRecords creating MutationMeta object ");
         lvMM = new MutationMeta(config);
      }
      catch (Exception e) {
         if (LOG.isTraceEnabled()) LOG.trace("writeMutationRecords Exception creating MutationMeta " , e);
         throw new IOException("writeMutationRecords Exception creating MutationMeta " , e);
      }

      for (Map.Entry<String, TableRecoveryGroup> tableEntry : recoveryTableMap.entrySet()) {
         String tableName = tableEntry.getKey();
         final List<MutationMetaRecord> lvMutationList = tableEntry.getValue().getMutationList();
         ListIterator<MutationMetaRecord> mutationListIterator = lvMutationList.listIterator();
         while (mutationListIterator.hasNext()){
            MutationMetaRecord mmRec = mutationListIterator.next();

            // It's possible the mutation records have changed underneath us, so
            // we must check the shadow table for the latest version of the record
            MutationMetaRecord mmRecShadow = lvMM.getMutationRecord(mmRec.getRowKey(), true /* readShadow */);
            if (mmRecShadow != null){
               if (mmRec.getSmallestCommitId() != mmRecShadow.getSmallestCommitId()){
                  mmRecShadow.setSmallestCommitId(Math.max(mmRec.getSmallestCommitId(), mmRecShadow.getSmallestCommitId()));
               }
               if (mmRec.getLargestCommitId() != mmRecShadow.getLargestCommitId()){
                  mmRecShadow.setLargestCommitId(Math.max(mmRec.getLargestCommitId(), mmRecShadow.getLargestCommitId()));
               }
               mmRec = mmRecShadow;
               lvMM.putMutationRecord(mmRec, true /* blind write */);
               if (LOG.isDebugEnabled())LOG.debug("writeMutationRecords mmRec updated from shadow table " + mmRec);
            }

            try {
               lvMM.putMutationRecord(mmRec, true /* blindWrite */);
            }
            catch (Exception e) {
               if (LOG.isTraceEnabled()) LOG.trace("writeMutationRecords Exception writing mutation record " , e);
               throw new IOException("writeMutationRecords Exception writing mutation record " , e);
            }
         }
      }
   }

   /**
    * excludeSnapshots
    * @return void
    *
    * This method is called during a restore operation and looks
    * for any snapshots that were performed after the time of the
    * specified restore operation as these should never be
    * considered for any future operations.
    */
   public void excludeSnapshots (Configuration config) throws IOException {
      if (LOG.isInfoEnabled()) LOG.info("excludeSnapshots start for tag " + tag);
      try{
         SnapshotMeta lvSM = new SnapshotMeta(config);
         // We exclude any newer snapshots that are associated with
         // each table because they should not be considered.
         Map<String, TableRecoveryGroup> tableMap =  this.getRecoveryTableMap();
         long maxKey = 0;
         HashSet<String> tableNameSet = new HashSet<>(tableMap.size());
         for (Map.Entry<String, TableRecoveryGroup> tableEntry : tableMap.entrySet()){
            String tableName = tableEntry.getKey();
            TableRecoveryGroup tableGroup = tableEntry.getValue();
            long excludePoint = tableGroup.getSnapshotRecord().getKey();
            maxKey = Math.max(maxKey, excludePoint);
            if (LOG.isTraceEnabled()) LOG.trace("excludeSnapshots "
                    + " Now looking for excluded snapshots for table " + tableName
                    + " and excludePoint " + excludePoint);
            tableNameSet.add(tableName);

         } // for (Map.Entry<String, TableRecoveryGroup> tableEntry : tableMap.entrySet()){
         if(maxKey > 0){
            lvSM.excludeSnapshotFromTable(tableNameSet, tag, maxKey);
         }
         if (LOG.isInfoEnabled()) LOG.info("excludeSnapshots end for tag " + tag);
      }
      catch (Exception e){
         LOG.error("excludeSnapshots Exception ", e);
         throw new IOException("excludeSnapshots Exception", e);
      }
   }

   /**
    * completeRestore
    * @return void
    *
    * This method is called during a restore operation and
    * performs the final necessary operations.  This can only be called
    * after we have trimmed the table list down in the case of subset restore.
    */
   public void completeRestore (Configuration config, boolean restoreSavedObjects, boolean restoreToTs) throws IOException {
      if (LOG.isInfoEnabled()) LOG.info("completeRestore start for tag " + tag
          + " restoreSavedObjects " + restoreSavedObjects + " restoreToTs " + restoreToTs);
      try{
         if (restoreSavedObjects){
            // Now we have the filtered table list, but we still need to make sure
            // none of the included tables have their mutations marked as 'PENDING'
            // if they should be part of this restore when coming from an import
            writeMutationRecords(config);
         }
         if (! restoreToTs){
            // Now we delete any 'PENDING' mutations that are
            // orphaned and should be deleted.
            removeOrphanedMutations(config);
         }

         // Now exclude any snapshots that occurred after the restore time
         excludeSnapshots(config);
         if (LOG.isInfoEnabled()) LOG.info("completeRestore end for tag " + tag);
      }
      catch (Exception e){
         LOG.error("completeRestore Exception ", e);
         throw new IOException("completeRestore Exception", e);
      }
   }

   /**
    * removeTable
    * @return void
    *
    */
   public void removeTable (String tableName) {
      if (LOG.isTraceEnabled()) LOG.trace("removeTable removing "
              + tableName + " from RecoveryRecord " + tag);
	  recoveryTableMap.remove(tableName);
   }




   /**
    * getSnapshotStartRecord
    * @return SnapshotMetaStartRecord
    *
    */
   public SnapshotMetaStartRecord getSnapshotStartRecord() {
     return smsr;
   }

   /**
    * getOrphanedMutations
    * @return List<MutationMetaRecord> orphanedMutations
    *
    */
   public List<MutationMetaRecord> getOrphanedMutations() {
     return orphanedMutations;
   }
/**
    * getRecoveryTableMap
    * @return Map<String, TableRecoveryGroup> recoveryTableMap
    *
    */
   public Map<String, TableRecoveryGroup> getRecoveryTableMap() {
     return recoveryTableMap;
   }

   /**
    * getIncrementalBackup
    * @return boolean
    *
    */
   public boolean getIncrementalBackup() {
     return incremental;
   }

   /**
    * getBackupType
    * @return String
    *
    */
   public String getBackupType() {
     return bkpType;
   }

   /**
    * getOpType
    * @return String
    *
    */
   public String getOpType() {
     return opType;
   }

   /**
    * setOpType
    * @param String
    * @return void
    *
    */
   public void setOpType(String op) {
     this.opType = op;
   }

   /**
    * getTag
    * @return String
    *
    */
   public String getTag() {
     return tag;
   }

   /**
    * getEndTime
    * @return long
    *
    */
   public long getEndTime() {
     return endTime;
   }

   /**
    * getEndWallTime
    * @return long
    *
    */
   public long getEndWallTime() {
     return endWallTime;
   }

   /**
    * getStartTime
    * @return long
    *
    */
   public long getStartTime() {
     return startTime;
   }

   /**
    * toString
    * @return String this
    *
    */
   @Override
   public String toString() {
     return "RecoveryRecord: tag: " + getTag() + " bkpType " + getBackupType() + " opType " + getOpType()
            + " startTime " + startTime + " endTime " + endTime + " incremental " + getIncrementalBackup()
            + " orphanedMutations " + getOrphanedMutations().size() + " recoveryTableMap size: " + recoveryTableMap.size();
   }
}
