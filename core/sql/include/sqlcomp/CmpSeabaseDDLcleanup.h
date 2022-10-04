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

/*
 ********************************************************************************
 *
 * File:         CmpSeabaseDDLcleanup.h
 * Description:  This file contains cleanup methods to handle
 *                     obsolete and orphan objects.
 *
 * *****************************************************************************
 */

#ifndef _CMP_SEABASE_CLEANUP_H_
#define _CMP_SEABASE_CLEANUP_H_

class StmtDDLCleanupObjects;

class CmpSeabaseMDcleanup : public CmpSeabaseDDL {
 public:
  CmpSeabaseMDcleanup(NAHeap *heap);

  short processCleanupErrors(ExeCliInterface *cliInterface, NABoolean &errorSeen);

  Int64 getCleanupObjectUID(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                            const char *objName, const char *inObjType, char *outObjType, Int32 &objectOwner,
                            Int64 *objectFlags = NULL, Int64 *objDataUID = NULL);

  short getCleanupObjectName(ExeCliInterface *cliInterface, Int64 objUID, NAString &catName, NAString &schName,
                             NAString &objName, NAString &objType, Int32 &objectOwner, Int64 *objectFlags = NULL,
                             Int64 *objDataUID = NULL);

  /* is there inferior partitions */
  short hasInferiorPartitons(ExeCliInterface *cliInterface, Int64 objUID);
  short getCleanupObjectPartitions(ExeCliInterface *cliInterface);
  short validateInputValues(StmtDDLCleanupObjects *stmtCleanupNode, ExeCliInterface *cliInterface);

  short gatherDependentObjects(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi);

  short deleteMDentries(ExeCliInterface *cliInterface);

  short deleteMDConstrEntries(ExeCliInterface *cliInterface);

  short deleteMDViewEntries(ExeCliInterface *cliInterface);

  short deleteHistogramEntries(ExeCliInterface *cliInterface);

  short dropIndexes(ExeCliInterface *cliInterface);

  short dropSequences(ExeCliInterface *cliInterface);

  short dropUsingViews(ExeCliInterface *cliInterface);

  short deletePrivs(ExeCliInterface *cliInterface);

  short deleteSchemaPrivs(ExeCliInterface *cliInterface);

  short deleteTenantSchemaUsages(ExeCliInterface *cliInterface);

  short deletePartitionEntries(ExeCliInterface *cliInterface);

  short addReturnDetailsEntry(ExeCliInterface *cliInterface, Queue *&list, const char *value, NABoolean init,
                              NABoolean isUID = FALSE);

  short addReturnDetailsEntryForText(ExeCliInterface *cliInterface, Queue *&list, Int64 objUID, Int32 objType,
                                     NABoolean init);

  short addReturnDetailsEntryFromList(ExeCliInterface *cliInterface, Queue *fromList, Lng32 fromIndex, Queue *toList,
                                      NABoolean isUID = FALSE, NABoolean processTextInfo = FALSE);

  void cleanupSchemaObjects(ExeCliInterface *cliInterface);

  short cleanupUIDs(ExeCliInterface *cliInterface, Queue *entriesList, CmpDDLwithStatusInfo *dws);

  short cleanupOrphanObjectsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, CmpDDLwithStatusInfo *dws);

  short cleanupOrphanHbaseEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, CmpDDLwithStatusInfo *dws);

  short cleanupInconsistentObjectsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                          CmpDDLwithStatusInfo *dws);

  short cleanupOrphanViewsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, CmpDDLwithStatusInfo *dws);
  short cleanupInconsistentPartitionEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                            CmpDDLwithStatusInfo *dws);

  short cleanupInconsistentHiveEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi);

  short cleanupInconsistentPrivEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi);

  short cleanupInconsistentGroupEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi);

  short cleanupInconsistentTextEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                       CmpDDLwithStatusInfo *dws);

  void cleanupInferiorPartitionEntries(ExeCliInterface *cliInterface);
  void cleanupHBaseObject(const StmtDDLCleanupObjects *stmtCleanupNode, ExeCliInterface *cliInterface);

  void cleanupMetadataEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, CmpDDLwithStatusInfo *dws);

  void cleanupObjects(StmtDDLCleanupObjects *stmtCleanupNode, NAString &currCatName, NAString &currSchName,
                      CmpDDLwithStatusInfo *dws);

 private:
  enum MDCleanupSteps {
    START_CLEANUP,
    ORPHAN_OBJECTS_ENTRIES,
    HBASE_ENTRIES,
    INCONSISTENT_OBJECTS_ENTRIES,
    INCONSISTENT_PARTITIONS_ENTRIES,
    VIEWS_ENTRIES,
    HIVE_ENTRIES,
    PRIV_ENTRIES,
    GROUP_ENTRIES,
    INCONSISTENT_TEXT_ENTRIES,
    DONE_CLEANUP
  };

  // stop cleanup if an error occurs
  NABoolean stopOnError_;

  NABoolean isHive_;
  NAString catName_;
  NAString schName_;
  NAString objName_;
  NAString extNameForHbase_;
  NAString extNameForHive_;
  NAString objType_;  // BT, IX, SG...
  Int64 objUID_;
  Int64 objectFlags_;
  Int64 objDataUID_;
  Int32 objectOwner_;
  NAString btObjName_;

  NABoolean cleanupMetadataEntries_;
  NABoolean checkOnly_;      // return status of cleanup, do not actually cleanup
  NABoolean returnDetails_;  // return details of cleanup
  NABoolean hasInferiorPartitons_;

  // only cleanup MD, dont drop HBase objects. Internal Usage only.
  NABoolean noHBaseDrop_;

  Queue *indexesUIDlist_;
  Queue *uniqueConstrUIDlist_;
  Queue *refConstrUIDlist_;
  Queue *seqUIDlist_;
  Queue *usingViewsList_;
  Queue *obsoleteEntriesList_;

  Queue *returnDetailsList_;
  Lng32 currReturnEntry_;

  NABoolean lobV2_;
  Lng32 numLOBs_;
  short *lobNumList_;
  short *lobTypList_;
  char **lobLocList_;
  char *lobMDName_;
  char *lobMDNameBuf_;
  Int32 numLOBdatafiles_;

  Lng32 numOrphanMetadataEntries_;
  Lng32 numOrphanHbaseEntries_;
  Lng32 numOrphanObjectsEntries_;
  Lng32 numOrphanViewsEntries_;
  Lng32 numInconsistentPartitionEntries_;
  Lng32 numInconsistentHiveEntries_;
  Lng32 numInconsistentPrivEntries_;
  Lng32 numInconsistentGroupEntries_;
  Lng32 numInconsistentTextEntries_;
};

#endif
