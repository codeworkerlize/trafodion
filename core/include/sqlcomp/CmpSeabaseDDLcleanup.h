

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

  long getCleanupObjectUID(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                           const char *inObjType, char *outObjType, int &objectOwner, long *objectFlags = NULL,
                           long *objDataUID = NULL);

  short getCleanupObjectName(ExeCliInterface *cliInterface, long objUID, NAString &catName, NAString &schName,
                             NAString &objName, NAString &objType, int &objectOwner, long *objectFlags = NULL,
                             long *objDataUID = NULL);

  /* is there inferior partitions */
  short hasInferiorPartitons(ExeCliInterface *cliInterface, long objUID);
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

  short addReturnDetailsEntryForText(ExeCliInterface *cliInterface, Queue *&list, long objUID, int objType,
                                     NABoolean init);

  short addReturnDetailsEntryFromList(ExeCliInterface *cliInterface, Queue *fromList, int fromIndex, Queue *toList,
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
  long objUID_;
  long objectFlags_;
  long objDataUID_;
  int objectOwner_;
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
  int currReturnEntry_;

  NABoolean lobV2_;
  int numLOBs_;
  short *lobNumList_;
  short *lobTypList_;
  char **lobLocList_;
  char *lobMDName_;
  char *lobMDNameBuf_;
  int numLOBdatafiles_;

  int numOrphanMetadataEntries_;
  int numOrphanHbaseEntries_;
  int numOrphanObjectsEntries_;
  int numOrphanViewsEntries_;
  int numInconsistentPartitionEntries_;
  int numInconsistentHiveEntries_;
  int numInconsistentPrivEntries_;
  int numInconsistentGroupEntries_;
  int numInconsistentTextEntries_;
};

#endif
