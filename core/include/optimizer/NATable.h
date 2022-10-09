
#ifndef NATABLE_H
#define NATABLE_H

#include <vector>

#include "optimizer/NAPartition.h"
#include "optimizer/Stats.h"
#include "arkcmp/CmpISPStd.h"
#include "cli/sqlcli.h"
#include "common/BaseTypes.h"
#include "common/Collections.h"
#include "common/ComEncryption.h"
#include "common/ComMvAttributeBitmap.h"
#include "common/ComSecurityKey.h"
#include "common/ComSizeDefs.h"
#include "common/ComSmallDefs.h"  // added for ComDiskFileFormat enum
#include "common/ComViewColUsage.h"
#include "common/Int64.h"
#include "common/NAMemory.h"
#include "common/SequenceGeneratorAttributes.h"
#include "common/charinfo.h"
#include "common/nawstring.h"
#include "exp/ExpHbaseDefs.h"
#include "optimizer/ItemConstr.h"
#include "optimizer/NAColumn.h"
#include "optimizer/NAFileSet.h"
#include "optimizer/ObjectNames.h"

// forward declaration(s)
//  -----------------------------------------------------------------------
//  contents of this file
//  -----------------------------------------------------------------------
class NATable;
class NATableDB;
class HistogramCache;
class HistogramsCacheEntry;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class BindWA;
class NATableDB;
struct TrafDesc;
class HbaseCreateOption;
class PrivMgrUserPrivs;
class ExpHbaseInterface;

typedef QualifiedName *QualifiedNamePtr;
typedef int (*HashFunctionPtr)(const QualifiedName &);
typedef SUBARRAY(int) CollIndexSet;

NAType *getSQColTypeForHive(const char *hiveType, NAMemory *heap);

#define NATABLE_MAX_REFCOUNT 12

// This class represents a single entry in the histogram cache.
// Each object of this class contains references to all the
// different histograms of a particular base table.
// It is intended to:
//  1) encapsulate a memory/information efficient representation of a
//     table's cached histograms that live in mxcmp's context heap
//     (which goes away only when mxcmp dies)
//  2) shield & keep invariant the current optimizer interface to histograms
//     that live in mxcmp's statement heap (which goes away at end of each
//     statement compilation)
class HistogramsCacheEntry : public NABasicObject {
  friend class HistogramCache;

 public:
  // constructor for creating memory efficient representation of colStats
  HistogramsCacheEntry(const StatsList &colStats, const QualifiedName &qualifiedName, long tableUID,
                       const long &statsTime, const long &redefTime, NAMemory *heap);

  // destructor
  virtual ~HistogramsCacheEntry();

  // setter methods
  // should be called to indicate that histograms for a given table
  // have been pre-fetched
  void setPreFetched(NABoolean preFetched = TRUE) { preFetched_ = preFetched; };

  const ColStatsSharedPtr getStatsAt(CollIndex x) const;

  const MultiColumnHistogram *getMultiColumnAt(CollIndex x) const;

  NABoolean contains(CollIndex colPos) const { return singleColumnPositions_.contains(colPos); }

  // insert all multicolumns referencing col into list
  // use singleColsFound to avoid duplicates
  void getMCStatsForColFromCacheIntoList(StatsList &list, NAColumn &col, ColumnSet &singleColsFound);

  // insert all the desired expressions histograms into list
  void getDesiredExpressionsStatsIntoList(StatsList &list,
                                          NAHashDictionary<NAString, NABoolean> &interestingExpressions);

  // adds histograms to this cache entry
  void addToCachedEntry(NAColumnArray &columns, StatsList &list);

  // add multi-column histogram to this cache entry
  void addMultiColumnHistogram(const ColStats &mcStat, ColumnSet *singleColPositions = NULL);

  // accessor methods
  ColStatsSharedPtr const getHistForCol(NAColumn &col) const;

  CollIndex singleColumnCount() const { return full_ ? full_->entries() : 0; }

  CollIndex multiColumnCount() const { return multiColumn_ ? multiColumn_->entries() : 0; }

  CollIndex expressionsCount() const { return expressions_ ? expressions_->entries() : 0; }

  NABoolean preFetched() const { return preFetched_; };
  const QualifiedName *getName() const;

  NABoolean accessedInCurrentStatement() const { return accessedInCurrentStatement_; }

  void resetAfterStatement() { accessedInCurrentStatement_ = FALSE; }

  // overloaded operator to satisfy hashdictionary
  inline NABoolean operator==(const HistogramsCacheEntry &other) { return (this == &other); };

  long getRefreshTime() const { return refreshTime_; };

  void setRefreshTime(long refreshTime) { refreshTime_ = refreshTime; };

  void updateRefreshTime();

  void setRedefTime(long redefTime) { redefTime_ = redefTime; };

  long getRedefTime() const { return redefTime_; };

  long getStatsTime() const { return statsTime_; };

  static long getLastUpdateStatsTime();

  static void setUpdateStatsTime(long updateTime);

  inline NABoolean isAllStatsFake() { return allFakeStats_; };

  inline void allStatsFake(NABoolean allFakeStats) { allFakeStats_ = allFakeStats; }

  inline int getSize() { return size_; }

  long getTableUID() const { return tableUID_; };

  void display() const;
  void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "HistogramsCacheEntry") const;
  void monitor(FILE *ofd) const;

 private:
  inline void setSize(int newSize) { size_ = newSize; }

  NAMemory *heap_;

  NABoolean preFetched_;

  // ---------------------------------------------------------------------
  // The time histograms for this table were last refreshed
  // ---------------------------------------------------------------------
  long refreshTime_;

  // ---------------------------------------------------------------------
  // The time this table was last altered
  // ---------------------------------------------------------------------
  long redefTime_;

  long statsTime_;  // STATS_TIME value from SB_HISTOGRAMS table

  // ----------------------------------------------------------------
  // Do all columns of this table consis of default statistics
  // ----------------------------------------------------------------
  NABoolean allFakeStats_;

  // The full histograms
  NAList<ColStatsSharedPtr> *full_;
  // is the memory-efficient contextheap representation
  // of a table's single-column histograms only

  ColumnSet singleColumnPositions_;
  // tracks single-column histograms that are in cache

  // multicolum histograms
  MultiColumnHistogramList *multiColumn_;
  // is the memory-efficient contextheap representation
  // of a table's multi-column histograms only

  // expressions histograms
  NAList<ColStatsSharedPtr> *expressions_;

  // pointer to qualified name of the table
  QualifiedName *name_;
  long tableUID_;
  NABoolean accessedInCurrentStatement_;
  int size_;
};  // class HistogramsCacheEntry

/****************************************************************************
** Class HistogramCache is used to cache a histograms to be used by a future
** compilation of statements on the same/subset/superset of tables.
** Internally it is basically a hashtable that hashes histograms(value) based
** on the name of the table(key)
** Currently NATable is the only class that references this particular class.
** If we decide to cache the NATable this class will not be necessary because
** NATable keeps the histograms as its private member.
** But the potential gain of caching the whole NATable compared to just caching
** the histogram for a MX table is little as we cache NATable in MX catalog.
*****************************************************************************/

class HistogramCache : public NABasicObject {
 public:
  HistogramCache(NAMemory *heap, int initSize = 107);

  // method called by NATable to get the cached histogram if any
  void getHistograms(NATable &table, NABoolean useStoredStats = FALSE);

  void invalidateCache();

  inline NAMemory *getHeap() { return heap_; }

  // set an upper limit for the heap used by the HistogramCache
  void setHeapUpperLimit(size_t newUpperLimit) { heap_->setUpperLimit(newUpperLimit); }

  inline int hits() { return hits_; }
  inline int lookups() { return lookups_; }

  inline void resetIntervalWaterMark() { heap_->resetIntervalWaterMark(); }

  void resizeCache(size_t limit);

  inline int getSize() { return size_; }

  // reset all entries to not accessedInCurrentStatement
  void resetAfterStatement();

  void closeTraceFile();
  void openTraceFile(const char *filename);

  void closeMonitorFile();
  void openMonitorFile(const char *filename);

  FILE *getTraceFileDesc() const { return tfd_; }
  FILE *getMonitorFileDesc() const { return mfd_; }
  void traceTable(NATable &table) const;
  void traceTablesFinalize() const;
  void monitor() const;

  void freeInvalidEntries(int returnedNumQiKeys, SQL_QIKEY *qiKeyArray);

 private:
  DISALLOW_COPY_AND_ASSIGN(HistogramCache);

  // is a helper function for getHistograms look at .cpp file
  // for more detail
  // retreive the statistics from the cache and put
  // them into colStatsList.
  void createColStatsList(NATable &table, HistogramsCacheEntry *cachedHistograms, NABoolean useStoredStats = FALSE);

  // gets the StatsList into list from the histogram cache
  // returns the number of columns whose statistics were
  // found in the cache. The columns whose statistics are required
  // are passed in through localArray.
  int getStatsListFromCache(StatsList &list,                                                // Out
                            NAColumnArray &localArray,                                      // In
                            NAHashDictionary<NAString, NABoolean> &interestingExpressions,  // In
                            HistogramsCacheEntry *cachedHistograms,                         // In
                            ColumnSet &singleColsFound);                                    // In \ Out

  // This method is used to put a StatsList object, that has been
  void putStatsListIntoCache(StatsList &colStatsList, const NAColumnArray &colArray, const QualifiedName &qualifiedName,
                             long tableUID, long statsTime, const long &redefTime, NABoolean allFakeStats);

  // lookup given table's histograms.
  // if found, return its HistogramsCacheEntry*.
  // otherwise, return NULL.
  HistogramsCacheEntry *lookUp(NATable &table);

  // decache entry and set it to NULL
  void deCache(HistogramsCacheEntry **entry);

  int entries() const;

  void display() const;
  void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "HistogramCache") const;

  size_t memoryLimit() const { return memoryLimit_; }

  NABoolean enforceMemorySpaceConstraints();

  NAMemory *heap_;

  size_t memoryLimit_;
  // start evicting cache entries when heap_ hits memoryLimit_

  NAList<HistogramsCacheEntry *> lruQ_;
  // lruQ_.first is least recently used cache entry

  // The Cache
  NAHashDictionary<QualifiedName, HistogramsCacheEntry> *histogramsCache_;

  long lastTouchTime_;  // last time cache was touched
  int hits_;            // cache hit counter
  int lookups_;         // entries lookup counter
  int size_;
  FILE *tfd_;  // trace file handle
  FILE *mfd_;  // monitor file handle
};             // class HistogramCache

struct NATableEntryDetails {
  char catalog[ComMAX_1_PART_INTERNAL_UTF8_NAME_LEN_IN_BYTES + 1];  // +1 for NULL byte
  char schema[ComMAX_1_PART_INTERNAL_UTF8_NAME_LEN_IN_BYTES + 1];
  char object[ComMAX_1_PART_INTERNAL_UTF8_NAME_LEN_IN_BYTES + 1];
  int hasPriv;
  int size;
};

class minmaxKey : public NABasicObject {
 public:
  minmaxKey(const NAString &joinCols, const NAString &localPreds, NAMemory *heap)
      :  // joinCols_(NAString(joinCols, heap)),
         // localPreds_(NAString(localPreds, heap))
        joinCols_(joinCols, heap),
        localPreds_(localPreds, heap){};

  inline UInt32 hash() const { return joinCols_.hash() ^ localPreds_.hash(); };

  inline NABoolean operator==(const minmaxKey &other) const {
    return joinCols_ == other.joinCols_ && localPreds_ == other.localPreds_;
  }

  void display(const char *msg = NULL);

 private:
  NAString joinCols_;
  NAString localPreds_;
};

class minmaxValue : public NABasicObject {
 public:
  minmaxValue(CostScalar rc, CostScalar min, CostScalar max) : rowcount_(rc), minValue_(min), maxValue_(max){};

  NABoolean operator==(const minmaxValue &other) const {
    CMPASSERT(FALSE);
    return FALSE;
  };

  const CostScalar &getRowcount() { return rowcount_; };
  const CostScalar &getMinValue() { return minValue_; };
  const CostScalar &getMaxValue() { return maxValue_; };

 private:
  CostScalar rowcount_;
  CostScalar minValue_;
  CostScalar maxValue_;
};

// ***********************************************************************
// NATable : The basic (Non-Acidic) Table class
//
// A NATable contains the description of the physical schema for an
// SQL table or a table-valued stored procedure, which is a source
// of rows that cannot be decomposed. It contains information such as
// the number of columns as well as the the set of files (fileset)
// used for implementing the table. It can shared across the optimization
// of several SQL statements.
//
// One NATable is shared by one or more references to the same table
// within a given query.
//
// NATables are organized in a search structure called the NATableDB.
// It uses the qualified name (ANSI name or os name) for the table
// as the key for imposing an organization on NATables as well as the
// lookup.
//
// ***********************************************************************

class NATable : public NABasicObject {
  friend class NATableDB;

 public:
  // the type of heap pointed to by the heap_ datamember
  // this is so that in the destructor we can delete heap_ if
  // it is not a statement or a context heap.
  enum NATableHeapType { STATEMENT, CONTEXT, OTHER };

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------

  NATable(BindWA *bindWA, const CorrName &corrName, NAMemory *heap, TrafDesc *inTableDesc, UInt32 currentEpoch,
          UInt32 currentFlags);

  // copy cstr
  NATable(const NATable &other, NAMemory *heap, NATableHeapType heapType);

  virtual ~NATable();

  // Obtain a list of table identifiers for all the indices and vertical
  // partitions of the NATable object, stored in tableIdList_
  const LIST(CollIndex) & getTableIdList() const;
  // void reset();      // not needed/implemented yet but see .C file for notes
  // IMPORTANT READ THIS IF U CHANGE ANYTHING IN NATABLE
  // reset stuff after statement is done so that this NATable object
  // can be used by subsequent statements (i.e. NATable Caching).
  //***************************************************************************
  // If u change anything in the NATable after NATable construction as a result
  // of statement specific state, please set it back to the value it had after
  // NATable construction.
  //***************************************************************************
  void resetAfterStatement();

  // setup this NATable for the statement.
  // This has to be done after NATable construction
  // or after an NATable has been retrieved from the cache
  void setupForStatement();

  // This method changes the partFunc of the base table to remove
  // all partitions other than the ones specified by pName. Currently
  // this method only supports having a single partition name in pName.
  NABoolean filterUnusedPartitions(const PartitionClause &pClause);

  // by default column histograms are marked to not be fetched,
  // i.e. needHistogram_ is initialized to DONT_NEED_HIST.
  // this method will mark columns for appropriate histograms depending on
  // where they have been referenced in the query
  void markColumnsForHistograms();

  // accumulate a list of interesting expressions that might
  // have an expression histogram
  void addInterestingExpression(NAString &unparsedExpr);

  // get the set of interesting expressions (the NABoolean is just a dummy value)
  NAHashDictionary<NAString, NABoolean> &interestingExpressions() { return interestingExpressions_; }

  const QualifiedName &getFullyQualifiedGuardianName();
  ExtendedQualName::SpecialTableType getTableType();
  StatsList *getColStats() { return colStats_; }

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const ExtendedQualName &getExtendedQualName() const { return qualifiedName_; }
  const QualifiedName &getTableName() const { return qualifiedName_.getQualifiedNameObj(); }
  QualifiedName &getTableName() { return qualifiedName_.getQualifiedNameObj(); }
  const NAString getSynonymReferenceName() const { return synonymReferenceName_; }
  const ComUID &getSynonymReferenceObjectUid() const { return synonymReferenceObjectUid_; }
  NABoolean getIsSynonymTranslationDone() const { return isSynonymTranslationDone_; }
  const QualifiedName &getFileSetName() const { return fileSetName_; }
  ExtendedQualName::SpecialTableType getSpecialType() const { return qualifiedName_.getSpecialType(); }

  int getReferenceCount() const { return referenceCount_; }
  void incrReferenceCount() { ++referenceCount_; }
  void decrReferenceCount();
  void resetReferenceCount();

  void setRefsIncompatibleDP2Halloween() { refsIncompatibleDP2Halloween_ = TRUE; }
  NABoolean getRefsIncompatibleDP2Halloween() const { return refsIncompatibleDP2Halloween_; }
  void setIsHalloweenTable() { isHalloweenTable_ = TRUE; }
  NABoolean getIsHalloweenTable() const { return isHalloweenTable_; }

  CollIndex getColumnCount() const { return colcount_; }
  CollIndex getUserColumnCount() const;
  const NAColumnArray &getNAColumnArray() const { return colArray_; }
  const NAColumnArray &getHiveNAColumnArray() const { return hiveColArray_; }
  void getNAColumnMap(std::map<int, char *> &colMap, NABoolean lowercase);

  const NAPartitionArray &getNAPartitionArray() const { return partArray_; }

  int getRecordLength() const { return recordLength_; }
  Cardinality getEstRowCount() const { return clusteringIndex_->getEstimatedNumberOfRecords(); }
  int getKeyCount() const { return clusteringIndex_->getIndexKeyColumns().entries(); }

  const NAFileSet *getClusteringIndex() const { return clusteringIndex_; }
  NAFileSet *getClusteringIndex() { return clusteringIndex_; }
  const NAFileSetList &getIndexList() const { return indexes_; }
  NABoolean hasSecondaryIndexes() const { return indexes_.entries() > 1; }
  const NAFileSetList &getVerticalPartitionList() const { return vertParts_; }

  NABoolean getCorrespondingIndex(NAList<NAString> &inputCols, NABoolean explicitIndex, NABoolean lookForUniqueIndex,
                                  NABoolean lookForPrimaryKey, NABoolean lookForAnyIndexOrPkey,
                                  NABoolean lookForSameSequenceOfCols, NABoolean excludeAlwaysComputedSystemCols,
                                  NAString *indexName);

  NABoolean getCorrespondingConstraint(NAList<NAString> &inputCols, NABoolean uniqueConstr, NAString *constrName = NULL,
                                       NABoolean *isPkey = NULL, NAList<int> *reorderList = NULL);

  const TrafDesc *getColumnsDesc() const { return columnsDesc_; }
  TrafDesc *getColumnsDesc() { return columnsDesc_; }

  // A not-found partition is an offline partition.
  NABoolean containsPartition(const NAString &partitionName) const {
    for (CollIndex i = 0; i < indexes_.entries(); i++)
      if (indexes_[i]->containsPartition(partitionName)) return TRUE;
    return FALSE;
  }
  NABoolean isOfflinePartition(const NAString &partitionName) const {
    return !partitionName.isNull() && !containsPartition(partitionName);
  }

  // move relevant attributes from etTable to this.
  // Currently, column and key info is moved.
  short updateExtTableAttrs(NATable *etTable);

  const long &getCreateTime() const { return createTime_; }
  const long &getRedefTime() const { return redefTime_; }
  const long &getStatsTime() const { return statsTime_; }

  const ComUID &getCatalogUid() const { return catalogUID_; }
  const ComUID &getSchemaUid() const { return schemaUID_; }

  const ComUID &objectUid() const {
    if (objectUID_.get_value() == 0) {
      // no need to lookup objectuid.
      // const_cast<NATable*>(this)->lookupObjectUid();  // cast off const
    }

    return objectUID_;
  }

  const ComUID &objDataUID() const {
    if (objDataUID_.get_value() == 0) {
      // no need to lookup objectuid.
      // const_cast<NATable*>(this)->lookupObjectUid();  // cast off const
    }

    return objDataUID_;
  }

  const ComUID &baseTableUid() const { return baseTableUID_; }

  // fetch the object UID that is associated with the external
  // table object (if any) for a native table.
  // Set objectUID_ to 0 if no such external table exists;
  // set objectUID_ to -1 if there is error during the fetch operation;
  NABoolean fetchObjectUIDForNativeTable(const CorrName &corrName, NABoolean isView);

  long lookupObjectUid();  // Used to look up uid on demand for metadata tables.
                           // On return, the "Object Not Found" error (-1389)
                           // is filtered out from CmpCommon::diags().

  bool isEnabledForDDLQI() const;

  const ComObjectType &getObjectType() const { return objectType_; }

  const COM_VERSION &getObjectSchemaVersion() const { return osv_; }
  const COM_VERSION &getObjectFeatureVersion() const { return ofv_; }

  const int &getOwner() const { return owner_; }
  const int &getSchemaOwner() const { return schemaOwner_; }

  const void *getRCB() const { return rcb_; }
  int getRCBLength() const { return rcbLen_; }
  int getKeyLength() const { return keyLength_; }

  const char *getParentTableName() const { return parentTableName_; }
  const ComPartitioningScheme &getPartitioningScheme() const { return partitioningScheme_; }

  const HostVar *getPrototype() const { return prototype_; }

  const ComReplType xnRepl() const { return xnRepl_; }

  const char *getViewText() const { return viewText_; }
  const NAWchar *getViewTextInNAWchars() const {
    return viewTextInNAWchars_.length() > 0 ? viewTextInNAWchars_.data() : NULL;
  }
  const NAWString &getViewTextAsNAWString() const { return viewTextInNAWchars_; }
  CharInfo::CharSet getViewTextCharSet() const { return viewTextCharSet_; }
  NABoolean isView() const { return (viewText_ != NULL); }

  // getViewLen is needed to compute buffer len for a parseDML call
  // locale-to-unicode conversion in parseDML requires buffer len (tcr)
  int getViewLen() const { return viewText_ ? strlen(viewText_) : 0; }
  int getViewTextLenInNAWchars() const { return viewTextInNAWchars_.length(); }

  const char *getViewCheck() const { return viewCheck_; }
  const NAList<ComViewColUsage *> *getViewColUsages() const { return viewColUsages_; }

  const char *getHiveExpandedViewText() const { return hiveExpandedViewText_; }
  const char *getHiveOriginalViewText() const { return hiveOriginalViewText_; }

  NABoolean hasSaltedColumn(int *saltColPos = NULL) const;
  //  const NABoolean hasSaltedColumn(int * saltColPos = NULL) const;
  NABoolean hasDivisioningColumn(int *divColPos = NULL);
  NABoolean hasTrafReplicaColumn(int *repColPos = NULL) const;
  Int16 getNumTrafReplicas() const;

  void setUpdatable(NABoolean value) { value ? flags_ |= IS_UPDATABLE : flags_ &= ~IS_UPDATABLE; }

  NABoolean isUpdatable() const { return (flags_ & IS_UPDATABLE) != 0; }

  void setInsertable(NABoolean value) { value ? flags_ |= IS_INSERTABLE : flags_ &= ~IS_INSERTABLE; }

  NABoolean isInsertable() const { return (flags_ & IS_INSERTABLE) != 0; }

  void setSQLMXTable(NABoolean value) { value ? flags_ |= SQLMX_ROW_TABLE : flags_ &= ~SQLMX_ROW_TABLE; }

  NABoolean isSQLMXTable() const { return (flags_ & SQLMX_ROW_TABLE) != 0; }

  void setSQLMXAlignedTable(NABoolean value) {
    (value ? flags_ |= SQLMX_ALIGNED_ROW_TABLE : flags_ &= ~SQLMX_ALIGNED_ROW_TABLE);
  }

  NABoolean isSQLMXAlignedTable() const {
    if (getClusteringIndex() != NULL)
      return getClusteringIndex()->isSqlmxAlignedRowFormat();
    else
      return getSQLMXAlignedTable();
  }

  NABoolean isAlignedFormat(const IndexDesc *indexDesc) const {
    NABoolean isAlignedFormat;

    if (isHbaseRowTable() || isHbaseCellTable() || (indexDesc == NULL))
      isAlignedFormat = isSQLMXAlignedTable();
    else
      isAlignedFormat = indexDesc->getNAFileSet()->isSqlmxAlignedRowFormat();
    return isAlignedFormat;
  }

  void setVerticalPartitions(NABoolean value) {
    value ? flags_ |= IS_VERTICAL_PARTITION : flags_ &= ~IS_VERTICAL_PARTITION;
  }

  NABoolean isVerticalPartition() const { return (flags_ & IS_VERTICAL_PARTITION) != 0; }

  void setHasVerticalPartitions(NABoolean value) {
    value ? flags_ |= HAS_VERTICAL_PARTITIONS : flags_ &= ~HAS_VERTICAL_PARTITIONS;
  }

  NABoolean hasVerticalPartitions() const { return (flags_ & HAS_VERTICAL_PARTITIONS) != 0; }

  void setHasAddedColumn(NABoolean value) { value ? flags_ |= ADDED_COLUMN : flags_ &= ~ADDED_COLUMN; }

  NABoolean hasAddedColumn() const { return (flags_ & ADDED_COLUMN) != 0; }

  void setHasVarcharColumn(NABoolean value) { value ? flags_ |= VARCHAR_COLUMN : flags_ &= ~VARCHAR_COLUMN; }

  NABoolean hasVarcharColumn() const { return (flags_ & VARCHAR_COLUMN) != 0; }

  void setVolatileTable(NABoolean value) { value ? flags_ |= VOLATILE : flags_ &= ~VOLATILE; }

  NABoolean isVolatileTable() const { return ((flags_ & VOLATILE) != 0); }

  void setIsVolatileTableMaterialized(NABoolean value) {
    value ? flags_ |= VOLATILE_MATERIALIZED : flags_ &= ~VOLATILE_MATERIALIZED;
  }

  NABoolean isVolatileTableMaterialized() const { return ((flags_ & VOLATILE_MATERIALIZED) != 0); }

  NABoolean useRowIdEncryption() const { return (flags_ & ROWID_ENCRYPT) != 0; }
  void setUseRowIdEncryption(NABoolean v) { v ? flags_ |= ROWID_ENCRYPT : flags_ &= ~ROWID_ENCRYPT; }

  NABoolean useDataEncryption() const { return (flags_ & DATA_ENCRYPT) != 0; }
  void setUseDataEncryption(NABoolean v) { v ? flags_ |= DATA_ENCRYPT : flags_ &= ~DATA_ENCRYPT; }

  NABoolean useEncryption() const { return (useRowIdEncryption() || useDataEncryption()); }

  // this object was only created in mxcmp memory(catman cache, NAtable
  // cache. It doesn't exist in metadata or physical labels.
  // Used to test different access plans without actually creating
  // the object.
  void setInMemoryObjectDefn(NABoolean value) { value ? flags_ |= IN_MEM_OBJECT_DEFN : flags_ &= ~IN_MEM_OBJECT_DEFN; }

  NABoolean isInMemoryObjectDefn() const { return ((flags_ & IN_MEM_OBJECT_DEFN) != 0); }

  void setRemoveFromCacheBNC(NABoolean value) /* BNC = Before Next Compilation attempt */
  {
    value ? flags_ |= REMOVE_FROM_CACHE_BNC : flags_ &= ~REMOVE_FROM_CACHE_BNC;
  }

  NABoolean isToBeRemovedFromCacheBNC() const /* BNC = Before Next Compilation attempt */
  {
    return ((flags_ & REMOVE_FROM_CACHE_BNC) != 0);
  }

  void setDroppableTable(NABoolean value) { value ? flags_ |= DROPPABLE : flags_ &= ~DROPPABLE; }

  NABoolean isDroppableTable() const { return ((flags_ & DROPPABLE) != 0); }

  ComInsertMode getInsertMode() const { return insertMode_; }
  void setInsertMode(ComInsertMode im) { insertMode_ = im; }

  NABoolean isSetTable() const { return (insertMode_ == COM_SET_TABLE_INSERT_MODE); }

  // Set to TRUE if a SYSTEM_COLUMN is being treated as a USER_COLUMN.
  // Currently SYSKEY is treated as a USER_COLUMN if OVERRIDE_SYSKEY CQD
  // is set to 'on'
  void setSystemColumnUsedAsUserColumn(NABoolean value) {
    value ? flags_ |= SYSTEM_COL_AS_USER_COL : flags_ &= ~SYSTEM_COL_AS_USER_COL;
  }
  NABoolean hasSystemColumnUsedAsUserColumn() const { return ((flags_ & SYSTEM_COL_AS_USER_COL) != 0); }

  void setHasSerializedEncodedColumn(NABoolean value) {
    value ? flags_ |= SERIALIZED_ENCODED_COLUMN : flags_ &= ~SERIALIZED_ENCODED_COLUMN;
  }

  NABoolean hasSerializedEncodedColumn() const { return (flags_ & SERIALIZED_ENCODED_COLUMN) != 0; }

  void setHasSerializedColumn(NABoolean value) { value ? flags_ |= SERIALIZED_COLUMN : flags_ &= ~SERIALIZED_COLUMN; }

  NABoolean hasSerializedColumn() const { return (flags_ & SERIALIZED_COLUMN) != 0; }

  ComReplType xnRepl() { return xnRepl_; }
  void setXnRepl(ComReplType v) { xnRepl_ = v; }

  const NABoolean incrBackupEnabled() const { return incrBackupEnabled_; }
  NABoolean incrBackupEnabled() { return incrBackupEnabled_; }
  void setIncrBackupEnabled(NABoolean v) { incrBackupEnabled_ = v; }

  const NABoolean readOnlyEnabled() const { return readOnlyEnabled_; }
  NABoolean readOnlyEnabled() { return readOnlyEnabled_; }
  void setReadOnlyEnabled(NABoolean v) { readOnlyEnabled_ = v; }

  ComStorageType storageType() const { return storageType_; }
  void setStorageType(ComStorageType v) { storageType_ = v; }

  void setIsTrafExternalTable(NABoolean value) {
    value ? flags_ |= IS_TRAF_EXTERNAL_TABLE : flags_ &= ~IS_TRAF_EXTERNAL_TABLE;
  }

  NABoolean isTrafExternalTable() const { return (flags_ & IS_TRAF_EXTERNAL_TABLE) != 0; }

  void setIsImplicitTrafExternalTable(NABoolean value) {
    value ? flags_ |= IS_IMPLICIT_TRAF_EXT_TABLE : flags_ &= ~IS_IMPLICIT_TRAF_EXT_TABLE;
  }

  NABoolean isImplicitTrafExternalTable() const { return (flags_ & IS_IMPLICIT_TRAF_EXT_TABLE) != 0; }

  void setHasExternalTable(NABoolean value) { value ? flags_ |= HAS_EXTERNAL_TABLE : flags_ &= ~HAS_EXTERNAL_TABLE; }

  NABoolean hasExternalTable() const { return (flags_ & HAS_EXTERNAL_TABLE) != 0; }

  void setIsHbaseMapTable(NABoolean value) { value ? flags_ |= HBASE_MAP_TABLE : flags_ &= ~HBASE_MAP_TABLE; }

  NABoolean isHbaseMapTable() const { return (flags_ & HBASE_MAP_TABLE) != 0; }

  void setHbaseDataFormatString(NABoolean value) {
    value ? flags_ |= HBASE_DATA_FORMAT_STRING : flags_ &= ~HBASE_DATA_FORMAT_STRING;
  }

  NABoolean isHbaseDataFormatString() const { return (flags_ & HBASE_DATA_FORMAT_STRING) != 0; }

  void setIsHistogramTable(NABoolean value) { value ? flags_ |= IS_HISTOGRAM_TABLE : flags_ &= ~IS_HISTOGRAM_TABLE; }

  NABoolean isHistogramTable() const { return (flags_ & IS_HISTOGRAM_TABLE) != 0; }

  void setHasHiveExtTable(NABoolean value) { value ? flags_ |= HAS_HIVE_EXT_TABLE : flags_ &= ~HAS_HIVE_EXT_TABLE; }
  NABoolean hasHiveExtTable() const { return (flags_ & HAS_HIVE_EXT_TABLE) != 0; }

  static const char *getNameOfInputFileCol() { return "INPUT__FILE__NAME"; }
  static const char *getNameOfBlockOffsetCol() { return "BLOCK__OFFSET__INSIDE__FILE"; }
  static const char *getNameOfInputRangeCol() { return "INPUT__RANGE__NUMBER"; }
  static const char *getNameOfRowInRangeCol() { return "ROW__NUMBER__IN__RANGE"; }

  void setHiveExtColAttrs(NABoolean value) { value ? flags_ |= HIVE_EXT_COL_ATTRS : flags_ &= ~HIVE_EXT_COL_ATTRS; }
  NABoolean hiveExtColAttrs() const { return (flags_ & HIVE_EXT_COL_ATTRS) != 0; }

  void setHiveExtKeyAttrs(NABoolean value) { value ? flags_ |= HIVE_EXT_KEY_ATTRS : flags_ &= ~HIVE_EXT_KEY_ATTRS; }
  NABoolean hiveExtKeyAttrs() const { return (flags_ & HIVE_EXT_KEY_ATTRS) != 0; }

  void setIsRegistered(NABoolean value) { value ? flags_ |= IS_REGISTERED : flags_ &= ~IS_REGISTERED; }

  NABoolean isRegistered() const { return (flags_ & IS_REGISTERED) != 0; }

  void setIsInternalRegistered(NABoolean value) {
    value ? flags_ |= IS_INTERNAL_REGISTERED : flags_ &= ~IS_INTERNAL_REGISTERED;
  }

  NABoolean isInternalRegistered() const { return (flags_ & IS_INTERNAL_REGISTERED) != 0; }

  // NATable was preloaded
  void setIsPreloaded(NABoolean value) { value ? flags2_ |= PRELOADED : flags2_ &= ~PRELOADED; }
  NABoolean isPreloaded() const { return (flags2_ & PRELOADED) != 0; }

  // NATable was created using stored descriptor in TEXT table
  void setStoredDesc(NABoolean value) { value ? flags2_ |= STORED_DESC : flags2_ &= ~STORED_DESC; }
  NABoolean isStoredDesc() const { return (flags2_ & STORED_DESC) != 0; }

  void setIsHiveExternalTable(NABoolean value) {
    value ? flags2_ |= IS_HIVE_EXTERNAL_TABLE : flags2_ &= ~IS_HIVE_EXTERNAL_TABLE;
  }
  NABoolean isHiveExternalTable() const { return (flags2_ & IS_HIVE_EXTERNAL_TABLE) != 0; }

  void setIsHiveManagedTable(NABoolean value) {
    value ? flags2_ |= IS_HIVE_MANAGED_TABLE : flags2_ &= ~IS_HIVE_MANAGED_TABLE;
  }
  NABoolean isHiveManagedTable() const { return (flags2_ & IS_HIVE_MANAGED_TABLE) != 0; }

  void setHasCompositeColumns(NABoolean value) {
    value ? flags2_ |= HAS_COMPOSITE_COLS : flags2_ &= ~HAS_COMPOSITE_COLS;
  }
  NABoolean hasCompositeColumns() const { return (flags2_ & HAS_COMPOSITE_COLS) != 0; }

  void setHasTrigger(NABoolean value) { value ? flags2_ |= HAS_TRIGGER : flags2_ &= ~HAS_TRIGGER; }
  NABoolean hasTrigger() const { return (flags2_ & HAS_TRIGGER) != 0; }

  void setLobV2(NABoolean value) { value ? flags2_ |= LOB_VERSION2 : flags2_ &= ~LOB_VERSION2; }
  NABoolean lobV2() const { return (flags2_ & LOB_VERSION2) != 0; }

  void setIsPartitionV2Table(NABoolean value) {
    value ? flags2_ |= IS_PARTITIONV2_TABLE : flags2_ &= ~IS_PARTITIONV2_TABLE;
  }
  NABoolean isPartitionV2Table() const { return (flags2_ & IS_PARTITIONV2_TABLE) != 0; }

  void setIsPartitionEntityTable(NABoolean value) {
    value ? flags2_ |= IS_PARTITION_ENTITY_TABLE : flags2_ &= ~IS_PARTITION_ENTITY_TABLE;
  }
  NABoolean isPartitionEntityTable() const { return (flags2_ & IS_PARTITION_ENTITY_TABLE) != 0; }

  void setIsPartitionV2IndexTable(NABoolean value) {
    value ? flags2_ |= IS_PARTITIONV2_INDEX_TABLE : flags2_ &= ~IS_PARTITIONV2_INDEX_TABLE;
  }
  NABoolean isPartitionV2IndexTable() const { return (flags2_ & IS_PARTITIONV2_INDEX_TABLE) != 0; }

  void setIsPartitionEntityIndexTable(NABoolean value) {
    value ? flags2_ |= IS_PARTITION_ENTITY_INDEX_TABLE : flags2_ &= ~IS_PARTITION_ENTITY_INDEX_TABLE;
  }
  NABoolean isPartitionEntityIndexTable() const { return (flags2_ & IS_PARTITION_ENTITY_INDEX_TABLE) != 0; }

  const CheckConstraintList &getCheckConstraints() const { return checkConstraints_; }
  const AbstractRIConstraintList &getUniqueConstraints() const { return uniqueConstraints_; }
  const AbstractRIConstraintList &getRefConstraints() const { return refConstraints_; }

  NABoolean rowsArePacked() const;

  StatsList *getStatistics(NABoolean useStoredStats = FALSE);

  StatsList *generateFakeStats();

  inline CostScalar getOriginalRowCount() const { return originalCardinality_; }
  void setOriginalRowCount(CostScalar rowcount) { originalCardinality_ = rowcount; }

  // ---------------------------------------------------------------------
  // Standard operators
  // ---------------------------------------------------------------------
  NABoolean operator==(const NATable &other) const;

  const ExtendedQualName *getKey() const { return &qualifiedName_; }

  char *getViewFileName() const { return viewFileName_; }

  // MV
  // ---------------------------------------------------------------------
  // Materialized Views support
  // ---------------------------------------------------------------------
  const UsingMvInfoList &getMvsUsingMe() const { return mvsUsingMe_; }
  NABoolean isAnMV() const { return mvAttributeBitmap_.getIsAnMv(); }
  NABoolean isAnMVMetaData() const { return isAnMVMetaData_; }
  const ComMvAttributeBitmap &getMvAttributeBitmap() const { return mvAttributeBitmap_; }
  NABoolean verifyMvIsInitializedAndAvailable(BindWA *bindWA) const;

  NABoolean accessedInCurrentStatement() { return accessedInCurrentStatement_; }
  void setAccessedInCurrentStatement() { accessedInCurrentStatement_ = TRUE; }

  // ---------------------------------------------------------------------
  // Sequence generator support
  // ---------------------------------------------------------------------

  const SequenceGeneratorAttributes *getSGAttributes() const { return sgAttributes_; }

  // size is the size of the table's heap
  // this is useful for NATable caching because a seperate
  // heap is created for each cached NATable
  int getSize() { return (heap_ ? heap_->getTotalSize() : 0); }

  // returns true if this is an MP table with an Ansi Name
  NABoolean isAnMPTableWithAnsiName() const { return isAnMPTableWithAnsiName_; };

  // returns true if this is a UMD table (not an MV UMD table, though)
  NABoolean isUMDTable() const { return isUMDTable_; };

  // returns true if this is a SMD table
  NABoolean isSMDTable() const { return isSMDTable_; };

  // returns true if this is a MV UMD table
  NABoolean isMVUMDTable() const { return isMVUMDTable_; };

  // returns true if this is a trigger temporary table
  NABoolean isTrigTempTable() const { return isTrigTempTable_; };

  // returns true if this is an exception table
  NABoolean isExceptionTable() const { return isExceptionTable_; };

  // return true if there were warnings generated during
  // the construction of this NATable object
  NABoolean constructionHadWarnings() { return tableConstructionHadWarnings_; }
  NABoolean doesMissingStatsWarningExist(CollIndexSet &listOfColPositions) const;

  NABoolean insertMissingStatsWarning(CollIndexSet colsSet) const;

  const TrafDesc *getStoredStatsDesc() const { return storedStatsDesc_; }
  const NABoolean storedStatsAvail() const { return (storedStatsDesc_ != NULL); }
  void statsToUse(TrafDesc *storedStatsDesc, NABoolean &useFastStats, NABoolean &useStoredStats);
  void updateStoredStats(TrafDesc *tableDesc);

  NAList<HbaseCreateOption *> *hbaseCreateOptions() { return clusteringIndex_->hbaseCreateOptions(); }

  int getNumReplications();

  NABoolean isStatsFetched() const { return statsFetched_; };

  void setStatsFetched(NABoolean flag = TRUE) { statsFetched_ = flag; }

  NABoolean isPartitionNameSpecified() const { return qualifiedName_.isPartitionNameSpecified(); }

  NABoolean isPartitionRangeSpecified() const { return qualifiedName_.isPartitionRangeSpecified(); }

  NABoolean isPartitionV2() const { return qualifiedName_.isPartitionV2(); }

  NABoolean isHiveTable() const { return isHive_; }
  NABoolean isORC() const { return isORC_; }
  NABoolean isParquet() const { return isParquet_; }
  NABoolean isAvro() const { return isAvro_; }
  NABoolean isExtStorage() const { return (isORC() || isParquet() || isAvro()); }

  static NABoolean usingSentry();
  static NABoolean usingLinuxGroups();
  static NABoolean usingLDAPGroups();

  NABoolean isHbaseTable() const { return isHbase_; }
  NABoolean isHbaseCellTable() const { return isHbaseCell_; }
  NABoolean isHbaseRowTable() const { return isHbaseRow_; }
  NABoolean isSeabaseTable() const { return isSeabase_; }
  NABoolean isSeabaseMDTable() const { return isSeabaseMD_; }
  NABoolean isSeabasePrivSchemaTable() const { return isSeabasePrivSchemaTable_; }
  NABoolean isSchemaObject() const {
    return (qualifiedName_.getQualifiedNameObj().getObjectName() == SEABASE_SCHEMA_OBJECTNAME);
  }

  NABoolean isUserUpdatableSeabaseMDTable() const { return isUserUpdatableSeabaseMD_; }

  void setIsORC(NABoolean v) { isORC_ = v; }
  void setIsParquet(NABoolean v) { isParquet_ = v; }
  void setIsAvro(NABoolean v) { isAvro_ = v; }

  void setIsHbaseTable(NABoolean v) { isHbase_ = v; }
  void setIsHbaseCellTable(NABoolean v) { isHbaseCell_ = v; }
  void setIsHbaseRowTable(NABoolean v) { isHbaseRow_ = v; }
  void setIsSeabaseTable(NABoolean v) { isSeabase_ = v; }
  void setIsSeabaseMDTable(NABoolean v) { isSeabaseMD_ = v; }
  void setIsUserUpdatableSeabaseMDTable(NABoolean v) { isUserUpdatableSeabaseMD_ = v; }

  // returns default string length in bytes
  int getHiveDefaultStringLen() const { return hiveDefaultStringLen_; }
  int getHiveTableId() const { return hiveTableId_; }

  void setClearHDFSStatsAfterStmt(NABoolean x) { resetHDFSStatsAfterStmt_ = x; };

  NABoolean getClearHDFSStatsAfterStmt() { return resetHDFSStatsAfterStmt_; };

  NAMemory *getHeap() const { return heap_; }
  NATableHeapType getHeapType() { return heapType_; }

  // Privilege related operations
  PrivMgrDescList *getPrivDescs() { return privDescs_; }
  PrivMgrUserPrivs *getPrivInfo() const { return privInfo_; }
  void setPrivInfo(PrivMgrUserPrivs *privInfo) { privInfo_ = privInfo; }
  ComSecurityKeySet getSecKeySet() { return secKeySet_; }
  void setSecKeySet(ComSecurityKeySet secKeySet) { secKeySet_ = secKeySet; }
  void removePrivInfo();

  // Get the part of the row size that is computable with info we have available
  // without accessing HBase. The result is passed to estimateHBaseRowCount(),
  // which completes the row size calculation with HBase info.
  int computeHBaseRowSizeFromMetaData() const;
  long estimateHBaseRowCount(int retryLimitMilliSeconds, int &errorCode, int &breadCrumb) const;
  NABoolean getHbaseTableInfo(int &hbtIndexLevels, int &hbtBlockSize) const;
  NABoolean getRegionsNodeName(int partns, ARRAY(const char *) & nodeNames) const;

  static NAArray<HbaseStr> *getRegionsBeginKey(const char *extHBaseName);

  NAString &defaultUserColFam() { return defaultUserColFam_; }
  const NAString &defaultUserColFam() const { return defaultUserColFam_; }
  NAString &defaultTrafColFam() { return defaultTrafColFam_; }
  const NAString &defaultTrafColFam() const { return defaultTrafColFam_; }
  NAList<NAString> &allColFams() { return allColFams_; }

  const NAString &getNamespace() { return tableNamespace_; }
  const

      NABoolean
      isMonarch() const {
    return (storageType_ == COM_STORAGE_MONARCH);
  };
  static void getConnectParams(ComStorageType storageType, char **connectParam1, char **connectParam2);

  static ItemExpr *getRangePartitionBoundaryValues(const char *keyValueBuffer, const int keyValueBufferSize,
                                                   NAMemory *heap, CharInfo::CharSet strCharSet = CharInfo::UTF8);

  NAHashDictionary<minmaxKey, minmaxValue> &minmaxCache() { return minmaxCache_; }

  ComEncryption::EncryptionInfo &encryptionInfo() { return encryptionInfo_; }

  char *getLobStorageLocation() { return lobStorageLocation_; }
  const char *getLobStorageLocation() const { return lobStorageLocation_; }
  void setLobStorageLocation(char *loc) { lobStorageLocation_ = loc; }

  int getNumLOBdatafiles() { return numLOBdatafiles_; }
  const int getNumLOBdatafiles() const { return numLOBdatafiles_; }
  void setNumLOBdatafiles(int n) { numLOBdatafiles_ = n; }

  int getLobChunksTableDataInHbaseColLen() { return lobChunksTableDataInHbaseColLen_; }
  const int getLobChunksTableDataInHbaseColLen() const { return lobChunksTableDataInHbaseColLen_; }
  void setLobChunksTableDataInHbaseColLen(int n) { lobChunksTableDataInHbaseColLen_ = n; }

  long &lobHbaseDataMaxLen() { return lobHbaseDataMaxLen_; }
  int &lobInlinedDataMaxLen() { return lobInlinedDataMaxLen_; }

  // Methods to support run-time DDL validation
  NABoolean DDLValidationRequired() const { return ddlValidationRequired_; }
  void setDDLValidationRequired(NABoolean v) { ddlValidationRequired_ = v; }

  NABoolean isUserBaseTable() const { return userBaseTable_; }
  void setUserBaseTable(NABoolean v) { userBaseTable_ = v; }

  UInt32 expectedEpoch() const { return expectedEpoch_; }
  void setExpectedEpoch(UInt32 expectedEpoch) { expectedEpoch_ = expectedEpoch; }

  UInt32 expectedFlags() const { return expectedFlags_; }
  void setExpectedFlags(UInt32 expectedFlags) { expectedFlags_ = expectedFlags; }

  // methods to support partitionV2
  void displayPartitonV2(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "ParitionV2",
                         CollHeap *c = NULL, char *buf = NULL);
  const char *partitionSchemeToString(ComPartitioningSchemeV2 ps);

  void getParitionColNameAsString(NAString &target, NABoolean getSubParType) const;

  const int &getPartitionColCount() const { return partitionColCount_; }
  const int *getPartitionColIdxArray() const { return partitionColIdxAry_; }
  const NAPartitionArray &getPartitionArray() const { return partArray_; }

  int partitionType() { return partitionType_; }
  int subPartitionType() { return subpartitionType_; }

  int FisrtLevelPartitionCount() { return stlPartitionCnt_; }

 protected:
  void setupForStatementAfterCopyCstr();

 private:
  NABoolean getSQLMXAlignedTable() const { return (flags_ & SQLMX_ALIGNED_ROW_TABLE) != 0; }

  // copy ctor
  NATable(const NATable &orig, NAMemory *h = 0);  // not written

  void setRecordLength(int recordLength) { recordLength_ = recordLength; }

  void getPrivileges(TrafDesc *priv_desc, BindWA *bindWA);
  void readPrivileges();
  ExpHbaseInterface *getHBaseInterface() const;
  static ExpHbaseInterface *getHBaseInterfaceRaw(ComStorageType storageType);

  short addEncryptionInfo();

  // size of All NATable related data after construction
  // this is used when NATables are cached and only then
  // is it meaningful as each NATable has its own heap
  int initialSize_;

  // size of All NATable related stuff after the end of the
  // previous statement. This is used when NATables are
  // cached and only then is it meaningful as each NATable
  // has its own heap. This is simply the allocated size
  // on the NATable heap.
  int sizeAfterLastStatement_;

  // -----------------------------------------------------------------------
  // The heap for the dynamic allocation of the NATable members.
  // -----------------------------------------------------------------------
  NAMemory *heap_;

  // The type of heap pointed to by heap_ (i.e. statement, context or other)
  NATableHeapType heapType_;

  // ---------------------------------------------------------------------
  // The number of table references to this table qualified name
  // there are in the entire query.
  // This count includes references in expanded view texts
  // (so includes view with-check-option check constraints),
  // but excludes references in table check constraints or RI constraints.
  // ---------------------------------------------------------------------
  int referenceCount_;

  // ---------------------------------------------------------------------
  // A reference may use access options that are not compatible with the
  // use of the DP2 Locks method of preventing the Halloween problem.
  // ---------------------------------------------------------------------
  NABoolean refsIncompatibleDP2Halloween_;

  // ---------------------------------------------------------------------
  // Helps keep track of which table, if any, in the query is the
  // self-referencing table.
  // ---------------------------------------------------------------------
  NABoolean isHalloweenTable_;

  // Bitfield flags to be used instead of numerous NABoolean fields
  enum Flags {
    UNUSED = 0x00000000,
    SQLMX_ROW_TABLE = 0x00000004,
    SQLMX_ALIGNED_ROW_TABLE = 0x00000008,
    IS_INSERTABLE = 0x00000010,
    IS_UPDATABLE = 0x00000020,
    IS_VERTICAL_PARTITION = 0x00000040,
    HAS_VERTICAL_PARTITIONS = 0x00000080,
    ADDED_COLUMN = 0x00000100,
    VARCHAR_COLUMN = 0x00000200,
    VOLATILE = 0x00000400,
    VOLATILE_MATERIALIZED = 0x00000800,
    SYSTEM_COL_AS_USER_COL = 0x00001000,
    IN_MEM_OBJECT_DEFN = 0x00002000,
    DROPPABLE = 0x00004000,
    REMOVE_FROM_CACHE_BNC = 0x00010000,  // Remove from NATable Cache Before Next Compilation
    SERIALIZED_ENCODED_COLUMN = 0x00020000,
    SERIALIZED_COLUMN = 0x00040000,
    IS_TRAF_EXTERNAL_TABLE = 0x00080000,
    HAS_EXTERNAL_TABLE = 0x00100000,
    IS_HISTOGRAM_TABLE = 0x00200000,
    HBASE_MAP_TABLE = 0x00400000,
    HBASE_DATA_FORMAT_STRING = 0x00800000,
    HAS_HIVE_EXT_TABLE = 0x01000000,
    HIVE_EXT_COL_ATTRS = 0x02000000,
    HIVE_EXT_KEY_ATTRS = 0x04000000,
    IS_IMPLICIT_TRAF_EXT_TABLE = 0x08000000,
    IS_REGISTERED = 0x10000000,
    IS_INTERNAL_REGISTERED = 0x20000000,

    ROWID_ENCRYPT = 0x40000000,
    DATA_ENCRYPT = 0x80000000,
  };

  enum Flags2 {
    STORED_DESC = 0x00000001,
    PRELOADED = 0x00000002,

    // if underlying hive table was created as an EXTERNAL table.
    //  hive syntax: create external table ...)
    //  Note: this is different than a traf external table created for
    //        a hive table.
    IS_HIVE_EXTERNAL_TABLE = 0x00000004,

    // if underlying hive table was not created as an EXTERNAL table.
    IS_HIVE_MANAGED_TABLE = 0x00000008,

    // one or more columns are composite (ARRAY or ROW)
    HAS_COMPOSITE_COLS = 0x00000010,

    // if table exist trigger, set this flag
    HAS_TRIGGER = 0x00000020,

    // lob columns in this table have new format
    LOB_VERSION2 = 0x00000040,

    // is a partitionV2 table
    IS_PARTITIONV2_TABLE = 0x00000080,

    // is a partition table entity
    IS_PARTITION_ENTITY_TABLE = 0x00000100,

    // is a partition v2 local index table
    IS_PARTITIONV2_INDEX_TABLE = 0x00000200,

    // is a partition v2 local index entity table
    IS_PARTITION_ENTITY_INDEX_TABLE = 0x00000400,
  };

  UInt32 flags_;
  UInt32 flags2_;

  // ---------------------------------------------------------------------
  // NORMAL, INDEX, VIRTUAL, etc.
  // ---------------------------------------------------------------------
  // ExtendedQualName::SpecialTableType specialType_;

  // ---------------------------------------------------------------------
  // Extended Qualified name for the table. This also has the specialType
  // (NORMAL, INDEX, VIRTUAL, etc.) and the location Name.
  // ---------------------------------------------------------------------
  ExtendedQualName qualifiedName_;

  // ---------------------------------------------------------------------
  // Save the synonym's reference object name here returned from
  // the catalog manager as well as the UID. If catalog manager has
  // translated a synonym name to its reference objects set the
  // isSynonymTranslationDone_ to TRUE.
  // ---------------------------------------------------------------------
  NAString synonymReferenceName_;
  NABoolean isSynonymTranslationDone_;
  ComUID synonymReferenceObjectUid_;

  // ---------------------------------------------------------------------
  // Fileset for the table (to be augmented later).
  // ---------------------------------------------------------------------
  QualifiedName fileSetName_;

  // ---------------------------------------------------------------------
  // Host Var associated with table, if any. Used by generator to
  // build late-name resolution information for use at execution time.
  // ---------------------------------------------------------------------
  HostVar *prototype_;

  // ---------------------------------------------------------------------
  // Column count
  // ---------------------------------------------------------------------
  CollIndex colcount_;

  // ---------------------------------------------------------------------
  // An array of columns belonging to this table
  // ---------------------------------------------------------------------
  NAColumnArray colArray_;

  // if this hive table has an associated external table, then entries in
  // colArray_ may be replaced by external table entries.
  // Save the original colArray_ in hiveColArray_ before replacing them.
  // This is done in method NATable::updateExtTableAttrs,
  NAColumnArray hiveColArray_;

  // ---------------------------------------------------------------------
  // A dictionary of expressions that occur as children of comparison
  // operators; expressions histograms might be helpful here.
  // ---------------------------------------------------------------------
  NAHashDictionary<NAString, NABoolean> interestingExpressions_;

  // ---------------------------------------------------------------------
  // Record length.
  // ---------------------------------------------------------------------
  int recordLength_;

  // ---------------------------------------------------------------------
  // The clustering key of the base table
  // ---------------------------------------------------------------------
  NAFileSet *clusteringIndex_;

  // ---------------------------------------------------------------------
  // A list of the primary index and the alternate indexes for the table
  // ---------------------------------------------------------------------
  NAFileSetList indexes_;

  // ---------------------------------------------------------------------
  // A list of vertical partitions for the table
  // ---------------------------------------------------------------------
  NAFileSetList vertParts_;

  // ---------------------------------------------------------------------
  // A list of column statistics for this table
  // ---------------------------------------------------------------------
  StatsList *colStats_;
  NABoolean statsFetched_;
  CostScalar originalCardinality_;

  // ---------------------------------------------------------------------
  // Catalog timestamps
  // ---------------------------------------------------------------------
  long createTime_;
  long redefTime_;
  long statsTime_;  // set by NATable::getStatistics()

  // ---------------------------------------------------------------------
  // UIDs
  // ---------------------------------------------------------------------
  ComUID catalogUID_;
  ComUID schemaUID_;
  ComUID objectUID_;
  ComUID objDataUID_;

  // if this NATable if for an index only access, then baseTableUID_
  // is the uid of the base table for that index.
  ComUID baseTableUID_;

  // ---------------------------------------------------------------------
  // Object owner
  // ---------------------------------------------------------------------
  int owner_;

  // ---------------------------------------------------------------------
  // ObjectType
  // ---------------------------------------------------------------------
  ComObjectType objectType_;

  // ---------------------------------------------------------------------
  // Schema of this object owner
  // ---------------------------------------------------------------------
  int schemaOwner_;

  // ---------------------------------------------------------------------
  // Partitioning Scheme - needed for parallel DDL operations.
  // ---------------------------------------------------------------------
  ComPartitioningScheme partitioningScheme_;

  // ---------------------------------------------------------------------
  // Parseable view text (includes terminating semicolon);
  // MP "WITH CHECK OPTION" text;
  // PHYSICAL filename of the view (e.g. \NSK.$VOL.ZSDN0000.AJPP0000
  //   in internal format i.e. no delimiting dquotes);
  // ---------------------------------------------------------------------
  char *viewFileName_;
  char *viewText_;
  NAWString viewTextInNAWchars_;
  CharInfo::CharSet viewTextCharSet_;
  char *viewCheck_;
  NAList<ComViewColUsage *> *viewColUsages_;

  // transaction replication across multiple clusters
  ComReplType xnRepl_;

  // storage engine where data is stored.
  // Currently: HBASE or MONARCH
  ComStorageType storageType_;

  // if incremental backup is enabled on this table.
  NABoolean incrBackupEnabled_;

  NABoolean readOnlyEnabled_;

  // expanded hive view text returned from hive.
  char *hiveExpandedViewText_;

  char *hiveOriginalViewText_;

  // ---------------------------------------------------------------------
  // Flags
  // ---------------------------------------------------------------------
  NABoolean accessedInCurrentStatement_;
  NABoolean setupForStatement_;
  NABoolean resetAfterStatement_;
  NABoolean tableConstructionHadWarnings_;
  NABoolean isAnMPTableWithAnsiName_;
  NABoolean isUMDTable_;
  NABoolean isSMDTable_;
  NABoolean isMVUMDTable_;
  NABoolean isTrigTempTable_;
  NABoolean isExceptionTable_;
  ComInsertMode insertMode_;

  // ---------------------------------------------------------------------
  // List of check constraints (only the unbound text is used;
  // the ItemExpr remains unbound here in the NATable)
  // ---------------------------------------------------------------------
  CheckConstraintList checkConstraints_;

  // ---------------------------------------------------------------------
  // Referential Integrity constraint lists
  // ---------------------------------------------------------------------
  AbstractRIConstraintList uniqueConstraints_;
  AbstractRIConstraintList refConstraints_;

  // MV
  // ---------------------------------------------------------------------
  // Materialized Views support
  // ---------------------------------------------------------------------
  NABoolean isAnMV_;            // Is this table a metarialized view?
  NABoolean isAnMVMetaData_;    // Is this table an MV metadata table?
  UsingMvInfoList mvsUsingMe_;  // List of MVs using this table.
  ComMvAttributeBitmap mvAttributeBitmap_;

  // Caching stats
  UInt32 hitCount_;
  UInt32 replacementCounter_;
  long sizeInCache_;
  NABoolean recentlyUsed_;

  COM_VERSION osv_;
  COM_VERSION ofv_;

  // RCB information, to be used for parallel label operations.
  void *rcb_;
  int rcbLen_;
  int keyLength_;

  char *parentTableName_;

  TrafDesc *columnsDesc_;
  TrafDesc *storedStatsDesc_;

  // hash table to store all the column positions for which missing
  // stats warning has been generated. We are not storing ValueIdSet
  // of the columns but the column positions. This is because for cases
  // where same base table appears in both the query and the sub-query, the
  // warning for the same column can appears twice, since the ValueId
  // for the column will be different in the two places. We don't want
  // that
  NAHashDictionary<CollIndexSet, int> *colsWithMissingStats_;

  // cached table Id list
  LIST(CollIndex) tableIdList_;

  // Sequence generator
  SequenceGeneratorAttributes *sgAttributes_;

  NABoolean isHive_;
  NABoolean isHbase_;
  NABoolean isORC_;
  NABoolean isParquet_;
  NABoolean isAvro_;
  NABoolean isHbaseCell_;
  NABoolean isHbaseRow_;
  NABoolean isSeabase_;
  NABoolean isSeabaseMD_;
  NABoolean isSeabasePrivSchemaTable_;
  NABoolean isUserUpdatableSeabaseMD_;

  NABoolean resetHDFSStatsAfterStmt_;
  int hiveDefaultStringLen_;  // in bytes
  int hiveTableId_;

  // Privilege information for the object
  //   privDescs_ is the list of all grants on the object
  //   privInfo_ are the privs for the current user
  //   secKeySet_ are the security keys for the current user
  PrivMgrDescList *privDescs_;
  PrivMgrUserPrivs *privInfo_;
  ComSecurityKeySet secKeySet_;

  // While creating the index keys, the NAColumn from colArray_
  // is not used in all cases. Sometimes, a new NAColumn is
  // constructured from the NAColumn. The variable below
  // keeps track of these new columnsa allowing us to
  // destroy them when NATable is destroyed.
  NAColumnArray newColumns_;

  NAString defaultUserColFam_;
  NAString defaultTrafColFam_;
  NAList<NAString> allColFams_;

  NAString tableNamespace_;

  NAHashDictionary<minmaxKey, minmaxValue> minmaxCache_;

  ComEncryption::EncryptionInfo encryptionInfo_;

  char *lobStorageLocation_;

  // number of hdfs files created for each lob.
  // Helps with concurrent lob access.
  int numLOBdatafiles_;

  // lob data that can be inlined. Based on cqd traf_lob_inlined_data_maxlen
  int lobInlinedDataMaxLen_;

  // lob data stored in hbase chunks table.
  // Based on cqd traf_lob_hbase_data_maxlen
  long lobHbaseDataMaxLen_;

  // length of DATA_IN_HBASE column in LOBCHUNKS table. For V2 lobs.
  int lobChunksTableDataInHbaseColLen_;

  // Indicate whether some data member is possiblly
  // shallow copied. Set to TRUE when this is copy
  // constructed.
  NABoolean shallowCopied_;

  // Fields needed for DDL validation at runtime
  NABoolean ddlValidationRequired_;  // set to TRUE for non-metadata Trafodion tables
  UInt32 expectedEpoch_;             // RMS ObjectEpochCache expected epoch
  UInt32 expectedFlags_;             // RMS ObjectEpochCache expected flags

  NABoolean userBaseTable_;  // set to TRUE for non-system tables

  /**************************partitionV2*********************/
  int partitionType_;
  int subpartitionType_;
  int partitionColCount_;
  int subpartitionColCount_;
  int *partitionColIdxAry_;
  int *subpartitionColIdxAry_;
  // first level partition count
  int stlPartitionCnt_;
  NAPartitionArray partArray_;

  // reserved
  char *partitionInterval_;
  char *subpartitionInterval_;
  int partitionAutolist_;
  int subpartitionAutolist_;
  long partitionV2Flags_;

  /**************************partitionV2 end*****************/
};  // class NATable

struct NATableCacheStats {
  char contextType[8];
  int numLookups;
  int numCacheHits;
  int preloadedCacheSize;
  int currentCacheSize;
  int defaultCacheSize;
  int maxCacheSize;
  int highWaterMark;
  int numEntries;
};

// ***********************************************************************
// A collection of NATables.
// ***********************************************************************
#define NATableDB_INIT_SIZE 23

class NATableDB : public NAKeyLookup<ExtendedQualName, NATable> {
  friend class NATableCacheStatStoredProcedure;
  friend class NATableCacheDeleteStoredProcedure;

 public:
  NATableDB(NAMemory *h);
  NATableDB(const NATableDB &orig, NAMemory *h)
      : NAKeyLookup<ExtendedQualName, NATable>(orig, h),
        heap_(h),
        statementTableList_(h),
        statementCachedTableList_(h),
        cachedTableList_(h),
        tablesToDeleteAfterStatement_(h),
        nonCacheableTableIdents_(h),
        nonCacheableTableNames_(h),
        inPreloading_(orig.inPreloading_),
        objUidToName_(&ComUID::hashKey, 100, TRUE, h) {}

  NAHeap *getHeap() { return (NAHeap *)heap_; }
  // Obtain a list of table identifiers for the current statement.
  // Allocate the list on the heap passed.
  const LIST(CollIndex) & getStmtTableIdList(NAMemory *heap) const;

  void resetAfterStatement();

  // set an upper limit to the heap used by NATableDB
  void setHeapUpperLimit(size_t newUpperLimit) {
    if (heap_) heap_->setUpperLimit(newUpperLimit);
  }

  NATable *get(const ExtendedQualName *key, BindWA *bindWA = NULL, NABoolean findInCacheOnly = FALSE);
  NATable *get(CorrName &corrName, BindWA *bindWA, TrafDesc *inTableDescStruct, NABoolean writeReference);

  void removeNATable(CorrName &corrName, ComQiScope qiScope, ComObjectType ot, NABoolean ddlXns, NABoolean atCommit,
                     long objUID = 0, NABoolean noCheck = FALSE);

  void RemoveFromNATableCache(NATable *NATablep, UInt32 currIndx);
  void remove_entries_marked_for_removal();
  static void unmark_entries_marked_for_removal();

  void free_entries_with_QI_key(int numSiKeys, SQL_QIKEY *qiKeyArray);
  void free_entries_with_schemaUID(int numKeys, SQL_QIKEY *qiKeyArray);
  void free_schema_entries();
  void reset_priv_entries();
  void free_hive_tables();
  void update_entry_stored_stats_with_QI_key(int numSiKeys, SQL_QIKEY *qiKeyArray);

  void setCachingOFF() {
    cacheMetaData_ = FALSE;
    flushCache();
  }

  void setCachingON();

  inline NABoolean cachingMetaData() { return cacheMetaData_; }

  void refreshCacheInThisStatement() { refreshCacheInThisStatement_ = TRUE; }

  void setUseCache(NABoolean x) { useCache_ = x; }
  void useCache() { useCache_ = TRUE; }
  void dontUseCache() { useCache_ = FALSE; }

  NABoolean usingCache() { return useCache_; }

  NABoolean isSQUtiDisplayExplain(CorrName &corrName);
  NABoolean isSQUmdTable(CorrName &corrName);

  NABoolean isSQInternalStoredProcedure(CorrName &corrName);

  void getCacheStats(NATableCacheStats &stats);

  int preloadedCacheSize() { return preloadedCacheSize_; }
  void setPreloadedCacheSize(int v) { preloadedCacheSize_ = v; }

  int defaultCacheSize() { return defaultCacheSize_; }
  void setDefaultCacheSize(int v) { defaultCacheSize_ = v; }

  int maxCacheSize() { return (preloadedCacheSize_ + defaultCacheSize_); }

  inline int currentCacheSize() { return currentCacheSize_; }
  inline int intervalWaterMark() { return intervalWaterMark_; }
  inline int hits() { return totalCacheHits_; }
  inline int lookups() { return totalLookupsCount_; }

  void resetIntervalWaterMark() { intervalWaterMark_ = currentCacheSize_; }

  // get details of this query cache entry
  void getEntryDetails(int i,                          // (IN) : NATable cache iterator entry
                       NATableEntryDetails &details);  // (OUT): cache entry's details

  NABoolean empty() { return end() == 0; }
  int begin() { return 0; }
  int end();

  static NABoolean isHiveTable(CorrName &corrName);
  NABoolean initHiveStructForHiveTable(CorrName &corrName);

  NAHashDictionary<ComUID, CorrName> &getObjUidToName() { return objUidToName_; }

  NABoolean isInPreloading() { return inPreloading_; }
  void setIsInPreloading(NABoolean x) { inPreloading_ = x; }

  void display();

  // it is used when we need the lastest stored desc
  TrafDesc *getTableDescFromCacheOrText(const ExtendedQualName &extQualName, long objectUid, NAHeap **descHeap = NULL);

 private:
  void flushCache();

  // this method tries to enforce the memory space constraints
  // on the NATable cache, by selectively deleting entries that
  // that are not needed in the current statement. It applies
  // a variant of the LRU algorithm to pick the entries to be
  // deleted.
  NABoolean enforceMemorySpaceConstraints();

  // maximum size of cache specified via default METADATA_CACHE_SIZE
  int defaultCacheSize_;

  // current size of the cache
  int currentCacheSize_;

  // size of cache after metadata preload
  int preloadedCacheSize_;

  // high Watermark of currentCacheSize_ ever reached for statistics
  int highWatermarkCache_;  // High watermark of currentCacheSize_
  int totalLookupsCount_;   // NATable entries lookup counter
  int totalCacheHits_;      // cache hit counter

  int intervalWaterMark_;  // resettable watermark

  // List of tables used during the current statement
  LIST(NATable *) statementTableList_;

  // List of cached tables used during the current statement
  LIST(NATable *) statementCachedTableList_;

  // List of tables in the cache
  LIST(NATable *) cachedTableList_;

  // List of tables to be deleted
  // This list is used to collect
  // tables that are supposed to be
  // deleted during the statement.
  // Deleting during the statement
  // affects compile-time, therefore
  // such tables are collected in a
  // list and deleted after the compiled
  // statement has been sent out to the
  // executor. The delete of the elements
  // in this list is done in
  // NATableDB::resetAfterStatement. This
  // method is called indirectly by the
  // CmpStatement destructor
  LIST(NATable *) tablesToDeleteAfterStatement_;

  // List of names of special tables
  LIST(ExtendedQualName *) nonCacheableTableNames_;
  LIST(CollIndex) nonCacheableTableIdents_;

  NAMemory *heap_;

  // indicates if there is something in cache
  NABoolean metaDataCached_;

  // indicates if NATables are to be cached or not
  NABoolean cacheMetaData_;

  // this flag indicates that the entries (i.e. NATable
  // objects) used during the current statement should
  // be re-read from disk instead of using the entries
  // already in the cache. This helps to refresh the
  // entries used in the current statement.
  NABoolean refreshCacheInThisStatement_;

  // indicates whether to use the NATable cache
  // or not. This TRUE for dynamic sql compiles
  // it is turned 'ON' in CmpMain::sqlcomp
  // it is turned 'OFF' after every statement in
  // NATableDB::resetAfterStatement()
  NABoolean useCache_;

  // pointer to current location in the cachedTableList_
  // used for cache entry replacement purposes
  int replacementCursor_;

  // indicates if NATableDB is in the preloading phase.
  NABoolean inPreloading_;

  NAHashDictionary<ComUID, CorrName> objUidToName_;

};  // class NATableDB

// Remove objects from other users's cache
void removeFromAllUsers(const QualifiedName &objName, ComQiScope qiScope, ComObjectType ot, NASet<long> &objectUIDs,
                        NABoolean ddlXns, NABoolean atCommit, NABoolean noCheck = FALSE);

#endif /* NATABLE_H */
