// memorymonitor

#include "comexe/ComTdbExeUtil.h"

#include "comexe/ComTdbCommon.h"
#include "common/ComSmallDefs.h"
#include "exp/ExpLOBinterface.h"

ComTdbExeUtil::ComTdbExeUtil(int type, char *query, int querylen, Int16 querycharset, char *tableName, int tableNameLen,
                             ex_expr *input_expr, int input_rowlen, ex_expr *output_expr, int output_rowlen,
                             ex_expr_base *scan_expr, ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                             ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
                             queue_index up, int num_buffers, int buffer_size)
    : ComTdbGenericUtil(query, querylen, querycharset, tableName, tableNameLen, input_expr, input_rowlen, output_expr,
                        output_rowlen, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up,
                        num_buffers, buffer_size),
      type_(type),
      child_(NULL),
      scanExpr_(scan_expr),
      flags_(0),
      explOptionsStr_(NULL),
      lobDmlOptions_(NULL),
      tableNameForUID_(NULL),
      replaceNameByUID_(FALSE) {
  setNodeType(ComTdb::ex_EXE_UTIL);
}

Long ComTdbExeUtil::pack(void *space) {
  child_.pack(space);
  scanExpr_.pack(space);
  if (explOptionsStr_) explOptionsStr_.pack(space);
  if (NEOCatalogName_) NEOCatalogName_.pack(space);
  if (lobDmlOptions_) lobDmlOptions_.pack(space);
  if (tableNameForUID_) tableNameForUID_.pack(space);
  return ComTdbGenericUtil::pack(space);
}

int ComTdbExeUtil::unpack(void *base, void *reallocator) {
  if (child_.unpack(base, reallocator)) return -1;
  if (scanExpr_.unpack(base, reallocator)) return -1;
  if (explOptionsStr_.unpack(base)) return -1;
  if (NEOCatalogName_.unpack(base)) return -1;
  if (lobDmlOptions_.unpack(base)) return -1;
  if (tableNameForUID_.unpack(base)) return -1;
  return ComTdbGenericUtil::unpack(base, reallocator);
}

const ComTdb *ComTdbExeUtil::getChild(int pos) const {
  if (pos == 0)
    return child_;
  else
    return NULL;
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilDisplayExplain
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilDisplayExplain::ComTdbExeUtilDisplayExplain(
    char *query, int querylen, Int16 querycharset, char *moduleName, char *stmtName, ex_expr *input_expr,
    int input_rowlen, ex_expr *output_expr, int output_rowlen, ex_cri_desc *work_cri_desc,
    const unsigned short work_atp_index, int colDescSize, int outputRowSize, ex_cri_desc *given_cri_desc,
    ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::DISPLAY_EXPLAIN_, query, querylen, querycharset, NULL, 0, input_expr, input_rowlen,
                    output_expr, output_rowlen, NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc,
                    down, up, num_buffers, buffer_size),
      moduleName_(moduleName),
      stmtName_(stmtName),
      colDescSize_(colDescSize),
      outputRowSize_(outputRowSize),
      flags_(0) {
  setNodeType(ComTdb::ex_DISPLAY_EXPLAIN);
}

Long ComTdbExeUtilDisplayExplain::pack(void *space) {
  if (moduleName_) moduleName_.pack(space);
  if (stmtName_) stmtName_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilDisplayExplain::unpack(void *base, void *reallocator) {
  if (moduleName_.unpack(base)) return -1;
  if (stmtName_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilDisplayExplain::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbExeUtilDisplayExplain :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "optionN = %d, optionF = %d, optionC = %d, optionP = %d, optionE = %d, optionM = %d", isOptionN(),
                isOptionF(), isOptionC(), isOptionP(), isOptionE(), isOptionM());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilDisplayExplainComplex
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilDisplayExplainComplex::ComTdbExeUtilDisplayExplainComplex(
    int explainType, char *qry1, char *qry2, char *qry3, char *qry4, char *objectName, int objectNameLen,
    ex_expr *input_expr, int input_rowlen, ex_expr *output_expr, int output_rowlen, ex_cri_desc *work_cri_desc,
    const unsigned short work_atp_index, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
    queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::DISPLAY_EXPLAIN_COMPLEX_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, objectName,
                    objectNameLen, input_expr, input_rowlen, output_expr, output_rowlen, NULL, work_cri_desc,
                    work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers, buffer_size),
      explainType_(explainType),
      qry1_(qry1),
      qry2_(qry2),
      qry3_(qry3),
      qry4_(qry4),
      flags_(0) {
  setNodeType(ComTdb::ex_DISPLAY_EXPLAIN_COMPLEX);
}

Long ComTdbExeUtilDisplayExplainComplex::pack(void *space) {
  if (qry1_) qry1_.pack(space);

  if (qry2_) qry2_.pack(space);

  if (qry3_) qry3_.pack(space);

  if (qry4_) qry4_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilDisplayExplainComplex::unpack(void *base, void *reallocator) {
  if (qry1_.unpack(base)) return -1;
  if (qry2_.unpack(base)) return -1;
  if (qry3_.unpack(base)) return -1;
  if (qry4_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilDisplayExplainComplex::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];

    str_sprintf(buf, "\nFor ComTdbExeUtilDisplayExplainComplex :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (qry1_) {
      char query[400];
      if (strlen(qry1_) > 390) {
        strncpy(query, qry1_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, qry1_);

      str_sprintf(buf, "Qry1 = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (qry2_) {
      char query[400];
      if (strlen(qry2_) > 390) {
        strncpy(query, qry2_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, qry2_);

      str_sprintf(buf, "Qry2 = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (qry3_) {
      char query[400];
      if (strlen(qry3_) > 390) {
        strncpy(query, qry3_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, qry3_);

      str_sprintf(buf, "Qry3 = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (qry4_) {
      char query[400];
      if (strlen(qry4_) > 390) {
        strncpy(query, qry4_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, qry4_);

      str_sprintf(buf, "Qry4 = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (objectName_) {
      str_sprintf(buf, "ObjectName = %s ", getObjectName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

//////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilMaintainObject
//
//////////////////////////////////////////////////////////////////////////
ComTdbExeUtilMaintainObject::ComTdbExeUtilMaintainObject(
    char *objectName, int objectNameLen, char *schemaName, int schemaNameLen, UInt16 ot, char *parentTableName,
    int parentTableNameLen, ex_expr *input_expr, int input_rowlen, ex_expr *output_expr, int output_rowlen,
    ex_cri_desc *work_cri_desc, const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
    ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::MAINTAIN_OBJECT_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, objectName, objectNameLen,
                    input_expr, input_rowlen, output_expr, output_rowlen, NULL, work_cri_desc, work_atp_index,
                    given_cri_desc, returned_cri_desc, down, up, num_buffers, buffer_size),
      ot_(ot),
      schemaName_(schemaName),
      schemaNameLen_(schemaNameLen),
      parentTableName_(parentTableName),
      parentTableNameLen_(parentTableNameLen),
      flags_(0),
      controlFlags_(0),
      controlFlags2_(0),
      formatFlags_(0),
      maintainedTableCreateTime_(0),
      parentTableObjectUID_(0),
      from_(0),
      to_(0),
      flags2_(0) {
  setNodeType(ComTdb::ex_MAINTAIN_OBJECT);
}

Long ComTdbExeUtilMaintainObject::pack(void *space) {
  reorgTableOptions_.pack(space);
  reorgIndexOptions_.pack(space);
  updStatsTableOptions_.pack(space);
  updStatsMvlogOptions_.pack(space);
  updStatsMvsOptions_.pack(space);
  updStatsMvgroupOptions_.pack(space);
  refreshMvgroupOptions_.pack(space);
  refreshMvsOptions_.pack(space);
  reorgMvgroupOptions_.pack(space);
  reorgMvsOptions_.pack(space);
  reorgMvsIndexOptions_.pack(space);
  cleanMaintainCITOptions_.pack(space);

  indexList_.pack(space);
  refreshMvgroupList_.pack(space);
  refreshMvsList_.pack(space);
  reorgMvgroupList_.pack(space);
  reorgMvsList_.pack(space);
  reorgMvsIndexList_.pack(space);
  updStatsMvgroupList_.pack(space);
  updStatsMvsList_.pack(space);
  multiTablesNamesList_.pack(space);
  multiTablesCreateTimeList_.pack(space);
  skippedMultiTablesNamesList_.pack(space);

  if (parentTableName_) parentTableName_.pack(space);
  if (schemaName_) schemaName_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilMaintainObject::unpack(void *base, void *reallocator) {
  if (reorgTableOptions_.unpack(base)) return -1;
  if (reorgIndexOptions_.unpack(base)) return -1;
  if (updStatsTableOptions_.unpack(base)) return -1;
  if (updStatsMvlogOptions_.unpack(base)) return -1;
  if (updStatsMvsOptions_.unpack(base)) return -1;
  if (updStatsMvgroupOptions_.unpack(base)) return -1;
  if (refreshMvgroupOptions_.unpack(base)) return -1;
  if (refreshMvsOptions_.unpack(base)) return -1;
  if (reorgMvgroupOptions_.unpack(base)) return -1;
  if (reorgMvsOptions_.unpack(base)) return -1;
  if (reorgMvsIndexOptions_.unpack(base)) return -1;
  if (cleanMaintainCITOptions_.unpack(base)) return -1;

  if (indexList_.unpack(base, reallocator)) return -1;
  if (refreshMvgroupList_.unpack(base, reallocator)) return -1;
  if (refreshMvsList_.unpack(base, reallocator)) return -1;
  if (reorgMvgroupList_.unpack(base, reallocator)) return -1;
  if (reorgMvsList_.unpack(base, reallocator)) return -1;
  if (reorgMvsIndexList_.unpack(base, reallocator)) return -1;
  if (updStatsMvgroupList_.unpack(base, reallocator)) return -1;
  if (updStatsMvsList_.unpack(base, reallocator)) return -1;
  if (multiTablesNamesList_.unpack(base, reallocator)) return -1;
  if (multiTablesCreateTimeList_.unpack(base, reallocator)) return -1;
  if (skippedMultiTablesNamesList_.unpack(base, reallocator)) return -1;

  if (parentTableName_.unpack(base)) return -1;
  if (schemaName_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilMaintainObject::setParams(NABoolean reorgTable, NABoolean reorgIndex, NABoolean updStatsTable,
                                            NABoolean updStatsMvlog, NABoolean updStatsMvs, NABoolean updStatsMvgroup,
                                            NABoolean refreshMvgroup, NABoolean refreshMvs, NABoolean reorgMvgroup,
                                            NABoolean reorgMvs, NABoolean reorgMvsIndex, NABoolean continueOnError,
                                            NABoolean cleanMaintainCIT, NABoolean getSchemaLabelStats,
                                            NABoolean getLabelStats, NABoolean getTableLabelStats,
                                            NABoolean getIndexLabelStats, NABoolean getLabelStatsIncIndexes,
                                            NABoolean getLabelStatsIncInternal, NABoolean getLabelStatsIncRelated) {
  setReorgTable(reorgTable);
  setReorgIndex(reorgIndex);
  setUpdStatsTable(updStatsTable);
  setUpdStatsMvlog(updStatsMvlog);
  setUpdStatsMvs(updStatsMvs);
  setUpdStatsMvgroup(updStatsMvgroup);
  setRefreshMvgroup(refreshMvgroup);
  setRefreshMvs(refreshMvs);
  setReorgMvgroup(reorgMvgroup);
  setReorgMvs(reorgMvs);
  setReorgMvsIndex(reorgMvsIndex);
  setContinueOnError(continueOnError);
  setCleanMaintainCIT(cleanMaintainCIT);
  setSchemaLabelStats(getSchemaLabelStats);
  setTableLabelStats(getTableLabelStats);
  setIndexLabelStats(getIndexLabelStats);
  setLabelStatsIncIndexes(getLabelStatsIncIndexes);
  setLabelStatsIncInternal(getLabelStatsIncInternal);
  setLabelStatsIncRelated(getLabelStatsIncRelated);
}

void ComTdbExeUtilMaintainObject::setOptionsParams(char *reorgTableOptions, char *reorgIndexOptions,
                                                   char *updStatsTableOptions, char *updStatsMvlogOptions,
                                                   char *updStatsMvsOptions, char *updStatsMvgroupOptions,
                                                   char *refreshMvgroupOptions, char *refreshMvsOptions,
                                                   char *reorgMvgroupOptions, char *reorgMvsOptions,
                                                   char *reorgMvsIndexOptions, char *cleanMaintainCITOptions) {
  reorgTableOptions_ = reorgTableOptions;
  reorgIndexOptions_ = reorgIndexOptions;
  updStatsTableOptions_ = updStatsTableOptions;
  updStatsMvlogOptions_ = updStatsMvlogOptions;
  updStatsMvsOptions_ = updStatsMvsOptions;
  updStatsMvgroupOptions_ = updStatsMvgroupOptions;
  refreshMvgroupOptions_ = refreshMvgroupOptions;
  refreshMvsOptions_ = refreshMvsOptions;
  reorgMvgroupOptions_ = reorgMvgroupOptions;
  reorgMvsOptions_ = reorgMvsOptions;
  reorgMvsIndexOptions_ = reorgMvsIndexOptions;
  cleanMaintainCITOptions_ = cleanMaintainCITOptions;
}

void ComTdbExeUtilMaintainObject::setLists(Queue *indexList, Queue *refreshMvgroupList, Queue *refreshMvsList,
                                           Queue *reorgMvgroupList, Queue *reorgMvsList, Queue *reorgMvsIndexList,
                                           Queue *updStatsMvgroupList, Queue *updStatsMvsList,
                                           Queue *multiTablesNamesList, Queue *skippedMultiTablesNamesList) {
  indexList_ = indexList;
  refreshMvgroupList_ = refreshMvgroupList;
  refreshMvsList_ = refreshMvsList;
  reorgMvgroupList_ = reorgMvgroupList;
  reorgMvsList_ = reorgMvsList;
  reorgMvsIndexList_ = reorgMvsIndexList;
  updStatsMvgroupList_ = updStatsMvgroupList;
  updStatsMvsList_ = updStatsMvsList;
  multiTablesNamesList_ = multiTablesNamesList;
  skippedMultiTablesNamesList_ = skippedMultiTablesNamesList;
}

void ComTdbExeUtilMaintainObject::setControlParams(
    NABoolean disableReorgTable, NABoolean enableReorgTable, NABoolean disableReorgIndex, NABoolean enableReorgIndex,
    NABoolean disableUpdStatsTable, NABoolean enableUpdStatsTable, NABoolean disableUpdStatsMvs,
    NABoolean enableUpdStatsMvs, NABoolean disableRefreshMvs, NABoolean enableRefreshMvs, NABoolean disableReorgMvs,
    NABoolean enableReorgMvs, NABoolean resetReorgTable, NABoolean resetUpdStatsTable, NABoolean resetUpdStatsMvs,
    NABoolean resetRefreshMvs, NABoolean resetReorgMvs, NABoolean resetReorgIndex, NABoolean enableUpdStatsMvlog,
    NABoolean disableUpdStatsMvlog, NABoolean resetUpdStatsMvlog, NABoolean enableReorgMvsIndex,
    NABoolean disableReorgMvsIndex, NABoolean resetReorgMvsIndex, NABoolean enableRefreshMvgroup,
    NABoolean disableRefreshMvgroup, NABoolean resetRefreshMvgroup, NABoolean enableReorgMvgroup,
    NABoolean disableReorgMvgroup, NABoolean resetReorgMvgroup, NABoolean enableUpdStatsMvgroup,
    NABoolean disableUpdStatsMvgroup, NABoolean resetUpdStatsMvgroup, NABoolean enableTableLabelStats,
    NABoolean disableTableLabelStats, NABoolean resetTableLabelStats, NABoolean enableIndexLabelStats,
    NABoolean disableIndexLabelStats, NABoolean resetIndexLabelStats) {
  setDisableReorgTable(disableReorgTable);
  setDisableReorgIndex(disableReorgIndex);
  setDisableUpdStatsTable(disableUpdStatsTable);
  setDisableUpdStatsMvs(disableUpdStatsMvs);
  setDisableRefreshMvs(disableRefreshMvs);
  setDisableReorgMvs(disableReorgMvs);
  setEnableReorgTable(enableReorgTable);
  setEnableReorgIndex(enableReorgIndex);
  setEnableUpdStatsTable(enableUpdStatsTable);
  setEnableUpdStatsMvs(enableUpdStatsMvs);
  setEnableRefreshMvs(enableRefreshMvs);
  setEnableReorgMvs(enableReorgMvs);
  setResetReorgTable(resetReorgTable);
  setResetUpdStatsTable(resetUpdStatsTable);
  setResetUpdStatsMvs(resetUpdStatsMvs);
  setResetRefreshMvs(resetRefreshMvs);
  setResetReorgMvs(resetReorgMvs);
  setResetReorgIndex(resetReorgIndex);
  setEnableUpdStatsMvlog(enableUpdStatsMvlog);
  setDisableUpdStatsMvlog(disableUpdStatsMvlog);
  setResetUpdStatsMvlog(resetUpdStatsMvlog);
  setEnableReorgMvsIndex(enableReorgMvsIndex);
  setDisableReorgMvsIndex(disableReorgMvsIndex);
  setResetReorgMvsIndex(resetReorgMvsIndex);
  setEnableRefreshMvgroup(enableRefreshMvgroup);
  setDisableRefreshMvgroup(disableRefreshMvgroup);
  setResetRefreshMvgroup(resetRefreshMvgroup);
  setEnableReorgMvgroup(enableReorgMvgroup);
  setDisableReorgMvgroup(disableReorgMvgroup);
  setResetReorgMvgroup(resetReorgMvgroup);
  setEnableUpdStatsMvgroup(enableUpdStatsMvgroup);
  setDisableUpdStatsMvgroup(disableUpdStatsMvgroup);
  setResetUpdStatsMvgroup(resetUpdStatsMvgroup);
  setEnableTableLabelStats(enableTableLabelStats);
  setDisableTableLabelStats(disableTableLabelStats);
  setResetTableLabelStats(resetTableLabelStats);
  setEnableIndexLabelStats(enableIndexLabelStats);
  setDisableIndexLabelStats(disableIndexLabelStats);
  setResetIndexLabelStats(resetIndexLabelStats);
}

void ComTdbExeUtilMaintainObject::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilMaintainObject :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilLoadVolatileTable
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilLoadVolatileTable::ComTdbExeUtilLoadVolatileTable(
    char *tableName, int tableNameLen, char *insertQuery, char *updStatsQuery, Int16 queryCharSet, long threshold,
    ex_cri_desc *work_cri_desc, const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
    ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::LOAD_VOLATILE_TABLE_, NULL, 0, queryCharSet /*for insertQuery & updStatsQuery*/,
                    tableName, tableNameLen, NULL, 0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      insertQuery_(insertQuery),
      updStatsQuery_(updStatsQuery),
      threshold_(threshold),
      flags_(0) {
  setNodeType(ComTdb::ex_LOAD_VOLATILE_TABLE);
}

Long ComTdbExeUtilLoadVolatileTable::pack(void *space) {
  if (insertQuery_) insertQuery_.pack(space);
  if (updStatsQuery_) updStatsQuery_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilLoadVolatileTable::unpack(void *base, void *reallocator) {
  if (insertQuery_.unpack(base)) return -1;
  if (updStatsQuery_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilLoadVolatileTable::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilLoadVolatileTable :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (insertQuery_) {
      char query[400];
      if (strlen(insertQuery_) > 390) {
        strncpy(query, insertQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, insertQuery_);

      str_sprintf(buf, "Insert Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (updStatsQuery_) {
      char query[400];
      if (strlen(updStatsQuery_) > 390) {
        strncpy(query, updStatsQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, updStatsQuery_);

      str_sprintf(buf, "UpdStats Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    str_sprintf(buf, "Threshold = %ld ", threshold_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilCleanupVolatileTables
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilCleanupVolatileTables::ComTdbExeUtilCleanupVolatileTables(char *catName, int catNameLen,
                                                                       ex_cri_desc *work_cri_desc,
                                                                       const unsigned short work_atp_index,
                                                                       ex_cri_desc *given_cri_desc,
                                                                       ex_cri_desc *returned_cri_desc, queue_index down,
                                                                       queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::CLEANUP_VOLATILE_SCHEMA_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, catName,
                    catNameLen, NULL, 0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      flags_(0) {
  setNodeType(ComTdb::ex_CLEANUP_VOLATILE_TABLES);
}

void ComTdbExeUtilCleanupVolatileTables::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilCleanupVolatileTables :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilGetVotalileInfo
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetVolatileInfo::ComTdbExeUtilGetVolatileInfo(char *param1, char *param2, ex_cri_desc *work_cri_desc,
                                                           const unsigned short work_atp_index,
                                                           ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                           queue_index down, queue_index up, int num_buffers,
                                                           int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::GET_VOLATILE_INFO, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0,
                    NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      param1_(param1),
      param2_(param2) {
  setNodeType(ComTdb::ex_GET_VOLATILE_INFO);
}

Long ComTdbExeUtilGetVolatileInfo::pack(void *space) {
  if (param1_) param1_.pack(space);
  if (param2_) param2_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilGetVolatileInfo::unpack(void *base, void *reallocator) {
  if (param1_.unpack(base)) return -1;
  if (param2_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilGetVolatileInfo::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetVotalileInfo :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilGetErrorInfo
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetErrorInfo::ComTdbExeUtilGetErrorInfo(int errType, int errNum, ex_cri_desc *work_cri_desc,
                                                     const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                                     ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                                     int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::GET_ERROR_INFO_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0,
                    NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      errNum_(errNum),
      errorType_(errType) {
  setNodeType(ComTdb::ex_GET_ERROR_INFO);
}

void ComTdbExeUtilGetErrorInfo::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetErrorInfo :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "ErrorNum = %d ", errNum_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilCreateTableAs
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilCreateTableAs::ComTdbExeUtilCreateTableAs(char *tableName, int tableNameLen, char *ctQuery, char *siQuery,
                                                       char *viQuery, char *usQuery, long threshold,
                                                       ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                                       ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                       queue_index down, queue_index up, int num_buffers,
                                                       int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::CREATE_TABLE_AS_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, tableName, tableNameLen,
                    NULL, 0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up,
                    num_buffers, buffer_size),
      ctQuery_(ctQuery),
      siQuery_(siQuery),
      viQuery_(viQuery),
      usQuery_(usQuery),
      threshold_(threshold),
      flags_(0) {
  setNodeType(ComTdb::ex_CREATE_TABLE_AS);
}

Long ComTdbExeUtilCreateTableAs::pack(void *space) {
  if (ctQuery_) ctQuery_.pack(space);
  if (siQuery_) siQuery_.pack(space);
  if (viQuery_) viQuery_.pack(space);
  if (usQuery_) usQuery_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilCreateTableAs::unpack(void *base, void *reallocator) {
  if (ctQuery_.unpack(base)) return -1;
  if (siQuery_.unpack(base)) return -1;
  if (viQuery_.unpack(base)) return -1;
  if (usQuery_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilCreateTableAs::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilCreateTableAs :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (ctQuery_) {
      char query[400];
      if (strlen(ctQuery_) > 390) {
        strncpy(query, ctQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, ctQuery_);

      str_sprintf(buf, "Create Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (siQuery_) {
      char query[400];
      if (strlen(siQuery_) > 390) {
        strncpy(query, siQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, siQuery_);

      str_sprintf(buf, "Sidetree Insert Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (viQuery_) {
      char query[400];
      if (strlen(viQuery_) > 390) {
        strncpy(query, viQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, viQuery_);

      str_sprintf(buf, "VSBB Insert Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (usQuery_) {
      char query[400];
      if (strlen(usQuery_) > 390) {
        strncpy(query, usQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, usQuery_);

      str_sprintf(buf, "UpdStats Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

// Methods for class ComTdbExeUtilGetStatistics
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetStatistics::ComTdbExeUtilGetStatistics(char *stmtName, short statsReqType, short statsMergeType,
                                                       short activeQueryNum, ex_cri_desc *work_cri_desc,
                                                       const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                                       ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                                       int num_buffers, int buffer_size, char *host, int port,
                                                       char *path, UInt64 queryHash)
    : ComTdbExeUtil(ComTdbExeUtil::GET_STATISTICS_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0,
                    NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      stmtName_(stmtName),
      statsReqType_(statsReqType),
      statsMergeType_(statsMergeType),
      activeQueryNum_(activeQueryNum),
      host_(host),
      port_(port),
      path_(path),
      queryHash_(queryHash) {
  setNodeType(ComTdb::ex_GET_STATISTICS);
}

Long ComTdbExeUtilGetObjectEpochStats::pack(void *space) { return ComTdbExeUtil::pack(space); }

int ComTdbExeUtilGetObjectEpochStats::unpack(void *base, void *reallocator) {
  return ComTdbExeUtil::unpack(base, reallocator);
}

Long ComTdbExeUtilGetObjectLockStats::pack(void *space) { return ComTdbExeUtil::pack(space); }

int ComTdbExeUtilGetObjectLockStats::unpack(void *base, void *reallocator) {
  return ComTdbExeUtil::unpack(base, reallocator);
}

Long ComTdbExeUtilGetStatistics::pack(void *space) {
  if (stmtName_) stmtName_.pack(space);

  if (host_) host_.pack(space);

  if (path_) path_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilGetStatistics::unpack(void *base, void *reallocator) {
  if (stmtName_.unpack(base)) return -1;

  if (host_.unpack(base)) return -1;

  if (path_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilGetStatistics::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetStatistics :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (stmtName_ != (NABasicPtr)NULL) {
      str_sprintf(buf, "StmtName = %s ", getStmtName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

void ComTdbExeUtilGetProcessStatistics::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetProcessStatistics :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (stmtName_ != (NABasicPtr)NULL) {
      str_sprintf(buf, "Pid = %s ", getPid());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilGetUID
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetUID::ComTdbExeUtilGetUID(long uid, ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                         ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
                                         queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::GET_UID_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0, NULL,
                    work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      uid_(uid) {
  setNodeType(ComTdb::ex_GET_UID);
}

Long ComTdbExeUtilGetUID::pack(void *space) { return ComTdbExeUtil::pack(space); }

int ComTdbExeUtilGetUID::unpack(void *base, void *reallocator) { return ComTdbExeUtil::unpack(base, reallocator); }

void ComTdbExeUtilGetUID::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetUID :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "UID = %ld", uid_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilGetQID
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetQID::ComTdbExeUtilGetQID(char *stmtName, ex_cri_desc *work_cri_desc,
                                         const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                         ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                         int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::GET_QID_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0, NULL,
                    work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      stmtName_(stmtName) {
  setNodeType(ComTdb::ex_GET_QID);
}

Long ComTdbExeUtilGetQID::pack(void *space) {
  if (stmtName_) stmtName_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilGetQID::unpack(void *base, void *reallocator) {
  if (stmtName_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilGetQID::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetQID :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "stmtName_ = %s ", getStmtName());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilPopulateInMemStats
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilPopulateInMemStats::ComTdbExeUtilPopulateInMemStats(
    long uid, char *inMemHistogramsTableName, char *inMemHistintsTableName, char *sourceTableCatName,
    char *sourceTableSchName, char *sourceTableObjName, char *sourceHistogramsTableName, char *sourceHistintsTableName,
    ex_cri_desc *work_cri_desc, const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
    ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::POP_IN_MEM_STATS_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0,
                    NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      flags_(0),
      uid_(uid),
      inMemHistogramsTableName_(inMemHistogramsTableName),
      inMemHistintsTableName_(inMemHistintsTableName),
      sourceTableCatName_(sourceTableCatName),
      sourceTableSchName_(sourceTableSchName),
      sourceTableObjName_(sourceTableObjName),
      sourceHistogramsTableName_(sourceHistogramsTableName),
      sourceHistintsTableName_(sourceHistintsTableName) {
  setNodeType(ComTdb::ex_POP_IN_MEM_STATS);
}

Long ComTdbExeUtilPopulateInMemStats::pack(void *space) {
  if (inMemHistogramsTableName_) inMemHistogramsTableName_.pack(space);
  if (inMemHistintsTableName_) inMemHistintsTableName_.pack(space);
  if (sourceTableCatName_) sourceTableCatName_.pack(space);
  if (sourceTableSchName_) sourceTableSchName_.pack(space);
  if (sourceTableObjName_) sourceTableObjName_.pack(space);
  if (sourceHistogramsTableName_) sourceHistogramsTableName_.pack(space);
  if (sourceHistintsTableName_) sourceHistintsTableName_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilPopulateInMemStats::unpack(void *base, void *reallocator) {
  if (inMemHistogramsTableName_.unpack(base)) return -1;
  if (inMemHistintsTableName_.unpack(base)) return -1;
  if (sourceTableCatName_.unpack(base)) return -1;
  if (sourceTableSchName_.unpack(base)) return -1;
  if (sourceTableObjName_.unpack(base)) return -1;
  if (sourceHistogramsTableName_.unpack(base)) return -1;
  if (sourceHistintsTableName_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilPopulateInMemStats::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilPopulateInMemStats :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "UID = %ld", uid_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if ((char *)inMemHistogramsTableName_ != (char *)NULL) {
      str_sprintf(buf, "inMemHistogramsTableName_ = %s ", getInMemHistogramsTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if ((char *)inMemHistintsTableName_ != (char *)NULL) {
      str_sprintf(buf, "inMemHistintsTableName_ = %s ", getInMemHistintsTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if ((char *)sourceTableCatName_ != (char *)NULL) {
      str_sprintf(buf, "sourceTableCatName_ = %s ", getSourceTableCatName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if ((char *)sourceTableSchName_ != (char *)NULL) {
      str_sprintf(buf, "sourceTableSchName_ = %s ", getSourceTableSchName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if ((char *)sourceTableObjName_ != (char *)NULL) {
      str_sprintf(buf, "sourceTableCatName_ = %s ", getSourceTableObjName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if ((char *)sourceHistogramsTableName_ != (char *)NULL) {
      str_sprintf(buf, "sourceHistogramsTableName_ = %s ", getSourceHistogramsTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if ((char *)sourceHistintsTableName_ != (char *)NULL) {
      str_sprintf(buf, "sourceHistintsTableName_ = %s ", getSourceHistintsTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilAqrWnrInsert
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilAqrWnrInsert::ComTdbExeUtilAqrWnrInsert(char *tableName, int tableNameLen, ex_cri_desc *work_cri_desc,
                                                     const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                                     ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                                     int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::AQR_WNR_INSERT_, (char *)eye_AQR_WNR_INS, 0, (Int16)SQLCHARSETCODE_UNKNOWN,
                    tableName, tableNameLen, NULL, 0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      aqrWnrInsflags_(0) {
  setNodeType(ComTdb::ex_ARQ_WNR_INSERT);
}

void ComTdbExeUtilAqrWnrInsert::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilAqrWnrInsert:");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    str_sprintf(buf, "Lock target = %s ", doLockTarget() ? "ON" : "OFF");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
//////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilLongRunning
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilLongRunning::ComTdbExeUtilLongRunning(char *tableName, int tableNameLen, ex_cri_desc *work_cri_desc,
                                                   const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                                   ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                                   int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::LONG_RUNNING_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, tableName, tableNameLen, NULL,
                    0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up,
                    num_buffers, buffer_size),
      flags_(0),
      lruStmt_(NULL),
      lruStmtLen_(0),
      lruStmtWithCK_(NULL),
      lruStmtWithCKLen_(0),
      predicate_(NULL),
      predicateLen_(0),
      multiCommitSize_(0),
      defaultSchemaName_(NULL),
      defaultCatalogName_(NULL) {
  setNodeType(ComTdb::ex_LONG_RUNNING);
}

Long ComTdbExeUtilLongRunning::pack(void *space) {
  if (lruStmt_) lruStmt_.pack(space);

  if (lruStmtWithCK_) lruStmtWithCK_.pack(space);

  if (predicate_) predicate_.pack(space);

  if (defaultSchemaName_) defaultSchemaName_.pack(space);

  if (defaultCatalogName_) defaultCatalogName_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilLongRunning::unpack(void *base, void *reallocator) {
  if (lruStmt_.unpack(base)) return -1;

  if (lruStmtWithCK_.unpack(base)) return -1;

  if (predicate_.unpack(base)) return -1;

  if (defaultSchemaName_.unpack(base)) return -1;

  if (defaultCatalogName_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilLongRunning::setPredicate(Space *space, char *predicate) {
  if (predicate != NULL) {
    predicateLen_ = strlen(predicate) - 5;  // skip "WHERE"
    predicate_ = space->allocateAlignedSpace((int)predicateLen_ + 1);
    strcpy(predicate_, &predicate[5]);
  }
}

void ComTdbExeUtilLongRunning::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilLongRunning :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilGetMetadataInfo
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilGetMetadataInfo::ComTdbExeUtilGetMetadataInfo(QueryType queryType, char *cat, char *sch, char *obj,
                                                           char *pattern, char *param1, ex_expr_base *scan_expr,
                                                           ex_cri_desc *work_cri_desc,
                                                           const unsigned short work_atp_index,
                                                           ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                           queue_index down, queue_index up, int num_buffers,
                                                           int buffer_size, char *server, char *zkPort)
    : ComTdbExeUtil(ComTdbExeUtil::GET_METADATA_INFO_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL,
                    0, scan_expr, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up,
                    num_buffers, buffer_size),
      queryType_(queryType),
      cat_(cat),
      sch_(sch),
      obj_(obj),
      pattern_(pattern),
      param1_(param1),
      flags_(0),
      server_(server),
      zkPort_(zkPort) {
  setNodeType(ComTdb::ex_GET_METADATA_INFO);
}

Long ComTdbExeUtilGetMetadataInfo::pack(void *space) {
  if (cat_) cat_.pack(space);
  if (sch_) sch_.pack(space);
  if (obj_) obj_.pack(space);
  if (pattern_) pattern_.pack(space);
  if (param1_) param1_.pack(space);
  if (server_) server_.pack(space);
  if (zkPort_) zkPort_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilGetMetadataInfo::unpack(void *base, void *reallocator) {
  if (cat_.unpack(base)) return -1;
  if (sch_.unpack(base)) return -1;
  if (obj_.unpack(base)) return -1;
  if (pattern_.unpack(base)) return -1;
  if (param1_.unpack(base)) return -1;
  if (server_.unpack(base)) return -1;
  if (zkPort_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilGetMetadataInfo::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilGetMetadataInfo :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "QueryType: %d", queryType_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getCat() != NULL) {
      str_sprintf(buf, "Catalog = %s ", getCat());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (getSch() != NULL) {
      str_sprintf(buf, "Schema = %s ", getSch());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (getObj() != NULL) {
      str_sprintf(buf, "Object = %s ", getObj());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (getPattern() != NULL) {
      str_sprintf(buf, "Pattern = %s ", getPattern());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (getParam1() != NULL) {
      str_sprintf(buf, "Param1 = %s ", getParam1());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    str_sprintf(buf, "Flags = %x", flags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilShowSet
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilShowSet::ComTdbExeUtilShowSet(UInt16 type, char *param1, char *param2, ex_cri_desc *work_cri_desc,
                                           const unsigned short work_atp_index, ex_cri_desc *given_cri_desc,
                                           ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                           int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::SHOW_SET_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0, NULL,
                    work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      type_(type),
      flags_(0),
      param1_(param1),
      param2_(param2) {
  setNodeType(ComTdb::ex_SHOW_SET);
}

Long ComTdbExeUtilShowSet::pack(void *space) {
  if (param1_) param1_.pack(space);
  if (param2_) param2_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilShowSet::unpack(void *base, void *reallocator) {
  if (param1_.unpack(base)) return -1;
  if (param2_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilShowSet::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbExeUtilShowSet :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////////////
//
// Methods for class ComTdbExeUtilAQR
//
///////////////////////////////////////////////////////////////////////////
ComTdbExeUtilAQR::ComTdbExeUtilAQR(int task, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                   queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::AQR_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0, NULL, NULL,
                    0, given_cri_desc, returned_cri_desc, down, up, num_buffers, buffer_size),
      task_(task),
      flags_(0) {
  setNodeType(ComTdb::ex_AQR);
}

Long ComTdbExeUtilAQR::pack(void *space) { return ComTdbExeUtil::pack(space); }

int ComTdbExeUtilAQR::unpack(void *base, void *reallocator) { return ComTdbExeUtil::unpack(base, reallocator); }

void ComTdbExeUtilAQR::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbExeUtilAQR :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

//*********************************************
// ComTdbExeUtilHBaseBulkLoad
//********************************************
ComTdbExeUtilHBaseBulkLoad::ComTdbExeUtilHBaseBulkLoad(char *tableName, int tableNameLen, char *ldStmtStr,
                                                       ex_expr_base *input_expr, int input_rowlen,
                                                       ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                                       ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                       queue_index down, queue_index up, int num_buffers,
                                                       int buffer_size, char *errCountTab, char *loggingLoc)
    : ComTdbExeUtil(ComTdbExeUtil::HBASE_LOAD_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, tableName, tableNameLen,
                    input_expr, input_rowlen, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      ldQuery_(ldStmtStr),
      flags_(0),
      maxErrorRows_(0),
      errCountTable_(errCountTab),
      loggingLocation_(loggingLoc),
      sampleTableName_(NULL) {
  setNodeType(ComTdb::ex_HBASE_LOAD);
}

Long ComTdbExeUtilHBaseBulkLoad::pack(void *space) {
  if (ldQuery_) ldQuery_.pack(space);
  if (errCountTable_) errCountTable_.pack(space);
  if (loggingLocation_) loggingLocation_.pack(space);
  if (sampleTableName_) sampleTableName_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilHBaseBulkLoad::unpack(void *base, void *reallocator) {
  if (ldQuery_.unpack(base)) return -1;
  if (errCountTable_.unpack(base)) return -1;
  if (loggingLocation_.unpack(base)) return -1;
  if (sampleTableName_.unpack(base)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}
void ComTdbExeUtilHBaseBulkLoad::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilHbaseLoad :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (ldQuery_) {
      char query[400];
      if (strlen(ldQuery_) > 390) {
        strncpy(query, ldQuery_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, ldQuery_);

      str_sprintf(buf, "ld Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (getLogErrorRows()) {
      if (loggingLocation_) {
        str_sprintf(buf, "Logging location = %s ", loggingLocation_.getPointer());
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      }
    }
    if (maxErrorRows_ > 0) {
      str_sprintf(buf, "Max Error Rows = %d", maxErrorRows_);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      if (errCountTable_) {
        str_sprintf(buf, "Error Counter Table Name = %s ", errCountTable_.getPointer());
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      }
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

ComTdbExeUtilRegionStats::ComTdbExeUtilRegionStats(char *tableName, char *catName, char *schName, char *objName,
                                                   ex_expr_base *input_expr, int input_rowlen, ex_expr_base *scan_expr,
                                                   ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                                   ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                   queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::REGION_STATS_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, tableName, strlen(tableName),
                    input_expr, input_rowlen, NULL, 0, scan_expr, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      flags_(0),
      catName_(catName),
      schName_(schName),
      objName_(objName) {
  setNodeType(ComTdb::ex_REGION_STATS);
}

Long ComTdbExeUtilRegionStats::pack(void *space) {
  if (catName_) catName_.pack(space);
  if (schName_) schName_.pack(space);
  if (objName_) objName_.pack(space);

  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilRegionStats::unpack(void *base, void *reallocator) {
  if (catName_.unpack(base)) return -1;
  if (schName_.unpack(base)) return -1;
  if (objName_.unpack(base)) return -1;

  return ComTdbExeUtil::unpack(base, reallocator);
}

// by default, max deep of a tree
#define TREE_MAX_DEEP 200
// by default, the max number of rows to return for hierachy query
#define MAX_ROW_NUM 100000
ComTdbExeUtilConnectby::ComTdbExeUtilConnectby(char *query, int querylen, Int16 querycharset, char *tableName,
                                               Int16 tblNameLen, char *stmtName, ex_expr *input_expr, int input_rowlen,
                                               ex_expr *output_expr, int output_rowlen, ex_expr *scan_expr,
                                               ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                               int colDescSize, int outputRowSize, ex_cri_desc *given_cri_desc,
                                               ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                               int num_buffers, int buffer_size, ExCriDescPtr workCriDesc,
                                               ex_expr *startwith_expr)
    : ComTdbExeUtil(ComTdbExeUtil::CONNECT_BY_, query, querylen, querycharset, tableName, tblNameLen, input_expr,
                    input_rowlen, output_expr, output_rowlen, scan_expr, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      flags_(0),
      myWorkCriDesc_(workCriDesc),
      tupleLen_(outputRowSize) {
  setNodeType(ComTdb::ex_CONNECT_BY);
  connTableName_ = tableName;
  maxDeep_ = TREE_MAX_DEEP;
  maxSize_ = MAX_ROW_NUM;
  hasPath_ = FALSE;
  hasIsLeaf_ = FALSE;
  isDual_ = FALSE;
  startwith_expr_ = startwith_expr;
}

Long ComTdbExeUtilConnectby::pack(void *space) {
  myWorkCriDesc_.pack(space);
  startwith_expr_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilConnectby::unpack(void *base, void *reallocator) {
  if (myWorkCriDesc_.unpack(base, reallocator)) return -1;
  if (startwith_expr_.unpack(base, reallocator)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

//////////////////////////////////////////////////////////////
// ComTdbExeUtilCompositeUnnest
//////////////////////////////////////////////////////////////
ComTdbExeUtilCompositeUnnest::ComTdbExeUtilCompositeUnnest(
    ex_expr *scanExpr, ex_expr *extractColExpr, UInt16 extractColAtpIndex, UInt32 extractColRowLen,
    UInt16 elemNumAtpIndex, UInt32 elemNumLen, ex_expr *returnColsExpr, UInt16 returnColsAtpIndex,
    UInt32 returnColsRowLen, ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
    ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers,
    int buffer_size)
    : ComTdbExeUtil(ComTdbExeUtil::COMPOSITE_UNNEST_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, NULL, 0, NULL, 0, NULL, 0,
                    scanExpr, work_cri_desc, work_atp_index, given_cri_desc, returned_cri_desc, down, up, num_buffers,
                    buffer_size),
      extractColExpr_(extractColExpr),
      extractColAtpIndex_(extractColAtpIndex),
      extractColRowLen_(extractColRowLen),
      returnColsExpr_(returnColsExpr),
      returnColsAtpIndex_(returnColsAtpIndex),
      returnColsRowLen_(returnColsRowLen),
      elemNumAtpIndex_(elemNumAtpIndex),
      elemNumLen_(elemNumLen) {
  setNodeType(ComTdb::ex_COMPOSITE_UNNEST);
}

Long ComTdbExeUtilCompositeUnnest::pack(void *space) {
  extractColExpr_.pack(space);
  returnColsExpr_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilCompositeUnnest::unpack(void *base, void *reallocator) {
  if (extractColExpr_.unpack(base, reallocator)) return -1;
  if (returnColsExpr_.unpack(base, reallocator)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

int ComTdbExeUtilCompositeUnnest::numExpressions() const { return (ComTdbExeUtil::numExpressions() + 2); }

ex_expr *ComTdbExeUtilCompositeUnnest::getExpressionNode(int pos) {
  if (pos >= numExpressions())
    return NULL;
  else if (pos < ComTdbExeUtil::numExpressions())
    return ComTdbExeUtil::getExpressionNode(pos);
  else if (pos == ComTdbExeUtil::numExpressions())
    return extractColExpr_;
  else
    return returnColsExpr_;
}

const char *ComTdbExeUtilCompositeUnnest::getExpressionName(int pos) const {
  if (pos >= numExpressions())
    return NULL;
  else if (pos < ComTdbExeUtil::numExpressions())
    return ComTdbExeUtil::getExpressionName(pos);
  else if (pos == ComTdbExeUtil::numExpressions())
    return "extractColExpr_";
  else
    return "returnColsExpr_";
}

//*********************************************
// ComTdbExeUtilUpdataDelete
//********************************************
ComTdbExeUtilUpdataDelete::ComTdbExeUtilUpdataDelete(char *tableName, int tableNameLen, char *ldStmtStr,
                                                     ex_cri_desc *work_cri_desc, const unsigned short work_atp_index,
                                                     ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                                     queue_index down, queue_index up, int num_buffers, int buffer_size,
                                                     ComStorageType storageType, char *server, char *zkPort)
    : ComTdbExeUtil(ComTdbExeUtil::SNAPSHOT_UPDATE_DELETE_, NULL, 0, (Int16)SQLCHARSETCODE_UNKNOWN, tableName,
                    tableNameLen, NULL, 0, NULL, 0, NULL, work_cri_desc, work_atp_index, given_cri_desc,
                    returned_cri_desc, down, up, num_buffers, buffer_size),
      query_(ldStmtStr),
      listOfIndexesAndTable_(NULL),
      incrBackupEnabled_(FALSE),
      storageType_(storageType),
      server_(server),
      zkPort_(zkPort),
      objUID_(0) {
  setNodeType(ComTdb::ex_SNAPSHOT_UPDATE_DELETE);
}

Long ComTdbExeUtilUpdataDelete::pack(void *space) {
  listOfIndexesAndTable_.pack(space);
  if (query_) query_.pack(space);
  if (server_) server_.pack(space);
  if (zkPort_) zkPort_.pack(space);
  return ComTdbExeUtil::pack(space);
}

int ComTdbExeUtilUpdataDelete::unpack(void *base, void *reallocator) {
  if (query_.unpack(base)) return -1;
  if (server_.unpack(base)) return -1;
  if (zkPort_.unpack(base)) return -1;
  if (listOfIndexesAndTable_.unpack(base, reallocator)) return -1;
  return ComTdbExeUtil::unpack(base, reallocator);
}

void ComTdbExeUtilUpdataDelete::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];
    str_sprintf(buf, "\nFor ComTdbExeUtilUpdataDelete :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (getTableName() != NULL) {
      str_sprintf(buf, "Tablename = %s ", getTableName());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (query_) {
      char query[400];
      if (strlen(query_) > 390) {
        strncpy(query, query_, 390);
        query[390] = 0;
        strcat(query, "...");
      } else
        strcpy(query, query_);

      str_sprintf(buf, "ld Query = %s ", query);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

///////////////////////////////////////////////////////////////////
