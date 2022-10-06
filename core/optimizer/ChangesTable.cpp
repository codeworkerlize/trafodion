
/* -*-C++-*-
******************************************************************************
*
* File:         ChangesTable.cpp
* Description:  Insertion, scanning and deleting from a changes table
*               such as the triggers temporary table, and the MV log.
* Created:      10/10/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "optimizer/ChangesTable.h"

#include "ComMvDefs.h"
#include "common/NumericType.h"
#include "optimizer/AllItemExpr.h"
#include "optimizer/AllRelExpr.h"
#include "optimizer/BindWA.h"
#include "optimizer/ItmFlowControlFunction.h"
#include "optimizer/NATable.h"
#include "sqlcomp/parser.h"

/*****************************************************************************
******************************************************************************
****  Class TriggersTempTable
******************************************************************************
*****************************************************************************/

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ChangesTable::ChangesTable(const CorrName &name, OperatorTypeEnum opType, BindWA *bindWA, RowsType scanType)
    : naTable_(NULL),
      subjectTable_(name, bindWA->wHeap()),
      subjectNaTable_(NULL),
      opType_(opType),
      tableCorr_(NULL),
      scanType_(scanType),
      scanNode_(NULL),
      corrNameForNewRow_(NEWCorr, bindWA->wHeap()),
      corrNameForOldRow_(OLDCorr, bindWA->wHeap()),
      bindWA_(bindWA),
      heap_(bindWA->wHeap()) {}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ChangesTable::ChangesTable(const GenericUpdate *baseNode, BindWA *bindWA)
    : naTable_(NULL),
      subjectTable_(baseNode->getTableDesc()->getCorrNameObj(), bindWA->wHeap()),
      subjectNaTable_(baseNode->getTableDesc()->getNATable()),
      opType_(baseNode->getOperatorType()),
      tableCorr_(NULL),
      scanNode_(NULL),
      corrNameForNewRow_(NEWCorr, bindWA->wHeap()),
      corrNameForOldRow_(OLDCorr, bindWA->wHeap()),
      bindWA_(bindWA),
      heap_(bindWA->wHeap()) {
  CMPASSERT(baseNode != NULL);
  if (baseNode->getUpdateCKorUniqueIndexKey() && baseNode->getOperatorType() == REL_UNARY_INSERT)
    opType_ = REL_UNARY_UPDATE;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ChangesTable::ChangesTable(Scan *baseNode, RowsType scanType, BindWA *bindWA)
    : subjectTable_(baseNode->getTableName(), bindWA->wHeap()),
      subjectNaTable_(NULL),
      naTable_(NULL),
      opType_(baseNode->getOperatorType()),
      tableCorr_(NULL),
      scanType_(scanType),
      scanNode_(baseNode),
      corrNameForNewRow_(NEWCorr, bindWA->wHeap()),
      corrNameForOldRow_(OLDCorr, bindWA->wHeap()),
      bindWA_(bindWA),
      heap_(bindWA->wHeap())

{
  CMPASSERT(baseNode != NULL);
}

//////////////////////////////////////////////////////////////////////////////
// This method is called after the Ctors of sub-classes of ChangesTable,
// in order to initialize the changes table name and NATable pointer.
// calcTargetTableName() is a pure virtual function, and so cannot be called
// by the base class Ctor.
//////////////////////////////////////////////////////////////////////////////
void ChangesTable::initialize() {
  CMPASSERT(subjectTable_ != "");  // Subject table name must be initialized.

  // Calculate the changes table name from the subject table name.
  const NATable *subjectNaTable = getSubjectNaTable();
  if (subjectNaTable && subjectNaTable->getIsSynonymTranslationDone()) {
    QualifiedName subjectQualName(subjectNaTable->getSynonymReferenceName(),
                                  3,  // minNameParts
                                  bindWA_->wHeap(), bindWA_);
    tableCorr_ = calcTargetTableName(subjectQualName);
  } else
    tableCorr_ = calcTargetTableName(getSubjectTableName().getQualifiedNameObj());

  // If the table is using late name resolution, the log should as well.
  if (getSubjectTableName().getPrototype() && supportsLateBinding()) {
    HostVar *logPrototype = new (bindWA_->wHeap()) HostVar(*getSubjectTableName().getPrototype());

    // Mark the table type on the prototype.
    logPrototype->setSpecialType(tableCorr_->getSpecialType());
    tableCorr_->setPrototype(logPrototype);
  }

  // Save the state of the RI flag
  BindContext *currentBindContext = bindWA_->getCurrentScope()->context();
  NABoolean prevRiState = currentBindContext->inRIConstraint();
  // Set the RI flag to allow calling getNATable() without counting it.
  currentBindContext->inRIConstraint() = TRUE;

  // Get the NATable of the changes table.
  naTable_ = bindWA_->getNATable(*tableCorr_);

  // if we hit any error while constructing naTable_, we should
  // catch it here as we will try to dereference it
  CMPASSERT(naTable_ != NULL);

  if (subjectNaTable_ == NULL) {
    subjectNaTable_ = bindWA_->getNATable(subjectTable_);
    CMPASSERT(subjectNaTable_)  // NATable must have been found!
  }

  // Restore the RI flag.
  currentBindContext->inRIConstraint() = prevRiState;
}

//////////////////////////////////////////////////////////////////////////////
// build the node to insert the OLD@ and NEW@ data into the changes table.
// Insert and Delete operations each cause a single row to be inserted into
// the changes table per affected set row. Update operations cause two rows
// to be inserted into the changes table per affected set row: one with the
// OLD data and one with the NEW data.
// For update operations, we can enforce insertion of only one set (NEW/OLD)
// using the second parameter. This parameter is defaulted to ALL_ROWS, so if
// not explicitly requested, it doesn't affect the insert at whole.
//////////////////////////////////////////////////////////////////////////////
RelExpr *ChangesTable::buildInsert(NABoolean useLeafInsert, int enforceRowsTypeForUpdate, NABoolean isUndo,
                                   NABoolean isForMvLogging) const {
  ItemExprList *tupleColRefList = NULL;
  ItemExpr *insItemList = NULL;
  ItemExpr *delItemList = NULL;
  NABoolean needOld = FALSE, needNew = FALSE;
  NABoolean isUpdate = FALSE, isInsert = FALSE, isDelete = FALSE;

  // Get the columns of the temp table.
  const NAColumnArray &tempColumns = getNaTable()->getNAColumnArray();

  switch (getOpType())  // What type of GenericUpdate node is this?
  {
    case REL_UNARY_INSERT:  // hhhheeeelllpppp - I'm falling to next case
    case REL_LEAF_INSERT:
      needNew = TRUE;
      isInsert = TRUE;
      break;
    case REL_UNARY_DELETE:
      needOld = TRUE;
      isDelete = TRUE;
      break;
    case REL_UNARY_UPDATE:
      needNew = TRUE;
      needOld = TRUE;
      isUpdate = TRUE;
      break;
    default:
      CMPASSERT(FALSE);
  }

  // For update operations, the second parameter controls which type of rows
  // is inserted into the temp-table. When both sets of values are inserted,
  // the insert is unary insert. When only one set of values is inserted, a
  // leaf insert is used.
  if (getOpType() == REL_UNARY_UPDATE) {
    CMPASSERT(needOld && needNew);

    if (!(enforceRowsTypeForUpdate & INSERTED_ROWS)) {
      needNew = FALSE;
    }

    if (!(enforceRowsTypeForUpdate & DELETED_ROWS)) {
      needOld = FALSE;
    }

    // use LeafInsert node for UPDATE operation as well, unless we need both
    // sets of values (NEW and OLD).
    if (needNew && needOld) {
      useLeafInsert = FALSE;
    }
  }

  if (useLeafInsert) tupleColRefList = new (heap_) ItemExprList(heap_);

  // Now for each of the log columns,
  // enter the correct value to be inserted.
  ItemExpr *delColExpr, *insColExpr;
  for (CollIndex i = 0; i < tempColumns.entries(); i++) {
    NAColumn *currentTempCol = tempColumns.getColumn(i);
    const NAString &colName = currentTempCol->getColName();

    delColExpr = insColExpr = NULL;
    if (colName.data()[0] == '@') {
      if (needOld) delColExpr = createAtColExpr(currentTempCol, FALSE, isUpdate);
      if (needNew) insColExpr = createAtColExpr(currentTempCol, TRUE, isUpdate, isUndo);
    } else {
      if (!colName.compareTo("SYSKEY")) {
        // This is the log SYSKEY column
        // iud log no longer has a SYSKEY
        if (needOld && useLeafInsert) delColExpr = createSyskeyColExpr(colName, FALSE);
        if (needNew && useLeafInsert) insColExpr = createSyskeyColExpr(colName, TRUE);
      } else {
        if (needOld) delColExpr = createBaseColExpr(colName, FALSE, isUpdate);
        if (needNew) insColExpr = createBaseColExpr(colName, TRUE, isUpdate);
      }
    }

    // Add to column list of the new record expression.
    if (useLeafInsert) {
      if (needOld) tupleColRefList->insert(delColExpr);
      if (needNew) tupleColRefList->insert(insColExpr);
    } else {
      if (needOld && delColExpr != NULL)
        if (delItemList == NULL)
          delItemList = delColExpr;
        else
          delItemList = new (heap_) ItemList(delItemList, delColExpr);

      if (needNew && insColExpr != NULL)
        if (insItemList == NULL)
          insItemList = insColExpr;
        else
          insItemList = new (heap_) ItemList(insItemList, insColExpr);
    }
  }

  Insert *insertNode;
  Union *unionNode = NULL;
  if (useLeafInsert) {
    // Create the leaf Insert node for INSERT or DELETE operations into the log table.
    insertNode = new (heap_) LeafInsert(*getTableName(), NULL, tupleColRefList);
  } else {
    Tuple *delTuple = NULL, *insTuple = NULL;
    RelExpr *belowInsert = NULL;

    if (needOld) belowInsert = delTuple = new (heap_) Tuple(delItemList);

    if (needNew) belowInsert = insTuple = new (heap_) Tuple(insItemList);

    if (isUpdate) {
      unionNode = new (heap_) Union(delTuple, insTuple, NULL, NULL, REL_UNION, CmpCommon::statementHeap(), TRUE);
      setTuplesUnionType(unionNode);
      belowInsert = unionNode;
    }

    insertNode = new (heap_) Insert(*getTableName(), NULL, REL_UNARY_INSERT, belowInsert);
  }

  // Rows inserted into the log table should not be counted.
  insertNode->rowsAffected() = GenericUpdate::DO_NOT_COMPUTE_ROWSAFFECTED;

  // set MvLogging flags in TrueRoot, Union, and Insert nodes
  if (isForMvLogging && ((isUpdate && CmpCommon::getDefault(MV_LOG_PUSH_DOWN_DP2_UPDATE) == DF_ON) ||
                         (isDelete && CmpCommon::getDefault(MV_LOG_PUSH_DOWN_DP2_DELETE) == DF_ON) ||
                         (isInsert && CmpCommon::getDefault(MV_LOG_PUSH_DOWN_DP2_INSERT) == DF_ON))) {
    bindWA_->getTopRoot()->getInliningInfo().setFlags(II_isUsedForMvLogging);
    insertNode->getInliningInfo().setFlags(II_isUsedForMvLogging);

    if (NULL != unionNode) {
      unionNode->getInliningInfo().setFlags(II_isUsedForMvLogging);
    }
  }

  return insertNode;
}  // ChangesTable::buildInsert()

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
RelExpr *ChangesTable::buildDelete(RowsType whichRows) const {
  // First build a Scan on the table.
  RelExpr *scanNode = buildScan(whichRows);

  // Build the Delete node on top of the Scan.
  Delete *deleteNode = new (heap_) Delete(*getTableName(), NULL, REL_UNARY_DELETE, scanNode);
  deleteNode->rowsAffected() = GenericUpdate::DO_NOT_COMPUTE_ROWSAFFECTED;

  // Put a root on top.
  RelRoot *rootNode = new (heap_) RelRoot(deleteNode);
  rootNode->setRootFlag(FALSE);
  rootNode->setEmptySelectList();

  return rootNode;
}  // ChangesTable::buildDelete()

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
RelExpr *ChangesTable::transformScan() const {
  CMPASSERT(scanNode_ != NULL);

  // If the Scan table does not have a correlation name, than the target
  // name should be made the correlation name.
  // Otherwise - the target name is redundant, and the Scan correlation
  // name stays.
  const NAString &baseCorrName = scanNode_->getTableName().getCorrNameAsString();

  const NAString &transitionName = baseCorrName != ""
                                       ? baseCorrName
                                       : isEmptyDefaultTransitionName()
                                             ? *(new (heap_) NAString("", heap_))
                                             : scanNode_->getTableName().getQualifiedNameObj().getObjectName();

  tableCorr_->setCorrName(transitionName);
  scanNode_->getTableName() = *tableCorr_;

  ItemExpr *selectionPredicate = createSpecificWhereExpr(scanType_);
  scanNode_->addSelPredTree(selectionPredicate);

  return scanNode_;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
Scan *ChangesTable::buildScan(RowsType type) const {
  Scan *scanNode = new (heap_) Scan(*getTableName());

  // Add uniqifier predicates to the scan node
  ItemExpr *selectionPredicate = createSpecificWhereExpr(type);
  scanNode->addSelPredTree(selectionPredicate);

  return scanNode;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
RelExpr *ChangesTable::buildOldAndNewJoin() const {
  NAString newName(NEWCorr);
  NAString oldName(OLDCorr);

  // Create two Scan nodes on the temporary table for the OLD and NEW data).
  Scan *tempScanOldNode = buildScan(ChangesTable::DELETED_ROWS);
  RenameTable *renameOld = new (heap_) RenameTable(tempScanOldNode, OLDCorr);

  Scan *tempScanNewNode = buildScan(ChangesTable::INSERTED_ROWS);
  RenameTable *renameNew = new (heap_) RenameTable(tempScanNewNode, NEWCorr);

  // Join the two Scans for one long row
  ItemExpr *joinPredicate = new (heap_)
      BiRelat(ITM_EQUAL, buildClusteringIndexVector(&newName, TRUE), buildClusteringIndexVector(&oldName, TRUE));

  Join *joinNode = new (heap_) Join(renameOld, renameNew, REL_JOIN, joinPredicate);
  // This join must be forced to a merge join.
  //  joinNode->forcePhysicalJoinType(Join::MERGE_JOIN_TYPE);

  return joinNode;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ItemExpr *ChangesTable::buildBaseColsSelectList(const CorrName &tableName) const {
  ItemExpr *selectList = NULL;
  ItemExpr *colRef;

  // Get the columns of the subject table.
  const NAColumnArray &subjectColumns = getSubjectNaTable()->getNAColumnArray();

  for (CollIndex i = 0; i < subjectColumns.entries(); i++) {
    NAString colName(subjectColumns.getColumn(i)->getColName());
    if (!colName.compareTo("SYSKEY")) continue;  // Skip SYSKEY.

    colRef = new (heap_) ColReference(new (heap_) ColRefName(colName, tableName, heap_));

    if (selectList == NULL)
      selectList = colRef;
    else
      selectList = new (heap_) ItemList(selectList, colRef);
  }

  return selectList;
}

//////////////////////////////////////////////////////////////////////////////
// Build a selection-list of the columns of the ChangesTable that have a
// corresponding column in the subject table.
//////////////////////////////////////////////////////////////////////////////
ItemExpr *ChangesTable::buildColsCorrespondingToBaseSelectList() const {
  ItemExpr *selectList = NULL;
  ItemExpr *colRef;

  // Get the columns of the ChangesTable.
  const NAColumnArray &columns = getNaTable()->getNAColumnArray();

  for (CollIndex i = 0; i < columns.entries(); i++) {
    NAString colName(columns.getColumn(i)->getColName());
    if (colName.data()[0] == '@' && colName.compareTo("@SYSKEY"))
      continue;  // Skip any special column that is not @SYSKEY.

    colRef = new (heap_) ColReference(new (heap_) ColRefName(colName, *getTableName(), heap_));

    if (selectList == NULL)
      selectList = colRef;
    else
      selectList = new (heap_) ItemList(selectList, colRef);
  }

  return selectList;
}

//////////////////////////////////////////////////////////////////////////////
// Build a list of ColReferences to the columns of the base table's
// clustering index columns.
// When the second parameter (useInternalSyskey) is TRUE, the returned vector
// is using the "@SYSKEY" column instead of "SYSKEY" column.
//////////////////////////////////////////////////////////////////////////////
ItemExpr *ChangesTable::buildClusteringIndexVector(const NAString *corrName, NABoolean useInternalSyskey) const {
  ItemExpr *result = NULL;

  const NATable *naTable = getSubjectNaTable();
  const NAColumnArray &indexColumns = naTable->getClusteringIndex()->getIndexKeyColumns();

  NAString tableName(""), schemaName(""), catalogName("");
  if (corrName != NULL) {
    tableName = *corrName;
  } else {
    const QualifiedName &qualName = naTable->getTableName();
    schemaName = qualName.getSchemaName();
    catalogName = qualName.getCatalogName();
    tableName = naTable->getTableName().getObjectName();
  }
  CorrName tableNameCorr(tableName, heap_, schemaName, catalogName);

  CollIndex lastCol = indexColumns.entries() - 1;
  for (CollIndex i = 0; i <= lastCol; i++) {
    NAString colName = indexColumns[i]->getColName();

    if (useInternalSyskey && !colName.compareTo("SYSKEY")) {
      colName = "@SYSKEY";  // "@SYSKEY" corresponds to SYSKEY column
    }

    ItemExpr *colExpr = new (heap_) ColReference(new (heap_) ColRefName(colName, tableNameCorr, heap_));

    if (result == NULL)
      result = colExpr;
    else
      result = new (heap_) ItemList(result, colExpr);
  }

  return result;
}

//////////////////////////////////////////////////////////////////////////////
// Build a list of RenameCols to map from the changes table column names to
// their euqivalent names in the subject table.
//////////////////////////////////////////////////////////////////////////////
ItemExpr *ChangesTable::buildRenameColsList() const {
  ItemExpr *renameList = NULL;

  // Get the columns of the subject table.
  const NAColumnArray &subjectColumns = getSubjectNaTable()->getNAColumnArray();

  for (CollIndex i = 0; i < subjectColumns.entries(); i++) {
    const NAString subjectCol(subjectColumns.getColumn(i)->getColName());
    const NAString *colName = &subjectCol;
    if (!subjectCol.compareTo("SYSKEY")) {
      // @SYSKEY in the changes table is the equivalent of the SYSKEY
      // column in the subject table.
      colName = new (heap_) NAString("@SYSKEY", heap_);
    }

    ColReference *newColName = new (heap_) ColReference(new (heap_) ColRefName(*colName, *getTableName(), heap_));

    RenameCol *renCol =
        new (heap_) RenameCol(newColName, new (heap_) ColRefName(subjectCol, getSubjectTableName(), heap_));

    if (renameList == NULL)
      renameList = renCol;
    else
      renameList = new (heap_) ItemList(renameList, renCol);
  }

  return renameList;
}  // ChangesTable::buildRenameColsList

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ItemExpr *ChangesTable::createBaseColExpr(const NAString &colName, NABoolean isInsert, NABoolean isUpdate) const {
  const CorrName &corrName = isInsert ? getCorrNameForNewRow() : getCorrNameForOldRow();

  return new (heap_) ColReference(new (heap_) ColRefName(colName, corrName, heap_));
}

//////////////////////////////////////////////////////////////////////////////
// Take the name of the subject table, and add to it the suffix to create
// the name of the changes table.
//////////////////////////////////////////////////////////////////////////////
void ChangesTable::addSuffixToTableName(NAString &tableName, const NAString &suffix) {
  size_t nameLen = tableName.length();
  ComBoolean isQuoted = FALSE;

  // special case: string ends with a quote char
  if ('"' == tableName[nameLen - 1]) {
    tableName.resize(nameLen - 1);
    isQuoted = TRUE;
  }

  tableName.append(suffix);

  if (isQuoted) tableName.append("\"");  // '"'
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
CorrName *ChangesTable::buildChangesTableCorrName(const QualifiedName &tableName, const NAString &suffix,
                                                  ExtendedQualName::SpecialTableType tableType, CollHeap *heap) {
  NAString changesTableName(tableName.getObjectName());
  addSuffixToTableName(changesTableName, suffix);
  CorrName *result = new (heap) CorrName(changesTableName, heap, tableName.getSchemaName(), tableName.getCatalogName());

  // Specify the trigger temporary table namespace.
  result->setSpecialType(tableType);
  return result;
}

/*****************************************************************************
******************************************************************************
****  Class TriggersTempTable
******************************************************************************
*****************************************************************************/

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
TriggersTempTable::TriggersTempTable(const GenericUpdate *baseNode, BindWA *bindWA)
    : ChangesTable(baseNode, bindWA), beforeTriggersExist_(FALSE), boundExecId_(NULL) {
  initialize();
}

//////////////////////////////////////////////////////////////////////////////
// This Ctor is used to transform the scan on the temp-table inside the
// trigger action sub-tree of a statement trigger.
// (see Scan::buildTriggerTransitionTableView in Inlining.cpp)
//////////////////////////////////////////////////////////////////////////////
TriggersTempTable::TriggersTempTable(const CorrName &subjectTableName, Scan *baseNode, RowsType scanType,
                                     BindWA *bindWA)
    : ChangesTable(subjectTableName, NO_OPERATOR_TYPE, bindWA, scanType),
      beforeTriggersExist_(FALSE),
      boundExecId_(NULL) {
  initialize();
  scanNode_ = baseNode;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
RelExpr *TriggersTempTable::transformScan() const {
  // The base class method fixes the scanned table name and takes care
  // of the uniquifier selection predicates.
  RelExpr *scanNode = ChangesTable::transformScan();

  // The select list of this root will filter away all the special columns
  // that are not columns of the subject table.
  ItemExpr *selectList = buildBaseColsSelectList(*getTableName());
  RelRoot *rootNode = new (heap_) RelRoot(scanNode, REL_ROOT, selectList);

  NAString transitionName = tableCorr_->getCorrNameAsString();

  RenameTable *renameNode = new (heap_) RenameTable(rootNode, transitionName);

  return renameNode;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
ItemExpr *TriggersTempTable::createAtColExpr(const NAColumn *naColumn, NABoolean isInsert, NABoolean isUpdate,
                                             NABoolean isUndo) const {
  const NAString &colName = naColumn->getColName();

  if (!colName.compareTo(UNIQUEEXE_COLUMN)) {
    // With before triggers, the UniqueExecuteId column is used to propagate
    // the Signal actions. If this is an update, only one signal should be
    // fired, not two.
    if (getBeforeTriggersExist() && (!isUpdate || isInsert)) {
      return new (heap_) ColReference(new (heap_) ColRefName(InliningInfo::getExecIdVirtualColName(), heap_));
    } else {
      if (boundExecId_ != NULL) {
        // Use the already bound existing exec id function.
        return boundExecId_;
      } else {
        // Normal case - use the UniqueExecuteId builtin function.
        return new (heap_) UniqueExecuteId();
      }
    }
  }

  if (!colName.compareTo(UNIQUEIUD_COLUMN)) {
    BindWA::uniqueIudNumOffset offset = isInsert ? BindWA::uniqueIudNumForInsert : BindWA::uniqueIudNumForDelete;

    return new (heap_) ConstValue(bindWA_->getUniqueIudNum(offset));
  }

  if (!colName.compareTo("@SYSKEY")) {
    return createBaseColExpr("SYSKEY", isInsert, TRUE);
  }

  return NULL;
}  // TriggersTempTable::createColumnExpression()

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
// iud log no longer has a SYSKEY
ItemExpr *TriggersTempTable::createSyskeyColExpr(const NAString &colName, NABoolean isInsert) const {
  CMPASSERT(FALSE);
  return NULL;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
CorrName *TriggersTempTable::calcTargetTableName(const QualifiedName &tableName) const {
  return buildChangesTableCorrName(tableName,
                                   TRIG_TEMP_TABLE_SUFFIX,  // __TEMP
                                   ExtendedQualName::TRIGTEMP_TABLE, heap_);
}  // TriggersTempTable::calcTargetTableName()

//////////////////////////////////////////////////////////////////////////////
//
// createSpecificWhereExpr
//
// Create the uniqifier where expresion for the scan node on the temp table.
// This method is used for scanning the temp table for statement triggers,
// for the temp Delete node, and for the effective GU of before triggers.
//
// The uniqifier fields UNIQUE_EXECUTE_ID and UNIQUE_IUD_ID are used to
// identify the rows in a temporary table belong to a specific IUD and a
// specific query execution
//
//////////////////////////////////////////////////////////////////////////////
ItemExpr *TriggersTempTable::createSpecificWhereExpr(RowsType type) const {
  // Create the ItemExpr for the function UniqueExecuteId()
  ItemExpr *col1 = new (heap_) ColReference(new (heap_) ColRefName(UNIQUEEXE_COLUMN, heap_));

  ItemExpr *execId;
  if (boundExecId_ != NULL) {
    // Use the already bound existing exec id function.
    execId = boundExecId_;
  } else {
    // Normal case - use the UniqueExecuteId builtin function.
    execId = new (heap_) UniqueExecuteId();
  }
  BiRelat *predExecId = new (heap_) BiRelat(ITM_EQUAL, col1, execId);

  ItemExpr *result;
  BiRelat *predIudId = NULL;
  if (type == ALL_ROWS)
    result = predExecId;
  else {
    // Create the ItemExpr for the constatnt UniqueIudNum
    ItemExpr *col2 = new (heap_) ColReference(new (heap_) ColRefName(UNIQUEIUD_COLUMN, heap_));

    // Compare it to the correct offset.
    BindWA::uniqueIudNumOffset offset =
        type == INSERTED_ROWS ? BindWA::uniqueIudNumForInsert : BindWA::uniqueIudNumForDelete;
    ItemExpr *iudConst = new (heap_) ConstValue(bindWA_->getUniqueIudNum(offset));
    predIudId = new (heap_) BiRelat(ITM_EQUAL, col2, iudConst);

    result = new (heap_) BiLogic(ITM_AND, predExecId, predIudId);
  }

  return result;
}  // TriggersTempTable::createSpecificWhereExpr()

//////////////////////////////////////////////////////////////////////////////
//
// buildScanForMV
//
// Create a sub-tree that read's only the NEW values from the temp-table, for
// use as a replacement tree for the scan on the subject table in the MV tree.
// This replacement is taking place in insert operations that needs to update
// an ON STATEMENT MV defined on the inserted table.
//
// The built sub-tree is as follows:
//
//   RenameTable (temp-table -> subject-table, "@syskey" -> syskey)
//        |
//     RelRoot (remove Uniquifier columns)
//        |
//  Scan temp-table (NEW@ values only)
//
// Note: The RelRoot node that removes the uniquifier columns is needed, since
//       the RenameTable node ensures that the two tables have the same degree.
///////////////////////////////////////////////////////////////////////////////
RelExpr *TriggersTempTable::buildScanForMV() const {
  // build scan to select NEW@ values only
  Scan *scanNode = buildScan(ChangesTable::INSERTED_ROWS);

  // build RelRoot to reduce to only columns that correspond to any column
  // in the subject table
  ItemExpr *selectList = buildColsCorrespondingToBaseSelectList();
  RelRoot *rootNode = new (heap_) RelRoot(scanNode, REL_ROOT, selectList);

  // build RenameTable to rename table name and columns' names to immitate
  // a scan on the subject table to the rest of the MV sub-tree
  ItemExpr *renameList = buildRenameColsList();
  RelExpr *renameNode = new (heap_) RenameTable(TRUE, rootNode, getSubjectTableName(), renameList);

  return renameNode;
}  // TriggersTempTable::buildScanForMV
