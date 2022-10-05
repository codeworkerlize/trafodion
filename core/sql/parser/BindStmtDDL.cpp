/* -*-C++-*-

 *****************************************************************************
 *
 * File:         BindStmtDDL.C
 * Description:  DDL statements
 *               Methods related to the SQL binder
 *
 * Created:      3/28/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#include "AllStmtDDL.h"
#include "optimizer/BindWA.h"
// QSTUFF
#include "optimizer/NormWA.h"
// QSTUFF
// #include "CatError.h"
// #define  CAT_ALTER_CANNOT_ADD_NOT_DROPPABLE_CONSTRAINT       1053
// The above are commented out because we use sqlcode 3067 instead.
#include "common/ComASSERT.h"
#include "common/ComObjectName.h"
#include "common/ComOperators.h"
#include "common/CmpCommon.h"
#include "sqlcomp/CmpMain.h"
#include "ElemDDLPartitionSystem.h"
#include "ElemDDLUdrLibrary.h"
#include "ElemDDLLike.h"
#include "ElemDDLConstraintCheck.h"
#include "ElemDDLConstraintRI.h"
#include "ElemDDLReferences.h"
#include "parser/ElemDDLPartitionList.h"
#include "optimizer/NATable.h"
#include "optimizer/RelMisc.h"
#include "optimizer/TableDesc.h"
#include "optimizer/RelJoin.h"
#include "parser/SqlParserGlobals.h"  // must be last #include
#include "optimizer/Triggers.h"
#include "optimizer/NormWA.h"
#include "optimizer/Analyzer.h"
#include "qmscommon/QRDescriptor.h"
#include "parser/StmtDDLAlterTableTruncatePartition.h"

// -----------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------
void CatCollectCheckConstraintUsages(const StmtDDLAddConstraintCheck &addCheckNode, const QualifiedName &tableQualName);

// -----------------------------------------------------------------------
// definition of method bindNode() for class ElemDDLConstraintRI
// -----------------------------------------------------------------------

ExprNode *ElemDDLConstraintRI::bindNode(BindWA *pBindWA) {
  for (int i = 0; i < getArity(); i++) {
    if (getChild(i)) {
      getChild(i)->castToElemDDLNode()->bindNode(pBindWA);
    }
  }

  markAsBound();

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class ElemDDLReferences
// -----------------------------------------------------------------------

ExprNode *ElemDDLReferences::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  getReferencedNameAsQualifiedName().applyDefaultsValidate(pBindWA->getDefaultSchema());

  markAsBound();

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class ElemDDLUdrLibrary
// -----------------------------------------------------------------------

ExprNode *ElemDDLUdrLibrary::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  libraryName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(libraryName_)) return this;

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// Added a new bindNode() for the class ElemDDLLike for the name expansion
// using Matthew's common code.
// -----------------------------------------------------------------------

ExprNode *ElemDDLLike::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  //  sourceTableCorrName_.getQualifiedNameObj().applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &sourceTableCorrName_.getQualifiedNameObj())) {
    pBindWA->setErrStatus();
    return this;
  }

  sourceTableName_ = sourceTableCorrName_.getQualifiedNameObj().getQualifiedNameAsAnsiString();
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLNode
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within a DDL Statement tree
//
ExprNode *StmtDDLNode::bindNode(BindWA * /* bindWAPtr */) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterAuditConfig
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterAuditConfig::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterIndex
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterIndex::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  indexQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(indexQualName_)) return this;
  indexName_ = indexQualName_.getQualifiedNameAsAnsiString();
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterLibrary
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterLibrary::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  libraryName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(libraryName_)) return this;
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterSynonym
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterSynonym::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  synonymName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(synonymName_)) return this;
  objectReference_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(objectReference_)) return this;
  if (synonymName_.getCatalogName() NEQ objectReference_.getCatalogName()) {
    *CmpCommon::diags() << DgSqlCode(-3230);
    pBindWA->setErrStatus();
  }

  markAsBound();
  return this;
}

// ----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAddConstraintUnique
// ----------------------------------------------------------------------
// virtual
ExprNode *StmtDDLAddConstraintUnique::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA) QualifiedName &tableQualName = getTableNameAsQualifiedName();

  //  tableQualName.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName)) return this;

  QualifiedName &constraintQualName = getConstraintNameAsQualifiedName();

  //
  // if constraint name was not specified, the "exec" layer (defined
  // in module CatAddCheckConstraint.C) will make up one.
  //
  if (NOT getConstraintName().isNull())  // constraint name was specified
  {
    constraintQualName.applyDefaults(tableQualName);
    if (pBindWA->violateAccessDefaultSchemaOnly(constraintQualName)) return this;

    if (tableQualName.getCatalogName() NEQ constraintQualName.getCatalogName()
            OR tableQualName.getSchemaName() NEQ constraintQualName.getSchemaName()) {
      *CmpCommon::diags() << DgSqlCode(-3050);
      pBindWA->setErrStatus();
      return this;
    }
  }

  markAsBound();
  return this;
}
// ----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAddConstraintRI
// ----------------------------------------------------------------------
// virtual
ExprNode *StmtDDLAddConstraintRI::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA)

      QualifiedName &tableQualName = getTableNameAsQualifiedName();
  tableQualName.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName)) return this;

  QualifiedName &constraintQualName = getConstraintNameAsQualifiedName();

  //
  // if constraint name was not specified, the "exec" layer (defined
  // in module CatAddCheckConstraint.C) will make up one.
  //
  if (NOT getConstraintName().isNull())  // constraint name was specified
  {
    constraintQualName.applyDefaults(tableQualName);
    if (pBindWA->violateAccessDefaultSchemaOnly(constraintQualName)) return this;

    if (tableQualName.getCatalogName() NEQ constraintQualName.getCatalogName()
            OR tableQualName.getSchemaName() NEQ constraintQualName.getSchemaName()) {
      *CmpCommon::diags() << DgSqlCode(-3050);
      pBindWA->setErrStatus();
      return this;
    }
  }

  getConstraint()->bindNode(pBindWA);

  markAsBound();
  return this;
}
// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAddConstraint
// -----------------------------------------------------------------------

// virtual
ExprNode *StmtDDLAddConstraint::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA)

      QualifiedName &tableQualName = getTableNameAsQualifiedName();
  tableQualName.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName)) return this;

  QualifiedName &constraintQualName = getConstraintNameAsQualifiedName();

  //
  // if constraint name was not specified, the "exec" layer (defined
  // in module CatAddCheckConstraint.C) will make up one.
  //
  if (NOT getConstraintName().isNull())  // constraint name was specified
  {
    constraintQualName.applyDefaults(tableQualName);
    if (pBindWA->violateAccessDefaultSchemaOnly(constraintQualName)) return this;

    if (tableQualName.getCatalogName() NEQ constraintQualName.getCatalogName()
            OR tableQualName.getSchemaName() NEQ constraintQualName.getSchemaName()) {
      *CmpCommon::diags() << DgSqlCode(-3050);
      pBindWA->setErrStatus();
      return this;
    }
  }

  markAsBound();

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAddConstraintCheck
// -----------------------------------------------------------------------

// virtual
ExprNode *StmtDDLAddConstraintCheck::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  pBindWA->setNameLocListPtr(&nameLocList_);
  pBindWA->setUsageParseNodePtr(this);

  QualifiedName &tableQualName = getTableNameAsQualifiedName();
  tableQualName.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName)) return this;

  QualifiedName &constraintQualName = getConstraintNameAsQualifiedName();

  ElemDDLConstraint *elemDDLC = getElemDDLConstraintCheck();

  // We can "add" nondroppable constraints only in a Create Table,
  // not an Alter Table.
  //
  if (!Get_SqlParser_Flags(ALLOW_ADD_CONSTRAINT_NOT_DROPPABLE)) {
    // Alter Table constraints default to DROPPABLE.
    if (!elemDDLC->anyDroppableClauseSpecifiedExplicitly()) elemDDLC->setDroppableFlag(TRUE);

    if (!elemDDLC->isDroppable()) {
      *CmpCommon::diags() << DgSqlCode(-3067);
      pBindWA->setErrStatus();
      return this;
    }
  }

  //
  // if constraint name was not specified, the "exec" layer (defined
  // in module CatAddCheckConstraint.C) will make up one.
  //
  if (NOT getConstraintName().isNull())  // constraint name was specified
  {
    constraintQualName.applyDefaults(tableQualName);
    if (pBindWA->violateAccessDefaultSchemaOnly(constraintQualName)) return this;

    if (tableQualName.getCatalogName() NEQ constraintQualName.getCatalogName()
            OR tableQualName.getSchemaName() NEQ constraintQualName.getSchemaName()) {
      *CmpCommon::diags() << DgSqlCode(-3050);
      pBindWA->setErrStatus();
      return this;
    }
  }

  // if isFakeNode() returns TRUE, the StmtDDLAddConstraintCheck node
  // represents a check constraint definition appearring in a create
  // table statement. Cannot bind the Search Condition expression for
  // this case because the table SOL object has not been created yet.
  // The binding of the expression will be done in CatExecCreateTable().
  if (NOT isFakeNode()) {  // check constraint def in alter table stmt
    //
    // Get the NATable for this object
    //
    CorrName tableCorrName(tableQualName, (CollHeap *)0, NAString() /*no corr. name*/);

    switch (getTableType()) {
      case COM_IUD_LOG_TABLE:
        tableCorrName.setSpecialType(ExtendedQualName::IUD_LOG_TABLE);
        break;
      case COM_SG_TABLE:
        tableCorrName.setSpecialType(ExtendedQualName::SG_TABLE);
        break;
      case COM_GHOST_IUD_LOG_TABLE:
        tableCorrName.setSpecialType(ExtendedQualName::GHOST_IUD_LOG_TABLE);
        break;
      case COM_RANGE_LOG_TABLE:
        tableCorrName.setSpecialType(ExtendedQualName::RANGE_LOG_TABLE);
        break;
      case COM_GHOST_MV_TABLE:
      case COM_GHOST_REGULAR_TABLE:
        tableCorrName.setSpecialType(ExtendedQualName::GHOST_TABLE);
        break;
      default:
        break;
    }

    NATable *naTable = pBindWA->getNATable(tableCorrName, FALSE /*don't collect usage info*/);
    if (pBindWA->errStatus()) return this;

    //
    // Allocate a TableDesc
    // This call also allocates a RETDesc and attaches to the BindScope
    //
    pBindWA->createTableDesc(naTable, tableCorrName);
    if (pBindWA->errStatus()) return this;

    // Genesis 10-980609-5773: tell Binder we're in a check constraint
    NAString tmpTxt("CHECK(...)");
    CheckConstraint tmpChk(constraintQualName, tmpTxt, pBindWA->wHeap());
    CheckConstraint *&savChk = pBindWA->getCurrentScope()->context()->inCheckConstraint();
    pBindWA->getCurrentScope()->context()->inCheckConstraint() = &tmpChk;

    getSearchCondition()->bindNode(pBindWA);

    pBindWA->getCurrentScope()->context()->inCheckConstraint() = savChk;
  }  // if (NOT isFakeNode())

  markAsBound();

  pBindWA->setNameLocListPtr(NULL);
  pBindWA->setUsageParseNodePtr(NULL);

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateCatalog
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Create Catalog tree
//
ExprNode *StmtDDLCreateCatalog::bindNode(BindWA * /* bindWAPtr */) {
  markAsBound();
  return this;
}

//----------------------------------------------------------------------------
// OZ
ExprNode *StmtDDLCreateMvRGroup::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);
  mvRGroupQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(mvRGroupQualName_)) return this;

  markAsBound();
  return this;
}



/***********************************************************/
// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateTrigger
// -----------------------------------------------------------------------

//
// Virtual function for performing name binding within the Create Trigger tree
//   This node has two children:
//     1. Columns (not null in case of explicit update columns) which does
//        not need binding here (the columns are verified seperately in
//        CatExecCreateTrigger. )
//     2. Trigger's action (including the "when" condition) which is bound
//        here.
//
ExprNode *StmtDDLCreateTrigger::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);

  // Verify that the name of the subject table was not used in the
  // REFERENCING clause.
  const NAString &subjectTable = getTableNameObject().getObjectName();
  if (((getOldName() != NULL) && (subjectTable == *getOldName())) ||
      ((getNewName() != NULL) && (subjectTable == *getNewName()))) {
    // 11021 The subject table name cannot be used in the REFERENCING clause.
    *CmpCommon::diags() << DgSqlCode(-11021);
    pBindWA->setErrStatus();
    return this;
  }

  if ((getOldName() != NULL) && (getNewName() != NULL) && (*getOldName() == *getNewName())) {
    // 11024 Cannot reference both OLD and NEW with the same name MYNEW.
    *CmpCommon::diags() << DgSqlCode(-11024) << DgString0(*getOldName());
    pBindWA->setErrStatus();
    return this;
  }

  pBindWA->setInTrigger();
  pBindWA->setNameLocListPtr(nameLocListPtr_);
  pBindWA->setUsageParseNodePtr(this);

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  triggerQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(triggerQualName_)) return this;

  ParNameLoc *pNameLoc = nameLocListPtr_->getNameLocPtr(triggerQualName_.getNamePosition());
  ComASSERT(pNameLoc);
  pNameLoc->setExpandedName(triggerQualName_.getQualifiedNameAsAnsiString());

  pNameLoc = nameLocListPtr_->getNameLocPtr(tableQualName_.getNamePosition());
  ComASSERT(pNameLoc);
  pNameLoc->setExpandedName(tableQualName_.getQualifiedNameAsAnsiString());

  // Bind child nodes

  //   For the first child (optional update columns) Do nothing!
  //   The columns would be checked in CatExecCreateTrigger

  //   Bind the second child -- the trigger's action expression
  if (getBodyType() == 0) {
    RelExpr *actionExpr = getChild(TRIGGER_ACTION_EXPRESSION)->castToRelExpr();

    ComASSERT(actionExpr NEQ NULL);
    pBindWA->getCurrentScope()->context()->triggerObj() = this;

    if (isStatement())  // Statement triggers don't need help binding.
      actionExpr = actionExpr->bindNode(pBindWA);
    else  // Row triggers need the transition values flowing in
      actionExpr = bindRowTrigger(pBindWA);

    if (!pBindWA->errStatus()) {  // bind succeeded
      ComASSERT(actionExpr->getOperatorType() == REL_ROOT);
      // run the transformNode() pass for further semantic checks
      actionExpr = CmpTransform(actionExpr);
    }

    //
    // sets a flag to let user know that the parse has  been bound.
    //

    setAction(actionExpr);
  }

  markAsBound();

  pBindWA->setNameLocListPtr(NULL);
  pBindWA->setUsageParseNodePtr(NULL);

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropTrigger
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropTrigger tree
//
ExprNode *StmtDDLDropTrigger::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);
  triggerQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(triggerQualName_)) return this;

  markAsBound();
  return this;
}

ExprNode *StmtDDLAlterCatalog::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);

  if (!isAllSchemaPrivileges_) {
    schemaName_ = ToAnsiIdentifier(schemaQualName_.getSchemaName());
  }

  markAsBound();
  return this;
}

//----------------------------------------------------------------------------
// MV - RG
ExprNode *StmtDDLAlterMvRGroup::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);
  // bind mv group
  mvRGroupQualName_.applyDefaults(pBindWA->getDefaultSchema());
  // do the following instead of the above if need to support public schema for alter mvgroup
  // mvRGroupQualName_.applyDefaultsValidate(pBindWA->getDefaultSchema(), COM_MVRG_NAME);

  if (pBindWA->violateAccessDefaultSchemaOnly(mvRGroupQualName_)) return this;

  // bind list of MV's

  QualifiedName *pQualName = &getFirstMvInList();
  pQualName->applyDefaults(pBindWA->getDefaultSchema());
  // do the following instead of the above if need to support public schema for alter mvgroup
  // pQualName->applyDefaultsValidate(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(*pQualName)) return this;

  while (listHasMoreMVs()) {
    pQualName = &getNextMvInList();
    pQualName->applyDefaults(pBindWA->getDefaultSchema());
    // do the following instead of the above if need to support public schema for alter mvgroup
    // pQualName->applyDefaultsValidate(pBindWA->getDefaultSchema());
    if (pBindWA->violateAccessDefaultSchemaOnly(*pQualName)) return this;
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterRoutine
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterRoutine::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA NEQ NULL);

  getRoutineNameAsQualifiedName().applyDefaults(pBindWA->getDefaultSchema());

  if (getRoutineNameSpace() EQU COM_UUDF_ACTION_NAME) {
    getActionNameAsQualifiedName().applyDefaults(pBindWA->getDefaultSchema());
  }

  markAsBound();
  return this;
}  // StmtDDLAlterRoutine::bindNode()

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterTrigger
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterTrigger::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  triggerOrTableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(triggerOrTableQualName_)) return this;

  markAsBound();
  return this;
}

/////////////////////////////////////////////////////////////////////////////
// This method is used during DDL only.
// it is called during a CREATE TRIGGER DDL operation, after the
// parsing was completed, the DDL statement is bound for semantic checks.
// This method is specific for binding of Row Triggers (before and after),
// since these triggers expect the OLD and NEW transition variables to be
// pipelined to them.
// Instead of creating a GenericUpdate node that looks like the operation
// driving the trigger, we are getting the same result by creating Scan nodes
// on the subject table, with the correct OLD and NEW correlation names. In
// case this is an Update trigger, a cross join is created to supply all the
// needed names. This is not expensive since this code will only be used for
// semantic checks, and will never be executed.
// The resulting tree that is bound looks like this (for Update triggers):
//
// An After Row trigger:                  A Before trigger:
//
//         RelRoot                            RelRoot
//            |                                  |
//           TSJ                            BeforeTrigger
//          /   \                                |
//      Join     Trigger action                 Join
//     /    \                                  /    \
//  Scan     Scan                            Scan    Scan
//  OLD@     NEW@                            OLD@    NEW@
/////////////////////////////////////////////////////////////////////////////
RelExpr *StmtDDLCreateTrigger::bindRowTrigger(BindWA *pBindWA) {
  CollHeap *heap = pBindWA->wHeap();
  RelExpr *rowTrigger = getActionExpression();
  RelExpr *scanTree = NULL;
  RelExpr *topNode = NULL;
  CorrName *NewName = NULL;
  CorrName *OldName = NULL;

  switch (getIUDEvent()) {
    case COM_INSERT:  // Insert: Need only NEW values
    {
      NewName = new (heap) CorrName(tableQualName_, heap, NEWCorr);
      scanTree = new (heap) Scan(*NewName);
    } break;

    case COM_UPDATE:  // Update: Need both OLD and NEW values
    {
      OldName = new (heap) CorrName(tableQualName_, heap, OLDCorr);
      NewName = new (heap) CorrName(tableQualName_, heap, NEWCorr);
      Scan *scanOld = new (heap) Scan(*OldName);
      Scan *scanNew = new (heap) Scan(*NewName);
      scanTree = new (heap) Join(scanOld, scanNew);
    } break;

    case COM_DELETE:  // Delete: Need only OLD values
    {
      OldName = new (heap) CorrName(tableQualName_, heap, OLDCorr);
      scanTree = new (heap) Scan(*OldName);
    } break;

    default:
      CMPASSERT(FALSE);
  }

  // Now connect the scan nodes to the trigger action
  if (isAfter()) {  // An after row trigger is driven from above using a TSJ.
    topNode = new (heap) Join(scanTree, rowTrigger, REL_TSJ);
  } else {  // A before trigger is driven from below.
    CMPASSERT(rowTrigger->getOperatorType() == REL_BEFORE_TRIGGER);
    // BeforeTrigger nodes are constructed childless.
    rowTrigger->child(0) = scanTree;
    topNode = rowTrigger;
  }

  // TransformNode() will fail if the top most node is not a RelRoot.
  RelRoot *rootNode = new (heap) RelRoot(topNode);
  rootNode->setRootFlag(TRUE);

  // Now bind the resulting tree.
  return rootNode->bindNode(pBindWA);
}  // StmtDDLCreateTrigger::bindRowTrigger()

//////////////////////////////////////////////////////////////////////////////
// Does the tableName match the Old or New transition names from the
// REFERENCING clause?
//////////////////////////////////////////////////////////////////////////////

// Is this a Scan on the OLD transition table name?
NABoolean StmtDDLCreateTrigger::isOldTransitionName(const NAString &tableName) {
  if (getOldName() != NULL)
    if (getOldName()->compareTo(tableName) == 0) return TRUE;
  return FALSE;
}

// Is this a Scan on the NEW transition table name?
NABoolean StmtDDLCreateTrigger::isNewTransitionName(const NAString &tableName) {
  if (getNewName() != NULL)
    if (getNewName()->compareTo(tableName) == 0) return TRUE;
  return FALSE;
}

NABoolean StmtDDLCreateTrigger::isTransitionName(const NAString &tableName) {
  return (isOldTransitionName(tableName) || isNewTransitionName(tableName));
}

// -----------------------------------------------------------------------
// Before beginning the expanded pass, add the system added columns
// to the select list of the root. This will make sure the system added
// columns are analyzed correctly.
// -----------------------------------------------------------------------
void addSystemColumnsToRootSelectList(RelRoot *queryRoot, MVInfoForDDL *mvInfo, BindWA *bindWA) {
  // Currently not supporting MJVs because of a bug (SYSKEY is ambiguous).
  if (mvInfo->getMVType() == COM_MJV) return;

  // Get the expressions for the system added columns.
  const NAString &systemAddedColsText = mvInfo->getSysColExprs();
  if (systemAddedColsText == "") return;

  // Verify that the first char is a ',' and skip it.
  const char *colsTextPtr = systemAddedColsText.data();
  CMPASSERT(colsTextPtr[0] == ',');
  colsTextPtr++;

  // Parse the expression text
  Parser parser(bindWA->currentCmpContext());
  ItemExpr *systemAddedColsExpr = parser.getItemExprTree(colsTextPtr);

  // Add the system added column expressions to the root select list.
  queryRoot->addCompExprTree(systemAddedColsExpr);
}





// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterView
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterView::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  viewQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(viewQualName_)) return this;

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateExceptionTable
// -----------------------------------------------------------------------

ExprNode *StmtDDLCreateExceptionTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  exceptionName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(exceptionName_)) return this;

  objectReference_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(objectReference_)) return this;

  if (exceptionName_.getSchemaName() NEQ objectReference_.getSchemaName()) {
    *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("Exception Tables");
    pBindWA->setErrStatus();
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateIndex
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Create Index tree
//
ExprNode *StmtDDLCreateIndex::bindNode(BindWA *pBindWA) {
  CollIndex index;

  //
  // binds all partition parse nodes associating
  // this Create Table parse node.
  //

  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  tableQualName_.applyDefaultsValidate(pBindWA->getDefaultSchema());
  /*  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_))
      {
        pBindWA->setErrStatus();
        return this;
      }*/
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  indexQualName_ =
      QualifiedName(getIndexName(), tableQualName_.getSchemaName(), tableQualName_.getCatalogName(), pBindWA->wHeap());
  if (pBindWA->violateAccessDefaultSchemaOnly(indexQualName_)) return this;
  indexName_ = indexQualName_.getQualifiedNameAsAnsiString();

  ComASSERT(getPartitionArray().entries() > 0);
  for (index = 0; index < getPartitionArray().entries(); index++) {
    getPartitionArray()[index]->bindNode(pBindWA);
  }

  if (getPartitionArray()[0]->castToElemDDLPartitionSystem()) {
    //
    // Note that class ElemDDLPartitionRange is
    // derived from class ElemDDLPartitionSystem.
    // Also note that the primary partition parse
    // node has been bound.
    //
    guardianLocation_ = getPartitionArray()[0]->castToElemDDLPartitionSystem()->getGuardianLocation();
  }

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLPopulateIndex
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Create Index tree
//
ExprNode *StmtDDLPopulateIndex::bindNode(BindWA *pBindWA) {
  CollIndex index;

  //
  // binds all partition parse nodes associating
  // this Create Table parse node.
  //

  ComASSERT(pBindWA);

  indexQualName_ = QualifiedName(getIndexName());
  indexName_ = indexQualName_.getQualifiedNameAsAnsiString();

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateLibrary
// -----------------------------------------------------------------------

ExprNode *StmtDDLCreateLibrary::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  libraryName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(libraryName_)) return this;

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreatePackage
// -----------------------------------------------------------------------
ExprNode *StmtDDLCreatePackage::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  int defaulted = packageQualName_.applyDefaults(pBindWA->getDefaultSchema());
  int actionDefaulted = 0;
  if (pBindWA->violateAccessDefaultSchemaOnly(packageQualName_)) return this;

  //
  // diagnose if non-ANSI defaults were applied to the name
  //
  if (NOT packageQualName_.verifyAnsiNameQualification(defaulted)) {
    *CmpCommon::diags() << DgSqlCode(-3206);
    pBindWA->setErrStatus();
  }

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateRoutine
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Create Routine tree
//
ExprNode *StmtDDLCreateRoutine::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  //
  // expands routine name
  //

  int defaulted = routineQualName_.applyDefaults(pBindWA->getDefaultSchema());
  int actionDefaulted = 0;
  if (pBindWA->violateAccessDefaultSchemaOnly(routineQualName_)) return this;
  if (NOT actionQualName_.getObjectName().isNull()) {
    ComASSERT(getRoutineType() EQU COM_ACTION_UDF_TYPE);
    actionDefaulted = actionQualName_.applyDefaults(pBindWA->getDefaultSchema());
    if (pBindWA->violateAccessDefaultSchemaOnly(actionQualName_)) return this;
  }

  //
  // diagnose if non-ANSI defaults were applied to the name
  //
  if (NOT routineQualName_.verifyAnsiNameQualification(defaulted)
          OR NOT actionQualName_.verifyAnsiNameQualification(actionDefaulted)) {
    *CmpCommon::diags() << DgSqlCode(-3206);
    pBindWA->setErrStatus();
  }

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateSynonym
// -----------------------------------------------------------------------

ExprNode *StmtDDLCreateSynonym::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  synonymName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(synonymName_)) return this;

  objectReference_.applyDefaultsValidate(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(objectReference_)) return this;

  if (synonymName_.getCatalogName() NEQ objectReference_.getCatalogName()) {
    *CmpCommon::diags() << DgSqlCode(-3230);
    pBindWA->setErrStatus();
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateSchema
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Create Schema tree
//
ExprNode *StmtDDLCreateSchema::bindNode(BindWA *pBindWA) {
  //  *CmpCommon::diags() <<  DgSqlCode(-4222)
  //                    << DgString0("CREATE SCHEMA");
  // pBindWA->setErrStatus();
  // create schema is a no-op in open source
  markAsBound();
  return this;
}  // StmtDDLCreateSchema::bindNode()

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateTable
// -----------------------------------------------------------------------

static void setColNotNullableIfCheckIsISNOTNULL(BindWA *pBindWA, StmtDDLAddConstraintCheck *pAddConstraintCheck,
                                                ElemDDLColDefArray &colDefArray) {
  ElemDDLConstraintCheck *elemDDLC = pAddConstraintCheck->getElemDDLConstraintCheck();

  if (elemDDLC->isDroppable()) return;

  ItemExprList nnndCols(pBindWA->wHeap());
  elemDDLC->getColumnsNotNull(nnndCols);
  for (CollIndex i = 0; i < nnndCols.entries(); i++) {
    ItemExpr *nnndCol = nnndCols[i];
    CMPASSERT(nnndCol->getOperatorType() == ITM_REFERENCE);
    NAString colName(((ColReference *)nnndCol)->getColRefNameObj().getColName());
    // Bug fix: CollIndex i scoping error, index j added
    // - Reviewed by Rayees Pasha 3/28/02
    CollIndex j;
    for (j = 0; j < colDefArray.entries(); j++)
      if (colName == colDefArray[j]->getColumnName()) break;
    if (j < colDefArray.entries()) {
      // Set the column to be not nullable.
      colDefArray[j]->getColumnDataType()->setNullable(FALSE);

      // If user didn't specify any DEFAULT clause,
      // set it to NO DEFAULT (instead of the default of DEFAULT NULL).
      if (colDefArray[j]->getDefaultClauseStatus() == ElemDDLColDef::DEFAULT_CLAUSE_NOT_SPEC) {
        colDefArray[j]->setDefaultClauseStatus(ElemDDLColDef::NO_DEFAULT_CLAUSE_SPEC);
        CMPASSERT(!colDefArray[j]->getDefaultValueExpr());
      }
    }
  }
}  // setColNotNullableIfCheckIsISNOTNULL()

// a virtual function for performing name
// binding within the Create Table tree
//
ExprNode *StmtDDLCreateTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  //
  // expands table name
  //

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  // if table name is specified as  HBASE."_MAP_".<tab>, then it is an external table
  if ((NOT isExternal()) && (tableQualName_.getCatalogName() == HBASE_SYSTEM_CATALOG) &&
      (tableQualName_.getSchemaName() == HBASE_MAP_SCHEMA)) {
    setIsExternal(TRUE);
  }

  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  if (getIsLikeOptionSpecified()) {
    origLikeSourceTableQualName_ = likeSourceTableCorrName_.getQualifiedNameObj();
    likeSourceTableCorrName_.getQualifiedNameObj().applyDefaultsValidate(pBindWA->getDefaultSchema());
    if (pBindWA->violateAccessDefaultSchemaOnly(likeSourceTableCorrName_.getQualifiedNameObj())) return this;
  }

  //
  // binds all children
  //

  // binds Primary Key constraint (if exists)

  if (getAddConstraintPK()) {
    getAddConstraintPK()->bindNode(pBindWA);
  }

  CollIndex index;
  Set_SqlParser_Flags(ALLOW_ADD_CONSTRAINT_NOT_DROPPABLE);

  // binds addConstraintCheckArray_

  StmtDDLAddConstraintCheck *pAddConstraintCheck = NULL;
  for (index = 0; index < addConstraintCheckArray_.entries(); index++) {
    pAddConstraintCheck = addConstraintCheckArray_[index];
    pAddConstraintCheck->bindNode(pBindWA);
    setColNotNullableIfCheckIsISNOTNULL(pBindWA, pAddConstraintCheck, getColDefArray());
  }

  // checks addConstraintRIArray_

  StmtDDLAddConstraintRI *pAddConstraintRI = NULL;
  for (index = 0; index < addConstraintRIArray_.entries(); index++) {
    pAddConstraintRI = addConstraintRIArray_[index];
    pAddConstraintRI->bindNode(pBindWA);
  }

  // checks addConstraintUniqueArray_

  StmtDDLAddConstraintUnique *pAddConstraintUnique = NULL;
  for (index = 0; index < addConstraintUniqueArray_.entries(); index++) {
    pAddConstraintUnique = addConstraintUniqueArray_[index];
    pAddConstraintUnique->bindNode(pBindWA);
  }

  Reset_SqlParser_Flags(ALLOW_ADD_CONSTRAINT_NOT_DROPPABLE);

  //
  // binds all partition parse nodes associating
  // this Create Table parse node.
  //

  ComASSERT(getPartitionArray().entries() > 0);
  for (index = 0; index < getPartitionArray().entries(); index++) {
    getPartitionArray()[index]->bindNode(pBindWA);
  }

  if (getPartitionArray()[0]->castToElemDDLPartitionSystem()) {
    //
    // Note that class ElemDDLPartitionRange is
    // derived from class ElemDDLPartitionSystem.
    // Also note that the primary partition parse
    // node has been bound.
    //
    guardianLocation_ = getPartitionArray()[0]->castToElemDDLPartitionSystem()->getGuardianLocation();
  }
  //
  // sets a flag to let user know that the parse has
  // been bound.
  //
  markAsBound();

  checkHbasePartitionKey();

  return this;
}

// a virtual function for performing name
// binding within the Create Hbase Table tree
//
ExprNode *StmtDDLCreateHbaseTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  //
  // expands table name
  //

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// New comments
//--This check will cover following cases:
//------When primary key columns specified------
//--1.PARTITION BY clause is not allowed for trafodion table.
//--2.STORE BY column list(if any) == primay key column list, also ordinally equal.
//------When no primary key columns specified------
//--3.PARTITION BY clause is not allowed for trafodion table.
//--[Already catched by 1195]Salt columns must be subset of clustering key, in any order.
//--[Already catched by 1195]There must be STORE BY or PRIMARY KEY columns.
void StmtDDLCreateTable::checkHbasePartitionKey() {
  // Called as last step of bind.
  if (!nodeIsBound()) return;

  // Make sure table created is in TRAFODION catalog
  QualifiedName qualifiedTableName = getTableNameAsQualifiedName();
  if (NOT(qualifiedTableName.getCatalogName() == TRAFODION_SYSCAT_LIT)) return;

  // Disable PARTITION BY clause for tables of TRAFODION catalog.
  if (getPartitionKeyColRefArray().entries() > 0) {
    *CmpCommon::diags() << DgSqlCode(-1199);
    return;
  }
  // Check primary key and clustering key consistant
  if (getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION && getKeyColumnArray().entries() > 0 &&
      getPrimaryKeyColRefArray().entries() > 0) {
    if (getPrimaryKeyColRefArray() != getKeyColumnArray()) {
      *CmpCommon::diags() << DgSqlCode(-1193) << DgString0("clustering key") << DgString1("STORE BY");
      return;
    }
  }
}

ExprNode *StmtDDLCreateSequence::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  if (applyDefaultsAndValidateObject(pBindWA, &seqQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  // update default values
  ElemDDLSGOptions *sgo = getSGoptions();

  if (isAlter()) {
    CorrName cn(getSeqNameAsQualifiedName(), (CollHeap *)NULL, NAString());
    cn.setSpecialType(ExtendedQualName::SG_TABLE);
    NATable *naTable = pBindWA->getNATable(cn);
    if (naTable == NULL || pBindWA->errStatus()) {
      return this;
    }

    const SequenceGeneratorAttributes *sga = naTable->getSGAttributes();
    if (sga) {
      ElemDDLSGOptions tempSGO;
      tempSGO.importSGA(sga);

      if (sgo) {
        if (sgo->isMaxValueSpecified() && sgo->isNoMaxValue()) sgo->recomputeMaxValue(sga->getSGFSDataType());
        tempSGO.importSGO(sgo);
      }

      if (tempSGO.validate(1 /*alter sequence*/)) {
        return this;
      }
      // if alter sequence set nextval, we need to know ORDER and INCREMENT BY attributes
      if (sgo && sgo->isNextValSpecified() && sga->getSGOrder()) {
        sgo->setOrder(ORDER);
        sgo->setIncrement(sga->getSGIncrement());
      }
    }
  } else {
    if (sgo->getFSDataType() == COM_UNKNOWN_FSDT) sgo->setFSDataType(COM_SIGNED_BIN64_FSDT);
    if (sgo->validate(isAlter() ? 1 : 0)) {
      pBindWA->setErrStatus();

      return this;
    }
  }

  markAsBound();

  return this;
}

ExprNode *StmtDDLDropSequence::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  if (applyDefaultsAndValidateObject(pBindWA, &seqQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  markAsBound();

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCreateView
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the CreateView tree
//
ExprNode *StmtDDLCreateView::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // QSTUFF
  CmpMain cmp;
  NormWA normWA(CmpCommon::context());
  // QSTUFF

  pBindWA->setNameLocListPtr(&nameLocList_);
  pBindWA->setUsageParseNodePtr(this);
  viewQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(viewQualName_)) return this;

  ParNameLoc *pNameLoc = nameLocList_.getNameLocPtr(viewQualName_.getNamePosition());
  ComASSERT(pNameLoc);
  pNameLoc->setExpandedName(viewQualName_.getQualifiedNameAsAnsiString());

  // not allowed on volatile objects.
  CmpCommon::context()->sqlSession()->disableVolatileSchemaInUse();

  for (int i = 0; i < getArity(); i++) {
    if (getChild(i)) {
      if (getChild(i)->castToElemDDLNode()) {
        getChild(i)->castToElemDDLNode()->bindNode(pBindWA);
      } else if (getChild(i)->castToRelExpr()) {
        pBindWA->resetNumTablesPerSelect();
        pBindWA->resetNumTablesPerQuery();
        RelExpr *queryExpr = getChild(i)->castToRelExpr()->bindNode(pBindWA);
        if (pBindWA->isBindingIUD()) {
          // cannot have IUD in create view
          *CmpCommon::diags()
              << DgSqlCode(-3242)
              << DgString0(
                     "Cannot have embedded insert, update, delete or merge operators in a create view statement.");

          pBindWA->setErrStatus();
          return this;
        }

        if (!pBindWA->errStatus()) {
          // If the operator type not REL_ROOT, reset VolatileSchemaInUse and
          // assert
          if (queryExpr->getOperatorType() != REL_ROOT) {
            CmpCommon::context()->sqlSession()->restoreVolatileSchemaInUse();
            CMPASSERT(FALSE);
          }

          ((RelRoot *)queryExpr)->setRootFlag(TRUE);

          // we need to check unique constraints on joins when
          // when binding a view containing an embedded update
          if (pBindWA->isEmbeddedIUDStatement()) {
            CmpCommon::context()->sqlSession()->restoreVolatileSchemaInUse();
            *CmpCommon::diags() << DgSqlCode(-3218);
            pBindWA->setErrStatus();
            return this;
          }

          queryExpr = cmp.transform(normWA, queryExpr);

          ComASSERT(queryExpr->getOperatorType() == REL_ROOT);
          RelRoot *rootExpr = (RelRoot *)queryExpr;
          isUpdatable_ = rootExpr->isUpdatableView(isInsertable_);

          if (getViewColDefArray().entries() == 0) {
            const ColumnDescList &colDescList = *rootExpr->getRETDesc()->getColumnList();
            for (CollIndex i = 0; i < colDescList.entries(); i++) {
              // Get internal colname, not external Ansi-format string
              getViewColDefArray().insert(new (pBindWA->wHeap())
                                              ElemDDLColViewDef(colDescList[i]->getColRefNameObj().getColName()));
            }
          }

          // Change to always flush information to the metadata
          if (getIsUpdatable()) {
            //
            // For certain cases, the binder could guess that
            // the view is not updatable.  Better check to
            // make sure that the binder guessed correctly.
            //
            if (getViewUsages().isViewSurelyNotUpdatable()) {
              CmpCommon::context()->sqlSession()->restoreVolatileSchemaInUse();
              CMPASSERT(FALSE);
            }
          }

          QueryAnalysis *queryAnalysis = CmpCommon::statement()->initQueryAnalysis();
          queryExpr = cmp.normalize(normWA, queryExpr);
          if (queryExpr == NULL) {
            pBindWA->setErrStatus();
            return this;
          }
          queryExpr->synthLogProp();

        }  // if (!pBindWA->errStatus())
      } else {
        ABORT("internal logic error");
      }
    }
  }

  CmpCommon::context()->sqlSession()->restoreVolatileSchemaInUse();
  markAsBound();

  pBindWA->setNameLocListPtr(NULL);
  pBindWA->setUsageParseNodePtr(NULL);

  return this;
}  // StmtDDLCreateView::bindNode

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropCatalog
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropCatalog tree
//
ExprNode *StmtDDLDropCatalog::bindNode(BindWA * /* bindWAPtr */) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropConstraint
// -----------------------------------------------------------------------

ExprNode *StmtDDLDropConstraint::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  QualifiedName &tableQualName = getTableNameAsQualifiedName();
  tableQualName.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName)) return this;

  QualifiedName &constraintQualName = getConstraintNameAsQualifiedName();
  constraintQualName.applyDefaults(tableQualName);
  if (pBindWA->violateAccessDefaultSchemaOnly(constraintQualName)) return this;

  if (tableQualName.getCatalogName() NEQ constraintQualName.getCatalogName()
          OR tableQualName.getSchemaName() NEQ constraintQualName.getSchemaName()) {
    *CmpCommon::diags() << DgSqlCode(-3050);
    pBindWA->setErrStatus();
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropIndex
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropIndex tree
//
ExprNode *StmtDDLDropIndex::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original index name specified by user
  origIndexQualName_ = indexQualName_;

  indexQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(indexQualName_)) return this;

  markAsBound();
  return this;
}

// ----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropLibrary
// ----------------------------------------------------------------------

ExprNode *StmtDDLDropLibrary::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  libraryName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(libraryName_)) return this;

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropSchema
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropSchema tree
//
ExprNode *StmtDDLDropSchema::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  if (pBindWA->raiseAccessDefaultSchemaOnlyError()) return this;

  // expand schema Name.

  if (schemaQualName_.getCatalogName().isNull()) {
    schemaQualName_.setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
  }

  schemaName_ =
      ToAnsiIdentifier(schemaQualName_.getCatalogName()) + "." + ToAnsiIdentifier(schemaQualName_.getSchemaName());

  markAsBound();

  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterSchema
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the AlterSchema tree
//
ExprNode *StmtDDLAlterSchema::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  if (pBindWA->raiseAccessDefaultSchemaOnlyError()) return this;

  // expand schema Name.

  if (schemaQualName_.getCatalogName().isNull()) {
    schemaQualName_.setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
  }

  schemaName_ =
      ToAnsiIdentifier(schemaQualName_.getCatalogName()) + "." + ToAnsiIdentifier(schemaQualName_.getSchemaName());

  markAsBound();

  return this;
}

// ----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropSynonym
// ----------------------------------------------------------------------

ExprNode *StmtDDLDropSynonym::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  synonymName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(synonymName_)) return this;

  markAsBound();
  return this;
}

// ----------------------------------------------------------------------
// defination of method bindnode() for class StmtDDLDropModule
// ----------------------------------------------------------------------

ExprNode *StmtDDLDropModule::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  moduleQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(moduleQualName_)) return this;

  moduleName_ = moduleQualName_.getQualifiedNameAsAnsiString();
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropTable
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropTable tree
//
ExprNode *StmtDDLDropTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  // if tablename is specified as HBASE."_MAP_".<tab>, then it is an external table
  if ((NOT isExternal()) && (tableQualName_.getCatalogName() == HBASE_SYSTEM_CATALOG) &&
      (tableQualName_.getSchemaName() == HBASE_MAP_SCHEMA)) {
    setIsExternal(TRUE);
  }

  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropHbaseTable
// -----------------------------------------------------------------------

ExprNode *StmtDDLDropHbaseTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropView
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropView tree
//
ExprNode *StmtDDLDropView::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  viewQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(viewQualName_)) return this;

  markAsBound();
  return this;
}

// ----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropExceptionTable
// ----------------------------------------------------------------------

ExprNode *StmtDDLDropExceptionTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  objectReference_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(objectReference_)) return this;

  // If a single exception table is to be dropped verify
  // exception table & schema match
  if (dropType_ == COM_DROP_SINGLE) {
    exceptionName_.applyDefaults(pBindWA->getDefaultSchema());
    if (pBindWA->violateAccessDefaultSchemaOnly(exceptionName_)) return this;
    if (exceptionName_.getSchemaName() NEQ objectReference_.getSchemaName()) {
      *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("Exception Tables");
      pBindWA->setErrStatus();
    }
  }

  markAsBound();
  return this;
}

//----------------------------------------------------------------------------
// StmtDDLAlterTableAlterColumnLoggable
//----------------------------------------------------------------------------

ExprNode *StmtDDLAlterTableAlterColumnLoggable::bindNode(BindWA *pBindWA) {
  StmtDDLAlterTable::bindNode(pBindWA);

  CorrName tableCorrName(getTableNameAsQualifiedName(), (CollHeap *)0,
                         NAString()  // no corr. name
  );

  NATable *pNaTable = pBindWA->getNATable(tableCorrName,
                                          FALSE  // don't collect usage info
  );

  if (pBindWA->errStatus()) {
    return this;
  }

  const NAColumnArray &columnArray = pNaTable->getNAColumnArray();

  NAColumn *pColumn = columnArray.getColumn(columnName_.data());

  if (NULL == pColumn) {
    // If we are getting -4001,-4002 or -4003 when binding an ORDER BY column,
  } else {
    columnNum_ = pColumn->getPosition();
  }

  return this;
}  // StmtDDLAlterTableAlterColumnLoggable::bindNode

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropPackage
// -----------------------------------------------------------------------
ExprNode *StmtDDLDropPackage::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  int defaulted = packageQualName_.applyDefaults(pBindWA->getDefaultSchema());

  //
  // diagnose if non-ANSI defaults were applied to the name
  //
  if (NOT packageQualName_.verifyAnsiNameQualification(defaulted)) {
    *CmpCommon::diags() << DgSqlCode(-3206);
    pBindWA->setErrStatus();
  }

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLDropRoutine
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the DropRoutine tree
//
ExprNode *StmtDDLDropRoutine::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  int defaulted = routineQualName_.applyDefaults(pBindWA->getDefaultSchema());
  int actionDefaulted = 0;
  if (pBindWA->violateAccessDefaultSchemaOnly(routineQualName_)) return this;
  if (NOT routineActionQualName_.getObjectName().isNull()) {
    ComASSERT(getRoutineType() EQU COM_ACTION_UDF_TYPE);
    actionDefaulted = routineActionQualName_.applyDefaults(pBindWA->getDefaultSchema());
    if (pBindWA->violateAccessDefaultSchemaOnly(routineActionQualName_)) return this;
  }

  //
  // diagnose if non-ANSI defaults were applied to the name.
  //
  if (NOT routineQualName_.verifyAnsiNameQualification(defaulted)
          OR NOT routineActionQualName_.verifyAnsiNameQualification(actionDefaulted)) {
    *CmpCommon::diags() << DgSqlCode(-3206);
    pBindWA->setErrStatus();
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLAlterTable
// -----------------------------------------------------------------------

ExprNode *StmtDDLAlterTable::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  // remember the original table name specified by user
  origTableQualName_ = tableQualName_;

  //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualName_)) return this;

  CollIndex index;
  if (getOperatorType() == DDL_ALTER_TABLE_ADD_COLUMN) {
    StmtDDLAlterTableAddColumn *altAddCol = castToStmtDDLAlterTableAddColumn();

    if (altAddCol->getAddConstraintPK()) {
      altAddCol->getAddConstraintPK()->bindNode(pBindWA);
    }

    Set_SqlParser_Flags(ALLOW_ADD_CONSTRAINT_NOT_DROPPABLE);

    // Bind Check constraints

    StmtDDLAddConstraintCheck *pAddConstraintCheck = NULL;
    StmtDDLAddConstraintCheckArray &addConstraintCheckArray = altAddCol->getAddConstraintCheckArray();
    for (index = 0; index < addConstraintCheckArray.entries(); index++) {
      pAddConstraintCheck = addConstraintCheckArray[index];
      pAddConstraintCheck->bindNode(pBindWA);
      ElemDDLColDefArray colDefArray;
      colDefArray.insert((altAddCol->getColToAdd())->castToElemDDLColDef());
      setColNotNullableIfCheckIsISNOTNULL(pBindWA, pAddConstraintCheck, colDefArray);
    }

    // Bind RI Constraints

    StmtDDLAddConstraintRI *pAddConstraintRI = NULL;
    StmtDDLAddConstraintRIArray &addConstraintRIArray = altAddCol->getAddConstraintRIArray();
    for (index = 0; index < addConstraintRIArray.entries(); index++) {
      pAddConstraintRI = addConstraintRIArray[index];
      pAddConstraintRI->bindNode(pBindWA);
    }

    // Bind Unique constraints

    StmtDDLAddConstraintUnique *pAddConstraintUnique = NULL;
    StmtDDLAddConstraintUniqueArray &addConstraintUniqueArray = altAddCol->getAddConstraintUniqueArray();
    for (index = 0; index < addConstraintUniqueArray.entries(); index++) {
      pAddConstraintUnique = addConstraintUniqueArray[index];
      pAddConstraintUnique->bindNode(pBindWA);
    }

    Reset_SqlParser_Flags(ALLOW_ADD_CONSTRAINT_NOT_DROPPABLE);
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLGrant
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Grant tree
//
ExprNode *StmtDDLGrant::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  //  objectQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &objectQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(objectQualName_)) return this;

  objectName_ = objectQualName_.getQualifiedNameAsAnsiString();

  // If specified, validate actionQualName
  if (actionQualName_) {
    if (applyDefaultsAndValidateObject(pBindWA, actionQualName_)) {
      pBindWA->setErrStatus();
      return this;
    }
    if (pBindWA->violateAccessDefaultSchemaOnly(*actionQualName_)) return this;
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLSchGrant
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Schema Grant tree
//
ExprNode *StmtDDLSchGrant::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  if (schemaQualName_.getCatalogName().isNull()) {
    schemaQualName_.setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
  }
  QualifiedName qn("", ToAnsiIdentifier(schemaQualName_.getSchemaName()),
                   ToAnsiIdentifier(schemaQualName_.getCatalogName()));
  if (pBindWA->violateAccessDefaultSchemaOnly(qn)) return this;

  schemaName_ =
      ToAnsiIdentifier(schemaQualName_.getCatalogName()) + "." + ToAnsiIdentifier(schemaQualName_.getSchemaName());
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLGiveAll
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Give All tree
//
ExprNode *StmtDDLGiveAll::bindNode(BindWA *pBindWA) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLGiveCatalog
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Give Catalog tree
//
ExprNode *StmtDDLGiveCatalog::bindNode(BindWA *pBindWA) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLGiveObject
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Give Object tree
//
ExprNode *StmtDDLGiveObject::bindNode(BindWA *pBindWA) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLGiveSchema
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Give Schema tree
//
ExprNode *StmtDDLGiveSchema::bindNode(BindWA *pBindWA) {
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLRevoke
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Revoke tree
//
ExprNode *StmtDDLRevoke::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  //  objectQualName_.applyDefaults(pBindWA->getDefaultSchema());
  if (applyDefaultsAndValidateObject(pBindWA, &objectQualName_)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(objectQualName_)) return this;

  objectName_ = objectQualName_.getQualifiedNameAsAnsiString();

  // If specified, validate actionQualName
  if (actionQualName_) {
    if (applyDefaultsAndValidateObject(pBindWA, actionQualName_)) {
      pBindWA->setErrStatus();
      return this;
    }
    if (pBindWA->violateAccessDefaultSchemaOnly(*actionQualName_)) return this;
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLSchRevoke
// -----------------------------------------------------------------------

//
// a virtual function for performing name
// binding within the Revoke tree
//
ExprNode *StmtDDLSchRevoke::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  if (schemaQualName_.getCatalogName().isNull()) {
    schemaQualName_.setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
  }
  QualifiedName qn("", ToAnsiIdentifier(schemaQualName_.getSchemaName()),
                   ToAnsiIdentifier(schemaQualName_.getCatalogName()));
  if (pBindWA->violateAccessDefaultSchemaOnly(qn)) return this;

  schemaName_ =
      ToAnsiIdentifier(schemaQualName_.getCatalogName()) + "." + ToAnsiIdentifier(schemaQualName_.getSchemaName());

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLRegisterUser
// -----------------------------------------------------------------------
//
////
// a virtual function for performing name
// binding within the Register user tree.
//
ExprNode *StmtDDLRegisterUser::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLUserGroup
// -----------------------------------------------------------------------
//
////
// a virtual function for performing name
// binding within the Register user tree.
//
ExprNode *StmtDDLUserGroup::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLTenant
// -----------------------------------------------------------------------
//
// a virtual function for performing name binding within the tenant tree.
//
ExprNode *StmtDDLTenant::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  if (schemaList_) {
    NAList<NAString> newSchemaList(pBindWA->wHeap(), schemaList_->entries());

    // set default catalog if not found and check for duplicate schema names
    for (int i = 0; i < schemaList_->entries(); i++) {
      const SchemaName *schemaName = (*schemaList_)[i];
      if (schemaName && schemaName->getCatalogName().isNull())
        ((SchemaName *)schemaName)->setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
      if (newSchemaList.contains(schemaName->getSchemaNameAsAnsiString())) {
        NAString msg(schemaName->getSchemaNameAsAnsiString());
        msg += " schema";
        *CmpCommon::diags() << DgSqlCode(-3103) << DgString0(msg.data());
        pBindWA->setErrStatus();
        return NULL;
      } else
        newSchemaList.insert(schemaName->getSchemaNameAsAnsiString());
    }
  }

  if (defaultSchema_) {
    if (defaultSchema_->getCatalogName().isNull())
      defaultSchema_->setCatalogName(pBindWA->getDefaultSchema().getCatalogName());
  }

  if (groupList_) {
    // check for duplicate group names
    NAList<NAString> newGroupList(pBindWA->wHeap(), groupList_->entries());
    for (CollIndex i = 0; i < groupList_->entries(); i++) {
      ElemDDLTenantGroup *group = (*groupList_)[i]->castToElemDDLTenantGroup();
      if (newGroupList.contains(group->getGroupName())) {
        NAString msg(group->getGroupName());
        msg += " group";
        *CmpCommon::diags() << DgSqlCode(-3103) << DgString0(msg.data());
        pBindWA->setErrStatus();
        return NULL;
      } else
        newGroupList.insert(group->getGroupName());
    }
  }

  if (rgroupList_) {
    // check for duplicate resourcegroup names
    NAList<NAString> newGroupList(pBindWA->wHeap(), rgroupList_->entries());
    for (CollIndex i = 0; i < rgroupList_->entries(); i++) {
      ElemDDLTenantResourceGroup *group = (*rgroupList_)[i]->castToElemDDLTenantResourceGroup();
      if (newGroupList.contains(group->getGroupName())) {
        NAString msg(group->getGroupName());
        msg += " resource group";
        *CmpCommon::diags() << DgSqlCode(-3103) << DgString0(msg.data());
        pBindWA->setErrStatus();
        return NULL;
      } else
        newGroupList.insert(group->getGroupName());
    }
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLResourceGroup
// -----------------------------------------------------------------------
//
// a virtual function for performing name binding within a resource group
//
ExprNode *StmtDDLResourceGroup::bindNode(BindWA *pBindWA) {
  if (CmpCommon::getDefault(ALLOW_TENANT_RESOURCE_GROUPS) == DF_OFF) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Resource group support is not enabled.");
    return NULL;
  }

  ComASSERT(pBindWA);
  if (nodeList_) {
    NAList<NAString> newNodeList(pBindWA->wHeap(), nodeList_->entries());

    // set check for duplicate node names
    for (int i = 0; i < nodeList_->entries(); i++) {
      NAString *nodeName = (NAString *)(*nodeList_)[i];
      size_t pos = nodeName->first('.');
      if (pos != NA_NPOS) nodeName->remove(pos);

      for (int j = 0; j < newNodeList.entries(); j++) {
        NAString newName = newNodeList[j];

        if (*nodeName == newName) {
          NAString msg(nodeName->data());
          msg += " node name";
          *CmpCommon::diags() << DgSqlCode(-3103) << DgString0(msg.data());
          pBindWA->setErrStatus();
          return NULL;
        }
      }
      newNodeList.insert(*nodeName);
    }
  }

  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLRoleGrant
// -----------------------------------------------------------------------
//
// a virtual function for performing name
// binding within the Create role tree.
//
ExprNode *StmtDDLRoleGrant::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLCreateRole
// -----------------------------------------------------------------------
//
// a virtual function for performing name
// binding within the Create role tree.
//
ExprNode *StmtDDLCreateRole::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// bindNode() for class StmtDDLAlterUser
// -----------------------------------------------------------------------
//
////
// a virtual function for performing name
// binding within the Alter User tree.
//
ExprNode *StmtDDLAlterUser::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  markAsBound();
  return this;
}

// -----------------------------------------------------------------------
// definition of method bindNode() for class StmtDDLCommentOn
// -----------------------------------------------------------------------

ExprNode *StmtDDLCommentOn::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  objectName_.applyDefaults(pBindWA->getDefaultSchema());
  if (pBindWA->violateAccessDefaultSchemaOnly(objectName_)) return this;

  if (this->type_ == COMMENT_ON_TYPE_COLUMN) {
    if (NULL == colRef_) {
      ComASSERT(pBindWA);
    } else {
      ActiveSchemaDB()->getNATableDB()->useCache();

      CorrName cn(objectName_, STMTHEAP);

      NATable *naTable = pBindWA->getNATable(cn);
      if (naTable == NULL || pBindWA->errStatus()) {
        *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(cn.getExposedNameAsAnsiString());

        return this;
      }

      const NAColumnArray &nacolArr = naTable->getNAColumnArray();
      const NAColumn *nacol = nacolArr.getColumn(getColName());
      if (!nacol) {
        // column doesnt exist. Error.
        *CmpCommon::diags() << DgSqlCode(-1009) << DgColumnName(getColName());

        return this;
      }

      isViewCol_ = (naTable->getViewText() ? TRUE : FALSE);
      colNum_ = nacol->getPosition();
    }
  }

  markAsBound();
  return this;
}

void StmtDDLAlterSharedCache::resolveName() {
  // parse the name into its components
  NAString ansiName = qualName_.getQualifiedNameAsAnsiString();

  switch (subject_) {
    case TABLE:
      qualName_ = QualifiedName(ansiName, 1, STMTHEAP);
      break;

    case SCHEMA:
      ansiName += ".";
      ansiName += SEABASE_SCHEMA_OBJECTNAME_EXT;
      qualName_ = QualifiedName(ansiName, 2, STMTHEAP);
      break;

    case ALL:
      qualName_ = QualifiedName(ansiName, 1, STMTHEAP);
      break;
  }
}

ExprNode *StmtDDLAlterSharedCache::bindNode(BindWA *bindWAPtr) {
  ComASSERT(bindWAPtr);

  // remember the original table name specified by user
  origQualName_ = qualName_;

  if (opForTable()) {
    //  tableQualName_.applyDefaults(pBindWA->getDefaultSchema());
    if (applyDefaultsAndValidateObject(bindWAPtr, &qualName_)) {
      bindWAPtr->setErrStatus();
      return NULL;
    }

    ActiveSchemaDB()->getNATableDB()->useCache();

    // If a table has LOB objects for version 2, naTable will not be found and
    // will cause the current transaction to abort.  We want to ignore not found
    // tables and continue, so skip the check for now.
#if 0
     CorrName cn(qualName_, STMTHEAP);
     
     NATable *naTable = bindWAPtr->getNATable(cn);
     if (naTable == NULL || bindWAPtr->errStatus())
       {
           // If the table does not exist, NATableDB::get() will raise
           // SQL error -4082. There is no need to raise it here again.
           return NULL;
       }
#endif
  }

  markAsBound();
  return this;
}

ExprNode *StmtDDLAlterTableRenamePartition::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  // get natable object
  QualifiedName &tableQualifiedName = getTableNameAsQualifiedName();

  if (applyDefaultsAndValidateObject(pBindWA, &tableQualifiedName)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualifiedName)) return this;

  CorrName tableCorrName(tableQualifiedName, (CollHeap *)0, NAString());
  NATable *naTable = pBindWA->getNATable(tableCorrName, FALSE);
  if (pBindWA->errStatus()) return this;

  // get tableDesc object
  TableDesc *tableDesc = NULL;
  tableDesc = pBindWA->createTableDesc(naTable, tableCorrName, FALSE, NULL);
  LIST(NAString) *partitionName = NULL;

  if (isForClause_) {
    // alter table p rename partition for(1) to p2
    ItemExprList valList(partitionKey_, pBindWA->wHeap());
    partitionName = tableDesc->getMatchedPartInfo(pBindWA, valList);
    if (!partitionName || partitionName->entries() == 0) {
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("The partition number is invalid or out-of-range");
      pBindWA->setErrStatus();
      return this;
    }
    partNameList_ = partitionName;
  }

  markAsBound();
  return this;
}

ExprNode *StmtDDLAlterTableSplitPartition::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);
  // get natable object
  QualifiedName &tableQualifiedName = getTableNameAsQualifiedName();

  if (applyDefaultsAndValidateObject(pBindWA, &tableQualifiedName)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualifiedName)) return this;

  CorrName tableCorrName(tableQualifiedName, (CollHeap *)0, NAString());
  NATable *naTable = pBindWA->getNATable(tableCorrName, FALSE);
  if (naTable == NULL) {
    pBindWA->setErrStatus();
    return this;
  }

  // get tableDesc object
  TableDesc *tableDesc = NULL;
  tableDesc = pBindWA->createTableDesc(naTable, tableCorrName, FALSE, NULL);
  LIST(NAString) *partEntityNames = NULL;
  LIST(NAString) *partEntityNamesfromKey = NULL;

  ElemDDLPartitionNameAndForValues *partitionElem = getTablePartition()->castToElemDDLPartitionNameAndForValues();

  ComASSERT(partitionElem);

  NAString partName("");
  NAString partEntityName("");
  if (partitionElem->isPartitionForValues()) {
    ItemExprList valList(partitionElem->getPartitionForValues(), pBindWA->wHeap());
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, valList);
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partitionElem->getPartitionForValues()->getText());
      return NULL;
    }
    partName = partitionElem->getPartitionForValues()->getText();
    partEntityName = partEntityNames->at(0);
  } else {
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, partitionElem->getPartitionName());
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partitionElem->getPartitionName());
      return NULL;
    }
    partName = partitionElem->getPartitionName();
    partEntityName = partEntityNames->at(0);
  }

  ItemExprList valList(splitedKey_, pBindWA->wHeap());
  partEntityNamesfromKey = tableDesc->getMatchedPartInfo(pBindWA, valList);
  if (!partEntityNamesfromKey || partEntityNamesfromKey->entries() == 0) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partitionElem->getPartitionName());
    return NULL;
  }
  if (partEntityNamesfromKey->at(0) != partEntityNames->at(0)) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("The partition number is invalid or out-of-range");
    return NULL;
  }

  NAPartitionArray naPartArray = naTable->getPartitionArray();

  NAString sp1 = (*splitPartNameList_)[0];
  NAString sp2 = (*splitPartNameList_)[1];

  naPartition_ = NULL;
  NABoolean reportError = FALSE;
  for (int i = 0; i < naPartArray.entries(); i++) {
    NAString tmpEntityName = naPartArray[i]->getPartitionEntityName();
    NAString tmpPTName = naPartArray[i]->getPartitionName();
    if (partEntityName == tmpEntityName) {
      naPartition_ = naPartArray[i];
    }
    if (tmpPTName == sp1) splitstatus_[0] = 1;
    if (tmpPTName == sp2) splitstatus_[1] = 1;
  }
  if (naPartition_ == NULL) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partName);
    return NULL;
  }

  if (splitstatus_[0] == 1 && splitstatus_[1] == 1) {
    *CmpCommon::diags() << DgSqlCode(-1229) << DgString0("split with a exist partition name");
  }

  markAsBound();
  return this;
}

NAPartition *StmtDDLAlterTableMergePartition::getNAPartitionFromElem(BindWA *pBindWA, TableDesc *tableDesc,
                                                                     std::map<NAString, NAPartition *> &partMap,
                                                                     ElemDDLPartitionNameAndForValues *elem) {
  NAPartition *part = NULL;
  std::map<NAString, NAPartition *>::iterator partInfoMapIt;

  LIST(NAString) *partEntityNames = NULL;

  NAString partName("");
  NAString partEntityName("");
  if (elem->isPartitionForValues()) {
    ItemExprList valList(elem->getPartitionForValues(), pBindWA->wHeap());
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, valList);
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(elem->getPartitionForValues()->getText());
      return NULL;
    }
    partName = elem->getPartitionForValues()->getText();
    partEntityName = partEntityNames->at(0);
  } else {
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, elem->getPartitionName());
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(elem->getPartitionName());
      return NULL;
    }
    partName = elem->getPartitionName();
    partEntityName = partEntityNames->at(0);
  }

  partInfoMapIt = partMap.find(partEntityName);
  if (partInfoMapIt == partMap.end()) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partName);
    return NULL;
  }
  part = partInfoMapIt->second;
  if (part == NULL) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partName);
    return NULL;
  }

  return part;
}

ExprNode *StmtDDLAlterTableMergePartition::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  QualifiedName &tableQualifiedName = getTableNameAsQualifiedName();
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualifiedName)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualifiedName)) return this;

  CorrName tableCorrName(tableQualifiedName, (CollHeap *)0, NAString());
  NATable *naTable = pBindWA->getNATable(tableCorrName, FALSE);
  if (naTable == NULL) {
    pBindWA->setErrStatus();
    return this;
  }

  NAPartitionArray naPartArray = naTable->getPartitionArray();

  TableDesc *tableDesc = pBindWA->createTableDesc(naTable, tableCorrName, FALSE, NULL);
  LIST(NAString) *partEntityNames = NULL;

  // generate partition map<PartEntityName, NAPartition*> info for later use
  std::map<NAString, NAPartition *> partInfoMap;

  for (int i = 0; i < naPartArray.entries(); i++) {
    NAString partName = naPartArray[i]->getPartitionEntityName();
    partInfoMap[partName] = naPartArray[i];
  }

  // check 01 all source partitions
  if (getSourcePartitions() != NULL) {
    // src partition list
    ElemDDLPartitionList *srcPartitions = getSourcePartitions()->castToElemDDLPartitionList();
    ElemDDLPartitionNameAndForValues *srcPartitionElem =
        getSourcePartitions()->castToElemDDLPartitionNameAndForValues();

    CMPASSERT(srcPartitions || srcPartitionElem);

    if (srcPartitions) {
      for (int idx = 0; idx < srcPartitions->entries(); idx++) {
        ElemDDLPartitionNameAndForValues *srcPartElem = (*srcPartitions)[idx]->castToElemDDLPartitionNameAndForValues();
        CMPASSERT(srcPartElem);

        // if exists
        NAPartition *partition = NULL;

        partition = getNAPartitionFromElem(pBindWA, tableDesc, partInfoMap, srcPartElem);
        if (partition == NULL) {
          pBindWA->setErrStatus();
          return this;
        }

        // get partition position
        int insert_pos = 0;
        for (; insert_pos < sortedSrcPart_.entries(); insert_pos++) {
          if (partition->getPartPosition() == sortedSrcPart_.at(insert_pos)->getPartPosition()) {
            // this partition is duplicate, ignore it
            insert_pos = -1;
            break;
          } else if (partition->getPartPosition() < sortedSrcPart_.at(insert_pos)->getPartPosition()) {
            break;
          }
        }
        if (insert_pos >= 0) sortedSrcPart_.insertAt(insert_pos, partition);
      }
    } else if (srcPartitionElem) {
      NAPartition *partition = getNAPartitionFromElem(pBindWA, tableDesc, partInfoMap, srcPartitionElem);
      if (partition == NULL) {
        pBindWA->setErrStatus();
        return this;
      }

      sortedSrcPart_.append(partition);
    }
  } else if (getBeginPartition() != NULL && getEndPartition() != NULL) {
    // src partition range, like "p3 to p8"
    ElemDDLPartitionNameAndForValues *beginPartitionElem =
        getBeginPartition()->castToElemDDLPartitionNameAndForValues();
    ElemDDLPartitionNameAndForValues *endPartitionElem = getEndPartition()->castToElemDDLPartitionNameAndForValues();

    CMPASSERT(beginPartitionElem != NULL && endPartitionElem != NULL);

    NAPartition *beginPartition = NULL;
    NAPartition *endPartition = NULL;

    // get begin partition info
    beginPartition = getNAPartitionFromElem(pBindWA, tableDesc, partInfoMap, beginPartitionElem);
    if (beginPartition == NULL) {
      pBindWA->setErrStatus();
      return this;
    }

    // get end partition info
    endPartition = getNAPartitionFromElem(pBindWA, tableDesc, partInfoMap, endPartitionElem);
    if (endPartition == NULL) {
      pBindWA->setErrStatus();
      return this;
    }

    // check src partition range
    if (beginPartition->getPartPosition() > endPartition->getPartPosition()) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("partition range is not right");
      pBindWA->setErrStatus();
      return this;
    }

    for (int i = 0; i < naPartArray.entries(); i++) {
      NAPartition *part = naPartArray[i];
      if (part->getPartPosition() >= beginPartition->getPartPosition() &&
          part->getPartPosition() <= endPartition->getPartPosition()) {
        // get partition position
        int insert_pos = 0;
        for (; insert_pos < sortedSrcPart_.entries(); insert_pos++) {
          if (part->getPartPosition() == sortedSrcPart_.at(insert_pos)->getPartPosition()) {
            // this partition is duplicate, ignore it
            insert_pos = -1;
            break;
          } else if (part->getPartPosition() < sortedSrcPart_.at(insert_pos)->getPartPosition()) {
            break;
          }
        }
        if (insert_pos >= 0) sortedSrcPart_.insertAt(insert_pos, part);
      }
    }
  } else
    CMPASSERT(0);

  if (naTable->partitionType() == RANGE_PARTITION) {
    // check if src partitions is adjacent
    NABoolean isAdjacent = FALSE;
    for (int j = 1; j < sortedSrcPart_.entries(); j++) {
      if (sortedSrcPart_[j]->getPartPosition() - sortedSrcPart_[j - 1]->getPartPosition() > 1) {
        // The specified partition partName does not exist.
        *CmpCommon::diags() << DgSqlCode(-1270);
        pBindWA->setErrStatus();
        return this;
      }
    }
  }

  // check 02 target partition
  NAString tgtPartName = getTargetPartition();

  partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, tgtPartName);
  if (partEntityNames && partEntityNames->entries() > 0) {
    std::map<NAString, NAPartition *>::iterator partInfoMapIt;
    partInfoMapIt = partInfoMap.find(partEntityNames->at(0));
    if (partInfoMapIt != partInfoMap.end()) {
      tgtPart_ = partInfoMapIt->second;
    }
  }

  markAsBound();
  return this;
}

ExprNode *StmtDDLAlterTableExchangePartition::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  QualifiedName &tableQualifiedName = getTableNameAsQualifiedName();
  if (applyDefaultsAndValidateObject(pBindWA, &tableQualifiedName)) {
    pBindWA->setErrStatus();
    return this;
  }
  if (pBindWA->violateAccessDefaultSchemaOnly(tableQualifiedName)) return this;

  CorrName tableCorrName(tableQualifiedName, (CollHeap *)0, NAString());
  NATable *naTable = pBindWA->getNATable(tableCorrName, FALSE);
  if (naTable == NULL) {
    pBindWA->setErrStatus();
    return this;
  }

  TableDesc *tableDesc = pBindWA->createTableDesc(naTable, tableCorrName, FALSE, NULL);
  LIST(NAString) *partEntityNames = NULL;

  ElemDDLPartitionNameAndForValues *partitionElem = getTablePartition()->castToElemDDLPartitionNameAndForValues();
  ComASSERT(partitionElem);

  NAString partName("");
  NAString partEntityName("");
  if (partitionElem->isPartitionForValues()) {
    ItemExprList valList(partitionElem->getPartitionForValues(), pBindWA->wHeap());
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, valList);
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partitionElem->getPartitionForValues()->getText());
      return NULL;
    }
    partName = partitionElem->getPartitionForValues()->getText();
    partEntityName = partEntityNames->at(0);
  } else {
    partEntityNames = tableDesc->getMatchedPartInfo(pBindWA, partitionElem->getPartitionName());
    if (!partEntityNames || partEntityNames->entries() == 0) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partitionElem->getPartitionName());
      return NULL;
    }
    partName = partitionElem->getPartitionName();
    partEntityName = partEntityNames->at(0);
  }

  NAPartitionArray naPartArray = naTable->getPartitionArray();

  naPartition_ = NULL;
  for (int i = 0; i < naPartArray.entries(); i++) {
    NAString tmpEntityName = naPartArray[i]->getPartitionEntityName();
    if (partEntityName == tmpEntityName) {
      naPartition_ = naPartArray[i];
      break;
    }
  }
  if (naPartition_ == NULL) {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(partName);
    return NULL;
  }

  markAsBound();
  return this;
}

//
// End of File
//
