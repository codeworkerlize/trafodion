
#ifndef STMTDDLALTERTABLETRUNCATEPARTITION_H
#define STMTDDLALTERTABLETRUNCATEPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableTruncatePartition.h
 * Description:  class for Alter Table <table-name> Partition ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      2021/02/27
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/sqtypes.h"
#include "optimizer/ItemExpr.h"
#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableTruncatePartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableTruncatePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableTruncatePartition : public StmtDDLAlterTable {
  // Visit an element in a left linear tree list.  Each element
  // contains information about partition attributes in a partition.
  //
  // StmtDDLAlterTableTruncateParititon_visit is a global function
  // defined in StmtDDLAlter.C
  //

  friend void StmtDDLAlterTableTruncateParititon_visit(ElemDDLNode *pThisNode, CollIndex index, ElemDDLNode *pElement);

 public:
  enum globalIndexAction { NONE, UPDATE, INVALIDATE };

  StmtDDLAlterTableTruncatePartition(ElemDDLNode *pPartitionAction, UINT globalIdxAct, CollHeap *heap);
  // StmtDDLAlterTableTruncatePartition(ItemExpr *partitionKey, NABoolean isForClause, UINT globalIdxAct, CollHeap
  // *heap);
  virtual ~StmtDDLAlterTableTruncatePartition();
  virtual StmtDDLAlterTableTruncatePartition *castToStmtDDLAlterTableTruncatePartition();

  ElemDDLPartitionNameAndForValuesArray &getPartNameAndForValuesArray() { return partNameAndForValuesArray_; }
  // methods for tracing
  virtual const NAString getText() const;

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();

 private:
  UINT globalIdxAct_;
  ElemDDLPartitionNameAndForValuesArray partNameAndForValuesArray_;
};

#endif
