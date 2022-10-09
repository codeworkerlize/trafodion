
#ifndef STMTDDLALTERTABLESPLITPARTITION_H
#define STMTDDLALTERTABLESPLITPARTITION_H
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
#include "parser/StmtDDLAlterTable.h"

class StmtDDLAlterTableSplitPartition;
// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableSplitPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableSplitPartition : public StmtDDLAlterTable {
 public:
  StmtDDLAlterTableSplitPartition(PartitionEntityType entityType, ElemDDLNode *srcPartition, ItemExpr *splitKey,
                                  NAString &newSP1, NAString &newSP2);
  virtual ~StmtDDLAlterTableSplitPartition();
  virtual StmtDDLAlterTableSplitPartition *castToStmtDDLAlterTableSplitPartition();
  inline ItemExpr *splitedKey() { return splitedKey_; }
  ElemDDLNode *getTablePartition() { return partition_; }
  virtual ExprNode *bindNode(BindWA *pBindWA);
  inline const LIST(NAString) * getSplitedPartNameList() { return splitPartNameList_; }
  NAPartition *getNAPartition() { return naPartition_; }
  virtual const NAString getText() const;
  inline short *getStatus() { return splitstatus_; }

 private:
  ElemDDLNode *partition_;
  ItemExpr *splitedKey_;  // split key
  LIST(NAString) * splitPartNameList_;
  NAPartition *naPartition_;
  short splitstatus_[2];
};

#endif
