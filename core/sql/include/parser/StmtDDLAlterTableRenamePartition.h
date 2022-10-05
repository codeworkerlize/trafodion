
#ifndef STMTDDLALTERTABLERENAMEPARTITION_H
#define STMTDDLALTERTABLERENAMEPARTITION_H
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
#include "StmtDDLAlterTable.h"

class StmtDDLAlterTableRenamePartition;
// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableTruncatePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableRenamePartition : public StmtDDLAlterTable {
 public:
  StmtDDLAlterTableRenamePartition(NAString &oldPartName, NAString &newPartName, NABoolean isForClause, CollHeap *heap);
  StmtDDLAlterTableRenamePartition(ItemExpr *partitionKey, NAString &newPartName, NABoolean isForClause,
                                   CollHeap *heap);
  virtual ~StmtDDLAlterTableRenamePartition();
  virtual StmtDDLAlterTableRenamePartition *castToStmtDDLAlterTableRenamePartition();
  inline NAString &getOldPartName() { return oldPartName_; }
  inline NAString &getNewPartName() { return newPartName_; }
  inline const LIST(NAString) * getPartNameList() { return partNameList_; }
  inline NABoolean getIsForClause() { return isForClause_; }
  virtual ExprNode *bindNode(BindWA *pBindWA);
  virtual const NAString getText() const;

 private:
  NAString oldPartName_;           // old partition name
  NAString newPartName_;           // new partition name
  ItemExpr *partitionKey_;         // rename for(1,2,3) to p1
  LIST(NAString) * partNameList_;  // according to partitionKey_ convert to partition name
  NABoolean isForClause_;
};

#endif
