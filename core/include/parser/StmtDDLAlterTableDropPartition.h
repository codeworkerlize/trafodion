
#ifndef STMTDDLALTERTABLEDROPPARTITION_H
#define STMTDDLALTERTABLEDROPPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableDropPartition.h
 * Description:  class for Alter Table <table-name>  Drop Partition ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      3/8/2021
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLPartitionNameAndForValues.h"
#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableDropPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableDropPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableDropPartition : public StmtDDLAlterTable {
  // Visit an element in a left linear tree list.  Each element
  // contains information about partition attributes in a partition.
  //
  // StmtDDLAlterTableDropParititon_visit is a global function
  // defined in StmtDDLAlter.C
  //

  friend void StmtDDLAlterTableDropParititon_visit(ElemDDLNode *pThisNode, CollIndex index, ElemDDLNode *pElement);

 public:
  // constructor
  StmtDDLAlterTableDropPartition(ElemDDLNode *pPartitionAction, NABoolean isSubpartition = FALSE)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_DROP_PARTITION, pPartitionAction), isSubpartition_(isSubpartition) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableDropPartition();

  // cast
  virtual StmtDDLAlterTableDropPartition *castToStmtDDLAlterTableDropPartition();

  ElemDDLPartitionNameAndForValuesArray &getPartitionNameAndForValuesArray() { return partNameAndForValuesArray_; }

  const NABoolean isSubpartition() const { return isSubpartition_; }

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();

  // method for tracing
  virtual const NAString getText() const;

 private:
  NABoolean isSubpartition_;
  ElemDDLPartitionNameAndForValuesArray partNameAndForValuesArray_;
};  // class StmtDDLAlterTableDropPartition

#endif  // STMTDDLALTERTABLEDROPPARTITION_H
