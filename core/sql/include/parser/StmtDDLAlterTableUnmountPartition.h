
#ifndef STMTDDLALTERTABLEUNMOUNTPARTITION_H
#define STMTDDLALTERTABLEUNMOUNTPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableUnmountPartition.h
 * Description:  class for Alter Table <table-name>  Unmount Partition ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      9/20/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "StmtDDLAlterTable.h"
#include "ElemDDLPartitionNameAndForValues.h"
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableUnmountPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableUnmountPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableUnmountPartition : public StmtDDLAlterTable {
  friend void StmtDDLAlterTableUnmountParititon_visit(ElemDDLNode *pThisNode, CollIndex index, ElemDDLNode *pElement);

 public:
  // constructor
  StmtDDLAlterTableUnmountPartition(ElemDDLNode *pPartitionAction);

  // virtual destructor
  virtual ~StmtDDLAlterTableUnmountPartition();

  // cast
  virtual StmtDDLAlterTableUnmountPartition *castToStmtDDLAlterTableUnmountPartition();

  ElemDDLPartitionNameAndForValuesArray &getPartitionNameAndForValuesArray() { return partNameAndForValuesArray_; }

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();

  // method for tracing
  virtual const NAString getText() const;

 private:
  ElemDDLPartitionNameAndForValuesArray partNameAndForValuesArray_;

};  // class StmtDDLAlterTableUnmountPartition

#endif  // STMTDDLALTERTABLEUNMOUNTPARTITION_H
