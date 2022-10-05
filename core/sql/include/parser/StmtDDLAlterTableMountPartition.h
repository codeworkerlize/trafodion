
#ifndef STMTDDLALTERTABLEMOUNTPARTITION_H
#define STMTDDLALTERTABLEMOUNTPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableMountPartition.h
 * Description:  class for Alter Table <table-name>  Mount Partition ...
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

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableMountPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableMountPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableMountPartition : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableMountPartition(ElemDDLPartitionV2 *tgtPartition, NAString s, NABoolean v);

  // virtual destructor
  virtual ~StmtDDLAlterTableMountPartition();

  // cast
  virtual StmtDDLAlterTableMountPartition *castToStmtDDLAlterTableMountPartition();

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

  ElemDDLPartitionV2 *getTargetPartition() { return targetPartition_; }
  NAString getTargetPartitionName() { return targetPartitionName_; }

  NABoolean getValidation() { return validation_; }

  void setValidation(NABoolean v) { validation_ = v; }

 private:
  ElemDDLPartitionV2 *targetPartition_;
  NABoolean validation_;
  NAString targetPartitionName_;

};  // class StmtDDLAlterTableMountPartition

#endif  // STMTDDLALTERTABLEMOUNTPARTITION_H
