
#ifndef STMTDDLALTERTABLEPARTITION_H
#define STMTDDLALTERTABLEPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTablePartition.h
 * Description:  class for Alter Table <table-name> Partition ...
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

#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTablePartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTablePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTablePartition : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTablePartition(ElemDDLNode *pPartitionAction);

  // virtual destructor
  virtual ~StmtDDLAlterTablePartition();

  // cast
  virtual StmtDDLAlterTablePartition *castToStmtDDLAlterTablePartition();

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class StmtDDLAlterTablePartition

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTablePartition
// -----------------------------------------------------------------------

//
// accessor
//

inline ElemDDLNode *StmtDDLAlterTablePartition::getPartitionAction() const { return getAlterTableAction(); }

#endif  // STMTDDLALTERTABLEPARTITION_H
