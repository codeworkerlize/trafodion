
#ifndef STMTDDLALTERTABLEMOVE_H
#define STMTDDLALTERTABLEMOVE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableMove.h
 * Description:  class for Alter Table <table-name> Move ...
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
class StmtDDLAlterTableMove;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableMove
// -----------------------------------------------------------------------
class StmtDDLAlterTableMove : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableMove(ElemDDLNode *pMoveAction) : StmtDDLAlterTable(DDL_ALTER_TABLE_MOVE, pMoveAction) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableMove();

  // cast
  virtual StmtDDLAlterTableMove *castToStmtDDLAlterTableMove();

  // accessor
  inline ElemDDLAlterTableMove *getMoveAction() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class StmtDDLAlterTableMove

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableMove
// -----------------------------------------------------------------------

inline ElemDDLAlterTableMove *StmtDDLAlterTableMove::getMoveAction() const {
  return getAlterTableAction()->castToElemDDLAlterTableMove();
}

#endif  // STMTDDLALTERTABLEMOVE_H
