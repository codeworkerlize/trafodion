
#ifndef STMTDDLALTERTABLEALTERCOLUMNLOGGABLE_H
#define STMTDDLALTERTABLEALTERCOLUMNLOGGABLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableAlterColumnLoggable.h
 * Description:  class for Alter Table <table-name> Add Column <column-definition>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      5/16/00
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

//----------------------------------------------------------------------------
// Change history:
//
//----------------------------------------------------------------------------

#include "StmtDDLAlterTable.h"

//----------------------------------------------------------------------------
// forward references
//----------------------------------------------------------------------------
// None.

class StmtDDLAlterTableAlterColumnLoggable : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableAlterColumnLoggable(ElemDDLNode *pColumnDefinition, NABoolean loggableVal,
                                       CollHeap *heap = PARSERHEAP());

  // constructor
  StmtDDLAlterTableAlterColumnLoggable(NAString columnName, NABoolean loggableVal, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableAlterColumnLoggable();

  // cast
  virtual StmtDDLAlterTableAlterColumnLoggable *castToStmtDDLAlterTableAlterColumnLoggable();

  // accessors
  NABoolean getIsLoggable() const { return loggable_; }
  CollIndex getColumnNum() const { return columnNum_; }

  // please do not use the following methods
  StmtDDLAlterTableAlterColumnLoggable(const StmtDDLAlterTableAlterColumnLoggable &);
  StmtDDLAlterTableAlterColumnLoggable &operator=(const StmtDDLAlterTableAddColumn &);

  ExprNode *bindNode(BindWA *pBindWA);

 private:
  // column definition
  ElemDDLNode *pColumnToAdd_;

  NAString columnName_;

  ComBoolean loggable_;

  CollIndex columnNum_;

};  // class StmtDDLAlterTableAddColumn

#endif  // STMTDDLALTERTABLEALTERCOLUMNLOGGABLE_H
