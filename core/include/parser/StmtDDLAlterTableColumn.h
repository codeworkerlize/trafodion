
#ifndef STMTDDLALTERTABLECOLUMN_H
#define STMTDDLALTERTABLECOLUMN_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableColumn.h
 * Description:  class for Alter Table <table-name> Column <column-name>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      9/19/95
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
class StmtDDLAlterTableColumn;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableColumn
// -----------------------------------------------------------------------
class StmtDDLAlterTableColumn : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableColumn(const NAString &columnName, ElemDDLNode *pColumnHeadingNode, CollHeap *h = 0)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_COLUMN, QualifiedName(h) /*no table name*/, pColumnHeadingNode),
        columnName_(columnName, h) {}

  // copy ctor
  StmtDDLAlterTableColumn(const StmtDDLAlterTableColumn &orig, CollHeap *h = 0);

  // virtual destructor
  virtual ~StmtDDLAlterTableColumn();

  // cast
  virtual StmtDDLAlterTableColumn *castToStmtDDLAlterTableColumn();

  // accessors
  inline ElemDDLColHeading *getColumnHeading() const;
  inline const NAString &getColumnName() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
  NAString columnName_;

};  // class StmtDDLAlterTableColumn

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableColumn
// -----------------------------------------------------------------------

inline ElemDDLColHeading *StmtDDLAlterTableColumn::getColumnHeading() const {
  return getAlterTableAction()->castToElemDDLColHeading();
}

inline const NAString &StmtDDLAlterTableColumn::getColumnName() const { return columnName_; }

#endif  // STMTDDLALTERTABLECOLUMN_H
