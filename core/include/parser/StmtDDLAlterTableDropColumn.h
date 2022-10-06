#ifndef STMTDDLALTERTABLEDROPCOLUMN_H
#define STMTDDLALTERTABLEDROPCOLUMN_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableDropColumn.h
 * Description:  class for Alter Table <table-name> Drop Column <column-definition>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      1/27/98
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
// Change history:
//
// -----------------------------------------------------------------------

#include "StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableDropColumn;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableDropColumn
// -----------------------------------------------------------------------
class StmtDDLAlterTableDropColumn : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableDropColumn(NAString &colName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableDropColumn();

  // cast
  virtual StmtDDLAlterTableDropColumn *castToStmtDDLAlterTableDropColumn();

  // accessors
  const NAString &getColName() const { return colName_; }

  const NABoolean dropIfExists() const { return dropIfExists_; }
  void setDropIfExists(NABoolean v) { dropIfExists_ = v; }

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableDropColumn();                                                // DO NOT USE
  StmtDDLAlterTableDropColumn(const StmtDDLAlterTableDropColumn &);             // DO NOT USE
  StmtDDLAlterTableDropColumn &operator=(const StmtDDLAlterTableDropColumn &);  // DO NOT USE

 private:
  NAString colName_;

  // drop only if column exists. Otherwise just return.
  NABoolean dropIfExists_;

};  // class StmtDDLAlterTableDropColumn

#endif  // STMTDDLALTERTABLEDROPCOLUMN_H
