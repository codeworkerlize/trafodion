
#ifndef STMTDDLALTERTABLEALTERCOLUMN_H
#define STMTDDLALTERTABLEALTERCOLUMN_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableAlterColumn.h
 * Description:  class for Alter Table <table-name> alter column
 *               DDL statements (datatype and default clauses)
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLColDefArray.h"
#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableAlterColumnDefaultValue;
class StmtDDLAlterTableAlterColumnDatatype;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAlterColumn
// -----------------------------------------------------------------------
class StmtDDLAlterTableAlterColumn : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableAlterColumn(OperatorTypeEnum operatorType, const NAString &columnName, ElemDDLNode *pColDefault,
                               CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableAlterColumn();

  // accessors
  inline NAString getColumnName();

  // method for tracing
  virtual const NAString getText() const;

 private:
  // column name
  NAString columnName_;

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableAlterColumn();                                                 // DO NOT USE
  StmtDDLAlterTableAlterColumn(const StmtDDLAlterTableAlterColumn &);             // DO NOT USE
  StmtDDLAlterTableAlterColumn &operator=(const StmtDDLAlterTableAlterColumn &);  // DO NOT USE

};  // class StmtDDLAlterTableAlterColumn

inline NAString StmtDDLAlterTableAlterColumn::getColumnName() { return columnName_; }

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAlterColumnDefaultValue
// -----------------------------------------------------------------------
class StmtDDLAlterTableAlterColumnDefaultValue : public StmtDDLAlterTableAlterColumn {
 public:
  // constructor
  StmtDDLAlterTableAlterColumnDefaultValue(const NAString &columnName, ElemDDLNode *pColDefault,
                                           CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableAlterColumnDefaultValue();

  // cast
  virtual StmtDDLAlterTableAlterColumnDefaultValue *castToStmtDDLAlterTableAlterColumnDefaultValue();

  // method for tracing
  virtual const NAString getText() const;

 private:
  //
  // please do not use the following methods
  //

  StmtDDLAlterTableAlterColumnDefaultValue();                                                             // DO NOT USE
  StmtDDLAlterTableAlterColumnDefaultValue(const StmtDDLAlterTableAlterColumnDefaultValue &);             // DO NOT USE
  StmtDDLAlterTableAlterColumnDefaultValue &operator=(const StmtDDLAlterTableAlterColumnDefaultValue &);  // DO NOT USE

};  // class StmtDDLAlterTableAlterColumnDefaultValue

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAlterColumnDatatype
// -----------------------------------------------------------------------
class StmtDDLAlterTableAlterColumnDatatype : public StmtDDLAlterTableAlterColumn {
 public:
  // constructor
  StmtDDLAlterTableAlterColumnDatatype(ElemDDLNode *pColumnToAlter, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableAlterColumnDatatype();

  // cast
  virtual StmtDDLAlterTableAlterColumnDatatype *castToStmtDDLAlterTableAlterColumnDatatype();

  // method for tracing
  virtual const NAString getText() const;

  inline ElemDDLNode *getColToAlter() { return pColumnToAlter_; };
  inline ElemDDLColDefArray &getColDefArray() { return columnDefArray_; };

 private:
  // column definition
  ElemDDLNode *pColumnToAlter_;

  // list of (only one) column definition
  ElemDDLColDefArray columnDefArray_;

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableAlterColumnDatatype();                                                         // DO NOT USE
  StmtDDLAlterTableAlterColumnDatatype(const StmtDDLAlterTableAlterColumnDatatype &);             // DO NOT USE
  StmtDDLAlterTableAlterColumnDatatype &operator=(const StmtDDLAlterTableAlterColumnDatatype &);  // DO NOT USE

};  // class StmtDDLAlterTableAlterColumnDatatype

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAlterColumnRename
// -----------------------------------------------------------------------
class StmtDDLAlterTableAlterColumnRename : public StmtDDLAlterTableAlterColumn {
 public:
  // constructor
  StmtDDLAlterTableAlterColumnRename(const NAString &columnName, const NAString &renamedColumnName,
                                     CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableAlterColumnRename();

  // cast
  virtual StmtDDLAlterTableAlterColumnRename *castToStmtDDLAlterTableAlterColumnRename();

  // accessors
  NAString getRenamedColumnName() { return renamedColumnName_; }
  const NAString getRenamedColumnName() const { return renamedColumnName_; }

  // method for tracing
  virtual const NAString getText() const;

 private:
  // column name
  NAString renamedColumnName_;

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableAlterColumnRename();                                                       // DO NOT USE
  StmtDDLAlterTableAlterColumnRename(const StmtDDLAlterTableAlterColumnRename &);             // DO NOT USE
  StmtDDLAlterTableAlterColumnRename &operator=(const StmtDDLAlterTableAlterColumnRename &);  // DO NOT USE

};  // class StmtDDLAlterTableAlterColumnRename

#endif  // STMTDDLALTERTABLEALTERCOLUMN_H
