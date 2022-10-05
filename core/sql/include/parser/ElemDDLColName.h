
#ifndef ELEMDDLCOLNAME_H
#define ELEMDDLCOLNAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColName.h
 * Description:  class for Column Name (parse node) elements in DDL
 *               statements
 *
 *
 * Created:      4/12/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColName;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Column Name (parse node) elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLColName : public ElemDDLNode {
 public:
  // constructor
  ElemDDLColName(const NAString &columnName) : ElemDDLNode(ELM_COL_NAME_ELEM), columnName_(columnName, PARSERHEAP()) {}

  // virtual destructor
  virtual ~ElemDDLColName();

  // cast
  virtual ElemDDLColName *castToElemDDLColName();

  // accessor
  inline const NAString &getColumnName() const;

  // member functions for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NAString columnName_;

};  // class ElemDDLColName

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLColName
// -----------------------------------------------------------------------

inline const NAString &ElemDDLColName::getColumnName() const { return columnName_; }

#endif  // ELEMDDLCOLNAME_H
