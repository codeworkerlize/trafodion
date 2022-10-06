
#ifndef ELEMDDLCONSTRAINTNOTNULL_H
#define ELEMDDLCONSTRAINTNOTNULL_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintNotNull.h
 * Description:  class for Not Null constraint definitions in DDL statements
 *
 *
 * Created:      3/29/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLConstraint.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintNotNull;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// defintion of class ElemDDLConstraintNotNull
// -----------------------------------------------------------------------
class ElemDDLConstraintNotNull : public ElemDDLConstraint {
 public:
  // constructors
  ElemDDLConstraintNotNull(NABoolean isNotNull = TRUE, CollHeap *h = PARSERHEAP())
      : ElemDDLConstraint(h, ELM_CONSTRAINT_NOT_NULL_ELEM), isNotNull_(isNotNull) {}

  inline ElemDDLConstraintNotNull(const NAString &constraintName, NABoolean isNotNull = TRUE,
                                  CollHeap *h = PARSERHEAP());

  // copy ctor
  ElemDDLConstraintNotNull(const ElemDDLConstraintNotNull &orig,
                           CollHeap *h = 0);  // not written

  // virtual destructor
  virtual ~ElemDDLConstraintNotNull();

  // cast
  virtual ElemDDLConstraintNotNull *castToElemDDLConstraintNotNull();
  virtual NABoolean isConstraintNotNull() const
  //  { return TRUE; }
  {
    return isNotNull_;
  }

  // methods for tracing
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  NABoolean isNotNull_;
};  // class ElemDDLConstraintNotNull

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLConstraintNotNull
// -----------------------------------------------------------------------

//
// constructors
//

inline ElemDDLConstraintNotNull::ElemDDLConstraintNotNull(const NAString &constraintName, const NABoolean isNotNull,
                                                          CollHeap *h)
    : ElemDDLConstraint(h, ELM_CONSTRAINT_NOT_NULL_ELEM, constraintName), isNotNull_(isNotNull) {}

#endif  // ELEMDDLCONSTRAINTNOTNULL_H
