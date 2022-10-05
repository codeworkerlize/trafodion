
#ifndef ELEMDDLCONSTRAINTATTRENFORCED_H
#define ELEMDDLCONSTRAINTATTRENFORCED_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintAttrEnforced.h
 * Description:  class for parse node representing the enforced or
 *               not enforced constraint attribute specified in
 *               constraint definitions
 *
 * Created:      04/15/08
 * Language:     C++
 *
 *
 */

#include "ElemDDLConstraintAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintAttrEnforced;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLConstraintAttrEnforced
// -----------------------------------------------------------------------
class ElemDDLConstraintAttrEnforced : public ElemDDLConstraintAttr {
 public:
  // default constructor
  ElemDDLConstraintAttrEnforced(NABoolean isEnforced = TRUE)
      : ElemDDLConstraintAttr(ELM_CONSTRAINT_ATTR_ENFORCED_ELEM), isEnforced_(isEnforced) {}

  // virtual destructor
  virtual ~ElemDDLConstraintAttrEnforced();

  // cast
  virtual ElemDDLConstraintAttrEnforced *castToElemDDLConstraintAttrEnforced();

  // accessor
  inline NABoolean isEnforced() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NABoolean isEnforced_;

};  // class ElemDDLConstraintAttrEnforced

//
// accessor
//

inline NABoolean ElemDDLConstraintAttrEnforced::isEnforced() const { return isEnforced_; }

#endif  // ELEMDDLCONSTRAINTATTRENFORCED_H
