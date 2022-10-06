
#ifndef ELEMDDLCONSTRAINTATTRDROPPABLE_H
#define ELEMDDLCONSTRAINTATTRDROPPABLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintAttrDroppable.h
 * Description:  class for parse node representing the droppable or
 *               nondroppable constraint attribute specified in
 *               constraint definitions
 *
 * Created:      11/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLConstraintAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintAttrDroppable;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLConstraintAttrDroppable
// -----------------------------------------------------------------------
class ElemDDLConstraintAttrDroppable : public ElemDDLConstraintAttr {
 public:
  // default constructor
  ElemDDLConstraintAttrDroppable(NABoolean isDroppable = FALSE)
      : ElemDDLConstraintAttr(ELM_CONSTRAINT_ATTR_DROPPABLE_ELEM), isDroppable_(isDroppable) {}

  // virtual destructor
  virtual ~ElemDDLConstraintAttrDroppable();

  // cast
  virtual ElemDDLConstraintAttrDroppable *castToElemDDLConstraintAttrDroppable();

  // accessor
  inline NABoolean isDroppable() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NABoolean isDroppable_;

};  // class ElemDDLConstraintAttrDroppable

//
// accessor
//

inline NABoolean ElemDDLConstraintAttrDroppable::isDroppable() const { return isDroppable_; }

#endif  // ELEMDDLCONSTRAINTATTRDROPPABLE_H
