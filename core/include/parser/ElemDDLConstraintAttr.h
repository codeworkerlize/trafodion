
#ifndef ELEMDDLCONSTRAINTATTR_H
#define ELEMDDLCONSTRAINTATTR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintAttr.h
 * Description:  base class for (generic) parse nodes representing
 *               constraint attributes specified in constraint
 *               definitions
 *
 *
 * Created:      11/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintAttr;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLConstraintAttr
// -----------------------------------------------------------------------
class ElemDDLConstraintAttr : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLConstraintAttr(OperatorTypeEnum operType = ELM_ANY_CONSTRAINT_ATTR_ELEM) : ElemDDLNode(operType) {}

  // virtual destructor
  virtual ~ElemDDLConstraintAttr();

  // cast
  virtual ElemDDLConstraintAttr *castToElemDDLConstraintAttr();

  // methods for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLConstraintAttr

#endif  // ELEMDDLCONSTRAINTATTR_H
