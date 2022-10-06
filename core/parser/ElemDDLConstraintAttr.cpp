/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLConstraint.C
 * Description:  methods for classes representing constraints.
 *
 * Created:      9/21/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "AllElemDDLConstraintAttr.h"
#include "common/BaseTypes.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLConstraintAttr
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLConstraintAttr::~ElemDDLConstraintAttr() {}

// cast
ElemDDLConstraintAttr *ElemDDLConstraintAttr::castToElemDDLConstraintAttr() { return this; }

//
// method for tracing
//

const NAString ElemDDLConstraintAttr::getText() const {
  NAAbort("ElemDDLConstraintAttr.C", __LINE__, "internal logic error");
  return "ElemDDLConstraintAttr";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLConstraintAttrDroppable
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLConstraintAttrDroppable::~ElemDDLConstraintAttrDroppable() {}

// cast
ElemDDLConstraintAttrDroppable *ElemDDLConstraintAttrDroppable::castToElemDDLConstraintAttrDroppable() { return this; }

//
// methods for tracing
//

const NAString ElemDDLConstraintAttrDroppable::displayLabel1() const {
  return NAString("is droppable? ") + YesNo(isDroppable());
}

const NAString ElemDDLConstraintAttrDroppable::getText() const { return "ElemDDLConstraintAttrDroppable"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLConstraintAttrEnforced
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLConstraintAttrEnforced::~ElemDDLConstraintAttrEnforced() {}

// cast
ElemDDLConstraintAttrEnforced *ElemDDLConstraintAttrEnforced::castToElemDDLConstraintAttrEnforced() { return this; }

//
// methods for tracing
//

const NAString ElemDDLConstraintAttrEnforced::displayLabel1() const {
  return NAString("is enforced? ") + YesNo(isEnforced());
}

const NAString ElemDDLConstraintAttrEnforced::getText() const { return "ElemDDLConstraintAttrEnforced"; }

//
// End of File
//
