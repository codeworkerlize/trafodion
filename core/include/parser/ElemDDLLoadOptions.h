
#ifndef ELEMDDLLOADOPTIONS_H
#define ELEMDDLLOADOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLoadOptions.h
 * Description:  classes for load options specified in DDL statements
 *
 *
 * Created:      9/28/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "common/BaseTypes.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLLoadOpt;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLLoadOpt
// -----------------------------------------------------------------------
class ElemDDLLoadOpt : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLLoadOpt(OperatorTypeEnum operatorType = ELM_ANY_LOAD_OPT_ELEM) : ElemDDLNode(operatorType) {}

  // virtual destructor
  virtual ~ElemDDLLoadOpt();

  // cast
  virtual ElemDDLLoadOpt *castToElemDDLLoadOpt();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLoadOpt



#endif  // ELEMDDLLOADOPTIONS_H
