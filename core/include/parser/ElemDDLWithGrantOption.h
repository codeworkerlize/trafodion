
#ifndef ELEMDDLWITHGRANTOPTION_H
#define ELEMDDLWITHGRANTOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLWithGrantOption.h
 * Description:  class respresenting the With Grant Option
 *               clause/phrase in Grant DDL statement
 *
 *
 * Created:      10/6/95
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
class ElemDDLWithGrantOption;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLWithGrantOption
// -----------------------------------------------------------------------
class ElemDDLWithGrantOption : public ElemDDLNode {
 public:
  // constructor
  ElemDDLWithGrantOption() : ElemDDLNode(ELM_WITH_GRANT_OPTION_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLWithGrantOption();

  // cast
  virtual ElemDDLWithGrantOption *castToElemDDLWithGrantOption();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLWithGrantOption

#endif  // ELEMDDLWITHGRANTOPTION_H
