
#ifndef ELEMDDLWITHCHECKOPTION_H
#define ELEMDDLWITHCHECKOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLWithCheckOption.h
 * Description:  class respresenting the With Check Option clause
 *               in Create View statement
 *
 *
 * Created:      1/12/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLWithCheckOption;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLWithCheckOption
// -----------------------------------------------------------------------
class ElemDDLWithCheckOption : public ElemDDLNode {
 public:
  // constructor
  ElemDDLWithCheckOption(ComLevels level) : ElemDDLNode(ELM_WITH_CHECK_OPTION_ELEM), level_(level) {}

  // virtual destructor
  virtual ~ElemDDLWithCheckOption();

  // cast
  virtual ElemDDLWithCheckOption *castToElemDDLWithCheckOption();

  // accessors
  inline ComLevels getLevel() const;
  NAString getLevelAsNAString() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;  // display level info
  virtual const NAString getText() const;

 private:
  ComLevels level_;

};  // class ElemDDLWithCheckOption

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLWithCheckOption
// -----------------------------------------------------------------------

//
// accessor
//

inline ComLevels ElemDDLWithCheckOption::getLevel() const { return level_; }

#endif  // ELEMDDLWITHCHECKOPTION_H
