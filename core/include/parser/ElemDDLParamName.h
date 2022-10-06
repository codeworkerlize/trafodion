
#ifndef ELEMDDLPARAMNAME_H
#define ELEMDDLPARAMNAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLParamName.h
 * Description:  class for Param Name (parse node) elements in DDL
 *               statements
 *
 *
 * Created:      10/01/1999
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
class ElemDDLParamName;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Param Name (parse node) elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLParamName : public ElemDDLNode {
 public:
  // constructor
  ElemDDLParamName(const NAString &paramName) : ElemDDLNode(ELM_PARAM_NAME_ELEM), paramName_(paramName, PARSERHEAP()) {}

  // virtual destructor
  virtual ~ElemDDLParamName();

  // cast
  virtual ElemDDLParamName *castToElemDDLParamName();

  // accessor
  inline const NAString &getParamName() const;

  // member functions for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NAString paramName_;

};  // class ElemDDLParamName

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLParamName
// -----------------------------------------------------------------------

inline const NAString &ElemDDLParamName::getParamName() const { return paramName_; }

#endif  // ELEMDDLPARAMNAME_H
