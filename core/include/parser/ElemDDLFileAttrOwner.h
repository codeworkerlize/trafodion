
#ifndef ELEMDDLFILEATTROWNER_H
#define ELEMDDLFILEATTROWNER_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrOwner.h
* Description:  class for owner information in DDL create statements
*               initially used for bulk replicate
*
*
* Created:      3/29/10
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrOwner;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Generic Lock Length (parse node) elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLFileAttrOwner : public ElemDDLFileAttr {
 public:
  // constructor
  ElemDDLFileAttrOwner(NAString &objectOwner) : ElemDDLFileAttr(ELM_FILE_ATTR_OWNER_ELEM), objectOwner_(objectOwner) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrOwner();

  // cast
  virtual ElemDDLFileAttrOwner *castToElemDDLFileAttrOwner();

  // accessor
  inline const NAString &getOwner() const;

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NAString objectOwner_;

};  // class ElemDDLFileAttrOwner

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrOwner
// -----------------------------------------------------------------------
//
// accessor
//

inline const NAString &ElemDDLFileAttrOwner::getOwner() const { return objectOwner_; }

#endif  // ELEMDDLFILEATTROWNER_H
