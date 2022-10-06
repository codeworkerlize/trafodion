
#ifndef ELEMDDLFILEATTRCLEARONPURGE_H
#define ELEMDDLFILEATTRCLEARONPURGE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrClearOnPurge.h
* Description:  class for Clear-on-purge File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      4/21/95
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrClearOnPurge;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Clear-on-purge File Attribute (parse node) elements in DDL
// statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrClearOnPurge : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrClearOnPurge(NABoolean clearOnPurgeSpec = FALSE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_CLEAR_ON_PURGE_ELEM), isClearOnPurge_(clearOnPurgeSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrClearOnPurge();

  // cast
  virtual ElemDDLFileAttrClearOnPurge *castToElemDDLFileAttrClearOnPurge();

  // accessor
  const NABoolean getIsClearOnPurge() const { return isClearOnPurge_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NABoolean isClearOnPurge_;

};  // class ElemDDLFileAttrClearOnPurge

#endif /* ELEMDDLFILEATTRCLEARONPURGE_H */
