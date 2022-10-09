#ifndef ELEMDDLFILEATTRAUDIT_H
#define ELEMDDLFILEATTRAUDIT_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrAudit.h
* Description:  class for Audit File Attribute (parse node) elements in
*               DDL statements
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

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrAudit;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Audit File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrAudit : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrAudit(NABoolean auditSpec = TRUE) : ElemDDLFileAttr(ELM_FILE_ATTR_AUDIT_ELEM), isAudit_(auditSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrAudit();

  // cast
  virtual ElemDDLFileAttrAudit *castToElemDDLFileAttrAudit();

  // accessor
  const NABoolean getIsAudit() const { return isAudit_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

 private:
  NABoolean isAudit_;

};  // class ElemDDLFileAttrAudit

#endif /* ELEMDDLFILEATTRAUDIT_H */
