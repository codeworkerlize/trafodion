
#ifndef ELEMDDL_MV_FILEATTR_AUDIT_H
#define ELEMDDL_MV_FILEATTR_AUDIT_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrMvAudit.h
* Description:  class for MVS AUDIT File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      04/02/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"

class ElemDDLFileAttrMvAudit : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrMvAudit(ComMvAuditType mvAuditType = COM_MV_AUDIT)
      : ElemDDLFileAttr(ELM_FILE_ATTR_MVAUDIT_ELEM), mvAuditType_(mvAuditType) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrMvAudit();

  // cast
  virtual ElemDDLFileAttrMvAudit *castToElemDDLFileAttrMvAudit();

  // accessor
  ComMvAuditType getMvAuditType() const { return mvAuditType_; }

  NABoolean isBTAudit();

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;
  static NAString getMvAuditTypeTypeAsNAString(ComMvAuditType type);

  // method for building text
  virtual NAString getSyntax() const;

 private:
  ComMvAuditType mvAuditType_;

};  // class ElemDDLFileAttrMvAudit

#endif  // ELEMDDL_MV_FILEATTR_AUDIT_H
