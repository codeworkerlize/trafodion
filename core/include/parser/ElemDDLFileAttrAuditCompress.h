
#ifndef ELEMDDLFILEATTRAUDITCOMPRESS_H
#define ELEMDDLFILEATTRAUDITCOMPRESS_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrAuditCompress.h
* Description:  class for Audit-Compress File Attribute (parse node)
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

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrAuditCompress;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Audit-Compress File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrAuditCompress : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrAuditCompress(NABoolean auditCompressSpec = FALSE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_AUDIT_COMPRESS_ELEM), isAuditCompress_(auditCompressSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrAuditCompress();

  // cast
  virtual ElemDDLFileAttrAuditCompress *castToElemDDLFileAttrAuditCompress();

  // accessor
  const NABoolean getIsAuditCompress() const { return isAuditCompress_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NABoolean isAuditCompress_;

};  // class ElemDDLFileAttrAuditCompress

#endif /* ELEMDDLFILEATTRAUDITCOMPRESS_H */
