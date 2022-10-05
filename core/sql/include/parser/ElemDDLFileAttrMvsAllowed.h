
#ifndef ELEMDDLFILEATTRMVSALLOWED_H
#define ELEMDDLFILEATTRMVSALLOWED_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrMvsAllowed.h
* Description:  class for MVS ALLOWED File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      03/30/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"
#include "common/ComSmallDefs.h"

class ElemDDLFileAttrMvsAllowed : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrMvsAllowed(ComMvsAllowed mvsAllowedType = COM_NO_MVS_ALLOWED)
      : ElemDDLFileAttr(ELM_FILE_ATTR_MVS_ALLOWED_ELEM), mvsAllowedType_(mvsAllowedType) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrMvsAllowed();

  // cast
  virtual ElemDDLFileAttrMvsAllowed *castToElemDDLFileAttrMvsAllowed();

  // accessor
  ComMvsAllowed getMvsAllowedType() const { return mvsAllowedType_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;
  static NAString getMvsAllowedTypeAsNAString(ComMvsAllowed type);

  // method for building text
  virtual NAString getSyntax() const;

 private:
  ComMvsAllowed mvsAllowedType_;

};  // class ElemDDLFileAttrMvsAllowed

#endif  // ELEMDDLFILEATTRMVSALLOWED_H
