
#ifndef ELEMDDLFILEATTRICOMPRESS_H
#define ELEMDDLFILEATTRICOMPRESS_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrICompress.h
* Description:  class for I-Compress File Attribute (parse node) elements in
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

#include "ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrICompress;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// I-Compress File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrICompress : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrICompress(NABoolean iCompressSpec = TRUE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_I_COMPRESS_ELEM), isICompress_(iCompressSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrICompress();

  // cast
  virtual ElemDDLFileAttrICompress *castToElemDDLFileAttrICompress();

  // accessor
  const NABoolean getIsICompress() const { return isICompress_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  virtual NAString getSyntax() const;

 private:
  NABoolean isICompress_;

};  // class ElemDDLFileAttrICompress

#endif /* ELEMDDLFILEATTRICOMPRESS_H */
