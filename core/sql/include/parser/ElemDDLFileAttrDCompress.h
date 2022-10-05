
#ifndef ELEMDDLFILEATTRDCOMPRESS_H
#define ELEMDDLFILEATTRDCOMPRESS_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrDCompress.h
* Description:  class for D-Compress File Attribute (parse node) elements in
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
class ElemDDLFileAttrDCompress;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// D-Compress File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrDCompress : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrDCompress(NABoolean dCompressSpec = TRUE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_D_COMPRESS_ELEM), isDCompress_(dCompressSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrDCompress();

  // cast
  virtual ElemDDLFileAttrDCompress *castToElemDDLFileAttrDCompress();

  // accessor
  const NABoolean getIsDCompress() const { return isDCompress_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NABoolean isDCompress_;

};  // class ElemDDLFileAttrDCompress

#endif /* ELEMDDLFILEATTRDCOMPRESS_H */
