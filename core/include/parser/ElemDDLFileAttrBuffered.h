
#ifndef ELEMDDLFILEATTRBUFFERED_H
#define ELEMDDLFILEATTRBUFFERED_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrBuffered.h
* Description:  class for Buffered File Attribute (parse node) elements in
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
class ElemDDLFileAttrBuffered;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Buffered File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrBuffered : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrBuffered(NABoolean bufferedSpec = TRUE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_BUFFERED_ELEM), isBuffered_(bufferedSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrBuffered();

  // cast
  virtual ElemDDLFileAttrBuffered *castToElemDDLFileAttrBuffered();

  // accessor
  const NABoolean getIsBuffered() const { return isBuffered_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NABoolean isBuffered_;

};  // class ElemDDLFileAttrBuffered

#endif /* ELEMDDLFILEATTRBUFFERED_H */
