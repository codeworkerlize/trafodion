#ifndef ELEMDDLFILEATTRNOLABELUPDATE_H
#define ELEMDDLFILEATTRNOLABELUPDATE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrNoLabelUpdate.h
* Description:
*
*
* Created:
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
class ElemDDLFileAttrNoLabelUpdate;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

class ElemDDLFileAttrNoLabelUpdate : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrNoLabelUpdate(NABoolean noLabelUpdate = FALSE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_NO_LABEL_UPDATE_ELEM), noLabelUpdate_(noLabelUpdate) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrNoLabelUpdate();

  // cast
  virtual ElemDDLFileAttrNoLabelUpdate *castToElemDDLFileAttrNoLabelUpdate();

  // accessor
  const NABoolean getIsNoLabelUpdate() const { return noLabelUpdate_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

 private:
  NABoolean noLabelUpdate_;

};  // class ElemDDLFileAttrNoLabelUpdate

#endif /* ELEMDDLFILEATTRNOLABELUPDATE_H */
