
#ifndef ELEMDDLFILEATTRLOCKONREFRESH_H
#define ELEMDDLFILEATTRLOCKONREFRESH_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrLockOnRefresh.h
* Description:  class for Lock On Refresh File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      3/29/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"

class ElemDDLFileAttrLockOnRefresh : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrLockOnRefresh(NABoolean lockOnRefresh)
      : ElemDDLFileAttr(ELM_FILE_ATTR_LOCK_ON_REFRESH_ELEM), isLockOnRefresh_(lockOnRefresh) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrLockOnRefresh();

  // cast
  virtual ElemDDLFileAttrLockOnRefresh *castToElemDDLFileAttrLockOnRefresh();

  // accessor
  NABoolean isLockOnRefresh() const { return isLockOnRefresh_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  const NABoolean isLockOnRefresh_;

};  // class ElemDDLFileAttrLockOnRefresh

#endif  // ELEMDDLFILEATTRRANGELOG_H
