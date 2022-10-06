
#ifndef ELEMDDLFILEATTRINSERTLOG_H
#define ELEMDDLFILEATTRINSERTLOG_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrInsertLog.h
* Description:  class for INSERTLOG File Attribute (parse node)
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

class ElemDDLFileAttrInsertLog : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrInsertLog(NABoolean isInsertLog = FALSE)
      : ElemDDLFileAttr(ELM_FILE_ATTR_INSERT_LOG_ELEM), isInsertLog_(isInsertLog) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrInsertLog();

  // cast
  virtual ElemDDLFileAttrInsertLog *castToElemDDLFileAttrInsertLog();

  // accessor
  NABoolean isInsertLog() const { return isInsertLog_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  NABoolean isInsertLog_;

};  // class ElemDDLFileAttrInsertLog

#endif  // ELEMDDLFILEATTRINSERTLOG_H
