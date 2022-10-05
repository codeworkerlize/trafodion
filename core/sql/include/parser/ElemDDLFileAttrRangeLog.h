
#ifndef ELEMDDLFILEATTRRANGELOG_H
#define ELEMDDLFILEATTRRANGELOG_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrRangeLog.h
* Description:  class for Range log File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      11/21/99
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrRangeLog;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Range-Log File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrRangeLog : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrRangeLog(ComRangeLogType rangelogType = COM_NO_RANGELOG)
      : ElemDDLFileAttr(ELM_FILE_ATTR_RANGE_LOG_ELEM), rangelogType_(rangelogType) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrRangeLog();

  // cast
  virtual ElemDDLFileAttrRangeLog *castToElemDDLFileAttrRangeLog();

  // accessor
  ComRangeLogType getRangelogType() const { return rangelogType_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;
  static NAString getRangeLogTypeAsNAString(ComRangeLogType type);

  // method for building text
  virtual NAString getSyntax() const;

 private:
  ComRangeLogType rangelogType_;

};  // class ElemDDLFileAttrRangeLog

#endif  // ELEMDDLFILEATTRRANGELOG_H
