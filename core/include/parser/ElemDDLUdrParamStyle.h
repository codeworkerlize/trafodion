
#ifndef ELEMDDLUDRPARAMSTYLE_H
#define ELEMDDLUDRPARAMSTYLE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrParamStyle.h
* Description:  class for UDR Param Style (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

class ElemDDLUdrParamStyle : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrParamStyle(ComRoutineParamStyle theParamStyle);

  // virtual destructor
  virtual ~ElemDDLUdrParamStyle(void);

  // cast
  virtual ElemDDLUdrParamStyle *castToElemDDLUdrParamStyle(void);

  // accessor
  inline const ComRoutineParamStyle getParamStyle(void) const { return paramStyle_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineParamStyle paramStyle_;

};  // class ElemDDLUdrParamStyle

#endif /* ELEMDDLUDRPARAMSTYLE_H */
