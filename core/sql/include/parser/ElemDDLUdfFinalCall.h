/* -*-C++-*- */

#ifndef ELEMDDLUDFFINALCALL_H
#define ELEMDDLUDFFINALCALL_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfFinalCall.h
* Description:  class for UDF Final Call attribute (parse node) elements in
*               DDL statements
*
*
* Created:      7/14/09
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"

class ElemDDLUdfFinalCall : public ElemDDLNode {
 public:
  // constructor
  ElemDDLUdfFinalCall(NABoolean theFinalCall);

  // virtual destructor
  virtual ~ElemDDLUdfFinalCall(void);

  // cast
  virtual ElemDDLUdfFinalCall *castToElemDDLUdfFinalCall(void);

  // accessor
  inline const NABoolean getFinalCall(void) const { return finalCall_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NABoolean finalCall_;

};  // class ElemDDLUdfFinalCall

#endif /* ELEMDDLUDFFINALCALL_H */
