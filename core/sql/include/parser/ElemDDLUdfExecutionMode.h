/* -*-C++-*- */
#ifndef ELEMDDLUDFEXECUTIONMODE_H
#define ELEMDDLUDFEXECUTIONMODE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfExecutionMode.h
* Description:  class for routine execution mode (parse node)
*               elements in DDL statements
*
*
* Created:      1/19/2010
* Language:     C++
*
*

*
*
******************************************************************************
*/

#include "ElemDDLNode.h"

class ElemDDLUdfExecutionMode : public ElemDDLNode {
 public:
  // constructor
  ElemDDLUdfExecutionMode(ComRoutineExecutionMode theExecutionMode);

  // virtual destructor
  virtual ~ElemDDLUdfExecutionMode(void);

  // cast
  virtual ElemDDLUdfExecutionMode *castToElemDDLUdfExecutionMode(void);

  // accessor
  inline const ComRoutineExecutionMode getExecutionMode(void) const { return executionMode_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineExecutionMode executionMode_;

};  // class ElemDDLUdfExecutionMode

#endif /* ELEMDDLUDFEXECUTIONMODE_H */
