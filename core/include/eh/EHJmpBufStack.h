#ifndef EHJMPBUFSTACK_H
#define EHJMPBUFSTACK_H
/* -*-C++-*-
******************************************************************************
*
* File:         EHJmpBufStack.h
* Description:  class for the shadow runtime stack
*
*
* Created:      5/16/95
* Language:     C++
*
*

*
*
******************************************************************************
*/

#include "EHBaseTypes.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class EHExceptionJmpBufStack;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class EHExceptionJmpBufNode;

// -----------------------------------------------------------------------
// class for shadow runtime stack
// -----------------------------------------------------------------------
class EHExceptionJmpBufStack {
 public:
  // constructor
  EHExceptionJmpBufStack() : pTopNode_(NULL) {}

  // virtual destructor
  virtual ~EHExceptionJmpBufStack();

  // push
  //
  //   pJmpBufNode must point to a space allocated
  //   via the new operator
  //
  void push(EHExceptionJmpBufNode *pJmpBufNode);

  // pop
  //
  //   the pointer returned by pop() points to space
  //   allocated via the new operator
  //
  EHExceptionJmpBufNode *pop();

 private:
  EHExceptionJmpBufNode *pTopNode_;

};  // class EHExceptionJmpBufStack

#endif  // EHJMPBUFSTACK_H
