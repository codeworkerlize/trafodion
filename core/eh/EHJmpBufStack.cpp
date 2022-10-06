/* -*-C++-*-
******************************************************************************
*
* File:         EHJmpBufStack.C
* Description:  member functions of class EHExceptionJmpBufStack
* Created:      5/16/95
* Language:     C++
*
*

*
*
******************************************************************************
*/

#include "EHJmpBufStack.h"

#include <stdio.h>

#include "EHCommonDefs.h"
#include "EHJmpBufNode.h"

// -----------------------------------------------------------------------
// methods for class EHExceptionJmpBufStack
// -----------------------------------------------------------------------

// virtual destructor
EHExceptionJmpBufStack::~EHExceptionJmpBufStack() {}

// push
//
//   pJmpBufNode must point to a space allocated
//   via the new operator
//
void EHExceptionJmpBufStack::push(EHExceptionJmpBufNode *pJmpBufNode) {
  EH_ASSERT(pJmpBufNode->getLink() == NULL);

  pJmpBufNode->setLink(pTopNode_);
  pTopNode_ = pJmpBufNode;
}

// pop
//
//   the pointer returned by pop() points to space allocated
//   via the new operator
//
EHExceptionJmpBufNode *EHExceptionJmpBufStack::pop() {
  EHExceptionJmpBufNode *pNode = pTopNode_;

  if (pTopNode_ != NULL) pTopNode_ = pNode->getLink();
  return pNode;
}
