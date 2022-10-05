#ifndef EHEXCEPTIONTYPENODE_H
#define EHEXCEPTIONTYPENODE_H
/* -*-C++-*-
******************************************************************************
*
* File:         EHExceptionTypeNode.h
* Description:  class for a node in a singular linked list.  The node
*               contains the type of an exception associating with a
*               try block.
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
#include "EHExceptionTypeEnum.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class EHExceptionTypeNode;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// class for a node in a singular linked list.  The node contains the
// type of an exception associating with a try block.
// -----------------------------------------------------------------------
class EHExceptionTypeNode {
 public:
  // default constructor

  EHExceptionTypeNode(EHExceptionTypeEnum exceptionType = EH_NORMAL, EHExceptionTypeNode *pNextNode = NULL)
      : exceptionType_(exceptionType), pNextNode_(pNextNode) {}

  // destructor
  //
  //   Use the destructor provided by the C++ compiler.
  //   We don't need to call this destructor when we call
  //   longjmp() to cut back the runtime stack.

  // accessors

  EHExceptionTypeEnum getExceptionType() const { return exceptionType_; }

  EHExceptionTypeNode *getNextNode() const { return pNextNode_; }

  // mutators

  void setExceptionType(EHExceptionTypeEnum exceptionType) { exceptionType_ = exceptionType; }

  void setNextNode(EHExceptionTypeNode *pNextNode) { pNextNode_ = pNextNode; }

 private:
  // data

  EHExceptionTypeEnum exceptionType_;

  // link

  EHExceptionTypeNode *pNextNode_;

};  // class EHExceptionTypeNode

#endif  // EHEXCEPTIONTYPENODE_H
