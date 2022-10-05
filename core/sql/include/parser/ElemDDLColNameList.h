
#ifndef ELEMDDLCOLNAMELIST_H
#define ELEMDDLCOLNAMELIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColNameList.h
 * Description:  class for lists of Column Name elements in DDL statements
 *
 * Created:      10/16/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLList.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColNameList;
class ElemDDLColNameListNode;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLColNameList
// -----------------------------------------------------------------------
class ElemDDLColNameList : public ElemDDLList {
 public:
  // constructor
  ElemDDLColNameList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_COL_NAME_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLColNameList();

  // cast
  virtual ElemDDLColNameList *castToElemDDLColNameList();

  // methods for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLColNameList

// -----------------------------------------------------------------------
// definition of class ElemDDLColNameListNode
// -----------------------------------------------------------------------
//
// This class allows a list node to be an array of other list nodes,
// i.e., allows a 2-dimensional list.
//

class ElemDDLColNameListNode : public ElemDDLNode {
 public:
  // constructor
  ElemDDLColNameListNode(ElemDDLNode *theList) : ElemDDLNode(ELM_COL_NAME_LIST_NODE) { columnNameList_ = theList; }

  // virtual destructor
  virtual ~ElemDDLColNameListNode();

  // cast
  virtual ElemDDLColNameListNode *castToElemDDLColNameListNode();

  // methods for tracing
  virtual const NAString getText() const;

  inline ElemDDLNode *getColumnNameList();

 private:
  ElemDDLNode *columnNameList_;
};  // class ElemDDLColNameListNode

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLColNameListNode
// -----------------------------------------------------------------------

inline ElemDDLNode *ElemDDLColNameListNode::getColumnNameList() { return columnNameList_; }

#endif  // ELEMDDLCOLNAMELIST_H
