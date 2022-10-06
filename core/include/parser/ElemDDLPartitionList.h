
#ifndef ELEMDDLPARTITIONLIST_H
#define ELEMDDLPARTITIONLIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartitionList.h
 * Description:  class for lists of Partition elements specified in DDL
 *               statements.  Each Partition element contains all legal
 *               partition attributes associating with the partition.
 *               For the partition attributes that are not specified
 *               (in DDL statements), the default values are used.
 *
 *               Note that class ElemDDLPartitionList is derived from
 *               the base class ElemDDLList; therefore, the class
 *               ElemDDLPartitionList also represents a left linear
 *               tree.
 *
 *
 * Created:      4/7/95
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
class ElemDDLPartitionList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// A list of partition elements specified in DDL statement
// associating with PARTITION.
// -----------------------------------------------------------------------
class ElemDDLPartitionList : public ElemDDLList {
 public:
  // constructor
  ElemDDLPartitionList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_PARTITION_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLPartitionList();

  // cast
  virtual ElemDDLPartitionList *castToElemDDLPartitionList();

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLPartitionList

#endif  // ELEMDDLPARTITIONLIST_H
