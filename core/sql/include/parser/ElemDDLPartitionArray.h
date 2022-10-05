#ifndef ELEMDDLPARTITIONARRAY_H
#define ELEMDDLPARTITIONARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartitionArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLPartition
 *
 *
 * Created:      5/26/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/Collections.h"
#include "ElemDDLPartition.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPartitionArray;
class ElemDDLPartitionV2Array;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLPartitionArray
// -----------------------------------------------------------------------
class ElemDDLPartitionArray : public LIST(ElemDDLPartition *) {
 public:
  // constructor
  ElemDDLPartitionArray(CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLPartitionArray();

 private:
};  // class ElemDDLPartitionArray

// -----------------------------------------------------------------------
// Definition of class ElemDDLPartitionV2Array
// -----------------------------------------------------------------------
class ElemDDLPartitionV2Array : public LIST(ElemDDLPartitionV2 *) {
 public:
  // constructor
  ElemDDLPartitionV2Array(CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLPartitionV2Array();

 private:
};  // class ElemDDLPartitionV2Array

#endif /* ELEMDDLPARTITIONARRAY_H */
