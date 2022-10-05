
#ifndef ELEMDDLPARTITIONNAMEANDFORVALUES_H
#define ELEMDDLPARTITIONNAMEANDFORVALUES_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartitionNameAndForValues.h
 * Description:  class for parse nodes representing partition clauses
 *               in DDL statements.  Note that this class is derived
 *               from class ElemDDLNode instead of class ElemDDLPartition.
 *
 *
 * Created:      3/8/2021
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "ItemConstValueArray.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPartitionNameAndForValues;
class ElemDDLPartitionNameAndForValuesArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLPartitionNameAndForValues
// -----------------------------------------------------------------------

class ElemDDLPartitionNameAndForValues : public ElemDDLNode {
 public:
  ElemDDLPartitionNameAndForValues(const NAString &partitionName)
      : ElemDDLNode(ELM_PARTITION_NAME_AND_FOR_VALUES),
        partName_(partitionName),
        isPartForValues_(FALSE),
        partForValues_(NULL) {}

  ElemDDLPartitionNameAndForValues(ItemExpr *forValues)
      : ElemDDLNode(ELM_PARTITION_NAME_AND_FOR_VALUES), isPartForValues_(FALSE), partForValues_(forValues) {}

  virtual ~ElemDDLPartitionNameAndForValues();

  // cast
  virtual ElemDDLPartitionNameAndForValues *castToElemDDLPartitionNameAndForValues();

  inline const NAString &getPartitionName() const { return partName_; }

  inline ItemExpr *getPartitionForValues() const { return partForValues_; }

  inline NABoolean isPartitionForValues() const { return isPartForValues_; }

  inline void setIsPartitionForValues(const NABoolean isPartitionForValues) { isPartForValues_ = isPartitionForValues; }

 private:
  NABoolean isPartForValues_;
  NAString partName_;
  ItemExpr *partForValues_;
};  // class ElemDDLPartitionNameAndForValues

// -----------------------------------------------------------------------
// Definition of typedef ElemDDLPartitionNameAndForValuesArray
// -----------------------------------------------------------------------

class ElemDDLPartitionNameAndForValuesArray : public LIST(ElemDDLPartitionNameAndForValues *) {
 public:
  // constructor
  ElemDDLPartitionNameAndForValuesArray(CollHeap *heap = PARSERHEAP())
      : LIST(ElemDDLPartitionNameAndForValues *)(heap) {}

  // virtual destructor
  virtual ~ElemDDLPartitionNameAndForValuesArray() {}

 private:
};  // class ElemDDLPartitionNameAndKeyValuesArray

#endif  // ELEMDDLPARTITIONNAMEANDFORVALUES_H
