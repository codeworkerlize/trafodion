/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
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

class ElemDDLPartitionNameAndForValues : public ElemDDLNode
{
public:
  ElemDDLPartitionNameAndForValues(const NAString& partitionName)
    : ElemDDLNode(ELM_PARTITION_NAME_AND_FOR_VALUES)
    , partName_(partitionName)
    , isPartForValues_(FALSE)
    , partForValues_(NULL)
  {}

  ElemDDLPartitionNameAndForValues(ItemExpr *forValues)
    : ElemDDLNode(ELM_PARTITION_NAME_AND_FOR_VALUES)
    , isPartForValues_(FALSE)
    , partForValues_(forValues)
  {}

  virtual ~ElemDDLPartitionNameAndForValues();

  // cast
  virtual ElemDDLPartitionNameAndForValues * castToElemDDLPartitionNameAndForValues();
  
  inline const NAString & getPartitionName() const
  { return partName_; }

  inline ItemExpr * getPartitionForValues() const
  { return partForValues_; }

  inline NABoolean isPartitionForValues() const
  { return isPartForValues_; }
  
  inline void setIsPartitionForValues(const NABoolean isPartitionForValues)
  { isPartForValues_ = isPartitionForValues; }

private:
  NABoolean isPartForValues_;
  NAString partName_;
  ItemExpr* partForValues_;
}; // class ElemDDLPartitionNameAndForValues


// -----------------------------------------------------------------------
// Definition of typedef ElemDDLPartitionNameAndForValuesArray
// -----------------------------------------------------------------------

class ElemDDLPartitionNameAndForValuesArray : public LIST(ElemDDLPartitionNameAndForValues *)
{
public:

  // constructor
  ElemDDLPartitionNameAndForValuesArray(CollHeap *heap = PARSERHEAP())
    : LIST(ElemDDLPartitionNameAndForValues *)(heap)
  {}

  // virtual destructor
  virtual ~ElemDDLPartitionNameAndForValuesArray()
  {}

private:

}; // class ElemDDLPartitionNameAndKeyValuesArray



#endif // ELEMDDLPARTITIONNAMEANDFORVALUES_H

