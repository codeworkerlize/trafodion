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
#ifndef ELEMDDLPARTITION_H
#define ELEMDDLPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartition.h
 * Description:  class to contain information about partition attributes
 *               associating with a DDL statement.  This class is a
 *               base class.  Classes ElemDDLPartitionRange and
 *               ElemDDLPartitionSystem are derived from this class.
 *
 *
 * Created:      4/6/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */


#include "ElemDDLNode.h"
#include "parser/ElemDDLPartitionClause.h"
#include "ElemDDLPartitionArray.h"
#include "ItemConstValueArray.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPartition;
class ElemDDLPartitionV2;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None
class ElemDDLPartitionV2Array;

// -----------------------------------------------------------------------
// definition of class ElemDDLPartition
// -----------------------------------------------------------------------
class ElemDDLPartition : public ElemDDLNode
{

public:

  enum optionEnum { ADD_OPTION, DROP_OPTION };

  // default constructor
  ElemDDLPartition(OperatorTypeEnum operType =
                          ELM_ANY_PARTITION_ELEM)
  : ElemDDLNode(operType)
  , isTheSpecialCase1aOr1b_(FALSE) // Fix for Bugzilla bug 1369

  { }

  // virtual destructor
  virtual ~ElemDDLPartition();

  // cast
  virtual ElemDDLPartition * castToElemDDLPartition();

  //
  // accessors
  //

  inline NABoolean isTheSpecialCase1aOr1b() const
  { return isTheSpecialCase1aOr1b_; } // Fix for Bugzilla bug 1369

  //
  // mutators
  //

  inline void setTheSpecialCase1aOr1bFlag(NABoolean setting)
  { isTheSpecialCase1aOr1b_= setting; }

private:

  NABoolean isTheSpecialCase1aOr1b_; // Fix for Bugzilla bug 1369 - See comments in StmtDDLCreate.cpp

}; // class ElemDDLPartition

// class for hash/range/list partition new implement

class ElemDDLPartitionV2 : public ElemDDLNode
{
public:
  ElemDDLPartitionV2( NAString &pname
        , ItemExpr *partValue = NULL
        , ElemDDLNode *subPart = NULL);
  
  ElemDDLPartitionV2( Int32 numPart
        , ElemDDLNode* subPart = NULL);

  virtual ~ElemDDLPartitionV2();
  // cast
  virtual ElemDDLPartitionV2 * castToElemDDLPartitionV2()
  { return this; }

  static NABoolean isSupportedOperatorType(OperatorTypeEnum type)
  {
    switch(type)
    {
      case ITM_CONSTANT :
      case ITM_TO_TIMESTAMP :
      case ITM_TO_DATE :
      case ITM_DATEFORMAT :
        return true;
        break;

      default :
        return false;
    }
  }

  // accessors
  inline NABoolean hasSubpartition() { return hasSubparition_; }
  inline ElemDDLNode *getSubpartition() { return subPartition_; }
  ElemDDLPartitionV2Array * getSubpartitionArray()
  { return subpartitionArray_; }

  inline const NAString &getPartitionName() const
  { return partitionName_; }

  inline ItemExpr* getPartitionValue()
  { return partitionValue_; }

  short buildPartitionValueArray(NABoolean strict = true);
  inline const ItemExprList &getPartionValueArray() const
  { return partionValueArray_; }

  NABoolean isValidMaxValue(ColReference *valueExpr);
  short replaceWithConstValue();

private:
  NAString partitionName_;
  Int32 numPartitions_;
  NABoolean hasSubparition_;
  ItemExpr* partitionValue_;
  //parser node
  ElemDDLNode *subPartition_;
  ElemDDLPartitionV2Array *subpartitionArray_;
  ItemExprList partionValueArray_;
};

#endif // ELEMDDLPARTITION_H

