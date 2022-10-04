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
#ifndef STMTDDLALTERTABLEADDPARTITION_H
#define STMTDDLALTERTABLEADDPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableAddPartition.h
 * Description:  class for Alter Table <table-name>  Add Partition ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      9/20/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableAddPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAddPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableAddPartition : public StmtDDLAlterTable {
 public:
  // constructor
  // StmtDDLAlterTableAddPartition(ElemDDLPartitionV2* tgtPartition);
  StmtDDLAlterTableAddPartition(ElemDDLNode *tgtPartition);

  // virtual destructor
  virtual ~StmtDDLAlterTableAddPartition();

  // cast
  virtual StmtDDLAlterTableAddPartition *castToStmtDDLAlterTableAddPartition();

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

  NABoolean isAddSinglePartition() const;
  ElemDDLPartitionV2 *getTargetPartition() { return targetPartitions_->castToElemDDLPartitionV2(); }
  ElemDDLPartitionList *getTargetPartitions() { return targetPartitions_->castToElemDDLPartitionList(); }

 private:
  ElemDDLNode *targetPartitions_;
};  // class StmtDDLAlterTableAddPartition

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableMergePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableMergePartition : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableMergePartition(ElemDDLNode *srcPartitions, NAString tgtPartition);
  StmtDDLAlterTableMergePartition(ElemDDLNode *beginPartition, ElemDDLNode *endPartition, NAString tgtPartition);

  // virtual destructor
  virtual ~StmtDDLAlterTableMergePartition();

  // cast
  virtual StmtDDLAlterTableMergePartition *castToStmtDDLAlterTableMergePartition();

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

  virtual ExprNode *bindNode(BindWA *pBindWA);

  ElemDDLNode *getBeginPartition() { return beginPartition_; }
  ElemDDLNode *getEndPartition() { return endPartition_; }

  ElemDDLNode *getSourcePartitions() { return sourcePartitions_; }
  NAString getTargetPartition() { return targetPartition_; }

  NAList<NAPartition *> &getSortedSrcPart() { return sortedSrcPart_; }
  NAPartition *getTgtPart() { return tgtPart_; }

  NAPartition *getNAPartitionFromElem(BindWA *pBindWA, TableDesc *tableDesc, std::map<NAString, NAPartition *> &partMap,
                                      ElemDDLPartitionNameAndForValues *elem);

 private:
  ElemDDLNode *beginPartition_;
  ElemDDLNode *endPartition_;

  ElemDDLNode *sourcePartitions_;
  NAString targetPartition_;

  NAList<NAPartition *> sortedSrcPart_;
  NAPartition *tgtPart_;
};  // class StmtDDLAlterTableMergePartition

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableMergePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableExchangePartition : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableExchangePartition(NABoolean isSubpart, ElemDDLNode *srcPartition, QualifiedName tgtPartition);

  // virtual destructor
  virtual ~StmtDDLAlterTableExchangePartition();

  // cast
  virtual StmtDDLAlterTableExchangePartition *castToStmtDDLAlterTableExchangePartition();

  virtual ExprNode *bindNode(BindWA *pBindWA);

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

  NABoolean getIsSubPartition() { return isSubPartition_; }
  ElemDDLNode *getTablePartition() { return partition_; }
  QualifiedName &getExchangeTableName() { return exchangeTableName_; }

  NAPartition *getNAPartition() { return naPartition_; }

 private:
  NABoolean isSubPartition_;

  ElemDDLNode *partition_;
  QualifiedName exchangeTableName_;

  NAPartition *naPartition_;
};  // class StmtDDLAlterTableMergePartition

#endif  // STMTDDLALTERTABLEADDPARTITION_H
