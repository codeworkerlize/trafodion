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
#ifndef STMTDDLALTERTABLEMOUNTPARTITION_H
#define STMTDDLALTERTABLEMOUNTPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableMountPartition.h
 * Description:  class for Alter Table <table-name>  Mount Partition ...
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
class StmtDDLAlterTableMountPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableMountPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableMountPartition : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableMountPartition(ElemDDLPartitionV2 *tgtPartition, NAString s, NABoolean v);

  // virtual destructor
  virtual ~StmtDDLAlterTableMountPartition();

  // cast
  virtual StmtDDLAlterTableMountPartition *castToStmtDDLAlterTableMountPartition();

  // accessor
  inline ElemDDLNode *getPartitionAction() const;

  // method for tracing
  virtual const NAString getText() const;

  ElemDDLPartitionV2 *getTargetPartition() { return targetPartition_; }
  NAString getTargetPartitionName() { return targetPartitionName_; }

  NABoolean getValidation() { return validation_; }

  void setValidation(NABoolean v) { validation_ = v; }

 private:
  ElemDDLPartitionV2 *targetPartition_;
  NABoolean validation_;
  NAString targetPartitionName_;

};  // class StmtDDLAlterTableMountPartition

#endif  // STMTDDLALTERTABLEMOUNTPARTITION_H
