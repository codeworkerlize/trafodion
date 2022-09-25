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
#ifndef STMTDDLALTERTABLEUNMOUNTPARTITION_H
#define STMTDDLALTERTABLEUNMOUNTPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableUnmountPartition.h
 * Description:  class for Alter Table <table-name>  Unmount Partition ...
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
#include "ElemDDLPartitionNameAndForValues.h"
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableUnmountPartition;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableUnmountPartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableUnmountPartition : public StmtDDLAlterTable
{
  friend void StmtDDLAlterTableUnmountParititon_visit(ElemDDLNode* pThisNode,
    CollIndex index,
    ElemDDLNode* pElement);

public:

  // constructor
  StmtDDLAlterTableUnmountPartition(ElemDDLNode* pPartitionAction);


  // virtual destructor
  virtual ~StmtDDLAlterTableUnmountPartition();

  // cast
  virtual StmtDDLAlterTableUnmountPartition* castToStmtDDLAlterTableUnmountPartition();

  ElemDDLPartitionNameAndForValuesArray& getPartitionNameAndForValuesArray()
  {
    return partNameAndForValuesArray_;
  }

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();

  // method for tracing
  virtual const NAString getText() const;

private:
  ElemDDLPartitionNameAndForValuesArray partNameAndForValuesArray_;

}; // class StmtDDLAlterTableUnmountPartition



#endif // STMTDDLALTERTABLEUNMOUNTPARTITION_H
