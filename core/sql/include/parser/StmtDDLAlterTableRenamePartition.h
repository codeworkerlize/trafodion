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
#ifndef STMTDDLALTERTABLERENAMEPARTITION_H
#define STMTDDLALTERTABLERENAMEPARTITION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableTruncatePartition.h
 * Description:  class for Alter Table <table-name> Partition ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *               
 * Created:      2021/02/27
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
 #include "StmtDDLAlterTable.h"
 
class StmtDDLAlterTableRenamePartition;
// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableTruncatePartition
// -----------------------------------------------------------------------
class StmtDDLAlterTableRenamePartition : public StmtDDLAlterTable
{
public:
  StmtDDLAlterTableRenamePartition(NAString &oldPartName, NAString &newPartName, NABoolean isForClause, CollHeap *heap);
	StmtDDLAlterTableRenamePartition(ItemExpr *partitionKey, NAString &newPartName, NABoolean isForClause, CollHeap *heap);
	virtual ~StmtDDLAlterTableRenamePartition();
  virtual StmtDDLAlterTableRenamePartition* castToStmtDDLAlterTableRenamePartition();
  inline NAString &getOldPartName() { return oldPartName_; }
  inline NAString &getNewPartName() { return newPartName_; }
  inline const LIST(NAString) *getPartNameList() { return partNameList_; }
  inline NABoolean getIsForClause() { return isForClause_; }
  virtual ExprNode *bindNode(BindWA *pBindWA);
  virtual const NAString getText() const;
private:
  NAString          oldPartName_;   //old partition name
	NAString          newPartName_;   //new partition name
	ItemExpr*         partitionKey_;  //rename for(1,2,3) to p1
  LIST(NAString)*   partNameList_;  //according to partitionKey_ convert to partition name
  NABoolean         isForClause_;
};

#endif
