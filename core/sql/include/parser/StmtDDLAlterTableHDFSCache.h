#ifndef STMTDDLALTERTABLEHDFSCACHE_H
#define STMTDDLALTERTABLEHDFSCACHE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableHDFSCache.h
 * Description:  class for 
 *               --Add table to HDFS cache pool
 *               Alter Table <table-name> CACHE IN <pool-name>
 *               --Remove table from HDFS cache pool 
 *               Alter Table <table-name> DECACHE FROM <pool-name>
 *
 *               
 *
 *
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
 *
 *
 *****************************************************************************
 */

#include "StmtDDLAlterTable.h"

class StmtDDLAlterTableHDFSCache : public StmtDDLAlterTable
{
public:

  // constructor
  StmtDDLAlterTableHDFSCache(const NAString &     pool,
                             NABoolean            atc, 
                             NAMemory *           heap = PARSERHEAP()
                            );

  // virtual destructor
  virtual ~StmtDDLAlterTableHDFSCache();
  //cast
  virtual StmtDDLAlterTableHDFSCache * castToStmtDDLAlterTableHDFSCache();

  // method for tracing
  virtual const NAString getText() const 
  { 
    return "StmtDDLAlterTableHDFSCache"; 
  }

  NAString & poolName() { return poolName_; }

  NABoolean & isAddToCache() { return isAddToCache_; }

private:

  NAString poolName_;

  NABoolean isAddToCache_;

}; // class StmtDDLAlterTableHDFSCache

#endif //STMTDDLALTERTABLEHDFSCACHE_H
