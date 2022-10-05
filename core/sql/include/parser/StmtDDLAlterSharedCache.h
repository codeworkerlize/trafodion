#ifndef STMTDDL_ALTER_SHAREDCACHE_H
#define STMTDDL_ALTER_SHAREDCACHE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterSharedCache.h
 * Description:  base class for Alter Shared Cache statements
 *
 *
 * Created:      09/11/2019
 * Language:     C++
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

#include "StmtDDLNode.h"

class LoadSharedCacheTargetSpec {
 public:
  LoadSharedCacheTargetSpec(ConstStringList *nameList, bool loadLocalIfEmpty)
      : nodeNameList_(nameList), loadLocalIfEmpty_(loadLocalIfEmpty) {}

  ~LoadSharedCacheTargetSpec() {}

  ConstStringList *getNodeNameList() { return nodeNameList_; }
  bool getLoadLocalIfEmpty() { return loadLocalIfEmpty_; }

 protected:
  ConstStringList *nodeNameList_;
  bool loadLocalIfEmpty_;
};

// typedef LoadSharedCacheTargetSpec* pLoadSharedCacheTargetSpec;

class StmtDDLAlterSharedCache : public StmtDDLNode {
 public:
  enum Options { CHECK, CLEAR, DELETE, DISABLE, ENABLE, INSERT, UPDATE, CHECK_DETAILS };

  enum Subject { ALL, SCHEMA, TABLE, INDEX };

  enum SharedObjType { INVALID_, DESCRIPTOR, TABLEDATA };

 public:
  // constructors
  StmtDDLAlterSharedCache(enum StmtDDLAlterSharedCache::Subject, const QualifiedName &name,
                          enum StmtDDLAlterSharedCache::Options, NABoolean isInternal, NAMemory *heap);

  StmtDDLAlterSharedCache(enum StmtDDLAlterSharedCache::Subject, enum StmtDDLAlterSharedCache::Options, NAMemory *heap);

  // virtual destructor
  virtual ~StmtDDLAlterSharedCache(){};

  // cast
  StmtDDLAlterSharedCache *castToStmtDDLAlterSharedCache() { return this; }

  //
  // accessors
  //

  virtual int getArity() const { return 0; }
  virtual ExprNode *getChild(int index) { return NULL; }

  const QualifiedName &getQualifiedName() const { return qualName_; }

  void setQualifiedName(const QualifiedName &name) { qualName_ = name; }

  inline const QualifiedName &getOrigName() const { return origQualName_; }

  const NABoolean isInternal() const { return internal_; }

  // methods for tracing
  virtual const NAString getText() const { return "StmtDDLAlterSharedCache"; }

  virtual const NAString displayLabel1() const {
    return "Object name: " + qualName_.getQualifiedNameAsAnsiString(TRUE, TRUE);
  }

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // resolve name by reparsing the name into components
  void resolveName();

  enum Options getOptions() { return options_; }
  enum Subject getSubject() { return subject_; }

  NABoolean opForTable() { return subject_ == TABLE; }
  NABoolean opForIndex() { return subject_ == INDEX; }
  NABoolean opForSchema() { return subject_ == SCHEMA; }
  NABoolean opForAll() { return subject_ == ALL; }

  NABoolean doCheck() { return options_ == CHECK; }
  NABoolean doCheckDetails() { return options_ == CHECK_DETAILS; }
  NABoolean doUpdate() { return options_ == UPDATE; }
  NABoolean doInsert() { return options_ == INSERT; }
  NABoolean doClear() { return options_ == CLEAR; }

  void setIsDataCache() { sharedObjType_ = TABLEDATA; }
  void setIsDescCache() { sharedObjType_ = DESCRIPTOR; }
  NABoolean isDataCache() { return sharedObjType_ == TABLEDATA; }
  NABoolean isDescCache() { return sharedObjType_ == DESCRIPTOR; }
  NABoolean isInvalid() { return sharedObjType_ == INVALID_; }

  NABoolean isUpdateOp() {
    return (options_ == DISABLE || options_ == ENABLE || options_ == DELETE || options_ == UPDATE ||
            options_ == INSERT || options_ == CLEAR);
  }

  NABoolean clearAll() { return opForAll() && doClear(); }
  NABoolean checkAll() { return opForAll() && doCheck(); }
  NABoolean checkAllDetails() { return doCheckDetails() && opForAll(); }
  NABoolean isCheckOP() { return doCheckDetails() || doCheck(); }

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------
  StmtDDLAlterSharedCache(const StmtDDLAlterSharedCache &);             // DO NOT USE
  StmtDDLAlterSharedCache &operator=(const StmtDDLAlterSharedCache &);  // DO NOT USE

  // the tablename specified by user in the alter stmt.
  // This name is not fully qualified during bind phase.
  QualifiedName origQualName_;

  QualifiedName qualName_;

  enum Options options_;

  enum Subject subject_;

  enum SharedObjType sharedObjType_;

  NABoolean internal_;

};  // class StmtDDLAlterSharedCache

#endif  // STMTDDL_ALTER_SHAREDCACHE_H
