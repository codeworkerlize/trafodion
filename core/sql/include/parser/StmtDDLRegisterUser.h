#ifndef STMTDDLREGISTERUSER_H
#define STMTDDLREGISTERUSER_H
//******************************************************************************
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
//******************************************************************************
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLRegisterUser.h
 * Description:  class for parse nodes representing register and unregister
 *                 user statements
 *
 * Created:      February 2, 2011
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/ComLocationNames.h"
#include "ElemDDLLocation.h"
#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"
#include "ElemDDLAuthSchema.h"

#define CREATE_USER_PASSWORD 100
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLRegisterUser;
class StmtDDLRegisterUserArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Register and unregister user statements
// -----------------------------------------------------------------------
class StmtDDLRegisterUser : public StmtDDLNode {
 public:
  enum RegisterUserType { REGISTER_USER, UNREGISTER_USER, REGISTER_TENANT, UNREGISTER_TENANT };

  // constructors
  // register user
  StmtDDLRegisterUser(const NAString &externalUserName, const NAString *pDbUserName, const NAString *pConfig,
                      ElemDDLNode *authSchema, NABoolean isUser, CollHeap *heap, const NAString *authPassword = NULL);

  // unregister user
  StmtDDLRegisterUser(const NAString &dbUserName, ComDropBehavior dropBehavior, NABoolean isUser, CollHeap *heap);

  // virtual destructor
  virtual ~StmtDDLRegisterUser();

  // cast
  virtual StmtDDLRegisterUser *castToStmtDDLRegisterUser();

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString &getExternalUserName() const { return externalUserName_; }
  inline const NAString &getDbUserName() const { return dbUserName_; }
  inline const NAString &getConfig() const { return config_; }
  inline const NAString &getAuthPassword() const { return authPassword_; }
  inline const RegisterUserType getRegisterUserType() const { return registerUserType_; }
  inline const ComDropBehavior getDropBehavior() const { return dropBehavior_; }
  inline const NABoolean isSchemaSpecified() const { return authSchema_ ? TRUE : FALSE; }
  inline const SchemaName *getSchemaName() const {
    return (authSchema_ && authSchema_->isSchemaNameSpecified()) ? &authSchema_->getSchemaName() : NULL;
  }
  inline const ComSchemaClass getSchemaClass() const {
    return (authSchema_) ? authSchema_->getSchemaClass() : COM_SCHEMA_CLASS_DEFAULT;
  }
  inline const NABoolean isSetupWithDefaultPassword() { return this->isSetupWithDefaultPassword_; }

  // for tracing

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString externalUserName_;
  NAString dbUserName_;
  NAString config_;
  NAString authPassword_;
  RegisterUserType registerUserType_;
  ComDropBehavior dropBehavior_;
  ElemDDLAuthSchema *authSchema_;
  NABoolean isSetupWithDefaultPassword_;

};  // class StmtDDLRegisterUser

// -----------------------------------------------------------------------
// Definition of class StmtDDLRegisterUserArray
// -----------------------------------------------------------------------
class StmtDDLRegisterUserArray : public LIST(StmtDDLRegisterUser *) {
 public:
  // constructor
  StmtDDLRegisterUserArray(CollHeap *heap) : LIST(StmtDDLRegisterUser *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLRegisterUserArray();

 private:
};  // class StmtDDLRegisterUserArray

#endif  // STMTDDLREGISTERUSER_H
