#ifndef STMTDDLUSERGROUP_H
#define STMTDDLUSERGROUP_H
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
 * File:         StmtDDLUserGroup.h
 * Description:  class for parse nodes representing register, alter,  and 
 *                 unregister user group statements
 *
 * Created:      July 25, 2017
 * Language:     C++
 *
 *****************************************************************************
 */

#include "ComSmallDefs.h"
#include "StmtDDLNode.h"
#include "ElemDDLList.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLUserGroup;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Register and unregister user statements
// -----------------------------------------------------------------------
class StmtDDLUserGroup : public StmtDDLNode
{

public:

  enum UserGroupType { REGISTER_USER_GROUP, 
                       ALTER_USER_GROUP,
                       UNREGISTER_USER_GROUP,
                       ADD_USER_GROUP_MEMBER,
                       REMOVE_USER_GROUP_MEMBER };

  // constructors
  // register and alter user
  StmtDDLUserGroup(const NAString & userGroupName,
                   const NAString * pConfig,
                   NABoolean isRegister,
                   CollHeap * heap)
 : StmtDDLNode(DDL_USER_GROUP),
   externalGroupName_(userGroupName, heap), 
   memberList_(NULL), 
   dropBehavior_(COM_UNKNOWN_DROP_BEHAVIOR)
  {
    if (pConfig)
    {
      NAString config(*pConfig, heap);
      config_ = config;
    }
    userGroupType_ = (isRegister ? REGISTER_USER_GROUP : ALTER_USER_GROUP);
  }

  //for alter group add/remove member
  StmtDDLUserGroup(const NAString &userGroupName,
                   ElemDDLNode* memberList,
                   NABoolean isMemberAdd,
                   CollHeap *heap)
      : StmtDDLNode(DDL_USER_GROUP),
        externalGroupName_(userGroupName, heap),
        memberList_(memberList),
        dropBehavior_(COM_UNKNOWN_DROP_BEHAVIOR)
  {
      //not need pConfig,the 'alter group add/remove member' statement is only use on local user auth
      userGroupType_ = isMemberAdd ? ADD_USER_GROUP_MEMBER : REMOVE_USER_GROUP_MEMBER;
  }

  // unregister user
  StmtDDLUserGroup(const NAString & userGroupName,
                   ComDropBehavior dropBehavior,
                   CollHeap * heap)
  : StmtDDLNode(DDL_USER_GROUP),
    externalGroupName_(userGroupName),
    memberList_(NULL),
    dropBehavior_(dropBehavior),
    userGroupType_(UNREGISTER_USER_GROUP)
  {}

  // virtual destructor
  virtual ~StmtDDLUserGroup()
  {
      if (memberList_)
      {
          delete memberList_;
      }
  }

  // cast
  virtual StmtDDLUserGroup * castToStmtDDLUserGroup() { return this; }

  // for binding
  ExprNode * bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString & getGroupName() const { return externalGroupName_; }
  inline const NAString & getConfig() const { return config_; }
  inline const UserGroupType getUserGroupType() const { return userGroupType_; }
  inline const ComDropBehavior getDropBehavior() const { return dropBehavior_; }
  inline ElemDDLList* getMembers() const { return (ElemDDLList *)memberList_; }

private:

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString externalGroupName_;
  NAString config_;
  UserGroupType userGroupType_;
  ComDropBehavior dropBehavior_;
  ElemDDLNode* memberList_;

}; // class StmtDDLUserGroup

#endif // STMTDDLUSERGROUP_H
