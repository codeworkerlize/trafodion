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
#ifndef ELEMDDLGROUP_H
#define ELEMDDLGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLGroup.h
 * Description:  class for Group infomation elements in ALTER GROUP
 *               DDL statements
 *
 *
 * Created:      02/26/2020
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------

class ElemDDLGroup : public ElemDDLNode {
 public:
  // constructors
  ElemDDLGroup(NAString &authorizationIdentifier, CollHeap *h = 0)
      : ElemDDLNode(ELM_GROUP_ELEM), authorizationIdentifier_(authorizationIdentifier, h) {}

  // copy ctor
  ElemDDLGroup(const ElemDDLGroup &orig, CollHeap *h = 0);  // not written

  virtual ~ElemDDLGroup(){};

  // cast
  virtual ElemDDLGroup *castToElemDDLGroup() { return this; };

  //
  // accessors
  //

  inline const NAString &getAuthorizationIdentifier() const { return authorizationIdentifier_; };

  // member functions for tracing
  virtual const NAString displayLabel1() const {
    return (NAString("Authorization identifier: ") + this->getAuthorizationIdentifier());
  }
  virtual const NAString getText() const { return "ElemDDLGroup"; }

 private:
  NAString authorizationIdentifier_;

};  // class ElemDDLGroup

#endif  // ELEMDDLGROUP_H