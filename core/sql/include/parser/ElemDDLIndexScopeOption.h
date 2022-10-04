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
#ifndef ELEMDDLINDEXSCOPEOPTION_H
#define ELEMDDLINDEXSCOPEOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLIndexScopeOption.h
 * Description:  class for Create Index LOCAL OR GLOBAL
 *
 *
 * Created:      04/01/21
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"

class ElemDDLIndexScopeOption : public ElemDDLNode {
 public:
  ElemDDLIndexScopeOption() : ElemDDLNode(ELM_WITH_SCOPE_OPTION_ELEM) {
    LOCAL_OPT_ = FALSE;
    GLOBAL_OPT_ = FALSE;
  }

  // return 1 if populate is specified. Otherwise, return 0.
  ComBoolean getIndexLocalOption() { return LOCAL_OPT_; };

  // return 1 if no populate is specified . Otherwise, return 0.
  ComBoolean getIndexGlobalOption() { return GLOBAL_OPT_; };

  // specified the no populate clause.
  void setIndexLocalClause(ComBoolean option) { LOCAL_OPT_ = option; };

  // specified the populate clause.
  void setIndexGlobalClause(ComBoolean option) { GLOBAL_OPT_ = option; };

  // cast virtual function.
  virtual ElemDDLIndexScopeOption *castToElemDDLIndexScopeOption() { return this; };

 private:
  ComBoolean LOCAL_OPT_;
  ComBoolean GLOBAL_OPT_;
};

#endif  // ELEMDDLINDEXSCOPEOPTION_H