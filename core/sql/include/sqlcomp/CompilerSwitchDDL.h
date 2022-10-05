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

#ifndef _COMPILER_SWITCH_DDL_H_
#define _COMPILER_SWITCH_DDL_H_

#include "common/Platform.h"
#include "cli/Context.h"
#include "arkcmp/CmpContext.h"

// *****************************************************************************
// *
// * File:         CompilerSwitchDDL.h
// * Description:  the class responsible for compiler instance
// *               switching on behave of DDL operations.
// *
// * Contents:
// *
// *****************************************************************************

class CompilerSwitchDDL {
 public:
  CompilerSwitchDDL();
  ~CompilerSwitchDDL(){};

  short switchCompiler(int cntxtType = CmpContextInfo::CMPCONTEXT_TYPE_META);

  short switchBackCompiler();

 protected:
  void setAllFlags();
  void saveAllFlags();
  void restoreAllFlags();
  short sendAllControlsAndFlags(CmpContext *prevContext, int cntxtType);
  void restoreAllControlsAndFlags();

 protected:
  NABoolean cmpSwitched_;
  ULng32 savedCmpParserFlags_;
  ULng32 savedCliParserFlags_;
};

#endif
