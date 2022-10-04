/* -*-C++-*-
******************************************************************************
*
* File:         AllRelExpr.h
* Description:  Just a dumb file that includes RelExpr.h and all the files
*               that define classes derived from RelExpr
*
* Created:      11/04/94
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
******************************************************************************
*/

// -----------------------------------------------------------------------

#include "optimizer/RelExpr.h"
#include "optimizer/Rel3GL.h"
#include "optimizer/RelControl.h"
#include "RelDCL.h"
#include "RelEnforcer.h"
#include "optimizer/RelGrby.h"
#include "RelJoin.h"
#include "optimizer/RelExeUtil.h"
#include "RelFastTransport.h"
#include "optimizer/RelMisc.h"
#include "optimizer/RelRoutine.h"
#include "RelProbeCache.h"
#include "optimizer/RelStoredProc.h"
#include "optimizer/RelScan.h"
#include "optimizer/RelSet.h"
#include "RelUpdate.h"
#include "MultiJoin.h"

// eof
