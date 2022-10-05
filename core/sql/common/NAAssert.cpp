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
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "common/NAAssert.h"
#include "cli/SQLCLIdev.h"

#include <iostream>

// To debug an inline function:
// - Put a "NAInlineBreakpoint;" statement in the inline function;
// - Optionally edit this function, NAInlineBreakpointFunc,
//   to return based on the parameters (i.e., filter out names of
//   files you aren't interested in breaking on each time),
//   - and rebuild;
// - Set a breakpoint here, after the filtering-out lines you added.
void NAInlineBreakpointFunc(char *file_, int line_) {
  cout << "Tracepoint at: file " << file_ << ", line " << line_ << "\n";
}

NAAssertGlobals *GetNAAssertGlobals(NABoolean *logEmsEvents) { return NULL; }

NABoolean IsExecutor(NABoolean *logEmsEvents) {
  if (logEmsEvents != NULL) *logEmsEvents = TRUE;
  return FALSE;
}
