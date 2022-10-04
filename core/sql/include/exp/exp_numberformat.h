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
// Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
**********************************************************************/
#ifndef EXP_NUMBERFORMAT_H
#define EXP_NUMBERFORMAT_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  Number format for TO_CHAR
 *
 * Created:      2/21/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/Platform.h"
#include "exp/ExpError.h"
#include "exp/exp_attrs.h"
#include "common/Int64.h"
#include "exp/exp_clause.h"

class ExpNumerFormat {
 public:
  enum NUMKey {
    NUM_COMMA,
    NUM_DEC,
    NUM_0,
    NUM_9,
    NUM_B,
    NUM_C,
    NUM_D,
    NUM_E,
    NUM_FM,
    NUM_G,
    NUM_L,
    NUM_MI,
    NUM_PR,
    NUM_RN,
    NUM_Rn,
    NUM_S,
    NUM_V,
    NUM_b,
    NUM_c,
    NUM_d,
    NUM_e,
    NUM_fm,
    NUM_g,
    NUM_l,
    NUM_mi,
    NUM_pr,
    NUM_rn,
    NUM_rN,
    NUM_s,
    NUM_v,
    NUM_LAST
  };
  static int convertBigNumToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1, Attributes *arg2,
                                   char *arg1Str, char *arg2Str, CollHeap *heap, ComDiagsArea **diagsArea);

  static int convertFloatToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1, Attributes *arg2,
                                  char *arg1Str, char *arg2Str, CollHeap *heap, ComDiagsArea **diagsArea);

  static int convertInt32ToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1, Attributes *arg2,
                                  char *arg1Str, char *arg2Str, CollHeap *heap, ComDiagsArea **diagsArea);

  static int convertInt64ToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1, Attributes *arg2,
                                  char *arg1Str, char *arg2Str, CollHeap *heap, ComDiagsArea **diagsArea);
};

#endif
