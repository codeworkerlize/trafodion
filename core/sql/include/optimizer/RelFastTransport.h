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
#ifndef RELFASTTRANSPORT_H
#define RELFASTTRANSPORT_H
/* -*-C++-*-
**************************************************************************
*
* File:         RelFastTransport.h
* Description:  RelExprs related to support of FastTransport
* Created:      09/29/12
* Language:     C++
*
*************************************************************************
*/

#include "optimizer/RelExpr.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class CostMethodFastExtract;

class UnloadOption {
 public:
  enum UnloadOptionType {
    DELIMITER_,
    NULL_STRING_,
    RECORD_SEP_,
    APPEND_,
    HEADER_,
    COMPRESSION_,
    EMPTY_TARGET_,
    LOG_ERRORS_,
    STOP_AFTER_N_ERRORS_,
    NO_OUTPUT_,
    COMPRESS_,
    ONE_FILE_,
    USE_SNAPSHOT_SCAN_
  };
  UnloadOption(UnloadOptionType option, int numericVal, char *stringVal, char *stringVal2 = NULL)
      : option_(option), numericVal_(numericVal), stringVal_(stringVal){};

 private:
  UnloadOptionType option_;
  int numericVal_;
  char *stringVal_;
};

#endif /* RELFASTTRANSPORT_H */
