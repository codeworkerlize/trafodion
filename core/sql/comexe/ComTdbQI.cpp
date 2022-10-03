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
****************************************************************************
*
* File:         ComTdbQI.cpp
* Description:
*
* Created:      3/29/2021
* Language:     C++
*
*
*
*
****************************************************************************
*/

#include "comexe/ComTdbQI.h"
#include "comexe/ComTdbCommon.h"
#include "comexe/ComTdbRoot.h"
#include "common/ComCextdecs.h"

ComTdbQryInvalid::ComTdbQryInvalid() : ComTdb(ComTdb::ex_QUERY_INVALIDATION, eye_QUERY_INVALIDATION) {}

ComTdbQryInvalid::ComTdbQryInvalid(ULng32 tupleLen, ULng32 returnedTupleLen, ULng32 inputTupleLen,
                                   ex_cri_desc *criDescParentDown, ex_cri_desc *criDescParentUp,
                                   queue_index queueSizeDown, queue_index queueSizeUp, Lng32 numBuffers,
                                   ULng32 bufferSize, ex_expr *scanExpr, ex_expr *inputExpr, ex_expr *projExpr,
                                   ex_cri_desc *workCriDesc, UInt16 qi_row_atp_index, UInt16 input_row_atp_index)
    : ComTdb(ComTdb::ex_QUERY_INVALIDATION, eye_QUERY_INVALIDATION, (Cardinality)0.0, criDescParentDown,
             criDescParentUp, queueSizeDown, queueSizeUp, numBuffers, bufferSize),
      scanExpr_(scanExpr),
      inputExpr_(inputExpr),
      projExpr_(projExpr),
      workCriDesc_(workCriDesc),
      tupleLen_(tupleLen),
      returnedTupleLen_(returnedTupleLen),
      inputTupleLen_(inputTupleLen),
      qiTupleAtpIndex_(qi_row_atp_index),
      inputTupleAtpIndex_(input_row_atp_index) {}

const char *ComTdbQryInvalid::getExpressionName(Int32 expNum) const {
  switch (expNum) {
    case 0:
      return "Scan Expr";
    case 1:
      return "Input Expr";
    default:
      return 0;
  }
}

ex_expr *ComTdbQryInvalid::getExpressionNode(Int32 expNum) {
  switch (expNum) {
    case 0:
      return scanExpr_;
    case 1:
      return inputExpr_;
    default:
      return 0;
  }
}

Long ComTdbQryInvalid::pack(void *space) {
  scanExpr_.pack(space);
  inputExpr_.pack(space);
  projExpr_.pack(space);
  workCriDesc_.pack(space);

  return ComTdb::pack(space);
}

Lng32 ComTdbQryInvalid::unpack(void *base, void *reallocator) {
  if (scanExpr_.unpack(base, reallocator)) return -1;
  if (inputExpr_.unpack(base, reallocator)) return -1;
  if (projExpr_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}
