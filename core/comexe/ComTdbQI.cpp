
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

ComTdbQryInvalid::ComTdbQryInvalid(int tupleLen, int returnedTupleLen, int inputTupleLen,
                                   ex_cri_desc *criDescParentDown, ex_cri_desc *criDescParentUp,
                                   queue_index queueSizeDown, queue_index queueSizeUp, int numBuffers, int bufferSize,
                                   ex_expr *scanExpr, ex_expr *inputExpr, ex_expr *projExpr, ex_cri_desc *workCriDesc,
                                   UInt16 qi_row_atp_index, UInt16 input_row_atp_index)
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

const char *ComTdbQryInvalid::getExpressionName(int expNum) const {
  switch (expNum) {
    case 0:
      return "Scan Expr";
    case 1:
      return "Input Expr";
    default:
      return 0;
  }
}

ex_expr *ComTdbQryInvalid::getExpressionNode(int expNum) {
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

int ComTdbQryInvalid::unpack(void *base, void *reallocator) {
  if (scanExpr_.unpack(base, reallocator)) return -1;
  if (inputExpr_.unpack(base, reallocator)) return -1;
  if (projExpr_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}
