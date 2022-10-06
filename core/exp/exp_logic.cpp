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

#include <stddef.h>
#include "exp/exp_stdh.h"
#include "exp/exp_clause_derived.h"

ex_expr::exp_return_type ex_unlogic_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;
  Attributes *tgtAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  switch (getOperType()) {
    case ITM_IS_NULL: {
      // null value for operand 1 is pointed to by
      // op_data[- (2 * MAX_OPERANDS) + 1].
      if (srcAttr->getNullFlag() &&             // nullable and
          (!op_data[-(2 * MAX_OPERANDS) + 1]))  // missing (null)
      {
        if (tgtAttr->isSQLMXAlignedFormat())
          ExpAlignedFormat::setNullBit(op_data[0], tgtAttr->getNullBitIndex());
        else
          *(int *)op_data[0] = 1;  // value is null
      } else {
        if (tgtAttr->isSQLMXAlignedFormat())
          ExpAlignedFormat::clearNullBit(op_data[0], tgtAttr->getNullBitIndex());
        else
          *(int *)op_data[0] = 0;
      }
    } break;

    case ITM_IS_NOT_NULL: {
      if (getOperand(1)->getNullFlag() && (!op_data[-(2 * MAX_OPERANDS) + 1])) {
        if (tgtAttr->isSQLMXAlignedFormat())
          ExpAlignedFormat::clearNullBit(op_data[0], tgtAttr->getNullBitIndex());
        else
          *(int *)op_data[0] = 0;
      } else {
        if (tgtAttr->isSQLMXAlignedFormat())
          ExpAlignedFormat::setNullBit(op_data[0], tgtAttr->getNullBitIndex());
        else
          *(int *)op_data[0] = 1;  // value is not null
      }
    } break;

    case ITM_IS_UNKNOWN: {
      if (*(int *)op_data[1] == -1)  // null
        *(int *)op_data[0] = 1;      // result is true
      else
        *(int *)op_data[0] = 0;  // result is false
    } break;

    case ITM_IS_NOT_UNKNOWN: {
      if (*(int *)op_data[1] != -1)  // not null
        *(int *)op_data[0] = 1;      // result is true
      else
        *(int *)op_data[0] = 0;  // result is false
    } break;

    case ITM_IS_TRUE: {
      if (*(int *)op_data[1] == 1)  // true
        *(int *)op_data[0] = 1;     // result is true
      else
        *(int *)op_data[0] = 0;  // result is false
    } break;

    case ITM_IS_FALSE: {
      if (*(int *)op_data[1] == 0)  // false
        *(int *)op_data[0] = 1;     // result is true
      else
        *(int *)op_data[0] = 0;  // result is false
    } break;

    case ITM_NOT: {
      if (*(int *)op_data[1] == 1)       // TRUE
        *(int *)op_data[0] = 0;          // make it FALSE
      else if (*(int *)op_data[1] == 0)  // FALSE
        *(int *)op_data[0] = 1;          // make it TRUE
      else
        *(int *)op_data[0] = -1;  // NULL
    } break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      retcode = ex_expr::EXPR_ERROR;
      break;
  }
  return retcode;
}
