
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
#include "common/unicode_char_set.h"
#include "common/wstr.h"
#include "executor/ex_globals.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_datetime.h"
#include "exp/exp_stdh.h"

ex_expr::exp_return_type ex_comp_clause::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  if (isSpecialNulls()) {
    // special nulls. Nulls are values.
    // Null = Null, non-null-value < NULL, etc.
    short left_is_null = 0;
    short right_is_null = 0;

    if (getOperand(1)->getNullFlag() && (!op_data[1])) left_is_null = -1;

    if (getOperand(2)->getNullFlag() && (!op_data[2])) right_is_null = -1;

    int result = 0;
    if ((left_is_null) || (right_is_null)) {
      switch (getOperType()) {
        case ITM_EQUAL:
          result = (left_is_null && right_is_null ? -1 : 0);
          break;
        case ITM_NOT_EQUAL:
          result = (left_is_null && right_is_null ? 0 : -1);
          break;

        case ITM_GREATER:
          result = (right_is_null ? 0 : -1);
          break;

        case ITM_LESS:
          result = (left_is_null ? 0 : -1);
          break;

        case ITM_GREATER_EQ:
          result = (left_is_null ? -1 : 0);
          break;
        case ITM_LESS_EQ:
          result = (right_is_null ? -1 : 0);
          break;
      }

      if (result) {
        // the actual result of this operation is pointed to
        // by op_data[2 * MAX_OPERANDS].
        *(int *)op_data[2 * MAX_OPERANDS] = 1;  // result is TRUE
      } else {
        *(int *)op_data[2 * MAX_OPERANDS] = 0;  // result is FALSE

        if ((getRollupColumnNum() >= 0) && (getExeGlobals())) {
          getExeGlobals()->setRollupColumnNum(getRollupColumnNum());
        }
      }
      return ex_expr::EXPR_NULL;
    }  // one of the operands is a null value.
  }    // nulls are to be treated as values

  for (short i = 1; i < getNumOperands(); i++) {
    // if value is missing,
    // then move boolean unknown value to result and return.
    if (getOperand(i)->getNullFlag() && (!op_data[i]))  // missing value
    {
      // move null value to result.
      *(int *)op_data[2 * MAX_OPERANDS] = -1;
      return ex_expr::EXPR_NULL;
    }
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_comp_clause::processResult(int compare_code, int *result, CollHeap *heap,
                                                       ComDiagsArea **diagsArea) {
  *result = 0;

  switch (getOperType()) {
    case ITM_EQUAL:
      if (compare_code == 0) *result = 1;
      break;
    case ITM_NOT_EQUAL:
      if (compare_code != 0) *result = 1;
      break;

    case ITM_LESS:
      if (compare_code < 0) *result = 1;
      break;

    case ITM_LESS_EQ:
      if (compare_code <= 0) *result = 1;
      break;

    case ITM_GREATER:
      if (compare_code > 0) *result = 1;
      break;

    case ITM_GREATER_EQ:
      if (compare_code >= 0) *result = 1;
      break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
      break;
  }
  return ex_expr::EXPR_OK;
}

/////////////////////////////////////////////////////////////////
// Compares operand 1 and operand 2. Moves boolean result to
// operand 0. Result is a boolean datatype.
// Result values: 1, TRUE. 0, FALSE.
//               -1, NULL (but this shouldn't happen here.
//                         Nulls have already been processed
//                         before coming here).
////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ex_comp_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  switch (getInstruction()) {
    // EQUAL opcode
    case EQ_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] == *(Int8 *)op_data[2]);
      break;

    case EQ_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] == *(UInt8 *)op_data[2]);
      break;

    case EQ_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] == *(short *)op_data[2]);
      break;

    case EQ_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] == *(unsigned short *)op_data[2]);
      break;
    case EQ_BIN16S_BIN32U:
      *(int *)op_data[0] = ((int)*(short *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] == *(short *)op_data[2]);
      break;
    case EQ_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] == *(unsigned short *)op_data[2]);
      break;

    case EQ_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] == *(short *)op_data[2]);
      break;

    case EQ_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] == *(unsigned short *)op_data[2]);
      break;

    case EQ_BIN32S_BIN32U:
      *(int *)op_data[0] = ((int)*(int *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN32U_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] == (int)*(short *)op_data[2]);
      break;

    case EQ_BIN32U_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] == (int)*(int *)op_data[2]);
      break;

    case EQ_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] == *(unsigned short *)op_data[2]);
      break;

    case EQ_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] == *(int *)op_data[2]);
      break;

    case EQ_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] == *(long *)op_data[2]);
      break;

    case EQ_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] == *(UInt64 *)op_data[2]);
      break;

    case EQ_BIN64U_BIN64S:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] == *(long *)op_data[2]);
      break;

    case EQ_BIN64S_BIN64U:
      *(int *)op_data[0] = (*(long *)op_data[1] == *(UInt64 *)op_data[2]);
      break;

    case EQ_DECU_DECU:
    case EQ_DECS_DECS:
    case EQ_ASCII_F_F:
    case EQ_UNICODE_F_F:  // 11/3/97 added for Unicode support

      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) == 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case EQ_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] == *(float *)op_data[2]);
      break;

    case EQ_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] == *(double *)op_data[2]);
      break;

    case EQ_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) == 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    // NOT EQUAL operator
    case NE_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] != *(Int8 *)op_data[2]);
      break;

    case NE_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] != *(UInt8 *)op_data[2]);
      break;

    case NE_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] != *(short *)op_data[2]);
      break;

    case NE_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] != *(unsigned short *)op_data[2]);
      break;

    case NE_BIN16S_BIN32U:
      *(int *)op_data[0] = ((int)*(short *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] != *(short *)op_data[2]);
      break;

    case NE_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] != *(int *)op_data[2]);
      break;
    case NE_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] != *(unsigned short *)op_data[2]);
      break;

    case NE_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] != *(short *)op_data[2]);
      break;

    case NE_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] != *(unsigned short *)op_data[2]);
      break;

    case NE_BIN32S_BIN32U:
      *(int *)op_data[0] = ((int)*(int *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN32U_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] != (int)*(short *)op_data[2]);
      break;

    case NE_BIN32U_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] != (int)*(int *)op_data[2]);
      break;

    case NE_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] != *(unsigned short *)op_data[2]);
      break;
    case NE_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] != *(int *)op_data[2]);
      break;

    case NE_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] != *(long *)op_data[2]);
      break;

    case NE_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] != *(UInt64 *)op_data[2]);
      break;

    case NE_BIN64U_BIN64S:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] != *(long *)op_data[2]);
      break;

    case NE_BIN64S_BIN64U:
      *(int *)op_data[0] = (*(long *)op_data[1] != *(UInt64 *)op_data[2]);
      break;

    case NE_DECU_DECU:
    case NE_DECS_DECS:
    case NE_ASCII_F_F:
      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) != 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case NE_UNICODE_F_F:  // 11/3/97: Added for Unicode support
      if (wc_str_cmp((NAWchar *)op_data[1], (NAWchar *)op_data[2], (int)getOperand(1)->getLength() >> 1) != 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;
    case NE_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] != *(float *)op_data[2]);
      break;

    case NE_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] != *(double *)op_data[2]);
      break;

    case NE_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) != 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    // LESS THAN opcode
    case LT_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] < *(Int8 *)op_data[2]);
      break;

    case LT_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] < *(UInt8 *)op_data[2]);
      break;

    case LT_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] < *(short *)op_data[2]);
      break;

    case LT_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] < *(int *)op_data[2]);
      break;

    case LT_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] < *(unsigned short *)op_data[2]);
      break;

    case LT_BIN16S_BIN32U:
      *(int *)op_data[0] = ((long)*(short *)op_data[1] < (long)*(int *)op_data[2]);
      break;

    case LT_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] < *(short *)op_data[2]);
      break;
    case LT_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] < *(int *)op_data[2]);
      break;
    case LT_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] < *(unsigned short *)op_data[2]);
      break;

    case LT_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] < *(int *)op_data[2]);
      break;

    case LT_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] < *(short *)op_data[2]);
      break;

    case LT_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] < *(int *)op_data[2]);
      break;

    case LT_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] < *(unsigned short *)op_data[2]);
      break;

    case LT_BIN32S_BIN32U:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] < (long)*(int *)op_data[2]);
      break;

    case LT_BIN32U_BIN16S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] < *(short *)op_data[2]);
      break;

    case LT_BIN32U_BIN32S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] < (long)*(int *)op_data[2]);
      break;

    case LT_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] < *(unsigned short *)op_data[2]);
      break;

    case LT_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] < *(int *)op_data[2]);
      break;

    case LT_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] < *(long *)op_data[2]);
      break;

    case LT_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] < *(UInt64 *)op_data[2]);
      break;

    case LT_BIN64U_BIN64S:
      *(int *)op_data[0] = ((*(long *)op_data[2] < 0) ? 0 : (*(UInt64 *)op_data[1] < *(long *)op_data[2]));
      break;

    case LT_BIN64S_BIN64U:
      *(int *)op_data[0] = ((*(long *)op_data[1] < 0) ? 1 : (*(long *)op_data[1] < *(UInt64 *)op_data[2]));
      break;

    case LT_DECS_DECS: {
      if ((op_data[1][0] & 0200) == 0) {
        // first operand is positive
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) < 0)  // l < r
            *(int *)op_data[0] = 1;
          else
            *(int *)op_data[0] = 0;
        } else {
          // second operand is negative
          *(int *)op_data[0] = 0;  // +ve not < -ve
        }
      } else {
        // first operand is negative
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          *(int *)op_data[0] = 1;  // -ve negative always < +ve
        } else {
          // second operand is negative
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) <= 0)  // l <= r
            *(int *)op_data[0] = 0;
          else
            *(int *)op_data[0] = 1;
        }
      }  // first operand is negative
    } break;

    case LT_DECU_DECU:
    case LT_ASCII_F_F:
      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) < 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case LT_UNICODE_F_F:  // 11/5/97: added for Unicode support
      if (wc_str_cmp((NAWchar *)op_data[1], (NAWchar *)op_data[2], (int)getOperand(1)->getLength() >> 1) < 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;

      break;

    case LT_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] < *(float *)op_data[2]);
      break;

    case LT_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] < *(double *)op_data[2]);
      break;

    case LT_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) < 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    // LESS THAN OR EQUAL TO opcode
    case LE_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] <= *(Int8 *)op_data[2]);
      break;

    case LE_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] <= *(UInt8 *)op_data[2]);
      break;

    case LE_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] <= *(short *)op_data[2]);
      break;

    case LE_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] <= *(int *)op_data[2]);
      break;

    case LE_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] <= *(unsigned short *)op_data[2]);
      break;

    case LE_BIN16S_BIN32U:
      *(int *)op_data[0] = (*(short *)op_data[1] <= (long)*(int *)op_data[2]);
      break;

    case LE_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] <= *(short *)op_data[2]);
      break;

    case LE_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] <= *(int *)op_data[2]);
      break;

    case LE_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] <= *(unsigned short *)op_data[2]);
      break;

    case LE_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] <= *(int *)op_data[2]);
      break;

    case LE_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] <= *(short *)op_data[2]);
      break;

    case LE_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] <= *(int *)op_data[2]);
      break;

    case LE_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] <= *(unsigned short *)op_data[2]);
      break;

    case LE_BIN32S_BIN32U:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] <= (long)*(int *)op_data[2]);
      break;

    case LE_BIN32U_BIN16S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] <= *(short *)op_data[2]);
      break;

    case LE_BIN32U_BIN32S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] <= (long)*(int *)op_data[2]);
      break;

    case LE_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] <= *(unsigned short *)op_data[2]);
      break;

    case LE_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] <= *(int *)op_data[2]);
      break;

    case LE_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] <= *(long *)op_data[2]);
      break;

    case LE_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] <= *(UInt64 *)op_data[2]);
      break;

    case LE_BIN64U_BIN64S:
      *(int *)op_data[0] = ((*(long *)op_data[2] < 0) ? 0 : (*(UInt64 *)op_data[1] <= *(long *)op_data[2]));
      break;

    case LE_BIN64S_BIN64U:
      *(int *)op_data[0] = ((*(long *)op_data[1] < 0) ? 1 : (*(long *)op_data[1] <= *(UInt64 *)op_data[2]));
      break;

    case LE_DECS_DECS: {
      if ((op_data[1][0] & 0200) == 0) {
        // first operand is positive

        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) <= 0)  // l <= r
            *(int *)op_data[0] = 1;
          else
            *(int *)op_data[0] = 0;
        } else {
          // second operand is negative
          *(int *)op_data[0] = 0;  // +ve not < -ve
        }
      } else {
        // first operand is negative
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          *(int *)op_data[0] = 1;  // -ve negative always < +ve
        } else {
          // second operand is negative
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) < 0)  // l < r
            *(int *)op_data[0] = 0;
          else
            *(int *)op_data[0] = 1;
        }
      }  // first operand is negative
    } break;

    case LE_DECU_DECU:
    case LE_ASCII_F_F:
      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) <= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case LE_UNICODE_F_F:  // 11/5/97: added for Unicode support
      if (wc_str_cmp((NAWchar *)op_data[1], (NAWchar *)op_data[2], (int)getOperand(1)->getLength() >> 1) <= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;

      break;

    case LE_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] <= *(float *)op_data[2]);
      break;

    case LE_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] <= *(double *)op_data[2]);
      break;

    case LE_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) <= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    // GREATER THAN opcode
    case GT_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] > *(Int8 *)op_data[2]);
      break;

    case GT_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] > *(UInt8 *)op_data[2]);
      break;

    case GT_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] > *(short *)op_data[2]);
      break;

    case GT_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] > *(int *)op_data[2]);
      break;

    case GT_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] > *(unsigned short *)op_data[2]);
      break;

    case GT_BIN16S_BIN32U:
      *(int *)op_data[0] = (*(short *)op_data[1] > (long)*(int *)op_data[2]);
      break;

    case GT_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] > *(short *)op_data[2]);
      break;

    case GT_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] > *(int *)op_data[2]);
      break;

    case GT_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] > *(unsigned short *)op_data[2]);
      break;

    case GT_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] > *(int *)op_data[2]);
      break;

    case GT_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] > *(short *)op_data[2]);
      break;

    case GT_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] > *(int *)op_data[2]);
      break;

    case GT_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] > *(unsigned short *)op_data[2]);
      break;

    case GT_BIN32S_BIN32U:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] > (long)*(int *)op_data[2]);
      break;

    case GT_BIN32U_BIN16S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] > *(short *)op_data[2]);
      break;

    case GT_BIN32U_BIN32S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] > (long)*(int *)op_data[2]);
      break;

    case GT_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] > *(unsigned short *)op_data[2]);
      break;

    case GT_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] > *(int *)op_data[2]);
      break;

    case GT_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] > *(long *)op_data[2]);
      break;

    case GT_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] > *(UInt64 *)op_data[2]);
      break;

    case GT_BIN64U_BIN64S:
      *(int *)op_data[0] = ((*(long *)op_data[2] < 0) ? 1 : (*(UInt64 *)op_data[1] > *(long *)op_data[2]));
      break;

    case GT_BIN64S_BIN64U:
      *(int *)op_data[0] = ((*(long *)op_data[1] < 0) ? 0 : (*(long *)op_data[1] > *(UInt64 *)op_data[2]));
      break;

    case GT_DECS_DECS: {
      if ((op_data[1][0] & 0200) == 0) {
        // first operand is positive
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) > 0)  // l > r
            *(int *)op_data[0] = 1;
          else
            *(int *)op_data[0] = 0;
        } else {
          // second operand is negative
          *(int *)op_data[0] = 1;  // +ve always > -ve
        }
      } else {
        // first operand is negative
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          *(int *)op_data[0] = 0;  // -ve always <= +ve
        } else {
          // second operand is negative
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) >= 0)  // l >= r
            *(int *)op_data[0] = 0;
          else
            *(int *)op_data[0] = 1;
        }
      }  // first operand is negative
    } break;

    case GT_DECU_DECU:
    case GT_ASCII_F_F:
      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) > 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case GT_UNICODE_F_F:
      // 11/3/97: added for Unicode
      if (wc_str_cmp((NAWchar *)op_data[1], (NAWchar *)op_data[2], (int)(getOperand(1)->getLength()) >> 1) > 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case GT_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] > *(float *)op_data[2]);
      break;

    case GT_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] > *(double *)op_data[2]);
      break;

    case GT_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) > 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    // GREATER THAN OR EQUAL TO
    case GE_BIN8S_BIN8S:
      *(int *)op_data[0] = (*(Int8 *)op_data[1] >= *(Int8 *)op_data[2]);
      break;

    case GE_BIN8U_BIN8U:
      *(int *)op_data[0] = (*(UInt8 *)op_data[1] >= *(UInt8 *)op_data[2]);
      break;

    case GE_BIN16S_BIN16S:
      *(int *)op_data[0] = (*(short *)op_data[1] >= *(short *)op_data[2]);
      break;

    case GE_BIN16S_BIN32S:
      *(int *)op_data[0] = (*(short *)op_data[1] >= *(int *)op_data[2]);
      break;

    case GE_BIN16S_BIN16U:
      *(int *)op_data[0] = (*(short *)op_data[1] >= *(unsigned short *)op_data[2]);
      break;

    case GE_BIN16S_BIN32U:
      *(int *)op_data[0] = (*(short *)op_data[1] >= (long)*(int *)op_data[2]);
      break;

    case GE_BIN16U_BIN16S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] >= *(short *)op_data[2]);
      break;
    case GE_BIN16U_BIN32S:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] >= *(int *)op_data[2]);
      break;

    case GE_BIN16U_BIN16U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] >= *(unsigned short *)op_data[2]);
      break;

    case GE_BIN16U_BIN32U:
      *(int *)op_data[0] = (*(unsigned short *)op_data[1] >= *(int *)op_data[2]);
      break;

    case GE_BIN32S_BIN16S:
      *(int *)op_data[0] = (*(int *)op_data[1] >= *(short *)op_data[2]);
      break;

    case GE_BIN32S_BIN32S:
      *(int *)op_data[0] = (*(int *)op_data[1] >= *(int *)op_data[2]);
      break;

    case GE_BIN32S_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] >= *(unsigned short *)op_data[2]);
      break;

    case GE_BIN32S_BIN32U:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] >= (long)*(int *)op_data[2]);
      break;

    case GE_BIN32U_BIN16S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] >= *(short *)op_data[2]);
      break;

    case GE_BIN32U_BIN32S:
      *(int *)op_data[0] = ((long)*(int *)op_data[1] >= (long)*(int *)op_data[2]);
      break;

    case GE_BIN32U_BIN16U:
      *(int *)op_data[0] = (*(int *)op_data[1] >= *(unsigned short *)op_data[2]);
      break;

    case GE_BIN32U_BIN32U:
      *(int *)op_data[0] = (*(int *)op_data[1] >= *(int *)op_data[2]);
      break;

    case GE_BIN64S_BIN64S:
      *(int *)op_data[0] = (*(long *)op_data[1] >= *(long *)op_data[2]);
      break;

    case GE_BIN64U_BIN64U:
      *(int *)op_data[0] = (*(UInt64 *)op_data[1] >= *(UInt64 *)op_data[2]);
      break;

    case GE_BIN64U_BIN64S:
      *(int *)op_data[0] = ((*(long *)op_data[2] < 0) ? 1 : (*(UInt64 *)op_data[1] >= *(long *)op_data[2]));
      break;

    case GE_BIN64S_BIN64U:
      *(int *)op_data[0] = ((*(long *)op_data[1] < 0) ? 0 : (*(long *)op_data[1] >= *(UInt64 *)op_data[2]));
      break;

    case GE_DECS_DECS: {
      if ((op_data[1][0] & 0200) == 0) {
        // first operand is positive
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) >= 0)  // l >= r
            *(int *)op_data[0] = 1;
          else
            *(int *)op_data[0] = 0;
        } else {
          // second operand is negative
          *(int *)op_data[0] = 1;  // +ve always >= -ve
        }
      } else {
        // first operand is negative
        if ((op_data[2][0] & 0200) == 0) {
          // second operand is positive
          *(int *)op_data[0] = 0;  // -ve always < +ve
        } else {
          // second operand is negative
          if (str_cmp(op_data[1], op_data[2],
                      (int)getOperand(1)->getLength()) > 0)  // l > r
            *(int *)op_data[0] = 0;
          else
            *(int *)op_data[0] = 1;
        }
      }  // first operand is negative
    } break;

    case GE_DECU_DECU:
    case GE_ASCII_F_F:
      if (str_cmp(op_data[1], op_data[2], (int)getOperand(1)->getLength()) >= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case GE_UNICODE_F_F:
      // 11/3/97: added for Unicode
      if (wc_str_cmp((NAWchar *)op_data[1], (NAWchar *)op_data[2], (int)(getOperand(1)->getLength()) >> 1) >= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case GE_FLOAT32_FLOAT32:
      *(int *)op_data[0] = (*(float *)op_data[1] >= *(float *)op_data[2]);
      break;

    case GE_FLOAT64_FLOAT64:
      *(int *)op_data[0] = (*(double *)op_data[1] >= *(double *)op_data[2]);
      break;

    case GE_DATETIME_DATETIME:
      if (((ExpDatetime *)getOperand(1))->compDatetimes(op_data[1], op_data[2]) >= 0)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = 0;
      break;

    case ASCII_COMP:
    case EQ_ASCII_COMP:
    case GT_ASCII_COMP:
    case GE_ASCII_COMP:
    case LT_ASCII_COMP:
    case LE_ASCII_COMP:
    case NE_ASCII_COMP: {
      int length1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
      int length2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

      char padChar = ' ';

      if (getCollationEncodeComp()) {
        padChar = 0;
      }

      int compare_code;
      if (getInstruction() == ASCII_COMP)  // char vs char, ignore trailing blank
        compare_code = charStringCompareWithPad(op_data[1], length1, op_data[2], length2, padChar, true);
      else
        compare_code = charStringCompareWithPad(op_data[1], length1, op_data[2], length2, padChar);

      retcode = processResult(compare_code, (int *)op_data[0], heap, diagsArea);
      break;
    }

    case UNICODE_COMP:  // 11/3/95: Unicode
    {
      int length1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
      int length2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

      int compare_code = wcharStringCompareWithPad((NAWchar *)op_data[1], length1 >> 1, (NAWchar *)op_data[2],
                                                   length2 >> 1, unicode_char_set::space_char());

      retcode = processResult(compare_code, (int *)op_data[0], heap, diagsArea);

      break;
    }

      // boolean comparison
    case EQ_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] == *(Int8 *)op_data[2]);
    } break;

    case NE_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] != *(Int8 *)op_data[2]);
    } break;

    case LT_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] < *(Int8 *)op_data[2]);
    } break;

    case LE_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] <= *(Int8 *)op_data[2]);
    } break;

    case GT_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] > *(Int8 *)op_data[2]);
    } break;

    case GE_BOOL_BOOL: {
      *(int *)op_data[0] = (*(Int8 *)op_data[1] >= *(Int8 *)op_data[2]);
    } break;

    case COMP_COMPLEX:
      *(int *)op_data[0] = ((ComplexType *)getOperand(1))->comp(getOperType(), getOperand(2), op_data);
      break;

    case BINARY_COMP:
    case BINARY_COMP_VAR: {
      int length1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
      int length2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

      char padChar = 0;

      int compare_code;
      if (getInstruction() == BINARY_COMP)
        compare_code = charStringCompareWithPad(op_data[1], length1, op_data[2], length2, padChar, true);
      else
        compare_code = charStringCompareWithPad(op_data[1], length1, op_data[2], length2, padChar);

      retcode = processResult(compare_code, (int *)op_data[0], heap, diagsArea);
    } break;

    case COMP_NOT_SUPPORTED: {
      // this comparison operation not supported.
      // See if it could still be evaluated by doing some intermediate
      // operations.
      if (evalUnsupportedOperations(op_data, heap, diagsArea) != ex_expr::EXPR_OK) return ex_expr::EXPR_ERROR;
    } break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      retcode = ex_expr::EXPR_ERROR;
      break;
  }

  if ((getRollupColumnNum() >= 0) && (*(int *)op_data[0] == 0) && (getExeGlobals())) {
    getExeGlobals()->setRollupColumnNum(getRollupColumnNum());
  }

  return retcode;
}
ex_expr::exp_return_type ex_comp_clause::evalUnsupportedOperations(char *op_data[], CollHeap *heap,
                                                                   ComDiagsArea **diagsArea) {
  // if this operation could be done by converting to an
  // intermediate datatype, do it.
  short op1Type = getOperand(1)->getDatatype();
  short op2Type = getOperand(2)->getDatatype();

  ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
  return ex_expr::EXPR_ERROR;
}
