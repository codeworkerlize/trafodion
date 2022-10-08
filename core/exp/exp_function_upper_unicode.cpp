
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_function_upper_unicode.h
 * RCS:          $Id:
 * Description:  The implementation of NCHAR version of SQL UPPER() function
 *
 *
 * Created:      7/8/98
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "common/unicode_char_set.h"
#include "exp/exp_function.h"

ex_function_upper_unicode::ex_function_upper_unicode(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space) {}

ex_function_upper_unicode::ex_function_upper_unicode() {}

extern NAWchar unicodeToUpper(NAWchar wc);

ex_expr::exp_return_type ex_function_upper_unicode::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  // Now, copy the contents of operand 1 after the case change into
  // operand 0.

  NAWchar *target = (NAWchar *)op_data[0];
  NAWchar *source = (NAWchar *)op_data[1];
  int wc_len = len1 / sizeof(NAWchar);
  int actual_len = 0;
  NAWchar *tmpWCP = NULL;
  int maxWideChars = getOperand(0)->getLength() / sizeof(NAWchar);

  // JQ
  // A given character will be searched against two mapping tables
  for (int i = 0; i < wc_len; i++) {
    // search against unicode_lower2upper_mapping_table_full
    tmpWCP = unicode_char_set::to_upper_full(source[i]);
    if (tmpWCP) {
      // there is an entry for the given character
      // for the sake of uniformity, the length for the returned NAWchar
      // array is always 3, but the third element can be 0,
      // assign the third element to the target if it is not 0

      if (actual_len + ((tmpWCP[2] == 0) ? 2 : 3) > maxWideChars) {
        ExRaiseSqlError(heap, diagsArea, EXE_STRING_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      }

      target[actual_len++] = tmpWCP[0];
      target[actual_len++] = tmpWCP[1];

      if (tmpWCP[2] != (NAWchar)0) {
        target[actual_len++] = tmpWCP[2];
      }
    } else {
      if (actual_len >= maxWideChars) {
        ExRaiseSqlError(heap, diagsArea, EXE_STRING_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      }

      // a NULL return
      // search against unicode_lower2upper_mapping_table then
      target[actual_len++] = unicode_char_set::to_upper(source[i]);
    }
  }

  getOperand(0)->setVarLength(actual_len * 2, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
};
