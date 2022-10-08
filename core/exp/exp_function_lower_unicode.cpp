
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_function_lower_unicode.h
 * RCS:          $Id:
 * Description:  The implementation of NCHAR version of SQL LOWER() function
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

ex_function_lower_unicode::ex_function_lower_unicode(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space) {}

ex_function_lower_unicode::ex_function_lower_unicode() {}

extern NAWchar unicodeToLower(NAWchar wc);

ex_expr::exp_return_type ex_function_lower_unicode::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  getOperand(0)->setVarLength(len1, op_data[-MAX_OPERANDS]);

  // Now, copy the contents of operand 1 after the case change into
  // operand 0.

  NAWchar *target = (NAWchar *)op_data[0];
  NAWchar *source = (NAWchar *)op_data[1];
  int wc_len = len1 / sizeof(NAWchar);

  for (int i = 0; i < wc_len; i++) {
    // op_data[0][i] =  TOLOWER(op_data[1][i]);
    // op_data[0][i] = '1';

    target[i] = unicode_char_set::to_lower(source[i]);
  }

  return ex_expr::EXPR_OK;
};
