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

#include "exp/exp_stdh.h"
#include "exp/exp_clause_derived.h"

ex_expr::exp_return_type ex_inout_clause::eval(char * /*op_data*/[], CollHeap *, ComDiagsArea **) {
  return ex_expr::EXPR_OK;
}
