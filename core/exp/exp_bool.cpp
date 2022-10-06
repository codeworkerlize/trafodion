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

#include <stddef.h>

#include "common/Platform.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_stdh.h"

ex_expr::exp_return_type ex_bool_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  // boolean values: 0 = false, 1 = true, -1 = null
  switch (getOperType()) {
    case ITM_AND:
      if ((*(int *)op_data[1] == -1) && (*(int *)op_data[2] != 0))
        *(int *)op_data[0] = -1;
      else if (*(int *)op_data[1] == 0)
        *(int *)op_data[0] = 0;
      else
        *(int *)op_data[0] = *(int *)op_data[2];
      break;

    case ITM_OR:
      if ((*(int *)op_data[1] == -1) && (*(int *)op_data[2] != 1))
        *(int *)op_data[0] = -1;
      else if (*(int *)op_data[1] == 1)
        *(int *)op_data[0] = 1;
      else
        *(int *)op_data[0] = *(int *)op_data[2];

      break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      retcode = ex_expr::EXPR_ERROR;
      break;
  }
  return retcode;
}

///////////////////////////////////////////////////////////////
// class ex_branch_clause
///////////////////////////////////////////////////////////////
ex_expr::exp_return_type ex_branch_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  // boolean values: 0 = false, 1 = true, -1 = null

  switch (getOperType()) {
    case ITM_AND:
      if (*(int *)op_data[1] == 0) {
        *(int *)op_data[0] = 0;
        setNextClause(branch_clause);
      } else {
        *(int *)op_data[0] = *(int *)op_data[1];
        setNextClause(saved_next_clause);
      }

      break;

    case ITM_OR:
      if (*(int *)op_data[1] == 1) {
        *(int *)op_data[0] = 1;
        setNextClause(branch_clause);
      } else {
        *(int *)op_data[0] = *(int *)op_data[1];
        setNextClause(saved_next_clause);
      }

      break;

    case ITM_RETURN_TRUE:
      setNextClause(branch_clause);
      break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      retcode = ex_expr::EXPR_ERROR;
      break;
  }
  return retcode;
}

/////////////////////////////////////////////////////////////
// class bool_result_clause
/////////////////////////////////////////////////////////////
ex_expr::exp_return_type bool_result_clause::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  // boolean values: 0 = false, 1 = true, -1 = null
  if ((*(int *)op_data[0] == 0) || (*(int *)op_data[0] == -1))
    return ex_expr::EXPR_FALSE;
  else
    return ex_expr::EXPR_TRUE;
}
