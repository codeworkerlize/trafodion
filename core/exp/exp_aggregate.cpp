
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

// -----------------------------------------------------------------------

#include <stddef.h>

#include "common/Platform.h"
#include "common/str.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_stdh.h"

///////////////////////////////////////////////////
// class AggrExpr
//////////////////////////////////////////////////
ex_expr::exp_return_type AggrExpr::initializeAggr(atp_struct *atp) {
  if (initExpr_) {
    if (initExpr_->eval(atp, atp) == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
  }

  if (perrecExpr_) {
    ex_clause *clause = perrecExpr_->getClauses();
    while (clause) {
      if (clause->getType() == ex_clause::AGGREGATE_TYPE) {
        if (((ex_aggregate_clause *)clause)->init() == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
      } else {
        if (ExFunctionRangeOfValues::IsRangeOfValuesEnum(clause->getOperType())) {
          if (((ExFunctionRangeOfValues *)clause)->init() == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
        }
      }

      clause = clause->getNextClause();
    }
  }

  if (groupingExpr_) {
    ex_clause *clause = groupingExpr_->getClauses();
    while (clause) {
      if (clause->getType() == ex_clause::AGGREGATE_TYPE) {
        if (((ex_aggregate_clause *)clause)->init() == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
      }

      clause = clause->getNextClause();
    }
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type AggrExpr::finalizeAggr(atp_struct * /*atp*/) { return ex_expr::EXPR_OK; }

ex_expr::exp_return_type AggrExpr::finalizeNullAggr(atp_struct *atp) {
  if (finalNullExpr_)
    return finalNullExpr_->eval(atp, atp);
  else
    return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type AggrExpr::evalGroupingForNull(Int16 startEntry, Int16 endEntry) {
  if (groupingExpr_) {
    ex_clause *clause = groupingExpr_->getClauses();
    while (clause) {
      if (clause->getOperType() == ITM_AGGR_GROUPING_FUNC) {
        ExFunctionGrouping *g = (ExFunctionGrouping *)clause;
        if ((g->getRollupGroupIndex() >= startEntry) && (g->getRollupGroupIndex() <= endEntry)) g->setRollupNull(-1);
      }

      clause = clause->getNextClause();
    }
  }

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////
// class ex_aggregate_clause
//////////////////////////////////////////////////
ex_expr::exp_return_type ex_aggregate_clause::init() { return ex_expr::EXPR_OK; }
ex_expr::exp_return_type ex_aggregate_clause::eval(char * /*op_data*/[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
  return ex_expr::EXPR_ERROR;
}
/////////////////////////////////////////////////
// class ex_aggr_one_row_clause
/////////////////////////////////////////////////
ex_expr::exp_return_type ex_aggr_one_row_clause::init() {
  oneRowProcessed_ = 0;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_aggr_one_row_clause::eval(char * /*op_data*/[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  // if oneRowProcessed_ is > 0, return error.
  if (oneRowProcessed_) {
    ExRaiseSqlError(heap, diagsArea, EXE_CARDINALITY_VIOLATION);
    retcode = ex_expr::EXPR_ERROR;
  } else {
    oneRowProcessed_ = 1;
  }

  return retcode;
}

/////////////////////////////////////////////////
// class ex_aggr_any_true_max_clause
/////////////////////////////////////////////////
ex_expr::exp_return_type ex_aggr_any_true_max_clause::init() {
  nullSeen_ = 0;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_aggr_any_true_max_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  switch (*(int *)op_data[1]) {
    case 1:  // operand is TRUE
    {
      // return TRUE as result
      *(int *)op_data[0] = 1;
      retcode = ex_expr::EXPR_TRUE;
    } break;

    case -1:  // operand is NULL
    {
      // remember that a null was seen.
      nullSeen_ = 1;

      // Genesis 10-040203-2921
      // Fix for nested query returning different number of rows with ESP CQD.
      // The case where all operands are NULL wasnt being handled. When all
      // the operands are NULL, the result too is NULL.
      *(int *)op_data[0] = -1;
    } break;

    case 0:  // operand is FALSE
    {
      if (nullSeen_)
        *(int *)op_data[0] = -1;
      else
        *(int *)op_data[0] = 0;

      retcode = ex_expr::EXPR_TRUE;
    } break;

    default: {
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      retcode = ex_expr::EXPR_ERROR;
    } break;
  }

  // return code of EXPR_TRUE tells the caller to short circuit
  // and not process any more rows.
  return retcode;
}

/////////////////////////////////////////////////
// class ex_aggr_min_max_clause
//  op1: the operand accumulating current computed min/max
//  op2: the result of <A> OR <B>, where
//  <A>:  (<col> is NOT NULL) AND <col> < <op1>
//  <B>:  <op1> is NULL
/////////////////////////////////////////////////
ex_expr::exp_return_type ex_aggr_min_max_clause::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  // if the second expression is true, make child to be current
  // aggregate.
  if (*(int *)op_data[2] == 1) {
    if (getOperand(0)->getNullFlag()) {
      // A pointer to the null indicators of the operands.
      //
      char **null_data = &op_data[-2 * ex_clause::MAX_OPERANDS];

      if ((getOperand(1)->getNullFlag()) && (!null_data[1]))  // missing value, indicates
                                                              // child is null. Keep the current result.
        return ex_expr::EXPR_OK;

      ExpTupleDesc::clearNullValue(null_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
    }

    if (getOperand(0)->getVCIndicatorLength() > 0) {
      // variable length operand. Note that first child (operand1)
      // and result have the SAME attributes for min/max aggr.
      int src_length = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
      int tgt_length = getOperand(0)->getLength();  // max varchar length

      str_cpy_all(op_data[0], op_data[1], src_length);

      // copy source length bytes to target length bytes.
      // Note that the array index -MAX_OPERANDS will get to
      // the corresponding varlen entry for that operand.
      getOperand(0)->setVarLength(src_length, op_data[-MAX_OPERANDS]);
    } else {
      str_cpy_all(op_data[0], op_data[1], getOperand(0)->getLength());
    }
  }

  return retcode;
}

/////////////////////////////////////////////////
// class ExFunctionGrouping
/////////////////////////////////////////////////
ex_expr::exp_return_type ExFunctionGrouping::init() {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  rollupNull_ = 0;

  return retcode;
}

ex_expr::exp_return_type ExFunctionGrouping::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *tgt = op_data[0];
  if (rollupNull_)
    *(UInt32 *)tgt = 1;
  else
    *(UInt32 *)tgt = 0;

  return ex_expr::EXPR_OK;
}

/////////////////////////////////////////////////
// class ex_pivot_group_clause
/////////////////////////////////////////////////
ex_expr::exp_return_type ex_pivot_group_clause::init() {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  currPos_ = 0;
  currTgtLen_ = 0;
  setOvflWarn(FALSE);

  return retcode;
}

ex_expr::exp_return_type ex_pivot_group_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgtOp = getOperand(0);
  Attributes *srcOp = getOperand(1);
  Attributes *delimOp = getOperand(2);

  // do nothing if input is NULL
  if (srcOp->getNullFlag() && (!op_data[-(2 * MAX_OPERANDS) + 1])) {
    return ex_expr::EXPR_OK;
  }

  int src_length = srcOp->getLength(op_data[-MAX_OPERANDS + 1]);
  int delim_length = delimOp->getLength(op_data[-MAX_OPERANDS + 2]);

  char *tgt = op_data[0];
  char *src = op_data[1];
  char *delim = op_data[2];

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    src_length = Attributes::trimFillerSpaces(op_data[1], prec1, src_length, cs);
  }
  cs = ((SimpleType *)getOperand(2))->getCharSet();
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(2))->getPrecision();
    delim_length = Attributes::trimFillerSpaces(op_data[2], prec1, delim_length, cs);
  }

  int currSrcPos = currPos_;

  int delimSize = delim_length;
  int tgtBufNeeded = (currPos_ > 0 ? delimSize : 0) + src_length;

  if ((currTgtLen_ + tgtBufNeeded) > maxLen_) {
    ExRaiseSqlError(heap, diagsArea, EXE_STRING_OVERFLOW);
    return ex_expr::EXPR_ERROR;
  }

  if (currPos_ > 0) {
    str_cpy_all(&tgt[currPos_], delim, delim_length);
    currPos_ += delim_length;
  }

  str_cpy_all(&tgt[currPos_], src, src_length);
  currPos_ += src_length;

  currTgtLen_ += (currPos_ - currSrcPos);

  //
  // Blankpad the target (if needed).
  //
  if ((currTgtLen_) < maxLen_) str_pad(&op_data[0][currTgtLen_], maxLen_ - currTgtLen_, ' ');
  tgtOp->setVarLength(currTgtLen_, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}
