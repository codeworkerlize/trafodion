
#include "comexe/ComKeyMDAM.h"

#include "comexe/ComKeyRange.h"
#include "comexe/ComPackDefs.h"
#include "exp/exp_clause_derived.h"

keyMdamGen::keyMdamGen(int keyLen, ex_cri_desc *workCriDesc, unsigned short keyValuesAtpIndex,
                       unsigned short excludeFlagAtpIndex, unsigned short dataConvErrorFlagAtpIndex,
                       unsigned short valueAtpIndex, MdamColumnGen *first, MdamColumnGen *last,
                       NABoolean complementKeysBeforeReturning, CollHeap *heap)
    : keyRangeGen(KEYMDAM, keyLen, workCriDesc, keyValuesAtpIndex, excludeFlagAtpIndex, dataConvErrorFlagAtpIndex),
      valueAtpIndex_(valueAtpIndex),
      first_(first),
      last_(last),
      complementKeysBeforeReturning_(complementKeysBeforeReturning) {
  // calculate the maximum disjunct number (and the number of columns
  // while we are at it)

  maxDisjunctNumber_ = 0;  // make sure there is at least one disjunct
                           // (if there aren't any the query is known
                           // to be false and we should not get here)
  int numberOfColumns = 0;
  MdamColumnGen *cg = first;
  for (; cg != 0; cg = cg->getNext()) {
    int temp = cg->getLastDisjunctNumber();
    if (temp > maxDisjunctNumber_) maxDisjunctNumber_ = temp;
    numberOfColumns++;
  }

  // calculate upper bounds on storage required

  // for a discussion on these upper bounds, see
  // \tess.$sqldoc.gemdoc.arkmemmg.

  maxMdamIntervals_ = 0;
  maxMdamRefs_ = 0;
  maxMdamRefsForStopLists_ = 0;

  int sumByColumn = 0;
  int maxByColumn = -1;
  int numberOfDisjuncts = maxDisjunctNumber_ + 1;
  int *sumsByDisjunct = NULL;
  sumsByDisjunct = new (heap) int[numberOfDisjuncts];
  int d;
  int maxByColumnAndDisjunct = -1;
  int nullableCount = 0;
  int totalSum = 0;

  for (d = 0; d < numberOfDisjuncts; d++) sumsByDisjunct[d] = 0;

  for (cg = first; cg != 0; cg = cg->getNext()) {
    MdamPred *p = cg->getFirstPred();
    NABoolean isNullable = TRUE;  // $$$ safe stub for now

    sumByColumn = 1;
    if (isNullable) {
      nullableCount++;
      sumByColumn++;
    }

    for (d = 0; d < numberOfDisjuncts; d++) {
      // calculate the number of equality predicates and the
      // number of other predicates for this column and disjunct
      int equalityPreds = 0;
      int otherPreds = 0;

      while ((p) && (p->getDisjunctNumber() <= d)) {
        if (p->getPredType() == MdamPred::MDAM_EQ)
          equalityPreds++;
        else
          otherPreds++;
        p = p->getNext();
      }

      int columnDisjunctCount = 2 * equalityPreds + otherPreds + 1;
      if (isNullable) columnDisjunctCount++;

      // at this point,
      //     columnDisjunctCount = 2 * # of equality predicates on
      //                                 this column and disjunct
      //                         + # of other predicates on this
      //                                 column and disjunct
      //                         + (1 if column is nullable, 0 otherwise)
      //                         + 1

      if (maxByColumnAndDisjunct < columnDisjunctCount) maxByColumnAndDisjunct = columnDisjunctCount;

      sumByColumn += 2 * equalityPreds + otherPreds;
      sumsByDisjunct[d] += 2 * equalityPreds + otherPreds;
    }

    // at this point,
    //       sumByColumn = 2 * # of equality predicates on this column
    //                   + # of other predicates on this column
    //                   + (1 if column is nullable, 0 otherwise)
    //                   + 1

    if (maxByColumn < sumByColumn) maxByColumn = sumByColumn;
  }

  // at this point,
  //    sumsByDisjunct[d] = 2 * # of equality predicates on disjunct d
  //                      + # of other predicates on disjunct d

  int maxByDisjunct = -1;

  for (d = 0; d < numberOfDisjuncts; d++) {
    totalSum += sumsByDisjunct[d];
    if (maxByDisjunct < sumsByDisjunct[d]) maxByDisjunct = sumsByDisjunct[d];
  }

  maxByDisjunct += (nullableCount + numberOfColumns);
  totalSum += (nullableCount + numberOfColumns);

  // at this point,
  //    maxByDisjunct = max over all disjuncts of
  //                  ( 2 * # of equality predicates on disjunct d
  //                    + # of other predicates on disjunct d
  //                    + # of nullable columns
  //                    + # of columns )
  // and
  //   totalSum = 2 * # of equality predicates in the scan
  //            + # of other predicates in the scan
  //            + # of nullable columns
  //            + # of columns

  NADELETEBASIC(sumsByDisjunct, heap);  // done with it now

  // calculate upper bound on number of MdamIntervals

  maxMdamIntervals_ = 1 + maxByColumnAndDisjunct + maxByDisjunct + totalSum + maxByColumn;

  // calculate upper bound on number of MdamRefListEntrys

  maxMdamRefs_ = numberOfDisjuncts * totalSum + (numberOfDisjuncts - 1) * maxByColumn;

  // set the upper bound on number of MdamRefListEntrys used fr stop lists.
  maxMdamRefsForStopLists_ = numberOfDisjuncts;
};

keyMdamGen::~keyMdamGen() {
  MdamColumnGen *next;

  for (MdamColumnGen *m = first_; m != NULL; m = next) {
    next = m->getNext();  // get it before we destroy it
    delete m;
  }

  first_ = 0;
  last_ = 0;
};

Long keyMdamGen::pack(void *space) {
  first_.pack(space);
  last_.pack(space);
  return keyRangeGen::pack(space);
}

int keyMdamGen::unpack(void *base, void *reallocator) {
  // On NSK and Linux, there are stack limitations that are hit when we use
  // recursive calls. Make this an iterative function instead.

  MdamColumnGenPtr currGenPtr;
  MdamColumnGen *nextColumnGen;
  MdamColumnGen *currColumnGen;

  if (first_.unpack(base, reallocator)) return -1;
  currColumnGen = first_;
  if (currColumnGen != NULL) {
    nextColumnGen = first_->getNext();
    currGenPtr = nextColumnGen;
  } else
    currGenPtr = 0;

  while (currGenPtr != (MdamColumnGenPtr)NULL) {
    if (currGenPtr.unpack(base, reallocator)) return -1;

    nextColumnGen = currGenPtr.getPointer();
    currColumnGen->setNext(nextColumnGen);
    currColumnGen = nextColumnGen;
    nextColumnGen = currColumnGen->getNext();
    currGenPtr = nextColumnGen;
  }

  if (last_.unpack(base, reallocator)) return -1;
  currColumnGen = last_;
  if (currColumnGen != NULL) {
    nextColumnGen = last_->getNext();
    currGenPtr = nextColumnGen;
  } else
    currGenPtr = 0;

  while (currGenPtr != (MdamColumnGenPtr)NULL) {
    if (currGenPtr.unpack(base, reallocator)) return -1;

    nextColumnGen = currGenPtr.getPointer();
    currColumnGen->setNext(nextColumnGen);
    currColumnGen = nextColumnGen;
    nextColumnGen = currColumnGen->getNext();
    currGenPtr = nextColumnGen;
  }
  return keyRangeGen::unpack(base, reallocator);
}

ex_expr *keyMdamGen::getExpressionNode(int) { return NULL; }

ex_expr::exp_return_type MdamColumnGen::fixup(int base, unsigned short mode, Space *space, CollHeap *heap,
                                              const ex_tcb *tcb) {
  // the return from this procedure will be either EXPR_OK or it will be
  // the return from the first fixup() that fails
  ex_expr::exp_return_type rc;
  ex_expr::exp_return_type rc1;

  rc = loExpr_->fixup(base, mode, tcb, space, heap, FALSE, NULL);

  rc1 = hiExpr_->fixup(base, mode, tcb, space, heap, FALSE, NULL);
  if (rc == ex_expr::EXPR_OK) rc = rc1;

  rc1 = nonNullLoExpr_->fixup(base, mode, tcb, space, heap, FALSE, NULL);
  if (rc == ex_expr::EXPR_OK) rc = rc1;

  rc1 = nonNullHiExpr_->fixup(base, mode, tcb, space, heap, FALSE, NULL);
  if (rc == ex_expr::EXPR_OK) rc = rc1;

  for (MdamPred *p = preds_; p != 0; p = p->getNext()) {
    rc1 = p->fixup(base, mode, tcb, space, heap);
    if (rc == ex_expr::EXPR_OK) rc = rc1;
  }

  return rc;
};

MdamColumnGen::~MdamColumnGen() {
  MdamPred *next = preds_;

  while (preds_) {
    MdamPred *current = next;

    next = current->getNext();  // get it before we destroy it
    delete current;
  }
  preds_ = 0;
}

Long MdamColumnGen::pack(void *space) {
  loExpr_.pack(space);
  hiExpr_.pack(space);
  nonNullLoExpr_.pack(space);
  nonNullHiExpr_.pack(space);
  preds_.pack(space);
  lastPred_.pack(space);
  previous_.pack(space);
  next_.pack(space);
  return NAVersionedObject::pack(space);
}

int MdamColumnGen::unpack(void *base, void *reallocator) {
  if (loExpr_.unpack(base, reallocator)) return -1;
  if (hiExpr_.unpack(base, reallocator)) return -1;
  if (nonNullLoExpr_.unpack(base, reallocator)) return -1;
  if (nonNullHiExpr_.unpack(base, reallocator)) return -1;
  // On NSK and Linux, there are stack limitations that are hit when we use
  // recursive calls. Make this an iterative function instead.

  MdamPredPtr currPredPtr;
  MdamPred *nextPred;
  MdamPred *currPred;

  if (preds_.unpack(base, reallocator)) return -1;
  currPred = preds_;
  if (currPred != NULL) {
    nextPred = preds_->getNext();
    currPredPtr = nextPred;
  } else
    currPredPtr = 0;

  while (currPredPtr != (MdamPredPtr)NULL) {
    if (currPredPtr.unpack(base, reallocator)) return -1;
    nextPred = currPredPtr.getPointer();
    currPred->setNext(nextPred);
    currPred = nextPred;
    nextPred = currPred->getNext();
    currPredPtr = nextPred;
  }
  if (lastPred_.unpack(base, reallocator)) return -1;
  if (previous_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

MdamPred::MdamPredType MdamPred::getTransformedPredType(int dataConvErrorFlag, int dataConvErrorFlag2,
                                                        MdamEnums::MdamInclusion &startInclusion,
                                                        MdamEnums::MdamInclusion &endInclusion) {
  MdamPredType returnPredType = (MdamPredType)predType_;

  switch (predType_) {
    case MdamPred::MDAM_EQ:
      if (dataConvErrorFlag != ex_conv_clause::CONV_RESULT_OK) returnPredType = MdamPred::MDAM_RETURN_FALSE;
      break;
    case MdamPred::MDAM_LE:
      switch (dataConvErrorFlag) {
        case ex_conv_clause::CONV_RESULT_OK:
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
          // predType remains MDAM_LE.
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
        case ex_conv_clause::CONV_RESULT_FAILED:
          returnPredType = MdamPred::MDAM_RETURN_FALSE;
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
          returnPredType = MdamPred::MDAM_LT;
          break;
        default:
          // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
          break;
      }
      break;
    case MdamPred::MDAM_LT:
      switch (dataConvErrorFlag) {
        case ex_conv_clause::CONV_RESULT_OK:
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
          // predType remains MDAM_LT.
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
        case ex_conv_clause::CONV_RESULT_FAILED:
          returnPredType = MdamPred::MDAM_RETURN_FALSE;
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
          returnPredType = MdamPred::MDAM_LE;
          break;
        default:
          // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
          break;
      }
      break;
    case MdamPred::MDAM_GE:
      switch (dataConvErrorFlag) {
        case ex_conv_clause::CONV_RESULT_OK:
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
          // predType remains MDAM_GE.
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
        case ex_conv_clause::CONV_RESULT_FAILED:
          returnPredType = MdamPred::MDAM_RETURN_FALSE;
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
          returnPredType = MdamPred::MDAM_GT;
          break;
        default:
          // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
          break;
      }
      break;
    case MdamPred::MDAM_GT:
      switch (dataConvErrorFlag) {
        case ex_conv_clause::CONV_RESULT_OK:
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
          // predType remains MDAM_GT.
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
        case ex_conv_clause::CONV_RESULT_FAILED:
          returnPredType = MdamPred::MDAM_RETURN_FALSE;
          break;
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
        case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
          returnPredType = MdamPred::MDAM_GE;
          break;
        default:
          // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
          break;
      }
      break;
    case MdamPred::MDAM_BETWEEN:
      // predType remains MDAM_BETWEEN in all cases (unless an error makes it
      // MDAM_RETURN_FALSE); only the inclusivity of the endpoints is subject
      // to change. We don't want to modify an MdamPred object here (runtime),
      // so the inclusivity values are passed by reference to this function
      // and the possibly modified values are used by the caller.

      // Check start of interval.
      if (startInclusion == MdamEnums::MDAM_INCLUDED) {
        switch (dataConvErrorFlag) {
          case ex_conv_clause::CONV_RESULT_OK:
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
            // start of interval remains inclusive
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
          case ex_conv_clause::CONV_RESULT_FAILED:
            returnPredType = MdamPred::MDAM_RETURN_FALSE;
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
            // start of interval becomes noninclusive
            startInclusion = MdamEnums::MDAM_EXCLUDED;
            ;
            break;
          default:
            // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
            break;
        }
      } else  // start of interval is noninclusive
      {
        switch (dataConvErrorFlag) {
          case ex_conv_clause::CONV_RESULT_OK:
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
            // start of interval remains noninclusive
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
          case ex_conv_clause::CONV_RESULT_FAILED:
            returnPredType = MdamPred::MDAM_RETURN_FALSE;
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
            // start of interval becomes inclusive
            startInclusion = MdamEnums::MDAM_INCLUDED;
            break;
          default:
            // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag.");
            break;
        }
      }

      // Check end of interval.
      if (endInclusion == MdamEnums::MDAM_INCLUDED) {
        switch (dataConvErrorFlag2) {
          case ex_conv_clause::CONV_RESULT_OK:
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
            // end of interval remains inclusive.
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
          case ex_conv_clause::CONV_RESULT_FAILED:
            returnPredType = MdamPred::MDAM_RETURN_FALSE;
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
            // end of interval becomes noninclusive.
            endInclusion = MdamEnums::MDAM_EXCLUDED;
            break;
          default:
            // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag2.");
            break;
        }
      } else  // end of interval is noninclusive
      {
        switch (dataConvErrorFlag2) {
          case ex_conv_clause::CONV_RESULT_OK:
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP:
            // end of interval remains noninclusive.
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN:
          case ex_conv_clause::CONV_RESULT_FAILED:
            returnPredType = MdamPred::MDAM_RETURN_FALSE;
            break;
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN:
          case ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX:
            // end of interval becomes inclusive.
            endInclusion = MdamEnums::MDAM_INCLUDED;
            break;
          default:
            // ex_assert(0,"adjustPredType: invalid dataConvErrorFlag2.");
            break;
        }
      }
      break;
    case MdamPred::MDAM_ISNULL:  // IS NULL predicate on ASC column
      break;
    case MdamPred::MDAM_ISNULL_DESC:  // IS NULL predicate on DESC column
      break;
    case MdamPred::MDAM_ISNOTNULL:
      break;
    default:
      // ex_assert(0,"adjustPredType: invalid predType.");
      break;
  }

  return returnPredType;
}

ex_expr::exp_return_type MdamPred::getValue_(ExExprPtr value, atp_struct *atp0, atp_struct *atp1) {
  ex_expr::exp_return_type returnExpReturnType = ex_expr::EXPR_OK;
  if (value) {
    returnExpReturnType = value->eval(atp0, atp1);
  }
  return returnExpReturnType;
}

Long MdamPred::pack(void *space) {
  value_.pack(space);
  value2_.pack(space);
  next_.pack(space);
  return NAVersionedObject::pack(space);
}

int MdamPred::unpack(void *base, void *reallocator) {
  if (value_.unpack(base, reallocator)) return -1;
  if (value2_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

ex_expr::exp_return_type MdamPred::fixup(int base, unsigned short mode, const ex_tcb *tcb, Space *space,
                                         CollHeap *heap) {
  ex_expr::exp_return_type rc = ex_expr::EXPR_OK;

  if (value_) rc = value_->fixup(base, mode, tcb, space, heap, FALSE, NULL);

  if (rc == ex_expr::EXPR_OK && value2_) rc = value2_->fixup(base, mode, tcb, space, heap, FALSE, NULL);

  return rc;
}
