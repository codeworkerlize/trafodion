
/* -*-C++-*-
**************************************************************************
*
* File:         TableDesc.C
* Description:  A table descriptor
* Created:      4/27/94
* Language:     C++
* Status:       Experimental
*
*
**************************************************************************
*/

#include "optimizer/Cost.h" /* for lookups in the defaults table */
#include "parser/ParNameLocList.h"
#include "QRDescGenerator.h"
#include "common/ComCextdecs.h"
#include "common/ComOperators.h"
#include "executor/ExExeUtilCli.h"
#include "executor/ex_error.h"
#include "optimizer/AllItemExpr.h"
#include "optimizer/AllRelExpr.h"
#include "optimizer/Analyzer.h"
#include "optimizer/BindWA.h"
#include "optimizer/Sqlcomp.h"
#include "sqlcat/TrafDDLdesc.h"

void PartRangePerCol::print(FILE *ofd, const char *indent, const char *title, CollHeap *c, char *buf) const {
  BUMP_INDENT(indent);
  Space *space = (Space *)c;
  char mybuf[2000];

  // snprintf(mybuf, sizeof(mybuf), "%s%s\n",NEW_INDENT,title);
  // PRINTIT(ofd, c, space, buf, mybuf);

  if (intervalRange_) {
    for (int i = 0; i < intervalRange_->getTotalRanges(); i++) {
      ostringstream os;
      (intervalRange_->getSubranges())[i]->write(os);
      if (i == 0)
        snprintf(mybuf, sizeof(mybuf), "         Interval Range: %4d : %s\n", i, os.str().c_str());
      else
        snprintf(mybuf, sizeof(mybuf), "                         %4d : %s\n", i, os.str().c_str());
      PRINTIT(ofd, c, space, buf, mybuf);
    }
  }

  if (highBoundRange_) {
    for (int i = 0; i < highBoundRange_->getTotalRanges(); i++) {
      ostringstream os;
      (highBoundRange_->getSubranges())[i]->write(os);
      if (i == 0)
        snprintf(mybuf, sizeof(mybuf), "    High Boundary Range: %4d : %s\n", i, os.str().c_str());
      else
        snprintf(mybuf, sizeof(mybuf), "                         %4d : %s\n", i, os.str().c_str());
      PRINTIT(ofd, c, space, buf, mybuf);
    }
  }

  if (lowBoundaryExpr_ != NULL) {
    snprintf(mybuf, sizeof(mybuf), " Low Boundary Predicate: ");
    PRINTIT(ofd, c, space, buf, mybuf);
    lowBoundaryExpr_->print(stdout, "", "");
  } else {
    snprintf(mybuf, sizeof(mybuf), " Low Boundary Predicate: NULL\n");
    PRINTIT(ofd, c, space, buf, mybuf);
  }

  if (intervalExpr_ != NULL) {
    snprintf(mybuf, sizeof(mybuf), "     Interval Predicate: ");
    PRINTIT(ofd, c, space, buf, mybuf);
    intervalExpr_->print(stdout, "", "");
  } else {
    snprintf(mybuf, sizeof(mybuf), "     Interval Predicate: NULL\n");
    PRINTIT(ofd, c, space, buf, mybuf);
  }

  if (highBoundaryExpr_ != NULL) {
    snprintf(mybuf, sizeof(mybuf), "High Boundary Predicate: ");
    PRINTIT(ofd, c, space, buf, mybuf);
    highBoundaryExpr_->print(stdout, "", "");
  } else {
    snprintf(mybuf, sizeof(mybuf), "High Boundary Predicate: NULL\n");
    PRINTIT(ofd, c, space, buf, mybuf);
  }

  if (boundary_) {
    if (boundary_->lowBound_) {
      snprintf(mybuf, sizeof(mybuf), "      Low Boundary Expr: %s\n", boundary_->lowBound_->getText().data());
      PRINTIT(ofd, c, space, buf, mybuf);
    } else {
      snprintf(mybuf, sizeof(mybuf), "      Low Boundary Expr: NULL\n");
      PRINTIT(ofd, c, space, buf, mybuf);
    }

    if (boundary_->highBound_) {
      snprintf(mybuf, sizeof(mybuf), "     High Boundary Expr: %s\n", boundary_->highBound_->getText().data());
      PRINTIT(ofd, c, space, buf, mybuf);
    } else {
      snprintf(mybuf, sizeof(mybuf), "     High Boundary Expr: NULL\n");
      PRINTIT(ofd, c, space, buf, mybuf);
    }
  }
}

void PartRangePerCol::display() { print(); }

void PartRangePerCol::createBoundForPartCol(BindWA *bindWA, const NATable *naTable, BoundaryValue *boundary,
                                            BoundaryValue *nextBoundary, const NABoolean genRange) {
  const NAColumnArray &colArray = naTable->getNAColumnArray();
  const int *colIdxArray = naTable->getPartitionColIdxArray();

  // get column name
  ColRefName *colRefName =
      new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[partColPos_]]->getColName(), bindWA->wHeap());
  colName_ = new (bindWA->wHeap()) ColReference(colRefName);

  boundary_ = new (bindWA->wHeap()) BoundaryExpr();
  isMaxValue() = boundary->isMaxValue();
  // parser the string expression
  Parser parser(bindWA->currentCmpContext());
  if (boundary->lowValue && !boundary->isLowMaxValue()) {
    ItemExpr *lowExpr = parser.getItemExprTree(boundary->lowValue, strlen(boundary->lowValue),
                                               CharInfo::UTF8  // ComGetNameInterfaceCharSet()
    );
    if (lowExpr) {
      lowExpr = lowExpr->bindNode(bindWA);
      boundary_->lowBound_ = lowExpr->constFold();
    }
  }
  if (boundary->highValue && !boundary->isMaxValue()) {
    ItemExpr *highPred = parser.getItemExprTree(boundary->highValue, strlen(boundary->highValue),
                                                CharInfo::UTF8  // ComGetNameInterfaceCharSet()
    );
    if (highPred) {
      highPred = highPred->bindNode(bindWA);
      boundary_->highBound_ = highPred->constFold();
    }
  }
  // generate additional pred for the first partition column,
  // except for the first partition
  BiRelat *lowInterval = NULL;
  BiRelat *highInterval = NULL;
  BiRelat *highEqPred = NULL;
  BiRelat *lowBoundExpr = NULL;
  ItemExpr *interval = NULL;

  if (partIdx_ > 0 && boundary_->lowBound_ != NULL) {
    // generate low boundary predicate
    if (!nextBoundary || (nextBoundary && !nextBoundary->isLowMaxValue())) {
      lowBoundExpr = new (bindWA->wHeap()) BiRelat(ITM_EQUAL, colName_, boundary_->lowBound_);
      ((BiRelat *)lowBoundExpr)->setConvertNumToChar(TRUE);
      lowBoundaryExpr_ = lowBoundExpr;
    }
    // generate interval predicate
    if (partColPos_ == 0) {
      lowInterval = new (bindWA->wHeap()) BiRelat(ITM_GREATER, colName_, boundary_->lowBound_);
      ((BiRelat *)lowInterval)->setConvertNumToChar(TRUE);
    }
  }

  if (boundary_->highBound_ && !isMaxValue()) {
    // generate less predicate for all partition columns
    highInterval = new (bindWA->wHeap()) BiRelat(ITM_LESS, colName_, boundary_->highBound_);
    ((BiRelat *)highInterval)->setConvertNumToChar(TRUE);
  }

  if (lowInterval && highInterval)
    interval = new (bindWA->wHeap()) BiLogic(ITM_AND, lowInterval, highInterval);
  else if (lowInterval)
    interval = lowInterval;
  else
    interval = highInterval;

  intervalExpr_ = interval;

  if (lowBoundExpr && interval)
    interval = new (bindWA->wHeap()) BiLogic(ITM_OR, lowBoundExpr, interval);
  else if (lowBoundExpr)
    interval = lowBoundExpr;

  ValueIdSet intervalPred;
  if (interval) {
    interval->bindNode(bindWA);
    if ((interval->child(1)->getOperatorType() == ITM_CAST) &&
        interval->child(1)->doesExprEvaluateToConstant(FALSE, FALSE)) {
      ItemExpr *constFoldExpr = interval->child(1)->constFold();
      if (constFoldExpr) interval->setChild(1, constFoldExpr);
    }
    intervalPred.addMember(interval);
  }

  if (genRange && interval) {
    // generate range spec for interval predicate
    QRDescGenerator *descGen1 = new (bindWA->wHeap()) QRDescGenerator(false, bindWA->wHeap());
    descGen1->createEqualitySets(intervalPred);
    intervalRange_ = OptRangeSpec::createRangeSpec(descGen1, interval, bindWA->wHeap(), FALSE);
  }

  // for the maxvalue boundary, do not generate high boundary pred
  if (!isMaxValue()) {
    // generate high boundary predicate
    highEqPred = new (bindWA->wHeap()) BiRelat(ITM_EQUAL, colName_, boundary_->highBound_);
    highEqPred->setConvertNumToChar(TRUE);
    highEqPred->bindNode(bindWA);
    highBoundaryExpr_ = highEqPred;
  }
  // don't generate EQ predicate for the last partition column
  if (partColPos_ != naTable->getPartitionColCount() - 1 && naTable->getPartitionColCount() > 1 && highEqPred) {
    ValueIdSet highBoundaryPred;
    highBoundaryPred.addMember(highEqPred);

    if (genRange) {
      // generate range spec
      QRDescGenerator *descGen2 = new (bindWA->wHeap()) QRDescGenerator(false, bindWA->wHeap());
      descGen2->createEqualitySets(highBoundaryPred);
      highBoundRange_ = OptRangeSpec::createRangeSpec(descGen2, highEqPred, bindWA->wHeap(), FALSE);
    }
  }
}

void PartRange::print(FILE *ofd, const char *indent, const char *title, CollHeap *c, char *buf) const {
  BUMP_INDENT(indent);
  Space *space = (Space *)c;
  char mybuf[2000];

  const NAPartitionArray &partArray = table_->getPartitionArray();
  NAPartition *part = partArray[partIdx_];
  const char *partName = part->getPartitionName();

  snprintf(mybuf, sizeof(mybuf), "Parition Name: %s\n", partName);
  PRINTIT(ofd, c, space, buf, mybuf);

  for (int i = 0; i < entries(); i++) {
    at(i)->print(ofd, indent, title, c, buf);
  }
}

void PartRange::display() { print(); }

void Partition::print(FILE *ofd, const char *indent, const char *title, CollHeap *c, char *buf) const {
  BUMP_INDENT(indent);
  Space *space = (Space *)c;
  char mybuf[2000];

  snprintf(mybuf, sizeof(mybuf), "Parition Type: %s\n", partType_ == 1 ? "Range Patition" : "Other Partition");
  PRINTIT(ofd, c, space, buf, mybuf);

  snprintf(mybuf, sizeof(mybuf), "Parition Name: %s, Partition Entity Name: %s\n", partName_.data(),
           partEntityName_.data());
  PRINTIT(ofd, c, space, buf, mybuf);

  snprintf(mybuf, sizeof(mybuf), "Parition Index: %d, Partition Count: %d\n", partIdx_, partCount_);
  PRINTIT(ofd, c, space, buf, mybuf);

  snprintf(mybuf, sizeof(mybuf), "Is last partition : %s\n", isLastPart_ ? "TRUE" : "FALSE");
  PRINTIT(ofd, c, space, buf, mybuf);

  if (isLastPart_ || partIdx_ == partCount_) {
    snprintf(mybuf, sizeof(mybuf), "Is MAXVALUE partition : %s\n", firstMaxvalIdx_ > 0 ? "TRUE" : "FALSE");
    PRINTIT(ofd, c, space, buf, mybuf);
    snprintf(mybuf, sizeof(mybuf), "The first MAXVALUE column is : %d\n", firstMaxvalIdx_ + 1);
    PRINTIT(ofd, c, space, buf, mybuf);
  }

  if (partType_ == Partition::RANGE_PARTITION) {
    if (lowBoundary_) {
      NAString low;
      lowBoundary_->unparse(low, BINDER_PHASE);
      snprintf(mybuf, sizeof(mybuf), "Low Boundary : %s\n", low.data());
    } else
      snprintf(mybuf, sizeof(mybuf), "Low Boundary : %s\n", "NULL");
    PRINTIT(ofd, c, space, buf, mybuf);

    if (highBoundary_) {
      NAString high;
      highBoundary_->unparse(high, BINDER_PHASE);
      snprintf(mybuf, sizeof(mybuf), "High Boundary : %s\n", high.data());
    } else
      snprintf(mybuf, sizeof(mybuf), "High Boundary : %s\n", "NULL");
    PRINTIT(ofd, c, space, buf, mybuf);
  } else if (partType_ == 2 && listBoundary_) {
    if (listBoundary_) {
      NAString list;
      listBoundary_->unparse(list, BINDER_PHASE, EXPLAIN_FORMAT);
      snprintf(mybuf, sizeof(mybuf), "List Partition Boundary : %s\n", list.data());
    } else
      snprintf(mybuf, sizeof(mybuf), "List Partition Boundary : %s\n", "NULL");
    PRINTIT(ofd, c, space, buf, mybuf);
  }
}

void Partition::display() { print(); }

// -----------------------------------------------------------------------
// Constructor (but note that much more useful stuff goes on in
// static createTableDesc in BindRelExpr.C)
// -----------------------------------------------------------------------
TableDesc::TableDesc(BindWA *bindWA, const NATable *table, CorrName &corrName)
    : table_(table),
      indexes_(bindWA->wHeap()),
      uniqueIndexes_(bindWA->wHeap()),
      vertParts_(bindWA->wHeap()),
      hintIndexes_(bindWA->wHeap()),
      colStats_(bindWA->wHeap()),
      corrName_("", bindWA->wHeap()),
      analysis_(NULL),
      interestingExpressions_(hashKey, NAHashDictionary_Default_Size, TRUE /* enforce uniqueness */, bindWA->wHeap()),
      partitionList_(bindWA->wHeap()),
      partitions_(bindWA->wHeap()) {
  corrName.applyDefaults(bindWA, bindWA->getDefaultSchema());
  corrName_ = corrName;
  selectivityHint_ = NULL;
  cardinalityHint_ = NULL;
  histogramsCompressed_ = FALSE;
  minRC_ = csOne;
  maxRC_ = COSTSCALAR_MAX;
  isAInternalcheckConstraints_ = FALSE;

  // Fix up the name location list to help with the computing
  // of view text, check constraint search condition text, etc.
  //
  if (corrName.getSpecialType() == ExtendedQualName::TRIGTEMP_TABLE)  // -- Triggers
    return;

  ParNameLocList *pNameLocList = bindWA->getNameLocListPtr();
  if (pNameLocList) {
    ParNameLoc *pNameLoc = pNameLocList->getNameLocPtr(corrName_.getNamePosition());
    if (pNameLoc AND pNameLoc->getExpandedName(FALSE).isNull()) {
      pNameLoc->setExpandedName(corrName_.getQualifiedNameObj().getQualifiedNameAsAnsiString());
    }
  }
}

// -----------------------------------------------------------------------
// Add a CheckConstraint to the TableDesc.
//
// A table check constraint "CHECK (pred)" evaluates as
// "WHERE (pred) IS NOT FALSE" (i.e. TRUE or NULL);
// see ANSI 4.10 and in particular 11.21 GR 5+6.
//
// A view check constraint is the WHERE clause of a WITH CHECK OPTION view;
// it must evaluate to TRUE:
// "WHERE pred" (FALSE or NULL *fails*).
//
// Note that if the pred is "IS NOT NULL" (certainly a common CHECK pred),
// we can optimize the run-time a tiny bit by not having to use a BoolVal
// (because "IS NOT NULL" only returns TRUE or FALSE, never UNKNOWN/NULL).
//
// This method should be kept in synch with NAColumn::getNotNullViolationCode.
// -----------------------------------------------------------------------
void TableDesc::addCheckConstraint(BindWA *bindWA, const NATable *naTable, const CheckConstraint *constraint,
                                   ItemExpr *constrPred) {
  BoolVal *ok = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);

  RaiseError *error =
      new (bindWA->wHeap()) RaiseError(0, constraint->getConstraintName().getQualifiedNameAsAnsiString(),
                                       naTable->getTableName().getQualifiedNameAsAnsiString(), bindWA->wHeap());

  if (constraint->isViewWithCheckOption()) {
    if (constraint->isTheCascadingView())
      error->setSQLCODE(EXE_CHECK_OPTION_VIOLATION);  // cascadING
    else
      error->setSQLCODE(EXE_CHECK_OPTION_VIOLATION_CASCADED);  // cascadED
    constrPred = new (bindWA->wHeap()) IfThenElse(constrPred, ok, error);
  } else if (constrPred->isISNOTNULL()) {
    error->setSQLCODE(EXE_TABLE_CHECK_CONSTRAINT);
    constrPred = new (bindWA->wHeap()) IfThenElse(constrPred, ok, error);
  } else {
    constrPred = new (bindWA->wHeap()) UnLogic(ITM_IS_FALSE, constrPred);
    error->setSQLCODE(EXE_TABLE_CHECK_CONSTRAINT);
    constrPred = new (bindWA->wHeap()) IfThenElse(constrPred, error, ok);
  }

  // IfThenElse only works if Case is its parent.
  constrPred = new (bindWA->wHeap()) Case(NULL, constrPred);

  constrPred->bindNode(bindWA);
  CMPASSERT(!bindWA->errStatus());

  checkConstraints_.insert(constrPred->getValueId());
  // isAInternalcheckConstraints_ = TRUE;

}  // TableDesc::addCheckConstraint

ItemExpr *TableDesc::genPartv2BaseCheckConstraint(BindWA *bindWA) {
  if (partitionList_.entries() == 0) return NULL;

  int colCount = getNATable()->getPartitionColCount();
  const NAColumnArray &colArray = getNATable()->getNAColumnArray();
  const int *colIdxArray = getNATable()->getPartitionColIdxArray();

  ItemExpr *colList = NULL;

  // generate low expr
  ItemExpr *lowExpr = NULL;
  ItemExpr *firstPartCol = NULL;
  Partition *firstPart = partitionList_[0];
  if (firstPart->getPartIndex() == 0 && firstPart->getlowBoundary() != NULL) {
    for (int i = 0; i < colCount; i++) {
      ColRefName *colRefName =
          new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[i]]->getColName(), bindWA->wHeap());
      if (i == 0) {
        firstPartCol = new (bindWA->wHeap()) ColReference(colRefName);
        colList = firstPartCol;
      } else
        colList = new (bindWA->wHeap()) ItemList(colList, new (bindWA->wHeap()) ColReference(colRefName));
    }

    lowExpr = firstPart->getlowBoundary();
    CMPASSERT(lowExpr);
    lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, colList, lowExpr);
  }

  // generate high expr
  ItemExpr *highColList = NULL;
  Partition *lastPart = partitionList_[partitionList_.entries() - 1];
  if (lastPart->getFirstMaxvalIdx() >= 0) colCount = lastPart->getFirstMaxvalIdx();
  for (int i = 0; i < colCount; i++) {
    ColRefName *colRefName = new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[i]]->getColName(), bindWA->wHeap());
    if (i == 0)
      highColList = new (bindWA->wHeap()) ColReference(colRefName);
    else
      highColList = new (bindWA->wHeap()) ItemList(highColList, new (bindWA->wHeap()) ColReference(colRefName));
  }
  colList = highColList;

  ItemExpr *unboundExpr = NULL;
  ItemExpr *highExpr = NULL;
  NABoolean needIfElse = TRUE;
  if (colList == NULL && lastPart->isLastPart() && lastPart->getFirstMaxvalIdx() == 0) {
    if (lowExpr) {
      highExpr = new (bindWA->wHeap()) UnLogic(ITM_IS_NULL, firstPartCol);
      unboundExpr = new (bindWA->wHeap()) BiLogic(ITM_OR, lowExpr, highExpr);
    } else {
      unboundExpr = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);
      needIfElse = false;
    }
  } else {
    highExpr = lastPart->getHighBoundary();
    CMPASSERT(highExpr);
    if (lastPart->getFirstMaxvalIdx() > 0)
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, colList, highExpr);
    else
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, colList, highExpr);

    if (lowExpr)
      unboundExpr = new (bindWA->wHeap()) BiLogic(ITM_AND, lowExpr, highExpr);
    else
      unboundExpr = highExpr;
  }

  if (needIfElse) {
    BoolVal *retTrue = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);
    BoolVal *retFalse = new (bindWA->wHeap()) BoolVal(ITM_RETURN_FALSE);
    unboundExpr = new (bindWA->wHeap()) IfThenElse(unboundExpr, retTrue, retFalse);
    // IfThenElse only works if Case is its parent.
    unboundExpr = new (bindWA->wHeap()) Case(NULL, unboundExpr);
  }

  // unboundExpr should be bound at partition Insert node
  return unboundExpr;
}

void TableDesc::addPartnPredToConstraint(BindWA *bindWA) {
  Partition *part = partitionList_[0];
  BoolVal *retTrue = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);
  BoolVal *retFalse = new (bindWA->wHeap()) BoolVal(ITM_RETURN_FALSE);

  int colCount = getNATable()->getPartitionColCount();
  const NAColumnArray &colArray = getNATable()->getNAColumnArray();
  const int *colIdxArray = getNATable()->getPartitionColIdxArray();

  if (part->getPartIndex() == 0 && part->getFirstMaxvalIdx() == 0) return;

  int validColCount = part->getFirstLowMaxvalIdx() >= 0 ? part->getFirstLowMaxvalIdx() : colCount;
  ItemExpr *colList = NULL;
  ItemExpr *firstPartCol = NULL;
  for (int i = 0; i < validColCount; i++) {
    ColRefName *colRefName = new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[i]]->getColName(), bindWA->wHeap());
    if (i == 0) {
      firstPartCol = new (bindWA->wHeap()) ColReference(colRefName);
      colList = firstPartCol;
    } else
      colList = new (bindWA->wHeap()) ItemList(colList, new (bindWA->wHeap()) ColReference(colRefName));
  }

  ItemExpr *lowExpr = NULL;
  if (part->getPartIndex() > 0) {
    lowExpr = part->getlowBoundary();
    CMPASSERT(lowExpr);
    if (part->getFirstLowMaxvalIdx() > 0)
      lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER, colList, lowExpr);
    else
      lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, colList, lowExpr);
  }

  ItemExpr *highColList = NULL;
  if (part->getFirstMaxvalIdx() >= 0) colCount = part->getFirstMaxvalIdx();
  for (int i = 0; i < colCount; i++) {
    ColRefName *colRefName = new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[i]]->getColName(), bindWA->wHeap());
    if (i == 0)
      highColList = new (bindWA->wHeap()) ColReference(colRefName);
    else
      highColList = new (bindWA->wHeap()) ItemList(highColList, new (bindWA->wHeap()) ColReference(colRefName));
  }
  colList = highColList;

  ItemExpr *boundExpr = NULL;
  ItemExpr *highExpr = NULL;
  if (part->isLastPart() && part->getFirstMaxvalIdx() == 0) {
    if (lowExpr) {
      highExpr = new (bindWA->wHeap()) UnLogic(ITM_IS_NULL, firstPartCol);
      boundExpr = new (bindWA->wHeap()) BiLogic(ITM_OR, lowExpr, highExpr);
    } else
      return;
  } else {
    highExpr = part->getHighBoundary();
    CMPASSERT(highExpr);
    if (part->getFirstMaxvalIdx() > 0)
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, colList, highExpr);
    else
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, colList, highExpr);

    if (lowExpr)
      boundExpr = new (bindWA->wHeap()) BiLogic(ITM_AND, lowExpr, highExpr);
    else
      boundExpr = highExpr;
  }
  boundExpr = new (bindWA->wHeap()) IfThenElse(boundExpr, retTrue, retFalse);
  // IfThenElse only works if Case is its parent.
  ItemExpr *caseExpr = new (bindWA->wHeap()) Case(NULL, boundExpr);
  caseExpr->bindNode(bindWA);
  CMPASSERT(!bindWA->errStatus());
  checkConstraints_.insert(caseExpr->getValueId());
  isAInternalcheckConstraints_ = TRUE;
}

void TableDesc::addPartPredToPrecondition(BindWA *bindWA, ItemExpr *selectPred) {
  // if there is no selection predicate
  if (selectPred == NULL || selectPred->getArity() <= 1) return;

  Partition *part = partitionList_[0];
  // if partition table has only on MAXVALUE partition
  // and the first partition column is MAXVALUE
  if (part->getPartIndex() == 0 && part->getFirstMaxvalIdx() == 0) return;

  int colCount = getNATable()->getPartitionColCount();

  selectPred->bindNode(bindWA);
  ValueIdList selPredList;
  selectPred->convertToValueIdList(selPredList, NULL, ITM_AND, FALSE);

  const int *colIdxArray = getNATable()->getPartitionColIdxArray();
  ItemExpr *eqList = NULL;
  ValueIdList eqValueList;
  ItemExpr *lowVal = NULL;
  ItemExpr *highVal = NULL;
  for (int i = 0; i < colCount; i++) {
    int position = colIdxArray[i];
    QRDescGenerator *descGenerator = new (bindWA->wHeap()) QRDescGenerator(false, bindWA->wHeap());
    ValueIdSet colPredSet;
    for (int j = 0; j < selPredList.entries(); j++) {
      ValueId &pred = selPredList[j];
      ItemExpr *item = pred.getItemExpr();
      if (item == NULL || item->getArity() == 1) continue;
      const ItemExpr *child0 = item->child(0);
      const ItemExpr *child1 = item->child(1);
      OperatorTypeEnum operType0 = child0->getOperatorType();
      OperatorTypeEnum operType1 = child1->getOperatorType();
      ItemExpr *itemCol = NULL;
      ItemExpr *itemVal = NULL;
      if (operType0 != ITM_BASECOLUMN && operType1 != ITM_BASECOLUMN) continue;
      if (operType0 == ITM_BASECOLUMN &&
          (operType1 == ITM_CONSTANT || operType1 == ITM_DYN_PARAM || operType1 == ITM_CACHE_PARAM)) {
        const NAColumn *predCol = child0->getValueId().getNAColumn();
        if (predCol->getPosition() != position) continue;
        item->bindNode(bindWA);
        colPredSet.addMember(item);
      } else if (operType1 == ITM_BASECOLUMN &&
                 (operType0 == ITM_CONSTANT || operType0 == ITM_DYN_PARAM || operType0 == ITM_CACHE_PARAM)) {
        const NAColumn *predCol = child1->getValueId().getNAColumn();
        if (predCol->getPosition() != position) continue;
        item->bindNode(bindWA);
        colPredSet.addMember(item);
      }
    }
    if (colPredSet.entries() > 0) {
      descGenerator->createEqualitySets(colPredSet);
      OptRangeSpec *range =
          OptRangeSpec::createRangeSpec(descGenerator, colPredSet.rebuildExprTree(ITM_AND), bindWA->wHeap(), FALSE);
      ValueIdList vidList;
      if (range)
        range->getRangeItemExpr()->convertToValueIdList(vidList, bindWA, ITM_AND);
      else
        vidList.insertSet(colPredSet);

      for (int n = 0; n < vidList.entries(); n++) {
        ItemExpr *tmpExpr = vidList[n].getItemExpr();
        if (tmpExpr->getOperatorType() == ITM_EQUAL) {
          if (i == 0)
            eqList = tmpExpr->child(1);
          else
            eqList = new (bindWA->wHeap()) ItemList(eqList, tmpExpr->child(1));

          eqValueList.addMember(tmpExpr->child(1));
        } else if (tmpExpr->getOperatorType() == ITM_GREATER || tmpExpr->getOperatorType() == ITM_GREATER_EQ) {
          lowVal = tmpExpr;
        } else if (tmpExpr->getOperatorType() == ITM_LESS || tmpExpr->getOperatorType() == ITM_LESS_EQ) {
          highVal = tmpExpr;
        }
      }

      if (lowVal || highVal) break;
    } else
      break;
  }

  ValueIdList &low = part->getLowValueList();
  ValueIdList &high = part->getHighValueList();
  ItemExpr *lowExpr = NULL;
  ItemExpr *highExpr = NULL;
  ItemExpr *condExpr = NULL;
  int lowValCount = low.entries();
  int highValCount = high.entries();
  int eqValCount = eqValueList.entries();
  if (eqValCount) {
    if (lowValCount > 0) {
      if (lowValCount == eqValCount) {
        lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, eqList, part->getlowBoundary());
      } else {
        int cnt = lowValCount < eqValCount ? lowValCount : eqValCount;
        ItemExpr *lowList = low[0].getItemExpr();
        ItemExpr *valList = eqValueList[0].getItemExpr();
        for (int i = 1; i < cnt; i++) {
          lowList = new (bindWA->wHeap()) ItemList(lowList, low[i].getItemExpr());
          valList = new (bindWA->wHeap()) ItemList(valList, eqValueList[i].getItemExpr());
        }
        lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, valList, lowList);
      }
    }
    int maxvalIdx = part->getFirstMaxvalIdx();
    if (highValCount > 0 && maxvalIdx != 0) {
      if (highValCount == eqValCount) {
        if (getNATable()->getPartitionColCount() > 1)
          highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, eqList, part->getHighBoundary());
        else
          highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, eqList, part->getHighBoundary());
      } else {
        int cnt = highValCount < eqValCount ? highValCount : eqValCount;
        ItemExpr *highList = high[0].getItemExpr();
        ItemExpr *valList = eqValueList[0].getItemExpr();
        for (int i = 1; i < cnt; i++) {
          highList = new (bindWA->wHeap()) ItemList(highList, low[i].getItemExpr());
          valList = new (bindWA->wHeap()) ItemList(valList, eqValueList[i].getItemExpr());
        }
        if (getNATable()->getPartitionColCount() > 1)
          highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, valList, highList);
        else
          highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, valList, highList);
      }
    }
    if (!lowExpr && !highExpr)
      return;
    else if (lowExpr && highExpr)
      condExpr = new (bindWA->wHeap()) BiLogic(ITM_AND, lowExpr, highExpr);
    else if (lowExpr)
      condExpr = lowExpr;
    else
      condExpr = highExpr;
  } else if (lowVal || highVal) {
    ValueIdList &lowBoundList = part->getLowValueList();
    ValueIdList &highBoundList = part->getHighValueList();
    if ((!lowVal && lowBoundList.entries() == 0) || (!highVal && highBoundList.entries() == 0))
      return;
    else if (!lowVal || highBoundList.entries() == 0) {
      if (highVal->getOperatorType() == ITM_LESS_EQ)
        condExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, lowBoundList[0].getItemExpr(), highVal->child(1));
      else if (highVal->getOperatorType() == ITM_LESS)
        condExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, lowBoundList[0].getItemExpr(), highVal->child(1));
    } else if (!highVal || lowBoundList.entries() == 0) {
      condExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER, highBoundList[0].getItemExpr(), lowVal->child(1));
    } else if (lowVal && highVal && highBoundList.entries() > 0 && lowBoundList.entries()) {
      ItemExpr *tmpExpr0 = NULL;
      ItemExpr *tmpExpr1 = NULL;
      if (highVal->getOperatorType() == ITM_LESS_EQ)
        tmpExpr0 = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, lowBoundList[0].getItemExpr(), highVal->child(1));
      else if (highVal->getOperatorType() == ITM_LESS)
        tmpExpr0 = new (bindWA->wHeap()) BiRelat(ITM_LESS, lowBoundList[0].getItemExpr(), highVal->child(1));
      tmpExpr1 = new (bindWA->wHeap()) BiRelat(ITM_GREATER, highBoundList[0].getItemExpr(), lowVal->child(1));
      CMPASSERT(tmpExpr0 && tmpExpr1);
      condExpr = new (bindWA->wHeap()) BiLogic(ITM_AND, tmpExpr0, tmpExpr1);
    }
  }
  BoolVal *retTrue = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);
  BoolVal *retFalse = new (bindWA->wHeap()) BoolVal(ITM_RETURN_FALSE);

  if (condExpr) {
    condExpr = new (bindWA->wHeap()) IfThenElse(condExpr, retTrue, retFalse);
    condExpr = new (bindWA->wHeap()) Case(NULL, condExpr);
    condExpr->bindNode(bindWA);
    CMPASSERT(!bindWA->errStatus());
    // getPreconditions().insert(condExpr->getValueId());
    getPreconditions().addMember(condExpr);
  }
}

void TableDesc::addPartPredToConstraint(BindWA *bindWA) {
  PartRange *part = partitions_[0];
  BoolVal *retTrue = new (bindWA->wHeap()) BoolVal(ITM_RETURN_TRUE);
  BoolVal *retFalse = new (bindWA->wHeap()) BoolVal(ITM_RETURN_FALSE);
  int partColCount = part->entries();
  const int partIdx = part->getPartIndex();
  CMPASSERT(partIdx >= 0);
  PartRangePerCol *partCol0 = (*part)[0];

  // table has only one partition column
  if (1 == partColCount) {
    ItemExpr *intervalExpr = partCol0->getIntervalExpr();
    ItemExpr *lowExpr = partCol0->getLowBoundExpr();

    // table has only one MAXVALUE partition
    if (intervalExpr == NULL && partCol0->isMaxValue()) return;

    // the partition is the first partition
    if (partIdx > 0) {
      CMPASSERT(lowExpr);
      intervalExpr = new (bindWA->wHeap()) BiLogic(ITM_OR, intervalExpr, lowExpr);
    }
    intervalExpr = new (bindWA->wHeap()) IfThenElse(intervalExpr, retTrue, retFalse);
    partCol0->getConstrnExpr() = intervalExpr;
  } else {
    // table has tow or more partition columns
    for (int i = partColCount - 1; i >= 0; i--) {
      ItemExpr *tmpLowExpr = NULL;
      ItemExpr *tmpHighExpr = NULL;
      PartRangePerCol *partCol = (*part)[i];
      ItemExpr *lowExpr = partCol->getLowBoundExpr();
      ItemExpr *intervalExpr = partCol->getIntervalExpr();
      ItemExpr *highExpr = partCol->getHighBoundExpr();

      // handle the last column
      if (i == partColCount - 1) {
        ItemExpr *tmpExpr = NULL;
        if (partIdx > 0) {
          CMPASSERT(lowExpr != NULL);
          lowExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, lowExpr->child(0), lowExpr->child(1));
          lowExpr = new (bindWA->wHeap()) IfThenElse(lowExpr, retTrue, retFalse);
          lowExpr = new (bindWA->wHeap()) Case(NULL, lowExpr);
          partCol->getLowConstrnExpr() = lowExpr;
        } else if (partIdx == 0)
          partCol->getLowConstrnExpr() = retFalse;

        if (partCol->isMaxValue())
          partCol->getHighConstrnExpr() = retFalse;
        else {
          CMPASSERT(highExpr != NULL);
          highExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, highExpr->child(0), highExpr->child(1));
          highExpr = new (bindWA->wHeap()) IfThenElse(highExpr, retTrue, retFalse);
          highExpr = new (bindWA->wHeap()) Case(NULL, highExpr);
          partCol->getHighConstrnExpr() = highExpr;
        }
      } else {
        ItemExpr *tmpExpr = NULL;
        if (partIdx > 0) {
          CMPASSERT(lowExpr != NULL);
          ItemExpr *elseExpr = retFalse;
          // not the first partition column
          if (i != 0) {
            ItemExpr *lowIntervExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER, lowExpr->child(0), lowExpr->child(1));
            lowIntervExpr = new (bindWA->wHeap()) IfThenElse(lowIntervExpr, retTrue, retFalse);
            elseExpr = lowIntervExpr;
          }
          ItemExpr *lowEqPred = lowExpr;
          lowExpr = new (bindWA->wHeap()) IfThenElse(lowEqPred, ((*part)[i + 1])->getLowConstrnExpr(), elseExpr);
          lowExpr = new (bindWA->wHeap()) Case(NULL, lowExpr);
          partCol->getLowConstrnExpr() = lowExpr;
        } else if (partIdx == 0) {
          // the first partition has not low boundary
          partCol->getLowConstrnExpr() = retFalse;
        }

        if (partCol->isMaxValue())
          partCol->getHighConstrnExpr() = retFalse;
        else {
          CMPASSERT(highExpr != NULL);
          ItemExpr *elseExpr = retFalse;
          // not the first partition column
          if (i != 0) {
            ItemExpr *highIntervExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, highExpr->child(0), highExpr->child(1));
            highIntervExpr = new (bindWA->wHeap()) IfThenElse(highIntervExpr, retTrue, retFalse);
            elseExpr = highIntervExpr;
          }

          // if (cx == y) then (check next partition column)
          // else ( if (cx < y) then return true else return false)
          ItemExpr *highEqExpr = highExpr;
          highExpr = new (bindWA->wHeap()) IfThenElse(highEqExpr, ((*part)[i + 1])->getHighConstrnExpr(), elseExpr);
          // if ( i != 0 )
          highExpr = new (bindWA->wHeap()) Case(NULL, highExpr);
          partCol->getHighConstrnExpr() = highExpr;
        }
      }
    }
    ItemExpr *lowConstr = partCol0->getLowConstrnExpr();
    ItemExpr *constraint = partCol0->getIntervalExpr();
    ItemExpr *highConstr = partCol0->getHighConstrnExpr();
    CMPASSERT(lowConstr != NULL);
    CMPASSERT(constraint != NULL);
    CMPASSERT(highConstr != NULL);

    // constraint = new (bindWA->wHeap()) UnLogic(ITM_IS_TRUE, constraint);
    ItemExpr *tmpExpr = NULL;
    if (partIdx == 0)
      tmpExpr = highConstr;
    else if (partCol0->isMaxValue())
      tmpExpr = lowConstr;
    else
      tmpExpr = new (bindWA->wHeap()) IfThenElse(lowConstr, retTrue, highConstr);
    constraint = new (bindWA->wHeap()) IfThenElse(constraint, retTrue, tmpExpr);
    partCol0->getConstrnExpr() = constraint;
  }

  // IfThenElse only works if Case is its parent.
  ItemExpr *caseExpr = new (bindWA->wHeap()) Case(NULL, partCol0->getConstrnExpr());
  caseExpr->bindNode(bindWA);
  CMPASSERT(!bindWA->errStatus());
  checkConstraints_.insert(caseExpr->getValueId());
  isAInternalcheckConstraints_ = TRUE;
}

void TableDesc::addPartitions(BindWA *bindWA, const NATable *naTable, const NAPartitionArray &partList,
                              const NABoolean genRange)

{
  // process every partition
  for (int i = 0; i < partList.entries(); i++) {
    NAString partEntityName(partList[i]->getPartitionEntityName(), bindWA->wHeap());

    PartRange *partRange =
        new (bindWA->wHeap()) PartRange(bindWA->wHeap(), naTable, partList[i]->getPartPosition(), partEntityName);
    LIST(BoundaryValue *) &boundList = partList[i]->getBoundaryValueList();
    CMPASSERT(boundList.entries() == naTable->getPartitionColCount());

    for (int j = 0; j < boundList.entries(); j++) {
      PartRangePerCol *rangePerCol =
          new (bindWA->wHeap()) PartRangePerCol(bindWA->wHeap(), partList[i]->getPartPosition(), j);
      partRange->insert(rangePerCol);
      rangePerCol->createBoundForPartCol(bindWA, naTable, boundList[j],
                                         j + 1 < boundList.entries() ? boundList[j + 1] : NULL, genRange);
    }
    partitions_.insert(partRange);
  }
}

void TableDesc::genPartitions(BindWA *bindWA, const NAPartitionArray &partList)

{
  // process every partition
  for (int i = 0; i < partList.entries(); i++) {
    NAString partName(partList[i]->getPartitionName(), bindWA->wHeap());
    NAString partEntityName(partList[i]->getPartitionEntityName(), bindWA->wHeap());
    Partition *partition =
        new (bindWA->wHeap()) Partition(bindWA->wHeap(), Partition::RANGE_PARTITION, partList[i]->getPartPosition(),
                                        (int)partList.entries(), partName, partEntityName);
    partition->isSubPartition() = FALSE;
    LIST(BoundaryValue *) &boundList = partList[i]->getBoundaryValueList();
    const NATable *naTable = getNATable();
    CMPASSERT(boundList.entries() == naTable->getPartitionColCount());
    ItemExpr *lowBoundary = NULL;
    ItemExpr *highBoundary = NULL;
    ValueIdList &lowValueList = partition->getLowValueList();
    ValueIdList &highValueList = partition->getHighValueList();
    if (i == partList.entries() - 1) partition->isLastPart() = TRUE;

    NAColumnArray colAry = naTable->getNAColumnArray();
    const int *idxAry = naTable->getPartitionColIdxArray();
    int colCount = naTable->getPartitionColCount();
    CMPASSERT(colCount == boundList.entries());
    char buf[1000];
    for (int j = 0; j < boundList.entries(); j++) {
      // parser the string expression
      Parser parser(bindWA->currentCmpContext());
      if (boundList[j]->lowValue && partition->getFirstLowMaxvalIdx() < 0) {
        if (boundList[j]->isLowMaxValue()) {
          if (partition->getFirstLowMaxvalIdx() < 0) partition->getFirstLowMaxvalIdx() = j;
        } else {
          memset(buf, 0, sizeof(buf));
          sprintf(buf, "(%s)=(%s)", ToAnsiIdentifier(colAry[idxAry[j]]->getColName()).data(), boundList[j]->lowValue);
          ItemExpr *tmpExpr = parser.getItemExprTree(buf, strlen(buf), CharInfo::UTF8);
          CMPASSERT(tmpExpr);
          ((BiRelat *)tmpExpr)->setConvertNumToChar(TRUE);
          tmpExpr->bindNode(bindWA);

          ItemExpr *lowExpr = tmpExpr->child(1);
          if (lowExpr && lowExpr->doesExprEvaluateToConstant(TRUE, TRUE)) {
            lowExpr = lowExpr->bindNode(bindWA);
            ItemExpr *constFoldExpr = lowExpr->constFold();
            if (constFoldExpr) lowExpr = constFoldExpr;
          }
          if (j == 0)
            lowBoundary = lowExpr;
          else
            lowBoundary = new (bindWA->wHeap()) ItemList(lowBoundary, lowExpr);

          lowValueList.addMember(lowExpr);
        }
      }

      if (boundList[j]->highValue && partition->getFirstMaxvalIdx() < 0) {
        if (boundList[j]->isMaxValue()) {
          if (partition->getFirstMaxvalIdx() < 0) partition->getFirstMaxvalIdx() = j;
        } else {
          memset(buf, 0, sizeof(buf));
          sprintf(buf, "(%s)=(%s)", ToAnsiIdentifier(colAry[idxAry[j]]->getColName()).data(), boundList[j]->highValue);
          ItemExpr *tmpExpr = parser.getItemExprTree(buf, strlen(buf), CharInfo::UTF8);
          CMPASSERT(tmpExpr);
          ((BiRelat *)tmpExpr)->setConvertNumToChar(TRUE);
          tmpExpr->bindNode(bindWA);

          ItemExpr *highExpr = tmpExpr->child(1);
          if (highExpr && highExpr->doesExprEvaluateToConstant(TRUE, TRUE)) {
            highExpr = highExpr->bindNode(bindWA);
            ItemExpr *constFoldExpr = highExpr->constFold();
            if (constFoldExpr) highExpr = constFoldExpr;
          }
          if (j == 0)
            highBoundary = highExpr;
          else
            highBoundary = new (bindWA->wHeap()) ItemList(highBoundary, highExpr);

          highValueList.addMember(highExpr);
        }
      }
    }

    partition->getlowBoundary() = lowBoundary;
    partition->getHighBoundary() = highBoundary;
    partition->getLowValueList() = lowValueList;
    partition->getHighValueList() = highValueList;

    partitionList_.insert(partition);
  }
}

// TableDesc::isKeyIndex()
// Parameter is an secondary index on the table. Table checks to see
// if the keys of the secondary index is built using the primary key
// of the table. If it is return true otherwise false.
NABoolean TableDesc::isKeyIndex(const IndexDesc *idesc) const {
  ValueIdSet pKeyColumns = clusteringIndex_->getIndexKey();
  ValueIdSet indexColumns = idesc->getIndexKey();
  ValueIdSet basePKeys = pKeyColumns.convertToBaseIds();

  for (ValueId id = indexColumns.init(); indexColumns.next(id); indexColumns.advance(id)) {
    ValueId baseId = ((BaseColumn *)(((IndexColumn *)id.getItemExpr())->getDefinition().getItemExpr()))->getValueId();
    if (NOT basePKeys.contains(baseId)) {
      return FALSE;
    }
  }

  return TRUE;
}

// this method sets the primary key columns. It goes through all the columns
// of the table, and collects the columns which are marked as primary keys
void TableDesc::setPrimaryKeyColumns() {
  ValueIdSet primaryColumns;

  for (CollIndex j = 0; j < colList_.entries(); j++) {
    ValueId valId = colList_[j];

    NAColumn *column = valId.getNAColumn();

    if (column->isPrimaryKey()) {
      primaryColumns.insert(valId);
      // mark column as referenced for histogram, as we may need its histogram
      // during plan generation
      if ((column->isUserColumn() || column->isSaltColumn()) &&
          (column->getNATable()->getSpecialType() == ExtendedQualName::NORMAL_TABLE))
        column->setReferencedForMultiIntHist();
    }
  }

  primaryKeyColumns_ = primaryColumns;
}

// -----------------------------------------------------------------------------
// NABoolean TableDesc::isSpecialObj() returns TRUE if the table is an internal
// table such as HISTOGRM, HISTINTS, DESCRIBE, or an SMD, UMD, or an MVUMD table.
// One of its usage is during
// getTableColStatas, where we do not want the compiler to print no stats
// warning.
// -----------------------------------------------------------------------------
NABoolean TableDesc::isSpecialObj() {
  const NATable *naTable = getNATable();
  if (naTable->isUMDTable() || naTable->isSMDTable() || naTable->isMVUMDTable() || naTable->isTrigTempTable())
    return TRUE;

  const NAString &fileName = getCorrNameObj().getQualifiedNameObj().getObjectName();
  if ((fileName == "DESCRIBE__") ||  // for non_dml statements such as showddl etc.
      (fileName == "HISTOGRM") ||    // following are used during update stats
      (fileName == "HISTINTS"))
    return TRUE;
  else
    return FALSE;
}

// -----------------------------------------------------------------------
// TableDesc::getUserColumnList()
// -----------------------------------------------------------------------
void TableDesc::getUserColumnList(ValueIdList &columnList) const {
  for (CollIndex i = 0; i < colList_.entries(); i++) {
    ValueId valId = colList_[i];
    NAColumn *column = valId.getNAColumn();
    if (column->isUserColumn()) columnList.insert(valId);
  }
}

// -----------------------------------------------------------------------
// TableDesc::getSystemColumnList()
// -----------------------------------------------------------------------
void TableDesc::getSystemColumnList(ValueIdList &columnList) const {
  for (CollIndex i = 0; i < colList_.entries(); i++) {
    ValueId valId = colList_[i];
    NAColumn *column = valId.getNAColumn();
    if (column->isSystemColumn()) columnList.insert(valId);
  }
}

// -----------------------------------------------------------------------
// TableDesc::getAllExceptCCList()
// -----------------------------------------------------------------------
void TableDesc::getAllExceptCCList(ValueIdList &columnList) const {
  for (CollIndex i = 0; i < colList_.entries(); i++) {
    ValueId valId = colList_[i];
    NAColumn *column = valId.getNAColumn();
    if (!(column->isComputedColumn())) columnList.insert(valId);
  }
}

// -----------------------------------------------------------------------
// TableDesc::getCCList()
// -----------------------------------------------------------------------
void TableDesc::getCCList(ValueIdList &columnList) const {
  for (CollIndex i = 0; i < colList_.entries(); i++) {
    ValueId valId = colList_[i];
    NAColumn *column = valId.getNAColumn();
    if (column->isComputedColumn()) columnList.insert(valId);
  }
}

// -----------------------------------------------------------------------
// TableDesc::getIdentityColumn()
// -----------------------------------------------------------------------
void TableDesc::getIdentityColumn(ValueIdList &columnList) const {
  for (CollIndex i = 0; i < colList_.entries(); i++) {
    ValueId valId = colList_[i];
    NAColumn *column = valId.getNAColumn();
    if (column->isIdentityColumn()) {
      columnList.insert(valId);
      break;  // Break when you find the first,
      // as there can only be one Identity column per table.
    }
  }
}

NABoolean TableDesc::isIdentityColumnGeneratedAlways(NAString *value) const {
  // Determine if an IDENTITY column exists and
  // has the default class of GENERATED ALWAYS AS IDENTITY.
  // Do not return TRUE, if the table type is an INDEX_TABLE.

  NABoolean result = FALSE;

  for (CollIndex j = 0; j < colList_.entries(); j++) {
    ValueId valId = colList_[j];
    NAColumn *column = valId.getNAColumn();

    if (column->isIdentityColumnAlways()) {
      if (getNATable()->getObjectType() != COM_INDEX_OBJECT) {
        if (value != NULL) *value = column->getColName();
        result = TRUE;
      }
    }
  }

  return result;
}

NABoolean TableDesc::hasIdentityColumnInClusteringKey() const {
  ValueIdSet pKeyColumns = clusteringIndex_->getIndexKey();
  NAColumn *column = NULL;
  for (ValueId id = pKeyColumns.init(); pKeyColumns.next(id); pKeyColumns.advance(id)) {
    column = id.getNAColumn();
    if (column && column->isIdentityColumn()) return TRUE;
  }
  return FALSE;
}

// -----------------------------------------------------------------------
// Given a column list providing identifiers for columns of this table,
// this method returns a list of VEG expressions and/or base columns that
// show the equivalence of base columns with index columns.
// -----------------------------------------------------------------------
void TableDesc::getEquivVEGCols(const ValueIdList &columnList, ValueIdList &VEGColumnList) const {
  for (CollIndex i = 0; i < columnList.entries(); i++) VEGColumnList.insert(getEquivVEGCol(columnList[i]));
}

void TableDesc::getEquivVEGCols(const ValueIdSet &columnSet, ValueIdSet &VEGColumnSet) const {
  for (ValueId v = columnSet.init(); columnSet.next(v); columnSet.advance(v)) VEGColumnSet += getEquivVEGCol(v);
}

ValueId TableDesc::getEquivVEGCol(const ValueId &column) const {
  BaseColumn *bc = column.castToBaseColumn();

  CMPASSERT(bc->getTableDesc() == this);
  return getColumnVEGList()[bc->getColNumber()];
}

// -----------------------------------------------------------------------
// Statistics stuff
// -----------------------------------------------------------------------
const ColStatDescList &TableDesc::getTableColStats() {
  // HIST_NO_STATS_UEC can never be greater than HIST_NO_STATS_ROWCOUNT.
  // If the customer has done an illegal setting, ignore that, and set
  // it to maximum permissible value

  if (CURRSTMT_OPTDEFAULTS->defNoStatsUec() > CURRSTMT_OPTDEFAULTS->defNoStatsRowCount()) {
    CURRSTMT_OPTDEFAULTS->setNoStatsUec(CURRSTMT_OPTDEFAULTS->defNoStatsRowCount());
  }

  if (colStats_.entries() > 0) {
    if (!areHistsCompressed() && (CmpCommon::getDefault(COMP_BOOL_18) != DF_ON)) {
      // compress histograms based on query predicates
      compressHistogramsForCurrentQuery();
    }
    return colStats_;
  }

  // For each ColStat, create a ColStat descriptor.
  StatsList *stats = NULL;

  NABoolean useEstimatedStats = FALSE;
  NABoolean useHistStats = FALSE;
  ((NATable *)table_)
      ->statsToUse((TrafDesc *)((NATable *)table_)->getStoredStatsDesc(), useEstimatedStats, useHistStats);

  if (useEstimatedStats) {
    TrafTableStatsDesc *statsDesc = table_->getStoredStatsDesc()->tableStatsDesc();
    ((NATable *)table_)->setOriginalRowCount(statsDesc->rowcount);
    stats = ((NATable *)table_)->generateFakeStats();
  } else if (useHistStats) {
    stats = ((NATable *)table_)->getStatistics(TRUE);
  } else {
    stats = ((NATable *)table_)->getStatistics();
  }

  // if for some reason, no histograms were returned by update statistics
  // generate fake histograms for the table
  if (!stats || stats->entries() == 0) stats = ((NATable *)table_)->generateFakeStats();

  const NAColumnArray &columnList = ((NATable *)table_)->getNAColumnArray();
  const ValueIdList &tableColList = getColumnList();
  colStats_.insertByPosition(*stats, columnList, interestingExpressions_, tableColList);

  // done creating a ColStatDesc for each ColStats;
  // ==> now store the multi-column uec information
  MultiColumnUecList *groupUecs = new (CmpCommon::statementHeap()) MultiColumnUecList(*stats, getColumnList());
  CostScalar rowcount = (*stats)[0]->getRowcount();
  groupUecs->initializeMCUecForUniqueIndxes(*this, rowcount);

  colStats_.setUecList(groupUecs);

  if (CmpCommon::getDefault(USTAT_COLLECT_MC_SKEW_VALUES) == DF_ON) {
    MultiColumnSkewedValueLists *mcSkewedValueLists =
        new (CmpCommon::statementHeap()) MultiColumnSkewedValueLists(*stats, getColumnList());
    colStats_.setMCSkewedValueLists(mcSkewedValueLists);
  }

  // -------------------------------------------------------------
  // set the UpStatsNeeded flag
  CostScalar needStatsRowcount = CostPrimitives::getBasicCostFactor(HIST_ROWCOUNT_REQUIRING_STATS);
  if (CURRSTMT_OPTDEFAULTS->ustatAutomation()) needStatsRowcount = 1;

  CMPASSERT(colStats_.entries() > 0);  // must have at least one histogram!
  CostScalar tableRowcount = colStats_[0]->getColStats()->getRowcount();

  if ((tableRowcount >= needStatsRowcount) &&
      !(isSpecialObj()))  // UpStatsNeeded flag is used for 6007 warning,
                          // we do not want to give this warning for
                          // smdTables, umdTables, MVUmd tables and other special tables
  {
    for (CollIndex i = 0; i < colStats_.entries(); i++) colStats_[i]->getColStatsToModify()->setUpStatsNeeded(TRUE);
  }
  // -------------------------------------------------------------

  // ENFORCE ROWCOUNT!  When we read them in, all stats from the same
  // table should report the same rowcount.  Currently there's a
  // problem with SYSKEY histograms having a default rowcount value --
  // which brings up the following question: are SYSKEY histograms
  // ever used in histogram synthesis?
  NABoolean printNoStatsWarning;
  if (isSpecialObj())
    printNoStatsWarning = FALSE;
  else
    printNoStatsWarning = TRUE;
  colStats_.enforceInternalConsistency(0, colStats_.entries(), printNoStatsWarning);

  /*  Estimate index levels based on row count, row size and key size of the index.
  Estimation will be based upon the following assumptions:

  a. There is no slack in data blocks. Slack is  the percentage in the  data block
  that is  kept empty for future inserts into  the block. For  sequential inserts
  DP2  does not leave  any slack in data blocks  but inserts in between  rows
  causes DP2 to  move following rows in the block into a new block.

  b. DP2 encodes and optimizes key storage. Basically it will keeps the portion of
  the key that is  necessary to distinguish the  blocks in next level  of index or
  data blocks. We will assume that the whole key is being used.

  c.  Data is  uniformly distributed  among all  the partitions.  Obviously it  is
  possible that one the partitions has more data thus more data blocks leading  to
  more index level than the other ones.
  */

  // compress histograms based on query predicates
  if (CmpCommon::getDefault(COMP_BOOL_18) != DF_ON) compressHistogramsForCurrentQuery();

  return colStats_;
}

// accumulate a list of interesting expressions that might have an expression histogram
void TableDesc::addInterestingExpression(NAString &unparsedExpr, const ValueIdSet &baseColRefs) {
  if (!interestingExpressions_.contains(&unparsedExpr)) {
    NAString *unparsedExprCopy = new (STMTHEAP) NAString(unparsedExpr, STMTHEAP);
    ValueIdSet *baseColRefsCopy = new (STMTHEAP) ValueIdSet(baseColRefs);
    interestingExpressions_.insert(unparsedExprCopy, baseColRefsCopy);

    // Add it to the NATable object too. Note: like method getTableColStats() above,
    // we shamelessly cast away constness here on the const NATable * table_ member.
    ((NATable *)table_)->addInterestingExpression(unparsedExpr);
  }
}

ValueIdSet TableDesc::getLocalPreds() {
  ValueIdSet localPreds;
  localPreds.clear();

  // We can get this information from TableAnalysis
  const TableAnalysis *tableAnalysis = getTableAnalysis();

  // if no tableAnalysis exists, return FALSE
  if (tableAnalysis) localPreds = tableAnalysis->getLocalPreds();

  return localPreds;
}

// Is there any column which has a local predicates and no stats
NABoolean TableDesc::isAnyHistWithPredsFakeOrSmallSample(const ValueIdSet &localPreds) {
  // if there are no local predicates return FALSE;
  if (localPreds.isEmpty()) return FALSE;

  const ColStatDescList &colStatsList = getTableColStats();
  // for each predicate, check to see if stats exist
  for (ValueId id = localPreds.init(); localPreds.next(id); localPreds.advance(id)) {
    ColStatsSharedPtr colStats = colStatsList.getColStatsPtrForPredicate(id);

    if (colStats == NULL) return FALSE;

    if (colStats->isOrigFakeHist() || colStats->isSmallSampleHistogram()) return TRUE;
  }

  return FALSE;
}

LIST(NAString) * TableDesc::getMatchedPartions(BindWA *bindWA, ItemList *values) {
  ItemExpr *valExpr = values;
  ItemExpr *tmpExpr = values;
  int colCount = getNATable()->getPartitionColCount();
  int valCount = 0;
  int nullPos = -1;

  if (values && values->getOperatorType() == ITM_CONSTANT) {
    valCount = 1;
    NABoolean negate;
    ConstValue *c1 = values->castToConstValue(negate);
    if (c1 && c1->isNull()) nullPos = 0;
  } else if (values && values->getOperatorType() == ITM_ITEM_LIST) {
    ValueIdList tmpList;
    values->convertToValueIdList(tmpList, bindWA, ITM_ITEM_LIST);
    valCount = tmpList.entries();
    for (int i = 0; i < tmpList.entries(); i++) {
      ItemExpr *e = tmpList[i].getItemExpr();
      NABoolean negate;
      ConstValue *c2 = e->castToConstValue(negate);
      if (c2 && c2->isNull()) {
        nullPos = i;
        break;
      }
    }
  } else if (values && values->getOperatorType() >= ITM_PLUS && values->getOperatorType() <= ITM_DIVIDE) {
    valCount = 1;
  }

  if (valCount != colCount) {
    *CmpCommon::diags() << DgSqlCode(-2300);
    bindWA->setErrStatus();
    return NULL;
  }
  LIST(NAString) *partNames = new (bindWA->wHeap()) LIST(NAString)(bindWA->wHeap());

  const LIST(Partition *) &partitions = getPartitionList();
  const int partCount = partitions.entries();
  int nullIndex = nullPos;

  for (int j = 0; j < partCount; j++) {
    ItemExpr *lowExpr = NULL;
    ItemExpr *highExpr = NULL;
    NABoolean lowMatched = FALSE;
    NABoolean highMatched = FALSE;
    // this is the first partition
    if (j != 0) {
      lowExpr = partitions[j]->getlowBoundary();
      CMPASSERT(lowExpr);
      if (nullPos > 0) {
        ItemExprList vl(valExpr, bindWA->wHeap());
        ItemExprList ll(lowExpr, bindWA->wHeap());
        for (int a = 0; a < nullIndex; a++) {
          if (a == 0) {
            valExpr = vl[a];
            lowExpr = ll[a];
          } else {
            valExpr = new (bindWA->wHeap()) ItemList(valExpr, vl[a]);
            lowExpr = new (bindWA->wHeap()) ItemList(lowExpr, ll[a]);
          }
        }
        CMPASSERT(valExpr);
        tmpExpr = valExpr;
      }

      if (partitions[j]->getFirstLowMaxvalIdx() > 0)
        lowExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS, lowExpr, tmpExpr);
      else
        lowExpr = new (bindWA->wHeap()) BiRelat(ITM_LESS_EQ, lowExpr, valExpr);

      if (nullPos != 0 && lowExpr && lowExpr->doesExprEvaluateToConstant(TRUE, TRUE)) {
        ((BiRelat *)lowExpr)->setConvertNumToChar(TRUE);
        lowExpr = lowExpr->bindNode(bindWA);
        ItemExpr *constFoldExpr = lowExpr->constFold();
        NABoolean negate;
        ConstValue *cv = constFoldExpr->castToConstValue(negate);
        if (constFoldExpr && cv) {
          // can get ConstValue
          if (cv && cv->getType()->getTypeQualifier() == NA_BOOLEAN_TYPE &&
              *(reinterpret_cast<int *>(cv->getConstValue())) == -1) {
            *CmpCommon::diags() << DgSqlCode(-2304);
            bindWA->setErrStatus();
            return NULL;
          } else if (cv && !cv->isAFalseConstant())
            lowMatched = TRUE;
        } else {
          *CmpCommon::diags() << DgSqlCode(-2304);
          bindWA->setErrStatus();
          return NULL;
        }
      }
      // first value is null like for(null,xx,xx)
      // default lowMatched true because null
      // record must stored at maxvalue partition
      // so low boundary should matched.
      else if (nullPos == 0)
        lowMatched = TRUE;
    } else
      lowMatched = TRUE;

    int index = partitions[j]->getFirstMaxvalIdx();
    // the MAXVALUE partition, the first column's value is MAXVALUE
    if (index == 0) {
      if (lowMatched && partitions[j]->getFirstMaxvalIdx() == 0) partNames->insert(partitions[j]->getPartEntityName());
      return partNames;
    }

    // before regenerate should inti valExpr to values;
    valExpr = values;

    // the MAXVALUE partition, the first column's value is not MAXVALUE
    // index > 0, regenerate the valExpr
    if (index > 0) {
      ItemExprList valList(valExpr, bindWA->wHeap());
      for (int n = 0; n < index; n++) {
        if (n == 0)
          tmpExpr = valList[n];
        else
          tmpExpr = new (bindWA->wHeap()) ItemList(tmpExpr, valList[n]);
      }
      CMPASSERT(tmpExpr);
    }

    highExpr = partitions[j]->getHighBoundary();
    CMPASSERT(highExpr);

    // index < nullPos than use maxvalue index
    // eg : partition definition pmax (xx,maxvalue,maxvalue)
    // for predicate is for(xx,xx,null)
    if (index > 0 && index < nullPos)
      nullIndex = index;
    else
      nullIndex = nullPos;

    if (nullPos > 0) {
      ItemExprList vl(valExpr, bindWA->wHeap());
      ItemExprList hl(highExpr, bindWA->wHeap());
      for (int a = 0; a < nullIndex; a++) {
        if (a == 0) {
          valExpr = vl[a];
          highExpr = hl[a];
        } else {
          valExpr = new (bindWA->wHeap()) ItemList(valExpr, vl[a]);
          highExpr = new (bindWA->wHeap()) ItemList(highExpr, hl[a]);
        }
      }
      CMPASSERT(valExpr);
      tmpExpr = valExpr;
    }

    if (index > 0)
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER_EQ, highExpr, tmpExpr);
    else
      highExpr = new (bindWA->wHeap()) BiRelat(ITM_GREATER, highExpr, valExpr);

    if (nullPos != 0 && highExpr && highExpr->doesExprEvaluateToConstant(TRUE, TRUE)) {
      ((BiRelat *)highExpr)->setConvertNumToChar(TRUE);
      highExpr = highExpr->bindNode(bindWA);
      ItemExpr *constFoldExpr = highExpr->constFold();
      NABoolean negate;
      ConstValue *cv = constFoldExpr->castToConstValue(negate);
      if (constFoldExpr && cv) {
        // can get ConstValue
        if (cv && cv->getType()->getTypeQualifier() == NA_BOOLEAN_TYPE &&
            *(reinterpret_cast<int *>(cv->getConstValue())) == -1) {
          *CmpCommon::diags() << DgSqlCode(-2304);
          bindWA->setErrStatus();
          return NULL;
        } else if (cv && !cv->isAFalseConstant())
          highMatched = TRUE;
      } else {
        *CmpCommon::diags() << DgSqlCode(-2304);
        bindWA->setErrStatus();
        return NULL;
      }
    }
    // first value is null like this. for(null,xx,xx)
    // default highMatched false because null
    // record must stored at maxvalue partition
    // so high boudnary should not matched.
    else if (nullPos == 0)
      highMatched = FALSE;

    if (lowMatched && highMatched) {
      partNames->insert(partitions[j]->getPartEntityName());
      return partNames;
    }
  }
  return partNames;
}

LIST(NAString) * TableDesc::getMatchedPartInfo(BindWA *bindWA, NAString partName) {
  // TODO:check if the partName is valid

  LIST(NAString) *partNames = new (bindWA->wHeap()) LIST(NAString)(bindWA->wHeap());
  const NAPartitionArray &partArray = table_->getPartitionArray();

  for (int i = 0; i < partArray.entries(); i++) {
    NAPartition *partition = partArray[i];
    const char *partitionName = partition->getPartitionName();

    if (strcmp(partName.data(), partitionName) == 0) {
      NAString entityName(partition->getPartitionEntityName(), bindWA->wHeap());
      partNames->insert(entityName);
    }
  }
  return partNames;
}

LIST(NAString) * TableDesc::getMatchedPartInfo(BindWA *bindWA, ItemExprList valList) {
  LIST(NAString) *partNames = new (bindWA->wHeap()) LIST(NAString)(bindWA->wHeap());
  int colCount = getNATable()->getPartitionColCount();
  if (valList.entries() != colCount) {
    *CmpCommon::diags() << DgSqlCode(-2300);
    bindWA->setErrStatus();
    return NULL;
  }

  const LIST(PartRange *) &partRanges = getPartitions();
  const int partCount = partRanges.entries();
  // The table has only one partition and the first partition
  // column's bounday is MAXVALUE, that means no limitation
  if (partCount == 1 && (*partRanges[0])[0]->isMaxValue()) {
    partNames->insert(partRanges[0]->getPartEntityName());
    return partNames;
  }

  const NAColumnArray &colArray = getNATable()->getNAColumnArray();
  const int *colIdxArray = getNATable()->getPartitionColIdxArray();
  LIST(OptRangeSpec *) *rangeList = new (bindWA->wHeap()) LIST(OptRangeSpec *)(bindWA->wHeap());
  for (int i = 0; i < colCount; i++) {
    ColRefName *colRefName = new (bindWA->wHeap()) ColRefName(colArray[colIdxArray[i]]->getColName(), bindWA->wHeap());
    ItemExpr *valExpr = valList[i];
    valExpr = valExpr->bindNode(bindWA);
    if (valExpr->getOperatorType() != ITM_CONSTANT) valExpr = valExpr->constFold();
    BiRelat *pred = new (bindWA->wHeap()) BiRelat(ITM_EQUAL, new (bindWA->wHeap()) ColReference(colRefName), valExpr);
    QRDescGenerator *descGenerator = new (bindWA->wHeap()) QRDescGenerator(false, bindWA->wHeap());

    ValueIdSet selPredSet;
    pred->bindNode(bindWA);
    if ((pred->child(1)->getOperatorType() == ITM_CAST) && pred->child(1)->doesExprEvaluateToConstant(FALSE, FALSE)) {
      ItemExpr *constFoldExpr = pred->child(1)->constFold();
      if (constFoldExpr) pred->setChild(1, constFoldExpr);
    }
    selPredSet.addMember(pred);
    descGenerator->createEqualitySets(selPredSet);
    OptRangeSpec *range = OptRangeSpec::createRangeSpec(descGenerator, pred, bindWA->wHeap(), FALSE);
    CMPASSERT(range);
    rangeList->insert(range);
  }
  if (rangeList->entries() == 0) return NULL;

  for (int i = 0; i < partCount; i++) {
    PartRange *partRange = partRanges[i];
    if (partRange->entries() != rangeList->entries()) {
      return NULL;
    }
    OptRangeSpec *selRange = NULL;
    for (int j = 0; j < partRange->entries(); j++) {
      PartRangePerCol *partRangePerCol = (*partRange)[j];
      if (partRangePerCol->isMaxValue()) {
        partNames->insert(partRange->getPartEntityName());
        return partNames;
      }

      selRange = ((*rangeList)[j])->clone(bindWA->wHeap());

      CMPASSERT(partRangePerCol->getInterval());
      selRange->intersectRange(partRangePerCol->getInterval());
      if (selRange->getSubranges().entries() > 0) {
        partNames->insert(partRange->getPartEntityName());
        return partNames;
      }
      if (j == partRange->entries() - 1) break;

      selRange = ((*rangeList)[j])->clone(bindWA->wHeap());
      CMPASSERT(partRangePerCol->getHighBoundary());
      selRange->intersectRange(partRangePerCol->getHighBoundary());
      if (selRange->getSubranges().entries() > 0) {
        // check the next column's boundary
        continue;
      } else {
        // the current partition does mismatch
        break;
      }
    }
  }

  return partNames;
}

LIST(NAString) * TableDesc::getMatchedPartInfo(CollHeap *heap) {
  LIST(NAString) *partNames = new (heap) LIST(NAString)(heap);
  const NAPartitionArray &partArray = table_->getPartitionArray();

  for (int i = 0; i < partArray.entries(); i++) {
    NAPartition *partition = partArray[i];
    const char *partitionName = partition->getPartitionName();
    NAString entityName(partition->getPartitionEntityName(), heap);
    partNames->insert(entityName);
  }
  return partNames;
}

LIST(NAString) * TableDesc::getMatchedPartInfo(BindWA *bindWA, ItemExpr *selectPred) {
  LIST(NAString) *partNames = new (bindWA->wHeap()) LIST(NAString)(bindWA->wHeap());
  int colCount = getNATable()->getPartitionColCount();

  const LIST(PartRange *) &partRanges = getPartitions();
  const int partCount = partRanges.entries();
  // The table has only one partition and the first partition column's
  // bounday is MAXVALUE, that means no limitation
  if (partCount == 1 && (*partRanges[0])[0]->isMaxValue()) {
    partNames->insert(partRanges[0]->getPartEntityName());
    return partNames;
  }

  if (selectPred == NULL || selectPred->getArity() <= 1) {
    for (int i = 0; i < partCount; i++) partNames->insert(partRanges[i]->getPartEntityName());
    return partNames;
  }

  selectPred->bindNode(bindWA);
  if (bindWA->errStatus()) return partNames;

  ValueIdList selPredList;

  selectPred->convertToValueIdList(selPredList, NULL, ITM_AND, FALSE);

  // const NAColumnArray &colArray = getNATable()->getNAColumnArray();
  const int *colIdxArray = getNATable()->getPartitionColIdxArray();
  LIST(OptRangeSpec *) *rangeList = new (bindWA->wHeap()) LIST(OptRangeSpec *)(bindWA->wHeap());
  ValueIdList tmpSelPredList;
  for (int m = 0; m < selPredList.entries(); m++) {
    ValueId &pred = selPredList[m];
    if (pred.getItemExpr()->getOperatorType() == ITM_EQUAL || pred.getItemExpr()->getOperatorType() == ITM_LESS ||
        pred.getItemExpr()->getOperatorType() == ITM_LESS_EQ || pred.getItemExpr()->getOperatorType() == ITM_GREATER ||
        pred.getItemExpr()->getOperatorType() == ITM_GREATER_EQ)
      tmpSelPredList.insert(pred);
  }
  if (tmpSelPredList.entries() > 0) {
    for (int i = 0; i < colCount; i++) {
      int position = colIdxArray[i];
      QRDescGenerator *descGenerator = new (bindWA->wHeap()) QRDescGenerator(false, bindWA->wHeap());
      ValueIdSet colPredSet;
      for (int j = 0; j < tmpSelPredList.entries(); j++) {
        ValueId &pred = tmpSelPredList[j];
        ItemExpr *item = pred.getItemExpr();
        ValueId child0 = item->child(0);
        ValueId child1 = item->child(1);
        ItemExpr *itemCol = NULL;
        ItemExpr *itemVal = NULL;
        if (child0.getItemExpr()->getOperatorType() != ITM_BASECOLUMN &&
            child1.getItemExpr()->getOperatorType() != ITM_BASECOLUMN)
          continue;
        if (child0.getItemExpr()->getOperatorType() == ITM_BASECOLUMN) {
          itemCol = child0.getItemExpr();
          const NAColumn *predCol = child0.getNAColumn();

          if (predCol->getPosition() != position) continue;
          if (child1.getItemExpr()->getOperatorType() == ITM_CONSTANT)
            colPredSet.addMember(item);
          else {
          }
          // int entries = colPredSet.entries();
        } else if (child1.getItemExpr()->getOperatorType() == ITM_BASECOLUMN) {
          itemCol = child1.getItemExpr();
          const NAColumn *predCol = child1.getNAColumn();

          if (predCol->getPosition() != position) continue;
          if (child0.getItemExpr()->getOperatorType() == ITM_CONSTANT)
            colPredSet.addMember(item);
          else {
          }
        }
      }
      if (colPredSet.entries() == 0) break;

      descGenerator->createEqualitySets(colPredSet);
      ItemExpr *colPredTmp = colPredSet.rebuildExprTree(ITM_AND, TRUE, TRUE);
      OptRangeSpec *range = OptRangeSpec::createRangeSpec(descGenerator, colPredTmp->child(0), bindWA->wHeap(), FALSE);
      if (range == NULL) break;
      rangeList->insert(range);
      int rangeEntries = rangeList->entries();
    }
  }
  if (rangeList->entries() > 0) {
    for (int i = 0; i < partCount; i++) {
      PartRange *partRange = partRanges[i];
      OptRangeSpec *selRange = NULL;
      // int colCount = rangeList->entries();
      for (int j = 0; j < rangeList->entries(); j++) {
        PartRangePerCol *partRangePerCol = (*partRange)[j];
        if (partRangePerCol->isMaxValue() && j != 0) {
          partNames->insert(partRange->getPartEntityName());
          break;
        }

        selRange = ((*rangeList)[j])->clone(bindWA->wHeap());
        selRange->intersectRange(partRangePerCol->getInterval());
        if (selRange->getSubranges().entries() > 0) {
          partNames->insert(partRange->getPartEntityName());
          break;
        }

        if (j != colCount - 1) {
          if (!partRangePerCol->getHighBoundary()) break;

          selRange = ((*rangeList)[j])->clone(bindWA->wHeap());
          selRange->intersectRange(partRangePerCol->getHighBoundary());
          if (selRange->getSubranges().entries() > 0 && j == rangeList->entries() - 1) {
            partNames->insert(partRange->getPartEntityName());
            break;
          } else if (selRange->getSubranges().entries() > 0) {
            // check the next column's boundary
            continue;
          } else {
            // the current partition does mismatch
            break;
          }
        }
      }
    }
  } else if (rangeList->entries() == 0) {
    const LIST(PartRange *) &partRanges = getPartitions();
    for (int i = 0; i < partRanges.entries(); i++) {
      PartRange *partRange = partRanges[i];
      partNames->insert(partRange->getPartEntityName());
    }
  }
  return partNames;
}

// This method computes the base selectivity for this table. It is defined
// cardinality after applying all local predicates with empty input logical
// properties over the base cardinality without hints

CostScalar Scan::computeBaseSelectivity() const {
  CostScalar scanCardWithoutHint =
      getGroupAttr()->outputLogProp((*GLOBAL_EMPTY_INPUT_LOGPROP))->getColStats().getScanRowCountWithoutHint();

  double cardAfterLocalPreds = scanCardWithoutHint.getValue();
  double baseRowCount = getTableDesc()->tableColStats()[0]->getColStats()->getRowcount().getValue();

  // both the minimum and the base row count have to be minimum 1.
  // This is ensured in the called routines. So no need to check here.

  return cardAfterLocalPreds / baseRowCount;
}

// This method computes the ratio of selectivity obtained with and without hint
// and sets that in the cardinalityHint

void TableDesc::setBaseSelectivityHintForScan(CardinalityHint *cardHint, CostScalar baseSelectivity) {
  double cardinalityHint = cardHint->getScanCardinality().getValue();
  CostScalar baseRowCount = tableColStats()[0]->getColStats()->getRowcount().getValue();
  double selectivityProportion;

  // cardinalityHint is minimum one. This is checked at the time of setting
  // it in the tableDesc. So, is the baseRowCount

  double selectivityFactor = cardinalityHint / baseRowCount.getValue();
  // selectivityProportion becomes invalid if selectivityFactor is
  // less than baseSelectivity. That causes an assertion failure
  // This was never caught earlier, maybe becasue we never tested hints with OR
  // predicates.
  selectivityProportion = log(selectivityFactor) / log(baseSelectivity.getValue());
  cardHint->setBaseScanSelectivityFactor(selectivityProportion);
  cardHint->setScanSelectivity(selectivityFactor);
  setCardinalityHint(cardHint);
  return;
}

// This method computes the ratio of selectivity obtained with and without hint
// and sets that in the selectivityHint

void TableDesc::setBaseSelectivityHintForScan(SelectivityHint *selHint, CostScalar baseSelectivity) {
  double selectivityFactor = selHint->getScanSelectivityFactor();

  double selectivityProportion;
  if (selectivityFactor == 0)
    selectivityProportion = 1.0;
  else
    selectivityProportion = log(selectivityFactor) / log(baseSelectivity.getValue());

  selHint->setBaseScanSelectivityFactor(selectivityProportion);
  setSelectivityHint(selHint);
  return;
}

// -----------------------------------------------------------------------
// Print function for TableDesc
// -----------------------------------------------------------------------
void TableDesc::print(FILE *ofd, const char *indent, const char *title) {
#ifndef NDEBUG
  BUMP_INDENT(indent);
  cout << title << " " << this << " NATable=" << (void *)table_ << " ix=" << indexes_.entries() << ","
       << clusteringIndex_ << " stat=" << colStats_.entries() << endl;
  for (CollIndex i = 0; i < indexes_.entries(); i++) indexes_[i]->print(ofd, indent, "TableDesc::indexes");
  clusteringIndex_->print(ofd, indent, "TableDesc::clusteringIndex_");
  corrName_.print(ofd, indent);
  colList_.print(ofd, indent, "TableDesc::colList_");
  colVEGList_.print(ofd, indent, "TableDesc::colVEGList_");
  cout << "cnstrnt=" << checkConstraints_.entries() << endl << endl;

#endif
}  // TableDesc::print()

// -----------------------------------------------------------------------
// Print function for TableDescList
// -----------------------------------------------------------------------
void TableDescList::print(FILE *ofd, const char *indent, const char *title) {
#ifndef NDEBUG
  BUMP_INDENT(indent);

  for (CollIndex i = 0; i < entries(); i++) {
    fprintf(ofd, "%s%s[%2d] = (%p)\n", NEW_INDENT, title, i, at(i));
    // at(i)->print(ofd,indent);
  }
#endif
}  // TableDescList::print()
CardinalityHint::CardinalityHint(CostScalar scanCardinality) {
  scanCardinality_ = scanCardinality;
  scanSelectivity_ = -1.0;
  localPreds_.clear();
  baseSelectivity_ = -1.0;
}

// constructor defined with local predicates
CardinalityHint::CardinalityHint(CostScalar scanCardinality, const ValueIdSet &localPreds) {
  scanCardinality_ = scanCardinality;
  scanSelectivity_ = -1.0;
  localPreds_ = localPreds;
  baseSelectivity_ = -1.0;
}
SelectivityHint::SelectivityHint(double selectivityFactor) {
  selectivityFactor_ = selectivityFactor;
  localPreds_.clear();
  baseSelectivity_ = -1.0;
}

void SelectivityHint::setScanSelectivityFactor(double selectivityFactor) {
  // This method is called only for selectivityFactor >= 0.0

  if (selectivityFactor > 1.0)
    selectivityFactor_ = 1.0;
  else
    selectivityFactor_ = selectivityFactor;
}

CostScalar TableDesc::getBaseRowCntIfUniqueJoinCol(const ValueIdSet &joinedCols)

{
  // get the joining columns for this table
  ValueIdList userColumns;

  // get All user columns for this table;
  getUserColumnList(userColumns);
  ValueIdSet userColumnSet(userColumns);

  ValueIdSet joinedColsCopy(joinedCols);

  ValueIdSet thisTableJoinCols = joinedColsCopy.intersect(userColumnSet);

  if (thisTableJoinCols.isEmpty()) return csMinusOne;

  CostScalar baseRowCount = csMinusOne;

  if (thisTableJoinCols.doColumnsConstituteUniqueIndex(this))
    baseRowCount = tableColStats()[0]->getColStats()->getRowcount();

  return baseRowCount;

}  // TableDesc::getBaseRowCntIfUniqueJoinCol

ValueIdSet TableDesc::getComputedColumns(NAColumnBooleanFuncPtrT fptr) {
  ValueIdSet computedColumns;

  for (CollIndex j = 0; j < getClusteringIndex()->getIndexKey().entries(); j++) {
    ItemExpr *ck = getClusteringIndex()->getIndexKey()[j].getItemExpr();

    if (ck->getOperatorType() == ITM_INDEXCOLUMN) ck = ((IndexColumn *)ck)->getDefinition().getItemExpr();

    CMPASSERT(ck->getOperatorType() == ITM_BASECOLUMN);

    NAColumn *x = ((BaseColumn *)ck)->getNAColumn();

    if (((*x).*fptr)()) computedColumns += ck->getValueId();
  }
  return computedColumns;
}

ValueIdSet TableDesc::getSaltColumnAsSet() { return getComputedColumns(&NAColumn::isSaltColumn); }

ValueIdSet TableDesc::getDivisioningColumns() { return getComputedColumns(&NAColumn::isDivisioningColumn); }

// compress the histograms based on query predicates on this table
void TableDesc::compressHistogramsForCurrentQuery() {
  // if there are some column statistics
  if ((colStats_.entries() != 0) && (table_) &&
      (table_->getExtendedQualName().getSpecialType() == ExtendedQualName::NORMAL_TABLE)) {  // if 1
    // check if query analysis info is available
    if (QueryAnalysis::Instance()->isAnalysisON()) {  // if 2
      // get a handle to the query analysis
      QueryAnalysis *queryAnalysis = QueryAnalysis::Instance();

      // get a handle to the table analysis
      const TableAnalysis *tableAnalysis = getTableAnalysis();

      if (!tableAnalysis) return;

      // iterate over statistics for each column
      for (CollIndex i = 0; i < colStats_.entries(); i++) {  // for 1
        // Get a handle to the column's statistics descriptor
        ColStatDescSharedPtr columnStatDesc = colStats_[i];

        // get a handle to the ColStats
        ColStatsSharedPtr colStats = columnStatDesc->getColStats();

        // if this is a single column, as opposed to a multicolumn
        if (colStats->getStatColumns().entries() == 1) {  // if 3
          // get column's value id
          const ValueId columnId = columnStatDesc->getColumn();

          // get column analysis
          ColAnalysis *colAnalysis = queryAnalysis->getColAnalysis(columnId);

          if (!colAnalysis) continue;

          ValueIdSet predicatesOnColumn = colAnalysis->getReferencingPreds();

          // we can compress this column's histogram if there
          // is a equality predicate against a constant

          ItemExpr *constant = NULL;

          NABoolean colHasEqualityAgainstConst = colAnalysis->getConstValue(constant);

          // if a equality predicate with a constant was found
          // i.e. predicate of the form col = 5
          if (colHasEqualityAgainstConst) {  // if 4
            if (constant)
              // compress the histogram
              columnStatDesc->compressColStatsForQueryPreds(constant, constant);
          }       // if 4
          else {  // else 4

            // since there is no equality predicates we might still
            // be able to compress the column's histogram based on
            // range predicates against a constant. Following are
            // examples of such predicates
            // * col > 1 <-- predicate defines a lower bound
            // * col < 3 <-- predicate defines a upper bound
            // * col >1 and col < 30 <-- window predicate, define both bounds
            ItemExpr *lowerBound = NULL;
            ItemExpr *upperBound = NULL;

            // Extract predicates from range spec and add it to the
            // original predicate set otherwise isARangePredicate() will
            // return FALSE, so histgram compression won't happen.
            ValueIdSet rangeSpecPred(predicatesOnColumn);
            for (ValueId predId = rangeSpecPred.init(); rangeSpecPred.next(predId); rangeSpecPred.advance(predId)) {
              ItemExpr *pred = predId.getItemExpr();
              if (pred->getOperatorType() == ITM_RANGE_SPEC_FUNC) {
                ValueIdSet vs;
                ((RangeSpecRef *)pred)->getValueIdSetForReconsItemExpr(vs);
                // remove rangespec vid from the original set
                predicatesOnColumn.remove(predId);
                // add preds extracted from rangespec to the original set
                predicatesOnColumn.insert(vs);
              }
            }

            // in the following loop we iterate over all the predicates
            // on this column. If there is a range predicate e.g. a > 2
            // or a < 3, then we use that to define upper and lower bounds.
            // Given predicate a > 2, we get a lower bound of 2.
            // Given predicate a < 3, we get a upper bound of 3.
            // The bound are then passed down to the histogram
            // compression methods.

            // iterate over predicates to see if any of them is a range
            // predicate e.g. a > 2
            for (ValueId predId = predicatesOnColumn.init(); predicatesOnColumn.next(predId);
                 predicatesOnColumn.advance(predId)) {  // for 2
              // check if this predicate is a range predicate
              ItemExpr *predicateOnColumn = predId.getItemExpr();
              if (predicateOnColumn->isARangePredicate()) {  // if 5

                // if a predicate is a range predicate we need to find out more
                // information regarding the predicate to see if it can be used
                // to compress the columns histogram. We look for the following:
                // * The predicate is against a constant e.g. a > 3 and not against
                //   another column e.g. a > b
                // Also give a predicate we need to find out what side is the column
                // and what side is the constant. Normally people write a range predicate
                // as a > 3, but the same could be written as 3 < a.
                // Also either on of the operands of the range predicate might be
                // a VEG, if so then we need to dig into the VEG to see where is
                // the constant and where is the column.

                // check the right and left children of this predicate to
                // see if one of them is a constant
                ItemExpr *leftChildItemExpr = (ItemExpr *)predicateOnColumn->getChild(0);
                ItemExpr *rightChildItemExpr = (ItemExpr *)predicateOnColumn->getChild(1);

                // by default assume the literal is at right i.e. predicate of
                // the form a > 2
                NABoolean columnAtRight = FALSE;

                // check if right child of predicate is a VEG
                if (rightChildItemExpr->getOperatorType() == ITM_VEG_REFERENCE) {  // if 6
                  // if child is a VEG
                  VEGReference *rightChildVEG = (VEGReference *)rightChildItemExpr;

                  // check if the VEG contains the current column
                  // if it does contain the current column then
                  // the predicate has the column on right and potentially
                  // a constant on the left.
                  if (rightChildVEG->getVEG()->getAllValues().contains(columnId)) {  // if 7
                    // column is at right i.e. predicate is of the form
                    // 2 < a
                    columnAtRight = TRUE;
                  }     // if 7
                }       // if 6
                else {  // else 6
                  // child is not a VEG
                  if (columnId == rightChildItemExpr->getValueId()) {  // if 8
                    // literals are at left i.e. predicate is of the form
                    // (1,2) < (a, b)
                    columnAtRight = TRUE;
                  }  // if 8
                }    // else 6

                ItemExpr *potentialConstantExpr = NULL;

                // check if the range predicate is against a constant
                if (columnAtRight) {  // if 9
                  // the left child is potentially a constant
                  potentialConstantExpr = leftChildItemExpr;
                }       // if 9
                else {  // else 9
                  // the right child is potentially a constant
                  potentialConstantExpr = rightChildItemExpr;
                }  // else 9

                // initialize constant to NULL before
                // looking for next constant
                constant = NULL;

                // check if potentialConstantExpr contains a constant.
                // we need to see if this range predicate is a predicate
                // against a constant e.g col > 1 and not a predicate
                // against another column e.g. col > anothercol

                // if the expression is a VEG
                if (potentialConstantExpr->getOperatorType() == ITM_VEG_REFERENCE) {  // if 10

                  // expression is a VEG, dig into the VEG to
                  // get see if it contains a constant
                  VEGReference *potentialConstantExprVEG = (VEGReference *)potentialConstantExpr;

                  potentialConstantExprVEG->getVEG()->getAllValues().referencesAConstValue(&constant);
                }       // if 10
                else {  // else 10

                  // express is not a VEG, it is a constant
                  if (potentialConstantExpr->getOperatorType() == ITM_CONSTANT) constant = potentialConstantExpr;
                }  // else 10

                // if predicate involves a constant, does the constant imply
                // a upper bound or lower bound
                if (constant) {  // if 11
                  // if range predicate has column at right e.g. 3 > a
                  if (columnAtRight) {  // if 12
                    if (predicateOnColumn->getOperatorType() == ITM_GREATER ||
                        predicateOnColumn->getOperatorType() == ITM_GREATER_EQ) {  // if 13
                      if (!upperBound) upperBound = constant;
                    }       // if 13
                    else {  // else 13
                      if (!lowerBound) lowerBound = constant;
                    }     // else 13
                  }       // if 12
                  else {  // else 12
                    // range predicate has column at left e.g. a < 3
                    if (predicateOnColumn->getOperatorType() == ITM_LESS ||
                        predicateOnColumn->getOperatorType() == ITM_LESS_EQ) {  // if 14
                      if (!upperBound) upperBound = constant;
                    }       // if 14
                    else {  // else 14
                      if (!lowerBound) lowerBound = constant;
                    }  // else 14
                  }    // else 12
                }      // if 11
              }        // if 5
            }          // for 2

            // if we found a upper bound or a lower bound
            if (lowerBound || upperBound) {
              // compress the histogram based on range predicates
              columnStatDesc->compressColStatsForQueryPreds(lowerBound, upperBound);
            }
          }  // else 4
        }    // if 3
      }      // for 1
    }        // if 2
  }          // if 1
  // All histograms compressed. Set the histCompressed flag to TRUE
  histsCompressed(TRUE);
}

// Compute probe stats for ORC NJ.
NABoolean TableDesc::computeMinMaxRowCountForLocalPred(const ValueIdSet &eqJoinCols, const ValueIdSet &localPreds,
                                                       CostScalar &count, CostScalar &min, CostScalar &max) {
  if (eqJoinCols.entries() != 1 || localPreds.entries() == 0) return TRUE;

  ValueId colValId = eqJoinCols.init();
  eqJoinCols.next(colValId);

  // Do it only for integer numeric data types for now.
  if (colValId.getType().getTypeQualifier() != NA_NUMERIC_TYPE) return FALSE;

  if (colValId.getType().getFSDatatype() != REC_BIN16_UNSIGNED &&
      colValId.getType().getFSDatatype() != REC_BIN16_SIGNED &&
      colValId.getType().getFSDatatype() != REC_BIN32_UNSIGNED &&
      colValId.getType().getFSDatatype() != REC_BIN32_SIGNED &&
      colValId.getType().getFSDatatype() != REC_BIN64_UNSIGNED &&
      colValId.getType().getFSDatatype() != REC_BIN64_SIGNED)
    return FALSE;

  NAString colName(STMTHEAP);
  colValId.getItemExpr()->unparse(colName, PARSER_PHASE, QUERY_FORMAT);

  NAString localPredsString(STMTHEAP);
  localPreds.unparse(localPredsString, PARSER_PHASE, QUERY_FORMAT, this);

  NATable *nt = const_cast<NATable *>(getNATable());
  NAHashDictionary<minmaxKey, minmaxValue> &minmaxCache = nt->minmaxCache();

  // Before any real work, check if there is a copy of the stats in minmax cache
  minmaxKey *key = new (STMTHEAP) minmaxKey(colName, localPredsString, STMTHEAP);

  NABoolean nj_compute_probes_debug = CmpCommon::getDefault(ORC_NJS_CPS_DEBUG) == DF_ON;

  if (nj_compute_probes_debug) {
    printf("\nTableDesc::computeMinMaxRowCountForLocalPred() called, this=%p\n", this);

    printf("NATable minmax cache entries=%d\n", minmaxCache.entries());

    printf("\nlocal preds=");
    localPreds.display();

    key->display();

    printf("\nhash(key)=%u\n", key->hash());
  }

  minmaxValue *value = minmaxCache.getFirstValue(key);
  if (value) {
    // A cached version is found, use it.
    count = value->getRowcount();
    min = value->getMinValue();
    max = value->getMaxValue();

    if (nj_compute_probes_debug) {
      printf("key in cache\n");
    }

    return TRUE;
  }

  // need to run the query to compute the value
  char query[4000];

  // formulate a SQL query to compute the count, the min and the max.
  // The following three queries in TPCDS have the desirable stats for
  // NJ to be very effective.
  //          count    gap (max-min+1)
  // q3       6200        72715
  // q46       314         1094
  // q79       157         1093
  //
  snprintf(query, sizeof(query),
           "select count(%s), min(%s), max(%s) "
           "from %s "
           "where %s",
           colName.data(), colName.data(), colName.data(), getCorrNameObj().getQualifiedNameAsString().data(),
           localPredsString.data());

  if (nj_compute_probes_debug) {
    printf("Probe stats query=%s\n", query);
  }

  Queue *queryInfoQueue = NULL;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  // Run the query
  int cliRC = cliInterface.fetchAllRows(queryInfoQueue, query, 0, FALSE, FALSE, TRUE);

  count = 0;

  // If execution error, bail out. Keep all errors as warnings to avoid
  // any compilation error to the main query.
  if (cliRC < 0) {
    if (nj_compute_probes_debug) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    } else {
      ComDiagsArea probingStatsDiags;
      cliInterface.retrieveSQLDiagnostics(&probingStatsDiags);

      // do not report "compilation error" as a warning
      probingStatsDiags.removeLastErrorCondition();

      // turn any errors into warnings.
      probingStatsDiags.negateAllErrors();
      CmpCommon::diags()->mergeAfter(probingStatsDiags);
    }

    return FALSE;
  }

  // If no row returned, bail out
  if (cliRC == 100) {
    return FALSE;
  }

  CMPASSERT(queryInfoQueue->numEntries() == 1);

  OutputInfo *vi = (OutputInfo *)queryInfoQueue->get(0);

  count = *(reinterpret_cast<int32_t *>(vi->get(0)));

  // If the row count is 0, set the min and max also to 0.
  if (count == 0) {
    min = max = 0.0;
  } else {
    // depending on the data types, populate min and max.
    if (colValId.getType().getFSDatatype() == REC_BIN16_UNSIGNED ||
        colValId.getType().getFSDatatype() == REC_BIN16_SIGNED) {
      min = *(reinterpret_cast<int16_t *>(vi->get(1)));
      max = *(reinterpret_cast<int16_t *>(vi->get(2)));
    } else if (colValId.getType().getFSDatatype() == REC_BIN32_UNSIGNED ||
               colValId.getType().getFSDatatype() == REC_BIN32_SIGNED) {
      min = *(reinterpret_cast<int32_t *>(vi->get(1)));
      max = *(reinterpret_cast<int32_t *>(vi->get(2)));
    } else if (colValId.getType().getFSDatatype() == REC_BIN64_UNSIGNED ||
               colValId.getType().getFSDatatype() == REC_BIN64_SIGNED) {
      min = *(reinterpret_cast<int64_t *>(vi->get(1)));
      max = *(reinterpret_cast<int64_t *>(vi->get(2)));
    }
  }

  // Cache the key and the values in minmax cache.
  NAMemory *heap = nt->getHeap();
  key = new (heap) minmaxKey(colName, localPredsString, heap);
  value = new (heap) minmaxValue(count, min, max);
  minmaxCache.insert(key, value);

  if (nj_compute_probes_debug)
    printf("Computed probe stats: rows=%.0f, gap distance=%.0f over [%.0f, %.0f]\n", count.getValue(),
           max.getValue() - min.getValue() + 1, min.getValue(), max.getValue());

  return TRUE;
}
