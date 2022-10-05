
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         RelDCL.C
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

#include "optimizer/Sqlcomp.h"
#include "optimizer/BindWA.h"
#include "optimizer/RelScan.h"
#include "optimizer/PhyProp.h"
#include "optimizer/opt.h"
#include "RelDCL.h"

///////////////////////////////////////////////////////////////////
//
// methods for class RelLock
//
///////////////////////////////////////////////////////////////////
RelLock::RelLock(const CorrName &name, LockMode lockMode, NABoolean lockIndex, OperatorTypeEnum otype,
                 NABoolean parallelExecution)
    : RelExpr(otype),
      userTableName_(name, CmpCommon::statementHeap()),
      lockMode_(lockMode),
      lockIndex_(lockIndex),
      isView_(FALSE),
      parallelExecution_(parallelExecution),
      baseTableNameList_(CmpCommon::statementHeap()),
      tabIds_(CmpCommon::statementHeap()) {
  setNonCacheable();
}

RelLock::~RelLock() {}

void RelLock::getPotentialOutputValues(ValueIdSet &outputValues) const {
  outputValues.clear();  // for now, it produces no output values
}

RelExpr *RelLock::copyTopNode(RelExpr *derivedNode, CollHeap *outHeap) {
  RelLock *result;

  if (derivedNode == NULL)
    result = new (outHeap) RelLock(userTableName_, lockMode_, lockIndex_, getOperatorType(), parallelExecution_);
  else
    result = (RelLock *)derivedNode;

  return RelExpr::copyTopNode(result, outHeap);
}

const NAString RelLock::getText() const {
  NAString result(CmpCommon::statementHeap());

  if (getOperatorType() == REL_LOCK)
    result = "Lock ";
  else
    result = "Unlock ";

  return result + userTableName_.getText();
}

void RelLock::addTableNameToList(RelExpr *node) {
  for (int i = 0; i < node->getArity(); i++) {
    addTableNameToList(node->child(i)->castToRelExpr());
  }

  if (node->getOperatorType() == REL_SCAN) baseTableNameList_.insert((CorrName *)(((Scan *)node)->getPtrToTableName()));
}

///////////////////////////////////////////////////////////////////
//
// methods for class RelTransaction
//
///////////////////////////////////////////////////////////////////
RelTransaction::RelTransaction(TransStmtType type, TransMode *mode, ItemExpr *diagAreaSizeExpr)
    : RelExpr(REL_TRANSACTION),
      type_(type),
      mode_(mode),
      hasSavepointName_(FALSE),
      savepointName_(""),
      diagAreaSizeExpr_(diagAreaSizeExpr) {
  setNonCacheable();
}

RelTransaction::~RelTransaction() {}

void RelTransaction::transformNode(NormWA & /*normWARef*/, ExprGroupId & /*locationOfPointerToMe*/) {}

void RelTransaction::getPotentialOutputValues(ValueIdSet &outputValues) const {
  outputValues.clear();  // for now, it produces no output values
}

RelExpr *RelTransaction::copyTopNode(RelExpr *derivedNode, CollHeap *outHeap) {
  RelTransaction *result;

  if (derivedNode == NULL) {
    result = new (outHeap) RelTransaction(type_, mode_, diagAreaSizeExpr_);
    if (hasSavepointName_) result->setSavepointName(savepointName_);
  } else
    result = (RelTransaction *)derivedNode;

  return RelExpr::copyTopNode(result, outHeap);
}

const NAString RelTransaction::getText() const {
  NAString result(CmpCommon::statementHeap());

  switch (type_) {
    case BEGIN_:
      result = "Begin Work ";
      break;

    case COMMIT_:
      result = "Commit Work ";
      break;

    case ROLLBACK_:
      result = "Rollback Work ";
      break;

    case ROLLBACK_WAITED_:
      result = "Rollback Work Waited ";
      break;

    case BEGIN_SAVEPOINT_:
      result = "Begin Savepoint ";
      if (hasSavepointName_) result += savepointName_;
      break;

    case ROLLBACK_SAVEPOINT_:
      result = "Rollback Savepoint ";
      if (hasSavepointName_) result += savepointName_;
      break;

    case COMMIT_SAVEPOINT_:
      result = "Commit Savepoint ";
      if (hasSavepointName_) result += savepointName_;
      break;

    case RELEASE_SAVEPOINT_:
      result = "Release Savepoint ";
      if (hasSavepointName_) result += savepointName_;
      break;

    case SET_TRANSACTION_:
      result = "Set Transaction ";
      break;
  }

  return result;
}

///////////////////////////////////////////////////////////////////
//
//   methods for class RelSetTimeout
//
///////////////////////////////////////////////////////////////////
RelSetTimeout::RelSetTimeout(const CorrName &tableName, ItemExpr *timeoutValueExpr,
                             NABoolean isStreamTimeout,  // default FALSE,
                             NABoolean isReset,          // default FALSE
                             NABoolean isForAllTables,   // default FALSE
                             char *physicalFileName)     // default NULL
    : RelExpr(REL_SET_TIMEOUT),
      userTableName_(tableName),
      timeoutValueExpr_(timeoutValueExpr),
      isStream_(isStreamTimeout),
      isReset_(isReset),
      isForAllTables_(isForAllTables) {
  setNonCacheable();
  CMPASSERT((NULL != timeoutValueExpr && !isReset) || (NULL == timeoutValueExpr && isReset));
  if (physicalFileName) setPhysicalFileName(physicalFileName);
}

void RelSetTimeout::transformNode(NormWA & /*normWARef*/, ExprGroupId & /*locationOfPointerToMe*/) {
  setNonCacheable();
}

void RelSetTimeout::getPotentialOutputValues(ValueIdSet &outputValues) const {
  outputValues.clear();  // for now, it produces no output values
}

RelExpr *RelSetTimeout::copyTopNode(RelExpr *derivedNode, CollHeap *outHeap) {
  RelSetTimeout *result;

  if (derivedNode == NULL)
    result = new (outHeap)
        RelSetTimeout(userTableName_, timeoutValueExpr_, isStream_, isReset_, isForAllTables_, physicalFileName_);
  else
    result = (RelSetTimeout *)derivedNode;

  return RelExpr::copyTopNode(result, outHeap);
}

const NAString RelSetTimeout::getText() const { return "SET TIMEOUT"; }
