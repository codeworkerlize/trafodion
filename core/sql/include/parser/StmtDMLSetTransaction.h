#ifndef STMTDMLSET_TRANSACTION_H
#define STMTDMLSET_TRANSACTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDMLSetTransaction.h
 *
 * Description:  class for parse node representing Set Transaction statements
 *
 * Created:      07/01/96
 *

 *
 *****************************************************************************
 */

#include "common/ComTransInfo.h"
#include "parser/StmtNode.h"
#include "optimizer/ItemExpr.h"
#include "optimizer/ItemExprList.h"

class StmtDMLSetTransaction : public StmtNode {
 public:
  StmtDMLSetTransaction(ItemExpr *transaction_mode);

  virtual ~StmtDMLSetTransaction();

  TransMode::AccessMode getAccessMode() const { return accessMode_; }
  TransMode::IsolationLevel getIsolationLevel() const { return isolationLevel_; }
  ItemExpr *getDiagSizeNode() const { return diagSize_; }
  TransMode::RollbackMode getRollbackMode() const { return rollbackMode_; }
  int getAutoAbortInterval() const { return autoabortInterval_; }
  TransMode::MultiCommit getMultiCommit() const { return multiCommit_; }

  int getMultiCommitSize() const { return multiCommitSize_; }

  NABoolean isIsolationLevelSpec() const { return isolationLevel_ != TransMode::IL_NOT_SPECIFIED_; }

  NABoolean isAccessModeSpec() const { return accessMode_ != TransMode::AM_NOT_SPECIFIED_; }

  NABoolean isRollbackModeSpec() const { return rollbackMode_ != TransMode::ROLLBACK_MODE_NOT_SPECIFIED_; }

  NABoolean isAutoabortIntervalSpec() const { return autoabortInterval_ != -1; }

  NABoolean isMultiCommitSize() const { return multiCommitSize_ != 0; }

  NABoolean isMultiCommitSpec() const { return multiCommit_ != TransMode::MC_NOT_SPECIFIED_; }

  TransMode::AutoOption beginAO() { return beginAO_; }
  TransMode::AutoOption commitAO() { return commitAO_; }
  TransMode::AutoOption savepointAO() { return savepointAO_; }

  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;
  void setAttribute(ItemExpr *child);

 private:
  ItemExpr *childNode_;
  ItemExpr *diagSize_;
  TransMode::AccessMode accessMode_;
  TransMode::IsolationLevel isolationLevel_;
  TransMode::RollbackMode rollbackMode_;
  int autoabortInterval_;
  int multiCommitSize_;
  TransMode::MultiCommit multiCommit_;
  TransMode::AutoOption beginAO_;
  TransMode::AutoOption commitAO_;
  TransMode::AutoOption savepointAO_;

};  // class StmtDMLSetTransaction

class TxnIsolationItem : public ItemExpr {
 public:
  TxnIsolationItem(TransMode::IsolationLevel isolationLevel_);
  TransMode::IsolationLevel getIsolationLevel() const { return isolationLevel_; }

 private:
  TransMode::IsolationLevel isolationLevel_;
};

class TxnAccessModeItem : public ItemExpr {
 public:
  TxnAccessModeItem(TransMode::AccessMode access_mode);
  TransMode::AccessMode getAccessMode() const { return accessMode_; }

 private:
  TransMode::AccessMode accessMode_;
};

class DiagnosticSizeItem : public ItemExpr {
 public:
  DiagnosticSizeItem(ItemExpr *child);
  ItemExpr *getDiagSizeNode() { return getChild(0)->castToItemExpr(); }

 private:
};

class TxnRollbackModeItem : public ItemExpr {
 public:
  TxnRollbackModeItem(TransMode::RollbackMode rollback_mode);
  TransMode::RollbackMode getRollbackMode() const { return rollbackMode_; }

 private:
  TransMode::RollbackMode rollbackMode_;
};

class TxnAutoabortIntervalItem : public ItemExpr {
 public:
  TxnAutoabortIntervalItem(int val);
  int getAutoabortInterval() const { return autoabortInterval_; }

 private:
  int autoabortInterval_;
};

class TxnMultiCommitItem : public ItemExpr {
 public:
  TxnMultiCommitItem(int val, NABoolean mc);
  TxnMultiCommitItem(NABoolean mc);
  int getMultiCommitSize() const { return multiCommitSize_; }
  TransMode::MultiCommit getMultiCommit() const { return multiCommit_; }

 private:
  int multiCommitSize_;
  TransMode::MultiCommit multiCommit_;
};

class TxnAutoModeItem : public ItemExpr {
 public:
  TxnAutoModeItem(TransMode::AutoOption beginAO, TransMode::AutoOption commitAO, TransMode::AutoOption savepointAO);
  TransMode::AutoOption beginAO() { return beginAO_; }
  TransMode::AutoOption commitAO() { return commitAO_; }
  TransMode::AutoOption savepointAO() { return savepointAO_; }

 private:
  TransMode::AutoOption beginAO_;
  TransMode::AutoOption commitAO_;
  TransMode::AutoOption savepointAO_;
};  // class TxnAutoModeItem

#endif  // STMTDMLSET_TRANSACTION_H
