
#ifndef _RU_SQL_MULTI_TXN_REFRESH_COMPOSER_H_
#define _RU_SQL_MULTI_TXN_REFRESH_COMPOSER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLMultiTxnRefreshComposer.h
* Description:  Definition of class CRUSQLRefreshComposer
*
*
* Created:      08/17/2000
* Language:     C++
*
*
*
******************************************************************************
*/

//--------------------------------------------------------------------------//
//	CRUMultiTxnRefreshSQLComposer
//
//	This class compose the internal refresh command for a multi-transaction MV
//
//--------------------------------------------------------------------------//

#include "RuRefreshSQLComposer.h"

class REFRESH_LIB_CLASS CRUMultiTxnRefreshSQLComposer : public CRURefreshSQLComposer {
 private:
  typedef CRURefreshSQLComposer inherited;

 public:
  CRUMultiTxnRefreshSQLComposer(CRURefreshTask *pTask);
  virtual ~CRUMultiTxnRefreshSQLComposer() {}

 public:
  void ComposeRefresh(int phase, BOOL catchup);
  void ComposeCreateContextLogTable();
  void ComposeReadContextLog();
  void ComposeCQSForIRPhase1();

 private:
  //-- Prevent copying
  CRUMultiTxnRefreshSQLComposer(const CRUMultiTxnRefreshSQLComposer &other);
  CRUMultiTxnRefreshSQLComposer &operator=(const CRUMultiTxnRefreshSQLComposer &other);

 private:
  void AddNRowsClause(int phase, BOOL catchup);
};

#endif
