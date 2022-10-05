
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
#include "RuSQLRefreshComposer.h"

class REFRESH_LIB_CLASS CRUSQLMultiTxnRefreshComposer : public CRUSQLRefreshComposer {
 public:
  CRUSQLMultiTxnRefreshComposer(CRURefreshTask *pTask);
  virtual ~CRUSQLMultiTxnRefreshComposer() {}

 public:
  void ComposeRefresh(int phase, BOOL catchup);

 private:
  void AddNRowsClause(int phase, BOOL catchup);
};

#endif
