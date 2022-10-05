
#ifndef _RU_LOG_CLEANUP_SQL_COMPOSER_H_
#define _RU_LOG_CLEANUP_SQL_COMPOSER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuLogCleanupSQLComposer.h
* Description:  Definition of class CRULogCleanupSQLComposer
*
*
* Created:      09/25/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuSQLComposer.h"
#include "RuLogCleanupTaskExecutor.h"

class CRULogCleanupTask;
class CRUTbl;

//--------------------------------------------------------------------------//
//	CRULogCleanupSQLComposer
//
//	This class composes the syntax of the DELETE statements
//	for deletion from the IUD- and range-log in the log
//	cleanup task.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRULogCleanupSQLComposer : public CRUSQLComposer {
 private:
  typedef CRUSQLComposer inherited;

 public:
  CRULogCleanupSQLComposer(CRULogCleanupTask *pTask);
  virtual ~CRULogCleanupSQLComposer() {}

 public:
  enum {

    // Epochs 0..+100 in the log are reserved for special purposes
    MAX_SPECIAL_EPOCH = 100,

    // To avoid lock escalation, delete 10000 rows in txn
    MAX_ROW_TO_DELETE_IN_SINGLE_TXN = 10000
  };

 public:
  void ComposeIUDLogCleanup(CRULogCleanupTaskExecutor::SQL_STATEMENT type);
  void ComposeRangeLogCleanup();
  void composeGetRowCount();

 private:
  void ComposeHeader(const CDSString &tableName, CRULogCleanupTaskExecutor::SQL_STATEMENT type);

 private:
  //-- Prevent copying
  CRULogCleanupSQLComposer(const CRULogCleanupSQLComposer &other);
  CRULogCleanupSQLComposer &operator=(const CRULogCleanupSQLComposer &other);

 private:
  CRUTbl &tbl_;
  TInt32 maxInapplicableEpoch_;
};

#endif
