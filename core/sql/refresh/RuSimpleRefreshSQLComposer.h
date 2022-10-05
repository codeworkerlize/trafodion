
#ifndef _RU_SQL_SIMPLE_REFRESH_COMPOSER_H_
#define _RU_SQL_SIMPLE_REFRESH_COMPOSER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLSimpleRefreshComposer.h
* Description:  Definition of class CRUSQLRefreshComposer
*
*
* Created:      08/13/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuRefreshSQLComposer.h"

//--------------------------------------------------------------------------//
//
// CRUSimpleRefreshSQLComposer
//
// This class composes the INTERNAL REFRESH command (both incremenetal and
// recompute ) that is used with an audited  single transaction MV
// and unaudited MV. The class also composes the LOCK/UNLOCK TABLE statements
// that are used in the unaudited executor.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUSimpleRefreshSQLComposer : public CRURefreshSQLComposer {
 public:
  CRUSimpleRefreshSQLComposer(CRURefreshTask *pTask);
  virtual ~CRUSimpleRefreshSQLComposer() {}

 public:
  void ComposeRefresh();

  enum NoDeleteClause { REC_DELETE = FALSE, REC_NODELETE = TRUE };
  void ComposeRecompute(NoDeleteClause flag);

  void ComposeLock(const CDSString &name, BOOL exclusive);
  void ComposeUnLock(const CDSString &name);

 private:
  //-- Prevent copying
  CRUSimpleRefreshSQLComposer(const CRUSimpleRefreshSQLComposer &other);
  CRUSimpleRefreshSQLComposer &operator=(const CRUSimpleRefreshSQLComposer &other);

  void AddDeltaDefListClause();
};

#endif
