
#ifndef _RU_REFRESH_SQL_COMPOSER_H_
#define _RU_REFRESH_SQL_COMPOSER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLRefreshComposer.h
* Description:  Definition of class CRURefreshSQLComposer
*
*
* Created:      08/13/2000
* Language:     C++
*
*
*
******************************************************************************
*/

//--------------------------------------------------------------------------//
//	CRURefreshSQLComposer
//
//	This class composes the syntax of the INTERNAL REFRESH command
//	for the given task. The SQL generation is based on universal
//	building blocks to allow flexibility. These basic methods are used
//	in the derived composer classes.
//
//--------------------------------------------------------------------------//

#include "RuSQLComposer.h"
#include "RuRefreshTask.h"
#include "RuDeltaDef.h"
#include "RuForceOptions.h"

class REFRESH_LIB_CLASS CRURefreshSQLComposer : public CRUSQLComposer {
 private:
  typedef CRUSQLComposer inherited;

 public:
  CRURefreshSQLComposer(CRURefreshTask *pTask);
  virtual ~CRURefreshSQLComposer() {}

 public:
  void ComposeCntrlTableMDAMText(CRUForceOptions::MdamOptions mdamOption, const CDSString *pName = NULL);
  void ComposeResetCntrlTableMDAMText(const CDSString *pName = NULL);
  void ComposeDeleteContextLogTable();
  void ComposeControlQueryShape();
  void ComposeControlQueryShapeCut();
  void ComposeShowExplain();

 protected:
  void StartInternalRefresh();
  void AddDeltaDefClause(const CRUDeltaDef *pDdef, CDSString &fromEpoch, CDSString &toEpoch);
  void AddPhaseParam();
  void AddPipeLineClause();
  void ComposeQueryShape();

  CDSString GetContextLogName() const;

 protected:
  CRUMV &GetRootMV() const { return *pRootMV_; }

  CRURefreshTask &GetRefreshTask() const {
    RUASSERT(NULL != pTask_);
    return *pTask_;
  }

 private:
  //-- Prevent copying
  CRURefreshSQLComposer(const CRURefreshSQLComposer &other);
  CRURefreshSQLComposer &operator=(const CRURefreshSQLComposer &other);

 private:
  void AddRngDeltaStatisticsClause(const CRUDeltaDef *pDdef);
  void AddIUDDeltaStatisticsClause(const CRUDeltaDef *pDdef);
  void AddUpdatedColumnsClause(const CRUUpdateBitmap *pUpdateBitmap);

 private:
  CRURefreshTask *pTask_;
  CRUMV *pRootMV_;
};

#endif
