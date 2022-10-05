
/* -*-C++-*-
******************************************************************************
*
* File:         RuLogCleanupTask.cpp
* Description:  Implementation of class	CRULogCleanupTask
*
*
* Created:      12/29/1999
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuLogCleanupTask.h"
#include "RuLogCleanupTaskExecutor.h"

#include "RuMV.h"

//---------------------------------------------------------------//
//	Constructor
//---------------------------------------------------------------//

CRULogCleanupTask::CRULogCleanupTask(int id, CRUTbl &table) : inherited(id, table), maxInapplicableEpoch_(0) {}

//---------------------------------------------------------//
//	CRULogCleanupTask::CreateExecutorInstance()
//
//	Task executor's instance creation.
//
//---------------------------------------------------------//

CRUTaskExecutor *CRULogCleanupTask::CreateExecutorInstance() {
  ComputeMaxInapplicableEpoch();

  CRUTaskExecutor *pTaskEx = new CRULogCleanupTaskExecutor(this);

  return pTaskEx;
}

//--------------------------------------------------------------------------//
//	CRULogCleanupSQLComposer::ComputeMaxInapplicableEpoch()
//
//	Compute the minimal value of MV.EPOCH[T] between the ON REQUEST
//	MVs that use T.
//
//	All the log records that have the ABSOLUTE epoch value between this value
//	and 100 (the first 100 epochs in the log are reserved for special purposes)
//	must be deleted.
//
//--------------------------------------------------------------------------//

void CRULogCleanupTask::ComputeMaxInapplicableEpoch() {
  const TInt64 myUID = GetTable().GetUID();

  CRUMVList &mvList = GetTable().GetOnRequestMVsUsingMe();

  DSListPosition pos = mvList.GetHeadPosition();

  CRUMV *pCurrMV = mvList.GetNext(pos);
  maxInapplicableEpoch_ = pCurrMV->GetEpoch(myUID);

  while (NULL != pos) {
    pCurrMV = mvList.GetNext(pos);

    // not interested in the epoch if its in ignore
    // changes list
    if (FALSE == pCurrMV->IsIgnoreChanges(myUID)) {
      TInt32 ep = pCurrMV->GetEpoch(myUID);

      maxInapplicableEpoch_ = (ep < maxInapplicableEpoch_) ? ep : maxInapplicableEpoch_;
    }
  }

  // One less than the minimal MV.EPOCH[T]
  maxInapplicableEpoch_--;
}
