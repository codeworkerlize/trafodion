
/* -*-C++-*-
******************************************************************************
*
* File:         RuLockEquivSetTaskExecutor.cpp
* Description:  Implementation of class CRULockEquivSetTaskExecutor.
*
*
* Created:      12/06/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuLockEquivSetTaskExecutor.h"
#include "RuLockEquivSetTask.h"

#include "RuTbl.h"

//--------------------------------------------------------------------------//
//	Constructor and destructor
//--------------------------------------------------------------------------//

CRULockEquivSetTaskExecutor::CRULockEquivSetTaskExecutor(CRUTask *pParentTask) : inherited(pParentTask) {}

CRULockEquivSetTaskExecutor::~CRULockEquivSetTaskExecutor() {}

//--------------------------------------------------------------------------//
//	CRULockEquivSetTaskExecutor::Init()
//
//	Initialize the emptiness check unit
//--------------------------------------------------------------------------//

void CRULockEquivSetTaskExecutor::Init() { inherited::Init(); }

//--------------------------------------------------------------------------//
//	CRULockEquivSetTaskExecutor::Work()
//
//--------------------------------------------------------------------------//

void CRULockEquivSetTaskExecutor::Work() {
  CRULockEquivSetTask *pParentTask = (CRULockEquivSetTask *)GetParentTask();
  RUASSERT(NULL != pParentTask);

  TESTPOINT(CRUGlobals::TESTPOINT173);
  // We open transaction here because we might do an Invalidate opens
  // sql statement and also we need to make get current time sql statement
  // that requires a transaction
  BeginTransaction();

  FreezeAllTables(pParentTask);

  SetTimestampToAllTbls(CRUGlobals::GetCurrentTimestamp());

  CommitTransaction();

  TESTPOINT(CRUGlobals::TESTPOINT174);

  SetState(EX_COMPLETE);
}

//--------------------------------------------------------------------------//
//	CRULockEquivSetTaskExecutor::FreezeAllTables()
//
//	Work() callee.
//
//  Each table in the set must be freezed by either locking the table or
//  by locking the log. If an epoch increment is needed we must lock only
//  the log (otherwise we will not be able to execute epoch increment of the
//  table). If an epoch increment is not needed and the table will be locked
//  only if long lock is needed.
//--------------------------------------------------------------------------//

void CRULockEquivSetTaskExecutor::FreezeAllTables(CRULockEquivSetTask *pParentTask) {
  CRUTblList &tblList = pParentTask->GetTableList();

  DSListPosition pos = tblList.GetHeadPosition();
  while (NULL != pos) {
    CRUTbl *pTbl = tblList.GetNext(pos);
    if (TRUE == pTbl->IsIncEpochNeeded()) {
      pTbl->ExecuteLogReadProtectedOpen();
    } else {
      if (TRUE == pTbl->IsLongLockNeeded()) {
        pTbl->ExecuteReadProtectedOpen();
      }
    }
  }
}

//--------------------------------------------------------------------------//
//	CRULockEquivSetTaskExecutor::SetTimestampToAllTbls()
//--------------------------------------------------------------------------//
void CRULockEquivSetTaskExecutor::SetTimestampToAllTbls(TInt64 ts) {
  CRULockEquivSetTask *pParentTask = (CRULockEquivSetTask *)GetParentTask();
  RUASSERT(NULL != pParentTask);

  CRUTblList &tblList = pParentTask->GetTableList();

  DSListPosition pos = tblList.GetHeadPosition();
  while (NULL != pos) {
    CRUTbl *pTbl = tblList.GetNext(pos);
    pTbl->SetTimestamp(ts);
  }
}
