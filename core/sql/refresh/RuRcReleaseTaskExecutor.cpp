
/* -*-C++-*-
******************************************************************************
*
* File:         RuRcReleaseTaskExecutor.cpp
* Description:  Implementation of class	CRURcReleaseTaskExecutor
*
* Created:      10/18/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "RuRcReleaseTask.h"
#include "RuObject.h"
#include "RuRcReleaseTaskExecutor.h"

//------------------------------------------------------------------//
//	Constructor
//------------------------------------------------------------------//

CRURcReleaseTaskExecutor::CRURcReleaseTaskExecutor(CRUTask *pParentTask) : inherited(pParentTask) {}

//------------------------------------------------------------------//
//	CRURcReleaseTaskExecutor::Init()
//------------------------------------------------------------------//

void CRURcReleaseTaskExecutor::Init() {
  CRURcReleaseTask *pParentTask = (CRURcReleaseTask *)GetParentTask();
  CRUObject &obj = pParentTask->GetObject();

  if (FALSE == obj.HoldsResources()) {
    ResetHasWork();
  }
}

//------------------------------------------------------------------//
//	CRURcReleaseTaskExecutor::Work()
//
//	The main finite-state machine switch
//------------------------------------------------------------------//

void CRURcReleaseTaskExecutor::Work() {
  CRURcReleaseTask *pParentTask = (CRURcReleaseTask *)GetParentTask();
  RUASSERT(NULL != pParentTask);

  BeginTransaction();

  CRUObject &obj = pParentTask->GetObject();
  RUASSERT(TRUE == obj.HoldsResources());

  obj.ReleaseResources();

  // Simulate the crash, and check the atomicity of SaveMetadata() !
  TESTPOINT(CRUGlobals::TESTPOINT112);

  CommitTransaction();

  SetState(EX_COMPLETE);
}
