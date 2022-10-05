
/* -*-C++-*-
******************************************************************************
*
* File:         RuEmpCheckTask.cpp
* Description:  Implementation of class CRUEmpCheckTask
*
*
* Created:      12/29/1999
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuEmpCheckTask.h"
#include "RuEmpCheckTaskExecutor.h"
#include "RuEmpCheckVector.h"
#include "RuTbl.h"

//---------------------------------------------------------------//
//	Constructor and destructor of CRUEmpCheckTask
//---------------------------------------------------------------//

CRUEmpCheckTask::CRUEmpCheckTask(int id, CRUTbl &table) : inherited(id, table) {}

CRUEmpCheckTask::~CRUEmpCheckTask() {}

//--------------------------------------------------------------------------//
//	CRUEmpCheckTask::GetTaskName()
//--------------------------------------------------------------------------//

CDSString CRUEmpCheckTask::GetTaskName() const {
  CDSString name("EC(");
  name += GetTable().GetFullName() + " ) ";

  return name;
}

//--------------------------------------------------------------------------//
//	CRUEmpCheckTask::CreateExecutorInstance()
//
//	Task executor's creation
//--------------------------------------------------------------------------//

CRUTaskExecutor *CRUEmpCheckTask::CreateExecutorInstance() {
  // Setup the data structure ...
  GetTable().BuildEmpCheckVector();

  CRUTaskExecutor *pTaskEx = new CRUEmpCheckTaskExecutor(this);

  return pTaskEx;
}

//--------------------------------------------------------------------------//
//	CRUEmpCheckTask::PullDataFromExecutor()
//
//	(1) Copy the updated emptiness check vector
//		from the executor to the table object.
//	(2) If the check's results are final (i.e.,
//	    the table is not an involved MV), broadcast
//		the check's results to the using MVs.
//
//--------------------------------------------------------------------------//

void CRUEmpCheckTask::PullDataFromExecutor() {
  inherited::PullDataFromExecutor();

  CRUEmpCheckTaskExecutor &taskEx = (CRUEmpCheckTaskExecutor &)GetExecutor();

  CRUTbl &tbl = GetTable();

  // Copy the emptiness check vector back to the table object
  CRUEmpCheckVector &empCheckVec = tbl.GetEmpCheckVector();
  empCheckVec = taskEx.GetEmpCheckVector();

  if (FALSE == tbl.IsInvolvedMV()) {
    empCheckVec.SetFinal();
    // Update the using MVs' delta-def lists
    tbl.PropagateEmpCheckToUsingMVs();
  }
}
