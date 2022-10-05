
/* -*-C++-*-
******************************************************************************
*
* File:         RuLogProcessingTask.cpp
* Description:  Implementation of class CRULogProcessingTask
*
*
* Created:      08/29/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuLogProcessingTask.h"
#include "RuException.h"

//--------------------------------------------------------------------------//
//	Constructor
//--------------------------------------------------------------------------//

CRULogProcessingTask::CRULogProcessingTask(int id, CRUTbl &table) : inherited(id), table_(table) {}

CRULogProcessingTask::~CRULogProcessingTask() {}

//--------------------------------------------------------------------------//
//	CRULogProcessingTask::HandleSuccessorFailure()
//
//	If this is the last task that depends on me -
//	I am obsolete, and must not be executed.
//--------------------------------------------------------------------------//

void CRULogProcessingTask::HandleSuccessorFailure(CRUTask &taask) {
  if (1 == GetTasksThatDependOnMe().GetCount()) {
    CRUException &ex = GetErrorDesc();
    ex.SetError(IDS_RU_OBSOLETE_PROBLEM);
    ex.AddArgument(GetTaskName());
  }
}
