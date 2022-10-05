
/* -*-C++-*-
******************************************************************************
*
* File:         RuTableSyncTask.cpp
* Description:  Implementation of class CRUTableSyncTask
*
*
* Created:      12/06/1999
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuTableSyncTask.h"
#include "RuTableSyncTaskExecutor.h"
#include "RuTbl.h"

//---------------------------------------------------------------//
//	Constructor and destructor of CRUTableSyncTask
//---------------------------------------------------------------//

CRUTableSyncTask::CRUTableSyncTask(int id, CRUTbl &table) : CRULogProcessingTask(id, table) {}

CRUTableSyncTask::~CRUTableSyncTask() {}

//---------------------------------------------------------//
//	CRUTableSyncTask::GetTaskName()
//---------------------------------------------------------//

CDSString CRUTableSyncTask::GetTaskName() const {
  CDSString name("TS(");
  name += GetTable().GetFullName() + ") ";

  return name;
}

//---------------------------------------------------------//
//	CRUTableSyncTask::CreateExecutorInstance()
//
//	Task executor's creation
//---------------------------------------------------------//

CRUTaskExecutor *CRUTableSyncTask::CreateExecutorInstance() {
  GetTable().CheckIfLongLockNeeded();

  CRUTaskExecutor *pTaskEx = new CRUTableSyncTaskExecutor(this);

  return pTaskEx;
}
