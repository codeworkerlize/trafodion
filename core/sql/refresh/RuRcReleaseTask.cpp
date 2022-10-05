
/* -*-C++-*-
******************************************************************************
*
* File:         RuRcReleaseTask.cpp
* Description:  Implementation of class	CRURcReleaseTask
*
* Created:      10/18/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "RuRcReleaseTask.h"
#include "RuRcReleaseTaskExecutor.h"

//-----------------------------------------------------//
//	Constructor
//-----------------------------------------------------//

CRURcReleaseTask::CRURcReleaseTask(int id, CRUObject &obj) : inherited(id), obj_(obj) {}

//-----------------------------------------------------//
//	CRURcReleaseTask::CreateExecutorInstance()
//-----------------------------------------------------//

CRUTaskExecutor *CRURcReleaseTask::CreateExecutorInstance() { return new CRURcReleaseTaskExecutor(this); }
