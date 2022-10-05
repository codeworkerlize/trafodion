
#ifndef _RU_FLOW_CONTROLLER_H_
#define _RU_FLOW_CONTROLLER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuFlowController.h
* Description:  Definition of class CRUFlowController
*
* Created:      05/07/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuRuntimeController.h"

class CRUDependenceGraph;

//--------------------------------------------------------------------------//
//	CRUFlowController
//
//	The flow controller FSM is responsible for task scheduling
//	and processing the results of task execution.
//
//	The class operates the dependence graph object, which is created
//	at the utility's prologue stage, and is responsible for task
//	communication and scheduling
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUFlowController : public CRURuntimeController {
 private:
  typedef CRURuntimeController inherited;

 public:
  CRUFlowController(CRUDependenceGraph &dg, TInt32 maxParallelism);
  virtual ~CRUFlowController() {}

 public:
  BOOL DidTaskFailuresHappen() const { return didTaskFailuresHappen_; }

 protected:
  //-- Pure virtual function implementation
  //-- The actual switch for event handling
  virtual void HandleRequest(CRURuntimeControllerRqst *pRequest);

 private:
  //-- Prevent copying
  CRUFlowController(const CRUFlowController &other);
  CRUFlowController &operator=(const CRUFlowController &other);

 private:
  //-- HandleRequest() callees
  void HandleScheduleRqst();
  void HandleFinishTaskRqst(CRURuntimeControllerRqst *pRqst);

  void RouteScheduledTask(CRUTask *pTask);
  static BOOL NeedToReportTaskFailure(CRUTask &task);

  // Notify the tasks that depend on this one about its completion
  void NotifyTaskEnvironmentOnFailure(CRUTask &task);

 private:
  CRUDependenceGraph &dg_;
  TInt32 maxParallelism_;
  unsigned short nRunningTasks_;

  BOOL didTaskFailuresHappen_;
};

#endif
