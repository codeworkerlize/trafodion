
#ifndef _REFRESH_TEST_H_
#define _REFRESH_TEST_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RefreshTest.h
* Description:  Test engine for the refresh utility
*
*
* Created:      01/09/2001
* Language:     C++
*
*
*
******************************************************************************
*/

#include "uofsProcessPool.h"
#include "RuSQLDynamicStatementContainer.h"
#include "RuTestTaskExecutor.h"
#include "RuOptions.h"
#include "RuGlobals.h"
#include "RuJournal.h"
#include "dsptrlist.h"
#include "dmresultset.h"
#include "uofsTransaction.h"

struct GroupStatement;

class RefreshTestController {
 public:
  enum {
    EX_SEND_FOR_INIT,
    EX_WAIT_FOR_ALL_RETURN_FROM_COMPILATION,
    EX_SEND_FOR_EXECUTE,
    EX_WAIT_FOR_ALL_RETURN_FROM_EXECUTION,
    EX_COMPLETE
  };

 public:
  RefreshTestController(int numOfProcesses)
      : numOfProcesses_(numOfProcesses),
        activeProcesses_(0),
        processPool_("tdm_arkutp.exe"),
        dynamicSQLContainer_(2),
        currentGroupId_(0),
        executorsList_(CDSPtrList<CRUTestTaskExecutor>::eItemsAreOwned),
        groupList_(CDSPtrList<GroupStatement>::eItemsAreOwned),
        state_(EX_SEND_FOR_INIT),
        endFlag_(FALSE),
        pJournal_(NULL) {}

  virtual ~RefreshTestController() {}

 public:
  int GetState() { return state_; }
  void SetState(int state) { state_ = state; }

 public:
  void Init();
  void Work();

 private:
  int InitiateTaskProcess();
  void GroupSendForInitialization();
  void SendForInitialization(int groupId, int processId, int numOfStmt);
  void SendExecutor(CRUTaskExecutor &executor);
  void WaitForAll();
  void SendSyncToAllExecutors();
  void HandleReturnOfExecutor(int pid);
  CRUTaskExecutor *FindRunningExecutor(int pid);
  void SaveTime(int groupId, int processId);

 private:
  int state_;
  int numOfProcesses_;
  int activeProcesses_;
  int currentGroupId_;
  BOOL endFlag_;

  CUOFsTaskProcessPool processPool_;
  CRUSQLDynamicStatementContainer dynamicSQLContainer_;

  CDSPtrList<CRUTestTaskExecutor> executorsList_;

  CDSPtrList<GroupStatement> groupList_;

  DSListPosition currentPos_;

  CRUOptions options_;

  //-- Output file , the journal can be initialized only after we
  // receive the globals message that contains the output filename
  CRUJournal *pJournal_;
  CUOFsTransManager transManager_;
};

struct GroupStatement {
  int groupId_;
  int processId_;
  int num_of_executions;
};

#endif
