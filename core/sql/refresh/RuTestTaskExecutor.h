
#ifndef _RU_TEST_TASK_EXECUTOR_H_
#define _RU_TEST_TASK_EXECUTOR_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuTestTaskExecutor.cpp
* Description:  Definition of class CRUTestTaskExecutor
*
*
* Created:      01/09/2001
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuTaskExecutor.h"
#include "RuSQLDynamicStatementContainer.h"

class REFRESH_LIB_CLASS CRUTestTaskExecutor : public CRUTaskExecutor {
 public:
  enum { SQL_TEXT_MAX_SIZE = 1000 };

 private:
  typedef CRUTaskExecutor inherited;

 public:
  CRUTestTaskExecutor(CRUTask *pParentTask = NULL)
      : pDynamicSQLContainer_(NULL),
        pNumberOfExecutions_(NULL),
        pNumberOfRetries_(NULL),
        pNumberOfFailures_(NULL),
        pAutoCommit_(NULL),
        errorDynamicSQLContainer_(1)

  {}

  virtual ~CRUTestTaskExecutor() {
    delete[] pNumberOfExecutions_;
    delete[] pNumberOfRetries_;
    delete[] pNumberOfFailures_;
    delete[] pAutoCommit_;
    delete pDynamicSQLContainer_;
  }

 public:
  virtual void Init() { SetState(EX_READ_GROUP); }

  virtual void Work();

  void SetNumberOfStatements(int numberOfStatements) { numberOfStatements_ = numberOfStatements; }

  void SetGroupId(int groupId) { groupId_ = groupId; }

  int GetGroupId() { return groupId_; }

  virtual int GetIpcBufferSize() const { return 2000; }

 public:
  // These functions serialize/de-serialize the executor's context
  // for the message communication with the remote server process

  // Used in the main process side
  virtual void StoreRequest(CUOFsIpcMessageTranslator &translator) {
    inherited::StoreRequest(translator);
    StoreData(translator);
  }

  virtual void LoadReply(CUOFsIpcMessageTranslator &translator) {
    inherited::LoadReply(translator);
    LoadData(translator);
  }

  // Used in the remote process side
  virtual void LoadRequest(CUOFsIpcMessageTranslator &translator) {
    inherited::LoadRequest(translator);
    LoadData(translator);
  }

  virtual void StoreReply(CUOFsIpcMessageTranslator &translator) {
    inherited::StoreReply(translator);
    StoreData(translator);
  }

 public:
  // fsm's state definitions
  enum STATES {
    EX_AFTER_COMPILATION_BEFORE_EXECUTION = MAIN_STATES_START,
    EX_READ_GROUP = REMOTE_STATES_START,
    EX_COMPILE_ALL,
    EX_EXECUTE
  };

 private:
  void ReadSqlStatement();
  void ExecuteAllStatements();
  void ExecuteStatement(int i);
  void HandleError(int groupId, int processId, int ordinal, CDMException &e);

  void StoreData(CUOFsIpcMessageTranslator &translator);
  void LoadData(CUOFsIpcMessageTranslator &translator);

 private:
  //-- Prevent copying
  CRUTestTaskExecutor(const CRUTestTaskExecutor &other);
  CRUTestTaskExecutor &operator=(const CRUTestTaskExecutor &other);

 private:
  int numberOfStatements_;
  int groupId_;

 private:
  CRUSQLDynamicStatementContainer *pDynamicSQLContainer_;
  int *pNumberOfExecutions_;
  int *pNumberOfRetries_;
  int *pNumberOfFailures_;
  int *pAutoCommit_;
  CRUSQLDynamicStatementContainer errorDynamicSQLContainer_;
};

#endif
