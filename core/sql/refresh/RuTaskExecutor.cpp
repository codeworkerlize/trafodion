
/* -*-C++-*-
******************************************************************************
*
* File:         RuTaskExecutor.cpp
* Description:  Implementation of class CRURefreshTaskExecutor.
*
* Created:      05/14/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "uofsException.h"
#include "uofsIpcMessageTranslator.h"

#include "RuTaskExecutor.h"
#include "RuGlobals.h"
#include "RuJournal.h"
#include "RuTask.h"
#include "RuOptions.h"
#include "RuSQLStatementContainer.h"
#include "uosessioninfo.h"

//--------------------------------------------------------------------------//
//	Constructor and destructor
//--------------------------------------------------------------------------//

CRUTaskExecutor::CRUTaskExecutor(CRUTask *pTask)
    : pParentTask_(pTask),
      state_(EX_START),
      hasWork_(TRUE),
      transIdx_(-1),
      startTimer_(0),
      endTimer_(0),
      processId_(-1),
      pIpcBuffer_(NULL),
      pIpcTranslator_(NULL) {}

CRUTaskExecutor::~CRUTaskExecutor() {
  if (NULL != pIpcBuffer_) {
    delete[] pIpcBuffer_;
  }

  if (NULL != pIpcTranslator_) {
    delete pIpcTranslator_;
  }
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::Init()
//
//	Generic executor initialization
//--------------------------------------------------------------------------//

void CRUTaskExecutor::Init() { LOGTIME("Starting to execute Task " + GetParentTask()->GetTaskName() + " \n"); }

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::BeginTransaction()
//
//	Start a new transaction (through the UOFS trans manager).
//--------------------------------------------------------------------------//

void CRUTaskExecutor::BeginTransaction() {
  CUOFsTransManager &transManager = CRUGlobals::GetInstance()->GetTransactionManager();

  transManager.BeginTrans();

  SetTransIdx(transManager.GetCurrentTrans());
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::CommitTransaction()
//
//	Start a new transaction (through the UOFS trans manager).
//--------------------------------------------------------------------------//

void CRUTaskExecutor::CommitTransaction() {
  CUOFsTransManager &transManager = CRUGlobals::GetInstance()->GetTransactionManager();

  SwitchTransContextBack();

  transManager.CommitTrans();

  SetTransIdx(-1);
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::SwitchTransContextBack()
//
//	Called by: CRUExecController
//
//	Switch the main(arkcmp) process back to the transaction
//	context of the execution's previous step. No work is
//	required if the process runs under the same transaction.
//
//	This feature is required in the parallel execution
//  scenario, when the main process operates a number of
//	task processes.
//
//--------------------------------------------------------------------------//

void CRUTaskExecutor::SwitchTransContextBack() {
  if (-1 == GetTransIdx()) {
    CRUGlobals::GetInstance()->GetTransactionManager().LeaveTransaction();
    return;
  }

  CRUGlobals::GetInstance()->GetTransactionManager().SetCurrentTrans(GetTransIdx());
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::RollbackTransaction()
//
//	In the case of failure, if the task executor has been
//	executing under a transaction - abort the transaction.
//
//	Caller: CRUExecController
//--------------------------------------------------------------------------//

void CRUTaskExecutor::RollbackTransaction() {
  try {
    CUOFsTransManager &transManager = CRUGlobals::GetInstance()->GetTransactionManager();

    SwitchTransContextBack();

    transManager.AbortTrans();

    SetTransIdx(-1);
  } catch (...) {
    SetTransIdx(-1);
    // If the system has already aborted the transaction
    // by itself - do nothing
  }
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::IsTransactionOpen()
//--------------------------------------------------------------------------//

BOOL CRUTaskExecutor::IsTransactionOpen() {
  CUOFsTransManager &transManager = CRUGlobals::GetInstance()->GetTransactionManager();

  return transManager.IsTransactionOpen();
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::LeaveTransaction()
//
//	Leave the transaction and enter a transaction nil state
//	(no current transaction).
//
//--------------------------------------------------------------------------//

void CRUTaskExecutor::LeaveTransaction() {
  CUOFsTransManager &transManager = CRUGlobals::GetInstance()->GetTransactionManager();

  transManager.LeaveTransaction();
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::StoreData()
//--------------------------------------------------------------------------//
void CRUTaskExecutor::StoreData(CUOFsIpcMessageTranslator &translator) {
  translator.WriteBlock(&state_, sizeof(int));
  translator.WriteBlock(&processId_, sizeof(int));
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::LoadData()
//--------------------------------------------------------------------------//
void CRUTaskExecutor::LoadData(CUOFsIpcMessageTranslator &translator) {
  translator.ReadBlock(&state_, sizeof(int));
  translator.ReadBlock(&processId_, sizeof(int));
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::AllocateBuffer()
//
//	The initial IPC buffer allocation, with the size associated
//	with the type of task.
//--------------------------------------------------------------------------//

void CRUTaskExecutor::AllocateBuffer() {
  RUASSERT(NULL == pIpcBuffer_ && NULL == pIpcTranslator_);

  // The value depends on the type of task (pure virtual)
  int bufsize = GetIpcBufferSize();

#ifdef _DEBUG
  // Force here an artificially small buffer size for testing purposes
  enum { MIN_BUFFER_SIZE = 50 };

  CRUOptions &options = CRUGlobals::GetInstance()->GetOptions();

  CRUOptions::DebugOption *pDO = options.FindDebugOption(CRUGlobals::SHRINK_IPC_BUFFER, "");

  if (NULL != pDO) {
    bufsize = MIN_BUFFER_SIZE;
  }
#endif

  CreateBufferAndTranslator(bufsize);
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::ReAllocateBuffer()
//
//	If the IPC buffer is not big enough, resize it by the constant factor.
//
//--------------------------------------------------------------------------//

void CRUTaskExecutor::ReAllocateBuffer(int factor) {
  RUASSERT(NULL != pIpcBuffer_ && NULL != pIpcTranslator_);

  int bufsize = pIpcTranslator_->GetBufferSize();

  bufsize *= factor;
  if (CUOFsIpcMessageTranslator::MaxMsgSize < bufsize) {
    // This is the maximum we can give ...
    bufsize = CUOFsIpcMessageTranslator::MaxMsgSize;
  }

  delete[] pIpcBuffer_;

  delete pIpcTranslator_;

  CreateBufferAndTranslator(bufsize);
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::ExecuteStatement()
//
//	A standard mechanism for the IUD statement execution by the utility.
//
//	The callee passes the error code and one (optional) string
//	argument for the diagnostics, in the case the statement fails.
//
//	In the DEBUG configuration, the diagnostics will contain the
//	failed command's syntax (unless the user has given his own argument).
//
//--------------------------------------------------------------------------//

void CRUTaskExecutor::ExecuteStatement(CDMPreparedStatement &stmt, int errorCode, const char *errorArgument,
                                       BOOL needRowCount, BOOL isQuery) {
  //++ MV - Eran
  // Adding retry mechanism
  short retry_delay = 1000;  // milliseconds.
  for (int retry = 0; retry < 2; retry++) {
    retry_delay = retry_delay * (retry + 1);

    try {
      if (!isQuery) {
        CUOSessionInfo sessionInfo(TRUE, FALSE, FALSE);
        stmt.ExecuteUpdate(TRUE /*special syntax*/, needRowCount /* Obtain row count */,
                           sessionInfo.BelongsToServicesRole());

        stmt.Close();
      } else {
        stmt.ExecuteQuery();
      }
      break;  // no retry needed, exit retry loop
    } catch (CDSException &ex) {
      // The purpose of this method call is to detect compilation errors
      // that are originated from a temporary lock on the OBJECTS table
      // (error 73) and execute retry. Due to the catalog error mechanism
      // the projected error code is currently 1100.
      if (ex.IsErrorFoundAndRetryNeeded(-1100, retry_delay)) {
        // error was found try again
        continue;
      }

#ifdef _DEBUG
      if (NULL == errorArgument) {
        RUASSERT(NULL != stmt.GetSqlString());
        HandleSqlError(ex, errorCode, stmt.GetSqlString());
        return;
      }
#endif

      // Standard error handling ...
      HandleSqlError(ex, errorCode, errorArgument);
    }
  }
}

//--------------------------------------------------------------------------//
//	PROTECTED AND PRIVATE AREA
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::HandleSqlError()
//
//	The standard mechanism of error handling (can be overridden
//	by the child classes): extend the diagnostics by the user-given
//	error code and (optionally) a string argument.
//
//--------------------------------------------------------------------------//
void CRUTaskExecutor::HandleSqlError(CDSException &ex, int errorCode, const char *errorArgument) {
  ex.SetError(errorCode);

  if (NULL != errorArgument) {
    ex.AddArgument(CDSString(errorArgument));
  }

  throw ex;  // Re-throw
}

//--------------------------------------------------------------------------//
//	CRUTaskExecutor::CreateBufferAndTranslator()
//--------------------------------------------------------------------------//

void CRUTaskExecutor::CreateBufferAndTranslator(int bufsize) {
  pIpcBuffer_ = new char[bufsize];

  pIpcTranslator_ = new CUOFsIpcMessageTranslator(pIpcBuffer_, bufsize);
}
