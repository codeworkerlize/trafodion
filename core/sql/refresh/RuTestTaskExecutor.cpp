
/* -*-C++-*-
******************************************************************************
*
* File:         CRUTestTaskExecutor.cpp
* Description:  Implementation of class CRUTestTaskExecutor
*
*
* Created:      01/09/2001
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuTestTaskExecutor.h"
#include "RuException.h"
#include "dmresultset.h"
#include "RuSQLComposer.h"
#include "uofsIpcMessageTranslator.h"

//--------------------------------------------------------------------------//
//	CRUTestTaskExecutor::Work()
//--------------------------------------------------------------------------//
void CRUTestTaskExecutor::Work() {
  switch (GetState()) {
    case EX_READ_GROUP:  // REMOTE PROCESS
    {
      ReadSqlStatement();

      SetState(EX_COMPILE_ALL);
      break;
    }
    case EX_COMPILE_ALL:  // REMOTE PROCESS
    {
      RUASSERT(NULL != pDynamicSQLContainer_);

      pDynamicSQLContainer_->PrepareSQL();

      SetState(EX_AFTER_COMPILATION_BEFORE_EXECUTION);
      break;
    }
    case EX_AFTER_COMPILATION_BEFORE_EXECUTION:  // MAIN PROCESS
    {
      // A dummy state ,used to syncronized all executors before
      // executing statements
      SetState(EX_EXECUTE);
      break;
    }
    case EX_EXECUTE:  // REMOTE PROCESS
    {
      ExecuteAllStatements();
      SetState(EX_COMPLETE);
      break;
    }
    default:
      RUASSERT(FALSE);
  }
}

//--------------------------------------------------------------------------//
//	CRUTestTaskExecutor::ReadSqlStatement()
//--------------------------------------------------------------------------//
void CRUTestTaskExecutor::ReadSqlStatement() {
  pDynamicSQLContainer_ = new CRUSQLDynamicStatementContainer(numberOfStatements_);

  pNumberOfExecutions_ = new int[numberOfStatements_];
  pNumberOfRetries_ = new int[numberOfStatements_];
  pAutoCommit_ = new int[numberOfStatements_];
  pNumberOfFailures_ = new int[numberOfStatements_];

  CRUSQLDynamicStatementContainer controlDynamicContainer(1);

  CDSString sqlText;

  sqlText = "	SELECT number_of_executions,sql_text,ordinal,number_of_retries,auto_commit ";
  sqlText += "	FROM CAT1.MVSCHM.RefreshControlTable ";
  sqlText += "	WHERE group_id = ";
  sqlText += CRUSQLComposer::ComposeCastExpr("INT UNSIGNED");
  sqlText += "	AND process_id = ";
  sqlText += CRUSQLComposer::ComposeCastExpr("INT UNSIGNED");
  sqlText += "	ORDER BY ordinal;";

  const int kNumber_of_executions = 1;
  const int kSql_text = 2;
  const int kSort_num = 3;
  const int kRetries = 4;
  const int kAutoCommit = 5;

  controlDynamicContainer.SetStatementText(0, sqlText);

  CDMPreparedStatement *pStmt = controlDynamicContainer.GetPreparedStatement(0);

  pStmt->SetInt(1, groupId_);
  pStmt->SetInt(2, GetProcessId() + 1);

  BeginTransaction();

  CDMResultSet *pResult = pStmt->ExecuteQuery();

  int i = 0;
  while (pResult->Next()) {
    RUASSERT(i < numberOfStatements_);

    char buffer[SQL_TEXT_MAX_SIZE];

    pNumberOfExecutions_[i] = pResult->GetInt(kNumber_of_executions);

    pNumberOfRetries_[i] = pResult->GetInt(kRetries);

    pAutoCommit_[i] = pResult->GetInt(kAutoCommit);

    pNumberOfFailures_[i] = 0;

    pResult->GetString(kSql_text, buffer, SQL_TEXT_MAX_SIZE);

    CDSString text(buffer);

    text.TrimLeft();

    pDynamicSQLContainer_->SetStatementText(i, text);

    i++;
  }

  pStmt->Close();

  CommitTransaction();

  // Error Handling
  sqlText = "INSERT INTO RefreshOutputTable VALUES(?,?,?,?,?,?,?)";

  errorDynamicSQLContainer_.SetStatementText(0, sqlText);
}

//--------------------------------------------------------------------------//
//	CRUTestTaskExecutor::ExecuteAllStatements()
//--------------------------------------------------------------------------//
void CRUTestTaskExecutor::ExecuteAllStatements() {
  int i = 0;
  for (int j = 0; j < pNumberOfRetries_[0]; j++) {
    try {
      BeginTransaction();

      BOOL more = TRUE;
      while (more) {
        more = FALSE;
        for (i = 0; i < numberOfStatements_; i++) {
          if (0 < pNumberOfExecutions_[i]) {
            more = TRUE;
            ExecuteStatement(i);
            pNumberOfFailures_[i] = 0;
            if (pAutoCommit_[i] != 0 && TRUE == IsTransactionOpen()) {
              CommitTransaction();
              BeginTransaction();
            }
            pNumberOfExecutions_[i]--;
          }
        }
      }

      CommitTransaction();
    } catch (CDMException &e) {
      HandleError(groupId_, GetProcessId(), i, e);

      if (e.GetErrorCode(0) != -8551) {
        throw e;
      }

      CDMPreparedStatement *pStmt = pDynamicSQLContainer_->GetPreparedStatement(i);

      pStmt->Close();
      continue;
    }
  }
}

//--------------------------------------------------------------------------//
//	CRUTestTaskExecutor::ExecuteStatement()
//--------------------------------------------------------------------------//
void CRUTestTaskExecutor::ExecuteStatement(int i) {
  CDMPreparedStatement *pStmt = pDynamicSQLContainer_->GetPreparedStatement(i);

  if (pDynamicSQLContainer_->GetLastSQL(i)[0] == 'S' || pDynamicSQLContainer_->GetLastSQL(i)[0] == 's') {
    //		Sleep(10);

    CDMResultSet *pResult = pStmt->ExecuteQuery();

    while (pResult->Next()) {
    }

    pStmt->Close();
  } else {
    if (pDynamicSQLContainer_->GetLastSQL(i)[0] == 'R' || pDynamicSQLContainer_->GetLastSQL(i)[0] == 'r') {
      if (TRUE == IsTransactionOpen()) {
        CommitTransaction();
      }

      //			Sleep(100);

      pStmt->ExecuteUpdate();

      pStmt->Close();

      BeginTransaction();
    } else {
      //			Sleep(1);

      pStmt->ExecuteUpdate();

      pStmt->Close();
    }
  }
}

//----------------------------------------------------------//
//	CRUTestTaskExecutor::HandleError()
//----------------------------------------------------------//

void CRUTestTaskExecutor::HandleError(int groupId, int processId, int ordinal, CDMException &e) {
  pNumberOfFailures_[ordinal]++;

  try {
    if (TRUE == IsTransactionOpen()) {
      CommitTransaction();
    }
  } catch (...) {
  }

  BeginTransaction();

  CDMPreparedStatement *pStmt = errorDynamicSQLContainer_.GetPreparedStatement(0);

  int numErrors = e.GetNumErrors();

  for (int index = 0; index < numErrors; index++) {
    char errorMsg[1024];

    pStmt->SetInt(1, groupId);
    pStmt->SetInt(2, processId + 1);
    pStmt->SetInt(3, ordinal + 1);
    pStmt->SetInt(4, pNumberOfFailures_[ordinal]);
    pStmt->SetInt(5, index);
    pStmt->SetInt(6, e.GetErrorCode(index));
    e.GetErrorMsg(index, errorMsg, 1024);
    pStmt->SetString(7, errorMsg);

    pStmt->ExecuteUpdate();
  }

  pStmt->Close();

  CommitTransaction();
}

//----------------------------------------------------------//
//	CRUTestTaskExecutor::StoreData()
//----------------------------------------------------------//

void CRUTestTaskExecutor::StoreData(CUOFsIpcMessageTranslator &translator) {
  inherited::StoreData(translator);

  translator.WriteBlock(&numberOfStatements_, sizeof(int));
  translator.WriteBlock(&groupId_, sizeof(int));
  translator.SetMessageType(CUOFsIpcMessageTranslator::RU_TEST_EXECUTOR);
}

//----------------------------------------------------------//
//	CRUTestTaskExecutor::LoadData()
//----------------------------------------------------------//

void CRUTestTaskExecutor::LoadData(CUOFsIpcMessageTranslator &translator) {
  inherited::LoadData(translator);

  translator.ReadBlock(&numberOfStatements_, sizeof(int));
  translator.ReadBlock(&groupId_, sizeof(int));
}
