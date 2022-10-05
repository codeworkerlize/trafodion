
/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLStatementContainer.cpp
* Description:  Implementation of class CRUSQLStatementContainer
*
*
* Created:      09/08/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuSQLStatementContainer.h"

#include "dmconnection.h"
#include "RuException.h"
#include "uofsIpcMessageTranslator.h"

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::StoreData()
//--------------------------------------------------------------------------//
void CRUSQLStatementContainer::StoreData(CUOFsIpcMessageTranslator &translator) {
  short size = GetNumOfStmt();

  translator.WriteBlock(&size, sizeof(short));

  for (int i = 0; i < GetNumOfStmt(); i++) {
    GetStmt(i).StoreData(translator);
  }
}

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::LoadData()
//--------------------------------------------------------------------------//
void CRUSQLStatementContainer::LoadData(CUOFsIpcMessageTranslator &translator) {
  short size;

  translator.ReadBlock(&size, sizeof(short));

  RUASSERT(size <= GetNumOfStmt());

  for (int i = 0; i < GetNumOfStmt(); i++) {
    GetStmt(i).LoadData(translator);
  }
}

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::Stmt::StoreData()
//--------------------------------------------------------------------------//
void CRUSQLStatementContainer::Stmt::StoreData(CUOFsIpcMessageTranslator &translator) {
  translator.WriteBlock(&executionCounter_, sizeof(int));
}

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::Stmt::LoadData()
//--------------------------------------------------------------------------//
void CRUSQLStatementContainer::Stmt::LoadData(CUOFsIpcMessageTranslator &translator) {
  translator.ReadBlock(&executionCounter_, sizeof(int));
}

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::Stmt::ExecuteQuery()
//--------------------------------------------------------------------------//
int CRUSQLStatementContainer::Stmt::ExecuteUpdate() {
  executionCounter_++;
  return GetPreparedStatement()->ExecuteUpdate();
}

//--------------------------------------------------------------------------//
//	CRUSQLStatementContainer::Stmt::ExecuteQuery()
//--------------------------------------------------------------------------//
CDMResultSet *CRUSQLStatementContainer::Stmt::ExecuteQuery() {
  executionCounter_++;
  return GetPreparedStatement()->ExecuteQuery();
}
