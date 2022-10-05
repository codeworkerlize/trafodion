
/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLStaticStatementContainer.cpp
* Description:  Implementation of class CRUSQLStaticStatementContainer
*
*
* Created:      09/08/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuSQLStaticStatementContainer.h"
#include "dmconnection.h"
#include "RuException.h"

//--------------------------------------------------------------------------//
//	Constructor and destructor
//--------------------------------------------------------------------------//

CRUSQLStaticStatementContainer::CRUSQLStaticStatementContainer(short nStmts)
    : CRUSQLStatementContainer(nStmts), pStaticStmtVec_(new CRUSQLStaticStatementContainer::StaticStmt[nStmts]) {}

CRUSQLStaticStatementContainer::~CRUSQLStaticStatementContainer() { delete[] pStaticStmtVec_; }

//--------------------------------------------------------------------------//
//	CRUSQLStaticStatementContainer::GetPreparedStatement()
//--------------------------------------------------------------------------//

CDMPreparedStatement *CRUSQLStaticStatementContainer::StaticStmt::GetPreparedStatement(BOOL DeleteUsedStmt) {
  RUASSERT(pInfo_ != NULL)

  if (NULL == inherited::GetPreparedStatement()) {
    CDMConnection *pConnect = GetConnection();
    pConnect->SetAllowSpecialSyntax(TRUE);

    // just loading the module
    CDMPreparedStatement *pPrepStmt = pConnect->PrepareStatement(schemaVersion_, pInfo_,
                                                                 // The connection does NOT own the statement
                                                                 CDMConnection::eItemIsOwned);

    inherited::SetPreparedStatement(pPrepStmt, DeleteUsedStmt);
  }
  return inherited::GetPreparedStatement();
}
