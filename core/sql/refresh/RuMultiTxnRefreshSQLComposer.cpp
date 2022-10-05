
/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLMultiTxnRefreshComposer.cpp
* Description:  Implementation of class RuSQLMultiTxnRefreshComposer
*
*
* Created:      08/17/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuSQLDynamicStatementContainer.h"
#include "RuMultiTxnRefreshSQLComposer.h"
#include "RuTbl.h"

//--------------------------------------------------------------------------//
//	Constructor
//--------------------------------------------------------------------------//

CRUMultiTxnRefreshSQLComposer::CRUMultiTxnRefreshSQLComposer(CRURefreshTask *pTask) : CRURefreshSQLComposer(pTask) {}

//--------------------------------------------------------------------------//
//	CRUMultiTxnRefreshSQLComposer::ComposeRefresh()
//--------------------------------------------------------------------------//

void CRUMultiTxnRefreshSQLComposer::ComposeRefresh(int phase, BOOL catchup) {
  StartInternalRefresh();

  sql_ += " FROM SINGLEDELTA ";

  CRUDeltaDef *pDdef = GetRefreshTask().GetDeltaDefList().GetAt(0);

  CDSString epochParameter(CRUSQLDynamicStatementContainer::COMPILED_PARAM_TOKEN);

  AddDeltaDefClause(pDdef, epochParameter, epochParameter);

  AddNRowsClause(phase, catchup);

  if (GetRefreshTask().GetMVList().GetCount() > 1) {
    AddPipeLineClause();
  }
}

//--------------------------------------------------------------------------//
//	CRUMultiTxnRefreshSQLComposer::AddNRowsClause()
//--------------------------------------------------------------------------//

void CRUMultiTxnRefreshSQLComposer::AddNRowsClause(int phase, BOOL catchup) {
  sql_ += "\n\t COMMIT EACH ";
  sql_ += TInt32ToStr(GetRootMV().GetCommitNRows());

  sql_ += " PHASE ";
  sql_ += TInt32ToStr(phase);

  if (catchup) {
    sql_ += " CATCHUP ";
    sql_ += CRUSQLDynamicStatementContainer::COMPILED_PARAM_TOKEN;
    sql_ += " ";
  }
}

//--------------------------------------------------------------------------//
//	CRUMultiTxnRefreshSQLComposer::ComposeReadContextLog()
//--------------------------------------------------------------------------//

void CRUMultiTxnRefreshSQLComposer::ComposeReadContextLog() {
  CDSString epochColName(ComposeQuotedColName(CRUTbl::logCrtlColPrefix, "EPOCH"));

  sql_ = "SELECT ";
  sql_ += epochColName;
  sql_ += " FROM TABLE ";
  sql_ += GetContextLogName();
  sql_ += " ORDER BY ";
  sql_ += epochColName;
}

//--------------------------------------------------------------------------//
//	CRUMultiTxnRefreshSQLComposer::ComposeCQSForIRPhase1()
//
//			  JOIN
//          /      \
//		  CUT	JOIN
//			   /	\
//           JOIN   UPDATE MV(sub-tree)
//			/    \
//        GRBY   SCAN
//         |      MV
//     DELTA CALC
//     (sub-tree)
//--------------------------------------------------------------------------//

void CRUMultiTxnRefreshSQLComposer::ComposeCQSForIRPhase1() {
  RUASSERT(NULL != GetRootMV().GetMVForceOption());

  const CRUMVForceOptions &forceOption = *GetRootMV().GetMVForceOption();

  sql_ = "CONTROL QUERY SHAPE  ";

  sql_ += " JOIN (CUT, ";
  inherited::ComposeQueryShape();
  sql_ += " ); ";
}
