
/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimTaskExUnit.h
* Description:  Implementation of classes
*               CRUDupElimTaskExUnit and CRUDupElimResolver
*
*
* Created:      04/09/2001
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuDupElimTaskExUnit.h"
#include "RuDupElimConst.h"

//--------------------------------------------------------------------------//
//	Constructor
//--------------------------------------------------------------------------//

CRUDupElimTaskExUnit::CRUDupElimTaskExUnit(const CRUDupElimGlobals &dupElimGlobals,
                                           CRUSQLDynamicStatementContainer &ctrlStmtContainer, int nStmts)
    : dupElimGlobals_(dupElimGlobals), ctrlStmtContainer_(ctrlStmtContainer), stmtContainer_(nStmts) {}

//--------------------------------------------------------------------------//
//	CRUDupElimResolver::ExecuteCQSForceMDAM()
//
//	Execute the CONTROL QUERY SHAPE statement to force the MDAM optimization.
//
//	Since the statements are executed in arkcmp (rather than
//	by the executor), the special syntax parameter must be
//	passed at the execution-time, rather than at compile-time.
//
//--------------------------------------------------------------------------//

void CRUDupElimResolver::ExecuteCQSForceMDAM(LogType logType) {
  CDMPreparedStatement *pStmt;

  if (IUD_LOG == logType) {
    //	CONTROL QUERY SHAPE TSJ
    //	(EXCHANGE (SCAN (TABLE '<T-IUD-log>', MDAM FORCED)), CUT)
    pStmt = GetControlStmtContainer().GetPreparedStatement(CRUDupElimConst::IUD_LOG_FORCE_MDAM_CQS);
  } else {
    //	CONTROL QUERY SHAPE TSJ
    //	(EXCHANGE (SCAN (TABLE '<T-range-log>', MDAM FORCED)), CUT)
    pStmt = GetControlStmtContainer().GetPreparedStatement(CRUDupElimConst::RNG_LOG_FORCE_MDAM_CQS);
  }

  pStmt->ExecuteUpdate(TRUE /* special syntax*/);
  pStmt->Close();
}

//--------------------------------------------------------------------------//
//	CRUDupElimResolver::ExecuteCQSOff()
//--------------------------------------------------------------------------//

void CRUDupElimResolver::ExecuteCQSOff() {
  CDMPreparedStatement *pStmt;

  //	CONTROL QUERY SHAPE OFF
  pStmt = GetControlStmtContainer().GetPreparedStatement(CRUDupElimConst::RESET_MDAM_CQS);

  pStmt->ExecuteUpdate(TRUE /* special syntax*/);
  pStmt->Close();
}

#ifdef _DEBUG
//--------------------------------------------------------------------------//
//	CRUDupElimResolver::DumpStmtStatistics()
//
//	Print the number of invocations of a statement
//--------------------------------------------------------------------------//

void CRUDupElimResolver::DumpStmtStatistics(CDSString &to, CDSString &stmt, int num) const {
  char buf[10];

  sprintf(buf, ": %d", num);

  to += CDSString("\t") + stmt + CDSString(buf) + CDSString(" invocations.\n");
}
#endif
