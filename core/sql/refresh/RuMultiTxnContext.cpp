
/* -*-C++-*-
******************************************************************************
*
* File:         RuMultiTxnContext.cpp
* Description:  Implementation of class CRUMultiTxnContext.
*
*
* Created:      08/17/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuMultiTxnContext.h"
#include "dmresultset.h"

//--------------------------------------------------------------------------//
//	PUBLIC METHODS
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUMultiTxnContext::ReadRowsFromContextLog()
//  This function fills the stack with context from the UMD
//  table
//--------------------------------------------------------------------------//

void CRUMultiTxnContext::ReadRowsFromContextLog(CDMPreparedStatement *readStmt) {
  RUASSERT(NULL != readStmt);

  try {
    // The rows are sorted by BEGIN_EPOCH asc

    CDMResultSet *pResult = readStmt->ExecuteQuery();

    while (pResult->Next()) {
      const int kEpoch = 1;

      stack_.AddHead(pResult->GetInt(kEpoch));
    }
  } catch (CDSException &ex) {
    ex.SetError(IDS_RU_FETCHCTX_FAILED);
    throw ex;  // Re-throw
  }

  readStmt->Close();
}

//--------------------------------------------------------------------------//
//	CRUMultiTxnContext::GetRowByIndex()
//
// This function return ROW_DOES_NOT_EXIST if no such row exists
//--------------------------------------------------------------------------//

TInt32 CRUMultiTxnContext::GetRowByIndex(int index) {
  RUASSERT(0 <= index);

  if ((stack_.GetCount() <= index)) {
    return ROW_DOES_NOT_EXIST;
  }

  DSListPosition pos = stack_.FindIndex(index);

  return stack_.GetAt(pos);
}
