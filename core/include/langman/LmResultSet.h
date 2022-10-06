
/* -*-C++-*-
**********************************************************************
*
* File:         LmResultSet.h
* Description:  Container base class for result set data.
*
* Created:      06/14/2005
* Language:     C++
*
**********************************************************************
*/

#ifndef LMRESULTSET_H
#define LMRESULTSET_H

#include "cli/sqlcli.h"
#include "common/ComSmallDefs.h"
#include "langman/LmCommon.h"

class LmParameter;
//////////////////////////////////////////////////////////////////////
//
// Contents
//
//////////////////////////////////////////////////////////////////////
class LmResultSet;

//////////////////////////////////////////////////////////////////////
//
// LmResultSet
//
//////////////////////////////////////////////////////////////////////

class SQLLM_LIB_FUNC LmResultSet : public NABasicObject {
 public:
  // Gets the pointer to the result set's SQLSTMT_ID CLI structure.
  SQLSTMT_ID *getStmtID() const { return stmtID_; }

  NABoolean isCliStmtAvailable() { return stmtID_ != NULL; }

  // Gets the CLI Context Handle containing the result set cursor
  SQLCTX_HANDLE getCtxHandle() const { return ctxHandle_; }

  // Destructor
  virtual ~LmResultSet(){};

  virtual NABoolean moreSpecialRows() { return 0; }

  // Gets the CLI statement status returned by JDBC
  virtual NABoolean isCLIStmtClosed() = 0;

  virtual char *getProxySyntax() = 0;

  // Gets a row from JDBC. It can not get a row from CLI.
  virtual int fetchSpecialRows(void *, LmParameter *, ComUInt32, ComDiagsArea &, ComDiagsArea *) = 0;

 protected:
  // Constructor
  LmResultSet() : ctxHandle_(0), stmtID_(NULL) {}

  // Mutator methods
  void setStmtID(SQLSTMT_ID *stmtID) { stmtID_ = stmtID; }
  void setCtxHandle(SQLCTX_HANDLE ctxHndl) { ctxHandle_ = ctxHndl; }

 private:
  SQLCTX_HANDLE ctxHandle_;  // CLI Context Handle containing the result set cursor
  SQLSTMT_ID *stmtID_;       // Pointer to result set's SQLSTMT_ID CLI structure.
};

#endif
