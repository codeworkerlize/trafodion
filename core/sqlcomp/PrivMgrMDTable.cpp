//*****************************************************************************

//*****************************************************************************

#include "PrivMgrMDTable.h"

#include <cstdio>
#include <string>

#include "arkcmp/CmpContext.h"
#include "cli/sqlcli.h"
#include "comexe/ComQueue.h"
#include "common/CmpCommon.h"
#include "common/ComSmallDefs.h"
#include "executor/ExExeUtilCli.h"
#include "export/ComDiags.h"
#include "sqlcomp/PrivMgrMD.h"

PrivMgrRowInfo::PrivMgrRowInfo(const PrivMgrRowInfo &other) {
  grantorID_ = other.grantorID_;
  grantorName_ = other.grantorName_;
  granteeID_ = other.granteeID_;
  granteeName_ = other.granteeName_;
  columnOrdinal_ = other.columnOrdinal_;
  privsBitmap_ = other.privsBitmap_;
  grantableBitmap_ = other.grantorID_;
}

// *****************************************************************************
//    PrivMgrMDRow methods
// *****************************************************************************
// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDRow::PrivMgrMDRow                                      *
// *                                                                           *
// *    This is the constructor for the abstract base class PrivMgrMDRow.  A   *
// *  fully qualified table name is required for construction.  The validity   *
// *  of the name is not verified at construction.                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <myTableName>                   const std::string &             In       *
// *    is the fully qualified name of the table (i.e.                         *
// *    TRAFODION.PRIVMGR_MD.tablename)                                        *
// *                                                                           *
// *****************************************************************************
PrivMgrMDRow::PrivMgrMDRow(std::string myTableName, PrivMgrTableEnum myTableEnum) : myTableName_(myTableName) {}

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------

PrivMgrMDRow::PrivMgrMDRow(const PrivMgrMDRow &other) { myTableName_ = other.myTableName_; }

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------
PrivMgrMDRow::~PrivMgrMDRow() {}

// *****************************************************************************
//    PrivMgrMDTable methods
// *****************************************************************************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::PrivMgrMDTable                                  *
// *                                                                           *
// *    This is the constructor for the abstract base class PrivMgrMDTable.    *
// *  A fully qualified table name is required for construction.  The validity *
// *  of the name is not verified at construction.  A pointer to the           *
// *  ComDiagsArea is also required.                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <myTableName>                   const std::string &             In       *
// *    is the fully qualified name of the table (i.e.                         *
// *    TRAFODION.PRIVMGR_MD.tablename)                                        *
// *                                                                           *
// *  <pDiags>                        ComDiagsArea *                  In       *
// *    is a pointer to the ComDiagsArea to be used for error reporting.       *
// *                                                                           *
// *****************************************************************************
PrivMgrMDTable::PrivMgrMDTable(const std::string &tableName, PrivMgrTableEnum myTableEnum, ComDiagsArea *pDiags)
    : tableName_(tableName),
      pDiags_(pDiags)

{
  if (pDiags == NULL) pDiags = CmpCommon::diags();
}
//******************** End of PrivMgrMDTable::PrivMgrMDTable *******************

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgrMDTable::PrivMgrMDTable(const PrivMgrMDTable &other)

{
  tableName_ = other.tableName_;
  pDiags_ = other.pDiags_;
}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------

PrivMgrMDTable::~PrivMgrMDTable() {}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::CLIFetch                                        *
// *                                                                           *
// *    This method calls the CLI to fetch a row from a table.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface &               In       *
// *    is the handle to the CLI interface.                                    *
// *                                                                           *
// *  <SQLStatement>                  const std::string &             In       *
// *    is the SQL statement to be executed.  Note, it can be any SQL          *
// *  statement, but unless data is being fetch the simpler CLIImmediate       *
// *  can be used instead.                                                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD,                                                             *
// *  STATUS_WARNING: Row read successfully.                                   *
// * STATUS_NOTFOUND: Statement executed successfully, but there were no rows. *
// *               *: Read failed. A CLI error is put into the diags area.     *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::CLIFetch(ExeCliInterface &cliInterface, const std::string &SQLStatement)

{
  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchRowsPrologue(SQLStatement.c_str(), true /*no exec*/);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find any rows
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  if (cliRC > 0) return STATUS_WARNING;

  return STATUS_GOOD;
}
//*********************** End of PrivMgrMDTable::CLIFetch **********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::CLIImmediate                                    *
// *                                                                           *
// *    This method calls the CLI to execute a SQL statement.                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <SQLStatement>                  const std::string &             In       *
// *    is the SQL statement to be executed.  Note, it can be any SQL          *
// *  statement, but if you need to access the data that was read, you need    *
// *  to use CLIFetch and provide a CLI Interface.                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: Statement executed successfully.                            *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::CLIImmediate(const std::string &SQLStatement) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  int32_t cliRC = cliInterface.executeImmediate(SQLStatement.c_str());

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }
  return STATUS_GOOD;
}
//********************* End of PrivMgrMDTable::CLIImmediate ********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::deleteWhere                                     *
// *                                                                           *
// *    This method deletes rows in table based on a WHERE clause.             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <whereClause>                  const std::string &              In       *
// *    is the WHERE clause (including the keyword WHERE).                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: Statement executed successfully.                            *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::deleteWhere(const std::string &whereClause) {
  std::string deleteStmt("DELETE FROM ");

  deleteStmt += tableName_;
  deleteStmt += " ";
  deleteStmt += whereClause;

  return CLIImmediate(deleteStmt);
}
//********************* End of PrivMgrMDTable::deleteWhere *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::executeFetchAll                                 *
// *                                                                           *
// *    This method calls the CLI to fetch rows from a table.                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface &              In        *
// *    is the interface object to the CLI. The Queue is allocated             *
// *    from its heap, so it cannot go out of scope before the Queue does      *
// *                                                                           *
// *  <SQLStatement>                  const std::string &             In       *
// *    is the SQL statement to be executed.                                   *
// *                                                                           *
// *  <queue>                         Queue * &                       Out      *
// *    passes back a pointer to the queue containing the rows.                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *     STATUS_GOOD: Row(s) read successfully.                                *
// * STATUS_NOTFOUND: Statement executed successfully, but there were no rows. *
// *               *: Read failed. A CLI error is put into the diags area.     *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::executeFetchAll(ExeCliInterface &cliInterface, const std::string &SQLStatement,
                                           Queue *&queue)

{
  queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = ((pDiags_ != NULL) ? pDiags_->mark() : -1);

  int32_t cliRC = cliInterface.fetchAllRows(queue, (char *)SQLStatement.c_str(), 0, false, false, true);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100 && diagsMark != -1)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  return STATUS_GOOD;
}
//******************* End of PrivMgrMDTable::executeFetchAll *******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::selectCountWhere                                *
// *                                                                           *
// *                                                                           *
// *    This method returns the number of rows in table that match the         *
// * criteria in a WHERE clause.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <whereClause>                  const std::string &              In       *
// *    is the WHERE clause (including the keyword WHERE).                     *
// *                                                                           *
// *  <whereClause>                  int64_t &                        Out      *
// *    passes back the number of rows read.                                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: Statement executed successfully, valid row count returned.  *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::selectCountWhere(const std::string &whereClause, int64_t &rowCount)

{
  rowCount = 0;

  std::string selectStmt("SELECT COUNT(*) FROM  ");

  selectStmt += tableName_;
  selectStmt += " ";
  selectStmt += whereClause;

  int32_t length = 0;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  int32_t cliRC = cliInterface.executeImmediate(selectStmt.c_str(), (char *)&rowCount, &length, FALSE);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return STATUS_ERROR;
  }

  return STATUS_GOOD;
}
//****************** End of PrivMgrMDTable::selectCountWhere *******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::update                                          *
// *                                                                           *
// *    This method updates rows in table based on a SET clause.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <setClause>                    const std::string &              In       *
// *    is the SET clause (including the keyword SET).                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: Statement executed successfully.                            *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::update(const std::string &setClause) {
  std::string updateStmt("UPDATE ");

  updateStmt += tableName_;
  updateStmt += " ";
  updateStmt += setClause;

  // TODO: support a WHERE clause?

  return CLIImmediate(updateStmt);
}
//************************ End of PrivMgrMDTable::update ***********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrMDTable::updateWhere                                     *
// *                                                                           *
// *    This method updates rows in table based on a SET an d WHERE clause.    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <setClause>                    const std::string &              In       *
// *    is the SET clause (including the keyword SET).                         *
// *                                                                           *
// *  <whereClause>                  const std::string &              In       *
// *    is the WHERE clause (including the keyword WHERE).                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// * STATUS_GOOD: Statement executed successfully.                             *
// * STATUS_NOTFOUND: No row found that match WHERE clause.                    *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrMDTable::updateWhere(const std::string &setClause, const std::string &whereClause,
                                       int64_t &rowCount) {
  std::string updateStmt("UPDATE ");

  updateStmt += tableName_;
  updateStmt += " ";
  updateStmt += setClause;
  updateStmt += " ";
  updateStmt += whereClause;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  int32_t cliRC = cliInterface.executeImmediate(updateStmt.c_str(), NULL, NULL, TRUE, &rowCount);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (rowCount == 0) {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  return STATUS_GOOD;
}
//********************* End of PrivMgrMDTable::updateWhere *********************
