

#include "sqlci/SqlciEnv.h"

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include "cli/SQLCLIdev.h"
#include "exp/ExpErrorEnums.h"
#include "sqlci/InputStmt.h"
#include "sqlci/SqlciDefs.h"
#include "sqlci/SqlciError.h"
#include "sqlci/SqlciParser.h"
#include "sqlci/SqlciStats.h"
#include "sqlci/SqlciStmts.h"
#include "sqlci/sqlcmd.h"

NAHeap sqlci_Heap((char *)"temp heap", NAMemory::DERIVED_FROM_SYS_HEAP);
ComDiagsArea sqlci_DA(&sqlci_Heap);

ComDiagsArea &SqlciEnv::diagsArea() { return sqlci_DA; }

extern SqlciEnv *global_sqlci_env;  // Global sqlci_env for break key handling purposes.

#define MXCI_DONOTISSUE_ERRMSGS -1

static char brkMessage[] = "Break.";
CRITICAL_SECTION g_CriticalSection;
CRITICAL_SECTION g_InterruptCriticalSection;

BOOL CtrlHandler(DWORD ctrlType) {
  if (Sqlci_PutbackChar == LOOK_FOR_BREAK)  // see InputStmt.cpp
    Sqlci_PutbackChar = FOUND_A_BREAK;
  breakReceived = 1;
  sqlci_DA << DgSqlCode(SQLCI_BREAK_RECEIVED, DgSqlCode::WARNING_);
  switch (ctrlType) {
    case CTRL_C_EVENT:
    case CTRL_BREAK_EVENT:
      if (TryEnterCriticalSection(&g_CriticalSection)) {
        int eCode = SQL_EXEC_Cancel(0);
        if (eCode == 0) {
          sqlci_DA << DgSqlCode(-SQL_Canceled, DgSqlCode::WARNING_);
          LeaveCriticalSection(&g_CriticalSection);

        } else {  // retry
          switch (-eCode) {
            case CLI_CANCEL_REJECTED: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_REJECTED, DgSqlCode::WARNING_);
              // sqlci_DA << DgSqlCode(SQLCI_COMMAND_NOT_CANCELLED, DgSqlCode::WARNING_);
              SqlciEnv s;
              sqlci_DA.clear();
              // breakReceived = 0;
              LeaveCriticalSection(&g_CriticalSection);

            } break;
            default: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_ERROR, DgSqlCode::WARNING_);
              SqlciEnv s;
              sqlci_DA.clear();
              LeaveCriticalSection(&g_CriticalSection);
            } break;
          }
        }
      }
      return TRUE;
      break;

    case CTRL_CLOSE_EVENT:

    case CTRL_LOGOFF_EVENT:

    case CTRL_SHUTDOWN_EVENT:

      if (TryEnterCriticalSection(&g_CriticalSection)) {
        int eCode = SQL_EXEC_Cancel(0);
        if (eCode == 0) {
          sqlci_DA << DgSqlCode(-SQL_Canceled, DgSqlCode::WARNING_);
          LeaveCriticalSection(&g_CriticalSection);

        } else {  // retry
          switch (-eCode) {
            case CLI_CANCEL_REJECTED: {
              SqlciEnv s;
              sqlci_DA.clear();
              LeaveCriticalSection(&g_CriticalSection);

            } break;
            default: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_ERROR, DgSqlCode::WARNING_);
              SqlciEnv s;
              sqlci_DA.clear();
              LeaveCriticalSection(&g_CriticalSection);
            } break;
          }
        }
      }
      return TRUE;
      break;

    default:
      return FALSE;
  }
}

SqlciEnv::SqlciEnv(short serv_type, NABoolean macl_rw_flag) {
  ole_server = serv_type;
  logfile = new Logfile;
  sqlci_stmts = new SqlciStmts(50);
  sqlci_stats = new SqlciStats();
  terminal_charset_ = CharInfo::UTF8;
  iso_mapping_charset_ = CharInfo::ISO88591;
  default_charset_ = CharInfo::ISO88591;
  infer_charset_ = FALSE;
  lastDmlStmtStatsType_ = SQLCLIDEV_NO_STATS;
  lastExecutedStmt_ = nullptr;
  statsStmt_ = nullptr;
  lastAllocatedStmt_ = nullptr;

  defaultCatalog_ = nullptr;
  defaultSchema_ = nullptr;

  doneWithPrologue_ = FALSE;
  noBanner_ = FALSE;

  setListCount(MAX_LISTCOUNT);
  resetSpecialError();
  defaultCatAndSch_ = nullptr;

  userNameFromCommandLine_ = "";
  tenantNameFromCommandLine_ = "";

  constructorFlag_ = FALSE;

  setMode(SqlciEnv::SQL_);  // Set the mode initially to be SQL
  eol_seen_on_input = -1;
  obey_file = 0;
  prev_err_flush_input = 0;

  logCommands_ = FALSE;
  deallocateStmt_ = FALSE;

  // Note that interactive_session is overwritten in SqlciEnv::run(infile) ...
  // Executing on NT
  if ((int)cin.tellg() == EOF)
    interactive_session = -1;  // Std. input is terminal keyboard
  else
    interactive_session = 0;
  cin.clear();  // Always clear() bad results from any tellg()

  if (getenv("QA_TEST_WITH_EXEC")) interactive_session = -1;

  prepareOnly_ = nullptr;
  executeOnly_ = nullptr;
}

SqlciEnv::~SqlciEnv() {
  delete logfile;
  delete sqlci_stmts;
  delete sqlci_stats;
  sqlci_stmts = 0;
}

void SqlciEnv::autoCommit() {
  SqlCmd::executeQuery("SET TRANSACTION AUTOCOMMIT ON;", this);
  // Should an error occur,  exit MXCI.
  if (!specialError_) {
    char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
    if (!noexit) exit(EXIT_FAILURE);
  }
}

void SqlciEnv::run() {
  int retval;
  do {
    retval = executeCommands();
  } while (retval != 0);
}

int SqlciEnv::executeCommands() {
  int retval = 0;
  int ignore_toggle = 0;
  SqlciNode *sqlci_node = 0;
  InputStmt *input_stmt = nullptr;

  NABoolean inputPassedIn = (input_stmt ? TRUE : FALSE);

  while (!retval) {
    input_stmt = new InputStmt(this);

    auto read_error = input_stmt->readStmt(nullptr);

    prev_err_flush_input = 0;

    if (cin.eof() || read_error == -99) {
      Sleep(50);  // milliseconds
      if (!input_stmt->isEmpty()) {
        if (!isInteractiveSession()) input_stmt->display((UInt16)0);
        input_stmt->logStmt();
        input_stmt->syntaxErrorOnEof();
        sqlci_DA.clear();
      }
      char command[10];
      strcpy(command, ">>exit;");

      sqlci_parser(&command[2], &command[2], &sqlci_node, this);

      if (sqlci_node) {
        retval = sqlci_node->process(this);
        delete sqlci_node;
        sqlci_node = nullptr;
      }
    } else {
      if (!input_stmt->sectionMatches()) {
        {
          getSqlciStmts()->add(input_stmt);
          if (!read_error) {
            retval = sqlci_parser(input_stmt->getPackedString(), input_stmt->getPackedString(), &sqlci_node, this);
            if (sqlci_node) {
              retval = sqlci_node->process(this);
              delete sqlci_node;
              sqlci_node = nullptr;

              if (retval == SQL_Canceled) retval = 0;
            } else {
              // pure MXCI synatax error. Reset retval
              retval = 0;
            }
          }
          if (retval > 0) {
            if (!eol_seen_on_input) {
              prev_err_flush_input = -1;
            }
            retval = 0;
          }

        }  // else
      }    // if
    }      // else

    sqlci_DA.clear();

    if (!input_stmt->isInHistoryList()) delete input_stmt;

    if (inputPassedIn) retval = 1;
  }
  if (retval == SQL_Canceled)
    return SQL_Canceled;
  else
    return 0;
}
