/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
//
**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SqlciEnv.C
 * RCS:          $Id: SqlciEnv.cpp,v 1.2 2007/10/17 00:13:53  Exp $
 * Description:
 *
 * Created:      4/15/95
 * Modified:     $ $Date: 2007/10/17 00:13:53 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
7 *
 *
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS
#include "parser/SqlParserGlobalsCmn.h"

#include "common/Platform.h"

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include "SqlciEnv.h"
#include "SqlciError.h"
#include "SqlciNode.h"
#include "SqlciParser.h"
#include "SqlciCmd.h"
#include "cli/sql_id.h"
#include "SqlciStats.h"
#include "sqlcmd.h"
#include "cli/SQLCLIdev.h"
#include "InputStmt.h"
#include "common/CmpCommon.h"
#include "export/ComDiags.h"
#include "copyright.h"
#include "eh/EHException.h"
#include "ErrorMessage.h"
#include "exp/ExpError.h"
#include "GetErrorMessage.h"
#include "common/Int64.h"
#include "NAError.h"
#include "ShowSchema.h"
#include "common/str.h"
#include "common/BaseTypes.h"
#include "common/ComSchemaName.h"
#include "arkcmp/CmpContext.h"

#include "sqlcomp/DefaultConstants.h"
#include "common/ComRtUtils.h"
#include "common/NLSConversion.h"
#include "qmscommon/QRLogger.h"
#include "cli/Globals.h"

Int32 total_opens = 0;
Int32 total_closes = 0;

NAHeap sqlci_Heap((char *)"temp heap", NAMemory::DERIVED_FROM_SYS_HEAP);
ComDiagsArea sqlci_DA(&sqlci_Heap);

ComDiagsArea &SqlciEnv::diagsArea() { return sqlci_DA; }

extern SqlciEnv *global_sqlci_env;  // Global sqlci_env for break key handling purposes.

#define MXCI_DONOTISSUE_ERRMSGS -1

static char brkMessage[] = "Break.";
CRITICAL_SECTION g_CriticalSection;
CRITICAL_SECTION g_InterruptCriticalSection;

// We'll jump into this function from the interruptHandler routine...
// This function never exits.
static void ThrowBreakException() {
  EnterCriticalSection(&g_CriticalSection);

  SQLCI_EXCEPTION_EPILOGUE("Sqlci ThrowBreak");

  throw EHBreakException();
}

BOOL CtrlHandler(DWORD ctrlType) {
  if (Sqlci_PutbackChar == LOOK_FOR_BREAK)  // see InputStmt.cpp
    Sqlci_PutbackChar = FOUND_A_BREAK;
  breakReceived = 1;
  sqlci_DA << DgSqlCode(SQLCI_BREAK_RECEIVED, DgSqlCode::WARNING_);
  switch (ctrlType) {
    case CTRL_C_EVENT:
    case CTRL_BREAK_EVENT:
      if (TryEnterCriticalSection(&g_CriticalSection)) {
        Lng32 eCode = SQL_EXEC_Cancel(0);
        if (eCode == 0) {
          sqlci_DA << DgSqlCode(-SQL_Canceled, DgSqlCode::WARNING_);
          LeaveCriticalSection(&g_CriticalSection);

        } else {  // retry
          switch (-eCode) {
            case CLI_CANCEL_REJECTED: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_REJECTED, DgSqlCode::WARNING_);
              // sqlci_DA << DgSqlCode(SQLCI_COMMAND_NOT_CANCELLED, DgSqlCode::WARNING_);
              SqlciEnv s;
              s.displayDiagnostics();
              sqlci_DA.clear();
              // breakReceived = 0;
              LeaveCriticalSection(&g_CriticalSection);

            } break;
            default: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_ERROR, DgSqlCode::WARNING_);
              SqlciEnv s;
              s.displayDiagnostics();
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
        Lng32 eCode = SQL_EXEC_Cancel(0);
        if (eCode == 0) {
          sqlci_DA << DgSqlCode(-SQL_Canceled, DgSqlCode::WARNING_);
          LeaveCriticalSection(&g_CriticalSection);

        } else {  // retry
          switch (-eCode) {
            case CLI_CANCEL_REJECTED: {
              SqlciEnv s;
              s.displayDiagnostics();
              sqlci_DA.clear();
              LeaveCriticalSection(&g_CriticalSection);

            } break;
            default: {
              sqlci_DA << DgSqlCode(SQLCI_BREAK_ERROR, DgSqlCode::WARNING_);
              SqlciEnv s;
              s.displayDiagnostics();
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

// The signal interrupt handler.  When break is hit, this function
// will be called.

void interruptHandler(Int32 signalType) {
  void (*intHandler_addr)(Int32);
  intHandler_addr = interruptHandler;
  Lng32 retcode = 0;  // for return of success or failure from handleBreak methods in MACL and RW.

  UInt32 recoverNeeded = 0;

  signal(SIGINT, intHandler_addr);  // arm the signal for break.
  // This is for the Break key abend problem in Outside View for NSK.  Instead of SIGINT,
  // SIGQUIT is being sent by the TELSRV people to OSS.  We catch it right here.

  if (TryEnterCriticalSection(&g_CriticalSection)) {
    Lng32 eCode = SQL_EXEC_Cancel(0);
    if (eCode == 0) {
      LeaveCriticalSection(&g_CriticalSection);

    } else {  // retry
      switch (-eCode) {
        case CLI_CANCEL_REJECTED: {
          SqlciEnv s;
          s.displayDiagnostics();
          sqlci_DA.clear();
          LeaveCriticalSection(&g_CriticalSection);

        } break;
        default: {
          sqlci_DA << DgSqlCode(SQLCI_BREAK_ERROR, DgSqlCode::WARNING_);
          SqlciEnv s;
          s.displayDiagnostics();
          sqlci_DA.clear();
          LeaveCriticalSection(&g_CriticalSection);
        } break;
      }
    }
  }
}

void setupForSockets() { return; }

void cleanupSockets() {}

void SqlciEnv::welcomeMessage() {
  if (msg_license_enterprise_enabled()) {
    cout << (msg_license_advanced_enabled() ? "EsgynDB Advanced " : "EsgynDB Enterprise ")
         << COPYRIGHT_SQLCI_PRODNAME_H " " << COPYRIGHT_VERSION_H << endl
         << "Copyright (c) " << COPYRIGHT_HEADER_H << endl;
  } else {
    COPYRIGHT_BANNER_1(cout, COPYRIGHT_SQLCI_PRODNAME_H);
  }
}

SqlciEnv::SqlciEnv(short serv_type, NABoolean macl_rw_flag) {
  ole_server = serv_type;
  logfile = new Logfile;
  sqlci_stmts = new SqlciStmts(50);
  prepared_stmts = new SqlciList<PrepStmt>;
  param_list = new SqlciList<Param>;
  pattern_list = new SqlciList<Param>;
  envvar_list = new SqlciList<Envvar>;
  cursorList_ = new SqlciList<CursorStmt>;
  sqlci_stats = new SqlciStats();
  terminal_charset_ = CharInfo::UTF8;
  iso_mapping_charset_ = CharInfo::ISO88591;
  default_charset_ = CharInfo::ISO88591;
  infer_charset_ = FALSE;
  lastDmlStmtStatsType_ = SQLCLIDEV_NO_STATS;
  lastExecutedStmt_ = NULL;
  statsStmt_ = NULL;
  lastAllocatedStmt_ = NULL;

  defaultCatalog_ = NULL;
  defaultSchema_ = NULL;

  doneWithPrologue_ = FALSE;
  noBanner_ = FALSE;

  setListCount(MAX_LISTCOUNT);
  resetSpecialError();
  defaultCatAndSch_ = NULL;

  userNameFromCommandLine_ = "";
  tenantNameFromCommandLine_ = "";

  // A break flag macl_rw_flag was added to the constructor in order for the SqlciEnv
  // constructor not to call the MACL & Report Writer Constructors
  // when the SqlciEnv constructor is called after a break key is hit.

  // 64-bit: no more report writer and macl
  /*
  //  if (macl_rw_flag) {
  //    jzLng32 retcode = RW_MXCI_Constructor(report_env->rwEnv(), this);
  //    if (retcode == ERR)
  //      SqlciError (SQLCI_RW_UNABLE_TO_GET_CONSTRUCTOR,(ErrorParam *) 0 );
  //
  //    jzLng32 maclretcode = CS_MXCI_Constructor(cs_env->csEnv());
  //    if (maclretcode == CSERROR)
  //      SqlciError (SQLCI_CS_UNABLE_TO_GET_CONSTRUCTOR,(ErrorParam *) 0 );
  //    constructorFlag_ = TRUE;
  //  }
  //  else
  */
  constructorFlag_ = FALSE;

  setMode(SqlciEnv::SQL_);  // Set the mode initially to be SQL
  showShape_ = FALSE;
  eol_seen_on_input = -1;
  obey_file = 0;
  prev_err_flush_input = 0;

  logCommands_ = FALSE;
  deallocateStmt_ = FALSE;  // Always set to FALSE.Set to TRUE only in global_sqlci_env if needed.

  // Note that interactive_session is overwritten in SqlciEnv::run(infile) ...
  // Executing on NT
  if ((Int32)cin.tellg() == EOF)
    interactive_session = -1;  // Std. input is terminal keyboard
  else
    interactive_session = 0;
  cin.clear();  // Always clear() bad results from any tellg()

  // Special case for QA testing
  // ---------------------------
  // If sqlci has been started via an exec, ALWAYS deem it to be interactive.
  // Since there is no easy way to determine if we have been exec'ed, we will
  // use an environment variable to decide this. Just the existence of this
  // variable is enough and the value is immaterial.

  if (getenv("QA_TEST_WITH_EXEC")) interactive_session = -1;

#ifndef NDEBUG
  // Ensure that SQLCI sqlcode is same as in ExpError.h
  ComASSERT(SQL_Canceled == -ABS(EXE_CANCELED));
#endif

  prepareOnly_ = NULL;
  executeOnly_ = NULL;
  maxHeapSize_ = 0;
}

SqlciEnv::~SqlciEnv() {
  delete logfile;
  delete sqlci_stmts;
  delete prepared_stmts;
  delete param_list;
  delete pattern_list;
  delete cursorList_;
  delete envvar_list;
  delete sqlci_stats;
  sqlci_stmts = 0;
  // Report Writer and MACL Destructors has to be called only if the SqlciEnv constructor
  // called the MACL and RW constructors.
  if (constructorFlag_) {
    // 64-bit: no more report writer
    //    jzLng32 retcode = RW_MXCI_Destructor(report_env->rwEnv(), this);
    //    if (retcode == ERR)
    //      SqlciError (SQLCI_RW_UNABLE_TO_GET_DESTRUCTOR,(ErrorParam *) 0 );

    // 64-bit: no more macl
    //    jzLng32 maclretcode = CS_MXCI_Destructor(cs_env->csEnv());
    //    if (maclretcode == CSERROR)
    //      SqlciError (SQLCI_CS_UNABLE_TO_GET_DESTRUCTOR,(ErrorParam *) 0 );
  }
}

void SqlciEnv::autoCommit() {
  // Queries issued from SQLCI are COMMITted at the end of execution,
  // that is, if a transaction was started by the executor to execute it
  // then that transaction is ended.  This is the default behavior.
  // It could be overridden by either a BEGIN WORK statement or
  // a SET TRANSACTION AUTOCOMMIT OFF statement.
  //
  //
  // This function is called during the initialization phase of MXCI
  // (SqlciEnv_prologue_to_run). Use specialERROR_ as a flag indicating that
  // the querry being executed is invoke during MXCI's initialization phase and
  // that any errors will be fatal.
  SqlCmd::executeQuery("SET TRANSACTION AUTOCOMMIT ON;", this);
  // Should an error occur,  exit MXCI.
  if (!specialError_) {
    char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
    if (!noexit) exit(EXIT_FAILURE);
  }
}

void SqlciEnv::pertableStatistics() {
  // Turn on pertable statistics. This will make queries issued
  // from sqlci to collect pertable statistics and will be the
  // same behavior as sql/mp.
  //
  // This function is called during the initialization phase of MXCI
  // (SqlciEnv_prologue_to_run). Use specialERROR_ as a flag indicating that
  // the querry being executed is invoke during MXCI's initialization phase and
  // that any errors will be fatal.

  char *buf = new char[300];
  sprintf(buf, "select variable_info from table(statistics(null, cast(? as char(256) character set iso88591)))");
  SqlCmd sqlCmd(SqlCmd::DML_TYPE, buf);
  if (!statsStmt_) {
    statsStmt_ = new PrepStmt("__SQLCI_GET_STATS__");

    short retcode = sqlCmd.do_prepare(this, statsStmt_, sqlCmd.get_sql_stmt(), FALSE);
  }
  char *collectionType = getenv("SQLMX_REGRESS");
  if (collectionType == NULL) {
    strcpy(buf, "SET SESSION DEFAULT STATISTICS_VIEW_TYPE 'PERTABLE';");
    SqlCmd::executeQuery(buf, this);
    if (!specialError_) {
      char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
      if (!noexit) exit(EXIT_FAILURE);
    }
  }
  delete[] buf;
}

void SqlciEnv::datatypeSupport() {
  SqlCmd::executeQuery(
      "CONTROL QUERY DEFAULTS TRAF_TINYINT_SUPPORT 'ON' , TRAF_TINYINT_RETURN_VALUES 'ON' , TRAF_TINYINT_INPUT_PARAMS "
      "'ON' , TRAF_LARGEINT_UNSIGNED_IO 'ON' , TRAF_BOOLEAN_IO 'ON' ",
      this);

  // can handle binary datatype
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT TRAF_BINARY_SUPPORT 'ON';", this);
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT TRAF_BINARY_INPUT 'ON';", this);
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT TRAF_BINARY_OUTPUT 'ON';", this);

  if (!specialError_) {
    char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
    if (!noexit) exit(EXIT_FAILURE);
  }
}

void SqlciEnv::sqlmxRegress() {
  char *regr = getenv("SQLMX_REGRESS");
  if (regr) {
    char buf[1000];
    str_sprintf(buf, "set envvar sqlmx_regress %s;", regr);
    SqlCmd::executeQuery(buf, this);
    if (!specialError_) {
      char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
      if (!noexit) exit(EXIT_FAILURE);
    }

    str_sprintf(buf, "cqd sqlmx_regress 'ON';");
    SqlCmd::executeQuery(buf, this);
    if (!specialError_) {
      char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
      if (!noexit) exit(EXIT_FAILURE);
    }
  }
}

static void SqlciEnv_prologue_to_run(SqlciEnv *sqlciEnv) {
  if (sqlciEnv->doneWithPrologue()) return;

  if (NOT sqlciEnv->noBanner()) sqlciEnv->welcomeMessage();

  setupForSockets();

  //  SQL_EXEC_LoadTrafMetadataInCache();

  // If a user name was specified on the command line, call CLI to set
  // the database user identity.
  //
  // *** NOTE: This should always be sqlci's first CLI interaction
  // *** because CLI treats this operation as the beginning of a new
  // *** session with the compiler. Previous state in the compiler is
  // *** not guaranteed to persist after this interaction.
  //
  sqlciEnv->setUserIdentityInCLI();
  // The cli global instance had been created in setUserIdentityInCLI().
  GetCliGlobals()->setSqlciProcess(TRUE);
  if (sqlciEnv->getMaxHeapSize() > 0) {
    GetCliGlobals()->setSqlciMaxHeap(sqlciEnv->getMaxHeapSize());
  }

  // Suppress console messages
  sqlciEnv->setSpecialError(MXCI_DONOTISSUE_ERRMSGS, NULL);

  sqlciEnv->autoCommit();
  sqlciEnv->pertableStatistics();
  sqlciEnv->datatypeSupport();
  sqlciEnv->sqlmxRegress();

  SqlCmd::executeQuery("CQD TRAF_RETURN_LOB_HANDLE_WITHOUT_INLINE_DATA 'ON';", sqlciEnv);

  char *lsh = getenv("TEST_LOAD_SHARED_CACHE");
  if (lsh) {
    Int32 retcode = SqlCmd::executeQuery("LOAD TRAFODION METADATA INTO SHARED CACHE", sqlciEnv);
    if (retcode == 0) cout << "Loaded Trafodion Metadata into Shared Cache." << endl;
    SqlCmd::clearCLIDiagnostics();
  }

  // see catman/CatWellKnownTables.cpp for this envvar need.
  char *ltmi = getenv("LOB_TEST_METADATA_INIT");
  if (ltmi) {
    SqlCmd::executeQuery("CONTROL QUERY DEFAULT CAT_DISTRIBUTE_METADATA  'OFF';", sqlciEnv);
  }

  sqlciEnv->resetSpecialError();

  // Enable break handling.
  SQL_EXEC_BreakEnabled_Internal(TRUE);

  const char *initCmd = NULL;

  if (initCmd) {
    NAString cmd(initCmd);
    TrimNAStringSpace(cmd);
    size_t len = cmd.length();
    if (len > 1) {  // ignore if empty or ';' or '0' or '1' or ...
      if (cmd[len - 1] != ';') cmd += ';';

      SqlCmd::executeQuery(cmd, sqlciEnv);
    }
  }

  // indicate that the caller is sqlci
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT IS_SQLCI 'ON';", sqlciEnv);

  // Protect our startup CQDs and SET SCHEMA (from any SQL_MXCI_INITIALIZATION)
  // from being RESET.  User may still manually alter them, of course.
  //
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT * RESET RESET;", sqlciEnv);

  // tell CLI that datetime & interval values are to be input/output in internal format.
  SqlCmd::executeQuery("SET SESSION DEFAULT INTERNAL_FORMAT_IO 'ON';", sqlciEnv);

  // get sqlmx_terminal_charset if environment variable exists
  const char *termCS = getenv("SQLMX_TERMINAL_CHARSET");
  if (termCS) {
    SetTerminalCharset setTermCS((char *)termCS);
    setTermCS.process(sqlciEnv);
  }

  // get the DEFAULT_CHARSET CQD default attribute
  sqlciEnv->retrieveDefaultCharsetViaShowControlDefault();

  // get the INFER_CHARSET CQD default attribute
  sqlciEnv->retrieveInferCharsetViaShowControlDefault();

  // undo the CQD set by DDOL while retrieving the other CQDs
  SqlCmd::executeQuery("CONTROL QUERY DEFAULT SHOWCONTROL_SHOW_ALL RESET;", sqlciEnv);

  sqlciEnv->setDoneWithPrologue(TRUE);
}

// sqlci run with input coming in at SQLCI prompt
void SqlciEnv::run() {
  // This function is called during the initialization phase of MXCI
  // (SqlciEnv_prologue_to_run). Use specialERROR_ as a flag indicating that
  // the querry being executed is invoke during MXCI's initialization phase and
  // that any errors will be fatal. Should an error occur,  exit MXCI.

  SqlciEnv_prologue_to_run(this);

  // tell CLI that this user session has started.
  SqlCmd::executeQuery("SET SESSION DEFAULT SQL_SESSION 'BEGIN';", this);

  // Initialize lifetime objects
  InputStmt *input_stmt = NULL;
  Int32 retval;
  void (*intHandler_addr)(Int32);
  intHandler_addr = interruptHandler;
  do {
    retval = executeCommands(input_stmt);
  } while (retval != 0);

  DeleteCriticalSection(&g_CriticalSection);
  DeleteCriticalSection(&g_InterruptCriticalSection);

  cleanupSockets();
}

// sqlci run with input coming in as a parameter.
void SqlciEnv::runWithInputString(char *input_string) {
  // This function is called during the initialization phase of MXCI
  // (SqlciEnv_prologue_to_run). Use specialERROR_ as a flag indicating that
  // the querry being executed is invoke during MXCI's initialization phase and
  // that any errors will be fatal. Should an error occur,  exit MXCI.
  SqlciEnv_prologue_to_run(this);

  SqlCmd::executeQuery("SET SESSION DEFAULT SQL_SESSION 'BEGIN';", this);
  // Initialize lifetime objects
  InputStmt *input_stmt;
  Int32 retval;
  void (*intHandler_addr)(Int32);
  intHandler_addr = interruptHandler;

  InputStmt dummyIS(this);
  input_stmt = new InputStmt(&dummyIS, input_string);
  retval = executeCommands(input_stmt);
  if (retval == 0) {
  }

  DeleteCriticalSection(&g_CriticalSection);
  DeleteCriticalSection(&g_InterruptCriticalSection);

  cleanupSockets();
}

// sqlci run with input coming in from an infile specified at command line
void SqlciEnv::run(char *in_filename, char *input_string) {
  if ((!in_filename) && (input_string)) {
    runWithInputString(input_string);
    return;
  }
  interactive_session = 0;  // overwrite value from ctor!
  // This function is called during the initialization phase of MXCI
  // (SqlciEnv_prologue_to_run). Use specialERROR_ as a flag indicating that
  // the querry being executed is invoke during MXCI's initialization phase and
  // that any errors will be fatal. Should an error occur,  exit MXCI.
  SqlciEnv_prologue_to_run(this);

  SqlCmd::executeQuery("SET SESSION DEFAULT SQL_SESSION 'BEGIN';", this);
  Int32 retval = 0;
  SqlciNode *sqlci_node = 0;

  // input is from a file given at command line (SQLCI -i<filename>).
  // Create an "OBEY filename" command and process it.
  char *command = new char[10 + strlen(in_filename)];
  strcpy(command, "OBEY ");
  strcat(command, in_filename);
  strcat(command, ";");
  sqlci_parser(command, command, &sqlci_node, this);
  delete[] command;
  void (*intHandler_addr)(Int32);
  intHandler_addr = interruptHandler;
  if (sqlci_node) {
    retval = sqlci_node->process(this);
    delete sqlci_node;
    sqlci_node = NULL;
    displayDiagnostics();
    sqlci_DA.clear();  // Clear the DiagnosticsArea for the next command...
  }

  if (!retval)  // EXIT not seen in the obey file
  {
    // create an EXIT command
    char command[10];
    strcpy(command, "exit;");
    get_logfile()->WriteAll(">>exit;");
    sqlci_parser(command, command, &sqlci_node, this);
    if (sqlci_node) {
      retval = sqlci_node->process(this);
      delete sqlci_node;
      sqlci_node = NULL;
      displayDiagnostics();
      sqlci_DA.clear();
    }
  }
  DeleteCriticalSection(&g_CriticalSection);
  DeleteCriticalSection(&g_InterruptCriticalSection);

  cleanupSockets();
}  // run (in_filename)

static Int64 costTh = -2;
static pid_t pid = getpid();

Int32 SqlciEnv::executeCommands(InputStmt *&input_stmt) {
  Int32 retval = 0;
  Int32 ignore_toggle = 0;
  SqlciNode *sqlci_node = 0;
  Int64 time1 = 0, time2 = 0;
  struct timeval curtime;

  NABoolean inputPassedIn = (input_stmt ? TRUE : FALSE);

  if (costTh == -2) {
    char *costThreshold = getenv("RECORD_TIME_COST_SQL");
    if (costThreshold != NULL) costTh = atoi(costThreshold);
    if (costTh == -2) costTh = -1;
  }

  try {
    while (!retval) {
      total_opens = 0;
      total_closes = 0;

      // This is new'd here, deleted when history buffer fills up,
      // in SqlciStmts::add/StmtEntry::set
      if (NOT inputPassedIn) input_stmt = new InputStmt(this);

      Int32 read_error = 0;
      if (NOT inputPassedIn) read_error = input_stmt->readStmt(NULL /*i.e. input is stdin*/);

      if (costTh >= 0) {
        gettimeofday(&curtime, NULL);
        time1 = (curtime.tv_sec * 1000 + curtime.tv_usec / 1000);
      }
      prev_err_flush_input = 0;

      if (cin.eof() || read_error == -99) {
        // allow the other thread to process
        Sleep(50);  // milliseconds
        if (!input_stmt->isEmpty()) {
          // Unterminated statement in input file (redirected stdin).
          // Make the parser emit an error message.
          if (!isInteractiveSession()) input_stmt->display((UInt16)0);
          input_stmt->logStmt();
          input_stmt->syntaxErrorOnEof();
          displayDiagnostics();
          sqlci_DA.clear();
        }
        char command[10];
        strcpy(command, ">>exit;");
        if (!isInteractiveSession())
          get_logfile()->WriteAll(command);
        else if (get_logfile()->IsOpen())
          get_logfile()->Write(command, strlen(command));
        sqlci_parser(&command[2], &command[2], &sqlci_node, this);

        if (sqlci_node) {
          retval = sqlci_node->process(this);
          delete sqlci_node;
          sqlci_node = NULL;
        }
      } else {
        if (!isInteractiveSession()) input_stmt->display((UInt16)0);

        if (logCommands()) get_logfile()->setNoLog(FALSE);
        input_stmt->logStmt();
        if (logCommands()) get_logfile()->setNoLog(TRUE);

        if (!input_stmt->sectionMatches()) {
          Int32 ignore_stmt = input_stmt->isIgnoreStmt();
          if (ignore_stmt) ignore_toggle = ~ignore_toggle;
          if (ignore_stmt || ignore_toggle || input_stmt->ignoreJustThis()) {
            // ignore until stmt following the untoggling ?ignore
            sqlci_DA.clear();
          } else {
            getSqlciStmts()->add(input_stmt);
            if (!read_error) {
              retval = sqlci_parser(input_stmt->getPackedString(), input_stmt->getPackedString(), &sqlci_node, this);
              if (sqlci_node) {
                retval = sqlci_node->process(this);
                delete sqlci_node;
                sqlci_node = NULL;

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
      if (read_error == -20) {
        sqlci_DA << DgSqlCode(SQLCI_BREAK_RECEIVED, DgSqlCode::WARNING_);
      }

      if (read_error == SqlciEnv::MAX_FRAGMENT_LEN_OVERFLOW && !eolSeenOnInput()) setPrevErrFlushInput();

      displayDiagnostics();
      sqlci_DA.clear();  // Clear the DiagnosticsArea for the next command...

      if (total_opens != total_closes) {
        char buf[100];

        sprintf(buf, "total opens = %d, total closes = %d", total_opens, total_closes);

        get_logfile()->WriteAll(buf, strlen(buf));
      }

      // Delete the stmt if not one of those we saved on the history list
      if (!input_stmt->isInHistoryList()) delete input_stmt;

      if (inputPassedIn) retval = 1;

      if (costTh >= 0) {
        gettimeofday(&curtime, NULL);
        time2 = (curtime.tv_sec * 1000 + curtime.tv_usec / 1000);
        if (time2 - time1 > costTh)
          QRWARN("SQLCI executeCommands PID %d txID %ld TTC %ld", pid,
                 (GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1), time2 - time1);
      }
    }  // while
    if (retval == SQL_Canceled)
      return SQL_Canceled;
    else
      return 0;
  } catch (EHBreakException &) {
    sqlci_DA << DgSqlCode(SQLCI_BREAK_RECEIVED, DgSqlCode::WARNING_);
    displayDiagnostics();
    sqlci_DA.clear();  // Clear the DiagnosticsArea for the next command...

    if (sqlci_node) delete sqlci_node;
    sqlci_node = NULL;
    cin.clear();
    // NOTE: EnterCriticalSection has been done in ThrowBreakException()
    LeaveCriticalSection(&g_CriticalSection);
    return -1;
  } catch (...) {
    return 1;
  }

}  // executeCommands

// returns -1, if a transaction is active; 0, otherwise.
// Optionally returns the transaction identifier, if transid is passed in.
short SqlciEnv::statusTransaction(Int64 *transid) {
  // if a transaction is active, get the transid by calling the CLI procedure.
  SQLDESC_ID transid_desc;  // added for multi charset module names
  SQLMODULE_ID module;

  init_SQLCLI_OBJ_ID(&transid_desc);
  init_SQLMODULE_ID(&module);

  module.module_name = 0;
  transid_desc.module = &module;
  transid_desc.name_mode = desc_handle;

  HandleCLIErrorInit();

  Lng32 rc = SQL_EXEC_AllocDesc(&transid_desc, 1);

  HandleCLIError(rc, this);

  Int64 transid_;
  rc = SQL_EXEC_SetDescItem(&transid_desc, 1, SQLDESC_VAR_PTR, (Long)&transid_, 0);
  if (rc) SQL_EXEC_DeallocDesc(&transid_desc);
  HandleCLIError(rc, this);

  rc = SQL_EXEC_Xact(SQLTRANS_STATUS, &transid_desc);
  if (rc == 0) {
    if (transid) *transid = transid_;  // return transID if arg was passed in.
    rc = -1;                           // transaction is active.
  } else
    rc = 0;
  SQL_EXEC_DeallocDesc(&transid_desc);

  return (short)rc;
}
//
void SqlciEnv::displayDiagnostics() {
  NADumpDiags(cout, &sqlci_DA, TRUE /*newline*/, FALSE /*comment-style*/, get_logfile()->GetLogfile());
}

void SqlciEnv::setPrepareOnly(char *prepareOnly) {
  if (prepareOnly_) delete prepareOnly_;

  prepareOnly_ = NULL;

  if (prepareOnly) {
    prepareOnly_ = new char[strlen(prepareOnly) + 1];
    strcpy(prepareOnly_, prepareOnly);

    delete executeOnly_;
    executeOnly_ = NULL;
  }
}

void SqlciEnv::setExecuteOnly(char *executeOnly) {
  if (executeOnly_) delete executeOnly_;

  executeOnly_ = NULL;

  if (executeOnly) {
    executeOnly_ = new char[strlen(executeOnly) + 1];
    strcpy(executeOnly_, executeOnly);

    delete prepareOnly_;
    prepareOnly_ = NULL;
  }
}

void callback_storeSchemaInformation(SqlciEnv *sqlci_env, Lng32 err, const char *cat, const char *sch) {
  if (sqlci_env->defaultCatalog()) delete sqlci_env->defaultCatalog();
  if (sqlci_env->defaultSchema()) delete sqlci_env->defaultSchema();

  sqlci_env->defaultCatalog() = new char[strlen(cat) + 1];
  sqlci_env->defaultSchema() = new char[strlen(sch) + 1];

  strcpy(sqlci_env->defaultCatalog(), cat);
  strcpy(sqlci_env->defaultSchema(), sch);
}

void SqlciEnv::updateDefaultCatAndSch() {
  setSpecialError(ShowSchema::DiagSqlCode(), callback_storeSchemaInformation);
  SqlCmd::executeQuery(ShowSchema::ShowSchemaStmt(), this);
  resetSpecialError();
}

// This is the command which will show the user
// which mode they are in.

void SqlciEnv::showMode(ModeType mode_) {
  switch (mode_) {
    case SQL_:
      cout << "The current mode is SQL mode." << endl;
      break;

    default:

      break;
  }
}

////////////////////////////////////////
// Processing of the ENV command.
////////////////////////////////////////

Env::Env(char *argument_, Lng32 arglen_) : SqlciCmd(SqlciCmd::ENV_TYPE, argument_, arglen_) {}

void callback_Env_showSchema(SqlciEnv *sqlci_env, Lng32 err, const char *cat, const char *sch) {
  sqlci_env->defaultCatAndSch().setCatalogNamePart(NAString(cat));
  sqlci_env->defaultCatAndSch().setSchemaNamePart(NAString(sch));
}

short Env::process(SqlciEnv *sqlci_env) {
  // ## Should any of this text come from the message file,
  // ## i.e. from a translatable file for I18N?

  // When adding new variables, please keep the information in
  // alphabetic order
  Logfile *log = sqlci_env->get_logfile();

  log->WriteAll("----------------------------------");
  log->WriteAll("Current Environment");
  log->WriteAll("----------------------------------");

  ComAuthenticationType authenticationType = COM_NOT_AUTH;
  bool authorizationEnabled = false;
  bool authorizationReady = false;
  bool auditingEnabled = false;
  Int32 rc =
      sqlci_env->getAuthState2((Int32 &)authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);

  // TDB: add auditing state
  log->WriteAllWithoutEOL("AUTHENTICATION     ");
  switch (authenticationType) {
    case COM_NOT_AUTH: {
      log->WriteAll("authentication is disabled");
    } break;
    case COM_NO_AUTH: {
      log->WriteAll("LOCAL with non-password login");
    } break;
    case COM_LDAP_AUTH: {
      log->WriteAll("LDAP");
    } break;
    case COM_LOCAL_AUTH:
    default: {
      log->WriteAll("LOCAL");
    } break;
  }

  log->WriteAllWithoutEOL("AUTHORIZATION      ");
  if (authorizationEnabled)
    log->WriteAll("enabled");
  else
    log->WriteAll("disabled");

  log->WriteAllWithoutEOL("CURRENT DIRECTORY  ");

  log->WriteAll(getcwd((char *)NULL, NA_MAX_PATH));

  log->WriteAllWithoutEOL("INSTANCE ID        ");
  char *insID = getenv("TRAF_INSTANCE_ID");
  if (insID && strlen(insID) > 0)
    log->WriteAll(insID);
  else
    log->WriteAll("1");

  log->WriteAllWithoutEOL("INSTANCE NAME      ");
  char *insName = getenv("TRAF_INSTANCE_NAME");
  if (insName && strlen(insName) > 0)
    log->WriteAll(insName);
  else
    log->WriteAll("ESGYNDB");

  char buf[100];

  log->WriteAllWithoutEOL("MESSAGEFILE        ");
  const char *mf = GetErrorMessageFileName();
  log->WriteAll(mf ? mf : "");

#if 0
  log->WriteAllWithoutEOL("ISO88591 MAPPING   ");
  log->WriteAll(CharInfo::getCharSetName(sqlci_env->getIsoMappingCharset()));

  log->WriteAllWithoutEOL("DEFAULT CHARSET    ");
  log->WriteAll(CharInfo::getCharSetName(sqlci_env->getDefaultCharset()));

  log->WriteAllWithoutEOL("INFER CHARSET      ");
  log->WriteAll((sqlci_env->getInferCharset())?"ON":"OFF");
#endif

  // ## These need to have real values detected from the env and written out:

  // "US English" is more "politically correct" than "American English".
  //
  log->WriteAllWithoutEOL("MESSAGEFILE LANG   US English\n");

  log->WriteAllWithoutEOL("MESSAGEFILE VRSN   ");
  char vmsgcode[10];
  sprintf(vmsgcode, "%d", SQLERRORS_MSGFILE_VERSION_INFO);
  Error vmsg(vmsgcode, strlen(vmsgcode), Error::ENVCMD_);
  vmsg.process(sqlci_env);

  ComAnsiNamePart defaultCat;
  ComAnsiNamePart defaultSch;

  sqlci_env->getDefaultCatAndSch(defaultCat, defaultSch);
  CharInfo::CharSet TCS = sqlci_env->getTerminalCharset();
  CharInfo::CharSet ISOMAPCS = sqlci_env->getIsoMappingCharset();

  if (TCS != CharInfo::UTF8) {
    NAString dCat = defaultCat.getExternalName();
    NAString dSch = defaultSch.getExternalName();
    charBuf cbufCat((unsigned char *)dCat.data(), dCat.length());
    charBuf cbufSch((unsigned char *)dSch.data(), dSch.length());
    NAWcharBuf *wcbuf = 0;
    Int32 errorcode = 0;

    wcbuf = csetToUnicode(cbufCat, 0, wcbuf, CharInfo::UTF8, errorcode);
    NAString *tempstr;
    if (errorcode != 0) {
      tempstr = new NAString(defaultCat.getExternalName().data());
    } else {
      tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), TCS, NULL, TRUE);
      TrimNAStringSpace(*tempstr, FALSE, TRUE);  // trim trailing blanks
    }
    log->WriteAllWithoutEOL("SQL CATALOG        ");
    log->WriteAll(tempstr->data());

    // Default Schema

    wcbuf = 0;  // must 0 out to get next call to allocate memory.
    wcbuf = csetToUnicode(cbufSch, 0, wcbuf, CharInfo::UTF8, errorcode);
    if (errorcode != 0) {
      tempstr = new NAString(defaultSch.getExternalName().data());
    } else {
      tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), TCS, NULL, TRUE);
      TrimNAStringSpace(*tempstr, FALSE, TRUE);  // trim trailing blanks
    }
    log->WriteAllWithoutEOL("SQL SCHEMA         ");
    log->WriteAll(tempstr->data());
  } else {
    log->WriteAllWithoutEOL("SQL CATALOG        ");
    log->WriteAll(defaultCat.getExternalName());
    log->WriteAllWithoutEOL("SQL SCHEMA         ");
    log->WriteAll(defaultSch.getExternalName());
  }

  // On Linux we include the database user name and user ID in the
  // command output
  NAString username;
  rc = sqlci_env->getExternalUserName(username);
  log->WriteAllWithoutEOL("SQL USER CONNECTED ");
  if (rc >= 0)
    log->WriteAll(username.data());
  else
    log->WriteAll("?");

  rc = sqlci_env->getDatabaseUserName(username);
  log->WriteAllWithoutEOL("SQL USER DB NAME   ");
  if (rc >= 0)
    log->WriteAll(username.data());
  else
    log->WriteAll("?");

  Int32 uid = 0;
  rc = sqlci_env->getDatabaseUserID(uid);
  log->WriteAllWithoutEOL("SQL USER ID        ");
  if (rc >= 0)
    sprintf(buf, "%d", (int)uid);
  else
    strcpy(buf, "?");
  log->WriteAll(buf);

  if (msg_license_multitenancy_enabled()) {
    rc = sqlci_env->getTenantID(uid);
    log->WriteAllWithoutEOL("TENANT ID          ");
    if (rc >= 0)
      sprintf(buf, "%d", (int)uid);
    else
      strcpy(buf, "?");
    log->WriteAll(buf);

    rc = sqlci_env->getTenantName(username);
    log->WriteAllWithoutEOL("TENANT NAME        ");
    if (rc >= 0)
      log->WriteAll(username.data());
    else
      log->WriteAll("?");
  }

  log->WriteAllWithoutEOL("TERMINAL CHARSET   ");
  log->WriteAll(CharInfo::getCharSetName(sqlci_env->getTerminalCharset()));

  Int64 transid;
  if (sqlci_env->statusTransaction(&transid)) {
    // transaction is active.
    char transid_str[20];
    convertInt64ToAscii(transid, transid_str);
    log->WriteAllWithoutEOL("TRANSACTION ID     ");
    log->WriteAll(transid_str);
    log->WriteAll("TRANSACTION STATE  in progress");
  } else {
    log->WriteAll("TRANSACTION ID     ");
    log->WriteAll("TRANSACTION STATE  not in progress");
  }

  if (log->isVerbose())
    log->WriteAll("WARNINGS           on");
  else
    log->WriteAll("WARNINGS           off");

  return 0;
}

////////////////////////////////////////
// Processing of the ChangeUser command.
////////////////////////////////////////

ChangeUser::ChangeUser(char *argument_, Lng32 argLen_, char *tenant_)
    : SqlciCmd(SqlciCmd::USER_TYPE, argument_, argLen_) {
  if (tenant_) {
    tenantName = new char[strlen(tenant_) + 1];
    strncpy(tenantName, tenant_, strlen(tenant_));
    tenantName[strlen(tenant_)] = 0;
  } else
    tenantName = NULL;
}

short ChangeUser::process(SqlciEnv *sqlci_env) {
  sqlci_env->setUserNameFromCommandLine(get_argument());
  if (tenantName) sqlci_env->setTenantNameFromCommandLine(tenantName);
  sqlci_env->setUserIdentityInCLI();
  return 0;
}

void SqlciEnv::getDefaultCatAndSch(ComAnsiNamePart &defaultCat, ComAnsiNamePart &defaultSch) {
  defaultCatAndSch_ = new ComSchemaName;

  setSpecialError(ShowSchema::DiagSqlCode(), callback_Env_showSchema);
  SqlCmd::executeQuery(ShowSchema::ShowSchemaStmt(), this);
  resetSpecialError();

  defaultCat = defaultCatAndSch_->getCatalogNamePart();
  defaultSch = defaultCatAndSch_->getSchemaNamePart();

  delete defaultCatAndSch_;
  defaultCatAndSch_ = NULL;
}

// Retrieve the external database user ID from CLI
Int32 SqlciEnv::getExternalUserName(NAString &username) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  Int32 rc = SQL_EXEC_GetSessionAttr(SESSION_EXTERNAL_USER_NAME, NULL, buf, 1024, NULL);
  HandleCLIError(rc, this);

  if (rc >= 0) username = buf;

  if (username.length() == 0) username = "user not connected";
  return rc;
}

// Retrieve the database user ID from CLI
Int32 SqlciEnv::getDatabaseUserID(Int32 &uid) {
  HandleCLIErrorInit();

  Int32 localUID = 0;
  Int32 rc = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_ID, &localUID, NULL, 0, NULL);
  HandleCLIError(rc, this);

  if (rc >= 0) uid = localUID;

  return rc;
}

Int32 SqlciEnv::getAuthState(bool &authenticationEnabled, bool &authorizationEnabled, bool &authorizationReady,
                             bool &auditingEnabled) {
  Int32 authenticationType = 0;
  Int32 ret = getAuthState2(authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);
  authenticationEnabled = authorizationReady;
  return ret;
}

// Retrieve the database user ID from CLI
Int32 SqlciEnv::getAuthState2(Int32 &authenticationType, bool &authorizationEnabled, bool &authorizationReady,
                              bool &auditingEnabled) {
  HandleCLIErrorInit();

  Int32 localUID = 0;
  Int32 rc = SQL_EXEC_GetAuthState(authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);
  HandleCLIError(rc, this);

  return rc;
}

// Retrieve the database user name from CLI. This will be the
// USER_NAME column from a USERS row not the EXTERNAL_USER_NAME
// column.
Int32 SqlciEnv::getDatabaseUserName(NAString &username) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  Int32 rc = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_NAME, NULL, buf, 1024, NULL);
  HandleCLIError(rc, this);

  if (rc >= 0) username = buf;

  if (rc != 0) SQL_EXEC_ClearDiagnostics(NULL);

  return rc;
}

// Retrieve the tenant ID from CLI
Int32 SqlciEnv::getTenantID(Int32 &uid) {
  HandleCLIErrorInit();

  Int32 localUID = 0;
  Int32 rc = SQL_EXEC_GetSessionAttr(SESSION_TENANT_ID, &localUID, NULL, 0, NULL);
  HandleCLIError(rc, this);

  if (rc >= 0) uid = localUID;

  return rc;
}

// Retrieve the tenant name from CLI.
Int32 SqlciEnv::getTenantName(NAString &tenantName) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  Int32 rc = SQL_EXEC_GetSessionAttr(SESSION_TENANT_NAME, NULL, buf, 1024, NULL);
  HandleCLIError(rc, this);

  if (rc >= 0) tenantName = buf;

  if (rc != 0) SQL_EXEC_ClearDiagnostics(NULL);

  return rc;
}

CharInfo::CharSet SqlciEnv::retrieveIsoMappingCharsetViaShowControlDefault() {
  this->iso_mapping_charset_ = CharInfo::ISO88591;

  return this->iso_mapping_charset_;
}

CharInfo::CharSet SqlciEnv::retrieveDefaultCharsetViaShowControlDefault() {
  this->default_charset_ = CharInfo::ISO88591;
  return this->default_charset_;
}

NABoolean SqlciEnv::retrieveInferCharsetViaShowControlDefault() {
  this->infer_charset_ = FALSE;
  return this->infer_charset_;
}

void SqlciEnv::setUserNameFromCommandLine(const char *s) {
  userNameFromCommandLine_ = s;
  userNameFromCommandLine_.strip();
}

void SqlciEnv::setTenantNameFromCommandLine(const char *s) {
  tenantNameFromCommandLine_ = s;
  tenantNameFromCommandLine_.strip();
}

// If a user name was specified on the command line, call CLI to set
// the database user identity.
void SqlciEnv::setUserIdentityInCLI() {
  PushAndSetSqlParserFlags savedParserFlags(INTERNAL_QUERY_FROM_EXEUTIL);

  // If username specified, use different interface to connect
  if (userNameFromCommandLine_.length() > 0) {
    // Within this function we do not want CLI errors to be
    // fatal. Setting specialError_ to 0 ensures they are not. We also
    // save the current value of specialError_ and restore if after
    // the CLI calls.
    Lng32 previousSpecialError = specialError_;
    specialError_ = 0;

    HandleCLIErrorInit();
    Lng32 sqlcode = 0;

    // get the authID (same as sessionID)
    NAString externalName("DB__ROOT");
    NAString databaseName("DB__ROOT");
    Int32 userID(33333);
    Int32 sessionID(33333);
    sqlcode = SQL_EXEC_GetAuthID(userNameFromCommandLine_.data(), userID);
    HandleCLIError(sqlcode, this);
    sessionID = userID;
    if (sqlcode >= 0) {
      printf("\nDatabase user: %s\n", userNameFromCommandLine_.data());
      externalName = userNameFromCommandLine_;
      databaseName = userNameFromCommandLine_;
    }

    // set the tenant name (if present)
    NAString tenantName("ESGYNDB");
    Int32 tenantID(1500000);
    if (tenantNameFromCommandLine_.length() > 0) {
      tenantName = tenantNameFromCommandLine_;
      sqlcode = SQL_EXEC_GetAuthID(tenantNameFromCommandLine_.data(), tenantID);
      HandleCLIError(sqlcode, this);

      if (sqlcode >= 0) {
        printf("Tenant: %s\n\n", tenantName.data());
        tenantName = tenantNameFromCommandLine_;
      }
    }

    USERS_INFO usersInfo;
    usersInfo.extUsername = new char[strlen(externalName.data()) + 1];
    strcpy(usersInfo.extUsername, externalName.data());
    usersInfo.dbUsername = new char[strlen(databaseName.data() + 1)];
    strcpy(usersInfo.dbUsername, databaseName.data());
    usersInfo.tenantName = new char[strlen(tenantName.data() + 1)];
    strcpy(usersInfo.tenantName, tenantName.data());
    usersInfo.tenantID = tenantID;
    usersInfo.effectiveUserID = userID;
    usersInfo.sessionUserID = sessionID;

    sqlcode = SQL_EXEC_SetAuthID2(usersInfo, NULL, 0, NULL, NULL, 0);
    HandleCLIError(sqlcode, this);

    if (sqlcode != 0) SQL_EXEC_ClearDiagnostics(NULL);
    specialError_ = previousSpecialError;
  } else {
    if (tenantNameFromCommandLine_.length() > 0)
      printf("\nTenant option ignored since no database user was specified.\n\n");

    Int32 uid = 0;
    getDatabaseUserID(uid);
  }
}
