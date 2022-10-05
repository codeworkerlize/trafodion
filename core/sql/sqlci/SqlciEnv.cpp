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

#include "sqlci/SqlciEnv.h"
#include "sqlci/SqlciError.h"
#include "sqlci/SqlciNode.h"
#include "sqlci/SqlciParser.h"
#include "sqlci/SqlciCmd.h"
#include "cli/sql_id.h"
#include "sqlci/SqlciStats.h"
#include "sqlci/sqlcmd.h"
#include "cli/SQLCLIdev.h"
#include "sqlci/InputStmt.h"
#include "common/CmpCommon.h"
#include "export/ComDiags.h"
#include "common/copyright.h"
#include "eh/EHException.h"
#include "sqlmsg/ErrorMessage.h"
#include "exp/ExpError.h"
#include "sqlmsg/GetErrorMessage.h"
#include "common/Int64.h"
#include "sqlci/SqlciParseGlobals.h"
#include "sqlcomp/ShowSchema.h"
#include "common/str.h"
#include "common/BaseTypes.h"
#include "common/ComSchemaName.h"
#include "arkcmp/CmpContext.h"

#include "sqlcomp/DefaultConstants.h"
#include "common/ComRtUtils.h"
#include "common/NLSConversion.h"
#include "qmscommon/QRLogger.h"
#include "cli/Globals.h"

int total_opens = 0;
int total_closes = 0;

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
        int eCode = SQL_EXEC_Cancel(0);
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
  deallocateStmt_ = FALSE;  // Always set to FALSE.Set to TRUE only in global_sqlci_env if needed.

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
  SqlCmd::executeQuery("SET TRANSACTION AUTOCOMMIT ON;", this);
  // Should an error occur,  exit MXCI.
  if (!specialError_) {
    char *noexit = getenv("SQL_MXCI_NO_EXIT_ON_COMPILER_STARTUP_ERROR");
    if (!noexit) exit(EXIT_FAILURE);
  }
}

void SqlciEnv::run() {
  InputStmt *input_stmt = nullptr;
  int retval;
  do {
    retval = executeCommands(input_stmt);
  } while (retval != 0);
}

static long costTh = -2;
static pid_t pid = getpid();

int SqlciEnv::executeCommands(InputStmt *&input_stmt) {
  int retval = 0;
  int ignore_toggle = 0;
  SqlciNode *sqlci_node = 0;
  long time1 = 0, time2 = 0;
  struct timeval curtime;

  NABoolean inputPassedIn = (input_stmt ? TRUE : FALSE);

  try {
    while (!retval) {
      total_opens = 0;
      total_closes = 0;

      if (NOT inputPassedIn) input_stmt = new InputStmt(this);

      int read_error = 0;
      if (NOT inputPassedIn) read_error = input_stmt->readStmt(nullptr /*i.e. input is stdin*/);

      prev_err_flush_input = 0;

      if (cin.eof() || read_error == -99) {
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
          sqlci_node = nullptr;
        }
      } else {
        if (!isInteractiveSession()) input_stmt->display((UInt16)0);

        if (logCommands()) get_logfile()->setNoLog(FALSE);
        input_stmt->logStmt();
        if (logCommands()) get_logfile()->setNoLog(TRUE);

        if (!input_stmt->sectionMatches()) {
          int ignore_stmt = input_stmt->isIgnoreStmt();
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
        gettimeofday(&curtime, nullptr);
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
    sqlci_node = nullptr;
    cin.clear();
    LeaveCriticalSection(&g_CriticalSection);
    return -1;
  } catch (...) {
    return 1;
  }
}

void SqlciEnv::displayDiagnostics() {
  NADumpDiags(cout, &sqlci_DA, TRUE /*newline*/, FALSE /*comment-style*/, get_logfile()->GetLogfile());
}

void SqlciEnv::setPrepareOnly(char *prepareOnly) {
  if (prepareOnly_) delete prepareOnly_;

  prepareOnly_ = nullptr;

  if (prepareOnly) {
    prepareOnly_ = new char[strlen(prepareOnly) + 1];
    strcpy(prepareOnly_, prepareOnly);

    delete executeOnly_;
    executeOnly_ = nullptr;
  }
}

void SqlciEnv::setExecuteOnly(char *executeOnly) {
  if (executeOnly_) delete executeOnly_;

  executeOnly_ = nullptr;

  if (executeOnly) {
    executeOnly_ = new char[strlen(executeOnly) + 1];
    strcpy(executeOnly_, executeOnly);

    delete prepareOnly_;
    prepareOnly_ = nullptr;
  }
}

void callback_storeSchemaInformation(SqlciEnv *sqlci_env, int err, const char *cat, const char *sch) {
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

Env::Env(char *argument_, int arglen_) : SqlciCmd(SqlciCmd::ENV_TYPE, argument_, arglen_) {}

void callback_Env_showSchema(SqlciEnv *sqlci_env, int err, const char *cat, const char *sch) {
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
  int rc =
      sqlci_env->getAuthState2((int &)authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);

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

  log->WriteAll(getcwd((char *)nullptr, NA_MAX_PATH));

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
    int errorcode = 0;

    wcbuf = csetToUnicode(cbufCat, 0, wcbuf, CharInfo::UTF8, errorcode);
    NAString *tempstr;
    if (errorcode != 0) {
      tempstr = new NAString(defaultCat.getExternalName().data());
    } else {
      tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), TCS, nullptr, TRUE);
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
      tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), TCS, nullptr, TRUE);
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

  int uid = 0;
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

  long transid;
  {
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

ChangeUser::ChangeUser(char *argument_, int argLen_, char *tenant_)
    : SqlciCmd(SqlciCmd::USER_TYPE, argument_, argLen_) {
  if (tenant_) {
    tenantName = new char[strlen(tenant_) + 1];
    strncpy(tenantName, tenant_, strlen(tenant_));
    tenantName[strlen(tenant_)] = 0;
  } else
    tenantName = nullptr;
}

short ChangeUser::process(SqlciEnv *sqlci_env) { return 0; }

void SqlciEnv::getDefaultCatAndSch(ComAnsiNamePart &defaultCat, ComAnsiNamePart &defaultSch) {
  defaultCatAndSch_ = new ComSchemaName;

  setSpecialError(ShowSchema::DiagSqlCode(), callback_Env_showSchema);
  SqlCmd::executeQuery(ShowSchema::ShowSchemaStmt(), this);
  resetSpecialError();

  defaultCat = defaultCatAndSch_->getCatalogNamePart();
  defaultSch = defaultCatAndSch_->getSchemaNamePart();

  delete defaultCatAndSch_;
  defaultCatAndSch_ = nullptr;
}

// Retrieve the external database user ID from CLI
int SqlciEnv::getExternalUserName(NAString &username) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  int rc = SQL_EXEC_GetSessionAttr(SESSION_EXTERNAL_USER_NAME, nullptr, buf, 1024, nullptr);
  HandleCLIError(rc, this);

  if (rc >= 0) username = buf;

  if (username.length() == 0) username = "user not connected";
  return rc;
}

// Retrieve the database user ID from CLI
int SqlciEnv::getDatabaseUserID(int &uid) {
  HandleCLIErrorInit();

  int localUID = 0;
  int rc = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_ID, &localUID, nullptr, 0, nullptr);
  HandleCLIError(rc, this);

  if (rc >= 0) uid = localUID;

  return rc;
}

int SqlciEnv::getAuthState(bool &authenticationEnabled, bool &authorizationEnabled, bool &authorizationReady,
                           bool &auditingEnabled) {
  int authenticationType = 0;
  int ret = getAuthState2(authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);
  authenticationEnabled = authorizationReady;
  return ret;
}

// Retrieve the database user ID from CLI
int SqlciEnv::getAuthState2(int &authenticationType, bool &authorizationEnabled, bool &authorizationReady,
                            bool &auditingEnabled) {
  HandleCLIErrorInit();

  int localUID = 0;
  int rc = SQL_EXEC_GetAuthState(authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);
  HandleCLIError(rc, this);

  return rc;
}

// Retrieve the database user name from CLI. This will be the
// USER_NAME column from a USERS row not the EXTERNAL_USER_NAME
// column.
int SqlciEnv::getDatabaseUserName(NAString &username) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  int rc = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_NAME, nullptr, buf, 1024, nullptr);
  HandleCLIError(rc, this);

  if (rc >= 0) username = buf;

  if (rc != 0) SQL_EXEC_ClearDiagnostics(nullptr);

  return rc;
}

// Retrieve the tenant ID from CLI
int SqlciEnv::getTenantID(int &uid) {
  HandleCLIErrorInit();

  int localUID = 0;
  int rc = SQL_EXEC_GetSessionAttr(SESSION_TENANT_ID, &localUID, nullptr, 0, nullptr);
  HandleCLIError(rc, this);

  if (rc >= 0) uid = localUID;

  return rc;
}

// Retrieve the tenant name from CLI.
int SqlciEnv::getTenantName(NAString &tenantName) {
  HandleCLIErrorInit();

  char buf[1024] = "";
  int rc = SQL_EXEC_GetSessionAttr(SESSION_TENANT_NAME, nullptr, buf, 1024, nullptr);
  HandleCLIError(rc, this);

  if (rc >= 0) tenantName = buf;

  if (rc != 0) SQL_EXEC_ClearDiagnostics(nullptr);

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
