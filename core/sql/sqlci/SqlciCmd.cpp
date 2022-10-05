

#include "sqlci/SqlciCmd.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

#include <iostream>
#include <sstream>

#include "cli/sql_id.h"
#include "common/ComCextdecs.h"
#include "common/ComRtUtils.h"
#include "common/ComSmallDefs.h"
#include "common/ComUser.h"
#include "common/NAWinNT.h"
#include "common/Platform.h"
#include "common/charinfo.h"
#include "common/str.h"
#include "export/ComDiags.h"
#include "sqlci/InputStmt.h"
#include "sqlci/ShellCmd.h"
#include "sqlci/Sqlci.h"
#include "sqlci/SqlciEnv.h"
#include "sqlci/SqlciError.h"
#include "sqlci/SqlciParser.h"
#include "sqlci/sqlcmd.h"
#include "sqlmsg/ErrorMessage.h"
#include "sqlmsg/GetErrorMessage.h"

extern ComDiagsArea sqlci_DA;

SqlciCmd::SqlciCmd(const sqlci_cmd_type cmd_type_) : SqlciNode(SqlciNode::SQLCI_CMD_TYPE), cmd_type(cmd_type_) {
  arglen = 0;
  argument = 0;
}

SqlciCmd::SqlciCmd(const sqlci_cmd_type cmd_type_, char *argument_, int arglen_)
    : SqlciNode(SqlciNode::SQLCI_CMD_TYPE), cmd_type(cmd_type_) {
  arglen = arglen_;

  if (argument_) {
    argument = new char[arglen_ + 1];
    strncpy(argument, argument_, arglen_);
    argument[arglen] = 0;
  } else {
    argument = 0;
  }
};

SqlciCmd::SqlciCmd(const sqlci_cmd_type cmd_type_, NAWchar *argument_, int arglen_)
    : SqlciNode(SqlciNode::SQLCI_CMD_TYPE), cmd_type(cmd_type_) {
  arglen = 2 * arglen_;

  if (argument_) {
    NAWchar *tgt = new NAWchar[arglen_ + 1];  // extra byte for NAWchar typed argument
    NAWstrncpy(tgt, argument_, arglen_);
    tgt[arglen_] = 0;
    argument = (char *)tgt;
  } else {
    argument = 0;
  }
};

SqlciCmd::SqlciCmd(const sqlci_cmd_type cmd_type_, int argument_)
    : SqlciNode(SqlciNode::SQLCI_CMD_TYPE), cmd_type(cmd_type_) {
  numeric_arg = argument_;
  argument = 0;
};

SqlciCmd::~SqlciCmd() { delete[] argument; };

Log::Log(char *argument_, int arglen_, log_type type_, int commands_only)
    : SqlciCmd(SqlciCmd::LOG_TYPE, argument_, arglen_), type(type_), commandsOnly_(commands_only){};

Shape::Shape(NABoolean type, char *infile, char *outfile)
    : SqlciCmd(SqlciCmd::SHAPE_TYPE), type_(type), infile_(infile), outfile_(outfile){};

Statistics::Statistics(char *argument_, int arglen_, StatsCmdType type, char *statsOptions)
    : SqlciCmd(SqlciCmd::STATISTICS_TYPE, argument_, arglen_), type_(type) {
  if (statsOptions) {
    statsOptions_ = new char[strlen(statsOptions) + 1];
    strcpy(statsOptions_, statsOptions);
  } else {
    statsOptions_ = NULL;
  }
};

Statistics::~Statistics() {
  if (statsOptions_) delete[] statsOptions_;
};

Mode::Mode(ModeType type_, NABoolean value) : SqlciCmd(SqlciCmd::MODE_TYPE), value(value) { type = type_; };

ParserFlags::ParserFlags(ParserFlagsOperation opType_, int param_) : SqlciCmd(SqlciCmd::PARSERFLAGS_TYPE, param_) {
  opType = opType_;
  param = param_;
};

Error::Error(char *argument_, int arglen_, error_type type_) : SqlciCmd(SqlciCmd::ERROR_TYPE, argument_, arglen_) {
  type = type_;
};

SubError::SubError(char *argument_, int arglen_, suberror_type type_)
    : SqlciCmd(SqlciCmd::ERROR_TYPE, argument_, arglen_) {
  type = type_;
};

Exit::Exit(char *argument_, int arglen_) : SqlciCmd(SqlciCmd::EXIT_TYPE, argument_, arglen_){};

Reset::Reset(reset_type type_, char *argument_, int arglen_)
    : SqlciCmd(SqlciCmd::SETPARAM_TYPE, argument_, arglen_), type(type_){};

Reset::Reset(reset_type type_) : SqlciCmd(SqlciCmd::SETPARAM_TYPE), type(type_){};

SetParam::SetParam(char *param_name_, int namelen_, char *argument_, int arglen_, CharInfo::CharSet x)
    : SqlciCmd(SqlciCmd::SETPARAM_TYPE, argument_, arglen_),
      cs(x),
      inSingleByteForm_(TRUE),
      m_convUTF16ParamStrLit(NULL),
      isQuotedStrWithoutCharSetPrefix_(FALSE),
      m_termCS(CharInfo::UnknownCharSet) {
  if (param_name_) {
    param_name = new char[namelen_ + 1];
    strcpy(param_name, param_name_);
    namelen = namelen_;
  } else {
    param_name = 0;
    namelen = 0;
  }
};

SetParam::SetParam(char *param_name_, int namelen_, NAWchar *argument_, int arglen_, CharInfo::CharSet x)
    : SqlciCmd(SqlciCmd::SETPARAM_TYPE, argument_, arglen_),
      cs(x),
      inSingleByteForm_(FALSE),
      m_convUTF16ParamStrLit(NULL),
      isQuotedStrWithoutCharSetPrefix_(FALSE),
      m_termCS(CharInfo::UnknownCharSet) {
  if (param_name_) {
    param_name = new char[namelen_ + 1];
    strcpy(param_name, param_name_);
    namelen = namelen_;
  } else {
    param_name = 0;
    namelen = 0;
  }
};

SetPattern::SetPattern(char *pattern_name_, int namelen_, char *argument_, int arglen_)
    : SqlciCmd(SqlciCmd::SETPATTERN_TYPE, argument_, arglen_) {
  if (pattern_name_) {
    pattern_name = new char[namelen_ + 1];
    strcpy(pattern_name, pattern_name_);
    namelen = namelen_;
  } else {
    pattern_name = 0;
    namelen = 0;
  }
};

short SetTerminalCharset::process(SqlciEnv *sqlci_env) {
  HandleCLIErrorInit();

  char *tcs = get_argument();
  int tcs_len;

  if (tcs != NULL && ((tcs_len = strlen(tcs)) <= 128)) {
    char tcs_uppercase[129];
    str_cpy_convert(tcs_uppercase, tcs, tcs_len, 1);
    tcs_uppercase[tcs_len] = 0;

    if (CharInfo::isCharSetSupported(tcs_uppercase) == FALSE) {
      SqlciError(SQLCI_INVALID_TERMINAL_CHARSET_NAME_ERROR, (ErrorParam *)0);
      return 0;
    }

    if (CharInfo::isTerminalCharSetSupported(tcs_uppercase) == FALSE) {
      ErrorParam *ep = new ErrorParam(tcs);
      SqlciError(2038, ep, (ErrorParam *)0);
      delete ep;
      return 0;
    }

    // The following code had been commented out but has been restored
    //  for the charset project		CQD removed on 12/11/2007
    /*
         char cqd_stmt[200]; // charset name can be up to 128 bytes long

         sprintf(cqd_stmt, "CONTROL QUERY DEFAULT TERMINAL_CHARSET '%s';",
                           tcs_uppercase
                );

         long retcode = SqlCmd::executeQuery(cqd_stmt, sqlci_env);

         if ( retcode == 0 )*/
    sqlci_env->setTerminalCharset(CharInfo::getCharSetEnum(tcs_uppercase));
    //     else
    //       HandleCLIError(retcode, sqlci_env);

  } else
    SqlciError(SQLCI_INVALID_TERMINAL_CHARSET_NAME_ERROR, (ErrorParam *)0);

  return 0;
}

short SetIsoMapping::process(SqlciEnv *sqlci_env) {
  HandleCLIErrorInit();

  char *omcs = get_argument();
  int omcs_len;

  if (omcs != NULL && ((omcs_len = strlen(omcs)) <= 128)) {
    char omcs_uppercase[129];
    str_cpy_convert(omcs_uppercase, omcs, omcs_len, 1);
    omcs_uppercase[omcs_len] = 0;

    if (strcmp(omcs_uppercase, "ISO88591") != 0) {
      // 15001 42000 99999 BEGINNER MAJOR DBADMIN
      // A syntax error occurred at or before: $0~string0
      ErrorParam *ep = new ErrorParam(omcs);
      SqlciError(15001, ep, (ErrorParam *)0);
      delete ep;
      return 0;
    }

  } else {
    // 15001 42000 99999 BEGINNER MAJOR DBADMIN
    // A syntax error occurred at or before: $0~string0
    ErrorParam *ep;
    if (omcs)
      ep = new ErrorParam(omcs);
    else
      ep = new ErrorParam("ISO_MAPPING");
    SqlciError(15001, ep, (ErrorParam *)0);
    delete ep;
  }

  return 0;
}

short SetDefaultCharset::process(SqlciEnv *sqlci_env) {
  HandleCLIErrorInit();

  char *dcs = get_argument();
  int dcs_len;

  if (dcs != NULL && ((dcs_len = strlen(dcs)) <= 128)) {
    char dcs_uppercase[129];
    str_cpy_convert(dcs_uppercase, dcs, dcs_len, 1);
    dcs_uppercase[dcs_len] = 0;

    if (strcmp(dcs_uppercase, "ISO88591") != 0 && strcmp(dcs_uppercase, "UTF8") != 0 &&
        strcmp(dcs_uppercase, "SJIS") != 0) {
      // 15001 42000 99999 BEGINNER MAJOR DBADMIN
      // A syntax error occurred at or before: $0~string0
      ErrorParam *ep = new ErrorParam(dcs);
      SqlciError(15001, ep, (ErrorParam *)0);
      delete ep;
      return 0;
    }

    char cqd_stmt[200];  // charset name can be up to 128 bytes long

    sprintf(cqd_stmt, "CONTROL QUERY DEFAULT DEFAULT_CHARSET '%s';", dcs_uppercase);

    int retcode = SqlCmd::executeQuery(cqd_stmt, sqlci_env);

    if (retcode == 0)
      sqlci_env->setDefaultCharset(CharInfo::getCharSetEnum(dcs_uppercase));
    else
      HandleCLIError(retcode, sqlci_env);

  } else {
    // 15001 42000 99999 BEGINNER MAJOR DBADMIN
    // A syntax error occurred at or before: $0~string0
    ErrorParam *ep;
    if (dcs)
      ep = new ErrorParam(dcs);
    else
      ep = new ErrorParam("DEFAULT_CHARSET");
    SqlciError(15001, ep, (ErrorParam *)0);
    delete ep;
  }

  return 0;
}

short SetInferCharset::process(SqlciEnv *sqlci_env) {
  HandleCLIErrorInit();

  char *ics = get_argument();
  int ics_len;

  if (ics != NULL && ((ics_len = strlen(ics)) <= 128)) {
    char ics_uppercase[129];
    str_cpy_convert(ics_uppercase, ics, ics_len, 1);
    ics_uppercase[ics_len] = 0;

    if (strcmp(ics_uppercase, "FALSE") != 0 && strcmp(ics_uppercase, "TRUE") != 0 && strcmp(ics_uppercase, "0") != 0 &&
        strcmp(ics_uppercase, "1") != 0) {
      // 15001 42000 99999 BEGINNER MAJOR DBADMIN
      // A syntax error occurred at or before: $0~string0
      ErrorParam *ep = new ErrorParam(ics);
      SqlciError(15001, ep, (ErrorParam *)0);
      delete ep;
      return 0;
    }

    char cqd_stmt[200];  // charset name can be up to 128 bytes long

    sprintf(cqd_stmt, "CONTROL QUERY DEFAULT INFER_CHARSET '%s';", ics_uppercase);

    int retcode = SqlCmd::executeQuery(cqd_stmt, sqlci_env);

    if (retcode == 0) {
      if (ics_uppercase[0] == '1' || ics_uppercase[0] == 'T' /*RUE*/)
        sqlci_env->setInferCharset(TRUE);
      else
        sqlci_env->setInferCharset(FALSE);
    } else
      HandleCLIError(retcode, sqlci_env);

  } else {
    // 15001 42000 99999 BEGINNER MAJOR DBADMIN
    // A syntax error occurred at or before: $0~string0
    ErrorParam *ep;
    if (ics)
      ep = new ErrorParam(ics);
    else
      ep = new ErrorParam("INFER_CHARSET");
    SqlciError(15001, ep, (ErrorParam *)0);
    delete ep;
  }

  return 0;
}

SleepVal::SleepVal(int v) : SqlciCmd(SqlciCmd::SLEEP_TYPE), val_(v){};

Wait::Wait(char *argument_, int arglen_) : SqlciCmd(SqlciCmd::WAIT_TYPE, argument_, arglen_){};

//////////////////////////////////////////////////
short Exit::process(SqlciEnv *sqlci_env) {
  // Default is to exit sqlci. In the special case of an
  // active transaction, the user may choose not to exit.
  short retval = -1;

  // tell CLI that this user session is finished.
  SqlCmd::executeQuery("SET SESSION DEFAULT SQL_SESSION 'DROP';", sqlci_env);

  if (retval == -1) {
    sqlci_env->get_logfile()->WriteAll("\nEnd of MXCI Session\n");  // ##I18N
  }

  return retval;
}

//////////////////////////////////////////////
// Begin ERROR
////////////////////////////////////////////////
short SubError::process(SqlciEnv *sqlci_env) {
  NAWchar *error_msg;
  int codeE, codeW;
  char stateE[10], stateW[10];
  NABoolean msgNotFound;
  ErrorType errType = (type == HBASE_ ? SUBERROR_HBASE : SUBERROR_TM);
  ostringstream omsg;

  codeW = ABS(atoi(get_argument()));  // >= 0, i.e. warning
  codeE = -codeW;                     // <= 0, i.e. error

  msgNotFound = GetErrorMessage(errType, codeE, error_msg, ERROR_TEXT);

  if (!msgNotFound || codeE == 0)  // SQL "success", special case
  {
    NAWriteConsole(error_msg, omsg, TRUE);

    if (codeE != 0) {
      msgNotFound = GetErrorMessage(errType, codeE, error_msg, CAUSE_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);

      msgNotFound = GetErrorMessage(errType, codeE, error_msg, EFFECT_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);

      msgNotFound = GetErrorMessage(errType, codeE, error_msg, RECOVERY_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);
    }
  } else {
    char errMsg[1024];
    sprintf(errMsg, "*** ERROR[%d]\n*** ERROR[16001] The error number %d is not used in SQL.\n", codeW, codeW);
    omsg << errMsg;
  }

  omsg << ends;  // to tack on a null-terminator
  sqlci_env->get_logfile()->WriteAllWithoutEOL(omsg.str().c_str());
  return 0;
}

//////////////////////////////////////////////
// Begin ERROR
////////////////////////////////////////////////
short Error::process(SqlciEnv *sqlci_env) {
  NAWchar *error_msg;
  int codeE, codeW;
  char stateE[10], stateW[10];
  NABoolean msgNotFound;

  ostringstream omsg;

#define GETERRORMESS(in, out, typ) GetErrorMessage(MAIN_ERROR, in, (NAWchar *&)out, typ)

  codeW = ABS(atoi(get_argument()));  // >= 0, i.e. warning
  codeE = -codeW;                     // <= 0, i.e. error

  // These calls must be done before any of the GETERRORMESS(),
  // as they all use (overwrite) the same static buffer in GetErrorMessage.cpp.
  ComSQLSTATE(codeE, stateE);
  ComSQLSTATE(codeW, stateW);
  msgNotFound = GETERRORMESS(codeE, error_msg, ERROR_TEXT);

  if (type == ENVCMD_) {
    if (msgNotFound) {
      error_msg[0] = NAWchar('\n');
      error_msg[1] = NAWchar('\0');
    } else {
      // Extract the {braces-enclosed version string}
      // that msgfileVrsn.ksh (called by GenErrComp.bat)
      // has put into this ENVCMD_ message.
      NAWchar *v = NAWstrchr(error_msg, NAWchar(']'));
      if (v) error_msg = v;
      v = NAWstrchr(error_msg, NAWchar('{'));
      if (v) error_msg = v;
      v = NAWstrchr(error_msg, NAWchar('}'));
      if (v++ && *v == NAWchar('.')) *v = NAWchar(' ');
    }
    NAWriteConsole(error_msg, omsg, FALSE);
  } else if (!msgNotFound || codeE == 0)  // SQL "success", special case
  {
    omsg << "\n*** SQLSTATE (Err): " << stateE << " SQLSTATE (Warn): " << stateW;
    omsg << endl;
    NAWriteConsole(error_msg, omsg, TRUE);

    if (codeE != 0) {
      msgNotFound = GETERRORMESS(codeE, error_msg, CAUSE_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);

      msgNotFound = GETERRORMESS(codeE, error_msg, EFFECT_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);

      msgNotFound = GETERRORMESS(codeE, error_msg, RECOVERY_TEXT);
      NAWriteConsole(error_msg, omsg, TRUE);
    }
  } else {
    // Msg not found.
    ComDiagsArea diags;
    diags << DgSqlCode(codeW);
    NADumpDiags(omsg, &diags, TRUE /*newline*/);
  }

#if 0   // CAUSE/EFFECT/RECOVERY text not implemented yet!
  if (!msgNotFound && type == DETAIL_)
    {
      GETERRORMESS(codeE, error_msg, CAUSE_TEXT);
      NAWriteConsole(error_msg,omsg, TRUE);

      GETERRORMESS(codeE, error_msg, EFFECT_TEXT);
      NAWriteConsole(error_msg,omsg, TRUE);

      GETERRORMESS(codeE, error_msg, RECOVERY_TEXT);
      NAWriteConsole(error_msg,omsg, TRUE);

    }
#endif  // 0		// CAUSE/EFFECT/RECOVERY text not implemented yet!

  omsg << ends;  // to tack on a null-terminator
  sqlci_env->get_logfile()->WriteAllWithoutEOL(omsg.str().c_str());
  return 0;
}

//////////////////////////////////////////////////
// Begin SHAPE
///////////////////////////////////////////////////
short Shape::process(SqlciEnv *sqlci_env) {
  if (!infile_) return 0;

  // open the infile to read Sql statements from
  FILE *fStream = 0;
  fStream = fopen(infile_, "r");
  if (!fStream) {
    ErrorParam *p1 = new ErrorParam(errno);
    ErrorParam *p2 = new ErrorParam(infile_);
    SqlciError(SQLCI_OBEY_FOPEN_ERROR, p1, p2, (ErrorParam *)0);
    delete p1;
    delete p2;
    return 0;
  }

  // close and remember the current logfile
  char *logname = NULL;
  if (sqlci_env->get_logfile()->IsOpen()) {
    logname = new char[strlen(sqlci_env->get_logfile()->Logname()) + 1];
    strcpy(logname, sqlci_env->get_logfile()->Logname());
    sqlci_env->get_logfile()->Close();
  }

  // if infile is the same as outfile, generate output into a temp
  // file(called outfile_ + __temp), and then rename it to infile.
  // Also, rename infile to infile.bak.
  NABoolean tempFile = FALSE;
  char *tempOutfile = NULL;
  if (outfile_) {
    if (strcmp(infile_, outfile_) == 0) {
      tempFile = TRUE;
      tempOutfile = new char[strlen(outfile_) + strlen("__temp") + 1];
      strcpy(tempOutfile, outfile_);
      strcat(tempOutfile, "__temp");
    } else
      tempOutfile = outfile_;

    sqlci_env->get_logfile()->Open(tempOutfile, Logfile::CLEAR_);
  }

  ////////////////////////////////////////////////////////////////
  // BEGIN PROCESS NEXT INPUT STMT
  ////////////////////////////////////////////////////////////////

  short retcode = processNextStmt(sqlci_env, fStream);
  if (retcode) {
    if (logname != NULL) delete[] logname;
    return retcode;
  }

  ////////////////////////////////////////////////////////////////
  // END PROCESS NEXT INPUT STMT
  ////////////////////////////////////////////////////////////////

  fclose(fStream);

  if (outfile_) {
    sqlci_env->get_logfile()->Close();

    char buf[200];
    if (tempFile) {
      snprintf(buf, 200, "/bin/bash mv %s %s.bak", infile_, infile_);
      ShellCmd *shCmd = new Shell(buf);
      shCmd->process(sqlci_env);
      delete shCmd;

      snprintf(buf, 200, "/bin/bash mv %s %s", tempOutfile, infile_);
      shCmd = new Shell(buf);
      shCmd->process(sqlci_env);
      delete shCmd;
    }
  }

  // reopen the original logfile
  if (logname) sqlci_env->get_logfile()->Open(logname, Logfile::APPEND_);

  delete[] logname;
  delete[] tempOutfile;
  tempOutfile = NULL;

  return 0;
}

short Shape::processNextStmt(SqlciEnv *sqlci_env, FILE *fStream) {
  short retcode = 0;

  enum ShapeState { PROCESS_STMT, DONE };

  int done = 0;
  int ignore_toggle = 0;
  ShapeState state;
  InputStmt *input_stmt;
  SqlciNode *sqlci_node = NULL;

  state = PROCESS_STMT;

  while (!done) {
    input_stmt = new InputStmt(sqlci_env);
    int read_error = 0;
    if (state != DONE) {
      read_error = input_stmt->readStmt(fStream, TRUE);

      if (feof(fStream) || read_error == -99) {
        if (!input_stmt->isEmpty() && read_error != -4) {
          // Unterminated statement in obey file.
          // Make the parser emit an error message.
          input_stmt->display((UInt16)0, TRUE);
          input_stmt->logStmt(TRUE);
          input_stmt->syntaxErrorOnEof();
        }
        state = DONE;
      }  // feof or error (=-99)
    }

    // if there is an eof directly after a statement
    // that is terminated with a semi-colon, process the
    // statement
    if (read_error == -4) state = PROCESS_STMT;

    switch (state) {
      case PROCESS_STMT: {
        int ignore_stmt = input_stmt->isIgnoreStmt();
        if (ignore_stmt) ignore_toggle = ~ignore_toggle;

        if (ignore_stmt || ignore_toggle || input_stmt->ignoreJustThis()) {
          // ignore until stmt following the untoggling ?ignore
          sqlci_DA.clear();
        } else {
          if (!read_error || read_error == -4) {
            sqlci_parser(input_stmt->getPackedString(), input_stmt->getPackedString(), &sqlci_node, sqlci_env);
            if ((sqlci_node) && (sqlci_node->getType() == SqlciNode::SQL_CMD_TYPE)) {
              delete sqlci_node;

              SqlCmd sqlCmd(SqlCmd::DML_TYPE, NULL);

              short retcode = sqlCmd.showShape(sqlci_env, input_stmt->getPackedString());
              if (retcode) {
                delete input_stmt;
                return retcode;
              }
            }

            input_stmt->display((UInt16)0, TRUE);
            input_stmt->logStmt(TRUE);
          }

          // Clear the DiagnosticsArea for the next command...
          sqlci_DA.clear();

          // if an EXIT statement was seen, then a -1 will be returned
          // from process. We are done in that case.
          if (retcode == -1) state = DONE;
        }
      } break;

      case DONE: {
        done = -1;
      } break;

      default: {
      } break;

    }  // switch on state

    delete input_stmt;

  }  // while not done

  return 0;
}

//////////////////////////////////////////////////
short Wait::process(SqlciEnv *sqlci_env) {
  char buf[100];

  cout << "Enter a character + RETURN to continue: ";
  cin >> buf;

  return 0;
}

//////////////////////////////////////////////////
short SleepVal::process(SqlciEnv *sqlci_env) {
  DELAY(val_ * 100);

  return 0;
}

//////////////////////////////////////////////////
short ParserFlags::process(SqlciEnv *sqlci_env) {
  int retCode;

  if (!ComUser::isRootUserID()) {
    // Return - "not authorized" error
    ComDiagsArea diags;
    diags << DgSqlCode(-1017);
    handleLocalError(&diags, sqlci_env);
    return -1;
  }

  if (opType == DO_SET) {
    if (param == 0) {
      // Warning 3190:
      // Please use "RESET PARSERFLAGS <value>" to reset the flags.
      ComDiagsArea diags;
      diags << DgSqlCode(3190);
      handleLocalError(&diags, sqlci_env);
    }
    retCode = SQL_EXEC_SetParserFlagsForExSqlComp_Internal2(param);
  }

  else {
    // It's DO_RESET
    retCode = SQL_EXEC_ResetParserFlagsForExSqlComp_Internal2(param);
  }

  if (retCode) {
    // This is most probably error 1017:
    // You are not authorized to perform this operation.
    ComDiagsArea diags;
    diags << DgSqlCode(retCode);
    handleLocalError(&diags, sqlci_env);
  }
  return 0;
}

///////////////////////////////
// Process of the MODE Command//
///////////////////////////////

//   SQL is the normal mode in which MXCI executes.
short Mode::process(SqlciEnv *sqlci_env) {
  short retcode = 1;
  switch (type) {
    case SQL_:
      retcode = process_sql(sqlci_env);
      break;
    default:
      SqlciError(SQLCI_INVALID_MODE, (ErrorParam *)0);
      break;
  }
  return retcode;
}

short Mode::process_sql(SqlciEnv *sqlci_env) {
  if (SqlciEnv::SQL_ == sqlci_env->getMode()) {
    SqlciError(SQLCI_RW_MODE_ALREADY_SQL, (ErrorParam *)0);
    return 0;
  }

  sqlci_env->setMode(SqlciEnv::SQL_);

  return 0;
}
