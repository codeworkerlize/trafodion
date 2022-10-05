

#ifndef SQLCICMD_H
#define SQLCICMD_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SqlciCmd.h
 * Description:
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "sqlci/SqlciEnv.h"
#include "sqlci/SqlciNode.h"

class SqlciCmd : public SqlciNode {
 public:
  enum sqlci_cmd_type {
    ERROR_TYPE,
    EXIT_TYPE,
    FC_TYPE,
    HELP_TYPE,
    HISTORY_TYPE,
    LISTCOUNT_TYPE,
    VERBOSE_TYPE,
    PARSERFLAGS_TYPE,
    LOG_TYPE,
    OBEY_TYPE,
    REPEAT_TYPE,
    SETENVVAR_TYPE,
    SETPARAM_TYPE,
    SETPATTERN_TYPE,
    SET_TERMINAL_CHARSET_TYPE,
    SHOW_TYPE,
    STATISTICS_TYPE,
    SHAPE_TYPE,
    WAIT_TYPE,
    SLEEP_TYPE,
    MODE_TYPE,
    QUERYID_TYPE,
    SET_ISO_MAPPING_TYPE,
    SET_DEFAULT_CHARSET_TYPE,
    SET_INFER_CHARSET_TYPE,
    USER_TYPE
  };

 private:
  sqlci_cmd_type cmd_type;
  char *argument;
  int arglen;
  int numeric_arg;

 public:
  SqlciCmd(const sqlci_cmd_type cmd_type_);
  SqlciCmd(const sqlci_cmd_type cmd_type_, char *, int);
  SqlciCmd(const sqlci_cmd_type cmd_type_, NAWchar *, int);
  SqlciCmd(const sqlci_cmd_type cmd_type_, int);
  ~SqlciCmd();
  inline char *get_argument(char *dummy_arg = 0) { return argument; };
  inline int get_arglen() { return arglen; };
};

class Shape : public SqlciCmd {
 private:
  NABoolean type_;
  char *infile_;
  char *outfile_;

 public:
  Shape(NABoolean, char *, char *);
  ~Shape(){};
  short process(SqlciEnv *sqlci_env);
  short processNextStmt(SqlciEnv *sqlci_env, FILE *fStream);
};

class Statistics : public SqlciCmd {
 public:
  enum StatsCmdType { SET_ON, SET_OFF };

 private:
  StatsCmdType type_;
  char *statsOptions_;

 public:
  Statistics(char *, int arglen_, StatsCmdType, char *statsOptions);
  ~Statistics();
  short process(SqlciEnv *sqlci_env);

  char *getStatsOptions() { return statsOptions_; }
};

class Log : public SqlciCmd {
 public:
  enum log_type { CLEAR_, APPEND_, STOP_ };

 private:
  log_type type;
  int commandsOnly_;

 public:
  Log(char *, int arglen_, log_type type_, int commands_only);
  ~Log(){};
  short process(SqlciEnv *sqlci_env);
};

class Mode : public SqlciCmd {
 public:
  enum ModeType { SQL_ };
  Mode(ModeType type, NABoolean value);
  ~Mode(){};
  short process(SqlciEnv *sqlci_env);

 private:
  ModeType type;
  NABoolean value;
  short process_sql(SqlciEnv *sqlci_env);
  short process_display(SqlciEnv *sqlci_env);
};

class ParserFlags : public SqlciCmd {
 public:
  enum ParserFlagsOperation { DO_SET, DO_RESET };

 private:
  int param;
  ParserFlagsOperation opType;

 public:
  ParserFlags(ParserFlagsOperation, int param_);
  ~ParserFlags(){};
  short process(SqlciEnv *sqlci_env);
};

class SetTerminalCharset : public SqlciCmd {
 private:
 public:
  SetTerminalCharset(char *new_cs_name) : SqlciCmd(SET_TERMINAL_CHARSET_TYPE, new_cs_name, strlen(new_cs_name)){};
  ~SetTerminalCharset(){};
  short process(SqlciEnv *sqlci_env);
};

class SetIsoMapping : public SqlciCmd {
 private:
 public:
  SetIsoMapping(char *new_cs_name) : SqlciCmd(SET_ISO_MAPPING_TYPE, new_cs_name, strlen(new_cs_name)){};
  ~SetIsoMapping(){};
  short process(SqlciEnv *sqlci_env);
};

class SetDefaultCharset : public SqlciCmd {
 private:
 public:
  SetDefaultCharset(char *new_cs_name) : SqlciCmd(SET_DEFAULT_CHARSET_TYPE, new_cs_name, strlen(new_cs_name)){};
  ~SetDefaultCharset(){};
  short process(SqlciEnv *sqlci_env);
};

class SetInferCharset : public SqlciCmd {
 private:
 public:
  SetInferCharset(char *new_boolean_setting)
      : SqlciCmd(SET_INFER_CHARSET_TYPE, new_boolean_setting, strlen(new_boolean_setting)){};
  ~SetInferCharset(){};
  short process(SqlciEnv *sqlci_env);
};

class Error : public SqlciCmd {
 public:
  enum error_type { BRIEF_, DETAIL_, ENVCMD_ };

 private:
  error_type type;

 public:
  Error(char *, int arglen_, error_type type_);
  ~Error(){};
  short process(SqlciEnv *sqlci_env);
};

class SubError : public SqlciCmd {
 public:
  enum suberror_type { HBASE_, TM_ };

 private:
  suberror_type type;

 public:
  SubError(char *, int arglen_, suberror_type type_);
  ~SubError(){};
  short process(SqlciEnv *sqlci_env);
};

class Help : public SqlciCmd {
 public:
  enum help_type { SYNTAX_, EXAMPLE_, DETAIL_ };

 private:
  help_type type;

 public:
  Help(char *, int, help_type);
  ~Help(){};
  short process(SqlciEnv *sqlci_env);
};

class Exit : public SqlciCmd {
 public:
  Exit(char *, int arglen_);
  short process(SqlciEnv *sqlci_env);
};

class Reset : public SqlciCmd {
 public:
  enum reset_type { PARAM_, PATTERN_, PREPARED_, CONTROL_ };

 private:
  reset_type type;
  short reset_control(SqlciEnv *sqlci_env);
  short reset_param(SqlciEnv *sqlci_env);
  short reset_pattern(SqlciEnv *sqlci_env);
  short reset_prepared(SqlciEnv *sqlci_env);

 public:
  Reset(reset_type type, char *argument_, int arglen_);
  Reset(reset_type type);
  ~Reset();
  short process(SqlciEnv *sqlci_env);
};

class SetParam : public SqlciCmd {
  char *param_name;
  int namelen;
  CharInfo::CharSet cs;
  NABoolean inSingleByteForm_;
  NABoolean isQuotedStrWithoutCharSetPrefix_;  // set to TRUE in w:/sqlci/sqlci_yacc.y
                                               // if the parameter value is a string
                                               // literal (i.e., quoted string) AND
                                               // the string literal does not have a
                                               // string literal character set prefix;
                                               // otherwise, this data member is set
                                               // to FALSE.
  NAWchar *m_convUTF16ParamStrLit;             // When isQuotedStrWithoutCharSetPrefix_ is TRUE,
                                               // this data member points to the UTF16 string
                                               // literal equivalent to the specified quoted
                                               // string parameter; otherwise, this data member
                                               // is set to NULL.
  CharInfo::CharSet m_termCS;                  // When isQuotedStrWithoutCharSetPrefix_ is TRUE, this
                                               // data member contains the TERMINAL_CHARSET CQD
                                               // setting at the time the SET PARAM command was
                                               // executed; otherwise, this data member is set to
                                               // CharInfo:UnknownCharSet.

 public:
  // if arglen_ passed in is -1, then set param to null value.
  SetParam(char *, int, char *, int arglen_, CharInfo::CharSet cs = CharInfo::UnknownCharSet);
  SetParam(char *, int, NAWchar *, int arglen_, CharInfo::CharSet cs = CharInfo::UnknownCharSet);
  SetParam(char *, int);
  ~SetParam();
  short process(SqlciEnv *sqlci_env);

  CharInfo::CharSet getCharSet() { return cs; };
  NABoolean isInSingleByteForm() { return inSingleByteForm_; };
  NAWchar *getUTF16ParamStrLit() { return m_convUTF16ParamStrLit; }
  void setUTF16ParamStrLit(const NAWchar *utf16Str, size_t ucs2StrLen);
  CharInfo::CharSet getTermCharSet() const { return m_termCS; }
  void setTermCharSet(CharInfo::CharSet termCS) { m_termCS = termCS; }
  NABoolean isQuotedStrWithoutCharSetPrefix() const { return isQuotedStrWithoutCharSetPrefix_; }
  void setQuotedStrWithoutPrefixFlag(NABoolean value) { isQuotedStrWithoutCharSetPrefix_ = value; }
};

class SetPattern : public SqlciCmd {
  char *pattern_name;
  int namelen;

 public:
  SetPattern(char *, int, char *, int arglen_);
  SetPattern(char *, int);
  ~SetPattern();

  // this method defined in Param.cpp
  short process(SqlciEnv *sqlci_env);
};

class SleepVal : public SqlciCmd {
 public:
  SleepVal(int);
  ~SleepVal(){};
  short process(SqlciEnv *sqlci_env);

 private:
  int val_;
};

class Wait : public SqlciCmd {
 public:
  Wait(char *, int);
  ~Wait(){};
  short process(SqlciEnv *sqlci_env);
};

#endif
