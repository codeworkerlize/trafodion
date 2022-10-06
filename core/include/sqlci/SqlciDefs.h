#pragma once

enum dml_type {
  DML_CONTROL_TYPE,
  DML_SELECT_TYPE,
  DML_UPDATE_TYPE,
  DML_INSERT_TYPE,
  DML_DELETE_TYPE,
  DML_DDL_TYPE,
  DML_DESCRIBE_TYPE,
  DML_SHOWSHAPE_TYPE,
  DML_DISPLAY_NO_ROWS_TYPE,
  DML_DISPLAY_NO_HEADING_TYPE,
  DML_CALL_STMT_TYPE,
  DML_CALL_STMT_RS_TYPE,
  DML_UNLOAD_TYPE,
  DML_OSIM_TYPE
};

enum SQLCI_CLI_RETCODE {
  SQL_Success = 0,
  SQL_Eof = 100,
  SQL_Error = -1,
  SQL_Warning = 1,
  SQL_Canceled = -8007,
  SQL_Rejected = -15026

};

// A simple structure used by the sqlci parser to hold information
// about a cursor
struct SqlciCursorInfo {
  int queryTextSpecified_;
  char *queryTextOrStmtName_;
  int resultSetIndex_;
  SqlciCursorInfo() {
    queryTextSpecified_ = 0;
    queryTextOrStmtName_ = 0;
    resultSetIndex_ = 0;
  }
};
