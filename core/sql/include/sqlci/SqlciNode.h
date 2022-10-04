#pragma once

#include "common/Platform.h"
#include "common/SqlCliDllDefines.h"
#include "common/NABoolean.h"
#include <string.h>

class SqlciEnv;

class SqlciNode {
 public:
  enum sqlci_node_type {
    SQLCI_CMD_TYPE,
    SQL_CMD_TYPE,
    UTIL_CMD_TYPE,
    SHELL_CMD_TYPE,
    SQLCLI_CMD_TYPE,
    REPORT_CMD_TYPE,
    MXCS_CMD_TYPE
  };

 private:
  char eye_catcher[4];
  sqlci_node_type node_type;
  SqlciNode *next;
  int errcode;

 public:
  SqlciNode(const sqlci_node_type);
  virtual ~SqlciNode();
  virtual short process(SqlciEnv *sqlci_env);

  void set_next(SqlciNode *next_) { next = next_; }
  SqlciNode *get_next() const { return next; }

  int errorCode() const { return errcode; }
  void setErrorCode(int e) { errcode = e; }

  bool isSqlciNode() const { return strncmp(eye_catcher, "CI  ", 4) == 0; }

  sqlci_node_type getType() { return node_type; }
};
