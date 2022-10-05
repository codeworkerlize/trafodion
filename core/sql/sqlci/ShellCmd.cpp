
#include "common/Platform.h"

#include <iostream>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common/NAString.h"
#include "sqlci/ShellCmd.h"
#include "sqlci/sqlcmd.h"
#include "sqlci/SqlciError.h"

ShellCmd::ShellCmd(const shell_cmd_type cmd_type_, char *argument_)
    : SqlciNode(SqlciNode::SHELL_CMD_TYPE), cmd_type(cmd_type_) {
  argument = new char[strlen(argument_) + 1];
  strcpy(argument, argument_);
}

ShellCmd::~ShellCmd() { delete[] argument; }

Chdir::Chdir(char *argument_) : ShellCmd(ShellCmd::CD_TYPE, argument_) {}

Ls::Ls(char *argument_) : ShellCmd(ShellCmd::LS_TYPE, argument_) {}

Shell::Shell(char *argument_) : ShellCmd(ShellCmd::SHELL_TYPE, argument_) {}

// Trim leading blanks, and trailing blanks and semicolons.
static char *trim(char *str) {
  while (*str && isspace((unsigned char)*str)) str++;  // advance head ptr to first token  // For VS2003
  size_t len = strlen(str);
  if (len) {  // trim trailing blanks and semicolons
    char *end = &str[len - 1];
    while (*end && (isspace((unsigned char)*end) || *end == ';')) end--;  // For VS2003
    *++end = '\0';
  }
  return str;
}

short Chdir::process(SqlciEnv *sqlci_env) {
  char *cmd = trim(get_argument());

  cmd += 2;  // get past length("cd")

  if (!*cmd)  // empty (i.e. "cd;" was input)
  {
    if (getenv("HOME")) cmd = getenv("HOME");
  } else
    cmd = trim(cmd);  // get past blanks preceding token 2

  NAString dir = LookupDefineName(cmd);

  if (chdir(dir)) {
    // use constructor and destructor when C++ compiler bug is fixed
    ErrorParam *ep1 = new ErrorParam(errno);
    ErrorParam *ep2 = new ErrorParam(dir);

    SqlciError(SQLCI_NO_DIRECTORY, ep1, ep2, (ErrorParam *)0);
    delete ep1;
    delete ep2;

    return 0 /*errno*/;
  }

  return 0;
}

short Ls::process(SqlciEnv *sqlci_env) {
  char *cmd = trim(get_argument());

  if (sqlci_env->isOleServer()) {
    SqlciError(SQLCI_CMD_NOT_SUPPORTED, (ErrorParam *)0);
    return 0;
  }

  memcpy(cmd, "ls", 2);  // Make sure that 'ls' is passed in
                         // lower case to the shell.
  system(cmd);

  return 0;
}

short Shell::process(SqlciEnv *sqlci_env) {
  char *cmd = trim(get_argument());

  // replace any user defined pattern
  //  cmd = SqlCmd::replacePattern(sqlci_env, cmd);

  cmd += 2;         // get past length("sh")
  cmd = trim(cmd);  // get past blanks preceding token 2

  if (sqlci_env->isOleServer()) {
    SqlciError(SQLCI_CMD_NOT_SUPPORTED, (ErrorParam *)0);
    return 0;
  }

  if (!*cmd)  // empty (i.e. "sh;" was input)
    system("sh");
  else
    system(cmd);

  if (sqlci_env->isInteractiveNow()) cout << endl;

  return 0;
}
