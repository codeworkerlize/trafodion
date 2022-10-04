
#ifndef SHELLCMD_H
#define SHELLCMD_H


#include "sqlci/SqlciNode.h"
#include "sqlci/SqlciEnv.h"

class ShellCmd : public SqlciNode {
 public:
  enum shell_cmd_type { CD_TYPE, LS_TYPE, SHELL_TYPE };

 private:
  shell_cmd_type cmd_type;
  char *argument;

 public:
  ShellCmd(const shell_cmd_type cmd_type_, char *argument_);
  ~ShellCmd();
  inline char *get_argument() { return argument; };
};

class Chdir : public ShellCmd {
 public:
  Chdir(char *argument_);
  ~Chdir(){};
  short process(SqlciEnv *sqlci_env);
};

class Ls : public ShellCmd {
 public:
  Ls(char *argument_);
  ~Ls(){};
  short process(SqlciEnv *sqlci_env);
};

class Shell : public ShellCmd {
 public:
  Shell(char *argument_);
  ~Shell(){};
  short process(SqlciEnv *sqlci_env);
};

#endif
