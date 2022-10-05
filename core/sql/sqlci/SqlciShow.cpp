
#include "common/Platform.h"

#include <stdlib.h>
#include <limits.h>

#include <unistd.h>

#include "sqlci/SqlciEnv.h"
#include "sqlci/SqlciCmd.h"
#include "sqlci/sqlcmd.h"
#include "cli/SQLCLIdev.h"

Show::~Show() {}

short Show::show_control(SqlciEnv *sqlci_env) { return 0; }

short Show::show_cursor(SqlciEnv *sqlci_env) { return 0; }

short Show::show_param(SqlciEnv *sqlci_env) { return 0; }

short Show::show_pattern(SqlciEnv *sqlci_env) { return 0; }

short Show::show_prepared(SqlciEnv *sqlci_env) { return 0; }

short Show::show_session(SqlciEnv *sqlci_env) {
  Env env(NULL, 0);  // the default ENV command
  return env.process(sqlci_env);
}

short Show::show_version(SqlciEnv *sqlci_env) { return 0; }

short Show::process(SqlciEnv *sqlci_env) {
  short retcode = 1;  // assume error

  switch (type) {
    case CONTROL_:
      retcode = show_control(sqlci_env);
      break;

    case CURSOR_:
      retcode = show_cursor(sqlci_env);
      break;

    case PARAM_:
      retcode = show_param(sqlci_env);
      break;

    case PATTERN_:
      retcode = show_pattern(sqlci_env);
      break;

    case PREPARED_:
      retcode = show_prepared(sqlci_env);
      break;

    case SESSION_:
      retcode = show_session(sqlci_env);
      break;

    case VERSION_:
      retcode = show_version(sqlci_env);
      break;

    default:
      break;
  }

  return retcode;
}
