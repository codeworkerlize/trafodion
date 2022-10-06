#include "sqlci/SqlciParser.h"

#include "common/NAAssert.h"
#include "export/ComDiags.h"
#include "sqlci/SqlciNode.h"
#include "sqlci/SqlciParseGlobals.h"
#include "sqlci/sqlcmd.h"

extern ComDiagsArea sqlci_DA;

static int sqlci_parser_subproc(char *instr, char *origstr, SqlciNode **node, SqlciEnv *sqlci_env) {
  int retval = 0;
  *node = nullptr;

  // Set (reset) globals
  SqlciParse_InputStr = origstr;
  SqlciParse_OriginalStr = origstr;
  SqlciParse_InputPos = 0;
  SqlciEnvGlobal = sqlci_env;  // setting the global SqlciEnv to the local sqlci_env

  if (origstr) {
    // remove trailing blanks
    int j = strlen(origstr) - 1;
    while ((j >= 0) && (origstr[j] == ' ')) j--;
    origstr[j + 1] = 0;

    if (j >= 0) {
      SqlciLexReinit();

      retval = sqlciparse();

      // success in parsing
      if (!retval)  // success so far in parsing
      {
        assert(SqlciParseTree->isSqlciNode());

        if (retval = SqlciParseTree->errorCode())  // oops, an error
        {
          delete SqlciParseTree;
          SqlciParseTree = nullptr;
          retval = -ABS(retval);  // error, caller won't retry
        }
      } else
        retval = +ABS(retval);  // error, caller will retry

      if (!retval) {
        *node = SqlciParseTree;  // successful return
      }
    }  // if (j >=0)
  }    // if (origstr)

  return retval;
}

int sqlci_parser(char *instr, char *origstr, SqlciNode **node, SqlciEnv *sqlci_env) {
  int prevDiags = sqlci_DA.getNumber();  // capture this before parsing

  // replace any user defined pattern in the query
  char *newstr = origstr;
  newstr = SqlCmd::replacePattern(sqlci_env, origstr);

  int retval = sqlci_parser_subproc(instr, newstr, node, sqlci_env);

  if (retval) {
    sqlci_parser_syntax_error_cleanup(nullptr, sqlci_env);
    if (!prevDiags && retval > 0)  // NOT the -99 from above!
    {
      sqlci_DA.clear();
      retval = sqlci_parser_subproc(instr, newstr, node, sqlci_env);
      if (retval) sqlci_parser_syntax_error_cleanup(nullptr, sqlci_env);
    }
  }

  if (retval > 0) {
    SqlciParse_OriginalStr = origstr;
  }

  if (newstr != origstr) delete[] newstr;

  return retval;
}

int sqlci_parser_syntax_error_cleanup(char *instr, SqlciEnv *sqlci_env) {
  SqlciNode *sqlci_node;

  if (instr) sqlci_parser_subproc(instr, instr, &sqlci_node, sqlci_env);

  SqlciParse_HelpCmd = -1;
  char junk[4];
  strcpy(junk, ".;");
  sqlci_parser_subproc(junk, junk, &sqlci_node, sqlci_env);
  SqlciParse_HelpCmd = 0;

  return 0;
}
