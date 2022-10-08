

#include "common/Platform.h"

#define CLI_GLOBALS_DEF_

#include <stdlib.h>

#include "cli/cli_stdh.h"
//#include "common/Ipc.h"
//#include "executor/ex_stdh.h"
//#include "executor/ex_frag_rt.h"

// This DLL exports the global variables used in executor.
// Since executor libraries are packaged in 2 ways ( user )
// . tdm_sqlcli.dll, this is for application programmer
// . cli, executor, exp, common, ..etc. static linked libs
//   for internal components
// For a program both statically and dynamically linked in these
// libraries gets more than one set of global variables which causes
// problems. So the globals used in executor are extracted into this
// DLL and linked in to tdm_sqlcli.dll , also any other programs needs
// to link cli.lib

__declspec(dllexport) CliGlobals *cli_globals = 0;
THREAD_P jmp_buf ExportJmpBuf;
