/* -*-C++-*-
 *****************************************************************************
 *
 * File:         EHAbort.C
 * Description:  global function to abort the current process
 *               The global function EHAbort was derived from
 *               the function CascadesAbort in the file opt.C
 *
 *
 * Created:      5/18/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include <stdio.h>
#include <stdlib.h>

#include "EHCommonDefs.h"
#include "sqlmxevents/logmxevent.h"
// -----------------------------------------------------------------------
// global function to abort the process
// -----------------------------------------------------------------------
void EHAbort(const char *filename, int lineno, const char *msg) {
  fflush(stdout);
  fprintf(stderr,
          "\n*** Fatal Error *** Exception handler aborted "
          "in \"%s\", line %d:\n%s\n\n",
          filename, lineno, msg);
  fflush(stderr);

  SQLMXLoggingArea::logSQLMXAbortEvent(filename, lineno, msg);

// This env variable should be used by development only and only when
// the tdm_nonstop is started from the command line (windows up).
#if defined(DEBUG) || defined(_DEBUG)
  if (getenv("SQLMX_FAILURE")) {
    char message[256];
    LPCSTR messagep = (const char *)&message;
    char line__[9];

    strcpy(message, "EHAbort: *** Fatal Error *** Exception handler aborted in ");
    strcat(message, filename);
    strcat(message, ", line ");
#ifdef NA_ITOA_NOT_SUPPORTED
    snprintf(line__, 9, "%d", lineno);
#else
    itoa(lineno, line__, 10);
#endif  // NA_ITOA_NOT_SUPPORTED
    strcat(message, line__);
    strcat(message, msg);

    MessageBox(NULL, (CHAR *)&message, "NonStop SQL/MX", MB_OK | MB_ICONINFORMATION);
  };
#endif  // DEBUG || _DEBUG

  exit(1);

}  // EHAbort()

//
// End of File
//
