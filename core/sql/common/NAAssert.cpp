
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "common/NAAssert.h"
#include "cli/SQLCLIdev.h"

#include <iostream>

// To debug an inline function:
// - Put a "NAInlineBreakpoint;" statement in the inline function;
// - Optionally edit this function, NAInlineBreakpointFunc,
//   to return based on the parameters (i.e., filter out names of
//   files you aren't interested in breaking on each time),
//   - and rebuild;
// - Set a breakpoint here, after the filtering-out lines you added.
void NAInlineBreakpointFunc(char *file_, int line_) {
  cout << "Tracepoint at: file " << file_ << ", line " << line_ << "\n";
}

NAAssertGlobals *GetNAAssertGlobals(NABoolean *logEmsEvents) { return NULL; }

NABoolean IsExecutor(NABoolean *logEmsEvents) {
  if (logEmsEvents != NULL) *logEmsEvents = TRUE;
  return FALSE;
}
