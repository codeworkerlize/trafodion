
#include "arkcmp/CmpEHCallBack.h"

#include <signal.h>
#include <stdio.h>

#include "arkcmp/CmpStatement.h"
#include "common/CmpCommon.h"
#include "common/NAError.h"
#include "common/Platform.h"
#include "sqlci/SqlciParseGlobals.h"

//-------------------------------------------------------------------------
// The following objects are used by Tandem Failure Data System (TFDS)
// to record state of process when software errors occur. First Failure Data
// Capture (FFDC) is carried out TFDS runtime library which is linked with
// the MX compiler. Currently this is switched on only for NSK.
//------------------------------------------------------------------------

//*******************************************************************
// Only for NSK: This routine gets a pointer to the user SQL query
//********************************************************************

//*******************************************************************
// For NSK only:This routine records stack trace at time of exception;
// An NSK event viewer can look at the stack trace.
// Control is returned to the compiler process by TFDS system.
// For NT only: this is a no-op.
//********************************************************************
void makeTFDSCall(const char *msg, const char *file, UInt32 line) {}

void CmpEHCallBack::doFFDC() { makeTFDSCall("", __FILE__, __LINE__); }

void CmpEHCallBack::dumpDiags() {
  // Do not clone this code; it's a bit kludgy.
  // If needed, rewrite arkcmp.cpp main and StaticCompiler try..catch,
  // *or* enhance diags ComCondition with a displayCount_ member
  // which dumpDiags looks at and will only display the cond if not already
  // done so (and then we'd replace this whole if{}else{} with a simple
  // dumpDiags *without* a clear).
  //
  if (CmpCommon::context()->GetMode() == STMT_STATIC) {
    // StaticCompiler.cpp needs us to display these before
    // its diags go out of scope in the longjmp stack unwind,
    // because static comp lets arkcmp.cpp main do the catch
    // (its caller, not itself).
    if (CmpCommon::diags()->getNumber()) {
      cerr << endl << "--- ERROR/EXCEPTION ..." << endl;
      CmpCommon::dumpDiags(cerr, TRUE /*newline*/);
      CmpCommon::diags()->clear();
    }
  } else {
    // Dynamic compilation will catch this exception itself;
    // it relies on the presence of *uncleared* diags conditions
    // to tell if there was an error.
  }

  ARKCMP_EXCEPTION_EPILOGUE("EHException");
}

//*********************************************************************
// Trap Handler is invoked when conditions can't be explicitly checked
// The TFDS system invokes this routine when operating system throws
// an exception.  An example would be when the process runs out of memory.
// Control is not returned to the compiler process.
//*********************************************************************

static int sigTypes[] = {SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM};

static const char *sigNames[] = {"SIGABRT", "SIGFPE", "SIGILL", "SIGINT", "SIGSEGV", "SIGTERM"};

void printSignalHandlers() {
  void (*oldHdlr)(int);
  void (*newHdlr)(int);
  for (int i = 0; i < sizeof(sigTypes) / sizeof(int); i++) {
    newHdlr = SIG_DFL;
    oldHdlr = signal(sigTypes[i], newHdlr);
    if (oldHdlr == SIG_ERR) {
      cerr << "signal() failed, cannot change " << sigNames[i] << " to the default handler" << endl;
      exit(-1);
    }
    printf("%s %p\n", sigNames[i], oldHdlr);
    newHdlr = signal(sigTypes[i], oldHdlr);
    if (oldHdlr == SIG_ERR) {
      cerr << "signal() failed, cannot restore " << sigNames[i] << " to the original handler" << endl;
      exit(-1);
    }
  }
}
