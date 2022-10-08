
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         arkcmp_proc.cpp
 * Description:  This is the main entry point for arkcmp procedural interface.
 *
 *
 *
 * Created:      7/9/2013
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include <string.h>

#include <fstream>

#include "common/Platform.h"
#include "seabed/fs.h"
#include "seabed/ms.h"
#include "sqlcomp/NewDel.h"
extern void my_mpi_fclose();
#include "common/SCMVersHelp.h"
DEFINE_DOVERS(tdm_arkcmp)

#include "arkcmp/CmpConnection.h"
#include "arkcmp/CmpContext.h"
#include "arkcmp/CmpErrLog.h"
#include "arkcmp/CmpStoredProc.h"
#include "arkcmp/CompException.h"
#include "comexe/CmpMessage.h"
#include "common/CmpCommon.h"
// #include "StaticCompiler.h"
#include "arkcmp/NATableSt.h"
#include "arkcmp/QueryCacheSt.h"
#include "common/ComCextdecs.h"
#include "sqlcomp/QCache.h"
#include "sqlmxevents/logmxevent.h"

#define CLI_DLL
#include "cli/SQLCLIdev.h"
#undef CLI_DLL

#include "cli/CliSemaphore.h"
#include "arkcmp/CmpEHCallBack.h"
#include "cli/Globals.h"
#include "eh/EHException.h"
#include "optimizer/ObjectNames.h"

ostream &operator<<(ostream &dest, const ComDiagsArea &da);

// mainNewHandler_CharSave and mainNewHandler are used in the error
// handling when running out of virtual memory for the main program.
//
// Save 4K bytes of memory for the error handling when running out of VM.

static char *mainNewHandler_CharSave = new char[4096];

static int mainNewHandler(size_t s)

{
  if (mainNewHandler_CharSave) {
    delete[] mainNewHandler_CharSave;
    mainNewHandler_CharSave = NULL;
    CmpErrLog((char *)"Memory allocation failure");
    ArkcmpFatalError(ARKCMP_ERROR_PREFIX "Out of virtual memory.", NOMEM_SEV);
  }
  return 0;
}

static void longJumpHandler() {
  // check if we have emergency memory for reporting error.
  if (mainNewHandler_CharSave == 0) exit(1);

  delete[] mainNewHandler_CharSave;
  mainNewHandler_CharSave = 0;
  ArkcmpFatalError(ARKCMP_ERROR_PREFIX "Assertion failed (thrown by longjmp).");
}

extern THREAD_P jmp_buf CmpInternalErrorJmpBuf;

static void initializeArkcmp() {
  CmpInternalErrorJmpBufPtr = &CmpInternalErrorJmpBuf;
  // noop for now
}

void deinitializeArkcmp() {
  // noop for now
}

// RETURN:  0, no error.
//          2, error during NADefaults creation.
//          1, other errors during CmpContext creation.
int arkcmp_main_entry() {
#ifdef _DEBUG
  if (getenv("SQL_CMP_MSGBOX_PROCESS") != NULL)
    MessageBox(NULL, "Server: Process Launched", "tdm_arkcmp", MB_OK | MB_ICONINFORMATION);
  // The following call causes output messages to be displayed in the
  // same order on NSK and Windows.
  cout.sync_with_stdio();
#endif
  initializeArkcmp();
  try {
    {  // a local ctor scope, within a try block
      // Set up the context info for the connection, it contains the variables
      // persistent through each statement loops.

      CmpContext *context = NULL;
      NAHeap *parentHeap = GetCliGlobals()->getCurrContextHeap();
      NAHeap *cmpContextHeap = new (parentHeap) NAHeap((char *)"Cmp Context Heap", parentHeap, (int)524288);
      try {
        CLISemaphore *cliSemaphore;
        cliSemaphore = GetCliGlobals()->getSemaphore();
        cliSemaphore->get();
        context = new (cmpContextHeap) CmpContext(CmpContext::IS_DYNAMIC_SQL, cmpContextHeap);
        cliSemaphore->release();

        if (!context->getSchemaDB()->getDefaults().getSqlParser_NADefaults_Ptr()) {
          // error during nadefault creation.
          // Cannot proceed.
          ArkcmpErrorMessageBox(ARKCMP_ERROR_PREFIX "- Cannot initialize NADefaults data.", ERROR_SEV, FALSE, FALSE,
                                TRUE);
          return (2);
        }

      } catch (...) {
        ArkcmpErrorMessageBox(ARKCMP_ERROR_PREFIX "- Cannot initialize Compiler global data.", ERROR_SEV, FALSE, FALSE,
                              TRUE);
        return (1);
      }

      //  moved down the IdentifyMyself so that it can be determined that the
      //  context has not yet been set up
      IdentifyMyself::SetMyName(I_AM_EMBEDDED_SQL_COMPILER);
      context->setIsEmbeddedArkcmp(TRUE);
      context->initContextGlobals();
      context->envs()->cleanup();

      // Clear the CLI globals context area if there are any leftover CLI diags
      // This is a fresh context and shouldn't have CLI diags.
      SQL_EXEC_ClearDiagnostics(NULL);

      assert(cmpCurrentContext == context);

      // Clear the SQL text buffer in the event logging area.
      cmpCurrentContext->resetLogmxEventSqlText();
    }

  }

  catch (BaseException &bE) {
    char msg[500];
    sprintf(msg, "%s BaseException: %s %d", ARKCMP_ERROR_PREFIX, bE.getFileName(), bE.getLineNum());
    ArkcmpFatalError(msg);
  } catch (...) {
    ArkcmpFatalError(ARKCMP_ERROR_PREFIX "Fatal exception.");
  }

  return 0;
}

void arkcmp_main_exit() {}
