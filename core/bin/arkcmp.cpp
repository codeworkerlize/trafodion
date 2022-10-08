
#include <errno.h>
#include <pthread.h>
#include <string.h>

#include <fstream>

#include "common/Platform.h"
#include "sqlcomp/NewDel.h"

static pthread_t gv_main_thread_id;

#include "seabed/fs.h"
#include "seabed/ms.h"
extern void my_mpi_fclose();
#include "common/SCMVersHelp.h"
DEFINE_DOVERS(tdm_arkcmp)

#include "arkcmp/CmpConnection.h"
#include "arkcmp/CmpContext.h"
#include "arkcmp/CmpErrLog.h"
#include "arkcmp/CmpStoredProc.h"
#include "arkcmp/CompException.h"
#include "arkcmp/NATableSt.h"
#include "arkcmp/QueryCacheSt.h"
#include "cmpargs.h"
#include "comexe/CmpMessage.h"
#include "common/CmpCommon.h"
#include "common/ComCextdecs.h"
#include "sqlcomp/QCache.h"
#include "sqlmxevents/logmxevent.h"
#define CLI_DLL
#include "cli/SQLCLIdev.h"
#undef CLI_DLL

#include "arkcmp/CmpEHCallBack.h"
#include "NamedSemaphore.h"
#include "SharedCache.h"
#include "SqlStats.h"
#include "cli/Globals.h"
#include "common/BloomFilter.h"
#include "eh/EHException.h"
#include "optimizer/ObjectNames.h"
#include "qmscommon/QRLogger.h"
#include "qmscommon/Range.h"
#include "sqlcomp/CmpISPInterface.h"
#include "testCollections.h"

THREAD_P jmp_buf ExportJmpBuf;

ostream &operator<<(ostream &dest, const ComDiagsArea &da);

extern CmpISPInterface cmpISPInterface;

// The following global is defined in CmpConnection.cpp
extern THREAD_P NABoolean CmpMainISPConnection;

// mainNewHandler_CharSave and mainNewHandler are used in the error
// handling when running out of virtual memory for the main program.
//
// Save 4K bytes of memory for the error handling when running out of VM.

static char *mainNewHandler_CharSave = new char[4096];

static int mainNewHandler(size_t s) {
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

// this copy is used by remote tdm_arkcmp process
static jmp_buf CmpInternalErrorJmpBuf;

static void initializeArkcmp(int argc, char **argv) {
  if (setjmp(ExportJmpBuf)) longJumpHandler();

  ExportJmpBufPtr = &ExportJmpBuf;

  if (setjmp(CmpInternalErrorJmpBuf)) longJumpHandler();

  CmpInternalErrorJmpBufPtr = &CmpInternalErrorJmpBuf;
}

// Please do not remove this function. Otherwise a linkage
// error will occur.
void deinitializeArkcmp() {}

static ofstream *initializeArkcmpCoutCerr() {
  // Initialize cout and cerr.

  ofstream *outstream = 0;
  char *streamName;
  char *NULLSTREAM = (char *)"/dev/null";
  streamName = NULLSTREAM;
  outstream = new ofstream(streamName, ios::app);
  cout.rdbuf(outstream->rdbuf());
  cin.rdbuf(outstream->rdbuf());

  return outstream;
}

// test HQC heap space management logic
void testHQC(char *testData) {
  // simulate the context heap
  NAHeap *heap = new NAHeap("simulated_context_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 524288, 0);

  CmpContext *context = NULL;
  context = new (heap) CmpContext(CmpContext::IS_DYNAMIC_SQL, heap);

  context->initContextGlobals();
  context->envs()->cleanup();

  QueryCache *qcache = new (CTXTHEAP) QueryCache();

  int maxSize = UINT_MAX;

  char *file = strdup(testData);
  char *ptr = strchr(file, ':');

  if (ptr) {
    maxSize = atoi(ptr + 1);
    *ptr = NULL;
  }

  qcache->test(file, maxSize);

  free(file);
}

static Cmdline_Args cmdlineArgs;
static int g_cmp_argc;
static char **g_cmp_argv = 0;

int main(int argc, char **argv) {
  dovers(argc, argv);
  try {
    file_init_attach(&argc, &argv, TRUE, (char *)"");
    file_mon_process_startup(true);
    msg_debug_hook("arkcmp", "ark.hook");
    atexit(my_mpi_fclose);
  } catch (...) {
    cerr << "Error while initializing messaging system. Exiting..." << endl;
    exit(1);
  }

  IdentifyMyself::SetMyName(I_AM_SQL_COMPILER);

  QRLogger::initLog4cplus(QRLogger::QRL_MXCMP);

  // Instantiate CliGlobals
  CliGlobals *cliGlobals = CliGlobals::createCliGlobals(FALSE);

  if (getenv("SQL_CMP_MSGBOX_PROCESS") != NULL)
    MessageBox(NULL, "Server: Process Launched", "tdm_arkcmp", MB_OK | MB_ICONINFORMATION);
  // The following call causes output messages to be displayed in the
  // same order on NSK and Windows.
  cout.sync_with_stdio();

  initializeArkcmp(argc, argv);

  cmpISPInterface.InitISPFuncs();

  // we wish stmt heap could be passed to this constructor,
  // but stmt heap is not yet created at this point in time.
  cmdlineArgs.processArgs(argc, argv);

  switch (cmdlineArgs.testMode()) {
    case Cmdline_Args::HQC: {
      testHQC(cmdlineArgs.testData());
      exit(0);
    }

    case Cmdline_Args::TEST_OBJECT_EPOCH_CACHE: {
      testObjectEpochCache(argc, argv);
      exit(0);
    }
    case Cmdline_Args::SHARED_SEGMENT_SEQUENTIAL_SCAN: {
      testSharedMemorySequentialScan(argc, argv);
      exit(0);
    }
    case Cmdline_Args::SHARED_META_CACHE_LOAD: {
      testSharedMemoryHashDictionaryPopulate();
      exit(0);
    }

    case Cmdline_Args::SHARED_META_CACHE_READ: {
      testSharedMemoryHashDictionaryLookup();
      exit(0);
    }

    case Cmdline_Args::SHARED_META_CACHE_FIND_ALL: {
      testSharedMemoryHashDictionaryFindAll();
      exit(0);
    }
    case Cmdline_Args::SHARED_DATA_CACHE_FIND_ALL: {
      testSharedDataMemoryHashDictionaryFindAll();
      exit(0);
    }
    case Cmdline_Args::SHARED_META_CACHE_CLEAN: {
      testSharedMemoryHashDictionaryClean();
      exit(0);
    }

    case Cmdline_Args::HASH_DICTIONARY_ITOR_NON_COPY_TEST: {
      testHashDirectionaryIteratorNoCopy();
      exit(0);
    }

    case Cmdline_Args::SHARED_META_CACHE_ENABLE_DISABLE_DELETE: {
      testSharedMemoryHashDictionaryEnableDisableDelete();
      exit(0);
    }

    case Cmdline_Args::NAMED_SEMAPHORE: {
      testNamedSemaphore();
      exit(0);
    }

    case Cmdline_Args::MEMORY_EXHAUSTION: {
      testMemoryExhaustion();
      exit(0);
    }

    case Cmdline_Args::WATCH_DLOCK: {
      const char *lockName = (argc >= 3) ? argv[2] : SHARED_CACHE_DLOCK_KEY;
      const char *intervalStr = (argc == 4) ? argv[3] : NULL;
      int interval = (intervalStr) ? atoi(intervalStr) : 1;

      DistributedLockObserver::watchDLocks(lockName, interval);
      exit(0);
    }

    case Cmdline_Args::LIST_DLOCK: {
      const char *lockName = (argc >= 3) ? argv[2] : SHARED_CACHE_DLOCK_KEY;
      const char *intervalStr = (argc == 4) ? argv[3] : NULL;
      int interval = (intervalStr) ? atoi(intervalStr) : 1;

      DistributedLockObserver::listDLocks(lockName, interval);
      exit(0);
    }

    case Cmdline_Args::HASHBUCKET: {
      testHashBucket();
      exit(0);
    }

    case Cmdline_Args::NONE:
    default:
      break;
  }
  try {
    {  // a local ctor scope, within a try block
      CmpIpcEnvironment ipcEnv;
      ExCmpMessage exCmpMessage(&ipcEnv);
      ipcEnv.initControl(cmdlineArgs.allocMethod(), cmdlineArgs.socketArg(), cmdlineArgs.portArg());
      exCmpMessage.addRecipient(ipcEnv.getControlConnection()->getConnection());

      CmpMessageConnectionType connectionType;
      // to receive the Connection Type information
      exCmpMessage.clearAllObjects();
      exCmpMessage.receive();

      // Set up the context info for the connection, it contains the variables
      // persistent through each statement loops.

      // Check for env var that indicates this is a secondary mxcmp process.
      // Keep this value in CmpContext so that secondary mxcmp on NSK
      // will allow MP DDL issued from a parent mxcmp process.
      NABoolean IsSecondaryMxcmp = FALSE;
      /*if (getenv("SECONDARY_MXCMP") != NULL)
        IsSecondaryMxcmp = TRUE;
      else
        // Any downstream process will be a "SECONDARY_MXCMP".
        putenv("SECONDARY_MXCMP=1");*/

      CmpContext *context = NULL;
      NAHeap *parentHeap = GetCliGlobals()->getCurrContextHeap();
      NAHeap *cmpContextHeap = new (parentHeap) NAHeap((char *)"Cmp Context Heap", parentHeap, (int)524288);

      try {
        context = new (cmpContextHeap)
            CmpContext(CmpContext::IS_DYNAMIC_SQL | (IsSecondaryMxcmp ? CmpContext::IS_SECONDARY_MXCMP : 0) |
                           (CmpMainISPConnection ? CmpContext::IS_ISP : 0) | (CmpContext::IS_MXCMP),
                       cmpContextHeap);

      } catch (...) {
        ArkcmpErrorMessageBox(ARKCMP_ERROR_PREFIX "- Cannot initialize Compiler global data.", ERROR_SEV, FALSE, FALSE,
                              TRUE);
        exit(1);
      }

      //  moved down the IdentifyMyself so that it can be determined that the
      //  context has not yet been set up
      // IdentifyMyself::SetMyName(I_AM_SQL_COMPILER);
      context->initContextGlobals();
      context->envs()->cleanup();
      // ## This dump-diags only goes to arkcmp console --
      // ## we really need to reply to sqlci (or whoever is requester)
      // ## with these diags!  (Work to be done in Ipc and CmpConnection...)
      NADumpDiags(cerr, CmpCommon::diags(), TRUE /*newline*/);

      assert(cmpCurrentContext == context);
      exCmpMessage.setCmpContext(context);

      while (!exCmpMessage.end()) {
        // Clear the SQL text buffer in the event logging area.
        cmpCurrentContext->resetLogmxEventSqlText();

        exCmpMessage.clearAllObjects();
        exCmpMessage.receive();
      }
      // in most (probably all?) cases, an mxci-spawned mxcmp calls NAExit
      // in CmpConnection.cpp. So, everything below this line is probably
      // dead code.
      CURRENTQCACHE->finalize((char *)"Dynamic (non-NAExit case) ");
    }
  } catch (BaseException &bE) {
    char msg[500];
    sprintf(msg, "%s BaseException: %s %d", ARKCMP_ERROR_PREFIX, bE.getFileName(), bE.getLineNum());
    ArkcmpFatalError(msg);
  } catch (...) {
    ArkcmpFatalError(ARKCMP_ERROR_PREFIX "Fatal exception.");
  }

  ENDTRANSACTION();

  return 0;
}

// stubs
int arkcmp_main_entry() {  // return 1 for embedded cmpiler not created
  return 1;
}
// no-op
void arkcmp_main_exit() {}
