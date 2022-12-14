
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Globals.cpp
 * Description:  CLI globals. For each process that uses the CLI there
 *               should be exactly one object of type CliGlobals.
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include <pthread.h>
#include <semaphore.h>
#include <stdlib.h>
#include <sys/syscall.h>

#include "cli/CliSemaphore.h"
#include "executor/ExCextdecs.h"
#include "executor/ExControlArea.h"
#include "executor/ExUdrServer.h"
#include "langman/LmLangManagerC.h"
#include "langman/LmLangManagerJava.h"
#include "cli/Context.h"
#include "cli/ExSqlComp.h"
#include "cli/Statement.h"
#include "cli/cli_stdh.h"
#include "common/ComEncryption.h"
#include "common/ComRtUtils.h"
#include "common/Ipc.h"
#include "common/NAClusterInfo.h"
#include "common/Platform.h"
#include "executor/ex_frag_rt.h"
#include "executor/ex_root.h"
#include "executor/DistributedLock_JNI.h"
#include "executor/ExStats.h"
#include "executor/HBaseClient_JNI.h"
#include "executor/TenantHelper_JNI.h"
#include "executor/ex_stdh.h"
#include "executor/ex_transaction.h"
CliGlobals *cli_globals = NULL;
__thread ContextTidMap *tsCurrentContextMap = NULL;

CLISemaphore globalSemaphore;

#include "arkcmp/CmpContext.h"
#include "seabed/sys.h"

CliGlobals::CliGlobals(NABoolean espProcess)
    : inConstructor_(TRUE),
      executorMemory_((const char *)"Global Executor Memory"),
      contextList_(NULL),
      envvars_(NULL),
      envvarsContext_(0),
      sharedArkcmp_(NULL),
      arkcmpInitFailed_(arkcmpIS_OK_),
      processIsStopping_(FALSE),
      totalCliCalls_(0),
      savedCompilerVersion_(COM_VERS_COMPILER_VERSION),
      globalSbbCount_(0),
      priorityChanged_(FALSE),
      currRootTcb_(NULL),
      processStats_(NULL),
      savedPriority_(148),  // Set it to some valid priority to start with
      tidList_(NULL),
      cliSemaphore_(NULL),
      defaultContext_(NULL),
      langManC_(NULL),
      langManJava_(NULL),
      myVerifier_(-1),
      espProcess_(espProcess),
      rmsProcess_(FALSE),
      hbaseClientJNI_(NULL),
      bigtableClientJNI_(NULL),
      sqlciProcess_(FALSE),
      sqlciMaxHeap_(0),
      authType_(COM_NO_AUTH) {
  globalsAreInitialized_ = FALSE;
  executorMemory_.setThreadSafe();
  init(espProcess, NULL);
  globalsAreInitialized_ = TRUE;
  {
    memset((void *)&pwdPolicy_, 0, sizeof(ComPwdPolicySyntax));
    pwdPolicy_.max_password_length = 128;
    pwdPolicy_.lower_chars_number = 4;
  }
}

void CliGlobals::init(NABoolean espProcess, StatsGlobals *statsGlobals) {
  int threadCount = NAAssertMutexCreate();
  if (threadCount != 1)  // The main executor thread must be first
    abort();
  SQLMXLoggingArea::init();

#if !(defined(__SSCP) || defined(__SSMP))
  sharedCtrl_ = new (&executorMemory_) ExControlArea(NULL /*context */, &executorMemory_);
  rmsProcess_ = FALSE;
#else
  rmsProcess_ = TRUE;
  sharedCtrl_ = NULL;
#endif

  char *_sqptr = 0;
  _sqptr = new (&executorMemory_) char[10];

  numCliCalls_ = 0;
  nodeName_[0] = '\0';

  breakEnabled_ = FALSE;

  SPBreakReceived_ = FALSE;
  isESPProcess_ = FALSE;

  logReclaimEventDone_ = FALSE;

  // find and initialize the directory this program is being run from.
  // Max length of oss dirname is 1K (process_getinfolist_ limit).
  // Also initialize the node, cpu and pin my process is running at.

  programDir_ = new (&executorMemory_) char[1024 + 1];
  short nodeNameLen;
  int retcomrt = 0;
  retcomrt = ComRtGetProgramInfo(programDir_, 1024, processType_, myCpu_, myPin_, myNodeNumber_, myNodeName_,
                                 nodeNameLen, myStartTime_, myProcessNameString_, parentProcessNameString_,
                                 &myVerifier_, &myAncestorNid_, &myAncestorPid_);

  if (retcomrt) {
    char errStr[128];
    sprintf(errStr, "Could not initialize CLI globals.ComRtGetProgramInfo returned an error :%d.", retcomrt);
    ex_assert(0, errStr);
  }

  QRINFO("Ancestor process NID: %d, PID: %d", myAncestorNid_, myAncestorPid_);
  ex_assert(myAncestorNid_ >= 0, "Invalid ancestor node id");
  ex_assert(myAncestorPid_ >= 0, "Invalid ancestor process id");

  ComRtGetProcessPriority(myPriority_);
  savedPriority_ = (short)myPriority_;
  clusterInfo_ = new (&executorMemory_) NAClusterInfoLinux(&executorMemory_, FALSE);

  // create global structures for IPC environment
#if !(defined(__SSCP) || defined(__SSMP))

  ipcHeap_ = new (&executorMemory_) NAHeap("IPC Heap", NAMemory::IPC_MEMORY, 2048 * 1024);
  ipcHeap_->setThreadSafe();
  if (!espProcess) {
    // Create the process global ARKCMP server.
    sharedArkcmp_ = NULL;
    nextUniqueContextHandle = DEFAULT_CONTEXT_HANDLE;
    lastUniqueNumber_ = 0;
    sessionUniqueNumber_ = 0;
    // It is not thread safe to set the globals cli_globals
    // before cli_globals is fully initialized, but it is being done
    // here because the code below expects it
    cli_globals = this;
    int error;
    statsGlobals_ = (StatsGlobals *)shareStatsSegmentWithRetry(shmId_);
    NABoolean reportError = FALSE;
    char msg[256];

    if (statsGlobals_ == NULL) {
      snprintf(msg, sizeof(msg), "shareStatsSegmentWithRetry return NULL");
      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
      exit(0);
    } else if ((statsGlobals_ != NULL) && (statsGlobals_->getInitError(myPin_, reportError))) {
      if (reportError) {
        snprintf(msg, sizeof(msg), "Version mismatch or Pid %d,%d is higher than the configured pid max %d", myCpu_,
                 myPin_, statsGlobals_->getConfiguredPidMax());
        SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
      }
      exit(0);

      /*statsGlobals_ = NULL;
      statsHeap_ = new (getExecutorMemory())
        NAHeap("Process Stats Heap", getExecutorMemory(),
               8192,
               0);
      statsHeap_->setThreadSafe();*/
    } else {
      error = statsGlobals_->openStatsSemaphoreWithRetry(semId_);

      if (error != 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "openStatsSemaphore failed after retry, the process will exit");
        SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
        exit(0);
        // statsGlobals_ = NULL;
        // statsHeap_ = getExecutorMemory();
      } else {
        bool reincarnated;
        error = statsGlobals_->getStatsSemaphore(semId_, myPin_);

        statsHeap_ = (NAHeap *)statsGlobals_->getStatsHeap()->allocateHeapMemory(sizeof *statsHeap_, FALSE);

        // The following assertion may be hit if the RTS shared memory
        // segment is full.  The stop catcher code will be responsible
        // for releasing the RTS semaphore.
        ex_assert(statsHeap_, "allocateHeapMemory returned NULL.");

        // This next allocation, a placement "new" will not fail.
        statsHeap_ = new (statsHeap_, statsGlobals_->getStatsHeap())
            NAHeap("Process Stats Heap", statsGlobals_->getStatsHeap(), 8192, 0);
        reincarnated = statsGlobals_->addProcess(myPin_, statsHeap_);
        processStats_ = statsGlobals_->getExProcessStats(myPin_);
        processStats_->setStartTime(myStartTime_);
        statsGlobals_->releaseStatsSemaphore(semId_, myPin_);
        if (reincarnated) statsGlobals_->logProcessDeath(myCpu_, myPin_, "Process reincarnated before RIP");
      }
    }
    // create a default context and make it the current context
    cliSemaphore_ = new (&executorMemory_) CLISemaphore();
    defaultContext_ = new (&executorMemory_) ContextCli(this, TRUE);
    contextList_ = new (&executorMemory_) HashQueue(&executorMemory_);
    tidList_ = new (&executorMemory_) HashQueue(&executorMemory_);
    SQLCTX_HANDLE ch = defaultContext_->getContextHandle();
    contextList_->insert((char *)&ch, sizeof(SQLCTX_HANDLE), (void *)defaultContext_);

  }  // (!espProcess)

  else {
    // For ESPs do not create the default context here. At this point
    // the ESP has not created an IpcEnvironment object yet so the
    // result context will have an invalid ExSqlComp object that
    // points to a NULL IpcEnvironment. In bin/ex_esp_main.cpp, the
    // following objects are created at ESP startup time:
    //   - CliGlobals
    //   - IpcEnvironment
    //   - Default ContextCli in CliGlobals
    //   - MemoryMonitor
    //   - ExEspFragInstanceDir
    //   - ExEspControl Message
    //   - Global UDR server manager
    cliSemaphore_ = new (&executorMemory_) CLISemaphore();
    statsGlobals_ = NULL;
    semId_ = -1;
    statsHeap_ = NULL;
    lastUniqueNumber_ = 0;

  }  // if (!espProcess) else ...

#else  // (defined(__SSCP) || defined(__SSMP))
  cliSemaphore_ = new (&executorMemory_) CLISemaphore();
  statsGlobals_ = statsGlobals;
  semId_ = -1;
  statsHeap_ = NULL;
  lastUniqueNumber_ = 0;
#endif

  inConstructor_ = FALSE;
  //
  // could initialize the program file name here but ...
  myProgName_[0] = '\0';

  ComEncryption::initializeEVP();
}
CliGlobals::~CliGlobals() {
  arkcmpInitFailed_ = arkcmpERROR_;  // (it's corrupt after deleting, anyway...)
  short error;

  if (sharedArkcmp_) {
    delete sharedArkcmp_;
    sharedArkcmp_ = NULL;
  }
  if (statsGlobals_ != NULL) {
    error = statsGlobals_->getStatsSemaphore(semId_, myPin_);
    statsGlobals_->removeProcess(myPin_);
    statsGlobals_->releaseStatsSemaphore(semId_, myPin_);
    statsGlobals_->logProcessDeath(myCpu_, myPin_, "Normal process death");
    sem_close((sem_t *)semId_);
  }

  ComEncryption::cleanupEVP();
}

int CliGlobals::getNextUniqueContextHandle() {
  int contextHandle;
  cliSemaphore_->get();
  contextHandle = nextUniqueContextHandle++;
  cliSemaphore_->release();
  return contextHandle;
}

IpcPriority CliGlobals::myCurrentPriority() {
  IpcPriority myPriority;

  int retcode = ComRtGetProcessPriority(myPriority);
  if (retcode) return -2;

  return myPriority;
}

// NOTE: Unlike REFPARAM_BOUNDSCHECK, this method does not verify that
//       the pointer "startAddress" actually points to a valid address
//       in the user address space (means that dereferencing the
//       pointer may cause a segmentation violation).
//
// Here are some DEFINEs from files DMEM and JMEMH in product T9050 that
// perform the PRIV address check:
// LOG2_BYTES_IN_T16PAGE      = 11,    ! 2048 bytes in a T16 page size
// LOG2_T16PAGES_IN_SEGMENT   =  6,    ! 64 T16 pages in a segment
// LOG2_BYTES_IN_SEGMENT      = LOG2_BYTES_IN_T16PAGE +
//                              LOG2_T16PAGES_IN_SEGMENT,
// SYSTEMDATASEG  = 1,  !  System Data Segment (always absolute segment 1)
//
// #define ADDR_IS_IN_KSEG0_1_2( a )         \
//    ( (int32) (a) < 0 )
//
// #define ADDR_IS_PRIV(a) (ADDR_IS_IN_KSEG0_1_2(a)               \
//              || ((vaddr_t)(a) >> LOG2_BYTES_IN_SEGMENT) == SYSTEMDATASEG)
//
// NOTE: we'll need to recompile if the NSK architecture changes, which
// should be infrequent as Charles Landau assures me.

int CliGlobals::boundsCheck(void *startAddress, int length, int &retcode) {
  // no bounds checking on NT because we're not PRIV
  return 0;
}

NAHeap *CliGlobals::getIpcHeap() { return currContext()->getIpcHeap(); }

IpcEnvironment *CliGlobals::getEnvironment() {
  ContextCli *currentContext = currContext();
  if (currentContext != NULL)
    return currentContext->getEnvironment();
  else
    return NULL;
}

ExEspManager *CliGlobals::getEspManager() { return currContext()->getEspManager(); }

ExSsmpManager *CliGlobals::getSsmpManager() { return currContext()->getSsmpManager(); }

LmLanguageManager *CliGlobals::getLanguageManager(ComRoutineLanguage language) {
  switch (language) {
    case COM_LANGUAGE_JAVA:
      return getLanguageManagerJava();
      break;
    case COM_LANGUAGE_C:
    case COM_LANGUAGE_CPP:
      return getLanguageManagerC();
      break;
    default:
      ex_assert(0, "Invalid language in CliGlobals::getLanguageManager()");
  }
  return NULL;
}

LmLanguageManagerC *CliGlobals::getLanguageManagerC() {
  if (!langManC_) {
    LmResult result;

    langManC_ = new (&executorMemory_) LmLanguageManagerC(result, FALSE, &(currContext()->diags()));

    if (result != LM_OK) {
      delete langManC_;
      langManC_ = NULL;
    }
  }

  return langManC_;
}

LmLanguageManagerJava *CliGlobals::getLanguageManagerJava() {
  if (!langManJava_) {
    LmResult result;

    langManJava_ = new (&executorMemory_) LmLanguageManagerJava(result, FALSE, 1,
                                                                NULL,  // Java options should have been
                                                                       // provided for earlier JNI calls
                                                                       // in Trafodion
                                                                &(currContext()->diags()));

    if (result != LM_OK) {
      delete langManJava_;
      langManJava_ = NULL;
    }
  }

  return langManJava_;
}

ExeTraceInfo *CliGlobals::getExeTraceInfo() { return currContext()->getExeTraceInfo(); }

ExSqlComp *CliGlobals::getArkcmp(short index) {
  // return sharedArkcmp_;
  return currContext()->getArkcmp(index);
}

CliGlobals *CliGlobals::createCliGlobals(NABoolean espProcess) {
  CliGlobals *result;

  result = new CliGlobals(espProcess);
  // pthread_key_create(&thread_key, SQ_CleanupThread);
  cli_globals = result;
  HBaseClient_JNI::getInstance(FALSE);
  HBaseClient_JNI::getInstance(TRUE);
  return result;
}

void *CliGlobals::getSegmentStartAddrOnNSK() {
  // this method should only be called on NSK, return NULL on other platforms
  return NULL;
}

CliGlobals *GetCliGlobals() { return cli_globals; }

bool isModuleOpen(int moduleId) {
  if (cli_globals) return true;

  // should never happen
  return true;
}

// used by ESP only
void CliGlobals::initiateDefaultContext() {
  // create a default context and make it the current context
  defaultContext_ = new (&executorMemory_) ContextCli(this, TRUE);
  contextList_ = new (&executorMemory_) HashQueue(&executorMemory_);
  tidList_ = new (&executorMemory_) HashQueue(&executorMemory_);
  cliSemaphore_ = new (&executorMemory_) CLISemaphore();
  SQLCTX_HANDLE ch = defaultContext_->getContextHandle();
  contextList_->insert((char *)&ch, sizeof(SQLCTX_HANDLE), (void *)defaultContext_);
}

ContextCli *CliGlobals::currContext() {
  if (tsCurrentContextMap == NULL || tsCurrentContextMap->context_ == NULL)
    return defaultContext_;
  else
    return tsCurrentContextMap->context_;
}

int CliGlobals::createContext(ContextCli *&newContext) {
  newContext = new (&executorMemory_) ContextCli(this);
  SQLCTX_HANDLE ch = newContext->getContextHandle();
  cliSemaphore_->get();
  contextList_->insert((char *)&ch, sizeof(SQLCTX_HANDLE), (void *)newContext);
  cliSemaphore_->release();

  return 0;
}
int CliGlobals::dropContext(ContextCli *context) {
  if (!context) return -1;

  if (context == getDefaultContext()) return 0;

  CLISemaphore *tmpSemaphore = context->getSemaphore();
  tmpSemaphore->get();
  try {
    context->deleteMe();
  } catch (...) {
    tmpSemaphore->release();
    return -1;
  }
  tmpSemaphore->release();
  pid_t tid = GETTID();
  cliSemaphore_->get();
  contextList_->remove((void *)context);
  tidList_->position();
  ContextTidMap *contextTidMap;
  while ((contextTidMap = (ContextTidMap *)tidList_->getNext()) != NULL) {
    if (contextTidMap->context_ == context) {
      if (contextTidMap->tid_ == tid) {
        tidList_->remove((char *)&contextTidMap->tid_, sizeof(pid_t), contextTidMap);
        NADELETE(contextTidMap, ContextTidMap, getExecutorMemory());
        tsCurrentContextMap = NULL;
      } else
        contextTidMap->context_ = NULL;
    }
  }
  delete context;
  cliSemaphore_->release();
  return 0;
}

ContextCli *CliGlobals::getContext(SQLCTX_HANDLE context_handle, NABoolean calledFromDrop) {
  ContextCli *context;
  cliSemaphore_->get();
  contextList_->position((char *)&context_handle, sizeof(SQLCTX_HANDLE));
  while ((context = (ContextCli *)contextList_->getNext()) != NULL) {
    if (context_handle == context->getContextHandle()) {
      if (context->isDropInProgress())
        context = NULL;
      else if (calledFromDrop)
        context->setDropInProgress();
      cliSemaphore_->release();
      return context;
    }
  }
  cliSemaphore_->release();
  return NULL;
}

ContextTidMap *CliGlobals::getThreadContext(pid_t tid) {
  SQLCTX_HANDLE ch;
  ContextTidMap *contextTidMap;

  if (tidList_ == NULL) return NULL;
  cliSemaphore_->get();
  tidList_->position((char *)&tid, sizeof(pid_t));
  while ((contextTidMap = (ContextTidMap *)tidList_->getNext()) != NULL) {
    if (contextTidMap->tid_ == tid) {
      if (contextTidMap->context_ == NULL) {
        NADELETE(contextTidMap, ContextTidMap, getExecutorMemory());
        tidList_->remove((char *)&tid, sizeof(pid_t), contextTidMap);
        contextTidMap = NULL;
      }
      cliSemaphore_->release();
      return contextTidMap;
    }
  }
  cliSemaphore_->release();
  return NULL;
}

int CliGlobals::switchContext(ContextCli *newContext) {
  int retcode = 0;

  pid_t tid;
  SQLCTX_HANDLE ch, currCh;

  tid = GETTID();
  if (newContext != defaultContext_ && tsCurrentContextMap != NULL && newContext == tsCurrentContextMap->context_)
    return 0;
  if (newContext == defaultContext_)
    if (tsCurrentContextMap) tsCurrentContextMap->context_ = NULL;
  retcode = currContext()->getTransaction()->suspendTransaction();
  if (retcode != 0) return retcode;
  cliSemaphore_->get();
  tidList_->position((char *)&tid, sizeof(pid_t));
  ContextTidMap *contextTidMap;
  NABoolean tidFound = FALSE;
  while ((contextTidMap = (ContextTidMap *)tidList_->getNext()) != NULL) {
    if (tid == contextTidMap->tid_) {
      contextTidMap->context_ = newContext;
      tidFound = TRUE;
      tsCurrentContextMap = contextTidMap;
      break;
    }
  }
  if (!tidFound) {
    contextTidMap = new (getExecutorMemory()) ContextTidMap(tid, newContext);
    tidList_->insert((char *)&tid, sizeof(pid_t), (void *)contextTidMap);
    tsCurrentContextMap = contextTidMap;
  }
  cliSemaphore_->release();
  retcode = currContext()->getTransaction()->resumeTransaction();
  return retcode;
}

int CliGlobals::sendEnvironToMxcmp() {
  ComDiagsArea &diags = currContext()->diags();

  if (NOT getArkcmp()->isConnected()) return 0;

  // send the current environment to mxcmp
  ExSqlComp::ReturnStatus sendStatus = getArkcmp()->sendRequest(CmpMessageObj::ENVS_REFRESH, NULL, 0);
  if (sendStatus != ExSqlComp::SUCCESS) {
    if (sendStatus == ExSqlComp::ERROR) {
      diags << DgSqlCode(-CLI_SEND_REQUEST_ERROR) << DgString0("SET ENVIRON");
      return -CLI_SEND_REQUEST_ERROR;
      //	  return SQLCLI_ReturnCode(&currContext,-CLI_SEND_REQUEST_ERROR);
    }
    // else
    //  retcode = WARNING;
  }

  if (getArkcmp()->status() != ExSqlComp::FINISHED) {
    diags << DgSqlCode(-CLI_IO_REQUESTS_PENDING) << DgString0("SET ENVIRON");
    return -CLI_IO_REQUESTS_PENDING;
    // return SQLCLI_ReturnCode(&currContext,-CLI_IO_REQUESTS_PENDING);
  }

  return 0;
}

int CliGlobals::setEnvVars(char **envvars) {
  if ((!envvars) || (isESPProcess_)) return 0;

  int nEnvs = 0;
  if (envvars_) {
    // deallocate the current set of envvars
    ipcHeap_->deallocateMemory(envvars_);
    envvars_ = NULL;
  }

  for (nEnvs = 0; envvars[nEnvs]; nEnvs++)
    ;

  // one extra to null terminate envvar list
  int envvarsLen = (nEnvs + 1) * sizeof(char *);

  int count;
  for (count = 0; count < nEnvs; count++) {
    envvarsLen += str_len(envvars[count]) + 1;
  }

  // allocate contiguous space for envvars
  envvars_ = (char **)(new (ipcHeap_) char[envvarsLen]);

  char *envvarsValue = (char *)envvars_ + (nEnvs + 1) * sizeof(char *);

  // and copy input envvars to envvars_
  for (count = 0; count < nEnvs; count++) {
    envvars_[count] = envvarsValue;
    int l = str_len(envvars[count]) + 1;
    str_cpy_all(envvarsValue, envvars[count], l);
    envvarsValue = envvarsValue + l;
  }

  envvars_[nEnvs] = 0;

  // also set it in IpcEnvironment so it could be used to send
  // it to mxcmp.
  getEnvironment()->setEnvVars(envvars_);
  getEnvironment()->setEnvVarsLen(envvarsLen);

  envvarsContext_++;

  return sendEnvironToMxcmp();
}

int CliGlobals::setEnvVar(const char *name, const char *value, NABoolean reset) {
  if ((!name) || (!value) || (isESPProcess_)) return 0;

  NABoolean found = FALSE;
  int envvarPos = -1;
  if (ComRtGetEnvValueFromEnvvars((const char **)envvars_, name, &envvarPos)) found = TRUE;

  if ((NOT found) && (reset)) return 0;

  int nEnvs = 0;
  if (envvars_) {
    for (nEnvs = 0; envvars_[nEnvs]; nEnvs++)
      ;
  }

  if (reset) {
    //      nEnvs--;
  } else if (NOT found) {
    envvarPos = nEnvs;
    nEnvs++;
  }

  // one extra entry, if envvar not found.
  // one extra to null terminate envvar list.
  //  long envvarsLen = (nEnvs + (NOT found ? 1 : 0) + 1) * sizeof(char*);
  int newEnvvarsLen = (nEnvs + 1) * sizeof(char *);

  int count;
  for (count = 0; count < nEnvs; count++) {
    if (count == envvarPos)
    //      if ((found) && (count == envvarPos))
    {
      if (NOT reset) newEnvvarsLen += strlen(name) + strlen("=") + strlen(value) + 1;
    } else if (NULL != envvars_)
      newEnvvarsLen += str_len(envvars_[count]) + 1;
  }

  /*  if (NOT found)
    {
      nEnvs++;
      newEnvvarsLen += strlen(name) + strlen("=") + strlen(value) + 1;
    }
    */

  // allocate contiguous space for envvars
  char **newEnvvars = (char **)(new (ipcHeap_) char[newEnvvarsLen]);

  char *newEnvvarsValue = (char *)(newEnvvars + ((reset ? (nEnvs - 1) : nEnvs) + 1));

  // and copy envvars_ to newEnvvars
  int tgtCount = 0;
  for (count = 0; count < nEnvs; count++) {
    newEnvvars[tgtCount] = newEnvvarsValue;
    int l = 0;
    if (count == envvarPos) {
      if (NOT reset) {
        strcpy(newEnvvarsValue, name);
        strcat(newEnvvarsValue, "=");
        strcat(newEnvvarsValue, value);
        l = strlen(name) + strlen("=") + strlen(value) + 1;

        tgtCount++;
      }
    } else if (NULL != envvars_) {
      l = str_len(envvars_[count]) + 1;
      str_cpy_all(newEnvvarsValue, envvars_[count], l);
      tgtCount++;
    }
    newEnvvarsValue = newEnvvarsValue + l;
  }

  if (reset) {
    nEnvs--;
  }

  newEnvvars[nEnvs] = 0;

  if (envvars_) {
    // deallocate the current set of envvars
    ipcHeap_->deallocateMemory(envvars_);
    envvars_ = NULL;
  }
  envvars_ = newEnvvars;

  // set or reset this envvar in SessionDefaults.
  SessionEnvvar *se = new (ipcHeap_) SessionEnvvar(ipcHeap_, (char *)name, (char *)value);

  // remove if an entry exists
  currContext()->getSessionDefaults()->sessionEnvvars()->remove(*se);

  // insert a new entry, if this is not a RESET operation.
  if (NOT reset) {
    currContext()->getSessionDefaults()->sessionEnvvars()->insert(*se);
  }

  delete se;

  // also set it in IpcEnvironment so it could be used to send
  // it to mxcmp.
  getEnvironment()->setEnvVars(envvars_);
  getEnvironment()->setEnvVarsLen(newEnvvarsLen);

  envvarsContext_++;

  // need to set the env to the embedded compiler too
  if (currContext()->isEmbeddedArkcmpInitialized()) {
    currContext()->getEmbeddedArkcmpContext()->setArkcmpEnvDirect(name, value, reset);
  }

  return sendEnvironToMxcmp();
}

char *CliGlobals::getEnv(const char *name) {
  return (char *)ComRtGetEnvValueFromEnvvars((const char **)envvars_, name);
}
//
int CliGlobals::resetContext(ContextCli *theContext, void *contextMsg) {
  theContext->reset(contextMsg);
  return SUCCESS;
}

NAHeap *CliGlobals::getCurrContextHeap() { return currContext()->exHeap(); }

long CliGlobals::getTransactionId() {
  long transId = -1;
  ContextCli *context = currContext();
  if (context != NULL) {
    ExTransaction *trans = context->getTransaction();
    transId = trans == NULL ? -1 : trans->getTransid();
  }
  return transId;
}

ExUdrServerManager *CliGlobals::getUdrServerManager() { return currContext()->getUdrServerManager(); }

NABoolean CliGlobals::getUdrErrorChecksEnabled() { return currContext()->getUdrErrorChecksEnabled(); }

int CliGlobals::getUdrSQLAccessMode() { return currContext()->getUdrSQLAccessMode(); }

NABoolean CliGlobals::getUdrAccessModeViolation() { return currContext()->getUdrAccessModeViolation(); }

NABoolean CliGlobals::getUdrXactViolation() { return currContext()->getUdrXactViolation(); }

NABoolean CliGlobals::getUdrXactAborted() { return currContext()->getUdrXactAborted(); }

void CliGlobals::setUdrErrorChecksEnabled(NABoolean b) { currContext()->setUdrErrorChecksEnabled(b); }

void CliGlobals::setUdrSQLAccessMode(int mode) { currContext()->setUdrSQLAccessMode(mode); }

void CliGlobals::setUdrAccessModeViolation(NABoolean b) { currContext()->setUdrAccessModeViolation(b); }

void CliGlobals::setUdrXactViolation(NABoolean b) { currContext()->setUdrXactViolation(b); }

void CliGlobals::setUdrXactAborted(long currTransId, NABoolean b) { currContext()->setUdrXactAborted(currTransId, b); }

void CliGlobals::clearUdrErrorFlags() { currContext()->clearUdrErrorFlags(); }

NABoolean CliGlobals::sqlAccessAllowed() { return currContext()->sqlAccessAllowed(); }

void CliGlobals::getUdrErrorFlags(NABoolean &sqlViolation, NABoolean &xactViolation, NABoolean &xactAborted) {
  currContext()->getUdrErrorFlags(sqlViolation, xactViolation, xactAborted);
}

int CliGlobals::createLocalCGroup(const char *tenantName, ComDiagsArea &diags) {
  TenantHelper_JNI *inst = TenantHelper_JNI::getInstance();

  if (!inst) {
    IpcProcessId myProcId = getEnvironment()->getMyOwnProcessId(IPC_DOM_GUA_PHANDLE);

    diags << DgSqlCode(-8749) << DgString0(tenantName);
    myProcId.addProcIdToDiagsArea(diags, 1);
    diags << DgString2("Unable to get TenantHelper JNI instance");

    return -1;
  }

  TH_RetCode rc = inst->createLocalCGroup(tenantName);

  if (rc != TH_OK) {
    IpcProcessId myProcId = getEnvironment()->getMyOwnProcessId(IPC_DOM_GUA_PHANDLE);

    diags << DgSqlCode(-8749) << DgString0(tenantName);
    myProcId.addProcIdToDiagsArea(diags, 1);
    diags << DgString2(inst->getErrorText(rc));

    return rc;
  }

  return 0;
}

void CliGlobals::updateTransMode(TransMode *transMode) {
  currContext()->getTransaction()->getTransMode()->updateTransMode(transMode);
}

void CliGlobals::initMyProgName() {
  char statusFileName[128];
  FILE *status_file = 0;
  size_t bytesRead, bytesCopy;
  char buf[1024];
  char *beginPtr, *endPtr;

  sprintf(statusFileName, "/proc/%d/status", getpid());
  status_file = fopen(statusFileName, "r");
  if (status_file == NULL) return;  // ignore error and return

  buf[0] = 0;
  if (fseek(status_file, 0, SEEK_SET)) {
    fclose(status_file);
    return;  // ignore error and return
  }
  bytesRead = fread(buf, 1, 1024, status_file);
  if (ferror(status_file)) {
    fclose(status_file);
    return;  // ignore error and return
  }
  fclose(status_file);
  beginPtr = strstr(buf, "Name:\t");
  if (!beginPtr) return;  // not found, return
  beginPtr += 6;
  endPtr = strstr(beginPtr, "\n");
  if (!endPtr || beginPtr == endPtr) return;  // no name found, return
  // myProgName_ is 64 bytes long, see Globals.h
  bytesCopy = (endPtr - beginPtr) < PROGRAM_NAME_LEN ? (endPtr - beginPtr) : PROGRAM_NAME_LEN - 1;
  memcpy(myProgName_, beginPtr, bytesCopy);
  *(myProgName_ + bytesCopy) = '\0';  // null terminates
}

#ifdef _DEBUG
// Delete the default context and all associated embedded CMP contexts
// This eventually causes dumping of all heap debug info if enabled
// Should ONLY be called right before process exits
void CliGlobals::deleteContexts() {
  if (defaultContext_) {
    defaultContext_->deleteMe();
    delete defaultContext_;
    defaultContext_ = NULL;
  }
}
#endif  // _DEBUG

// The unused BMO memory quota can now be utilized by the other
// BMO instances from the same or different fragment
// In case of ESP process, the unused memory quota is maintained
// at the default context. In case of master process, the ununsed
// memory quota is maintained in statement globals

NABoolean CliGlobals::grabMemoryQuotaIfAvailable(int size) {
  ContextCli *context;
  if (espProcess_)
    context = defaultContext_;
  else
    context = currContext();
  return context->grabMemoryQuotaIfAvailable(size);
}

void CliGlobals::resetMemoryQuota() {
  ContextCli *context;
  if (espProcess_)
    context = defaultContext_;
  else
    context = currContext();
  return context->resetMemoryQuota();
}

int CliGlobals::unusedMemoryQuota() {
  ContextCli *context;
  if (espProcess_)
    context = defaultContext_;
  else
    context = currContext();
  return context->unusedMemoryQuota();
}

void CliGlobals::yieldMemoryQuota(int size) {
  ContextCli *context;
  if (espProcess_)
    context = defaultContext_;
  else
    context = currContext();
  return context->yieldMemoryQuota(size);
}

void SQ_CleanupThread(void *arg) {
  HBaseClient_JNI::deleteInstance();
  DistributedLock_JNI::deleteInstance();
}
