
#ifndef GLOBALS_H
#define GLOBALS_H

#include "common/NAAssert.h"
/* -----------------------------------------------------------------------
 * Some parameters for the first privileged NSK flat segment used by CLI
 * and executor (NOTE: this part can be sourced in from a C program)
 * -----------------------------------------------------------------------
 */
#define NA_CLI_FIRST_PRIV_SEG_ID       NAASSERT_FIRST_PRIV_SEG_ID
#define NA_CLI_FIRST_PRIV_SEG_SIZE     4 * 1024 * 1024
#define NA_CLI_FIRST_PRIV_SEG_MAX_SIZE 64 * 1024 * 1024
/* The removal of spoofing has made these two #defines history
#define NA_CLI_GLOBALS_OFFSET_IN_PRIV_SEG   NAASSERT_GLOBALS_OFFSET_IN_PRIV_SEG
#define NA_CLI_FIRST_PRIV_SEG_START_ADDR    0x42000000
 */
#define BYTEALIGN           16  // 16-byte align entire first flat seg on all platforms
#define NA_CLI_GLOBALS_SIZE (((sizeof(CliGlobals) + BYTEALIGN - 1) / BYTEALIGN) * BYTEALIGN)

#ifdef __cplusplus

// -----------------------------------------------------------------------
#include <setjmp.h>

#include "cli/sqlcli.h"
#include "comexe/ComQueue.h"
#include "common/ComExeTrace.h"
#include "common/ComRtUtils.h"
#include "common/ComSmallDefs.h"
#include "common/Ipc.h"
#include "common/NAMemory.h"
#include "executor/JavaObjectInterface.h"
#include "sqlmxevents/logmxevent.h"

class ContextCli;
class Statement;
class ComDiagsArea;
class ExEspManager;
class ExSsmpManager;
class ExSqlComp;
class IpcEnvironment;
class HashQueue;
class ExUdrServerManager;
class ExControlArea;
class StatsGlobals;
class ex_tcb;  // for keeping the root (split bottom, in ESP) tcb
class ExProcessStats;
class CliGlobals;
class CLISemaphore;
class HBaseClient_JNI;
class TransMode;
class ContextTidMap;
class LmLanguageManager;
class LmLanguageManagerC;
class LmLanguageManagerJava;
class NAClusterInfo;
class MemoryTableDB;
extern CliGlobals *cli_globals;
extern __thread ContextTidMap *tsCurrentContextMap;
static pthread_key_t thread_key;

// A cleanup function when thread exits
void SQ_CleanupThread(void *arg);

enum ArkcmpFailMode { arkcmpIS_OK_ = FALSE /*no failure*/, arkcmpWARN_, arkcmpERROR_ };

class CliGlobals : public NAAssertGlobals {
 public:
  int getNextUniqueContextHandle();

  ExControlArea *getSharedControl() { return sharedCtrl_; }
  CollHeap *exCollHeap() { return &executorMemory_; }

  // old interface, changed to the get/set methods below
  ArkcmpFailMode &arkcmpInitFailed() { return arkcmpInitFailed_; }

  ArkcmpFailMode getArkcmpInitFailMode() { return arkcmpInitFailed_; }

  void setArkcmpInitFailMode(ArkcmpFailMode arkcmpFailMode) { arkcmpInitFailed_ = arkcmpFailMode; }

  void deleteAndCreateNewArkcmp();

  void init(NABoolean espProcess, StatsGlobals *statsGlobals);

  CliGlobals(NABoolean espProcess);

  ~CliGlobals();

  void initiateDefaultContext();

  ContextCli *currContext();

  ContextCli *getDefaultContext() { return defaultContext_; }

  inline ExProcessStats *getExProcessStats() { return processStats_; }
  void setExProcessStats(ExProcessStats *processStats) { processStats_ = processStats; }

  ExSqlComp *getArkcmp(short index = 0);

  IpcEnvironment *getEnvironment();
  char **getEnvVars() { return envvars_; };
  char *getEnv(const char *envvar);
  int setEnvVars(char **envvars);
  int setEnvVar(const char *name, const char *value, NABoolean reset = FALSE);
  int sendEnvironToMxcmp();

  ExEspManager *getEspManager();
  ExUdrServerManager *getUdrServerManager();

  inline NAHeap *getExecutorMemory() { return &executorMemory_; }

  NAClusterInfo *getNAClusterInfo() { return clusterInfo_; }
  inline int incrNumOfCliCalls() { return ++numCliCalls_; }
  inline int decrNumOfCliCalls() {
    if (numCliCalls_ > 0)
      return --numCliCalls_;
    else
      return numCliCalls_;
  }
  inline long incrTotalOfCliCalls() { return ++totalCliCalls_; }

  inline UInt32 *getEventConsumed() { return &eventConsumed_; }
  inline NABoolean processIsStopping() { return processIsStopping_; }

  // create the CLI globals (caller may determine their address)
  static CliGlobals *createCliGlobals(NABoolean espProcess = FALSE);
  static void *getSegmentStartAddrOnNSK();

  // perform a bounds check for a parameter passed into the CLI, note that
  // our bounds check does not prevent access violations if the address
  // points into unallocated memory that doesn't violate the bounds
  // (note that the method always returns a SQLCODE value, AND that it
  // side-effects retcode in case of an error)
  int boundsCheck(void *startAddress, int length, int &retcode);

  inline NABoolean breakEnabled() { return breakEnabled_; }
  inline void setBreakEnabled(NABoolean enabled) {
    breakEnabled_ = enabled;
    getEnvironment()->setBreakEnabled(enabled);
  }

  inline NABoolean SPBreakReceived() { return SPBreakReceived_; }
  inline void setSPBreakReceived(NABoolean val) { SPBreakReceived_ = val; }

  inline NABoolean isESPProcess() { return isESPProcess_; }
  inline void setIsESPProcess(NABoolean val) { isESPProcess_ = val; }

  int createContext(ContextCli *&newContext);
  int dropContext(ContextCli *context);
  ContextCli *getContext(SQLCTX_HANDLE context_handle, NABoolean calledFromDrop = FALSE);
  ContextTidMap *getThreadContext(pid_t tid);
  int switchContext(ContextCli *newContext);

  //
  // Context management functions added to implement user-defined routines
  //
  // int deleteContext(SQLCTX_HANDLE contextHandle);
  int resetContext(ContextCli *context, void *contextMsg);
  inline HashQueue *getContextList() { return contextList_; }

  NAHeap *getIpcHeap();
  NAHeap *getProcessIpcHeap() { return ipcHeap_; }

  StatsGlobals *getStatsGlobals() { return statsGlobals_; }
  void setStatsGlobals(StatsGlobals *statsGlobals) { statsGlobals_ = statsGlobals; }

  NAHeap *getStatsHeap() { return statsHeap_; }
  void setStatsHeap(NAHeap *statsHeap) { statsHeap_ = statsHeap; }

  // EMS event generation functions
  inline void setEMSBeginnerExperienceLevel() { emsEventExperienceLevel_ = SQLMXLoggingArea::eBeginnerEL; }
  inline SQLMXLoggingArea::ExperienceLevel getEMSEventExperienceLevel() { return emsEventExperienceLevel_; }
  inline void setUncProcess() { isUncProcess_ = TRUE; }
  inline NABoolean isUncProcess() { return isUncProcess_; }
  NAHeap *getCurrContextHeap();
  void setJniErrorStr(NAString errorStr) { setSqlJniErrorStr(errorStr); }
  void setJniErrorStr(const char *errorStr) { setSqlJniErrorStr(errorStr); }
  const char *getJniErrorStr() { return getSqlJniErrorStr(); }
  int createLocalCGroup(const char *tenantName, ComDiagsArea &diags);
  void updateTransMode(TransMode *transMode);
  long getTransactionId();

  inline short getGlobalSbbCount() { return globalSbbCount_; }
  inline void incGlobalSbbCount() { globalSbbCount_++; }

  inline void resetGlobalSbbCount() { globalSbbCount_ = 0; }
  //
  // Accessor and mutator functions for UDR error checking. Notes on
  // UDR error checking appear below with the data member
  // declarations.
  //
  NABoolean getUdrErrorChecksEnabled();
  int getUdrSQLAccessMode();
  NABoolean getUdrAccessModeViolation();
  NABoolean getUdrXactViolation();
  NABoolean getUdrXactAborted();

  void setUdrErrorChecksEnabled(NABoolean b);
  void setUdrSQLAccessMode(int mode);
  void setUdrAccessModeViolation(NABoolean b);
  void setUdrXactViolation(NABoolean b);
  void setUdrXactAborted(long currTransId, NABoolean b);

  //
  // A few useful wrapper functions to ease management of the UDR
  // error checking fields
  //
  void clearUdrErrorFlags();

  NABoolean sqlAccessAllowed();

  void getUdrErrorFlags(NABoolean &sqlViolation, NABoolean &xactViolation, NABoolean &xactAborted);
  char *programDir() { return programDir_; };

  // 0, oss process.  1, guardian process.
  short processType() { return processType_; };
  NABoolean ossProcess() { return (processType_ == 0); };
  inline NABoolean logReclaimEventDone() { return logReclaimEventDone_; };
  inline void setLogReclaimEventDone(NABoolean x) { logReclaimEventDone_ = x; };

  char *myNodeName() { return myNodeName_; }
  int myCpu() { return myCpu_; };
  int myPin() { return myPin_; };
  SB_Verif_Type myVerifier() const { return myVerifier_; }

  int myAncestorNid() { return myAncestorNid_; };
  int myAncestorPid() { return myAncestorPid_; };

  int myNodeNumber() { return myNodeNumber_; };
  long myStartTime() { return myStartTime_; };

  IpcPriority myPriority() { return myPriority_; }
  void setMyPriority(IpcPriority p) { myPriority_ = p; }
  NABoolean priorityChanged() { return priorityChanged_; }
  void setPriorityChanged(NABoolean v) { priorityChanged_ = v; }
  IpcPriority myCurrentPriority();

  long getNextUniqueNumber() { return ++lastUniqueNumber_; }

  void genSessionUniqueNumber() { sessionUniqueNumber_++; }
  long getSessionUniqueNumber() { return sessionUniqueNumber_; }

  // returns the current ENVVAR context.
  long getCurrentEnvvarsContext() { return envvarsContext_; };

  void incrCurrentEnvvarsContext() { envvarsContext_++; };
  Long &getSemId() { return semId_; };
  void setSemId(Long semId) { semId_ = semId; }

  void setSavedVersionOfCompiler(short version) { savedCompilerVersion_ = version; }
  short getSavedVersionOfCompiler() { return savedCompilerVersion_; }
  void setSavedSqlTerminateAction(short val) { savedSqlTerminateAction_ = val; }
  short getSavedSqlTerminateAction() { return savedSqlTerminateAction_; }

  void setSavedPriority(short val) { savedPriority_ = val; }
  short getSavedPriority() { return savedPriority_; }
  ExSsmpManager *getSsmpManager();
  NABoolean getIsBeingInitialized() { return inConstructor_; }

  // For debugging (dumps) only -- keep current fragment root tcb
  void setRootTcb(ex_tcb *root) { currRootTcb_ = root; }
  ex_tcb *getRootTcb() { return currRootTcb_; }

  char *myProcessNameString() { return myProcessNameString_; }
  char *myParentProcessNameString() { return parentProcessNameString_; }
  int getSharedMemId() { return shmId_; }
  void setSharedMemId(int shmId) { shmId_ = shmId; }

  void initMyProgName();
  char *myProgName() { return myProgName_; }
  ExeTraceInfo *getExeTraceInfo();
  CLISemaphore *getSemaphore() { return cliSemaphore_; }

  // for trusted UDR invocations from executor and compiler
  LmLanguageManager *getLanguageManager(ComRoutineLanguage language);
  LmLanguageManagerC *getLanguageManagerC();
  LmLanguageManagerJava *getLanguageManagerJava();

#ifdef _DEBUG
  void deleteContexts();
#endif  // _DEBUG
  NABoolean grabMemoryQuotaIfAvailable(int size);
  void resetMemoryQuota();
  int unusedMemoryQuota();
  void yieldMemoryQuota(int size);
  NABoolean isEspProcess() { return espProcess_; }
  NABoolean isRmsProcess() { return rmsProcess_; }

  // For Object lock, only master executor process (mxosrvr or sqlci)
  // need maintaining locks
  NABoolean isMasterProcess() {
    return (!espProcess_ && !isESPProcess_ &&  // Why we have two booleans for ESP?
            !rmsProcess_ && myAncestorNid_ == myNodeNumber_ && myAncestorPid_ == myPin_);
  }

  void setHbaseClient(HBaseClient_JNI *hbaseClientJNI) { hbaseClientJNI_ = hbaseClientJNI; }
  HBaseClient_JNI *getHBaseClient() { return hbaseClientJNI_; }

  void setBigtableClient(HBaseClient_JNI *hbaseClientJNI) { bigtableClientJNI_ = hbaseClientJNI; }
  HBaseClient_JNI *getBigtableClient() { return bigtableClientJNI_; }

  /*
    ObjectEpochCache *getObjectEpochCache()
    {
       ObjectEpochCache *objectEpochCache  = NULL;
       if (statsGlobals_ != NULL)
          objectEpochCache = statsGlobals_->getObjectEpochCache();
       return objectEpochCache;
    }
  */
  NABoolean isSqlciProcess() { return sqlciProcess_; }
  void setSqlciProcess(NABoolean val) { sqlciProcess_ = val; }

  UInt32 getSqlciMaxHeap() { return sqlciMaxHeap_; }
  void setSqlciMaxHeap(UInt32 val) { sqlciMaxHeap_ = val; }

  bool isLicenseModuleOpen(int moduleId) { return true; }

 private:
  enum { DEFAULT_CONTEXT_HANDLE = 2000 };

  ex_tcb *currRootTcb_;  // for keeping the root (split bottom, in ESP) tcb

  // pointer to the server used to communicate with ARKCMP.
  ExSqlComp *sharedArkcmp_;

  ArkcmpFailMode arkcmpInitFailed_;

  ExControlArea *sharedCtrl_;

  // The globals are placed at a fixed address in the first priv
  // segment allocated by the executor. To find out whether they
  // have been initialized, check this data member which will
  // be set to TRUE by the constructor (it is 0 when the memory
  // is unchanged since allocation of the segment)

  HashQueue *contextList_;

  // this is the the default context for executor.
  // Created on the first call to CLI when CliGlobals
  // is allocated.
  ContextCli *defaultContext_;

  // executor memory that maintains all heap memory for this executor
  NAHeap executorMemory_;

  // heap used by the IPC procedures
  NAHeap *ipcHeap_;

  // copy of the oss envvars
  char **envvars_;

  long envvarsContext_;

  // to return a unique SQLCTX_HANDLE on a new ContextCli
  SQLCTX_HANDLE nextUniqueContextHandle;
  // indicator for Sql_Qfo_IOComp() that a WAIT operation completed on LDONE
  // also contains indicator that IpcSetOfConnections::wait consumed LSIG
  UInt32 eventConsumed_;
  NABoolean processIsStopping_;

  long totalCliCalls_;
  short globalSbbCount_;
  short savedSqlTerminateAction_;

  NABoolean breakEnabled_;
  NABoolean SPBreakReceived_;
  NABoolean isESPProcess_;

  // location of the application program which is calling SQL.
  // Fully qualified oss pathname for OSS processes.
  char *programDir_;
  short processType_;  // 0, oss process.  1, guardian process.
  NABoolean logReclaimEventDone_;
  short savedCompilerVersion_;  // saved version from previous CLI call
  // node, cpu and pin this process is running at.
  char myNodeName_[8];
  int myCpu_;
  SB_Verif_Type myVerifier_;
  pid_t myPin_;
  int myNodeNumber_;

  // For object lock
  pid_t myAncestorPid_;
  int myAncestorNid_;

  NAClusterInfo *clusterInfo_;

  IpcPriority myPriority_;
  NABoolean priorityChanged_;

  // timestamp when cli globals were initialized. Start of master executor.
  long myStartTime_;

  // remember the last unique number.
  // Starts at 0 and increments when needed.
  long lastUniqueNumber_;

  // remember the last session unique number.
  // Starts at 0 and increments when needed.
  long sessionUniqueNumber_;

  // EMS event descriptor
  SQLMXLoggingArea::ExperienceLevel emsEventExperienceLevel_;

  char nodeName_[9];

  StatsGlobals *statsGlobals_;
  // heap used for the Stats collection
  NAHeap *statsHeap_;
  Long semId_;
  NABoolean inConstructor_;  // IsExecutor should return TRUE while cliGlobals is being
                             // constructed. CliGlobals is constructed outside of CLI
                             // calls and no. of cliCalls is not yet incremented
  short savedPriority_;
  int shmId_;
  NABoolean isUncProcess_;
  char myProcessNameString_[PROCESSNAME_STRING_LEN];  // PROCESSNAME_STRING_LEN in ComRtUtils.h =40 in ms.h the equiv
                                                      // seabed limit is 32
  char parentProcessNameString_[PROCESSNAME_STRING_LEN];
  // For executor trace.
  char myProgName_[PROGRAM_NAME_LEN];  // 64, see define in ComRtUtils.h
  HashQueue *tidList_;
  CLISemaphore *cliSemaphore_;
  ExProcessStats *processStats_;
  // for trusted UDR invocations from executor and compiler
  LmLanguageManagerC *langManC_;
  LmLanguageManagerJava *langManJava_;
  NABoolean espProcess_;
  NABoolean rmsProcess_;
  HBaseClient_JNI *hbaseClientJNI_;
  HBaseClient_JNI *bigtableClientJNI_;

  NABoolean sqlciProcess_;
  UInt32 sqlciMaxHeap_;
  MemoryTableDB *memoryTableDB_;

  /*
    The type of authentication
  */
  ComAuthenticationType authType_;

  /*
    If a valid password policy syntax is loaded on environment variable,
    this variable will be a non-null pointer and can be returned directly;
    Or load variables from the default table every time.
  */
  ComPwdPolicySyntax pwdPolicy_;

 public:
  void setAuthenticationType(ComAuthenticationType authType) { authType_ = authType; }
  void setPasswordCheckSyntax(ComPwdPolicySyntax pwdPolicy) { pwdPolicy_ = pwdPolicy; }

  ComAuthenticationType getAuthenticationType() { return authType_; }
  ComPwdPolicySyntax *getPasswordCheckSyntax() { return &pwdPolicy_; }
};

// -----------------------------------------------------------------------
// A global method to get a pointer to the CLI globals. The method
// will create the globals if they don't exist already (it will
// not create the executor segment, though).
//
// Note that this method is semi-expensive and shouldn't be called
// a lot of times. It is better to cache the pointer to the globals.
// -----------------------------------------------------------------------

CliGlobals *GetCliGlobals();

// module under license control
bool isModuleOpen(int moduleId);

#endif /* __cplusplus */

#endif
