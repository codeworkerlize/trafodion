/*********************************************************************

**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SqlStats.cpp
 * Description:
 * Created:      2/28/06
 * Language:     C++
 *
 *****************************************************************************
 */
#include "cli_stdh.h"
#include "ex_stdh.h"
#include "executor/ExStats.h"
#include "cli/sql_id.h"
#include "ExCextdecs.h"
#include "common/Ipc.h"
#include "common/ComSqlId.h"
#include "porting/PortProcessCalls.h"
#include <fcntl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <signal.h>
#include "seabed/ms.h"
#include "seabed/fserr.h"
#include "common/ComDistribution.h"
#include "SharedSegment.h"

extern NABoolean checkIfRTSSemaphoreLocked();

void *StatsGlobals::operator new(size_t size, void *loc) {
  if (loc) return loc;
  return ::operator new(size);
}

StatsGlobals::StatsGlobals(void *baseAddr, short envType, long maxSegSize)
    : statsHeap_("Stats Globals", getStatsSegmentId(), baseAddr, ((sizeof(StatsGlobals) + 16 - 1) / 16) * 16,
                 maxSegSize),
      recentSikeys_(NULL),
      newestRevokeTimestamp_(0),  // all new compilers are current.
      statsArray_(NULL),
      semPid_(-1),
      semPidCreateTime_(0),
      maxPid_(0),
      pidToCheck_(0),
      ssmpDumpedTimestamp_(0),
      lobLocks_(NULL),
      pidViolationCount_(0),
      rmsDeadLockSecs_(10),
      dumpRmsDeadLockProcess_(TRUE),
      dmlLockArraySize_(DML_LOCK_ARRAY_SIZE),
      ddlLockArraySize_(DDL_LOCK_ARRAY_SIZE) {
  statsHeap_.setUseSemaphore();
  // Phandle wrapper in porting layer
  NAProcessHandle phandle;
  phandle.getmine();
  phandle.decompose();
  cpu_ = phandle.getCpu();

  statsSharedSegAddr_ = baseAddr;
  version_ = CURRENT_SHARED_OBJECTS_VERSION_;
  isSscpInitialized_ = FALSE;

  // On Seaquest, no need for a private env.
  if (envType == 0)
    rtsEnvType_ = RTS_PRIVATE_ENV;
  else
    rtsEnvType_ = RTS_GLOBAL_ENV;
  storeSqlSrcLen_ = RMS_STORE_SQL_SOURCE_LEN;
  abortedSemPid_ = -1;
  errorSemPid_ = -1;
  releasingSemPid_ = -1;
  seabedError_ = 0;
  seabedPidRecycle_ = false;
  snapshotInProgress_ = false;
  // Get /proc/sys/kernel/pid_max
  // If it is greater than a reasonable value, then
  // let PID_MAX environment variable to override it
  // Make sure Pid Max is set to a PID_MAX_DEFAULT_MIN value at least
  char *pidMaxStr;
  configuredPidMax_ = ComRtGetConfiguredPidMax();
  if (configuredPidMax_ == 0) configuredPidMax_ = PID_MAX_DEFAULT;
  if (configuredPidMax_ > PID_MAX_DEFAULT_MAX) {
    if ((pidMaxStr = getenv("PID_MAX")) != NULL)
      configuredPidMax_ = atoi(pidMaxStr);
    else
      configuredPidMax_ = PID_MAX_DEFAULT_MAX;
  }
  if (configuredPidMax_ == 0)
    configuredPidMax_ = PID_MAX_DEFAULT;
  else if (configuredPidMax_ < PID_MAX_DEFAULT_MIN)
    configuredPidMax_ = PID_MAX_DEFAULT_MIN;
  statsArray_ = new (&statsHeap_) GlobalStatsArray[configuredPidMax_];
  for (pid_t i = 0; i < configuredPidMax_; i++) {
    statsArray_[i].processId_ = 0;
    statsArray_[i].processStats_ = NULL;
    statsArray_[i].phandleSeqNum_ = -1;
  }
}

void StatsGlobals::init() {
  Long semId;
  int error;
  char myNodeName[MAX_SEGMENT_NAME_LEN + 1];

  error = openStatsSemaphore(semId);
  ex_assert(error == 0, "BINSEM_OPEN returned an error");

  error = getStatsSemaphore(semId, GetCliGlobals()->myPin());

  stmtStatsList_ = new (&statsHeap_) SyncHashQueue(&statsHeap_, 512);
  rmsStats_ = new (&statsHeap_) ExRMSStats(&statsHeap_);
  recentSikeys_ = new (&statsHeap_) SyncHashQueue(&statsHeap_, 512);
  lobLocks_ = new (&statsHeap_) SyncHashQueue(&statsHeap_, 512);
  rmsStats_->setCpu(cpu_);
  rmsStats_->setRmsVersion(version_);
  rmsStats_->setRmsEnvType(rtsEnvType_);
  rmsStats_->setStoreSqlSrcLen(storeSqlSrcLen_);
  rmsStats_->setConfiguredPidMax(configuredPidMax_);
  int rc;
  nodeId_ = cpu_;
  MS_Mon_Node_Info_Type nodeInfo;

  rc = msg_mon_get_node_info_detail(nodeId_, &nodeInfo);
  if (rc == 0)
    strcpy(myNodeName, nodeInfo.node[0].node_name);
  else
    myNodeName[0] = '\0';
  rmsStats_->setNodeName(myNodeName);
  releaseStatsSemaphore(semId, GetCliGlobals()->myPin());
  sem_close((sem_t *)semId);
}

long StatsGlobals::getLastGCTime() { return rmsStats_->getLastGCTime(); }
pid_t StatsGlobals::getSsmpPid() { return rmsStats_->getSsmpPid(); }
long StatsGlobals::getSsmpTimestamp() { return rmsStats_->getSsmpTimestamp(); }
void StatsGlobals::setLastGCTime(long gcTime) { rmsStats_->setLastGCTime(gcTime); }
void StatsGlobals::incStmtStatsGCed(short inc) { rmsStats_->incStmtStatsGCed(inc); }
void StatsGlobals::incSsmpReqMsg(long msgBytes) { rmsStats_->incSsmpReqMsg(msgBytes); }
void StatsGlobals::incSsmpReplyMsg(long msgBytes) { rmsStats_->incSsmpReplyMsg(msgBytes); }
void StatsGlobals::incSscpReqMsg(long msgBytes) { rmsStats_->incSscpReqMsg(msgBytes); }
void StatsGlobals::incSscpReplyMsg(long msgBytes) { rmsStats_->incSscpReplyMsg(msgBytes); }
void StatsGlobals::setSscpOpens(short numSscps) { rmsStats_->setSscpOpens(numSscps); }
void StatsGlobals::setSscpDeletedOpens(short numSscps) { rmsStats_->setSscpDeletedOpens(numSscps); }
void StatsGlobals::setSscpPid(pid_t pid) { rmsStats_->setSscpPid(pid); }
void StatsGlobals::setSscpPriority(short pri) { rmsStats_->setSscpPriority(pri); }
void StatsGlobals::setSscpTimestamp(long timestamp) { rmsStats_->setSscpTimestamp(timestamp); }
void StatsGlobals::setSsmpPid(pid_t pid) { rmsStats_->setSsmpPid(pid); }
void StatsGlobals::setSsmpPriority(short pri) { rmsStats_->setSsmpPriority(pri); }
void StatsGlobals::setSsmpTimestamp(long timestamp) { rmsStats_->setSsmpTimestamp(timestamp); }
void StatsGlobals::setRMSStatsResetTimestamp(long timestamp) { rmsStats_->setRMSStatsResetTimestamp(timestamp); }
void StatsGlobals::incProcessRegd() { rmsStats_->incProcessRegd(); }
void StatsGlobals::decProcessRegd() { rmsStats_->decProcessRegd(); }
void StatsGlobals::incProcessStatsHeaps() { rmsStats_->incProcessStatsHeaps(); }
void StatsGlobals::decProcessStatsHeaps() { rmsStats_->decProcessStatsHeaps(); }
void StatsGlobals::setNodesInCluster(short numNodes) { rmsStats_->setNodesInCluster(numNodes); }

const char *StatsGlobals::rmsEnvType(RTSEnvType envType) {
  switch (envType) {
    case RTS_GLOBAL_ENV:
      return "Global Environment";
    case RTS_PRIVATE_ENV:
      return "Private Environment";
    default:
      return "Unknown";
  }
}

ObjectLockArray *StatsGlobals::dmlLockArray(pid_t pid) {
  ProcessStats *ps = checkProcess(pid);
  if (ps == NULL) {
    return NULL;
  }
  return ps->dmlLockArray();
}

ObjectLockArray *StatsGlobals::ddlLockArray(pid_t pid) {
  ProcessStats *ps = checkProcess(pid);
  if (ps == NULL) {
    return NULL;
  }
  return ps->ddlLockArray();
}

void StatsGlobals::copyDDLLocks(pid_t toPid) {
  if (!GetCliGlobals()->isMasterProcess()) {
    return;
  }
  LOGDEBUG(CAT_SQL_LOCK, "Copying DDL locks to %d", toPid);
  ex_assert(checkIfRTSSemaphoreLocked(), "Not proteced by RTS semaphore");
  ObjectLockArray *toLockArray = ddlLockArray(toPid);
  ex_assert(toLockArray != NULL, "Invalid target process id");
  for (pid_t pid = 0; pid <= maxPid_; pid++) {
    if (pid == toPid) {
      continue;
    }
    ObjectLockArray *lockArray = ddlLockArray(pid);
    if (lockArray != NULL && lockArray->maxEntries() > 0) {
      LOGDEBUG(CAT_SQL_LOCK, "Copying DDL locks from %d to %d", pid, toPid);
      toLockArray->copyLocks(*lockArray);
      return;
    }
  }
}

bool StatsGlobals::addProcess(pid_t pid, NAHeap *heap) {
  bool pidReincarnated = false;
  if (pid >= configuredPidMax_) return pidReincarnated;
  char msg[256];
  ;
  if (statsArray_[pid].processStats_ != NULL) {
    pidReincarnated = true;
    removeProcess(pid, TRUE);
  }
  unsigned short dmlLockSize = 0;
  unsigned short ddlLockSize = 0;
  if (GetCliGlobals()->isMasterProcess()) {
    dmlLockSize = dmlLockArraySize();
    ddlLockSize = ddlLockArraySize();
  }
  LOGINFO(CAT_SQL_LOCK, "Add process %d, DML lock array size: %d, DDL lock array size: %d", pid, dmlLockSize,
          ddlLockSize);
  statsArray_[pid].processId_ = pid;
  statsArray_[pid].phandleSeqNum_ = GetCliGlobals()->myVerifier();
  statsArray_[pid].processStats_ = new (heap) ProcessStats(heap, nodeId_, pid, dmlLockSize, ddlLockSize);
  incProcessRegd();
  incProcessStatsHeaps();
  copyDDLLocks(pid);
  if (pid > maxPid_) maxPid_ = pid;
  return pidReincarnated;
}

void StatsGlobals::removeProcess(pid_t pid, NABoolean calledAtAdd) {
  short retcode;
  NABoolean queryRemain = FALSE;
  NAHeap *prevHeap = NULL;
  if (pid >= configuredPidMax_) return;

  // Release all object locks held by the pid
  ObjectLockController::removeLocksHeldByProcess(pid);

  if (statsArray_[pid].processStats_ != NULL) {
    stmtStatsList_->position();
    StmtStats *ss;
    prevHeap = statsArray_[pid].processStats_->getHeap();
    while ((ss = (StmtStats *)stmtStatsList_->getNext()) != NULL) {
      if (ss->getPid() == pid) {
        retcode = removeQuery(pid, ss, FALSE, TRUE, TRUE);
        if (retcode == 1) {
          // Now that query remains after the process is gone,
          // you can delete the stats, when the query is removed
          queryRemain = TRUE;
        }
      }
    }
    NADELETE(statsArray_[pid].processStats_, ProcessStats, prevHeap);
    decProcessRegd();
    if (!(queryRemain)) {
      // Don't call NADELETE, since NADELETE needs update to VFPTR table of NAHeap object
      prevHeap->destroy();
      statsHeap_.deallocateMemory(prevHeap);
      decProcessStatsHeaps();
    }
  }
  statsArray_[pid].processId_ = 0;
  statsArray_[pid].phandleSeqNum_ = -1;
  statsArray_[pid].processStats_ = NULL;
  if (pid == maxPid_) {
    for (maxPid_--; maxPid_ > 0; maxPid_--) {
      if (statsArray_[maxPid_].processId_ != 0) break;
    }
  }
}

static bool DeadPollingInitialized = false;
static int CheckDeadFreq = 120;
static int CheckDeadTs = 0;

void StatsGlobals::checkForDeadProcesses(pid_t myPid) {
  int error = 0;

  if (myPid >= configuredPidMax_) return;

  if (!DeadPollingInitialized) {
    DeadPollingInitialized = true;  // make getenv calls once per process
    {
      char *cdf = getenv("MXSSMP_CHECK_DEAD_SECONDS");
      if (cdf) CheckDeadFreq = str_atoi(cdf, str_len(cdf));
    }
  }

  if (CheckDeadFreq <= 0) return;

  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  if (ts.tv_sec - CheckDeadTs < CheckDeadFreq) {
    if (CheckDeadTs == 0) CheckDeadTs = ts.tv_sec;
    // too soon to re-check.
    return;
  }

  CheckDeadTs = ts.tv_sec;

  error = getStatsSemaphore(ssmpProcSemId_, myPid);

  int pidRemainingToCheck = 20;
  pid_t pidsRemoved[pidRemainingToCheck];
  int numPidsRemoved = 0;
  pid_t firstPid = pidToCheck_;
  for (; maxPid_ > 0;) {
    if (pidRemainingToCheck <= 0) break;

    pidToCheck_++;

    if (pidToCheck_ > maxPid_) pidToCheck_ = 0;

    if (pidToCheck_ == firstPid) break;

    if (statsArray_[pidToCheck_].processId_ != 0) {
      pidRemainingToCheck--;

      char processName[MS_MON_MAX_PROCESS_NAME];
      int ln_error = msg_mon_get_process_name(cpu_, statsArray_[pidToCheck_].processId_, processName);
      if (ln_error == XZFIL_ERR_NOSUCHDEV) {
        pidsRemoved[numPidsRemoved++];
        removeProcess(pidToCheck_);
      }
    }
  }
  releaseStatsSemaphore(ssmpProcSemId_, myPid);
  // death messages are logged outside of semaphore due to process hop
  for (int i = 0; i < numPidsRemoved; i++) logProcessDeath(cpu_, pidsRemoved[i], "Dead Process");
}

// We expect a death message to be delivered to MXSSMP by the monitor
// when a generic SQL process exits -- see code handling system messages.
// This method is to address a concern that the death message may come
// more than 30 seconds after the process exits and the process could
// be unexpectedly holding the stats semaphore.

void StatsGlobals::cleanupDanglingSemaphore(NABoolean checkForSemaphoreHolders) {
  CliGlobals *cliGlobals = GetCliGlobals();
  NABoolean cleanupSemaphore = FALSE;
  char coreFile[512];
  if (semPid_ == -1) return;  // Nobody has the semaphore, nothing to do here.
  pid_t savedSemPid = semPid_;

  if (NOT(cliGlobals->myPin() == getSsmpPid() && cliGlobals->myStartTime() == getSsmpTimestamp()))
    return;  // Only ssmp is allowed to cleanup after another process.

  // Coverage notes - it would be too difficult to automate a test
  // for this since usually a death message is used to clean up a
  // generic SQL process' exit.  But this code has been unit tested
  // using gdb sessions on the generic process and on this MXSSMP
  // process.
  int lockedTimeInSecs = 0;
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  lockedTimeInSecs = ts.tv_sec - lockingTimestamp_.tv_sec;
  if (checkForSemaphoreHolders) {
    if (lockedTimeInSecs >= rmsDeadLockSecs_) cleanupSemaphore = TRUE;
  } else
    cleanupSemaphore = TRUE;
  if (cleanupSemaphore) {
    NAProcessHandle myPhandle;
    myPhandle.getmine();
    myPhandle.decompose();
    char processName[MS_MON_MAX_PROCESS_NAME + 1];
    pid_t tempPid = semPid_;
    bool semHoldingProcessExists = true;
    int ln_error = msg_mon_get_process_name(myPhandle.getCpu(), tempPid, processName);
    if (ln_error == XZFIL_ERR_NOSUCHDEV) {
      semHoldingProcessExists = false;
      seabedError_ = ln_error;
      seabedPidRecycle_ = false;
    } else {
      char msg[256];
      if (semPid_ != -1 && semPid_ == savedSemPid && lockedTimeInSecs >= rmsDeadLockSecs_) {
        setAbortedSemPid();
        snprintf(msg, sizeof(msg), "Pid %d, %d held semaphore for more than %d seconds ", cpu_, semPid_,
                 lockedTimeInSecs);
        SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
        if (dumpRmsDeadLockProcess_) msg_mon_dump_process_id(NULL, cpu_, semPid_, coreFile);
      }

      /*
              // Is this the same incarnation of the process name?
              // Do not be fooled by pid recycle.
              MS_Mon_Process_Info_Type processInfo;
              ln_error = msg_mon_get_process_info_detail(
                            processName, &processInfo);
              if ((ln_error == XZFIL_ERR_OK) &&
                  (ComRtGetJulianFromUTC(processInfo.creation_time) !=
                  semPidCreateTime_))
              {
                seabedError_ = 0;
                seabedPidRecycle_ = true;
                semHoldingProcessExists = false;
              }
      */
    }
    if (!semHoldingProcessExists) {
      cleanup_SQL(tempPid, myPhandle.getPin());
    }
  }
}

ProcessStats *StatsGlobals::checkProcess(pid_t pid) {
  if (pid >= configuredPidMax_) return NULL;
  if (statsArray_[pid].processId_ == pid)
    return statsArray_[pid].processStats_;
  else
    return NULL;
}

StmtStats *StatsGlobals::addQuery(pid_t pid, char *queryId, int queryIdLen, void *backRef, int fragId,
                                  char *sourceStr, int sourceStrLen, NABoolean isMaster) {
  StmtStats *ss;
  char *sqlSrc = NULL;
  int storeSqlSrcLen = 0;
  if (pid >= configuredPidMax_) return NULL;
  if (storeSqlSrcLen_ > 0) {
    sqlSrc = sourceStr;
    storeSqlSrcLen = ((sourceStrLen > storeSqlSrcLen_) ? storeSqlSrcLen_ : sourceStrLen);
  }
  if (statsArray_[pid].processStats_ != NULL && queryId != NULL) {
    ss = statsArray_[pid].processStats_->addQuery(pid, queryId, queryIdLen, backRef, fragId, sqlSrc, storeSqlSrcLen,
                                                  sourceStrLen, isMaster);
    stmtStatsList_->insert(queryId, queryIdLen, ss);
    return ss;
  } else
    return NULL;
}

int StatsGlobals::getStatsSemaphore(Long &semId, pid_t pid, NABoolean calledByVerifyAndCleanup) {
  int error = 0;
  timespec ts;
  ex_assert(pid >= 0, "Semaphore can't be obtained for pids less than 0");
  ex_assert(pid < configuredPidMax_, "Semaphore can't be obtained for pids greater than configured pid max") error =
      sem_trywait((sem_t *)semId);
  NABoolean retrySemWait = FALSE;
  NABoolean resetClock = TRUE;
  char buf[100];
  const int timeoutRetryCount = 100;  // 5min/3s
  int retryCount = 0;
  if (error != 0) {
    do {
      retrySemWait = FALSE;
      if (resetClock) {
        if ((error = clock_gettime(CLOCK_REALTIME, &ts)) < 0) {
          error = errno;
          sprintf(buf, "getStatsSemaphore() returning an error %d", error);
          ex_assert(FALSE, buf);
          return error;
        }
        ts.tv_sec += 3;
      }
      resetClock = FALSE;
      error = sem_timedwait((sem_t *)semId, &ts);
      if (error != 0) {
        switch (errno) {
          case EINTR:
            retrySemWait = TRUE;
            break;
          case EINVAL:
            error = openStatsSemaphore(semId);
            if (error == 0) retrySemWait = TRUE;
            break;
          case ETIMEDOUT:
            cleanupDanglingSemaphore(FALSE);
            retrySemWait = TRUE;
            resetClock = TRUE;
            ++retryCount;
            if (retryCount % 10 == 0)  // every 30s
            {
              QRLogger::log(
                  CAT_SQL_EXE, LL_WARN,
                  "StatsGlobals::getStatsSemaphore : sem_timedwait() retried %d times (%d*3s) due to ETIMEDOUT",
                  retryCount, retryCount);
            }
            break;
          default:
            error = errno;
            break;
        }
      }
    } while (retrySemWait && retryCount <= timeoutRetryCount);

    if (retryCount > timeoutRetryCount) {
      if (calledByVerifyAndCleanup && pid == getSsmpPid()) {
        char msg[256];
        snprintf(msg, sizeof(msg), "SSMP getStatsSemaphore exhausted retry times(%d * 3s)", retryCount);
        SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
        Long semId = ssmpProcSemId_;
        error = forceReleaseAndGetStatsSemaphore(semId, pid);
        ex_assert(error == 0, "forceReleaseAndGetStatsSemaphore returned an error");
      } else {
        QRLogger::log(CAT_SQL_EXE, LL_ERROR,
                      "StatsGlobals::getStatsSemaphore : sem_timedwait() reached its maximum(%d), retry count(%d), "
                      "exit process!!!",
                      timeoutRetryCount, retryCount);
        NAExit(0);
      }
    }
  }

  if (error == 0) {
    if (isShmDirty()) {
      genLinuxCorefile("Shared Segment might be corrupted");
      int ndRetcode = msg_mon_node_down2(getCpu(), "RMS shared segment is corrupted.");
      sleep(30);
      NAExit(0);  // already made a core.
    }
    semPid_ = pid;
    semPidCreateTime_ = GetCliGlobals()->myStartTime();
    clock_gettime(CLOCK_REALTIME, &lockingTimestamp_);
    return error;
  }
  sprintf(buf, "getStatsSemaphore() returning an error %d", error);
  ex_assert(FALSE, buf);
  return error;
}

void StatsGlobals::releaseStatsSemaphore(Long &semId, pid_t pid) {
  int error = 0;
  pid_t tempPid;
  NABoolean tempIsBeingUpdated;
  long tempSPCT;
  ex_assert(semPid_ != -1 && semPid_ == pid, "SemPid_ is -1 or semPid_ != pid");
  ex_assert(semPidCreateTime_ == GetCliGlobals()->myStartTime(), "semPidCreateTime_ unexpected.");
  tempPid = semPid_;
  tempIsBeingUpdated = isBeingUpdated_;
  tempSPCT = semPidCreateTime_;
  semPid_ = -1;
  semPidCreateTime_ = 0;
  isBeingUpdated_ = FALSE;
  error = sem_post((sem_t *)semId);
  if (error == -1) error = errno;
  if (error != 0) {
    semPid_ = tempPid;
    isBeingUpdated_ = tempIsBeingUpdated;
    semPidCreateTime_ = tempSPCT;
  }
  ex_assert(error == 0, "sem_post failed");
}

int StatsGlobals::releaseAndGetStatsSemaphore(Long &semId, pid_t pid, pid_t releasePid) {
  int error = 0;
  pid_t tempPid;
  NABoolean tempIsBeingUpdated;

  ex_assert(releasePid != -1, "release pid is -1");
  if (semPid_ == releasePid) {
    tempPid = semPid_;
    tempIsBeingUpdated = isBeingUpdated_;
    semPid_ = -1;
    errorSemPid_ = tempPid;
    releasingSemPid_ = pid;
    clock_gettime(CLOCK_REALTIME, &releasingTimestamp_);
    isBeingUpdated_ = FALSE;
    error = sem_post((sem_t *)semId);
    if (error == -1) {
      semPid_ = tempPid;
      releasingSemPid_ = -1;
      error = errno;
      isBeingUpdated_ = tempIsBeingUpdated;
      return error;
    }
  }
  error = getStatsSemaphore(semId, pid);
  return error;
}

StmtStats *StatsGlobals::addStmtStats(NAHeap *heap, pid_t pid, char *queryId, int queryIdLen, char *sourceStr,
                                      int sourceLength) {
  StmtStats *ss;

  ss = new (heap) StmtStats(heap, pid, queryId, queryIdLen, NULL, -1, sourceStr, sourceLength, sourceLength, TRUE);
  return ss;
}

short StatsGlobals::removeQuery(pid_t pid, StmtStats *stmtStats, NABoolean removeAlways, NABoolean globalScan,
                                NABoolean calledFromRemoveProcess) {
  short retcode = 1;
  NAHeap *heap = stmtStats->getHeap();
  ExMasterStats *masterStats;

  /*
   * Retain the stats in the shared segment for the following:
   * a) If it is not already flag to be GCed and
   * b) Stats from the master and
   * b) if it is either used by someone else or WMS has shown interest in it
   *    or if the query is monitored by WMS via CLI
   */
  masterStats = stmtStats->getMasterStats();
  if ((!stmtStats->canbeGCed()) && stmtStats->isMaster() && masterStats != NULL &&
      masterStats->getCollectStatsType() != (UInt16)ComTdb::NO_STATS &&
      masterStats->getCollectStatsType() != (UInt16)ComTdb::ALL_STATS &&
      (stmtStats->isStmtStatsUsed() || stmtStats->getMergedStats() != NULL || stmtStats->isWMSMonitoredCliQuery() ||
       stmtStats->aqrInProgress())) {
    if (calledFromRemoveProcess) {
      stmtStats->setTobeGCed();
      if (masterStats != NULL) {
        masterStats->setStmtState(Statement::PROCESS_ENDED_);
        masterStats->setEndTimes(TRUE);
      }
      stmtStats->setMergeReqd(TRUE);
      // When called from removeProces, it is okay to reduce the reference
      // count because the proces is already gone
      // if the reference count was incremented by SSMP, then also
      // it is ok to reduce the reference count
      // because the StmtStats will not be GCed for the next 15 minutes
      stmtStats->setStmtStatsUsed(FALSE);

    } else if (!stmtStats->aqrInProgress())
      stmtStats->setTobeGCed();
  } else {
    // Retain stats if it is in use,
    // or
    // if it not already flagged as "can be gced"
    // and the queries that are getting removed as part of removeProcess
    // to detect dead queries in case of master
    if (stmtStats->isStmtStatsUsed() || (!stmtStats->canbeGCed() && calledFromRemoveProcess && masterStats != NULL &&
                                         masterStats->getCollectStatsType() != (UInt16)ComTdb::NO_STATS &&
                                         masterStats->getCollectStatsType() != (UInt16)ComTdb::ALL_STATS)) {
      if (calledFromRemoveProcess) {
        stmtStats->setTobeGCed();
        if (masterStats != NULL) {
          masterStats->setStmtState(Statement::PROCESS_ENDED_);
          masterStats->setEndTimes(TRUE);
        }
        stmtStats->setMergeReqd(TRUE);
        // When called from removeProces, it is okay to reduce the reference
        // count because the proces is already gone
        // if the reference count was incremented by SSMP, then also
        // it is ok to reduce the reference count
        // because the StmtStats will not be GCed for the next 15 minutes
        stmtStats->setStmtStatsUsed(FALSE);
      } else if (!stmtStats->aqrInProgress())
        stmtStats->setTobeGCed();
    } else {
      if (((!stmtStats->aqrInProgress()) || calledFromRemoveProcess) && (!stmtStats->isDeleteError())) {
        stmtStats->setDeleteError(TRUE);
        stmtStats->setCalledFromRemoveQuery(TRUE);
        if (globalScan)
          stmtStatsList_->remove();
        else
          stmtStatsList_->remove(stmtStats->getQueryId(), stmtStats->getQueryIdLen(), stmtStats);
        stmtStats->deleteMe();
        memset(stmtStats, 0, sizeof(StmtStats));
        heap->deallocateMemory(stmtStats);
        retcode = 0;
      }
    }
  }

  // Remove the heap if there is no memory allocated in case of removeAlways
  // is set to TRUE. RemoveAlways should be set to TRUE only from SSMP
  if (removeAlways && retcode == 0) {
    int totalSize = (int)heap->getTotalSize();
    if (totalSize == 0) {
      // Don't call NADELETE, since NADELETE needs update to VFPTR table of NAHeap object
      heap->destroy();
      statsHeap_.deallocateMemory(heap);
      decProcessStatsHeaps();
    }
  }
  return retcode;
}

int StatsGlobals::openStatsSemaphore(Long &semId) {
  int error = 0;

  sem_t *ln_semId = sem_open((const char *)getRmsSemName(), 0);
  if (ln_semId == SEM_FAILED) {
    if (errno == ENOENT) {
      ln_semId = sem_open((const char *)getRmsSemName(), O_CREAT, RMS_SEMFLAGS, 1);
      if (ln_semId == SEM_FAILED) error = errno;
    } else
      error = errno;
  }
  if (error == 0) semId = (Long)ln_semId;
  return error;
}

int StatsGlobals::openStatsSemaphoreWithRetry(Long &semId) {
  int retry = 5, retry_cnt = 0, error = 0;
  for (retry_cnt = 0; retry_cnt < retry; ++retry_cnt) {
    error = openStatsSemaphore(semId);
    if (error != 0) {
      QRLogger::log(CAT_SQL_EXE, LL_WARN,
                    "openStatsSemaphore() failed with error = %d, will sleep 100ms then do %dth retry", error,
                    retry_cnt);
      usleep(100000);  // 100ms
    } else
      break;
  }
  return error;
}

// I removed this method and rebuilt successfully.  It seems to
// be dead code.  It is only to be extra cautious that I am not removing it
// for M5.
ExStatisticsArea *StatsGlobals::getStatsArea(char *queryId, int queryIdLen) {
  StmtStats *ss;

  stmtStatsList_->position(queryId, queryIdLen);
  ss = (StmtStats *)stmtStatsList_->getNext();
  while (ss != NULL) {
    if (str_cmp(ss->getQueryId(), queryId, queryIdLen) == 0) return ss->getStatsArea();
    ss = (StmtStats *)stmtStatsList_->getNext();
  }
  return NULL;
}

StmtStats *StatsGlobals::getMasterStmtStats(const char *queryId, int queryIdLen, short activeQueryNum) {
  StmtStats *ss;
  ExMasterStats *masterStats;
  short queryNum = 0;
  stmtStatsList_->position(queryId, queryIdLen);
  ss = (StmtStats *)stmtStatsList_->getNext();

  while (ss != NULL) {
    if (str_cmp(ss->getQueryId(), queryId, queryIdLen) == 0) {
      masterStats = ss->getMasterStats();
      if (masterStats != NULL) {
        if (filterStmtStats(masterStats, activeQueryNum, queryNum)) break;
      }
    }
    ss = (StmtStats *)stmtStatsList_->getNext();
  }
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(char *queryId, int queryIdLen) {
  StmtStats *ss;

  stmtStatsList_->position(queryId, queryIdLen);
  ss = (StmtStats *)stmtStatsList_->getNext();
  while (ss != NULL) {
    if (str_cmp(ss->getQueryId(), queryId, queryIdLen) == 0) break;
    ss = (StmtStats *)stmtStatsList_->getNext();
  }
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(pid_t pid, short activeQueryNum) {
  StmtStats *ss;
  ExMasterStats *masterStats;
  short queryNum = 0;

  stmtStatsList_->position();
  // Active Query if the pid is a master
  while ((ss = (StmtStats *)stmtStatsList_->getNext()) != NULL) {
    if (ss->getPid() == pid) {
      if (ss->getStatsArea() == NULL) continue;
      masterStats = ss->getMasterStats();
      if (masterStats != NULL) {
        if (filterStmtStats(masterStats, activeQueryNum, queryNum)) break;
      } else {
        queryNum++;
        if (queryNum == activeQueryNum) break;
      }
    }
  }
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(short activeQueryNum) {
  StmtStats *ss;
  ExMasterStats *masterStats;
  short queryNum = 0;

  stmtStatsList_->position();
  // Only the active Query whose master is that CPU is returned
  while ((ss = (StmtStats *)stmtStatsList_->getNext()) != NULL) {
    if (ss->getStatsArea() == NULL ||
        (ss->getStatsArea() != NULL && ss->getStatsArea()->getCollectStatsType() != ComTdb::ACCUMULATED_STATS &&
         ss->getStatsArea()->getCollectStatsType() != ComTdb::PERTABLE_STATS))
      continue;
    masterStats = ss->getMasterStats();
    if (masterStats != NULL) {
      if (filterStmtStats(masterStats, activeQueryNum, queryNum)) break;
    }
  }
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(pid_t pid, char *queryId, int queryIdLen, int fragId) {
  StmtStats *ss;
  stmtStatsList_->position(queryId, queryIdLen);
  ss = (StmtStats *)stmtStatsList_->getNext();

  while (ss != NULL) {
    if (ss->getPid() == pid && str_cmp(ss->getQueryId(), queryId, queryIdLen) == 0 && ss->getFragId() == fragId) {
      break;
    }
    ss = (StmtStats *)stmtStatsList_->getNext();
  }
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(short cpu, pid_t pid, long timeStamp, int queryNumber) {
  StmtStats *ss;
  char *queryId = (char *)NULL;
  int queryIdLen = 0;

  char dp2QueryId[ComSqlId::MAX_DP2_QUERY_ID_LEN];
  int dp2QueryIdLen = ComSqlId::MAX_DP2_QUERY_ID_LEN;

  int l_segment = 0;
  int l_cpu = 0;
  int l_pid = (pid_t)0;
  long l_timeStamp = 0;
  int l_queryNumber = 0;

  stmtStatsList_->position();
  while ((ss = (StmtStats *)stmtStatsList_->getNext()) != NULL) {
    if (ss->isMaster()) {
      queryId = ss->getQueryId();
      queryIdLen = ss->getQueryIdLen();

      if (-1 != ComSqlId::getDp2QueryIdString(queryId, queryIdLen, dp2QueryId, dp2QueryIdLen)) {
        if (-1 != ComSqlId::decomposeDp2QueryIdString(dp2QueryId, dp2QueryIdLen, &l_queryNumber, &l_segment, &l_cpu,
                                                      &l_pid, &l_timeStamp)) {
          if ((l_cpu == cpu) && (l_pid == pid) && (l_timeStamp == timeStamp) && (l_queryNumber == queryNumber)) break;
        }
      }
    }  // isMaster()
  }    // while
  return ss;
}

StmtStats *StatsGlobals::getStmtStats(pid_t pid, char *queryId, int queryIdLen) {
  StmtStats *ss;
  stmtStatsList_->position(queryId, queryIdLen);
  ss = (StmtStats *)stmtStatsList_->getNext();

  while (ss != NULL) {
    if (ss->getPid() == pid && str_cmp(ss->getQueryId(), queryId, queryIdLen) == 0) {
      break;
    }
    ss = (StmtStats *)stmtStatsList_->getNext();
  }
  return ss;
}

#define STATS_RETAIN_TIME_IN_MICORSECS (15 * 60 * 1000000)
void StatsGlobals::doFullGC() {
  StmtStats *ss;

  stmtStatsList_->position();
  long maxElapsedTime = STATS_RETAIN_TIME_IN_MICORSECS;
  long currentTimestamp = NA_JulianTimestamp();
  int retcode;

  short stmtStatsGCed = 0;
  while ((ss = (StmtStats *)stmtStatsList_->getNext()) != NULL) {
    // If this is the master stats, and it's marked for GC, and it's been more than 15 minutes since
    // the stats were merged the last time, remove this master stats area.

    if ((ss->isMaster()) && (ss->canbeGCed()) && (currentTimestamp - ss->getLastMergedTime() > maxElapsedTime)) {
      retcode = removeQuery(ss->getPid(), ss, TRUE, TRUE);
      if (retcode == 0) stmtStatsGCed++;
    }
  }
  incStmtStatsGCed(stmtStatsGCed);
}

void StatsGlobals::cleanupOldSikeys(long sikGcInterval) {
  long tooOld = NA_JulianTimestamp() - sikGcInterval;

  RecentSikey *recentSikey = NULL;
  recentSikeys_->position();

  while (NULL != (recentSikey = (RecentSikey *)recentSikeys_->getNext())) {
    if (recentSikey->revokeTimestamp_ < tooOld) recentSikeys_->remove();
  }

  return;
}

int StatsGlobals::registerQuery(ComDiagsArea &diags, pid_t pid, SQLQUERY_ID *query_id, int fragId, int tdbId,
                                  int explainTdbId, short statsCollectionType, int instNum,
                                  ComTdb::ex_node_type tdbType, char *tdbName, int tdbNameLen) {
  ProcessStats *processStats;
  NAHeap *heap;
  ExStatisticsArea *statsArea = NULL;
  ExOperStats *stat = NULL;
  StmtStats *ss;
  char *queryId;
  int queryIdLen;

  if (query_id == NULL || query_id->name_mode != queryid_str || query_id->identifier == NULL ||
      query_id->identifier_len < 0) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  queryId = (char *)query_id->identifier;
  queryIdLen = query_id->identifier_len;

  // Check if process is registered
  if ((processStats = checkProcess(pid)) == NULL) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }
  heap = processStats->getHeap();
  // Check if the query is already registered
  if ((ss = getStmtStats(pid, queryId, queryIdLen, fragId)) != NULL) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  // Register the query
  ss = addQuery(pid, queryId, queryIdLen, NULL, fragId);

  // allocate a FragRootOperStatsEntry
  if (ss && ((statsCollectionType == ComTdb::ALL_STATS) || (statsCollectionType == ComTdb::OPERATOR_STATS) ||
             (statsCollectionType == ComTdb::PERTABLE_STATS))) {
    statsArea = new (heap) ExStatisticsArea((NAMemory *)heap, 0, (ComTdb::CollectStatsType)statsCollectionType);
    // Pass in tdbId as zero for ex_ROOT, this stats entry shouldn't
    // be shown when TDBID_DETAIL=<tdbId> is used.
    // However, this entry will be shown when DETAIL=-1 is used
    stat = new (heap)
        ExFragRootOperStats((NAMemory *)heap, (ComTdb::CollectStatsType)statsCollectionType, (ExFragId)fragId, 0,
                            explainTdbId, instNum, ComTdb::ex_ROOT, (char *)"EX_ROOT", str_len("EX_ROOT"));
    ((ExFragRootOperStats *)stat)->setQueryId(ss->getQueryId(), ss->getQueryIdLen());
    statsArea->insert(stat);
    statsArea->setRootStats(stat);
    ss->setStatsArea(statsArea);
  }
  QueryIdInfo *queryIdInfo = new (heap) QueryIdInfo(ss, stat);
  processStats->setQueryIdInfo(queryIdInfo);
  // return the QueryIdInfo in the strucutre for easy access later
  query_id->handle = queryIdInfo;
  return SUCCESS;
}

int StatsGlobals::deregisterQuery(ComDiagsArea &diags, pid_t pid, SQLQUERY_ID *query_id, int fragId) {
  ProcessStats *processStats;
  StmtStats *ss;

  char *queryId;
  int queryIdLen;

  if (query_id == NULL || query_id->name_mode != queryid_str || query_id->identifier == NULL ||
      query_id->identifier_len < 0) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  queryId = (char *)query_id->identifier;
  queryIdLen = query_id->identifier_len;

  // Check if process is registered
  if ((processStats = checkProcess(pid)) == NULL) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  if (processStats->getQueryIdInfo() != query_id->handle) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  // Check if the query is already registered
  if ((ss = getStmtStats(pid, queryId, queryIdLen, fragId)) == NULL) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }
  if (removeQuery(pid, ss)) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }
  processStats->setQueryIdInfo(NULL);
  query_id->handle = NULL;
  return SUCCESS;
}

int StatsGlobals::updateStats(ComDiagsArea &diags, SQLQUERY_ID *query_id, void *operatorStats,
                                int operatorStatsLen) {
  int retcode = 0;
  char *queryId;
  int queryIdLen;
  QueryIdInfo *queryIdInfo;

  if (query_id == NULL || query_id->name_mode != queryid_str || query_id->identifier == NULL ||
      query_id->identifier_len < 0 || query_id->handle == 0) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }

  queryId = (char *)query_id->identifier;
  queryIdLen = query_id->identifier_len;
  queryIdInfo = (QueryIdInfo *)query_id->handle;

  ExFragRootOperStats *rootStats = NULL;
  if (queryIdInfo->ss_->getStatsArea() != NULL)
    rootStats = (ExFragRootOperStats *)queryIdInfo->ss_->getStatsArea()->getRootStats();

  if (str_cmp(queryIdInfo->eye_catcher_, QUERYID_INFO_EYE_CATCHER, 4) != 0) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return ERROR;
  }
  switch (queryIdInfo->operStats_->getTdbType()) {
    default:
      retcode = -1;
      break;
  }
  if (retcode < 0) diags << DgSqlCode(-CLI_INTERNAL_ERROR);
  return retcode;
}
int StatsGlobals::checkLobLock(CliGlobals *cliGlobals, char *&lobLockId) {
  int error = getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  if ((lobLocks_ == NULL) || lobLocks_->isEmpty()) {
    lobLockId = NULL;
    releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
    return 0;
  }
  lobLocks_->position(lobLockId, LOB_LOCK_ID_SIZE);
  // Look in the current chain for a match
  while (lobLocks_->getCurr() != NULL && memcmp(lobLockId, (char *)(lobLocks_->getCurr()), LOB_LOCK_ID_SIZE) != 0)
    lobLocks_->getNext();
  if (lobLocks_->getCurr() == NULL) lobLockId = NULL;

  releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  return 0;
}
int StatsGlobals::getSecInvalidKeys(CliGlobals *cliGlobals, long lastCallTimestamp, SQL_QIKEY siKeys[],
                                      int maxNumSiKeys, int *returnedNumSiKeys) {
  int retcode = 0;
  int error = getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());

  int numToReturn = 0;
  RecentSikey *recentSikey = NULL;
  recentSikeys_->position();
  while (NULL != (recentSikey = (RecentSikey *)recentSikeys_->getNext())) {
    if (recentSikey->revokeTimestamp_ > lastCallTimestamp) {
      numToReturn++;
      if (numToReturn <= maxNumSiKeys) siKeys[numToReturn - 1] = recentSikey->s_;
    }
  }
  *returnedNumSiKeys = numToReturn;
  if (numToReturn > maxNumSiKeys) retcode = -CLI_INSUFFICIENT_SIKEY_BUFF;

  releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  return retcode;
}

void StatsGlobals::mergeNewSikeys(int numSikeys, SQL_QIKEY sikeys[]) {
  newestRevokeTimestamp_ = NA_JulianTimestamp();
  for (int i = 0; i < numSikeys; i++) {
    SQL_QIKEY newKey;
    memset(&newKey, 0, sizeof(SQL_QIKEY));
    newKey.operation[0] = sikeys[i].operation[0];
    newKey.operation[1] = sikeys[i].operation[1];
    ComQIActionType siKeyType = ComQIActionTypeLiteralToEnum(newKey.operation);
    if (siKeyType == COM_QI_OBJECT_REDEF || siKeyType == COM_QI_STATS_UPDATED) {
      newKey.ddlObjectUID = sikeys[i].ddlObjectUID;
    } else {
      newKey.revokeKey.object = sikeys[i].revokeKey.object;
      newKey.revokeKey.subject = sikeys[i].revokeKey.subject;
    }
    bool updatedExistingRecentKey = false;
    recentSikeys_->position((char *)&newKey, sizeof(newKey));
    RecentSikey *existingRecentKey = NULL;
    while (NULL != (existingRecentKey = (RecentSikey *)recentSikeys_->getNext())) {
      if (!memcmp(&existingRecentKey->s_, &newKey, sizeof(newKey))) {
        existingRecentKey->revokeTimestamp_ = newestRevokeTimestamp_;
        updatedExistingRecentKey = true;
        break;
      }
    }
    if (!updatedExistingRecentKey) {
      RecentSikey *newRecentKey = new (&statsHeap_) RecentSikey;
      newRecentKey->s_ = newKey;
      newRecentKey->revokeTimestamp_ = newestRevokeTimestamp_;
      recentSikeys_->insert((char *)&newRecentKey->s_, sizeof(newRecentKey->s_), newRecentKey);
    }
  }
}

ProcessStats::ProcessStats(NAHeap *heap, short nid, pid_t pid, unsigned short dmlLockArraySize,
                           unsigned short ddlLockArraySize)
    : heap_(heap),
      stats_(NULL),
      queryIdInfo_(NULL),
      dmlLockArray_(dmlLockArraySize, heap),
      ddlLockArray_(ddlLockArraySize, heap),
      holdingDDLLocks_(false),
      ddlStmtLevel_(0) {
  exProcessStats_ = new (heap_) ExProcessStats(heap_, nid, pid);
}

ProcessStats::~ProcessStats() {
  if (queryIdInfo_ != NULL) {
    NADELETE(queryIdInfo_, QueryIdInfo, heap_);
    queryIdInfo_ = NULL;
  }
  if (exProcessStats_ != NULL) {
    NADELETE(exProcessStats_, ExProcessStats, heap_);
    exProcessStats_ = NULL;
  }
}

inline size_t ProcessStats::getExeMemHighWM() { return exProcessStats_->getExeMemHighWM(); }
inline size_t ProcessStats::getExeMemAlloc() { return exProcessStats_->getExeMemAlloc(); }
inline size_t ProcessStats::getExeMemUsed() { return exProcessStats_->getExeMemUsed(); }
inline size_t ProcessStats::getIpcMemHighWM() { return exProcessStats_->getIpcMemHighWM(); }
inline size_t ProcessStats::getIpcMemAlloc() { return exProcessStats_->getIpcMemAlloc(); }
inline size_t ProcessStats::getIpcMemUsed() { return exProcessStats_->getIpcMemUsed(); }

void ProcessStats::updateMemStats(NAHeap *exeHeap, NAHeap *ipcHeap) {
  if (exProcessStats_ != NULL) exProcessStats_->updateMemStats(exeHeap, ipcHeap);
}

void ProcessStats::setStatsArea(ExStatisticsArea *stats) {
  if (stats_ != NULL) {
    NADELETE(stats, ExStatisticsArea, heap_);
  }
  stats_ = stats;
}

StmtStats *ProcessStats::addQuery(pid_t pid, char *queryId, int queryIdLen, void *backRef, int fragId,
                                  char *sourceStr, int sourceLen, int sqlSourceLen, NABoolean isMaster) {
  StmtStats *ss;

  ss = new (heap_)
      StmtStats(heap_, pid, queryId, queryIdLen, backRef, fragId, sourceStr, sourceLen, sqlSourceLen, isMaster);
  return ss;
}

StmtStats::StmtStats(NAHeap *heap, pid_t pid, char *queryId, int queryIdLen, void *backRef, int fragId,
                     char *sourceStr, int sourceStrLen, int sqlStrLen, NABoolean isMaster)
    : heap_(heap), pid_(pid), stats_(NULL), refCount_(0), fragId_(fragId) {
  queryId_ = new (heap_) char[queryIdLen + 1];
  str_cpy_all(queryId_, queryId, queryIdLen);
  queryId_[queryIdLen] = '\0';
  queryIdLen_ = queryIdLen;
  if (isMaster)
    masterStats_ = new (heap_) ExMasterStats(heap_, sourceStr, sourceStrLen, sqlStrLen, queryId_, queryIdLen_);
  else
    masterStats_ = NULL;
  lastMergedTime_ = 0;
  mergedStats_ = NULL;
  flags_ = 0;
  setMaster(isMaster);
  backRef_ = backRef;
  explainInfo_ = NULL;
  updateChildQid_ = FALSE;
}

StmtStats::~StmtStats() { deleteMe(); }

void StmtStats::deleteMe() {
  if (!checkIfRTSSemaphoreLocked()) abort();
  // in case of Linux, create tempStats to do fixup
  // since vptr table will vary from one instance to another
  // of the same program (mxssmp)
  ExStatisticsArea tempStats(heap_);
  if (stats_ != NULL && deleteStats()) {
    stats_->fixup(&tempStats);
    NADELETE(stats_, ExStatisticsArea, stats_->getHeap());
    stats_ = NULL;
  }

  if (masterStats_ != NULL) {
    ExMasterStats masterStats;
    masterStats_->fixup(&masterStats);
    NADELETE(masterStats_, ExMasterStats, masterStats_->getHeap());
    masterStats_ = NULL;
  }
  if (mergedStats_ != NULL) {
    mergedStats_->fixup(&tempStats);
    NADELETE(mergedStats_, ExStatisticsArea, mergedStats_->getHeap());
    mergedStats_ = NULL;
  }

  NADELETEBASIC(queryId_, heap_);
  if (explainInfo_) {
    RtsExplainFrag explainInfo;
    explainInfo_->fixup(&explainInfo);
    NADELETE(explainInfo_, RtsExplainFrag, heap_);
    explainInfo_ = NULL;
  }
  return;
}

void StmtStats::setExplainFrag(void *explainFrag, int len, int topNodeOffset) {
  if (explainInfo_ == NULL) explainInfo_ = new (heap_) RtsExplainFrag((NAMemory *)heap_);
  explainInfo_->setExplainFrag(explainFrag, len, topNodeOffset);
}

void StmtStats::deleteExplainFrag() {
  if (explainInfo_) {
    NADELETE(explainInfo_, RtsExplainFrag, heap_);
    explainInfo_ = NULL;
  }
}

void StmtStats::setStatsArea(ExStatisticsArea *stats) {
  if (!isMaster()) {
    stats_ = stats;
    return;
  }
  if (stats == NULL) {
    if (stats_ != NULL) {
      masterStats_ = stats_->getMasterStats();
      stats_->setMasterStats(NULL);
    }
  } else {
    if (stats_ == NULL) {
      masterStats_->setCollectStatsType(stats->getCollectStatsType());
      stats->setMasterStats(masterStats_);
      masterStats_ = NULL;
    } else {
      stats->setMasterStats(stats_->getMasterStats());
      stats_->setMasterStats(NULL);
      // delete stats_ if the flag is set. Otherwise, someone else is
      // referring to this area, and will delete it later on
      if (deleteStats()) {
        NADELETE(stats_, ExStatisticsArea, stats_->getHeap());
      }
    }
  }
  stats_ = stats;
}

void StmtStats::setMergedStats(ExStatisticsArea *stats) {
  if (stats == mergedStats_) return;
  if (mergedStats_ != NULL) {
    mergedStats_->fixup(stats);
    NADELETE(mergedStats_, ExStatisticsArea, mergedStats_->getHeap());
  }
  mergedStats_ = stats;
  lastMergedTime_ = NA_JulianTimestamp();
  setMergeReqd(FALSE);
}

ExMasterStats *StmtStats::getMasterStats() {
  if (masterStats_ == NULL) {
    if (stats_ != NULL)
      return stats_->getMasterStats();
    else
      return NULL;
  } else
    return masterStats_;
}

void StmtStats::reuse(void *backRef) {
  setTobeGCed(FALSE);
  setMergeReqd(TRUE);
  if (masterStats_ == NULL && stats_ != NULL) {
    masterStats_ = stats_->getMasterStats();
    stats_->setMasterStats(NULL);
  }
  if (masterStats_ != NULL) masterStats_->reuse();
  if (stats_ != NULL) {
    if (deleteStats()) {
      NADELETE(stats_, ExStatisticsArea, stats_->getHeap());
      stats_ = NULL;
    }
  }
  backRef_ = backRef;
}

void StmtStats::setParentQid(char *parentQid, int parentQidLen, char *parentQidSystem, int parentQidSystemLen,
                             short myCpu, short myNodeId) {
  short parentQidCpu;
  short parentQidNodeId;
  long value = 0;
  int retcode;
  getMasterStats()->setParentQid(parentQid, parentQidLen);
  getMasterStats()->setParentQidSystem(parentQidSystem, parentQidSystemLen);
  if (parentQid != NULL) {
    retcode = ComSqlId::getSqlQueryIdAttr(ComSqlId::SQLQUERYID_SEGMENTNUM, parentQid, parentQidLen, value, NULL);
    if (retcode == 0)
      parentQidNodeId = (short)value;
    else
      parentQidNodeId = -1;

    retcode = ComSqlId::getSqlQueryIdAttr(ComSqlId::SQLQUERYID_CPUNUM, parentQid, parentQidLen, value, NULL);
    if (retcode == 0)
      parentQidCpu = (short)value;
    else
      parentQidCpu = -1;
    if (parentQidCpu == myCpu && parentQidNodeId == myNodeId) updateChildQid_ = TRUE;
  }
}

void StatsGlobals::cleanup_SQL(pid_t pidToCleanup, pid_t myPid) {
  if (myPid != getSsmpPid()) return;
  Long semId = ssmpProcSemId_;
  int error = releaseAndGetStatsSemaphore(semId, myPid, pidToCleanup);
  ex_assert(error == 0, "releaseAndGetStatsSemaphore() returned an error");

  unlockIfHeapMutexLocked();
  removeProcess(pidToCleanup);
  releaseStatsSemaphore(semId, myPid);
  logProcessDeath(cpu_, pidToCleanup, "Clean dangling semaphore");
}

void StatsGlobals::verifyAndCleanup(pid_t pidThatDied, SB_Int64_Type seqNum) {
  bool processRemoved = false;
  int error = getStatsSemaphore(ssmpProcSemId_, getSsmpPid(), TRUE);
  if (statsArray_ && (statsArray_[pidThatDied].processId_ == pidThatDied) &&
      (statsArray_[pidThatDied].phandleSeqNum_ == seqNum)) {
    removeProcess(pidThatDied);
    processRemoved = true;
  }
  releaseStatsSemaphore(ssmpProcSemId_, getSsmpPid());
  if (processRemoved) logProcessDeath(cpu_, pidThatDied, "Received death message");
}

void StatsGlobals::updateMemStats(pid_t pid, NAHeap *exeHeap, NAHeap *ipcHeap) {
  ProcessStats *processStats = checkProcess(pid);
  if (processStats != NULL) processStats->updateMemStats(exeHeap, ipcHeap);
}

ExProcessStats *StatsGlobals::getExProcessStats(pid_t pid) {
  ProcessStats *processStats = checkProcess(pid);
  if (processStats != NULL)
    return processStats->getExProcessStats();
  else
    return NULL;
}

void StatsGlobals::getMemOffender(ExStatisticsArea *statsArea, size_t filter) {
  pid_t pid;
  size_t memToCompare;
  ProcessStats *processStats;
  ExProcessStats *exProcessStats;
  size_t memThreshold;

  for (pid = 0; pid < maxPid_; pid++) {
    if ((processStats = statsArray_[pid].processStats_) == NULL) continue;
    switch (statsArea->getSubReqType()) {
      case SQLCLI_STATS_REQ_MEM_HIGH_WM:
        memThreshold = filter << 20;
        memToCompare = processStats->getExeMemHighWM() + processStats->getIpcMemHighWM();
        break;
      case SQLCLI_STATS_REQ_MEM_ALLOC:
        memThreshold = filter << 20;
        memToCompare = processStats->getExeMemAlloc() + processStats->getIpcMemAlloc();
        break;
      case SQLCLI_STATS_REQ_PFS_USE:
        memToCompare = processStats->getExProcessStats()->getPfsCurUse();
        memThreshold = (size_t)((double)filter * (double)processStats->getExProcessStats()->getPfsSize() / 100);
        break;
      default:
        continue;
    }
    if (memToCompare > 0 && memToCompare >= memThreshold) {
      exProcessStats = new (statsArea->getHeap()) ExProcessStats(statsArea->getHeap());
      exProcessStats->copyContents(processStats->getExProcessStats());
      statsArea->insert(exProcessStats);
    }
  }
}

StatsGlobals *shareStatsSegmentWithRetry(int &shmid, NABoolean checkForSSMP) {
  int retry = 5;
  StatsGlobals *statsGlobals;
  for (int i = 0; i < retry; i++) {
    statsGlobals = shareStatsSegment(shmid, checkForSSMP);
    if (statsGlobals != NULL)
      break;
    else {
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "shareStatsSegment return NULL, will sleep 1s then do %dth retry", i + 1);
      sleep(1);  // 1s
    }
  }
  return statsGlobals;
}

StatsGlobals *shareStatsSegment(int &shmid, NABoolean checkForSSMP) {
  void *statsGlobalsAddr = NULL;
  StatsGlobals *statsGlobals;
  long enableHugePages;
  int shmFlag = RMS_SHMFLAGS;
  static char *envShmHugePages = getenv("SQ_RMS_ENABLE_HUGEPAGES");
  if (envShmHugePages != NULL) {
    enableHugePages = (long)str_atoi(envShmHugePages, str_len(envShmHugePages));
    if (enableHugePages > 0) shmFlag = shmFlag | SHM_HUGETLB;
  }

  if ((shmid = shmget((key_t)getStatsSegmentId(),
                      0,  // size doesn't matter unless we are creating.
                      shmFlag)) == -1) {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "shmget() failed with errno = %d, SegmentId = %d, shmFlag = %d", errno,
                  getStatsSegmentId(), shmFlag);
    return NULL;
  }
  if ((statsGlobalsAddr = shmat(shmid, getRmsSharedMemoryAddr(), SHM_RND)) == (void *)-1) {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "shmat() failed with errno = %d, shmid = %d, getRmsSharedMemoryAddr = %p",
                  errno, shmid, getRmsSharedMemoryAddr());
    return NULL;
  }
  gRmsSharedMemoryAddr_ = statsGlobalsAddr;
  statsGlobals = (StatsGlobals *)statsGlobalsAddr;
  if (statsGlobals != NULL) {
    short i = 0;
    while (i < 3 && statsGlobals != (StatsGlobals *)statsGlobals->getStatsSharedSegAddr()) {
      DELAY(100);
      i++;
    }
    if (statsGlobals->getStatsSharedSegAddr() != NULL) {
      ex_assert(statsGlobals == (StatsGlobals *)statsGlobals->getStatsSharedSegAddr(),
                "Stats Shared segment can not be shared at the created addresss");
    } else {
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "statsGlobals->getStatsSharedSegAddr() == NULL");
      statsGlobals = NULL;
    }
    if (statsGlobals != NULL && statsGlobals->IsSscpInitialized() != TRUE) {
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "statsGlobals->IsSscpInitialized() is not TRUE");
      statsGlobals = NULL;
    }
  }
  return statsGlobals;
}

NABoolean StatsGlobals::getInitError(pid_t pid, NABoolean &reportError) {
  NABoolean retcode = FALSE;
  reportError = FALSE;
  if ((getVersion() != StatsGlobals::CURRENT_SHARED_OBJECTS_VERSION_) || (pid >= configuredPidMax_)) {
    retcode = TRUE;
    if (pidViolationCount_++ < PID_VIOLATION_MAX_COUNT) reportError = TRUE;
  }
  return retcode;
}

void StatsGlobals::logProcessDeath(short cpu, pid_t pid, const char *reason) {
  char msg[256];

  snprintf(msg, sizeof(msg), "Pid %d,%d de-registered from shared segment. Reason: %s ", cpu, pid, reason);
  SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
  return;
}

short getMasterCpu(char *uniqueStmtId, int uniqueStmtIdLen, char *nodeName, short maxLen, short &cpu) {
  int nodeNumber = 0;
  cpu = 0;
  short rc;
  int retcode;

  long value = 0;
  retcode = ComSqlId::getSqlQueryIdAttr(ComSqlId::SQLQUERYID_SEGMENTNUM, uniqueStmtId, uniqueStmtIdLen, value, NULL);
  if (retcode == 0)
    nodeNumber = (int)value;
  else
    return -1;

  retcode = ComSqlId::getSqlQueryIdAttr(ComSqlId::SQLQUERYID_CPUNUM, uniqueStmtId, uniqueStmtIdLen, value, NULL);
  if (retcode == 0)
    cpu = (short)value;
  else
    return -1;

  short len;
  rc = NODENUMBER_TO_NODENAME_(nodeNumber, nodeName, maxLen - 1, &len);
  if (rc == 0) {
    nodeName[len] = '\0';
    return 0;
  } else
    return -1;
}

short getStmtNameInQid(char *uniqueStmtId, int uniqueStmtIdLen, char *stmtName, short maxLen) {
  int retcode;
  long value = maxLen;

  retcode = ComSqlId::getSqlQueryIdAttr(ComSqlId::SQLQUERYID_STMTNAME, uniqueStmtId, uniqueStmtIdLen, value, stmtName);
  if (retcode == 0) stmtName[value] = '\0';
  return (short)retcode;
}

NABoolean filterStmtStats(ExMasterStats *masterStats, short activeQueryNum, short &queryNum) {
  NABoolean queryFound = FALSE;

  ex_assert(masterStats != NULL, "MasterStats can't be null");

  switch (activeQueryNum) {
    case RtsQueryId::ANY_QUERY_:
      queryFound = TRUE;
      break;
    case RtsQueryId::ALL_ACTIVE_QUERIES_:
      if ((masterStats->getExeEndTime() == -1 && masterStats->getExeStartTime() != -1) ||
          (masterStats->getCompEndTime() == -1 && masterStats->getCompStartTime() != -1))
        queryFound = TRUE;
      break;
    default:
      if ((masterStats->getExeEndTime() == -1 && masterStats->getExeStartTime() != -1) ||
          (masterStats->getCompEndTime() == -1 && masterStats->getCompStartTime() != -1))
        queryNum++;
      if (queryNum == activeQueryNum) queryFound = TRUE;
      break;
  }
  return queryFound;
}

SB_Phandle_Type *getMySsmpPhandle() {
  CliGlobals *cliGlobals = GetCliGlobals();
  if (cliGlobals->getStatsGlobals())
    return cliGlobals->getStatsGlobals()->getSsmpProcHandle();
  else
    return NULL;
}

short getRTSSemaphore() {
  // 0 means NO stats globals or not locked
  // 1 means locked
  short retcode = 0;
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = NULL;
  int error;
  if (cliGlobals) statsGlobals = cliGlobals->getStatsGlobals();

  if (statsGlobals) {
    if (statsGlobals->getSemPid() != cliGlobals->myPin()) {
      LOGTRACE(CAT_SQL_LOCK, "Acquiring RTS semaphore for pid %d", cliGlobals->myPin());
      error = statsGlobals->getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
      LOGTRACE(CAT_SQL_LOCK, "Acquired RTS semaphore for pid %d, error: %d", cliGlobals->myPin(), error);
      retcode = 1;
    }
  }
  return retcode;
}

void updateMemStats() {
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = NULL;
  if (cliGlobals) statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals)
    statsGlobals->updateMemStats(cliGlobals->myPin(), cliGlobals->getExecutorMemory(), cliGlobals->getProcessIpcHeap());
}

void releaseRTSSemaphore() {
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = NULL;
  if (cliGlobals) statsGlobals = cliGlobals->getStatsGlobals();

  if (statsGlobals) {
    if (statsGlobals->getSemPid() == cliGlobals->myPin()) {
      LOGTRACE(CAT_SQL_LOCK, "Releasing RTS semaphore for pid %d", cliGlobals->myPin());
      // Though the semPid_ is saved in abortedSemPid_, you need to look at
      // the stack trace if the process is being aborted. releaseRTSSemaphore
      // will be called even when the process is not aborting
      statsGlobals->setAbortedSemPid();
      statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
      LOGTRACE(CAT_SQL_LOCK, "Released RTS semaphore for pid %d", cliGlobals->myPin());
    }
  }
}

NABoolean checkIfRTSSemaphoreLocked() {
  NABoolean retcode = FALSE;

  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = NULL;

  if (cliGlobals) statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals) {
    if (statsGlobals->getSemPid() == cliGlobals->myPin()) {
      statsGlobals->setShmDirty();
      retcode = TRUE;
    }
  } else {
    retcode = TRUE;
  }
  return retcode;
}

char *gRmsSemName_ = NULL;

char *getRmsSemName() {
  if (gRmsSemName_ == NULL) {
    gRmsSemName_ = new char[100];
    sprintf(gRmsSemName_, "%s%d.%d", RMS_SEM_NAME, getuid(), getStatsSegmentId());
  }
  return gRmsSemName_;
}

void *gRmsSharedMemoryAddr_ = NULL;

void *getRmsSharedMemoryAddr() {
  const char *rmsAddrStr;
  long rmsAddr;
  char *endPtr;
  if (gRmsSharedMemoryAddr_ == NULL) {
    if ((rmsAddrStr = getenv("RMS_SHARED_MEMORY_ADDR")) == NULL)
      gRmsSharedMemoryAddr_ = (void *)RMS_SHARED_MEMORY_ADDR;
    else {
      rmsAddr = strtol(rmsAddrStr, &endPtr, 16);
      if (*endPtr == '\0')
        gRmsSharedMemoryAddr_ = (void *)rmsAddr;
      else
        ex_assert(0, "Invalid RMS Shared Memory Address (RMS_SHARED_MEMORY_ADDR)");
    }
  }
  return gRmsSharedMemoryAddr_;
}

short getDefineNumericValue(char *defineName, short *numValue) {
  short defineValueLen = 0;
  short error = 0;
  *numValue = 0;
  return error;
}

ObjectEpochCacheEntryName::ObjectEpochCacheEntryName(NAHeap *heap, const char *objectName, ComObjectType objectType,
                                                     long redefTime)
    : redefTime_(redefTime), objectName_(heap), dlockKey_(heap) {
  // Unfortunately, our engine is very sloppy in its handling of
  // double quotes for delimited identifiers. Sometimes they are
  // present and sometimes not. In order to correctly compute the
  // key for our cache, we need a canonical form. Distributed locks
  // don't tolerate double quotes, so we choose a quote-less
  // representation for our canonical form.

  size_t length = strlen(objectName);
  char temp[length + 1];
  char *next = temp;

  for (size_t i = 0; i < length; i++) {
    if (objectName[i] != '"')  // remove nasty double quotes
    {
      *next = objectName[i];
      next++;
    }
  }
  *next = '\0';

  // Unfortunately, our engine also has many different views of an
  // object name.  If the object name is prefixed with the namespace
  // strip it off before copying to the class.

  char *nsEnd = strchr(temp, ':');
  if (nsEnd)
    objectName_ = nsEnd + 1;
  else
    objectName_ = temp;

  dlockKey_ = "ObjectEpochCacheEntry_";
  dlockKey_ += objectName_;
  dlockKey_ += "_";
  dlockKey_ += comObjectTypeLit(objectType);
  hash_ = dlockKey_.hash();  // TODO: this is only 32 bit hash; is that good enough?
}

ObjectEpochCache::ObjectEpochCache(StatsGlobals *statsGlobals, NAHeap *heap)
    : statsGlobals_(statsGlobals), epochHeap_(heap) {
  statsGlobals_->setObjectEpochCache(this);
  epochList_ = new (epochHeap()) NAHashDictionary<ObjectEpochCacheKey, ObjectEpochCacheEntry>(
      &hashFunc,
      1021,  // prime, to avoid skew
      TRUE, epochHeap(), FALSE /* heap allocation failure is not fatal */);
}

short ObjectEpochCache::createEpochEntry(ObjectEpochCacheEntry *&entry, const ObjectEpochCacheKey &key, UInt32 epoch,
                                         UInt32 flags) {
  // A note on semaphore use: For now we are using the RTS semaphore to guard
  // access to the epochList_. We expect access to epochList_ to always be
  // fast as it is a fairly simple in-memory operation.
  //
  // Nevertheless, this may have the side effect of bottlenecking
  // concurrency to the RMS segment in general, so it would be better to use
  // a separate semaphore for this purpose. We can use the NamedSemaphore class
  // to get one. Some extension is needed, though: We need logic to clean up
  // a dangling NamedSemaphore should process death occur while the semaphore
  // is held. (Such logic exists for the RTS semaphore today in method
  // StatsGlobals::cleanupDanglingSemaphore.) This requires some thought, so
  // we defer that work to a later check-in.

  short semRetCode = getRTSSemaphore();
  if (hasEpochEntry(key)) {
    if (semRetCode == 1) releaseRTSSemaphore();
    return OEC_ERROR_ENTRY_EXISTS;
  }

  // Unfortunately, the NAHashDictionary object expects the key to be
  // in an area of memory that it can point to. So we have to allocate
  // a buffer and copy the key value there.
  ObjectEpochCacheKey *k = new (epochHeap()) ObjectEpochCacheKey(key, epochHeap());
  if (k == NULL) {
    if (semRetCode == 1) releaseRTSSemaphore();
    return OEC_ERROR_NO_MEMORY;
  }

  entry = new (epochHeap()) ObjectEpochCacheEntry(epoch, flags);
  if (entry == NULL) {
    NADELETE(k, ObjectEpochCacheKey, epochHeap());
    if (semRetCode == 1) releaseRTSSemaphore();
    return OEC_ERROR_NO_MEMORY;
  }

  epochList_->insert(k, entry, &hashFunc);
  if (semRetCode == 1) releaseRTSSemaphore();
  return OEC_ERROR_OK;
}

short ObjectEpochCache::getEpochEntry(ObjectEpochCacheEntry *&entry, const ObjectEpochCacheKey &key) {
  short semRetCode = getRTSSemaphore();
  entry = epochList_->getFirstValue(&key, &hashFunc);
  if (semRetCode == 1) releaseRTSSemaphore();
  if (entry == NULL) {
    return OEC_ERROR_ENTRY_NOT_EXISTS;
  }
  return OEC_ERROR_OK;
}

short ObjectEpochCache::updateEpochEntry(ObjectEpochCacheEntry *&entry, const ObjectEpochCacheKey &key, UInt32 newEpoch,
                                         UInt32 newFlags) {
  short rc = getEpochEntry(entry, key);  // semaphore gotten and released during call
  if (rc != OEC_ERROR_OK) {
    return rc;
  }

  entry->update(newEpoch, newFlags);
  return OEC_ERROR_OK;
}

short ObjectEpochCache::deleteEpochEntry(ObjectEpochCacheKey &key) {
  short semRetCode = getRTSSemaphore();
  epochList_->remove(&key, &hashFunc);
  if (semRetCode == 1) releaseRTSSemaphore();
  return OEC_ERROR_OK;
}

NABoolean ObjectEpochCache::hasEpochEntry(const ObjectEpochCacheKey &key) {
  return epochList_->contains(&key, &hashFunc);
}

void ObjectEpochCache::appendToStats(ExStatisticsArea *statsArea, const char *objectName, bool locked) {
  short semRetCode = getRTSSemaphore();

  NAMemory *heap = statsArea->getHeap();
  ObjectEpochCacheKey *key;
  ObjectEpochCacheEntry *entry;
  NAHashDictionaryIterator<ObjectEpochCacheKey, ObjectEpochCacheEntry> epochIter(*epochList_, NULL, NULL, heap);
  short cpu = statsGlobals_->getCpu();
  while (epochIter.getNext(key, entry)) {
    if (locked && !entry->isLocked()) {
      continue;
    }
    if ((objectName != NULL) && (strcmp(key->objectName(), objectName) != 0)) {
      continue;
    }
    ExObjectEpochStats *epochStats =
        new (heap) ExObjectEpochStats(heap, cpu, key->objectName(), key->objectType(), key->hash(),
                                      0,  // TODO redefTime
                                      entry->epoch(), entry->flags());
    statsArea->insert(epochStats);
  }

  if (semRetCode == 1) releaseRTSSemaphore();
}

ObjectEpochCacheKey::ObjectEpochCacheKey(const char *objectName, int hash, ComObjectType objectType, NAHeap *heap)
    : objectName_(heap), hash_(hash), objectType_(objectType) {
  // Unfortunately, our engine is very sloppy in its handling of
  // double quotes for delimited identifiers. Sometimes they are
  // present and sometimes not. In order to correctly compute the
  // key for our cache, we need a canonical form. Distributed locks
  // don't tolerate double quotes, so we choose a quote-less
  // representation for our canonical form.

  size_t length = strlen(objectName);
  char temp[length + 1];
  char *next = temp;

  for (size_t i = 0; i < length; i++) {
    if (objectName[i] != '"')  // remove nasty double quotes
    {
      *next = objectName[i];
      next++;
    }
  }
  *next = '\0';

  // Unfortunately, our engine also has many different views of an
  // object name.  If the object name is prefixed with the namespace
  // strip it off before copying to the class.

  char *nsEnd = strchr(temp, ':');
  if (nsEnd)
    objectName_ = nsEnd + 1;
  else
    objectName_ = temp;
}

void testObjectEpochCache(int argc, char **argv) {
  // find the ObjectEpochCache

  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = NULL;
  if (cliGlobals) statsGlobals = cliGlobals->getStatsGlobals();

  if (!statsGlobals) {
    cout << "RMS segment not available." << endl;
    return;
  }

  ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();

  cout << "The ObjectEpochCache currently has " << objectEpochCache->entries() << " entries." << endl;

  // TODO: It would be nice to have an option that dumps all of the
  // ObjectEpochCache entries. This is a little tricky though. There
  // is a handy class NAHashDictionaryIterator in common/Collections.h,
  // but it requires a CollHeap. We could use the one that the
  // ObjectEpochCache itself uses, but then we'd have to use a semaphore
  // to prevent concurrent access. This is a bit dangerous, because if
  // someone hit "break" while running this test utility, the semaphore
  // might get stuck and then no-one can add anything to the cache.
  // So it would be more robust to create our own CollHeap here on the
  // stack and use that. The iterator appears to never write anything
  // into the NAHashDictionary itself so this should be safe from a
  // node integrity standpoint. The NAHashDictionary could change while we
  // are reading it, though, so there are some small window exposures
  // where we might have difficulties and die. But since this is a test
  // utility only, those are probably acceptable.

  // for each object mentioned in the arguments, display its
  // ObjectEpochCacheEntry

  for (size_t i = 2; i < argc; i++) {
    // Massage the name: upshift any regular identifiers, and
    // remove quotes from any delimited identifiers. We won't
    // bother checking lexical correctness (e.g. double quote
    // in the middle of a regular identifier) since this is
    // just a test utility.

    // A note: To even get a delimited identifier here, one has
    // to escape the double quotes on the command line because
    // the shell removes unescaped double quotes. So, for example,
    // to test TRAFODION.SCH."tTt", one must specify
    // TRAFODION.SCH.\"tTt\" as the command line argument.

    bool inQuotes = false;
    size_t length = strlen(argv[i]);
    char objectName[length + 1];
    char *nextTarget = objectName;
    char *source = argv[i];

    for (size_t j = 0; j < length; j++) {
      if (source[j] == '"')
        inQuotes = !inQuotes;
      else if (inQuotes) {
        *nextTarget = source[j];
        nextTarget++;
      } else {
        *nextTarget = toupper(source[j]);
        nextTarget++;
      }
    }
    *nextTarget = '\0';

    // now look for it in the object cache

    ObjectEpochCacheEntryName oecName(NULL, objectName, COM_BASE_TABLE_OBJECT, 0);
    ObjectEpochCacheEntry *oecEntry = NULL;
    ObjectEpochCacheKey key(objectName, oecName.getHash(), COM_BASE_TABLE_OBJECT);
    short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */, key);
    if (oecEntry) {
      UInt32 flags = oecEntry->flags();
      cout << "Object " << objectName << " epoch: " << oecEntry->epoch() << " flags: " << flags;
      if (flags & ObjectEpochChangeRequest::DDL_IN_PROGRESS) {
        cout << " DDL operation in progress";
      }
      if (flags & ObjectEpochChangeRequest::READS_DISALLOWED) {
        cout << ", Reads disallowed";
      }
      cout << endl;
    } else {
      // maybe the user didn't fully qualify?
      int dots = 0;
      for (size_t k = 0; k < length; k++) {
        if (objectName[k] == '.') dots++;
      }

      cout << "Object " << objectName << " does not have an ObjectEpochCache entry";
      if (dots != 2) cout << " (please add catalog and schema name)";
      cout << endl;
    }
  }  // end of for that iterates over arguments
}

long computeSegmentSizeForRMS() {
  long maxSegSize = STATS_MAX_SEG_SIZE;
  char *envSegSize = getenv("RMS_SHARED_SEG_SIZE_MB");
  if (envSegSize) {
    maxSegSize = (long)str_atoi(envSegSize, str_len(envSegSize));
    if (maxSegSize < 32) maxSegSize = 32;
    maxSegSize *= 1024 * 1024;
  }
#ifdef USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHE
  maxSegSize += SharedSegment::computeSegmentSizeForSharedCache();
#endif
  return maxSegSize;
}

ObjectEpochCache *StatsGlobals::getObjectEpochCache() {
  long maxEpochCacheSegSize = EPOCH_CACHE_SEG_SIZE;
  if (epochCache_ == NULL) {
    char *envSegSize = getenv("EPOCH_CACHE_SEG_SIZE");
    if (envSegSize) {
      maxEpochCacheSegSize = (long)str_atoi(envSegSize, str_len(envSegSize));
      maxEpochCacheSegSize *= 1024 * 1024;
      if (maxEpochCacheSegSize < MIN_EPOCH_CACHE_SEG_SIZE)
        maxEpochCacheSegSize = MIN_EPOCH_CACHE_SEG_SIZE;
      else if (maxEpochCacheSegSize > MAX_EPOCH_CACHE_SEG_SIZE)
        maxEpochCacheSegSize = MAX_EPOCH_CACHE_SEG_SIZE;
    }

    short semRetCode = getRTSSemaphore();
    NAHeap *epochHeap = new (getStatsHeap()) NAHeap("Epoch Heap", (NAHeap *)getStatsHeap(), 8192,
                                                    maxEpochCacheSegSize);  // Upper limit of 2MB
    epochHeap->setThreadSafe();
    epochCache_ = new (epochHeap) ObjectEpochCache(this, epochHeap);
    if (semRetCode == 1)  // means that RTSSemaphore is locked here and by the caller of this function.
      releaseRTSSemaphore();
  }
  return epochCache_;
}

int StatsGlobals::calLimitLevel() {
  char *limitLevel = getenv("LIMIT_LEVEL");
  if (limitLevel) return atoi(limitLevel);
  int level = Statement::NO_LIMIT;
  const size_t level1Percent = 60;
  const size_t level2Percent = 80;
  NAHeap *statsHeap = getStatsHeap();
  if (statsHeap->getAllocSize() * 100 > statsHeap->getTotalSize() * level2Percent)
    level = Statement::LIMIT_RMS_USAGE_THOROUGHLY;
  else if (statsHeap->getAllocSize() * 100 > statsHeap->getTotalSize() * level1Percent)
    level = Statement::LIMIT_DETAILED_STATISTICS;
  return level;
}

ExRMSStats *StatsGlobals::getRMSStats(NAHeap *heap) {
  ExRMSStats *rmsStats = new (heap) ExRMSStats(heap);
  rmsStats->copyContents(getRMSStats());
  NAHeap *statsHeap = getStatsHeap();
  NAHeap *epochHeap = getEpochHeap();
  rmsStats->setGlobalStatsHeapAlloc(statsHeap->getTotalSize());
  rmsStats->setGlobalStatsHeapUsed(statsHeap->getAllocSize());
  rmsStats->setStatsHeapWaterMark(statsHeap->getHighWaterMark());
  rmsStats->setNoOfStmtStats(getStmtStatsList()->numEntries());
  rmsStats->setSemPid(getSemPid());
  rmsStats->setNumQueryInvKeys(getRecentSikeys()->entries());
  if (epochHeap != NULL) {
    rmsStats->setEpochHeapAlloc(epochHeap->getTotalSize());
    rmsStats->setEpochHeapUsed(epochHeap->getAllocSize());
    rmsStats->setEpochHeapWaterMark(epochHeap->getHighWaterMark());
    rmsStats->setNoOfEpochEntries(epochCache_->entries());
  } else {
    rmsStats->setEpochHeapAlloc(0);
    rmsStats->setEpochHeapUsed(0);
    rmsStats->setEpochHeapWaterMark(0);
    rmsStats->setNoOfEpochEntries(0);
  }

  return rmsStats;
}

NAHeap *StatsGlobals::getEpochHeap() {
  NAHeap *epochHeap = NULL;
  if (epochCache_ != NULL) epochHeap = epochCache_->epochHeap();
  return epochHeap;
}

void StatsGlobals::populateQueryInvalidateStats(ExStatisticsArea *statsArea) {
  short semRetCode = getRTSSemaphore();
  NAMemory *heap = statsArea->getHeap();
  RecentSikey *recentSikey = NULL;
  recentSikeys_->position();
  ExQryInvalidStats *qiStats = NULL;

  while (NULL != (recentSikey = (RecentSikey *)recentSikeys_->getNext())) {
    qiStats = new (heap) ExQryInvalidStats(heap);
    ComQIActionType siKeyType = ComQIActionTypeLiteralToEnum(recentSikey->s_.operation);
    if (siKeyType == COM_QI_OBJECT_REDEF || siKeyType == COM_QI_STATS_UPDATED) {
      qiStats->setObjectUid(recentSikey->s_.ddlObjectUID);
    } else {
      qiStats->setSubjectHash(recentSikey->s_.revokeKey.subject);
      qiStats->setObjectHash(recentSikey->s_.revokeKey.object);
    }
    qiStats->setCpu(cpu_);
    qiStats->setRevokeTime(recentSikey->revokeTimestamp_);
    qiStats->setOperation(recentSikey->s_.operation);
    statsArea->insert(qiStats);
  }

  if (semRetCode == 1) releaseRTSSemaphore();

  return;
}

void StatsGlobals::unlockIfHeapMutexLocked() {
  int ret = statsHeap_.tryLock();
  if (ret != 0) {
    // tryLock failed, heap locked by other process
    char msg[256];
    snprintf(msg, sizeof(msg), "StatsGlobals::statsHeap_ is locked by noexisting process, destory and reinitialize it");
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
    statsHeap_.destoryAndInit();
    statsHeap_.tryLock();
  }
  statsHeap_.unLock();
}

int StatsGlobals::forceReleaseAndGetStatsSemaphore(Long &semId, pid_t pid) {
  if (semPid_ != -1) return 0;

  clock_gettime(CLOCK_REALTIME, &releasingTimestamp_);
  isBeingUpdated_ = FALSE;

  int error = sem_post((sem_t *)semId);
  ex_assert(error == 0, "sem_post failed in forceReleaseAndGetStatsSemaphore");

  error = getStatsSemaphore(semId, pid);

  return error;
}

LockHolder::LockHolder(CliGlobals *cliGlobals) {
  nid_ = cliGlobals->myAncestorNid();
  pid_ = cliGlobals->myAncestorPid();
}

RTSSemaphoreController::RTSSemaphoreController() : semRetCode_(0) {
  LOGTRACE(CAT_SQL_LOCK, "Get RTS semaphore");
  semRetCode_ = getRTSSemaphore();
  LOGTRACE(CAT_SQL_LOCK, "Get RTS semaphore returned %d", semRetCode_);
  ex_assert(checkIfRTSSemaphoreLocked(), "Not proteced by RTS semaphore");
}

RTSSemaphoreController::~RTSSemaphoreController() {
  if (semRetCode_ == 1) {
    LOGTRACE(CAT_SQL_LOCK, "Release RTS semaphore");
    releaseRTSSemaphore();
  } else {
    LOGTRACE(CAT_SQL_LOCK, "Not acquired the RTS semaphore here, skip releasing.");
  }
}

bool ObjectLockEntry::match(const char *objectName, ComObjectType objectType) const {
  if (!inUse_ || objectType != objectType_) {
    return false;
  }
  return !strncmp(objectName, objectName_, OBJECT_LOCK_NAME_LEN);
}

ObjectLockEntry *ObjectLockArray::findLock(const char *objectName, ComObjectType objectType) {
  ObjectLockEntry *lock = NULL;
  for (unsigned short i = 0; i < entries_; i++) {
    lock = &locks_[i];
    if (lock->match(objectName, objectType)) {
      return lock;
    }
  }
  return NULL;
}

ObjectLockEntry *ObjectLockArray::addLock(const char *objectName, ComObjectType objectType, const LockHolder &holder) {
  unsigned short i = 0;
  ObjectLockEntry *lock = NULL;
  i = entries_;
  if (i >= maxEntries()) {
    deepRecycle();
  }
  i = entries_;
  if (i >= maxEntries()) {
    LOGERROR(CAT_SQL_LOCK, "Cannot add lock %s %s due to lock entry exhaustion", comObjectTypeName(objectType),
             objectName);
    return NULL;
  }
  lock = &locks_[i];
  lock->reset();
  lock->acquire(holder);
  lock->setObjectType(objectType);
  lock->setObjectName(objectName);
  entries_++;
  LOGDEBUG(CAT_SQL_LOCK, "Added lock entry %u (%p) of %s %s for %d %d", i, lock, comObjectTypeName(objectType),
           objectName, holder.getNid(), holder.getPid());
  return lock;
}

bool ObjectLockArray::lockedBy(const char *objectName, ComObjectType objectType, const LockHolder &holder) {
  NAMutexScope mutex(mutex_);
  ObjectLockEntry *lock = findLock(objectName, objectType);
  if (lock != NULL && lock->lockedBy(holder)) {
    return true;
  }
  return false;
}

bool ObjectLockArray::hasConflictLock(const char *objectName, ComObjectType objectType, const LockHolder &holder,
                                      LockHolder &conflictHolder) {
  NAMutexScope mutex(mutex_);
  ObjectLockEntry *lock = findLock(objectName, objectType);
  if (lock != NULL && lock->locked() && lock->holder() != holder) {
    conflictHolder = lock->holder();
    LOGDEBUG(CAT_SQL_LOCK, "Found a conflict lock for %s %s (holder %d %d) currently held by %d %d",
             comObjectTypeName(objectType), objectName, holder.getNid(), holder.getPid(), conflictHolder.getNid(),
             conflictHolder.getPid());
    return true;
  }
  return false;
}

bool ObjectLockArray::acquireLock(const char *objectName, ComObjectType objectType, const LockHolder &holder,
                                  LockHolder &conflictHolder, bool &outOfEntry) {
  NAMutexScope mutex(mutex_);
  ObjectLockEntry *lock = NULL;
  outOfEntry = false;
  lock = findLock(objectName, objectType);
  if (lock != NULL && lock->locked() && !lock->lockedBy(holder)) {
    conflictHolder = lock->holder();
    LOGWARN(CAT_SQL_LOCK, "Found a conflict lock for %s %s (holder %d %d) currently held by %d %d",
            comObjectTypeName(objectType), objectName, holder.getNid(), holder.getPid(), conflictHolder.getNid(),
            conflictHolder.getPid());
    return false;
  }
  if (lock == NULL) {
    lock = addLock(objectName, objectType, holder);
  }
  if (lock == NULL) {
    LOGERROR(CAT_SQL_LOCK, "Acquire lock %s %s (holder %d %d) failed due to out of lock entry",
             comObjectTypeName(objectType), objectName, holder.getNid(), holder.getPid());
    outOfEntry = true;
    return false;
  }
  return true;
}

// The deepRecyle function will try to copy/move all lock entries in
// use to the beginning of the lock array so that we can recyle all
// unused lock entries.
//
// Caller of this function need to make sure exclusive accessing
// during the process.
void ObjectLockArray::deepRecycle() {
  LOGINFO(CAT_SQL_LOCK, "Deep recyle object locks, current entries: %d", entries_);
  unsigned short i = 0;
  unsigned short j = entries_ - 1;
  while (i < j) {
    if (locks_[i].inUse()) {
      i++;
      continue;
    }
    if (locks_[j].inUse()) {
      locks_[i] = locks_[j];
      locks_[j].reset();
    }
    j--;
  }
  unsigned short oldEntries = entries_;
  if (locks_[j].inUse()) {
    entries_ = j + 1;
  } else {
    entries_ = j;
  }
  LOGINFO(CAT_SQL_LOCK, "Deep recyle recollected %d lock entries, current entries: %d", oldEntries - entries_,
          entries_);
}

// NOTE: This function will try to recycle the locks freed, but only
// to the maximum lock entry index in use, so that we do not need to
// copy/move the locks. For example, if lock entries 0 1 2 are current
// in use, and then lock 0 and 2 are freed, we will only recycle lock
// entry 2 and not recycle lock entry 0 since lock entry 1 is still in
// use.
void ObjectLockArray::removeLocksHeldBy(const LockHolder &holder) {
  ObjectLockEntry *lock = NULL;
  int maxUsedIndex = -1;
  unsigned short removedEntries = 0;

  NAMutexScope mutex(mutex_);
  for (unsigned short i = 0; i < entries_; i++) {
    lock = &locks_[i];
    if (lock->holder() == holder) {
      lock->reset();
      removedEntries++;
    } else if (lock->inUse()) {
      maxUsedIndex = i;
    }
  }
  unsigned short recycledEntries = entries_ - maxUsedIndex - 1;
  entries_ = maxUsedIndex + 1;
  // Save the value for later use in logging
  unsigned short entries = entries_;
  LOGDEBUG(CAT_SQL_LOCK, "Removed %d locks held by %d %d, recycled %d entries, %d entries left", removedEntries,
           holder.getNid(), holder.getPid(), recycledEntries, entries);
}

void ObjectLockArray::releaseLock(const char *objectName, ComObjectType objectType, const LockHolder &holder) {
  ObjectLockEntry *lock = NULL;

  NAMutexScope mutex(mutex_);
  lock = findLock(objectName, objectType);
  if (lock != NULL) {
    lock->release(holder);
  }
  // Do a shallow recycle
  unsigned short oldEntries = entries_;
  while (entries_ > 0 && !locks_[entries_ - 1].inUse()) {
    entries_--;
  }
  LOGDEBUG(CAT_SQL_LOCK, "Recycled %d entries, %d entries left", oldEntries - entries_, entries_);
}

// This function is only used for DDL locks, caller must held the RTS
// semaphore, which will block others from changing DDL lock arrays in
// all processes
void ObjectLockArray::copyLocks(const ObjectLockArray &from) {
  ex_assert(checkIfRTSSemaphoreLocked(), "Not proteced by RTS semaphore");
  ex_assert(from.maxEntries() == maxEntries(), "Object lock array size mismatch");
  for (unsigned short i = 0; i < from.entries(); i++) {
    const ObjectLockEntry *lock = &from.locks()[i];
    LOGDEBUG(CAT_SQL_LOCK, "Copy lock entry %s %s (holder %d %d)", comObjectTypeName(lock->objectType()),
             lock->objectName(), lock->holder().getNid(), lock->holder().getPid());
    locks_[i] = *lock;
  }
  entries_ = from.entries();
  LOGDEBUG(CAT_SQL_LOCK, "Copied %d entries", entries_);
}

ObjectLockArray *ObjectLockController::dmlLockArray(pid_t pid) {
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  ObjectLockArray *objectLockArray = statsGlobals->dmlLockArray(pid);
  ex_assert(objectLockArray, "DML ObjectLockArray not properly initialized");
  return objectLockArray;
}

ObjectLockArray *ObjectLockController::ddlLockArray(pid_t pid) {
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  ObjectLockArray *objectLockArray = statsGlobals->ddlLockArray(pid);
  ex_assert(objectLockArray, "DDL ObjectLockArray not properly initialized");
  return objectLockArray;
}

ObjectLockArray *ObjectLockController::dmlLockArray() {
  pid_t pid = GetCliGlobals()->myAncestorPid();
  return dmlLockArray(pid);
}

ObjectLockArray *ObjectLockController::ddlLockArray() {
  pid_t pid = GetCliGlobals()->myAncestorPid();
  return ddlLockArray(pid);
}

bool ObjectLockController::ddlLockedByMe() {
  LockHolder me(GetCliGlobals());
  return ddlLockArray()->lockedBy(objectName_, objectType_, me);
}

bool ObjectLockController::hasConflictDDL(const LockHolder &holder, LockHolder &conflictHolder /* OUT */) {
  return ddlLockArray()->hasConflictLock(objectName_, objectType_, holder, conflictHolder);
}

bool ObjectLockController::hasConflictDML(const LockHolder &holder) {
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  RTSSemaphoreController sem;
  pid_t pid = 0;
  pid_t maxPid = statsGlobals->maxPid();
  for (pid = 0; pid <= maxPid; pid++) {
    ObjectLockArray *dmlLockArray = statsGlobals->dmlLockArray(pid);
    if (dmlLockArray != NULL && dmlLockArray->maxEntries() > 0 &&
        dmlLockArray->hasConflictLock(objectName_, objectType_, holder)) {
      return true;
    }
  }
  return false;
}

ObjectLockController::Result ObjectLockController::lockDML(LockHolder &conflictHolder) {
  LOGDEBUG(CAT_SQL_LOCK, "DML lock %s %s", comObjectTypeName(objectType_), objectName_);
  LockHolder me(GetCliGlobals());
  ObjectLockEntry *dmlLock = NULL;

  {
    NAMutexScope mutex(dmlLockArray()->mutex());
    dmlLock = dmlLockArray()->findLock(objectName_, objectType_);
    if (dmlLock != NULL) {
      ex_assert(dmlLock->lockedBy(me), "DML locked not held by me");
      LOGDEBUG(CAT_SQL_LOCK, "DML lock %s %s is already held by me (%d %d)", comObjectTypeName(objectType_),
               objectName_, me.getNid(), me.getPid());
      return Result::LOCK_OK;
    }

    dmlLock = dmlLockArray()->addLock(objectName_, objectType_, me);
    if (dmlLock == NULL) {
      LOGERROR(CAT_SQL_LOCK, "DML lock %s %s failed due to out of lock entry", comObjectTypeName(objectType_),
               objectName_);
      return Result::LOCK_OUT_OF_ENTRY;
    }
  }

  if (hasConflictDDL(me, conflictHolder)) {
    LOGDEBUG(CAT_SQL_LOCK, "DML lock %s %s failed due to conflict DDL operations", comObjectTypeName(objectType_),
             objectName_);
    dmlLock->release(me);
    return Result::LOCK_CONFLICT_DDL;
  }
  return Result::LOCK_OK;
}

ObjectLockController::Result ObjectLockController::lockDDL(const LockHolder &holder,
                                                           LockHolder &conflictHolder /* OUT */) {
  LOGDEBUG(CAT_SQL_LOCK, "DDL lock %s %s (holder %d %d)", comObjectTypeName(objectType_), objectName_, holder.getNid(),
           holder.getPid());

  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  RTSSemaphoreController sem;
  int nid = cliGlobals->myNodeNumber();
  pid_t pid = 0;
  pid_t maxPid = statsGlobals->maxPid();
  for (pid = 0; pid <= maxPid; pid++) {
    ObjectLockArray *ddlLockArray = statsGlobals->ddlLockArray(pid);
    if (ddlLockArray == NULL || ddlLockArray->maxEntries() == 0) {
      continue;
    }
    LOGDEBUG(CAT_SQL_LOCK, "Acquire DDL lock %s %s in process %d", comObjectTypeName(objectType_), objectName_, pid);
    bool outOfEntry = false;
    if (!ddlLockArray->acquireLock(objectName_, objectType_, holder, conflictHolder, outOfEntry)) {
      if (outOfEntry) {
        LOGWARN(CAT_SQL_LOCK, "Acquire DDL lock %s %s failed in process %d due to out of lock entry",
                comObjectTypeName(objectType_), objectName_, pid);
        return Result::LOCK_OUT_OF_ENTRY;
      }
      LOGWARN(CAT_SQL_LOCK, "Acquire DDL lock %s %s failed in process %d due to conflict with other DDL operations",
              comObjectTypeName(objectType_), objectName_, pid);
      return Result::LOCK_CONFLICT_DDL;
    }
    ObjectLockArray *dmlLockArray = statsGlobals->dmlLockArray(pid);
    ex_assert(dmlLockArray, "DML lock array is NULL");
    if (dmlLockArray->hasConflictLock(objectName_, objectType_, holder, conflictHolder)) {
      LOGWARN(CAT_SQL_LOCK, "DDL lock %s %s conflict with DML operations in process %d", comObjectTypeName(objectType_),
              objectName_, pid);
      return Result::LOCK_CONFLICT_DML;
    }
  }
  return Result::LOCK_OK;
}

void ObjectLockController::unlockDDL(const LockHolder &holder) {
  LOGDEBUG(CAT_SQL_LOCK, "DDL unlock %s %s", comObjectTypeName(objectType_), objectName_);
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  RTSSemaphoreController sem;
  pid_t pid = 0;
  pid_t maxPid = statsGlobals->maxPid();
  for (pid = 0; pid <= maxPid; pid++) {
    ObjectLockArray *ddlLockArray = statsGlobals->ddlLockArray(pid);
    if (ddlLockArray == NULL || ddlLockArray->maxEntries() == 0) {
      continue;
    }
    LOGDEBUG(CAT_SQL_LOCK, "Release DDL lock %s %s in process %d", comObjectTypeName(objectType_), objectName_, pid);
    ddlLockArray->releaseLock(objectName_, objectType_, holder);
  }
}

void ObjectLockController::releaseDDLLocksHeldBy(const LockHolder &holder) {
  LOGDEBUG(CAT_SQL_LOCK, "Release all DDL locks held by %d %d", holder.getNid(), holder.getPid());
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  RTSSemaphoreController sem;
  pid_t pid = 0;
// Hot spot optimization of SSMP process function in arm architecture
#ifdef __aarch64__
  register pid_t maxPid = statsGlobals->maxPid();
#else
  pid_t maxPid = statsGlobals->maxPid();
#endif
  for (pid = 0; pid <= maxPid; pid++) {
    ObjectLockArray *ddlLockArray = statsGlobals->ddlLockArray(pid);
    if (ddlLockArray == NULL || ddlLockArray->maxEntries() == 0) {
      continue;
    }
    LOGDEBUG(CAT_SQL_LOCK, "Remove all DDL locks by holder %d %d in process %d", holder.getNid(), holder.getPid(), pid);
    ddlLockArray->removeLocksHeldBy(holder);
  }
}

void ObjectLockController::releaseDMLLocksHeldBy(const LockHolder &holder) {
  LOGDEBUG(CAT_SQL_LOCK, "Release all DML locks held by %d %d", holder.getNid(), holder.getPid());
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  int nid = cliGlobals->myNodeNumber();
  if (nid != holder.getNid()) {
    return;
  }
  RTSSemaphoreController sem;
  ObjectLockArray *dmlLockArray = statsGlobals->dmlLockArray(holder.getPid());
  if (dmlLockArray != NULL && dmlLockArray->maxEntries() > 0) {
    LOGDEBUG(CAT_SQL_LOCK, "Remove all DML locks in process %d", holder.getPid());
    dmlLockArray->removeAll();
  }
}

void ObjectLockController::removeLocksHeldByProcess(pid_t pid) {
  int nid = GetCliGlobals()->myNodeNumber();
  LockHolder holder(nid, pid);
  releaseLocksHeldBy(holder);
}

void ObjectLockController::releaseLocksHeldBy(const LockHolder &holder) {
  releaseDMLLocksHeldBy(holder);
  releaseDDLLocksHeldBy(holder);
}

void ObjectLockArray::appendToStats(ExStatisticsArea *statsArea, short cpu, pid_t pid, const char *objectName,
                                    ComObjectType objectType, bool ddlLock) {
  if (maxEntries_ == 0 || entries_ == 0) {
    return;
  }
  NAMemory *heap = statsArea->getHeap();

  // Add specific lock
  if (objectName != NULL) {
    const ObjectLockEntry *lock = findLock(objectName, objectType);
    if (lock != NULL && lock->locked()) {
      LockHolder lockHolder = lock->holder();
      ExObjectLockStats *lockStats =
          new (heap) ExObjectLockStats(heap, cpu, objectName, objectType, pid, lockHolder.getNid(), lockHolder.getPid(),
                                       !ddlLock /* dmlLocked */, ddlLock /* ddlLocked */);
      statsArea->insert(lockStats);
    }
    return;
  }

  // Add all locks
  for (int i = 0; i < entries_; i++) {
    const ObjectLockEntry *lock = &locks_[i];
    if (lock->locked()) {
      LockHolder lockHolder = lock->holder();
      ExObjectLockStats *lockStats =
          new (heap) ExObjectLockStats(heap, cpu, lock->objectName(), lock->objectType(), pid, lockHolder.getNid(),
                                       lockHolder.getPid(), !ddlLock /* dmlLocked */, ddlLock /* ddlLocked */);
      statsArea->insert(lockStats);
    }
  }
}

void ObjectLockController::appendToStats(ExStatisticsArea *statsArea, const char *objectName,
                                         ComObjectType objectType) {
  LOGDEBUG(CAT_SQL_LOCK, "Append lock stats of %s %s", comObjectTypeName(objectType), objectName);
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  short cpu = statsGlobals->getCpu();
  RTSSemaphoreController sem;
  pid_t pid = 0;
  pid_t maxPid = statsGlobals->maxPid();
  NAMemory *heap = statsArea->getHeap();
  for (pid = 0; pid <= maxPid; pid++) {
    ObjectLockArray *dmlLockArray = statsGlobals->dmlLockArray(pid);
    if (dmlLockArray != NULL) {
      LOGDEBUG(CAT_SQL_LOCK, "Add DML lock stats in process %d", pid);
      dmlLockArray->appendToStats(statsArea, cpu, pid, objectName, objectType, false /* ddlLock */);
    }
    ObjectLockArray *ddlLockArray = statsGlobals->ddlLockArray(pid);
    if (ddlLockArray != NULL) {
      LOGDEBUG(CAT_SQL_LOCK, "Add DDL lock stats in process %d", pid);
      ddlLockArray->appendToStats(statsArea, cpu, pid, objectName, objectType, true /* ddlLock*/);
    }
  }
}

DDLScope::DDLScope() : cmpContext_(NULL), myProcessStats_(NULL) { startDDLStmt(); }

DDLScope::~DDLScope() { endDDLStmt(); }

void DDLScope::startDDLStmt() {
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  if (!cliGlobals->isMasterProcess()) {
    return;
  }
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  int pid = cliGlobals->myPin();
  myProcessStats_ = statsGlobals->checkProcess(pid);
  ex_assert(myProcessStats_, "ProcessStats not properly initialized");

  cmpContext_ = CmpCommon::context();
  UInt32 level = myProcessStats_->ddlStmtLevel();
  level++;
  myProcessStats_->setDDLStmtLevel(level);
  LOGDEBUG(CAT_SQL_LOCK, "Start DDL stmt: level = %d", level);
}

void DDLScope::endDDLStmt() {
  CliGlobals *cliGlobals = GetCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  if (!cliGlobals->isMasterProcess()) {
    return;
  }
  UInt32 level = myProcessStats_->ddlStmtLevel();
  LOGDEBUG(CAT_SQL_LOCK, "End DDL Stmt: level = %d", level);

  bool xnInProgress = false;
  if (cliGlobals->currContext() && cliGlobals->currContext()->getTransaction() &&
      cliGlobals->currContext()->getTransaction()->xnInProgress()) {
    xnInProgress = true;
  }
  // NOTE: Some DDL statements (e.g. populate index) do not clear the
  // ddl object list when finished.
  if (level == 1 && !xnInProgress && cmpContext_ != NULL && cmpContext_->ddlObjsList().entries() > 0) {
    LOGWARN(CAT_SQL_LOCK, "  Clear residual DDL object list");
    cmpContext_->ddlObjsList().clear();
  }

  level--;
  myProcessStats_->setDDLStmtLevel(level);
}
