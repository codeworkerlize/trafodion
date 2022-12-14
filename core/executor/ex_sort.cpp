
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_sort.C
 * Description:  class to sort data using fastsort.
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

#include "ex_sort.h"

#include "ExpError.h"
#include "SortUtil.h"
#include "SortUtilCfg.h"
#include "comexe/ComTdb.h"
#include "common/NAMemory.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ExStats.h"
#include "executor/ex_expr.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "executor/sql_buffer_size.h"

#define ONE_MEG 1048576  // 1024 * 1024

void ReleaseTupp(void *td);
void CaptureTupp(void *td);

// Dont exceed more than 11 characters
const char *ExSortTcb::SortPhaseStr[] = {"SORT_PREP", "SORT_SEND", "SORT_RECV", "SORT_MERGE"};

NABoolean ExSortTcb::needStatsEntry() {
  ComTdb::CollectStatsType statsType = getGlobals()->getStatsArea()->getCollectStatsType();

  // stats are collected for ALL or OPERATOR options.
  if (statsType == ComTdb::ALL_STATS || statsType == ComTdb::OPERATOR_STATS)
    return TRUE;
  else
    return FALSE;
}

ExOperStats *ExSortTcb::doAllocateStatsEntry(CollHeap *heap, ComTdb *tdb) {
  ExBMOStats *stat;

  ComTdb::CollectStatsType statsType = getGlobals()->getStatsArea()->getCollectStatsType();
  if (statsType == ComTdb::ACCUMULATED_STATS) {
    // accum stats
    ex_assert(getGlobals()->getStatsArea()->numEntries() == 1, "Must have one and only one entry for accum stats case");
    getGlobals()->getStatsArea()->position();
    setStatsEntry(getGlobals()->getStatsArea()->getNext());
    return NULL;
  } else if (statsType == ComTdb::OPERATOR_STATS)
    stat = new (heap) ExBMOStats(heap, this, tdb);
  else {
    stat = (ExBMOStats *)new (heap) ExSortStats(heap, this, tdb);
    sortStats_ = (ExSortStats *)stat;
  }
  ExSortTdb *sortTdb = (ExSortTdb *)getTdb();
  sortUtil_->setBMOStats(stat);
  bmoStats_ = stat;
  return stat;
}

void ExSortTcb::setupPoolBuffers(ex_queue_entry *pentry_down) {
  // In case of prepare once and execute many, if sort overflowed,
  // sortSendPool_ is deallocated and receivePool_ is newly allocated
  // when switching from sortSend to sortReceive. Note that if sort
  // did not overflow, sortSendPool_ and receivePool_ are same.
  // Here we need to reset these pools to start again.
  if ((sortSendPool_ == NULL) && (receivePool_ != NULL) &&  // pool reference
      (sortPool_ != NULL))                                  // actual pool
  {
    // receivePool_ always allocated outside of quota system.
    // so no need to adjust quota system especially when sortSendPool_
    // and receivePool_ are not the same.
    NADELETE(receivePool_, ExSortBufferPool, sortSpace_);
    receivePool_ = NULL;

    NADELETE(sortPool_, sql_buffer_pool, sortSpace_);
    sortPool_ = NULL;

    // Also delete and reallocate the space object from which the sortPool_ is
    // allocated. This will really release the memory.
    NADELETE(sortSpace_, Space, sortHeap_);
    sortSpace_ = NULL;
  }

  // if any of these pools is already allocated, most likely
  // from a prepare once execute many scenario, then no need
  // to reallocate the pool again. Just return.
  // if topNSortPool_, reinitialize the pool since it uses
  // ExSimpleSqlBuffer that has some of the tuple datapointers readjusted
  // during sortReceive( not an issue with SqlBufferPool since tuple is
  // initialized each time before use)
  if (topNSortPool_) topNSortPool_->reinitializeTuples();
  if (partialSortPool_ || topNSortPool_ || sortPool_) return;

  if (!sortSpace_) {
    sortSpace_ = new (sortHeap_) Space(Space::EXECUTOR_SPACE, TRUE, (char *)"Sort Space setupPoolBuffers");
    sortSpace_->setParent(sortHeap_);
  }

  // Allocate the buffer pool.
  // Note that when memoryQuota system is enabled, we initialize the
  // pool with atleast 2 buffer. This is to accomodate sort to progress
  // even under extremely loaded environment(memory pressure)irrespective
  // of memory quota system. The buffer size is calculated taking into
  // account the estimate number of rows by the compiler and limited by
  // maximum of GEN_SORT_MAX_BUFFER_SIZE. The memory quota system will
  // come into force for additional buffers following this initial buffer.
  initialNumOfPoolBuffers_ = sortTdb().numBuffers_;
  int numSortBuffs = 0;

  // need separate pools for sorting and saving result rows
  if (sortTdb().partialSort()) {
    // give result pool and sort pool each half of the space
    numSortBuffs = initialNumOfPoolBuffers_ = (initialNumOfPoolBuffers_ + 1) / 2;
    if (numSortBuffs < 2) numSortBuffs = 2;  // initialize the pool with atleast 2 buffers.
    if (initialNumOfPoolBuffers_ < 2) initialNumOfPoolBuffers_ = 2;
  }

  // partial sort uses two pools. one partialSortPool_ and sortPool_
  // partialSortPool_ will be used for receiving the sorted records.
  if (numSortBuffs > 0) {
    partialSortPool_ = new (sortSpace_) sql_buffer_pool(numSortBuffs, sortTdb().bufferSize_, sortSpace_);
  }

  // setup sortSendPool_ reference handle. If TopN, topNSortPool_ will be allocated
  // from ExSimpleSQLBuffer based on numBuffs. If not TopN, sortPool_ will
  // be allocated from sql_buffer_pool based on numBuffs.
  // sortSendPool_ reference handle will either point to topNSortPool or
  // sortPool_.
  if ((pentry_down->downState.request == ex_queue::GET_N) && (pentry_down->downState.requestValue > 0) &&
      (sortTdb().topNSortEnabled()) && (pentry_down->downState.requestValue <= sortTdb().getTopNThreshold())) {
    topNSortPool_ =
        new (sortSpace_) ExSimpleSQLBuffer(pentry_down->downState.requestValue + 1, sortTdb().sortRecLen_, sortSpace_);

    sortSendPool_ =
        new (sortSpace_) ExSortBufferPool((void *)topNSortPool_, ExSortBufferPool::SIMPLE_BUFFER_TYPE, bmoStats_);
  } else {
    sortPool_ = new (sortSpace_) sql_buffer_pool(initialNumOfPoolBuffers_, sortTdb().bufferSize_, sortSpace_);

    sortSendPool_ = new (sortSpace_) ExSortBufferPool((void *)sortPool_, ExSortBufferPool::SQL_BUFFER_TYPE, bmoStats_);
  }

  // setup the receive pool. Receive pool is the same as sortPool for all cases except
  // for partial sort.
  if (sortTdb().partialSort()) {
    receivePool_ = new (sortSpace_) ExSortBufferPool(partialSortPool_, ExSortBufferPool::SQL_BUFFER_TYPE, bmoStats_);
  } else {
    // Assume sort does not overflow to being with.
    // In this case, receivePool_ and sortSendpool_ are same.
    // if overflow occured ( no overflow in topN case) then
    // sortSendpool_( and actual sortPool_)is deleted and receivePool_
    // is allocated new.
    receivePool_ = sortSendPool_;
  }

  // CIF defrag option only if NOT topNSortPool_
  defragTd_ = NULL;
  if (considerBufferDefrag() && (topNSortPool_ == NULL)) {
    defragTd_ = sortSendPool_->addDefragTuppDescriptor(sortTdb().sortRecLen_);
  }

  if (bmoStats_) {
    if (topNSortPool_) bmoStats_->setTopN(pentry_down->downState.requestValue);
  }
}

// This method is called only if sort overflowed and transitioning
// from sortSend to sortReceive. SortPool_ is deallocated and space
// object is deallocated and reallocated minimum to reuse memory (quota)
// for sort Receive.
void ExSortTcb::deleteAndReallocateSortPool() {
  // delete reference to sortPool_
  ex_assert(sortSendPool_ == receivePool_, "sortSendPool_ != receivePool_");

  // initialNumOfPoolBuffers_ is allocated outside of quota system.
  sortUtil_->returnConsumedMemoryQuota((sortSendPool_->get_number_of_buffers() - initialNumOfPoolBuffers_) *
                                       sortTdb().bufferSize_);

  NADELETE(sortSendPool_, ExSortBufferPool, sortSpace_);
  sortSendPool_ = NULL;
  receivePool_ = NULL;

  // delete actual pool.
  // if we are here, sortPool_ must be valid since we should not
  // reach here if topNSort or partial sort.
  NADELETE(sortPool_, sql_buffer_pool, sortSpace_);
  sortPool_ = NULL;

  // Also delete and reallocate the space object from which the pool was
  // allocated. This will really release the memory.
  NADELETE(sortSpace_, Space, sortHeap_);
  sortSpace_ = new (sortHeap_) Space(Space::EXECUTOR_SPACE, TRUE, (char *)"Sort Space reallocated");
  sortSpace_->setParent(sortHeap_);

  // now allocate a pool and assign it to receivePool_ handle.
  // Allocated outside of memory quota.
  sortPool_ = new (sortSpace_) sql_buffer_pool(initialNumOfPoolBuffers_, sortTdb().bufferSize_, sortSpace_);

  receivePool_ = new (sortSpace_) ExSortBufferPool((void *)sortPool_, ExSortBufferPool::SQL_BUFFER_TYPE, bmoStats_);

  if (bmoStats_) bmoStats_->setSpaceBufferCount(initialNumOfPoolBuffers_);
}

//
// Build a sort tcb
//
ex_tcb *ExSortTdb::build(ex_globals *glob) {
  // first build the child
  ex_tcb *child_tcb = tdbChild_->build(glob);

  ExSortTcb *sort_tcb = NULL;

  if (sortFromTop())
    sort_tcb = new (glob->getSpace()) ExSortFromTopTcb(*this, *child_tcb, glob);
  else
    sort_tcb = new (glob->getSpace()) ExSortTcb(*this, *child_tcb, glob);

  // add the sort_tcb to the schedule
  sort_tcb->registerSubtasks();

  return (sort_tcb);
}

//
// Constructor for sort_tcb
//
ExSortTcb::ExSortTcb(const ExSortTdb &sort_tdb,
                     const ex_tcb &child_tcb,  // child queue pair
                     ex_globals *glob)
    : ex_tcb(sort_tdb, 1, glob), partialSortPool_(NULL), setCompareTd_(NULL), sortPartiallyComplete_(FALSE) {
  bmoStats_ = NULL;
  sortStats_ = NULL;
  childTcb_ = &child_tcb;

  // Create heap to be used by sort.
  sortHeap_ = new (getHeap()) NAHeap("Sort Heap", (NAHeap *)getHeap(), 204800);

  // cast sort tdb to non-const
  ExSortTdb *st = (ExSortTdb *)&sort_tdb;

  // get the queue that child use to communicate with me
  qchild_ = child_tcb.getParentQueue();

  // Allocate the queue to communicate with parent
  allocateParentQueues(qparent_);
  // Intialize processedInputs_ to the next request to process
  processedInputs_ = qparent_.down->getTailIndex();
  workAtp_ = allocateAtp(sort_tdb.workCriDesc_, glob->getSpace());
  workAtp_->getTupp(2) = new (glob->getSpace()) tupp_descriptor();

  // buffer pools are allocated from sortSpace_ in SORT_PREP work phase.
  sortSpace_ = NULL;
  topNSortPool_ = NULL;
  sortPool_ = NULL;
  partialSortPool_ = NULL;
  initialNumOfPoolBuffers_ = 0;

  // pool reference handles. Initialized in SORT_PREP phase.
  sortSendPool_ = NULL;
  receivePool_ = NULL;

  *(short *)&sortType_ = 0;

  sortType_.doNotAllocRec_ = 1;
  sortType_.internalSort_ = 1;

  switch (st->sortOptions_->sortType()) {
    case SortOptions::REPLACEMENT_SELECT:
      sortType_.useRSForRunGeneration_ = 1;
      break;

    case SortOptions::QUICKSORT:
      sortType_.useQSForRunGeneration_ = 1;
      break;

    case SortOptions::ITER_HEAP:
      sortType_.useIterHeapForRunGeneration_ = 1;
      break;

    default:
    case SortOptions::ITER_QUICK:
      sortType_.useIterQSForRunGeneration_ = 1;
      break;
  }

  sortUtil_ = new (sortHeap_) SortUtil(sort_tdb.getExplainNodeId());

  sortDiag_ = NULL;

  sortCfg_ = new (sortHeap_) SortUtilConfig(sortHeap_);
  sortCfg_ = new (sortHeap_) SortUtilConfig(sortHeap_);

  sortCfg_->setSortType(sortType_);
  sortCfg_->setScratchThreshold(st->sortOptions_->scratchFreeSpaceThresholdPct());
  sortCfg_->setMaxNumBuffers(sort_tdb.maxNumBuffers_);
  sortCfg_->setRecSize((int)sort_tdb.sortRecLen_);
  sortCfg_->setKeyInfo((int)sort_tdb.sortKeyLen_);
  sortCfg_->setUseBuffered(st->sortOptions_->bufferedWrites());
  sortCfg_->setLogInfoEvent(st->sortOptions_->logDiagnostics());
  sortCfg_->setDisableCmpHintsOverflow(st->sortOptions_->disableCmpHintsOverflow());
  sortCfg_->setMemoryQuotaMB(st->sortOptions_->memoryQuotaMB());
  sortCfg_->setMemoryPressureThreshold(st->sortOptions_->pressureThreshold());
  sortCfg_->setSortMergeBlocksPerBuffer(st->sortOptions_->mergeBufferUnit());
  sortCfg_->setMinimalSortRecs(sort_tdb.minimalSortRecs_);
  sortCfg_->setPartialSort(st->partialSort());
  sortCfg_->setScratchIOBlockSize(st->sortOptions_->scratchIOBlockSize());
  sortCfg_->setScratchIOVectorSize(st->sortOptions_->scratchIOVectorSize());
  sortCfg_->setBmoCitizenshipFactor(st->getBmoCitizenshipFactor());
  sortCfg_->setMemoryContingencyMB(st->getMemoryContingencyMB());
  sortCfg_->setSortMemEstInKBPerNode(st->getSortMemEstInKBPerNode());
  sortCfg_->setEstimateErrorPenalty(st->sortGrowthPercent());
  sortCfg_->setBmoMaxMemThresholdMB(st->sortOptions_->bmoMaxMemThresholdMB());
  sortCfg_->setIntermediateScratchCleanup(st->sortOptions_->intermediateScratchCleanup());
  sortCfg_->setResizeCifRecord(st->sortOptions_->resizeCifRecord());
  sortCfg_->setConsiderBufferDefrag(st->sortOptions_->considerBufferDefrag());
  sortCfg_->setTopNSort(st->topNSortEnabled());

  switch (st->getOverFlowMode()) {
    case SQLCLI_OFM_SSD_TYPE:
      sortCfg_->setScratchOverflowMode(SCRATCH_SSD);
      break;
    case SQLCLI_OFM_MMAP_TYPE:
      sortCfg_->setScratchOverflowMode(SCRATCH_MMAP);
      break;
    default:
    case SQLCLI_OFM_DISK_TYPE:
      sortCfg_->setScratchOverflowMode(SCRATCH_DISK);
      break;
  }

  // This is the max heap_ memory available to Sort for it's own buffers.
  // SortOptions_ max heap size may be zero for pre existing plans, if so, default
  // it to 20MB size.
  if (short maxHeapMB = st->sortOptions_->sortMaxHeapSize()) {
    sortCfg_->setSortMaxMemory(maxHeapMB * ONE_MEG);
  } else {
    sortCfg_->setSortMaxMemory(20 * ONE_MEG);
  }

  // set scratch drive options as suggested by the compiler (who got it
  // from the defaults table)
  const ExScratchFileOptions *sfo = glob->castToExExeStmtGlobals()->getScratchFileOptions();
  if (sfo) {
    sortCfg_->setScratchDirListSpec(sfo->getSpecifiedScratchDirs());
    sortCfg_->setNumDirsSpec(sfo->getNumSpecifiedDirs());
    sortCfg_->setScratchMgmtOption(sfo->getScratchMgmtOption());
    sortCfg_->setScratchMaxOpens(sfo->getScratchMaxOpensSort());
    sortCfg_->setPreallocateExtents(sfo->getScratchPreallocateExtents());
    sortCfg_->setScratchDiskLogging(sfo->getScratchDiskLogging());
  }
  // In the case of ESPs set up number of ESPs and ESP instance information
  int espInstance = 0;
  int numEsps = 0;
  glob->castToExExeStmtGlobals()->getMyNodeLocalInstanceNumber(espInstance, numEsps);
  sortCfg_->setCallingTcb(this);

  sortCfg_->setNumEsps(numEsps);
  sortCfg_->setEspInstance(espInstance);
  sortCfg_->setIpcEnvironment(glob->castToExExeStmtGlobals()->getIpcEnvironment());

  nfDiags_ = NULL;
  sortUtil_->setupComputations(*sortCfg_);

  // fixup sort input expression
  if (sortKeyExpr())
    (void)sortKeyExpr()->fixup(0, getExpressionMode(), this, glob->getSpace(), glob->getDefaultHeap(), FALSE, glob);

  if (sortRecExpr())
    (void)sortRecExpr()->fixup(0, getExpressionMode(), this, glob->getSpace(), glob->getDefaultHeap(), FALSE, glob);
};

ExSortTcb::~ExSortTcb() {
  delete qparent_.up;
  delete qparent_.down;
  freeResources();

  if (sortHeap_) NADELETE(sortHeap_, NAHeap, getHeap());

  if (nfDiags_) nfDiags_->deallocate();
  nfDiags_ = NULL;
};

////////////////////////////////////////////////////////////////////////
// Free Resources
//
void ExSortTcb::freeResources() {
  if (sortUtil_) NADELETE(sortUtil_, SortUtil, sortHeap_);

  if (sortCfg_) NADELETE(sortCfg_, SortUtilConfig, sortHeap_);

  if (partialSortPool_) {
    NADELETE(partialSortPool_, sql_buffer_pool, sortSpace_);
    partialSortPool_ = NULL;
  }
  if (sortPool_) {
    NADELETE(sortPool_, sql_buffer_pool, sortSpace_);
    sortPool_ = NULL;
  }
  if (topNSortPool_) {
    NADELETE(topNSortPool_, ExSimpleSQLBuffer, sortSpace_);
    topNSortPool_ = NULL;
  }

  // sortSendPool_ and receivePool_
  // are ExSortBufferPool class objects.
  if (sortSendPool_) {
    if (sortSendPool_ != receivePool_) {
      NADELETE(sortSendPool_, ExSortBufferPool, sortSpace_);
    }
    sortSendPool_ = NULL;
  }
  if (receivePool_) {
    NADELETE(receivePool_, ExSortBufferPool, sortSpace_);
    receivePool_ = NULL;
  }

  if (sortSpace_) {
    NADELETE(sortSpace_, Space, sortHeap_);
    sortSpace_ = NULL;
  }
};

////////////////////////////////////////////////////////////////////////
// Register subtasks
//
void ExSortTcb::registerSubtasks() {
  ExScheduler *sched = getGlobals()->getScheduler();
  ex_queue_pair pQueue = getParentQueue();
  const ex_queue_pair cQueue = getChild(0)->getParentQueue();
  ex_tcb::registerSubtasks();
  // register events handled by workDown()
  ex_assert(pQueue.down && pQueue.up, "Parent down queue must exist");
  sched->registerInsertSubtask(sWorkDown, this, pQueue.down);
  sched->registerUnblockSubtask(sWorkDown, this, cQueue.down);

  // register the cancel subtask
  sched->registerCancelSubtask(sCancel, this, pQueue.down);

  // register events handled by workUp()
  sched->registerUnblockSubtask(sWorkUp, this, pQueue.up);
  sched->registerInsertSubtask(sWorkUp, this, cQueue.up);

  // Set up the event handler information

  ioEventHandler_ = sched->registerNonQueueSubtask(sWorkUp, this);

  // Set up the event handler information in the sortCfg truct to pass to Sort
  sortCfg_->setEventHandler(ioEventHandler_);

  // the parent queues will be resizable, so register a resize subtask.
  registerResizeSubtasks();
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state *ExSortTcb::allocatePstates(int &numElems,      // inout, desired/actual elements
                                                 int &pstateLength)  // out, length of one element
{
  PstateAllocator<ExSortPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

////////////////////////////////////////////////////////////////////////
// Create a Diags message from the sort error message returned by
// the sort subsystem.
////////////////////////////////////////////////////////////////////////

void ExSortTcb::createSortDiags() {
  ExExeStmtGlobals *exe_glob = getGlobals()->castToExExeStmtGlobals();
  CollHeap *heap = getGlobals()->getDefaultHeap();

  ComDiagsArea *da = exe_glob->getDiagsArea();
  if (!da) {
    da = ComDiagsArea::allocate(heap);
    exe_glob->setGlobDiagsArea(da);
    da->decrRefCount();
  }
  short sorterrorcode = sortUtil_->getSortError();    // contains the sort error code
  short sortSysError = sortUtil_->getSortSysError();  // contains any system level error but may be 0

  short sortErrorDetail =
      sortUtil_->getSortErrorDetail();  // contains any additional sort error returned from lower layers of sort eg
                                        // ScratchSpace or ScratchFile. But maybe 0 as well.
  // Guard against inserting a 0 error and causing an assertion or abend

  if (sorterrorcode == 0) {
    sorterrorcode = -EUnexpectErr;  // Generic sort error.
  }

  char msg[256];
  str_sprintf(msg, "Details: %s", sortUtil_->getSortErrorMsg());
  *da << DgSqlCode(sorterrorcode) << DgInt0(sortSysError) << DgInt1(sortErrorDetail) << DgString0(msg);

  if (sorterrorcode == -EXE_ERROR_INJECTED) {
    *da << DgString0(sortUtil_->getSortErrorMsg()) << DgInt0(0);
  }
  sortDiag_ = da;
  sortDiag_->incrRefCount();
}
////////////////////////////////////////////////////////////////////////
// Create a Diags message indicating FirstN is returning fewer rows
// than request. AQR will kick in.
////////////////////////////////////////////////////////////////////////

// LCOV_EXCL_START
void ExSortTcb::createSortNRetryDiags(int firstNCount, int sortCount) {
  ExExeStmtGlobals *exe_glob = getGlobals()->castToExExeStmtGlobals();
  CollHeap *heap = getGlobals()->getDefaultHeap();
  ComDiagsArea *da = exe_glob->getDiagsArea();
  if (!da) {
    da = ComDiagsArea::allocate(heap);
    exe_glob->setGlobDiagsArea(da);
    da->decrRefCount();
  }
  *da << DgSqlCode(-8967) << DgInt0(sortCount) << DgInt1(firstNCount);
  sortDiag_ = da;
  sortDiag_->incrRefCount();
}
////////////////////////////////////////////////////////////////////////
// processSortError()
//
NABoolean ExSortTcb::processSortError(ex_queue_entry *pentry_down, queue_index parentIndex, queue_index downIndex) {
  ex_queue_entry *up_entry = qparent_.up->getTailEntry();
  up_entry->upState.parentIndex = parentIndex;  // pentry_down->downState.parentIndex;
  up_entry->upState.parentIndex = downIndex;    // qparent_.down->getHeadIndex()
  up_entry->upState.setMatchNo(0);
  up_entry->upState.status = ex_queue::Q_SQLERROR;

  ComDiagsArea *diagsArea = up_entry->getDiagsArea();

  if (diagsArea == NULL)
    diagsArea = ComDiagsArea::allocate(this->getGlobals()->getDefaultHeap());
  else
    diagsArea->incrRefCount();

  if (sortDiag_) diagsArea->mergeAfter(*sortDiag_);

  up_entry->setDiagsArea(diagsArea);

  // insert into parent
  qparent_.up->insert();

  return TRUE;
}

short ExSortTcb::workStatus(short workRC) { return workRC; }

////////////////////////////////////////////////////////////////////////////
// This is where the action is.
////////////////////////////////////////////////////////////////////////////

// -----------------------------------------------------------------------
// Generic work procedure should never be called
// -----------------------------------------------------------------------
short ExSortTcb::work() {
  ex_assert(0, "Should never reach ExSortTcb::work()");
  return WORK_BAD_ERROR;
}
short ExSortTcb::workDown() {
  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  queue_index tail = qparent_.down->getTailIndex();

  for (; (processedInputs_ != tail) && (!qchild_.down->isFull()); processedInputs_++) {
    ex_queue_entry *pentry_down = qparent_.down->getQueueEntry(processedInputs_);
    ExSortPrivateState &pstate = *((ExSortPrivateState *)pentry_down->pstate);
    ex_queue::down_request request = pentry_down->downState.request;

    ex_assert((pstate.step_ == ExSortTcb::SORT_EMPTY || request == ex_queue::GET_NOMORE),
              "Invalid initial state in ex_sort_tcb::workDown()");

    if ((request == ex_queue::GET_NOMORE) ||
        ((request == ex_queue::GET_N) && (pentry_down->downState.requestValue == 0))) {
      // Parent canceled before start, or request 0 row,
      // goto SORT_DONE to reply
      pstate.step_ = ExSortTcb::SORT_DONE;
      ioEventHandler_->schedule();  // schedule workUp to reply

    } else {
      ex_queue_entry *centry = qchild_.down->getTailEntry();

      if (request == ex_queue::GET_N) {
        centry->downState.request = ex_queue::GET_ALL;
      } else
        centry->downState.request = request;

      centry->downState.requestValue = pentry_down->downState.requestValue;
      centry->downState.parentIndex = processedInputs_;

      centry->passAtp(pentry_down->getAtp());
      pstate.matchCount_ = 0;
      qchild_.down->insert();
      pstate.noOverflow_ = TRUE;
      // set the space buffer size and initial count whenever ALL request is sent
      if (bmoStats_) {
        ExSortTdb *sortTdb = (ExSortTdb *)getTdb();
        bmoStats_->setSpaceBufferSize(sortTdb->bufferSize_);
        bmoStats_->setSpaceBufferCount(sortTdb->numBuffers_);
      }
      pstate.step_ = ExSortTcb::SORT_PREP;
    }
  }
  return WORK_OK;
}

short ExSortTcb::workUp() {
  int rc = 0;
  short workRC = 0;
  int topNCount = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ExSortPrivateState &pstate = *((ExSortPrivateState *)pentry_down->pstate);
  ex_queue::down_request request = pentry_down->downState.request;

  // while there are requests in the parent down queue, process them
  while (qparent_.down->getHeadIndex() != processedInputs_) {
    pentry_down = qparent_.down->getHeadEntry();
    ExSortPrivateState &pstate = *((ExSortPrivateState *)pentry_down->pstate);
    request = pentry_down->downState.request;
    switch (pstate.step_) {
      case ExSortTcb::SORT_EMPTY:
        ex_assert(0, "Should never reach workUp with this state");
        return WORK_OK;
      case ExSortTcb::SORT_PREP: {
        if (sortDiag_ != NULL) {
          sortDiag_->decrRefCount();
          sortDiag_ = NULL;  // reset
        }
        if (bmoStats_) bmoStats_->setBmoPhase(SORT_PHASE_END - SORT_PREP_PHASE);
        setupPoolBuffers(pentry_down);

        if ((request == ex_queue::GET_N) && (pentry_down->downState.requestValue > 0))
          topNCount = (int)pentry_down->downState.requestValue;

        if (sortUtil_->sortInitialize(*sortCfg_, topNCount) != SORT_SUCCESS) {
          createSortDiags();
          pstate.step_ = ExSortTcb::SORT_ERROR;
          break;
        }

        pstate.step_ = ExSortTcb::SORT_SEND;
      } break;

      case ExSortTcb::RESTART_PARTIAL_SORT: {
        pstate.noOverflow_ = TRUE;
        sortPartiallyComplete_ = FALSE;
        sortUtil_->sortEnd();
        pstate.step_ = ExSortTcb::SORT_PREP;
      } break;

      case ExSortTcb::SORT_SEND: {
        if (request == ex_queue::GET_NOMORE) {
          // Parent canceled, inform child and consume input rows
          qchild_.down->cancelRequestWithParentIndex(qparent_.down->getHeadIndex());
          pstate.step_ = ExSortTcb::SORT_CANCELED;
          break;
        }

        // if nothing returned from child. Get outta here.
        if (qchild_.up->isEmpty()) {
          return workStatus(WORK_OK);
        }

        ex_queue_entry *centry = qchild_.up->getHeadEntry();
        ex_queue::up_status child_status = centry->upState.status;
        ex_queue_entry *upEntry = qparent_.up->getTailEntry();
        if (bmoStats_) bmoStats_->setBmoPhase(SORT_PHASE_END - SORT_SEND_PHASE);

        rc = sortSend(centry, child_status, pentry_down, upEntry,
                      FALSE,  // not sort from top
                      pstate.step_, pstate.matchCount_, pstate.allocatedTuppDesc_, pstate.noOverflow_, workRC);
        if (rc == 1) return workRC;

        // consume the child row in all cases except when
        // sort is partially complete. sortPartiallyComplete is TRUE
        // when subset of rows read is complete.
        if (!sortPartiallyComplete_) qchild_.up->removeHead();
      } break;

      case ExSortTcb::SORT_CANCELED: {
        // ignore all up rows from child. wait for Q_NO_DATA.
        int done = 0;
        while (!done) {
          if (qchild_.up->isEmpty()) {
            return workStatus(WORK_OK);
          } else {
            ex_queue_entry *cEntry = qchild_.up->getHeadEntry();
            switch (cEntry->upState.status) {
              case ex_queue::Q_OK_MMORE:
              case ex_queue::Q_SQLERROR: {
                qchild_.up->removeHead();
              } break;
              case ex_queue::Q_NO_DATA: {
                qchild_.up->removeHead();
                done = -1;
                pstate.step_ = ExSortTcb::SORT_DONE;
              } break;

              case ex_queue::Q_INVALID: {
                ex_assert(0, "ExSortTcb::work() invalid state returned by child");
              }; break;
            }
          }
        }
      } break;

      case ExSortTcb::SORT_ERROR: {
        // Inform child to cancel immediately. This may be called
        // several times if the parent up queue remains full,
        // but it has no unwanted side effect.
        qchild_.down->cancelRequestWithParentIndex(qparent_.down->getHeadIndex());
        // continue
      }
      case ExSortTcb::SORT_ERROR_ON_RECEIVE: {
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);  // parent queue is full. Just return
        }

        processSortError(pentry_down, pentry_down->downState.parentIndex, qparent_.down->getHeadIndex());

        // If the child has not finished sending rows to us,
        // enter SORT_CANCELED state so we can consume them
        // and throw them away. Otherwise, enter SORT_DONE
        // state. (Note that if we unconditionally enter
        // SORT_CANCELED state, then we may loop infinitely
        // waiting for the child to send us Q_NO_DATA a second
        // time.)
        if (pstate.allocatedTuppDesc_) {
          ReleaseTupp(pstate.allocatedTuppDesc_);
          pstate.allocatedTuppDesc_ = NULL;
        }

        if (pstate.step_ == ExSortTcb::SORT_ERROR)
          pstate.step_ = ExSortTcb::SORT_CANCELED;
        else
          pstate.step_ = ExSortTcb::SORT_DONE;
      } break;
      case ExSortTcb::SORT_RECEIVE: {
        // check if we've got room in the up queue
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);  // parent queue is full. Just return
        }
        if (bmoStats_) bmoStats_->setBmoPhase(SORT_PHASE_END - SORT_RECV_PHASE);

        // First time reaching here and before calling
        // sortReceive, release the buffers used during
        // sortSend phase ONLY if sort overflowed( by this
        // time, all sort records are in scratch files).
        // Overflow does not happen in TopNSort. Partial sort
        // has a separate receive pool.
        if ((sortSendPool_ != NULL) &&   // not yet released
            (!pstate.noOverflow_) &&     // overflow happened
            (!sortTdb().partialSort()))  // not partial sort
        {
          deleteAndReallocateSortPool();
        }

        ex_queue_entry *pentry = qparent_.up->getTailEntry();
        rc = sortReceive(pentry_down, request, pentry, FALSE, pentry_down->downState.parentIndex, pstate.step_,
                         pstate.matchCount_, pstate.allocatedTuppDesc_, pstate.noOverflow_, workRC);
        if (rc == 1) return workRC;

      } break;

      case ExSortTcb::SORT_DONE: {
        // check if we've got room in the parent up queue
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);  // parent queue is full. Just return
        }

        rc = done(TRUE,  // send Q_NO_DATA
                  pentry_down->downState.parentIndex, pstate.step_, pstate.matchCount_);
        qparent_.down->removeHead();
      } break;
      default:
        break;
    }  // switch pstate.step_

  }  // while

  return workStatus(WORK_OK);  // parent queue is full. Just return
};

///////////////////////////////////////////////////////////////////////
//
// Return code: if 1, then WORK returncode is returned in workRC.
//              Caller need to return that to its caller.
//              if 0, all ok.
//              if -1, error.
///////////////////////////////////////////////////////////////////////
short ExSortTcb::sortSend(ex_queue_entry *srcEntry, ex_queue::up_status srcStatus, ex_queue_entry *pentry_down,
                          ex_queue_entry *upEntry, NABoolean sortFromTop, SortStep &step, long &matchCount,
                          tupp_descriptor *&allocatedTuppDesc, NABoolean &noOverflow, short &workRC) {
  int rc = 0;

  ComDiagsArea *cda = NULL;

  switch (srcStatus) {
    case ex_queue::Q_OK_MMORE: {
      // first check if we have already allocated a td to hold
      // the returned row
      tupp_descriptor *td = NULL;
      SqlBuffer *buf = NULL;
      UInt32 defragLength = 0;

      if (!allocatedTuppDesc) {
        // accumulate any NF errors if any
        if (sortTdb().collectNFErrors()) {
          cda = srcEntry->getDiagsArea();
          if (cda && cda->mainSQLCODE() <= 0) {
            if (nfDiags_) {
              nfDiags_->insert(cda);
              cda->incrRefCount();
            } else {
              nfDiags_ = new (getGlobals()->getDefaultHeap()) NAList<ComDiagsArea *>(getGlobals()->getDefaultHeap());
              nfDiags_->insert(cda);
              cda->incrRefCount();
            }
          }
        }
        // allocate space to hold the encoded key followed
        // by the input record. Align the sort input record.

        // allocate space to hold the returned sorted row.

        td = NULL;

        if (defragTd_ &&  // considerBufferDefrag() && //resizeCifRecord() &&
            !sortSendPool_->currentBufferHasEnoughSpace(sortTdb().sortRecLen_)) {
#if defined(_DEBUG)
          assert(resizeCifRecord());
#endif
          // if variabble length and we can resize we try to allocate the actual size
          // which can be smaller than the max row size and may fit in the remaining
          // space in the buffer
          td = defragTd_;
          UInt32 dataOffset = sortTdb().sortKeyLen_ + numberOfBytesForRecordSize();
          UInt32 savedRowLen = sortTdb().sortRecLen_ - dataOffset;
          UInt32 rowLen = savedRowLen;
          UInt32 newRecLen = sortTdb().sortRecLen_;
          UInt32 *rowLenPtr = NULL;

          workAtp_->getTupp(2).setDataPointer(defragTd_->getTupleAddress());
          rowLenPtr = &rowLen;
          ex_expr::exp_return_type retCode = ex_expr::EXPR_OK;
          if (sortRecExpr()) {
            retCode = sortRecExpr()->eval(srcEntry->getAtp(), workAtp_, 0, -1, rowLenPtr);
            // if the expression succeeds and provides the actual length which is supposedly
            // less than the maxrow size, then we try to allocate space for the row using
            // the actual size.
            // if an error occur then we skip the allocation and defragmentation for this row
            if (retCode != ex_expr::EXPR_ERROR) {
              defragLength = *rowLenPtr;
              td = sortSendPool_->get_free_tupp_descriptor(defragLength + dataOffset, &buf);  // do we need &buf here??
            }
          }
        } else {
          td = sortSendPool_->get_free_tupp_descriptor(sortTdb().sortRecLen_, &buf);
        }

        ///////////////////////////////////////////////
        // add more buffers, if overflow not asked for.
        if (td == NULL) {
          if (sortTdb().sortOptions_->dontOverflow()) {
            sortSendPool_->addBuffer(sortTdb().bufferSize_);
          }
          // add more buffers if there is more space
          // available in the pool.
          else if (sortSendPool_->get_number_of_buffers() < sortTdb().maxNumBuffers_) {
            // No more space in the pool to allocate sorted rows.
            // Before adding a new buffer, check if we are within
            // limits of the memory quota system. Also try to get
            // additional quota if available.If not, perform
            // overflow processing.
            if (!sortUtil_->memoryQuotaSystemEnabled() || sortUtil_->consumeMemoryQuota(sortTdb().bufferSize_)) {
              // Add a new buffer.
              sortSendPool_->addBuffer(sortTdb().bufferSize_);
            } else {
              // Ask sort to overflow.
              rc = sortUtil_->sortClientOutOfMem();
              if (rc == SORT_IO_IN_PROGRESS) {
                // By returing work_call_again, scheduler will not give control back to
                // IPC and will schedule this operator again for next round
                workRC = WORK_CALL_AGAIN;

                return workStatus(1);
              }
              if (rc) {
                createSortDiags();
                step = ExSortTcb::SORT_ERROR;
                break;
              }
            }
          } else {
            // Ask sort to overflow.
            rc = sortUtil_->sortClientOutOfMem();
            if (rc == SORT_IO_IN_PROGRESS) {
              // By returing work_call_again, scheduler will not give
              //  control back to IPC and will schedule this operator
              //  again for next round
              workRC = WORK_CALL_AGAIN;
              return workStatus(1);
            }
            if (rc) {
              createSortDiags();
              step = ExSortTcb::SORT_ERROR;
              break;
            }
          }

          // allocate the tuple again. Either a new buffer could
          // have been added or tupples freed because of overflow
          // completion.
          td = sortSendPool_->get_free_tupp_descriptor(sortTdb().sortRecLen_, &buf);

          if (td == NULL) {
            // This is definitely a problem
            ex_assert(0, "Must get a tuple from pool as they must be available");
            step = ExSortTcb::SORT_ERROR;
            break;
          }
        }
        // reaching here td is not NULL.
      } else {
        // If the tuple was already allocated
        td = allocatedTuppDesc;
        allocatedTuppDesc = NULL;
      }
      // reaching here td is not NULL.
      ex_expr::exp_return_type retCode = ex_expr::EXPR_OK;
      char *dataPointer = td->getTupleAddress();

      // encode the  key value at the start of the tupp address + numberOfBytesForRecordSize
      workAtp_->getTupp(2).setDataPointer(dataPointer + numberOfBytesForRecordSize());

      // bump the data pointer past the encoded sort keys + numberOfBytesForRecordSize
      // since expressions assume offset starts at 0

      UInt32 dataOffset = sortTdb().sortKeyLen_ + numberOfBytesForRecordSize();

      UInt32 savedRowLen = sortTdb().sortRecLen_ - dataOffset;
      UInt32 rowLen = savedRowLen;
      UInt32 newRecLen = sortTdb().sortRecLen_;
      UInt32 *rowLenPtr = NULL;

      if (sortKeyExpr() != NULL) {
        retCode = sortKeyExpr()->eval(srcEntry->getAtp(), workAtp_);
      } else {
        dataOffset = 0;
      }

      if (retCode != ex_expr::EXPR_ERROR && sortRecExpr()) {
        // bump the data pointer past the encoded sort keys
        // since expressions assume offset starts at 0
        // if(sortKeyExpr() == NULL)
        //  dataOffset = sortTdb().numberOfBytesForRecordSize();

        workAtp_->getTupp(2).setDataPointer(dataPointer + dataOffset);

        // if the row is in compressed internal format, update the
        // rowlength with the actual row length.
        // if (workAtp_->getCriDesc()->getTupleDescriptor(2)->isSQLMXAlignedTable())

        if (defragLength) {
          str_cpy_all(dataPointer + dataOffset, defragTd_->getTupleAddress(), defragLength);

          newRecLen = defragLength + dataOffset;
          UInt32 *recSizePtr = (UInt32 *)dataPointer;
          *recSizePtr = newRecLen;

        } else {
          rowLenPtr = &rowLen;
          retCode = sortRecExpr()->eval(srcEntry->getAtp(), workAtp_, 0, -1, rowLenPtr);

          if (retCode != ex_expr::EXPR_ERROR) {
            // adjust row size
            // Applicable if sql_buffer_pool type.
            if (resizeCifRecord() > 0 && rowLenPtr && (topNSortPool_ == NULL)) {
              newRecLen = *rowLenPtr + dataOffset;
              if (*rowLenPtr != savedRowLen) {
                buf->resize_tupp_desc(newRecLen, dataPointer);
              }

              UInt32 *recSizePtr = (UInt32 *)dataPointer;
              *recSizePtr = newRecLen;
            }
          }
        }
      }

      if (retCode == ex_expr::EXPR_ERROR) {
        // The tupp_descriptors already sent to sortUtil_
        // will be cleaned up via sortUtil_->sortEnd(), but
        // this one must be freed now.
        td->setReferenceCount(0);

        if (qparent_.up->isFull()) {
          return WORK_OK;
        }

        ex_queue_entry *upEntry = qparent_.up->getTailEntry();

        upEntry->copyAtp(srcEntry);

        if (sortFromTop && (sortTdb().isNonFatalErrorTolerated()))
          upEntry->upState.status = ex_queue::Q_OK_MMORE;  // denotes nonfatal error
        else
          upEntry->upState.status = ex_queue::Q_SQLERROR;

        upEntry->upState.parentIndex = pentry_down->downState.parentIndex;
        upEntry->upState.downIndex = qparent_.down->getHeadIndex();
        upEntry->upState.setMatchNo(matchCount);

        qparent_.up->insert();

        if (sortFromTop) {
          if (NOT sortTdb().isNonFatalErrorTolerated()) step = ExSortTcb::SORT_ERROR;
        } else {
          qchild_.down->cancelRequestWithParentIndex(qparent_.down->getHeadIndex());
          step = ExSortTcb::SORT_CANCELED;
        }
        break;
      }

      // if partial sort and the setCompareTd_ is not set
      // then this is a new set. Lets keep a reference to
      // this rec for partialKey comparison.

      if (sortTdb().partialSort())
      // Partial sort is disabled and not active because it has issues
      {
        if (!setCompareTd_) {
          setCompareTd_ = td;

          // increment the reference count of this tupple.
          CaptureTupp(setCompareTd_);
        } else {
          // check if the current row is part of partial
          // set of rows read prior to this row.
          if (memcmp(setCompareTd_->getTupleAddress(), td->getTupleAddress(), sortTdb().sortPartialKeyLen_)) {
            // The child row belongs to a new set, hence
            // lets not consume the child row now. Lets
            // sort the existing set and revist again.
            // dereference setCompareTd_ for next set.

            ReleaseTupp(setCompareTd_);
            setCompareTd_ = NULL;

            // Don't consume the child row.
            // Also don't save td in pstate since
            // it may be set and reset by sort_receive.
            // dereference td for now.
            ReleaseTupp(td);

            if (sortUtil_->sortSendEnd(noOverflow) != SORT_SUCCESS) {
              createSortDiags();
              step = ExSortTcb::SORT_ERROR;
            } else
              // now retrieve the sorted rows
              step = ExSortTcb::SORT_RECEIVE;

            // indicate sort not yet complete.
            sortPartiallyComplete_ = TRUE;

            // This is a important break.
            break;
          }
        }
      }
      if (bmoStats_) bmoStats_->incInterimRowCount();
      rc = sortUtil_->sortSend(dataPointer, newRecLen, td);

      if (rc == SORT_IO_IN_PROGRESS) {
        // Don't consume the child row - just return
        // save the tuple descriptor that we used
        allocatedTuppDesc = td;

        // By returing work_call_again, scheduler will not give control back to
        // IPC and will schedule this operator again for next round
        workRC = WORK_CALL_AGAIN;
        return workStatus(1);
      }
      if (rc != SORT_SUCCESS) {
        // The tupp_descriptors already sent to sortUtil_
        // will be cleaned up via sortUtil_->sortEnd(), but
        // this one must be freed now.
        td->setReferenceCount(0);
        createSortDiags();
        step = ExSortTcb::SORT_ERROR;
        break;
      }
    } break;

    case ex_queue::Q_NO_DATA: {
      if (setCompareTd_) {
        // dereference setCompareTd_
        ReleaseTupp(setCompareTd_);
        setCompareTd_ = NULL;
      }
      sortPartiallyComplete_ = FALSE;

      if (sortUtil_->sortSendEnd(noOverflow) != SORT_SUCCESS) {
        createSortDiags();
        step = ExSortTcb::SORT_ERROR_ON_RECEIVE;
        break;
      }
      int topNCount = 0;
      if ((pentry_down->downState.request == ex_queue::GET_N) && (pentry_down->downState.requestValue > 0))
        topNCount = (int)pentry_down->downState.requestValue;

      if ((sortTdb().topNSortEnabled() == TRUE) && (sortTdb().raiseFirstNError() == TRUE) && (topNCount > 0) &&
          (sortUtil_->getRunSize() < topNCount)) {
        createSortNRetryDiags(topNCount, sortUtil_->getRunSize());
        step = ExSortTcb::SORT_ERROR_ON_RECEIVE;
        break;
      }

      // ensure that the rows affected are returned
      ComDiagsArea *da = srcEntry->getAtp()->getDiagsArea();
      if (da) {
        if (sortDiag_) {
          sortDiag_->mergeAfter(*da);
        } else {
          sortDiag_ = da;
          da->incrRefCount();
        }
      }

      // now retrieve the sorted rows
      step = ExSortTcb::SORT_RECEIVE;
    } break;

    case ex_queue::Q_SQLERROR: {
      if (qparent_.up->isFull()) {
        workRC = WORK_OK;
        return workStatus(1);
      }

      ex_queue_entry *upEntry = qparent_.up->getTailEntry();

      upEntry->copyAtp(srcEntry);
      upEntry->upState.status = ex_queue::Q_SQLERROR;
      upEntry->upState.parentIndex = pentry_down->downState.parentIndex;
      upEntry->upState.downIndex = qparent_.down->getHeadIndex();
      upEntry->upState.setMatchNo(matchCount);

      qparent_.up->insert();

      if (sortFromTop)
        step = ExSortTcb::SORT_ERROR;
      else {
        qchild_.down->cancelRequestWithParentIndex(qparent_.down->getHeadIndex());
        step = ExSortTcb::SORT_CANCELED;
      }
    } break;

    case ex_queue::Q_INVALID:
      ex_assert(0, "ExSortTcb::work() Invalid state returned by child");
      break;

  }  // switch srcStatus

  return 0;
}

///////////////////////////////////////////////////////////////////////
//
// Return code: if 1, then WORK returncode is returned in workRC.
//              Caller need to return that to its caller.
//              if 0, all ok.
//              if -1, error.
///////////////////////////////////////////////////////////////////////
short ExSortTcb::sortReceive(ex_queue_entry *pentry_down, ex_queue::down_request request, ex_queue_entry *tgtEntry,
                             NABoolean sortFromTop, queue_index parentIndex, SortStep &step, long &matchCount,
                             tupp_descriptor *&allocatedTuppDesc, NABoolean &noOverflow, short &workRC) {
  int rc = 0;
  SqlBuffer *buf = NULL;

  // if recs were written out to disk, then
  // allocate space to hold the returned sorted row.
  // If overflow didn't happen, then sort will return the
  // same tupp_descriptor that we gave it.
  int reclen = sortTdb().sortRecLen_;
  tupp_descriptor *td = NULL;
  tupp_descriptor *receiveTd = NULL;

  if (noOverflow == FALSE)  // overflow happened
  {
    if (!allocatedTuppDesc) {
      if (pentry_down) tgtEntry->copyAtp(pentry_down);

      td = receivePool_->get_free_tupp_descriptor(sortTdb().sortRecLen_, &buf);
      if (td == NULL) {
        // if sortSendPool_ is NULL, means there is option to
        // try and add additional buffers as long as upqueue is not full.
        // Upqueue will drive addition of buffers(sortSend is not called
        // if upQueue is full), assumption is very few buffers.
        // Add buffer outside of memory quota.
        receivePool_->addBuffer(sortTdb().bufferSize_);

        // try getting a tupp now.
        td = receivePool_->get_free_tupp_descriptor(sortTdb().sortRecLen_, &buf);
        if (td == NULL) {
          // no more space in the pool to allocate sorted rows from.
          // Return and come back later when some space gets freed up.
          workRC = WORK_POOL_BLOCKED;
          return workStatus(1);
        }
      }
      tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_) = td;
      ReleaseTupp(td);
      // Call sortReceive. It will copy the sorted row at
      // getDataPointer().
    } else {
      td = allocatedTuppDesc;
      allocatedTuppDesc = NULL;
    }
    char *dataPointer = td->getTupleAddress();
    rc = sortUtil_->sortReceive(dataPointer, reclen);
  } else {  // overflow did not happen
    void *rec = NULL;
    void *v = (void *)td;
    // If partial sort, then allocate a tuple from sort pool.
    // The result roecord is copied from regular pool_ to
    // partialSortPool_.
    if (sortTdb().partialSort()) {
      if (!allocatedTuppDesc) {
        if (pentry_down) tgtEntry->copyAtp(pentry_down);

        receiveTd = receivePool_->get_free_tupp_descriptor(sortTdb().sortRecLen_, &buf);
        if (receiveTd == NULL) {
          // no more space in the pool to allocate sorted rows from.
          // Return and come back later when some space gets freed up.
          workRC = WORK_POOL_BLOCKED;
          return workStatus(1);
        }

        tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_) = receiveTd;
      } else {
        // ex_assert(0, "???");
        receiveTd = allocatedTuppDesc;
        allocatedTuppDesc = NULL;
      }
    } else if (pentry_down)
      tgtEntry->copyAtp(pentry_down);

    // SortReceive will return a reference in the
    // tupp descriptor. We need to copy the contents
    // of resultTd if partial sort.
    rc = sortUtil_->sortReceive(rec, reclen, v);
    if ((rc == SORT_SUCCESS) && (reclen > 0))  // got a row
    {
      td = (tupp_descriptor *)v;
      td->setReferenceCount(0);
      if (sortTdb().partialSort()) {
        // In the case of partial sort, we need to copy
        // the record into sortPool directly.
        memcpy(receiveTd->getTupleAddress(), td->getTupleAddress(), reclen);
      } else
        tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_) = td;
    }
  }

  if (rc == SORT_IO_IN_PROGRESS) {
    if (noOverflow == FALSE)
      // Tupp descriptor allocated, remember it
      allocatedTuppDesc = td;
    else {  // overflow did not happen
      if (sortTdb().partialSort()) allocatedTuppDesc = receiveTd;
      receiveTd = NULL;
    }
    // Don't consume the child row - just return
    // By returing work_call_again, scheduler will not give control back to
    // IPC and will schedule this operator again for next round
    workRC = WORK_CALL_AGAIN;

    return workStatus(1);
  }
  if (rc != SORT_SUCCESS) {
    // error returned
    createSortDiags();
    if (sortPartiallyComplete_)
      step = ExSortTcb::SORT_ERROR;
    else
      step = ExSortTcb::SORT_ERROR_ON_RECEIVE;
    if (receiveTd) {
      tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_).release();
      receiveTd = NULL;
    }
    return 0;
  } else {
    if (reclen > 0)  // got a row
    {
      if (sortTdb().collectNFErrors()) {
        // If there are any nf error entries in the array
        if (nfDiags_) {
          ComDiagsArea *nfda = NULL;
          // Get the first NF error in the list
          NABoolean ret = nfDiags_->getFirst(nfda);
          // assign it to the tgtEntry diags
          if (ret && nfda) {
            tgtEntry->setDiagsArea(nfda);
            // remove this entry from the nf list
            nfDiags_->remove(nfda);
          }
        }
      }

      Int16 *rowLenPtr = NULL;
      if (resizeCifRecord() > 0 && (topNSortPool_ == NULL)) {
        if (buf) {
          char *dataPointer = tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_).getDataPointer();
          buf->resize_tupp_desc(*((int *)dataPointer), dataPointer);  //
        }
      }
      // bump the data pointer past the encoded sort keys
      // since parent expressions assume offset starts at 0

      if (sortKeyExpr() != NULL) {
        UInt32 dataOffset = sortTdb().sortKeyLen_ + numberOfBytesForRecordSize();

        // commented out for now-- wiil be removed later
        // if(sortKeyExpr() == NULL)
        // dataOffset = numberOfBytesForRecordSize();

        tgtEntry->getAtp()
            ->getTupp(sortTdb().tuppIndex_)
            .setDataPointer(tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_).getDataPointer() + dataOffset);
      }

      // if stats are to be collected, collect them.
      if (bmoStats_) bmoStats_->incActualRowsReturned();

      matchCount++;

      if (NOT sortFromTop)  // tgt is parent
      {
        // move it to parent's up queue
        tgtEntry->upState.status = ex_queue::Q_OK_MMORE;
        tgtEntry->upState.parentIndex = parentIndex;
        tgtEntry->upState.downIndex = qparent_.down->getHeadIndex();
        tgtEntry->upState.setMatchNo(matchCount);

        qparent_.up->insert();

        // stop if first N sorted rows have been returned.
        if ((request == ex_queue::GET_NOMORE) ||
            ((request == ex_queue::GET_N) && ((int)matchCount >= pentry_down->downState.requestValue))) {
          // If sort partially complete, lets cancel rest of
          // of the child row reads.
          if (sortPartiallyComplete_) {
            qchild_.down->cancelRequestWithParentIndex(qparent_.down->getHeadIndex());
            step = ExSortTcb::SORT_CANCELED;
          } else
            step = ExSortTcb::SORT_DONE;
        }
      } else {
        // move it to child's down queue
        // if (pentry_down)
        // tgtEntry->copyAtp(pentry_down);

        tgtEntry->downState.request = request;
        tgtEntry->downState.requestValue = 11;  // just a number
        tgtEntry->downState.parentIndex = parentIndex;
        qchild_.down->insert();
      }
    } else {
      // EOF
      if (receiveTd) {
        tgtEntry->getAtp()->getTupp(sortTdb().tuppIndex_).release();
        receiveTd = NULL;
      }

      if (sortPartiallyComplete_) {
        step = ExSortTcb::RESTART_PARTIAL_SORT;
      } else {
        step = ExSortTcb::SORT_DONE;

        if (getStatsEntry()) {
          SortStatistics s;
          sortUtil_->getStatistics(&s);
          // tbd -- these operator level stats need to be cleared
          // between statement executions.  Note that it will not
          // be sufficient to clear them between GET_ALL requests
          // because this sort could be on the right hand side of
          // a nested join (i.e., could get many requets per
          // statement execution), for example, non-atomic rowset
          // inserts with index maintenance.

          if (sortStats_) {
            sortStats_->runSize() = s.getStatRunSize();

            sortStats_->numRuns() = s.getStatNumRuns();
          }

          if (bmoStats_ == NULL && getStatsEntry()->castToExMeasStats()) {
            getStatsEntry()->castToExMeasStats()->incNumSorts(1);
            long elapsedTime;
            elapsedTime = s.getStatElapsedTime();

            getStatsEntry()->castToExMeasStats()->incSortElapsedTime(elapsedTime);
          }
        }
      }
    }
  }

  return 0;
}

short ExSortTcb::done(NABoolean sendQND, queue_index parentIndex, SortStep &step, long &matchCount) {
  ex_queue_entry *pentry = qparent_.up->getTailEntry();

  // all sorted rows have been returned.
  if (sendQND) {
    pentry->upState.status = ex_queue::Q_NO_DATA;
    pentry->upState.parentIndex = parentIndex;
    pentry->upState.downIndex = qparent_.down->getHeadIndex();
    pentry->upState.setMatchNo(matchCount);

    // mainly used to propagate rows affected count to parent
    if (sortDiag_) {
      ComDiagsArea *pUPda = pentry->getDiagsArea();
      if (pUPda == NULL)
        pUPda = ComDiagsArea::allocate(this->getGlobals()->getDefaultHeap());
      else
        pUPda->incrRefCount();

      pUPda->mergeAfter(*sortDiag_);
      pentry->setDiagsArea(pUPda);
    }

    // insert into parent up queue
    qparent_.up->insert();
  }

  matchCount = 0;
  step = ExSortTcb::SORT_EMPTY;

  sortUtil_->sortEnd();
  if (setCompareTd_) {
    // dereference setCompareTd_
    ReleaseTupp(setCompareTd_);
    setCompareTd_ = NULL;
  }
  sortPartiallyComplete_ = FALSE;

  if (nfDiags_) nfDiags_->deallocate();
  nfDiags_ = NULL;

  return 0;
}

short ExSortTcb::cancel() {
  queue_index pindex = qparent_.down->getHeadIndex();
  while (pindex != qparent_.down->getTailIndex()) {
    ex_queue_entry *pentry = qparent_.down->getQueueEntry(pindex);
    if (pentry->downState.request == ex_queue::GET_NOMORE) {
      ExSortPrivateState &pstate = *((ExSortPrivateState *)pentry->pstate);
      if (pstate.allocatedTuppDesc_) {
        ReleaseTupp(pstate.allocatedTuppDesc_);
        pstate.allocatedTuppDesc_ = NULL;
      }

      switch (pstate.step_) {
        case SORT_EMPTY:
          // Nothing is in the down queue yet for this request, but parent
          // does expect a Q_NO_DATA so just change pstate.step_ to
          // SORT_DONE;
          pstate.step_ = SORT_DONE;
          ioEventHandler_->schedule();  // schedule workUp to reply
          break;
        case SORT_PREP:
        case SORT_SEND:
          // Request has been sent to child, so cancel and discard child
          // responses.
          qchild_.down->cancelRequestWithParentIndex(pindex);
          pstate.step_ = ExSortTcb::SORT_CANCELED;
          break;
        case SORT_ERROR:
          // Request-to-child still pending, so cancel and discard child
          // responses.
          qchild_.down->cancelRequestWithParentIndex(pindex);
          // The parent is not interested to process error, go to cancel.
          pstate.step_ = ExSortTcb::SORT_CANCELED;
          break;
        case SORT_RECEIVE:
          if (sortPartiallyComplete_) {
            qchild_.down->cancelRequestWithParentIndex(pindex);
            pstate.step_ = ExSortTcb::SORT_CANCELED;
          } else
            // Child has returned Q_NO_DATA and entry pop'd from child up queue.
            pstate.step_ = SORT_DONE;
          break;
        case SORT_ERROR_ON_RECEIVE:
          // Child has returned Q_NO_DATA and entry pop'd from child up queue.
          // The parent is not interested to process error, go to done.
          pstate.step_ = SORT_DONE;
          break;
        case SORT_CANCELED:
          // Nothing to do that hasn't already been done.
          break;
        case SORT_DONE:
          // Too late.  nothing to do.
          break;
      }
    }
    pindex++;
  }
  return WORK_OK;
};

///////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for sort_private_state
///////////////////////////////////////////////////////////////////////////////

ExSortPrivateState::ExSortPrivateState() {
  matchCount_ = 0;
  allocatedTuppDesc_ = NULL;
  step_ = ExSortTcb::SORT_EMPTY;
  noOverflow_ = TRUE;
}
ExSortPrivateState::~ExSortPrivateState() {
  if (allocatedTuppDesc_) ReleaseTupp(allocatedTuppDesc_);
};
void ReleaseTupp(void *td) {
  ((tupp_descriptor *)td)->setReferenceCount(((tupp_descriptor *)td)->getReferenceCount() - 1);
}
void CaptureTupp(void *td) {
  ((tupp_descriptor *)td)->setReferenceCount(((tupp_descriptor *)td)->getReferenceCount() + 1);
}
////////////////////////////////////////////////////////////////////////////
// ExSortFromTopTcb: input is from parent, output is to child
////////////////////////////////////////////////////////////////////////////
ExSortFromTopTcb::ExSortFromTopTcb(const ExSortTdb &sort_tdb, const ex_tcb &child_tcb, ex_globals *glob)
    : ExSortTcb(sort_tdb, child_tcb, glob),
      step_(SORT_EMPTY),
      matchCount_(0),
      allocatedTuppDesc_(NULL),
      noOverflow_(TRUE) {
  CollHeap *space = glob->getSpace();

  // sorted rows will be copied to child.
  // Allocate target atps in the child's down queue.
  qchild_.down->allocateAtps(space);
}

ExSortFromTopTcb::~ExSortFromTopTcb() {}

void ExSortFromTopTcb::registerSubtasks() {
  ExScheduler *sched = getGlobals()->getScheduler();

  // Set up the event handler information
  ioEventHandler_ = sched->registerNonQueueSubtask(sWork, this);

  // Set up the event handler information in the sortCfg truct to pass to Sort
  sortCfg_->setEventHandler(ioEventHandler_);

  // the parent queues will be resizable, so register a resize subtask.
  registerResizeSubtasks();
  if (getPool()) getPool()->setStaticMode(FALSE);
}

short ExSortFromTopTcb::work() {
  int rc = 0;
  short workRC = 0;

  if (qparent_.down->isEmpty()) {
    return workStatus(WORK_OK);
  }

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

  if (pentry_down->downState.request == ex_queue::GET_NOMORE) {
    // cancel request
    step_ = ExSortTcb::SORT_CANCEL_CHILD;
  }

  while (1) {
    switch (step_) {
      case ExSortTcb::SORT_EMPTY: {
        if (qparent_.down->isEmpty()) {
          return workStatus(WORK_OK);
        }

        pentry_down = qparent_.down->getHeadEntry();

        matchCount_ = 0;
        allocatedTuppDesc_ = NULL;
        noOverflow_ = TRUE;

        numParentRows_ = 0;
        numSortedRows_ = 0;
        numChildEODs_ = 0;
        eodToChild_ = FALSE;

        if (pentry_down->downState.request == ex_queue::GET_EOD)
          step_ = ExSortTcb::SORT_DONE_WITH_QND;
        else
          step_ = ExSortTcb::SORT_PREP;
      } break;

      case ExSortTcb::SORT_PREP: {
        if (sortDiag_ != NULL) {
          sortDiag_->decrRefCount();
          sortDiag_ = NULL;  // reset
        }

        if (sortUtil_->sortInitialize(*sortCfg_, 0) != SORT_SUCCESS) {
          createSortDiags();
          step_ = ExSortTcb::SORT_ERROR;
          break;
        }

        step_ = ExSortTcb::SORT_SEND;
      } break;

      case ExSortTcb::SORT_SEND: {
        // get next input from parent and send it to sort.
        if (qparent_.down->isEmpty()) {
          return workStatus(WORK_OK);
        }

        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);
        }

        pentry_down = qparent_.down->getHeadEntry();
        ex_queue_entry *pentry = qparent_.up->getTailEntry();

        ex_queue::up_status srcStatus;
        if (pentry_down->downState.request == ex_queue::GET_EOD) {
          srcStatus = ex_queue::Q_NO_DATA;
        } else {
          srcStatus = ex_queue::Q_OK_MMORE;
        }

        rc = sortSend(pentry_down, srcStatus, pentry_down, pentry,
                      TRUE,  // sort from top
                      step_, matchCount_, allocatedTuppDesc_, noOverflow_, workRC);
        if (rc == 1) return workStatus(workRC);

        // step_ of SORT_SEND indicates that a row was successfully sent to
        // sort. State would have been changed to SORT_RECEIVE if all rows
        // were sent.
        if (step_ == ExSortTcb::SORT_SEND) {
          // no error, row was sent to sort.
          step_ = ExSortTcb::SORT_SEND_END;
          break;
        }
      } break;

      case ExSortTcb::SORT_SEND_END: {
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);
        }

        ex_queue_entry *pentry = qparent_.up->getTailEntry();

        // send a Q_NO_DATA to parent for all rows except for GET_EOD.
        // GET_EOD will be replied to after completion of work.
        // (which means after all rows have been sent to child and
        //  replies to those rows have been received from child.
        //  Any error will also be returned as part of the reply
        //  to GET_EOD).
        numParentRows_++;

        pentry->upState.status = ex_queue::Q_NO_DATA;
        pentry->upState.parentIndex = pentry_down->downState.parentIndex;
        pentry->upState.downIndex = qparent_.down->getHeadIndex();
        pentry->upState.setMatchNo(0);

        qparent_.up->insert();
        qparent_.down->removeHead();

        step_ = ExSortTcb::SORT_SEND;
      } break;

      case ExSortTcb::SORT_RECEIVE: {
        // get next sorted row from sort and send it to child.
        if (qchild_.down->isFull()) {
          return workStatus(WORK_OK);
        }

        pentry_down = qparent_.down->getHeadEntry();
        ex_queue_entry *centry = qchild_.down->getTailEntry();
        rc = sortReceive(pentry_down, ex_queue::GET_ALL, centry, TRUE, pentry_down->downState.parentIndex, step_,
                         matchCount_, allocatedTuppDesc_, noOverflow_, workRC);
        if (rc == 1) return workStatus(workRC);
        if (step_ == ExSortTcb::SORT_DONE) {
          // All sorted rows have been sent to the child.
          // Now send GET_EOD to indicate eod to the child.
          step_ = ExSortTcb::SORT_SEND_EOD_TO_CHILD;
        } else if (step_ == ExSortTcb::SORT_RECEIVE) {
          numSortedRows_++;

          // not all rows have been sent to child.
          // Switch to PROCESS_REPLY_FROM_CHILD state to
          // check if child has sent some replies to the sent rows.
          // If so, consume them.
          step_ = ExSortTcb::SORT_PROCESS_REPLY_FROM_CHILD;
        }
      } break;

      case ExSortTcb::SORT_SEND_EOD_TO_CHILD: {
        if (qchild_.down->isFull()) {
          return workStatus(WORK_OK);
        }

        eodToChild_ = TRUE;
        step_ = SORT_PROCESS_REPLY_FROM_CHILD;

        // GET_EOD is only sent to child if this is sidetree insert.
        // STI expects a GET_EOD to be sent to commit.
        if (NOT sortTdb().userSidetreeInsert()) {
          break;
        }

        pentry_down = qparent_.down->getHeadEntry();
        ex_queue_entry *centry = qchild_.down->getTailEntry();
        centry->downState.request = ex_queue::GET_EOD;
        centry->downState.requestValue = 11;  // just a number
        centry->downState.parentIndex = pentry_down->downState.parentIndex;
        qchild_.down->insert();

      } break;

      case ExSortTcb::SORT_PROCESS_REPLY_FROM_CHILD: {
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);
        }

        if (qchild_.up->isEmpty()) {
          // child hasn't returned any reply rows. If more rows are to
          // be sent to child, process that.
          if (NOT eodToChild_) {
            step_ = ExSortTcb::SORT_RECEIVE;
            break;
          }
          return workStatus(WORK_OK);
        }

        ex_queue_entry *centry = qchild_.up->getHeadEntry();
        switch (centry->upState.status) {
          case ex_queue::Q_NO_DATA: {
            qchild_.up->removeHead();
            numChildEODs_++;

            if ((eodToChild_) && (numChildEODs_ == numSortedRows_ + (sortTdb().userSidetreeInsert() ? 1 : 0)))
              step_ = ExSortTcb::SORT_DONE_WITH_QND;
          } break;

          case ex_queue::Q_SQLERROR: {
            ex_queue_entry *pentry = qparent_.up->getTailEntry();

            pentry->copyAtp(centry);
            pentry->upState.status = ex_queue::Q_SQLERROR;
            pentry->upState.parentIndex = pentry_down->downState.parentIndex;
            pentry->upState.downIndex = qparent_.down->getHeadIndex();
            pentry->upState.setMatchNo(0);

            qparent_.up->insert();
            qchild_.up->removeHead();

            step_ = ExSortTcb::SORT_CANCEL_CHILD;
          } break;

          case ex_queue::Q_OK_MMORE: {
            ex_queue_entry *pentry = qparent_.up->getTailEntry();

            pentry->copyAtp(centry);
            pentry->upState.status = ex_queue::Q_OK_MMORE;
            pentry->upState.parentIndex = pentry_down->downState.parentIndex;
            pentry->upState.downIndex = qparent_.down->getHeadIndex();
            pentry->upState.setMatchNo(0);

            qparent_.up->insert();
            qchild_.up->removeHead();
          } break;

          default: {
            ex_assert(0, "ExSortFromTopTcb::work() Error state Q_OK_MMORE from child");
          } break;

        }  // switch
      } break;

      case ExSortTcb::SORT_ERROR: {
        // SORT_ERROR can occur when processing rows before sending
        // any row to child. In other words,pentry_down->downState.request
        // is not received ex_queue::GET_EOD yet.
        ex_assert(numSortedRows_ == 0, "ExSortFromTopTcb::work() Sort Error when row to child");

        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);
        }

        // this error is returned during sortSend phase.
        // No rows have been sent to child yet.
        processSortError(pentry_down, pentry_down->downState.parentIndex, qparent_.down->getHeadIndex());

        step_ = ExSortTcb::SORT_DONE_WITH_QND;

      } break;

      case ExSortTcb::SORT_ERROR_ON_RECEIVE: {
        // this error is returned during sortReceive phase
        // or at the end of sortSend when all rows have been sent
        // to child.
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);  // parent queue is full.
        }

        processSortError(pentry_down, pentry_down->downState.parentIndex, qparent_.down->getHeadIndex());

        // step to cancel. Cancel will check for all scenarios.
        step_ = ExSortTcb::SORT_CANCEL_CHILD;
      } break;
      case ExSortTcb::SORT_CANCEL_CHILD: {
        // this state is reached when child returns an error
        // or parent sends a cancel request.

        // Or sortUtil encounters an error and reports to parent
        // even before any row is sent down to child. The result
        // of reporting error to parent, cancel state is called
        // for every pending queue entry in the parents down queue.
        // In this case, just report QND to parent.

        // if numSortedRows_ == 0, means no row was sent to child
        // and looks like we have received a cancel before that.
        // In this case, just send a QND. This is achived by stepping
        // to ExSortTcb::SORT_DONE_WITH_QND state. Cleanup is done in
        // ExSortTcb::SORT_DONE_WITH_QND state.
        if (numSortedRows_ == 0) {
          step_ = ExSortTcb::SORT_DONE_WITH_QND;
          break;
        }

        // If we arrive here because of cancel and if numChildEODs
        // has been received equal to what we have sent to child,
        // there is nothing to expect from child. So just reply QND
        if (numChildEODs_ == numSortedRows_ + (eodToChild_ && sortTdb().userSidetreeInsert() ? 1 : 0)) {
          step_ = ExSortTcb::SORT_DONE_WITH_QND;
          break;
        }

        if (qchild_.up->isEmpty()) return workStatus(WORK_OK);

        ex_queue_entry *centry = qchild_.up->getHeadEntry();
        switch (centry->upState.status) {
          case ex_queue::Q_OK_MMORE:
          case ex_queue::Q_SQLERROR: {
            qchild_.up->removeHead();
          } break;

          case ex_queue::Q_NO_DATA: {
            qchild_.up->removeHead();

            numChildEODs_++;
            if (numChildEODs_ == numSortedRows_ + (eodToChild_ && sortTdb().userSidetreeInsert() ? 1 : 0))
              step_ = ExSortTcb::SORT_DONE_WITH_QND;
          } break;

          default: {
            ex_assert(0, "ExSortFromTopTcb::work() Error state returned from child");
          } break;
        }
      } break;

      case ExSortTcb::SORT_DONE_WITH_QND: {
        // check if we've got room in the parent up queue
        if (qparent_.up->isFull()) {
          return workStatus(WORK_OK);
        }

        pentry_down = qparent_.down->getHeadEntry();

        rc = done(TRUE, pentry_down->downState.parentIndex, step_, matchCount_);

        qparent_.down->removeHead();

        if (allocatedTuppDesc_) ReleaseTupp(allocatedTuppDesc_);

        // all done, get outta here.
        return workStatus(WORK_OK);
      } break;

      default: {
        ex_assert(0, "ExSortFromTopTcb::work() Invalid switch entry");
      } break;

    }  // switch step_
  }    // while (1)

  return WORK_OK;
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state *ExSortFromTopTcb::allocatePstates(int &numElems,      // inout, desired/actual elements
                                                        int &pstateLength)  // out, length of one element
{
  PstateAllocator<ExSortFromTopPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

ExSortFromTopPrivateState::ExSortFromTopPrivateState() {}

ExSortFromTopPrivateState::~ExSortFromTopPrivateState(){};
