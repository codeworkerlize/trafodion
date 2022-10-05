// **********************************************************************

// **********************************************************************

#include "common/Platform.h"

#include "executor/ex_stdh.h"
#include "cli/Context.h"
#include "comexe/ComTdb.h"
#include "executor/ex_tcb.h"
#include "ExHbaseAccess.h"
#include "ex_exe_stmt_globals.h"
#include "exp_function.h"

#include "exp/ExpLOBinterface.h"

#include "SQLTypeDefs.h"

#include "exp/ExpHbaseInterface.h"

#include "executor/ExDDLValidator.h"

static char *costThreshold = getenv("RECORD_TIME_COST_JNI");
static char *costJniAll = getenv("RECORD_TIME_COST_JNI_ALL");
static int recordJniAll = costJniAll ? atoi(costJniAll) : -1;
static pid_t pid = getpid();
static int callCount[30] = {0};
static double sumCost[30] = {0};
static long curTransId = 0;

ExHbaseScanTaskTcb::ExHbaseScanTaskTcb(ExHbaseAccessSelectTcb *tcb) : ExHbaseTaskTcb(tcb), step_(NOT_STARTED) {}

void ExHbaseScanTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseScanTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;
  int remainingInBatch = batchSize_;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }

  while (1) {
    ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = SCAN_OPEN;
      } break;

      case SCAN_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        // Bypass scan when beginRowId_ is less than endRowId_
        if (tcb_->compareRowIds() < 0) {
          step_ = DONE;
          break;
        }

        if (tcb_->setupHbaseFilterPreds()) {
          step_ = HANDLE_ERROR;
          break;
        }

        retcode = tcb_->ehi_->scanOpen(
            tcb_->table_, tcb_->beginRowId_, tcb_->endRowId_, tcb_->columns_, -1,
            tcb_->hbaseAccessTdb().readUncommittedScan(), tcb_->hbaseAccessTdb().getScanMemoryTable(),
            tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
            tcb_->hbaseAccessTdb().skipReadConflict(), tcb_->hbaseAccessTdb().skipTransactionForced(),
            tcb_->hbaseAccessTdb().replSync(), tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScanner(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->numCacheRows(), FALSE,
            (tcb_->hbaseFilterColumns_.entries() > 0 ? &tcb_->hbaseFilterColumns_ : NULL),
            (tcb_->hbaseFilterOps_.entries() > 0 ? &tcb_->hbaseFilterOps_ : NULL),
            (tcb_->hbaseFilterValues_.entries() > 0 ? &tcb_->hbaseFilterValues_ : NULL),
            tcb_->hbaseAccessTdb().getNumReplications(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->dopParallelScanner(), tcb_->getSamplePercentage(), FALSE,
            0, NULL, NULL, 0,
            tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                ? &(tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions())
                : NULL);
        //                                           (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
        //                                            ?
        //                                            tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions().getNumVersions()
        //                                            : 0));

        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[0]++;
            sumCost[0] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanTaskTcb SCAN_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[0], (sumCost[0] / (callCount[0] ? callCount[0] : 1)), sumCost[0], time2, tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_ROW;
      } break;

      case NEXT_ROW: {
        if (--remainingInBatch <= 0) {
          rc = WORK_CALL_AGAIN;
          return 1;
        }
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[1]++;
            sumCost[1] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanTaskTcb NEXTROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[1], (sumCost[1] / (callCount[1] ? callCount[1] : 1)), sumCost[1], time2, tcb_->table_.val);
        }
        if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
          step_ = SCAN_CLOSE;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;
      case NEXT_CELL: {
        if (tcb_->colVal_.val == NULL)
          tcb_->colVal_.val = new (tcb_->getHeap()) char[tcb_->hbaseAccessTdb().convertRowLen()];
        tcb_->colVal_.len = tcb_->hbaseAccessTdb().convertRowLen();
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextCell(tcb_->rowId_, tcb_->colFamName_, tcb_->colName_, tcb_->colVal_, tcb_->colTS_);
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[2]++;
            sumCost[2] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanTaskTcb NEXTCELL PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[2], (sumCost[2] / (callCount[2] ? callCount[2] : 1)), sumCost[2], time2, tcb_->table_.val);
        }
        if (retcode == HBASE_ACCESS_EOD) {
          step_ = NEXT_ROW;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
          step_ = HANDLE_ERROR;
        else
          step_ = CREATE_ROW;
      } break;

      case CREATE_ROW: {
        rc = tcb_->createColumnwiseRow();
        if (rc == -1) {
          step_ = HANDLE_ERROR;
          break;
        } else if (tcb_->setupError(rc, "ExHbaseAccessTcb::createColumnwiseRow",
                                    "Not enough space in target buffer to move data")) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());
        if (rc == 1)
          step_ = RETURN_ROW;
        else if (rc == -1)
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case RETURN_ROW: {
        if (tcb_->moveRowToUpQueue(
                tcb_->convertRow_,
                tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(),
                &rc, FALSE))
          return 1;

        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();

        if ((pentry_down->downState.request == ex_queue::GET_N) &&
            (pentry_down->downState.requestValue == tcb_->matches_)) {
          step_ = SCAN_CLOSE;
          break;
        }

        step_ = NEXT_CELL;
      } break;

      case SCAN_CLOSE: {
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->scanClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[3]++;
            sumCost[3] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanTaskTcb SCAN_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[3], (sumCost[3] / (callCount[3] ? callCount[3] : 1)), sumCost[3], time2, tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE: {
        step_ = NOT_STARTED;
        return 0;
      } break;

    }  // switch

  }  // while
}

ExHbaseScanRowwiseTaskTcb::ExHbaseScanRowwiseTaskTcb(ExHbaseAccessSelectTcb *tcb)
    : ExHbaseTaskTcb(tcb), step_(NOT_STARTED) {}

void ExHbaseScanRowwiseTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseScanRowwiseTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;
  int remainingInBatch = batchSize_;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }

  while (1) {
    ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = SCAN_OPEN;
      } break;

      case SCAN_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        // Bypass scan when beginRowId_ is less than endRowId_
        if (tcb_->compareRowIds() < 0) {
          step_ = DONE;
          break;
        }
        if (tcb_->setupHbaseFilterPreds()) {
          step_ = HANDLE_ERROR;
          break;
        }

        retcode = tcb_->ehi_->scanOpen(
            tcb_->table_, tcb_->beginRowId_, tcb_->endRowId_, tcb_->columns_, -1,
            tcb_->hbaseAccessTdb().readUncommittedScan(), tcb_->hbaseAccessTdb().getScanMemoryTable(),
            tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
            tcb_->hbaseAccessTdb().skipReadConflict(), tcb_->hbaseAccessTdb().skipTransactionForced(),
            tcb_->hbaseAccessTdb().replSync(), tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScanner(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->numCacheRows(), FALSE,
            (tcb_->hbaseFilterColumns_.entries() > 0 ? &tcb_->hbaseFilterColumns_ : NULL),
            (tcb_->hbaseFilterOps_.entries() > 0 ? &tcb_->hbaseFilterOps_ : NULL),
            (tcb_->hbaseFilterValues_.entries() > 0 ? &tcb_->hbaseFilterValues_ : NULL),
            tcb_->hbaseAccessTdb().getNumReplications(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->dopParallelScanner(), tcb_->getSamplePercentage(), FALSE,
            0, NULL, NULL, 0,
            (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                 ? &(tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions())
                 : NULL));

        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[4]++;
            sumCost[4] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanRowwiseTaskTcb SCAN_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                   transId, callCount[4], (sumCost[4] / (callCount[4] ? callCount[4] : 1)), sumCost[4], time2,
                   tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_ROW;

        tcb_->isEOD_ = FALSE;
      } break;

      case NEXT_ROW: {
        if (--remainingInBatch <= 0) {
          rc = WORK_CALL_AGAIN;
          return 1;
        }

        tcb_->rowwiseRowLen_ = 0;
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[5]++;
            sumCost[5] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanRowwiseTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                   transId, callCount[5], (sumCost[5] / (callCount[5] ? callCount[5] : 1)), sumCost[5], time2,
                   tcb_->table_.val);
        }
        if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
          step_ = SCAN_CLOSE;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case NEXT_CELL: {
        if (tcb_->colVal_.val == NULL)
          tcb_->colVal_.val = new (tcb_->getHeap()) char[tcb_->hbaseAccessTdb().convertRowLen()];
        tcb_->colVal_.len = tcb_->hbaseAccessTdb().convertRowLen();
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextCell(tcb_->rowId_, tcb_->colFamName_, tcb_->colName_, tcb_->colVal_, tcb_->colTS_);
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[6]++;
            sumCost[6] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanRowwiseTaskTcb NEXT_CELL PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                   transId, callCount[6], (sumCost[6] / (callCount[6] ? callCount[6] : 1)), sumCost[6], time2,
                   tcb_->table_.val);
        }
        if (retcode == HBASE_ACCESS_EOD) {
          step_ = CREATE_ROW;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
          step_ = HANDLE_ERROR;
        else
          step_ = APPEND_ROW;
      } break;

      case APPEND_ROW: {
        retcode = tcb_->copyCell();
        if (tcb_->setupError(retcode, "ExHbaseAccessTcb::copyCell", "Not enough space in target buffer to move data"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case CREATE_ROW: {
        rc = tcb_->createRowwiseRow();
        if (rc == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());

        if (rc == 1) {
          step_ = RETURN_ROW;
          break;
        } else if (rc == -1) {
          step_ = HANDLE_ERROR;
          break;
        } else if (tcb_->isEOD_)
          step_ = SCAN_CLOSE;
        else
          step_ = NEXT_ROW;
      } break;

      case RETURN_ROW: {
        rc = 0;
        if (tcb_->moveRowToUpQueue(
                tcb_->convertRow_,
                tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(),
                &rc, FALSE))
          return 1;

        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();

        if ((pentry_down->downState.request == ex_queue::GET_N) &&
            (pentry_down->downState.requestValue == tcb_->matches_)) {
          step_ = SCAN_CLOSE;
          break;
        }
        step_ = NEXT_ROW;
      } break;

      case SCAN_CLOSE: {
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->scanClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[7]++;
            sumCost[7] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanRowwiseTaskTcb SCAN_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                   transId, callCount[7], (sumCost[7] / (callCount[7] ? callCount[7] : 1)), sumCost[7], time2,
                   tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE: {
        step_ = NOT_STARTED;
        return 0;
      } break;

    }  // switch

  }  // while

  return 0;  // WORK_OK
}

ExHbaseScanSQTaskTcb::ExHbaseScanSQTaskTcb(ExHbaseAccessSelectTcb *tcb) : ExHbaseTaskTcb(tcb), step_(NOT_STARTED) {
  // Note: logic to initialize ddlValidator_ has been pushed up into parent class ExHbaseTaskTcb
}

void ExHbaseScanSQTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseScanSQTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;
  int remainingInBatch = batchSize_;
  NABoolean isFirstBatch = false;
  // isFirstInBatch is a stack variable for optimization reason. It is used for the mdam small scanner optimization
  // heuristic that is performed at runtime. Since this function is invoke intensively for all scan (mdam or regular
  // scan), minimizing CPU/memory access impact on runtime code to a strict minimum is attempted. Given that we are
  // trying to detect if the actual scan is bellow the size of an HBase block, having the runtime logic performing the
  // detection only affect the first work invoke looks like the right idea. and leveraging an existing counter
  // (remainingInBatch) instead of creating a new one. The reasonable asumption to allow this is that 1- batchSize_
  // being 8K, most likely times the row size, we are good in assuming that first hbase block will fit in batchSize 2-
  // parent buffer size will be large enough to deal with one HBAse_Block_size without having to rely on re-invoking
  // work in the middle. and anyway, if none of the reasonable assumption is true, then that's fine, the heuristic won't
  // work, and we will use regular scanner, meaning optimization is off for the scan part of MDAM (still on for the
  // probe side of it).

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }

  while (1) {
    ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = SCAN_OPEN;
      } break;

      case SCAN_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        // Bypass scan when beginRowId_ is less than endRowId_
        if (tcb_->compareRowIds() < 0) {
          step_ = DONE;
          break;
        }
        if (tcb_->setupHbaseFilterPreds()) {
          step_ = HANDLE_ERROR;
          break;
        }
        isFirstBatch = true;
        NABoolean isForUpdate = (tcb_->hbaseAccessTdb().getLockMode() == HBaseLockMode::LOCK_U);
        tcb_->ehi_->setFirstReadBypassTm(tcb_->hbaseAccessTdb().getFirstReadBypassTm());

        retcode = tcb_->ehi_->scanOpen(
            tcb_->table_, tcb_->beginRowId_, tcb_->endRowId_, tcb_->columns_, -1,
            tcb_->hbaseAccessTdb().readUncommittedScan(), tcb_->hbaseAccessTdb().getScanMemoryTable(),
            tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
            tcb_->hbaseAccessTdb().skipReadConflict(), tcb_->hbaseAccessTdb().skipTransactionForced(),
            tcb_->hbaseAccessTdb().replSync(), tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScanner(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->numCacheRows(), !isForUpdate,
            (tcb_->hbaseFilterColumns_.entries() > 0 ? &tcb_->hbaseFilterColumns_ : NULL),
            (tcb_->hbaseFilterOps_.entries() > 0 ? &tcb_->hbaseFilterOps_ : NULL),
            (tcb_->hbaseFilterValues_.entries() > 0 ? &tcb_->hbaseFilterValues_ : NULL),
            tcb_->hbaseAccessTdb().getNumReplications(),
            tcb_->hbaseAccessTdb().getHbasePerfAttributes()->dopParallelScanner(), tcb_->getSamplePercentage(),
            tcb_->hbaseAccessTdb().getHbaseSnapshotScanAttributes()->getUseSnapshotScan(),
            tcb_->hbaseAccessTdb().getHbaseSnapshotScanAttributes()->getSnapshotScanTimeout(),
            tcb_->hbaseAccessTdb().getHbaseSnapshotScanAttributes()->getSnapshotName(),
            tcb_->hbaseAccessTdb().getHbaseSnapshotScanAttributes()->getSnapScanTmpLocation(),
            tcb_->getGlobals()->castToExExeStmtGlobals()->getMyInstanceNumber(),
            (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                 ? &(tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions())
                 : NULL),
            (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                 ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths()
                 : NULL),
            (tcb_->hbaseAccessTdb().useEncryption() ? (char *)&tcb_->encryptionInfo_ : NULL));

        if (tcb_->hbaseAccessTdb().getScanMemoryTable() && tcb_->ehi_->isReadFromMemoryTable() == false &&
            tcb_->ehi_->isMemoryTableDisabled() == false && tcb_->ehi_->ismemDBinitFailed() == false &&
            tcb_->beginRowId_.size() == 0 && tcb_->endRowId_.size() == 0) {
          if (tcb_->hbaseAccessTdb().getLoadDataIntoMemoryTable()) {
            tcb_->setLoadDataIntoMemoryTable(true);
            tcb_->ehi_->memoryTableCreate();
          }
        }

        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[8]++;
            sumCost[8] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanSQTaskTcb SCAN_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[8], (sumCost[8] / (callCount[8] ? callCount[8] : 1)), sumCost[8], time2, tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen"))
          step_ = HANDLE_ERROR;
        else {
          step_ = NEXT_ROW;
          // set up a DDL Validator to monitor for DDL changes
          if (ddlValidator_.validatingDDL()) {
            tcb_->ehi_->setDDLValidator(&ddlValidator_);
          }
        }
      } break;

      case NEXT_ROW: {
        if (--remainingInBatch <= 0) {
          rc = WORK_CALL_AGAIN;
          return 1;
        }
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[9]++;
            sumCost[9] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanSQTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[9], (sumCost[9] / (callCount[9] ? callCount[9] : 1)), sumCost[9], time2, tcb_->table_.val);
        }
        if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
          step_ = SCAN_CLOSE;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else if (tcb_->hbaseAccessTdb().multiVersions())
          step_ = SETUP_MULTI_VERSION_ROW;
        else
          step_ = CREATE_ROW;
      } break;

      case SETUP_MULTI_VERSION_ROW: {
        retcode = tcb_->setupSQMultiVersionRow();
        if (retcode == HBASE_ACCESS_NO_ROW) {
          step_ = NEXT_ROW;
          break;
        }

        if (retcode < 0) {
          rc = (short)retcode;
          tcb_->setupError(rc, "setupSQMultiVersionRow");
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = CREATE_ROW;
      } break;

      case CREATE_ROW: {
        if (tcb_->ehi_->isReadFromMemoryTable())
          retcode = tcb_->createSQRowDirectFromMemoryTable();
        else
          retcode = tcb_->createSQRowDirect();
        if (retcode == HBASE_ACCESS_NO_ROW) {
          step_ = NEXT_ROW;
          break;
        }
        if (retcode < 0) {
          // special case error handling for progress status table.
          // Rowid cells of this table sometimes get removed. That results
          // in an error being returned when this table is scanned.
          // The cause is still being diagnosed.
          // Until then, ignore the row if error 8034 is returned.
          if ((pentry_down) && (pentry_down->getDiagsArea()->mainSQLCODE() == -EXE_DEFAULT_VALUE_INCONSISTENT_ERROR) &&
              (tcb_->hbaseAccessTdb().getTableName() ||
               tcb_->hbaseAccessTdb().replaceNameByUID() && tcb_->hbaseAccessTdb().getDataUIDName())) {
            char tabnam[400];
            str_sprintf(tabnam, "%s:%s.%s.%s", TRAF_RESERVED_NAMESPACE2, TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA,
                        "BREI_PROGRESS_STATUS_TABLE");

            if (tcb_->hbaseAccessTdb().replaceNameByUID()) {
              if (NAString(tcb_->hbaseAccessTdb().getDataUIDName()) == NAString(tabnam)) {
                pentry_down->getDiagsArea()->clear();
                step_ = NEXT_ROW;
                break;
              }
            } else {
              if (NAString(tcb_->hbaseAccessTdb().getTableName()) == NAString(tabnam)) {
                pentry_down->getDiagsArea()->clear();
                step_ = NEXT_ROW;
                break;
              }
            }
          }

          rc = (short)retcode;
          tcb_->setupError(rc, "createSQRowDirect");
          step_ = HANDLE_ERROR;
          break;
        }
        if (retcode != HBASE_ACCESS_SUCCESS) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());
        if (rc == 1)
          step_ = RETURN_ROW;
        else if (rc == -1)
          step_ = HANDLE_ERROR;
        else if (tcb_->hbaseAccessTdb().multiVersions())
          step_ = CREATE_ROW;
        else
          step_ = NEXT_ROW;
      } break;

      case RETURN_ROW: {
        if (tcb_->moveRowToUpQueue(tcb_->convertRow_, tcb_->convertRowLen_, &rc, FALSE)) return 1;
        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();

        if ((pentry_down->downState.request == ex_queue::GET_N) &&
            (pentry_down->downState.requestValue == tcb_->matches_)) {
          step_ = SCAN_CLOSE;
          break;
        }

        if (tcb_->hbaseAccessTdb().multiVersions())
          step_ = CREATE_ROW;
        else

          step_ = NEXT_ROW;
      } break;

      case SCAN_CLOSE: {
        if (isFirstBatch)  // only if closed happen in a single batch, batchSize - remainingInBatch = nb rows retrieved
          tcb_->hbaseAccessTdb().getHbasePerfAttributes()->setUseSmallScannerForMDAMifNeeded(
              batchSize_ - remainingInBatch);  // calculate MDAM small scanner flag for next scan if it was MDAM
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->scanClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[10]++;
            sumCost[10] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseScanSQTaskTcb SCAN_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[10], (sumCost[10] / (callCount[10] ? callCount[10] : 1)), sumCost[10], time2,
                   tcb_->table_.val);
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        tcb_->ehi_->setDDLValidator(NULL);
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE: {
        tcb_->ehi_->setDDLValidator(NULL);
        step_ = NOT_STARTED;
        return 0;
      } break;

    }  // switch

  }  // while
}

int ExHbaseScanSQTaskTcb::getProbeResult(char *&keyData) {
  int retcode = 0;
  int rc = 0;
  int probeSize = 100;  // using fewer rows results in intermittent wrong
  // results. Using the hbase default scan size of 100 as a workarorund.
  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }
  if (tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useMinMdamProbeSize())
    probeSize = 1;  // if performance is vital, comp_bool_184 can be set to ON
  // to choose this path.

  long time1 = JULIANTIMESTAMP();
  retcode = tcb_->ehi_->scanOpen(
      tcb_->table_, tcb_->beginRowId_, tcb_->endRowId_, tcb_->columns_, -1,
      tcb_->hbaseAccessTdb().readUncommittedScan(), tcb_->hbaseAccessTdb().getScanMemoryTable(),
      tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
      tcb_->hbaseAccessTdb().skipReadConflict(), tcb_->hbaseAccessTdb().skipTransactionForced(),
      tcb_->hbaseAccessTdb().replSync(),
      tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks() ||
          tcb_->hbaseAccessTdb()
              .getHbasePerfAttributes()
              ->useSmallScannerForProbes(),  // when small scanner feature is ON or SYSTEM force cache ON
      tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScannerForProbes(), probeSize, TRUE, NULL, NULL, NULL,
      tcb_->hbaseAccessTdb().getNumReplications());
  if (recordCostTh_ >= 0) {
    long time2 = (JULIANTIMESTAMP() - time1) / 1000;
    if (transId > 0) {
      callCount[11]++;
      sumCost[11] += time2;
    }
    if (time2 >= recordCostTh_)
      QRWARN("ExHbaseScanSQTaskTcb::getProbeResult SCAN_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
             transId, callCount[11], (sumCost[11] / (callCount[11] ? callCount[11] : 1)), sumCost[11], time2,
             tcb_->table_.val);
  }

  if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen")) {
    rc = -1;
    goto label_return;
  }

  time1 = JULIANTIMESTAMP();
  retcode = tcb_->ehi_->nextRow();
  if (recordCostTh_ >= 0) {
    long time2 = (JULIANTIMESTAMP() - time1) / 1000;
    if (transId > 0) {
      callCount[12]++;
      sumCost[12] += time2;
    }
    if (time2 >= recordCostTh_)
      QRWARN("ExHbaseScanSQTaskTcb::getProbeResult NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
             transId, callCount[12], (sumCost[12] / (callCount[12] ? callCount[12] : 1)), sumCost[12], time2,
             tcb_->table_.val);
  }
  if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
    rc = 1;  // no row found
    goto label_return;
  }
  if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow")) {
    rc = -1;
    goto label_return;
  }
  retcode = tcb_->createSQRowDirect();
  if (retcode == HBASE_ACCESS_NO_ROW) {
    rc = 1;
    goto label_return;
  }
  if (retcode < 0) {
    rc = retcode;
    tcb_->setupError(rc, "createSQRowDirect");
    rc = -1;
    goto label_return;
  }
  if (retcode != HBASE_ACCESS_SUCCESS) {
    rc = -1;
    goto label_return;
  }

  // extract the key from the fetched row, encode it and pass it back to mdam
  if (tcb_->evalEncodedKeyExpr() == -1) {
    rc = -1;
    goto label_return;
  }

label_return:
  time1 = JULIANTIMESTAMP();
  retcode = tcb_->ehi_->scanClose();
  if (recordCostTh_ >= 0) {
    long time2 = (JULIANTIMESTAMP() - time1) / 1000;
    if (transId > 0) {
      callCount[13]++;
      sumCost[13] += time2;
    }
    if (time2 >= recordCostTh_)
      QRWARN("ExHbaseScanSQTaskTcb::getProbeResult SCAN_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
             transId, callCount[13], sumCost[13] / (callCount[13] ? callCount[13] : 1), sumCost[13], time2,
             tcb_->table_.val);
  }
  if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose")) {
    rc = -1;
  }

  keyData = tcb_->encodedKeyRow_;

  return rc;
}

ExHbaseGetTaskTcb::ExHbaseGetTaskTcb(ExHbaseAccessSelectTcb *tcb) : ExHbaseTaskTcb(tcb), step_(NOT_STARTED) {}

void ExHbaseGetTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseGetTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;
  int remainingInBatch = batchSize_;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }
  while (1) {
    ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        step_ = GET_OPEN;
      } break;

      case GET_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        tcb_->ehi_->setWaitOnSelectForUpdate(tcb_->hbaseAccessTdb().waitOnSelectForUpdate());
        tcb_->ehi_->setFirstReadBypassTm(tcb_->hbaseAccessTdb().getFirstReadBypassTm());

        if (tcb_->evalRowIdExpr(TRUE) == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        if (tcb_->rowIds_.entries() == 1) {
          retcode = tcb_->ehi_->getRowOpen(
              tcb_->table_, tcb_->rowIds_[0], tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable());
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        } else {
          retcode = tcb_->ehi_->getRowsOpen(
              tcb_->table_, &tcb_->rowIds_, tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable(), tcb_->hbaseAccessTdb().skipReadConflict(),
              tcb_->hbaseAccessTdb().skipTransactionForced());
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowsOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        }

        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[14]++;
            sumCost[14] += time2;
          }
          if (time2 >= recordCostTh_) {
            if (tcb_->rowIds_.entries() == 1)
              QRWARN("ExHbaseGetTaskTcb GET_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                     callCount[14], sumCost[14] / (callCount[14] ? callCount[14] : 1), sumCost[14], time2,
                     tcb_->table_.val);
            else
              QRWARN("ExHbaseGetTaskTcb GET_OPEN_ROWS PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                     callCount[14], sumCost[14] / (callCount[14] ? callCount[14] : 1), sumCost[14], time2,
                     tcb_->table_.val);
          }
        }
      } break;

      case NEXT_ROW: {
        if (--remainingInBatch <= 0) {
          rc = WORK_CALL_AGAIN;
          return 1;
        }
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[15]++;
            sumCost[15] += time2;
          }
          if (time2 >= recordCostTh_) {
            QRWARN("ExHbaseGetTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[15], sumCost[15] / (callCount[15] ? callCount[15] : 1), sumCost[15], time2,
                   tcb_->table_.val);
          }
        }
        if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
          step_ = GET_CLOSE;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case NEXT_CELL: {
        if (tcb_->colVal_.val == NULL)
          tcb_->colVal_.val = new (tcb_->getHeap()) char[tcb_->hbaseAccessTdb().convertRowLen()];
        tcb_->hbaseAccessTdb().convertRowLen();
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextCell(tcb_->rowId_, tcb_->colFamName_, tcb_->colName_, tcb_->colVal_, tcb_->colTS_);
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[16]++;
            sumCost[16] += time2;
          }
          if (time2 >= recordCostTh_) {
            QRWARN("ExHbaseGetTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[16], sumCost[16] / (callCount[16] ? callCount[16] : 1), sumCost[16], time2,
                   tcb_->table_.val);
          }
        }
        if (retcode == HBASE_ACCESS_EOD) {
          step_ = NEXT_ROW;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
          step_ = HANDLE_ERROR;
        else
          step_ = CREATE_ROW;
      } break;

      case CREATE_ROW: {
        rc = tcb_->createColumnwiseRow();
        if (rc == -1) {
          step_ = HANDLE_ERROR;
          break;
        } else if (tcb_->setupError(rc, "ExHbaseAccessTcb::createColumnwiseRow",
                                    "Not enough space in target buffer to move data")) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());

        if (rc == 1)
          step_ = RETURN_ROW;
        else if (rc == -1)
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case RETURN_ROW: {
        rc = 0;
        if (tcb_->moveRowToUpQueue(
                tcb_->convertRow_,
                tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(),
                &rc, FALSE))
          return 1;

        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();

        if ((pentry_down->downState.request == ex_queue::GET_N) &&
            (pentry_down->downState.requestValue == tcb_->matches_)) {
          step_ = GET_CLOSE;
          break;
        }

        step_ = NEXT_CELL;
      } break;

      case GET_CLOSE: {
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->getClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[17]++;
            sumCost[17] += time2;
          }
          if (time2 >= recordCostTh_) {
            QRWARN("ExHbaseGetTaskTcb GET_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[17], sumCost[17] / (callCount[17] ? callCount[17] : 1), sumCost[17], time2,
                   tcb_->table_.val);
          }
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::getClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE: {
        step_ = NOT_STARTED;
        return 0;
      } break;

    }  // switch

  }  // while
}

ExHbaseGetRowwiseTaskTcb::ExHbaseGetRowwiseTaskTcb(ExHbaseAccessSelectTcb *tcb)
    : ExHbaseTaskTcb(tcb), step_(NOT_STARTED) {}

void ExHbaseGetRowwiseTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseGetRowwiseTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;
  int remainingInBatch = batchSize_;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }

  while (1) {
    ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = GET_OPEN;
      } break;

      case GET_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        tcb_->ehi_->setWaitOnSelectForUpdate(tcb_->hbaseAccessTdb().waitOnSelectForUpdate());

        if (tcb_->evalRowIdExpr(TRUE) == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        if (tcb_->rowIds_.entries() == 1) {
          retcode = tcb_->ehi_->getRowOpen(
              tcb_->table_, tcb_->rowIds_[0], tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable());
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        } else {
          retcode = tcb_->ehi_->getRowsOpen(
              tcb_->table_, &tcb_->rowIds_, tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable(), tcb_->hbaseAccessTdb().skipReadConflict(),
              tcb_->hbaseAccessTdb().skipTransactionForced());
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowsOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        }

        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[18]++;
            sumCost[18] += time2;
          }
          if (time2 > recordCostTh_) {
            if (tcb_->rowIds_.entries() == 1)
              QRWARN("ExHbaseGetRowwiseTaskTcb GET_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                     transId, callCount[18], sumCost[18] / (callCount[18] ? callCount[18] : 1), sumCost[18], time2,
                     tcb_->table_.val);
            else
              QRWARN("ExHbaseGetRowwiseTaskTcb GET_OPEN_ROWS PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                     transId, callCount[18], sumCost[18] / (callCount[18] ? callCount[18] : 1), sumCost[18], time2,
                     tcb_->table_.val);
          }
        }
      } break;

      case NEXT_ROW: {
        if (--remainingInBatch <= 0) {
          rc = WORK_CALL_AGAIN;
          return 1;
        }

        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[19]++;
            sumCost[19] += time2;
          }
          if (time2 > recordCostTh_) {
            QRWARN("ExHbaseGetRowwiseTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[19], sumCost[19] / (callCount[19] ? callCount[19] : 1), sumCost[19], time2,
                   tcb_->table_.val);
          }
        }
        if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR) {
          step_ = GET_CLOSE;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case NEXT_CELL: {
        if (tcb_->colVal_.val == NULL)
          tcb_->colVal_.val = new (tcb_->getHeap()) char[tcb_->hbaseAccessTdb().convertRowLen()];
        tcb_->colVal_.len = tcb_->hbaseAccessTdb().convertRowLen();
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextCell(tcb_->rowId_, tcb_->colFamName_, tcb_->colName_, tcb_->colVal_, tcb_->colTS_);
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[20]++;
            sumCost[20] += time2;
          }
          if (time2 > recordCostTh_) {
            QRWARN("ExHbaseGetRowwiseTaskTcb nextCell PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[20], sumCost[20] / (callCount[20] ? callCount[20] : 1), sumCost[20], time2,
                   tcb_->table_.val);
          }
        }
        if (retcode == HBASE_ACCESS_EOD)
          step_ = CREATE_ROW;
        else if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
          step_ = HANDLE_ERROR;
        else
          step_ = APPEND_ROW;
      } break;

      case APPEND_ROW: {
        retcode = tcb_->copyCell();
        if (tcb_->setupError(retcode, "ExHbaseAccessTcb::copyCell", "Not enough space in target buffer to move data"))
          step_ = HANDLE_ERROR;
        else
          step_ = NEXT_CELL;
      } break;

      case CREATE_ROW: {
        rc = tcb_->createRowwiseRow();
        if (rc == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());

        if (rc == 1)
          step_ = RETURN_ROW;
        else if (rc == -1)
          step_ = HANDLE_ERROR;
        else
          step_ = GET_CLOSE;
      } break;

      case RETURN_ROW: {
        rc = 0;
        if (tcb_->moveRowToUpQueue(
                tcb_->convertRow_,
                tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(),
                &rc, FALSE))
          return 1;

        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();

        step_ = GET_CLOSE;
      } break;

      case GET_CLOSE: {
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->getClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[21]++;
            sumCost[21] += time2;
          }
          if (time2 > recordCostTh_) {
            QRWARN("ExHbaseGetRowwiseTaskTcb GET_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                   transId, callCount[21], sumCost[21] / (callCount[21] ? callCount[21] : 1), sumCost[21], time2,
                   tcb_->table_.val);
          }
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::getClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE: {
        step_ = NOT_STARTED;
        return 0;
      } break;

    }  // switch

  }  // while
}

ExHbaseGetSQTaskTcb::ExHbaseGetSQTaskTcb(ExHbaseAccessTcb *tcb, NABoolean rowsetTcb)
    : ExHbaseTaskTcb(tcb, rowsetTcb), step_(NOT_STARTED) {}

void ExHbaseGetSQTaskTcb::init() { step_ = NOT_STARTED; }

ExWorkProcRetcode ExHbaseGetSQTaskTcb::work(short &rc) {
  int retcode = 0;
  rc = 0;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }

  while (1) {
    ex_queue_entry *pentry_down = NULL;
    if (!tcb_->qparent_.down->isEmpty()) pentry_down = tcb_->qparent_.down->getHeadEntry();

    switch (step_) {
      case NOT_STARTED: {
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = GET_OPEN;
      } break;

      case GET_OPEN: {
        long time1 = JULIANTIMESTAMP();
        tcb_->fixObjName4PartTbl(tcb_->hbaseAccessTdb().getTableName(), tcb_->hbaseAccessTdb().getDataUIDName(),
                                 tcb_->hbaseAccessTdb().replaceNameByUID());
        remainingInBatch_ = tcb_->rowIds_.entries();
        tcb_->ehi_->setWaitOnSelectForUpdate(tcb_->hbaseAccessTdb().waitOnSelectForUpdate());
        tcb_->ehi_->setFirstReadBypassTm(tcb_->hbaseAccessTdb().getFirstReadBypassTm());
        if (tcb_->rowIds_.entries() == 1) {
          retcode = tcb_->ehi_->getRowOpen(
              tcb_->table_, tcb_->rowIds_[0], tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable(), tcb_->hbaseAccessTdb().skipReadConflict(),
              (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                   ? &(tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions())
                   : NULL),
              (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                   ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths()
                   : NULL),
              (tcb_->hbaseAccessTdb().useEncryption() ? (char *)&tcb_->encryptionInfo_ : NULL));
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        } else {
          retcode = tcb_->ehi_->getRowsOpen(
              tcb_->table_, &tcb_->rowIds_, tcb_->columns_, -1, tcb_->hbaseAccessTdb().getNumReplications(),
              tcb_->hbaseAccessTdb().getLockMode(), tcb_->hbaseAccessTdb().getIsolationLevel(),
              tcb_->hbaseAccessTdb().getScanMemoryTable(), tcb_->hbaseAccessTdb().skipReadConflict(),
              tcb_->hbaseAccessTdb().skipTransactionForced(),
              (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                   ? &(tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAccessOptions())
                   : NULL),
              (tcb_->hbaseAccessTdb().getComHbaseAccessOptions()
                   ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths()
                   : NULL),
              (tcb_->hbaseAccessTdb().useEncryption() ? (char *)&tcb_->encryptionInfo_ : NULL));
          if (recordCostTh_ >= 0) {
            long time2 = (JULIANTIMESTAMP() - time1) / 1000;
            if (transId > 0) {
              callCount[22]++;
              sumCost[22] += time2;
            }
            if (time2 >= recordCostTh_) {
              if (tcb_->rowIds_.entries() == 1)
                QRWARN("ExHbaseGetSQTaskTcb GET_OPEN PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                       callCount[22], (sumCost[22] / (callCount[22] ? callCount[22] : 1)), sumCost[22], time2,
                       tcb_->table_.val);
              else
                QRWARN("ExHbaseGetSQTaskTcb GET_OPEN_ROWS PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid,
                       transId, callCount[22], (sumCost[22] / (callCount[22] ? callCount[22] : 1)), sumCost[22], time2,
                       tcb_->table_.val);
            }
          }
          if (tcb_->setupError(retcode, "ExpHbaseInterface::getRowsOpen"))
            step_ = HANDLE_ERROR;
          else
            step_ = NEXT_ROW;
        }
      } break;

      case NEXT_ROW: {
        if (remainingInBatch_ <= 0) {
          step_ = GET_CLOSE;
          break;
        }
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->nextRow();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[23]++;
            sumCost[23] += time2;
          }
          if (time2 >= recordCostTh_) {
            QRWARN("ExHbaseGetSQTaskTcb NEXT_ROW PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[23], (sumCost[23] / (callCount[23] ? callCount[23] : 1)), sumCost[23], time2,
                   tcb_->table_.val);
          }
        }
        remainingInBatch_--;
        // EOD is end of data, EOR is end of result set.
        // for single get, EOD or EOR indicates DONE
        // for multi get, only EOR indicates DONE
        if ((retcode == HBASE_ACCESS_EOR) || ((retcode == HBASE_ACCESS_EOD) && (tcb_->rowIds_.entries() == 1))) {
          if (rowsetTcb_)
            step_ = DONE;
          else
            step_ = GET_CLOSE;
          break;
        }

        // for multi get, do FETCH if retcode is EOD
        if ((retcode == HBASE_ACCESS_EOD) && (tcb_->rowIds_.entries() > 1)) {
          if (rowsetTcb_)
            step_ = DONE;
          else
            step_ = NEXT_ROW;
          break;
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
          step_ = HANDLE_ERROR;
        else
          step_ = CREATE_ROW;
      } break;

      case CREATE_ROW: {
        if (tcb_->ehi_->isReadFromMemoryTable())
          retcode = tcb_->createSQRowDirectFromMemoryTable();
        else
          retcode = tcb_->createSQRowDirect();
        if (retcode == HBASE_ACCESS_NO_ROW) {
          if (rowsetTcb_)
            step_ = DONE;
          else
            step_ = NEXT_ROW;
          break;
        }
        if (retcode < 0) {
          // special case error handling for progress status table.
          // Rowid cells of this table sometimes get removed. That results
          // in an error being returned when this table is scanned.
          // The cause is still being diagnosed.
          // Until then, ignore the row if error 8034 is returned.
          if ((pentry_down) && (pentry_down->getDiagsArea()->mainSQLCODE() == -EXE_DEFAULT_VALUE_INCONSISTENT_ERROR) &&
              (tcb_->hbaseAccessTdb().getTableName() ||
               tcb_->hbaseAccessTdb().replaceNameByUID() && tcb_->hbaseAccessTdb().getDataUIDName())) {
            char tabnam[400];
            str_sprintf(tabnam, "%s:%s.%s.%s", TRAF_RESERVED_NAMESPACE2, TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA,
                        "BREI_PROGRESS_STATUS_TABLE");

            if (tcb_->hbaseAccessTdb().replaceNameByUID()) {
              if (NAString(tcb_->hbaseAccessTdb().getDataUIDName()) == NAString(tabnam)) {
                pentry_down->getDiagsArea()->clear();
                step_ = NEXT_ROW;
                break;
              }
            } else {
              if (NAString(tcb_->hbaseAccessTdb().getTableName()) == NAString(tabnam)) {
                pentry_down->getDiagsArea()->clear();
                step_ = NEXT_ROW;
                break;
              }
            }
          }

          rc = (short)retcode;
          tcb_->setupError(rc, "createSQRowDirect");
          step_ = HANDLE_ERROR;
          break;
        }
        if (retcode != HBASE_ACCESS_SUCCESS) {
          step_ = HANDLE_ERROR;
          break;
        }
        step_ = APPLY_PRED;
      } break;

      case APPLY_PRED: {
        rc = tcb_->applyPred(tcb_->scanExpr());

        if (rc == 1)
          step_ = RETURN_ROW;
        else if (rc == -1)
          step_ = HANDLE_ERROR;
        else {
          if (rowsetTcb_)
            step_ = DONE;
          else
            step_ = NEXT_ROW;
        }
      } break;
      case RETURN_ROW: {
        rc = 0;
        if (tcb_->moveRowToUpQueue(
                tcb_->convertRow_,
                tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(),
                &rc, FALSE))
          return 1;
        if (tcb_->getHbaseAccessStats()) tcb_->getHbaseAccessStats()->incUsedRows();
        if (rowsetTcb_)
          step_ = DONE;
        else {
          if ((pentry_down->downState.request == ex_queue::GET_N) &&
              (pentry_down->downState.requestValue == tcb_->matches_)) {
            step_ = GET_CLOSE;
            break;
          }
          step_ = NEXT_ROW;
        }
      } break;

      case GET_CLOSE: {
        long time1 = JULIANTIMESTAMP();
        retcode = tcb_->ehi_->getClose();
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[24]++;
            sumCost[24] += time2;
          }
          if (time2 >= recordCostTh_) {
            QRWARN("ExHbaseGetSQTaskTcb GET_CLOSE PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[24], (sumCost[24] / (callCount[24] ? callCount[24] : 1)), sumCost[24], time2,
                   tcb_->table_.val);
          }
        }
        if (tcb_->setupError(retcode, "ExpHbaseInterface::getClose"))
          step_ = HANDLE_ERROR;
        else
          step_ = ALL_DONE;
      } break;

      case HANDLE_ERROR: {
        step_ = NOT_STARTED;
        return -1;
      } break;

      case DONE:
        if (tcb_->handleDone(rc, 0))
          return 1;
        else
          step_ = NEXT_ROW;
        break;
      case ALL_DONE:
        step_ = NOT_STARTED;
        return 0;
        break;
    }  // switch

  }  // while
  return 0;
}

ExHbaseAccessSelectTcb::ExHbaseAccessSelectTcb(const ExHbaseAccessTdb &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {
  scanRowwiseTaskTcb_ = NULL;
  scanTaskTcb_ = NULL;
  getRowwiseTaskTcb_ = NULL;
  getTaskTcb_ = NULL;
  scanSQTaskTcb_ = NULL;
  getSQTaskTcb_ = NULL;

  ExHbaseAccessTdb &hbaseTdb = (ExHbaseAccessTdb &)hbaseAccessTdb;
  samplePercentage_ = hbaseTdb.samplingRate_;

  if ((hbaseTdb.listOfScanRows()) || ((hbaseTdb.keySubsetGen()) && (NOT hbaseTdb.uniqueKeyInfo())) ||
      (hbaseTdb.keyMDAMGen())) {
    if (hbaseTdb.sqHbaseTable())
      scanSQTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseScanSQTaskTcb(this);
    else if (hbaseTdb.rowwiseFormat())
      scanRowwiseTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseScanRowwiseTaskTcb(this);
    else
      scanTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseScanTaskTcb(this);
  }

  if ((hbaseTdb.listOfGetRows()) || ((hbaseTdb.keySubsetGen()) && (hbaseTdb.uniqueKeyInfo())) ||
      (hbaseTdb.keyMDAMGen())) {
    if (hbaseTdb.sqHbaseTable())
      getSQTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseGetSQTaskTcb(this, FALSE);
    else if (hbaseTdb.rowwiseFormat())
      getRowwiseTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseGetRowwiseTaskTcb(this);
    else
      getTaskTcb_ = new (getGlobals()->getDefaultHeap()) ExHbaseGetTaskTcb(this);
  }

  if (hbaseTdb.sqHbaseTable()) {
    scanTask_ = scanSQTaskTcb_;
    getTask_ = getSQTaskTcb_;
  } else if (hbaseTdb.rowwiseFormat()) {
    scanTask_ = scanRowwiseTaskTcb_;
    getTask_ = getRowwiseTaskTcb_;
  } else {
    scanTask_ = scanTaskTcb_;
    getTask_ = getTaskTcb_;
  }

  if (hbaseAccessTdb.validateDDL()) {
    // cache the address of the ObjectEpochCacheEntry into the ExDDLValidator object
    // note the assumption that it does not move once it is created!
    StatsGlobals *statsGlobals = getGlobals()->getStatsGlobals();
    ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
    ObjectEpochCacheEntryName oecName(NULL, hbaseAccessTdb.getBaseTableName(), COM_BASE_TABLE_OBJECT, 0);
    ObjectEpochCacheEntry *oecEntry = NULL;
    ObjectEpochCacheKey key(hbaseAccessTdb.getBaseTableName(), oecName.getHash(), COM_BASE_TABLE_OBJECT);

    short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */, key);
    if (retcode == ObjectEpochCache::OEC_ERROR_OK) {
      // oecEntry points to the ObjectEpochCacheEntry
      ddlValidator_.setValidationInfo(oecEntry);
    }
  }
}

ExWorkProcRetcode ExHbaseAccessSelectTcb::work() {
  int retcode = 0;
  short rc = 0;

  while (!qparent_.down->isEmpty()) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
    if ((pentry_down->downState.request == ex_queue::GET_NOMORE) && (step_ != DONE)) {
      step_ = SELECT_CLOSE_NO_ERROR;  // DONE;
    }

    switch (step_) {
      case NOT_STARTED: {
        matches_ = 0;

        if ((pentry_down->downState.request == ex_queue::GET_N) && (pentry_down->downState.requestValue == matches_)) {
          step_ = DONE;
          break;
        }

        step_ = SELECT_INIT;
      } break;

      case SELECT_INIT: {
        retcode = ehi_->init(getHbaseAccessStats());
        if (setupError(retcode, "ExpHbaseInterface::init")) {
          step_ = HANDLE_ERROR;
          break;
        }

        // set up a DDL Validator to monitor for DDL changes
        if (ddlValidator_.validatingDDL()) {
          ehi_->setDDLValidator(&ddlValidator_);
        }

        if (hbaseAccessTdb().listOfScanRows()) hbaseAccessTdb().listOfScanRows()->position();

        if (hbaseAccessTdb().listOfGetRows()) hbaseAccessTdb().listOfGetRows()->position();

        if (scanTask_) scanTask_->init();

        if (getTask_) getTask_->init();

        step_ = PROCESS_PRECONDITION;
      } break;

      case PROCESS_PRECONDITION: {
        rc = evalScanPreCondExpr();
        if (rc == -1)
          step_ = HANDLE_ERROR;
        else if (rc == 0)
          step_ = SELECT_CLOSE;
        else
          step_ = SETUP_SCAN;
      } break;

      case SETUP_SCAN: {
        if ((!scanTask_) || (!hbaseAccessTdb().listOfScanRows())) {
          step_ = SETUP_GET;
          break;
        }

        hsr_ = (ComTdbHbaseAccess::HbaseScanRows *)hbaseAccessTdb().listOfScanRows()->getCurr();

        retcode = setupSubsetRowIdsAndCols(hsr_);
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = PROCESS_SCAN;
      } break;

      case PROCESS_SCAN:
      case PROCESS_SCAN_KEY: {
        rc = 0;
        retcode = scanTask_->work(rc);

        if (retcode == 1)
          return rc;
        else if (retcode < 0)
          step_ = HANDLE_ERROR;
        else if (step_ == PROCESS_SCAN_KEY)
          step_ = SETUP_GET_KEY;
        else
          step_ = NEXT_SCAN;
      } break;

      case NEXT_SCAN: {
        hbaseAccessTdb().listOfScanRows()->advance();

        if (!hbaseAccessTdb().listOfScanRows()->atEnd()) {
          step_ = SETUP_SCAN;
          break;
        }

        step_ = SETUP_GET;
      } break;

      case SETUP_GET: {
        if ((!getTask_) || (!hbaseAccessTdb().listOfGetRows())) {
          step_ = SETUP_SCAN_KEY;
          break;
        }

        hgr_ = (ComTdbHbaseAccess::HbaseGetRows *)hbaseAccessTdb().listOfGetRows()->getCurr();

        retcode = setupUniqueRowIdsAndCols(hgr_);
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = PROCESS_GET;
      } break;

      case PROCESS_GET:
      case PROCESS_GET_KEY: {
        rc = 0;
        retcode = getTask_->work(rc);

        if (retcode == 1)
          return rc;
        else if (retcode < 0)
          step_ = HANDLE_ERROR;
        else if (step_ == PROCESS_GET_KEY)
          step_ = SELECT_CLOSE;
        else
          step_ = NEXT_GET;
      } break;

      case NEXT_GET: {
        hbaseAccessTdb().listOfGetRows()->advance();

        if (!hbaseAccessTdb().listOfGetRows()->atEnd()) {
          step_ = SETUP_GET;
          break;
        }

        step_ = SETUP_SCAN_KEY;
      } break;

      case SETUP_SCAN_KEY: {
        if (!hbaseAccessTdb().keySubsetGen()) {
          step_ = SELECT_CLOSE;
          break;
        }

        if (hbaseAccessTdb().uniqueKeyInfo()) {
          step_ = SETUP_GET_KEY;
          break;
        }

        retcode = setupSubsetKeysAndCols();
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = PROCESS_SCAN_KEY;
      } break;

      case SETUP_GET_KEY: {
        if ((!getTask_) || ((hbaseAccessTdb().keySubsetGen()) && (NOT hbaseAccessTdb().uniqueKeyInfo()))) {
          step_ = SELECT_CLOSE;
          break;
        }

        retcode = setupUniqueKeyAndCols(TRUE);
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        step_ = PROCESS_GET_KEY;
      } break;

      case SELECT_CLOSE:
      case SELECT_CLOSE_NO_ERROR: {
        retcode = ehi_->close();

        if (step_ == SELECT_CLOSE) {
          if (setupError(retcode, "ExpHbaseInterface::close")) {
            step_ = HANDLE_ERROR_NO_CLOSE;
            break;
          }
        }

        step_ = DONE;
      } break;

      case HANDLE_ERROR:
      case HANDLE_ERROR_NO_CLOSE: {
        ehi_->setDDLValidator(NULL);
        if (handleError(rc)) return rc;

        if (step_ == HANDLE_ERROR) retcode = ehi_->close();

        step_ = DONE;
      } break;

      case DONE: {
        ehi_->setDDLValidator(NULL);
        if (handleDone(rc)) return rc;

        if (scanTask_) scanTask_->init();

        if (getTask_) getTask_->init();

        step_ = NOT_STARTED;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}

ExHbaseAccessMdamSelectTcb::ExHbaseAccessMdamSelectTcb(const ExHbaseAccessTdb &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessSelectTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {}

ExWorkProcRetcode ExHbaseAccessMdamSelectTcb::work() {
  int retcode = 0;
  short rc = 0;

  while (!qparent_.down->isEmpty()) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
    if ((pentry_down->downState.request == ex_queue::GET_NOMORE) && (step_ != DONE)) {
      step_ = SELECT_CLOSE_NO_ERROR;
    }

    switch (step_) {
      case NOT_STARTED: {
        matches_ = 0;
        matchesBeforeFetch_ = 0;

        if ((pentry_down->downState.request == ex_queue::GET_N) && (pentry_down->downState.requestValue == matches_)) {
          step_ = DONE;
          break;
        }

        step_ = SELECT_INIT;
      } break;

      case SELECT_INIT: {
        retcode = ehi_->init(getHbaseAccessStats());
        if (setupError(retcode, "ExpHbaseInterface::init")) {
          step_ = HANDLE_ERROR;
          break;
        }

        fixObjName4PartTbl(hbaseAccessTdb().getTableName(), hbaseAccessTdb().getDataUIDName(),
                           hbaseAccessTdb().replaceNameByUID());

        step_ = PROCESS_PRECONDITION;
      } break;

      case PROCESS_PRECONDITION: {
        rc = evalScanPreCondExpr();
        if (rc == -1)
          step_ = HANDLE_ERROR;
        else if (rc == 0)
          step_ = SELECT_CLOSE;
        else
          step_ = INIT_NEXT_KEY_RANGE;
      } break;

      case INIT_NEXT_KEY_RANGE: {
        retcode = initNextKeyRange(pool_, pentry_down->getAtp());
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        setupListOfColNames(hbaseAccessTdb().listOfFetchedColNames(), columns_);

        fetchRangeHadRows_ = TRUE;

        step_ = GET_NEXT_KEY_RANGE;
      } break;

      case GET_NEXT_KEY_RANGE: {
        keyRangeEx::getNextKeyRangeReturnType keyRangeStatus = setupSubsetKeys(fetchRangeHadRows_);

        fetchRangeHadRows_ = FALSE;

        if (keyRangeStatus == keyRangeEx::EXPRESSION_ERROR) {
          step_ = HANDLE_ERROR;
          break;
        }

        if (keyRangeStatus == keyRangeEx::NO_MORE_RANGES) {
          step_ = SELECT_CLOSE;
          break;
        }

        if (keyRangeStatus == keyRangeEx::PROBE_RANGE)
          step_ = PROCESS_PROBE_RANGE;
        else if (keyRangeStatus == keyRangeEx::FETCH_RANGE) {
          matchesBeforeFetch_ = matches_;
          step_ = PROCESS_FETCH_RANGE;
        } else
          step_ = HANDLE_ERROR;
      } break;

      case PROCESS_PROBE_RANGE: {
        char *keyData = NULL;
        retcode = scanSQTaskTcb_->getProbeResult(keyData);
        if (retcode == -1) {
          step_ = HANDLE_ERROR;
          break;
        }

        if (retcode == 1)  // no rows found
        {
          keyExeExpr()->reportProbeResult(0);
          step_ = GET_NEXT_KEY_RANGE;
          break;
        }

        // pass the key value to the mdam generator
        keyExeExpr()->reportProbeResult(keyData);
        step_ = GET_NEXT_KEY_RANGE;
      } break;

      case PROCESS_FETCH_RANGE: {
        rc = 0;
        retcode = scanSQTaskTcb_->work(rc);

        if (retcode == 1)
          return rc;
        else if (retcode < 0)
          step_ = HANDLE_ERROR;
        else {
          if ((pentry_down->downState.request == ex_queue::GET_N) &&
              (pentry_down->downState.requestValue == matches_)) {
            step_ = SELECT_CLOSE;
            break;
          }

          if (matches_ > matchesBeforeFetch_) fetchRangeHadRows_ = TRUE;

          step_ = GET_NEXT_KEY_RANGE;
        }
      } break;

      case SELECT_CLOSE:
      case SELECT_CLOSE_NO_ERROR: {
        retcode = ehi_->close();
        if (step_ == SELECT_CLOSE) {
          if (setupError(retcode, "ExpHbaseInterface::close")) {
            step_ = HANDLE_ERROR_NO_CLOSE;
            break;
          }
        }

        step_ = DONE;
      } break;

      case HANDLE_ERROR:
      case HANDLE_ERROR_NO_CLOSE: {
        if (handleError(rc)) return rc;

        if (step_ == HANDLE_ERROR) retcode = ehi_->close();

        step_ = DONE;
      } break;

      case DONE: {
        if (handleDone(rc)) return rc;

        step_ = NOT_STARTED;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}

ExHbaseCoProcAggrTcb::ExHbaseCoProcAggrTcb(const ComTdbHbaseCoProcAggr &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {}

ExWorkProcRetcode ExHbaseCoProcAggrTcb::work() {
  int retcode = 0;
  short rc = 0;

  long transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId) {
    if (curTransId >= 0) {
      long count = 0;
      double cost = 0;
      for (int idx = 0; idx < 30; idx++) {
        count += callCount[idx];
        cost += sumCost[idx];
      }
      memset(sumCost, 0, sizeof(sumCost));
      memset(callCount, 0, sizeof(callCount));
      if (recordJniAll >= 0 && cost >= recordJniAll)
        QRWARN("ExHbaseSelect summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
    }
    curTransId = transId;
  }
  while (!qparent_.down->isEmpty()) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
    if ((pentry_down->downState.request == ex_queue::GET_NOMORE) && (step_ != DONE)) {
      //	  step_ = SELECT_CLOSE_NO_ERROR;
    }

    switch (step_) {
      case NOT_STARTED: {
        matches_ = 0;

        step_ = COPROC_INIT;
      } break;

      case COPROC_INIT: {
        retcode = ehi_->init(getHbaseAccessStats());
        if (setupError(retcode, "ExpHbaseInterface::init")) {
          step_ = HANDLE_ERROR;
          break;
        }

        fixObjName4PartTbl(hbaseAccessTdb().getTableName(), hbaseAccessTdb().getDataUIDName(),
                           hbaseAccessTdb().replaceNameByUID());

        hbaseAccessTdb().listOfFetchedColNames()->position();
        hbaseAccessTdb().listOfAggrTypes()->position();

        if (hbaseAccessTdb().listOfScanRows()) hbaseAccessTdb().listOfScanRows()->position();

        aggrIdx_ = 0;
        if (costThreshold != NULL) recordCostTh_ = atoi(costThreshold);
        step_ = COPROC_EVAL;
      } break;

      case COPROC_EVAL: {
        int aggrType = *(short *)hbaseAccessTdb().listOfAggrTypes()->getCurr();
        char *col = (char *)hbaseAccessTdb().listOfFetchedColNames()->getCurr();
        long time1 = JULIANTIMESTAMP();

        Text startKey;
        Text endKey;
        Text aggrVal;
        Text colFam;
        Text colName;
        retcode = ExFunctionHbaseColumnLookup::extractColFamilyAndName(col, -1, TRUE, colFam, colName);
        if (retcode) {
          step_ = HANDLE_ERROR;
          break;
        }

        hsr_ = (ComTdbHbaseAccess::HbaseScanRows *)hbaseAccessTdb().listOfScanRows()->getCurr();
        setupSubsetRowIdsAndCols(hsr_);
        int filterForNull = hbaseAccessTdb().getFilterForNull();
        startKey = beginRowId_;
        endKey = endRowId_;
        if (endKey.empty() && (filterForNull == 1)) endKey.assign(2, 0xFE);

        retcode = ehi_->coProcAggr(table_, aggrType,
                                   startKey,  // startRow
                                   endKey,    // stopRow
                                   colFam, colName,
                                   FALSE,  // cacheBlocks
                                   100,    // numCacheRows
                                   hbaseAccessTdb().replSync(), aggrVal, hbaseAccessTdb().getIsolationLevel(),
                                   hbaseAccessTdb().getLockMode());
        if (recordCostTh_ >= 0) {
          long time2 = (JULIANTIMESTAMP() - time1) / 1000;
          if (transId > 0) {
            callCount[25]++;
            sumCost[25] += time2;
          }
          if (time2 >= recordCostTh_)
            QRWARN("ExHbaseCoProcAggrTcb COPROC_EVAL PID %d txID %ld CC %d ATC %.2f TTC %.2f TC %ld  %s", pid, transId,
                   callCount[25], sumCost[25] / (callCount[25] ? callCount[25] : 1), sumCost[25], time2, table_.val);
        }
        if (setupError(retcode, "ExpHbaseInterface::coProcAggr")) {
          step_ = HANDLE_ERROR;
          break;
        }

        ExpTupleDesc *convertTuppTD =
            hbaseAccessTdb().workCriDesc_->getTupleDescriptor(hbaseAccessTdb().convertTuppIndex_);

        Attributes *attr = convertTuppTD->getAttr(aggrIdx_);
        if (!attr) {
          step_ = HANDLE_ERROR;
          break;
        }

        if (attr->getNullFlag()) {
          *(short *)&convertRow_[attr->getNullIndOffset()] = 0;
        }

        str_cpy_all(&convertRow_[attr->getOffset()], aggrVal.data(), aggrVal.length());

        hbaseAccessTdb().listOfAggrTypes()->advance();
        hbaseAccessTdb().listOfFetchedColNames()->advance();
        aggrIdx_++;

        if (hbaseAccessTdb().listOfAggrTypes()->atEnd()) {
          step_ = RETURN_ROW;
          break;
        }
      } break;

      case RETURN_ROW: {
        short rc = 0;
        if (moveRowToUpQueue(convertRow_,
                             hbaseAccessTdb().optLargVar() ? convertRowLen_ : hbaseAccessTdb().convertRowLen(), &rc,
                             FALSE))
          return 1;

        step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        if (handleError(rc)) return rc;

        retcode = ehi_->close();

        step_ = DONE;
      } break;

      case DONE: {
        if (handleDone(rc)) return rc;

        step_ = NOT_STARTED;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}
