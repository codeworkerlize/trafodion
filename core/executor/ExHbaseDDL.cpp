// **********************************************************************

// **********************************************************************

#include "ExHbaseAccess.h"
#include "SQLTypeDefs.h"
#include "cli/ExSqlComp.h"
#include "cli_stdh.h"
#include "comexe/ComTdb.h"
#include "common/Platform.h"
#include "ex_exe_stmt_globals.h"
#include "executor/ExExeUtilCli.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "exp/ExpHbaseInterface.h"
#include "export/ComDiags.h"

ExHbaseAccessDDLTcb::ExHbaseAccessDDLTcb(const ExHbaseAccessTdb &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {}

short ExHbaseAccessDDLTcb::createHbaseTable(HbaseStr &table, const char *cf1, const char *cf2, const char *cf3) {
  return 0;
}

short ExHbaseAccessDDLTcb::dropMDtable(const char *name) { return 0; }

ExWorkProcRetcode ExHbaseAccessDDLTcb::work() {
  short retcode = 0;
  short rc = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  while (1) {
    switch (step_) {
      case NOT_STARTED: {
        matches_ = 0;
        if (hbaseAccessTdb().accessType_ == ComTdbHbaseAccess::CREATE_)
          step_ = CREATE_TABLE;
        else if (hbaseAccessTdb().accessType_ == ComTdbHbaseAccess::DROP_)
          step_ = DROP_TABLE;
        else
          step_ = HANDLE_ERROR;
      } break;

      case CREATE_TABLE: {
        table_.val = hbaseAccessTdb().getTableName();
        table_.len = strlen(hbaseAccessTdb().getTableName());

        Queue *cfnl = hbaseAccessTdb().getColFamNameList();
        cfnl->position();
        HBASE_NAMELIST colFamList(getHeap());
        HbaseStr colFam;
        while (NOT cfnl->atEnd()) {
          char *cfName = (char *)cfnl->getCurr();
          colFam.val = cfName;
          colFam.len = strlen(cfName);

          colFamList.insert(colFam);
          cfnl->advance();
        }

        step_ = DONE;
      } break;

      case DROP_TABLE: {
        step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        if (handleError(rc)) return rc;

        step_ = DONE;
      } break;

      case DONE: {
        if (handleDone(rc)) return rc;

        step_ = NOT_STARTED;

        return WORK_OK;
      } break;

    }  // switch

  }  // while
}

ExHbaseAccessInitMDTcb::ExHbaseAccessInitMDTcb(const ExHbaseAccessTdb &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessDDLTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {}

ExWorkProcRetcode ExHbaseAccessInitMDTcb::work() {
  short retcode = 0;
  short rc = 0;
  int cliRC = 0;

  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ComDiagsArea *da = exeGlob->getDiagsArea();

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

  while (1) {
    switch (step_) {
      case NOT_STARTED: {
        if (!masterGlob) {
          step_ = HANDLE_ERROR;
          break;
        }

        matches_ = 0;
        if (hbaseAccessTdb().accessType_ == ComTdbHbaseAccess::INIT_MD_)
          step_ = INIT_MD;
        else if (hbaseAccessTdb().accessType_ == ComTdbHbaseAccess::DROP_MD_)
          step_ = DROP_MD;
        else
          step_ = HANDLE_ERROR;
      } break;

      case INIT_MD: {
        step_ = UPDATE_MD;
      } break;

      case UPDATE_MD: {
        step_ = DONE;
      } break;

      case DROP_MD: {
        step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        if (handleError(rc)) return rc;

        step_ = DONE;
      } break;

      case DONE: {
        if (handleDone(rc)) return rc;

        step_ = NOT_STARTED;

        return WORK_OK;
      } break;

    }  // switch

  }  // while
}

ExHbaseAccessGetTablesTcb::ExHbaseAccessGetTablesTcb(const ExHbaseAccessTdb &hbaseAccessTdb, ex_globals *glob)
    : ExHbaseAccessTcb(hbaseAccessTdb, glob), step_(NOT_STARTED) {}

ExWorkProcRetcode ExHbaseAccessGetTablesTcb::work() {
  int retcode = 0;
  short rc = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  while (1) {
    switch (step_) {
      case NOT_STARTED: {
        matches_ = 0;
        step_ = GET_TABLE;
      } break;

      case GET_TABLE: {
        retcode = ehi_->getTable(table_);
        if (retcode == HBASE_ACCESS_EOD) {
          step_ = CLOSE;
          break;
        }

        if (setupError(retcode, "ExpHbaseInterface::getTable"))
          step_ = HANDLE_ERROR;
        else
          step_ = RETURN_ROW;
      } break;

      case RETURN_ROW: {
        char *ptr = table_.val;
        short len = (short)table_.len;
        if (moveRowToUpQueue(ptr, len, &rc, TRUE)) {
          return rc;
        }

        step_ = GET_TABLE;
      } break;

      case CLOSE: {
        step_ = DONE;
      } break;

      case HANDLE_ERROR: {
        if (handleError(rc)) return rc;

        step_ = DONE;
      } break;

      case DONE: {
        if (handleDone(rc)) return rc;

        step_ = NOT_STARTED;

        return WORK_OK;
      } break;

    }  // switch

  }  // while
}
