// **********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
// **********************************************************************

#include "common/Platform.h"

#include <algorithm>

#include "executor/ex_stdh.h"
#include "comexe/ComTdb.h"
#include "executor/ex_tcb.h"
#include "ExHbaseAccess.h"
#include "ex_exe_stmt_globals.h"
#include "exp/ExpHbaseInterface.h"
#include "common/NLSConversion.h"
#include "cli/Context.h"

#include "executor/ExStats.h"

static char *costThreshold = getenv("RECORD_TIME_COST_JNI");
static char *costJniAll = getenv("RECORD_TIME_COST_JNI_ALL");
static Int32 recordJniAll = costJniAll ? atoi(costJniAll) : -1;
static pid_t pid = getpid();
static Int32 callCount[40] = {0};
static double sumCost[40] = {0};
static Int64 curTransId = 0;

ExHbaseAccessInsertTcb::ExHbaseAccessInsertTcb(
          const ExHbaseAccessTdb &tdb, 
          ex_globals * glob ) :
  ExHbaseAccessTcb( tdb, glob),
  step_(NOT_STARTED)
{
  insertRowlen_ = 0;
}

ExWorkProcRetcode ExHbaseAccessInsertTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        Int64 cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount)); 
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %ld", pid, curTransId, count, cost); 
      }
      curTransId = transId;
    }
  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = CLOSE_AND_DONE;

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;

	    step_ = INSERT_INIT;
	  }
	  break;
	  
	case INSERT_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

      fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                         hbaseAccessTdb().getDataUIDName(), 
                         hbaseAccessTdb().replaceNameByUID());

            if (costThreshold != NULL)
              recordCostTh_ = atoi(costThreshold);
	    step_ = SETUP_INSERT;
	  }
	  break;

	case SETUP_INSERT:
	  {
	    step_ = EVAL_INSERT_EXPR;
	  }
	  break;

	case EVAL_INSERT_EXPR:
	  {
	    workAtp_->getTupp(hbaseAccessTdb().convertTuppIndex_)
	      .setDataPointer(convertRow_);
	    
	    if (convertExpr())
	      {
		ex_expr::exp_return_type evalRetCode =
		  convertExpr()->eval(pentry_down->getAtp(), workAtp_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
                    step_ = HANDLE_ERROR;
                    break;
		  }
	      }
	  
	    ExpTupleDesc * convertRowTD =
	      hbaseAccessTdb().workCriDesc_->getTupleDescriptor
	      (hbaseAccessTdb().convertTuppIndex_);
	    
	    for (Lng32 i = 0; i <  convertRowTD->numAttrs(); i++)
	      {
		Attributes * attr = convertRowTD->getAttr(i);
		Lng32 len = 0;
		if (attr)
		  {
                    if (attr->getVCIndicatorLength() == sizeof(short))
                      len = *(short*)&convertRow_[attr->getVCLenIndOffset()];
                    else
                     len = *(Lng32*)&convertRow_[attr->getVCLenIndOffset()];
                      
		    switch (i)
		      {
		      case HBASE_ROW_ID_INDEX:
			{
			  insRowId_.assign(&convertRow_[attr->getOffset()], len);
			}
			break;
			
		      case HBASE_COL_FAMILY_INDEX:
			{
			  insColFam_.assign(&convertRow_[attr->getOffset()], len);
			}
			break;
			
		      case HBASE_COL_NAME_INDEX:
			{
			  insColNam_.assign(&convertRow_[attr->getOffset()], len);
			}
			break;
			
		      case HBASE_COL_VALUE_INDEX:
			{
			  insColVal_.assign(&convertRow_[attr->getOffset()], len);
			}
			break;
			
		      case HBASE_COL_TS_INDEX:
			{
			  insColTS_ = (Int64*)&convertRow_[attr->getOffset()];
			}
			break;
			
		      } // switch
		  } // if attr
	      }	// convertExpr
	    
	    step_ = PROCESS_INSERT;
	  }
	  break;

	case PROCESS_INSERT:
	  {
            createDirectRowBuffer(insColFam_, insColNam_, insColVal_);
            HbaseStr rowID;
            rowID.val = (char *)insRowId_.data();
            rowID.len = insRowId_.size();
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = ehi_->insertRow(table_,
				      rowID, 
				      row_,
                                      hbaseAccessTdb().useHbaseXn(),
				      hbaseAccessTdb().replSync(),
                                      hbaseAccessTdb().incrementalBackup(),
                                      hbaseAccessTdb().useRegionXn(),
				      *insColTS_,
                                      FALSE); // AsyncOperations is always FALSE for native HBase

	    if (recordCostTh_ >= 0)
	      {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[0]++;
                    sumCost[0] += time2;
                  }
	        if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseAccessInsertTcb PROCESS_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[0], sumCost[0] / (callCount[0] ? callCount[0] : 1), sumCost[0], time2, table_.val);
	      }
	    if (setupError(retcode, "ExpHbaseInterface::insertRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (getHbaseAccessStats())
	      getHbaseAccessStats()->incUsedRows();

	    matches_++;

	    step_ = INSERT_CLOSE;
	  }
	  break;

	case INSERT_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = ehi_->close();
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                    callCount[1]++;
                    sumCost[1] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessInsertTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[1], sumCost[1] / (callCount[1] ? callCount[1] : 1), sumCost[1], time2, table_.val);
              }
	    if (setupError(retcode, "ExpHbaseInterface::close"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;
	    step_ = CLOSE_AND_DONE;
	  }
	  break;

	case DONE:
        case CLOSE_AND_DONE:
	  {
            if (step_ == CLOSE_AND_DONE) {
               Int64 time1 = JULIANTIMESTAMP();
               ehi_->close();
               if (recordCostTh_ >= 0)
                 { 
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    { 
                      callCount[1]++;
                      sumCost[1] += time2;
                    }
                  if (time2 >= recordCostTh_)
                    QRWARN("ExHbaseAccessInsertTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld %s",
                            pid, transId, callCount[1], sumCost[1] / (callCount[1] ? callCount[1] : 1), sumCost[1], time2, table_.val);
                }
            }
	    if (handleDone(rc, matches_))
	      return rc;
	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch

    } // while

  return WORK_OK;
}

ExHbaseAccessInsertRowwiseTcb::ExHbaseAccessInsertRowwiseTcb(
          const ExHbaseAccessTdb &hbaseAccessTdb, 
          ex_globals * glob ) :
  ExHbaseAccessInsertTcb( hbaseAccessTdb, glob)
{
}

ExWorkProcRetcode ExHbaseAccessInsertRowwiseTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId;
    }

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = CLOSE_AND_DONE;

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;

	    step_ = INSERT_INIT;
	  }
	  break;
	  
	case INSERT_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

      fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                         hbaseAccessTdb().getDataUIDName(), 
                         hbaseAccessTdb().replaceNameByUID());

	    if (costThreshold != NULL)
	      recordCostTh_ = atoi(costThreshold);
	    step_ = SETUP_INSERT;
	  }
	  break;

	case SETUP_INSERT:
	  {
	    step_ = EVAL_INSERT_EXPR;
	  }
	  break;

	case EVAL_INSERT_EXPR:
	  {
	    workAtp_->getTupp(hbaseAccessTdb().convertTuppIndex_)
	      .setDataPointer(convertRow_);
	    
	    if (convertExpr())
	      {
		ex_expr::exp_return_type evalRetCode =
		  convertExpr()->eval(pentry_down->getAtp(), workAtp_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
                    step_ = HANDLE_ERROR;
                    break;
		  }
	      }
	  
	    ExpTupleDesc * convertRowTD =
	      hbaseAccessTdb().workCriDesc_->getTupleDescriptor
	      (hbaseAccessTdb().convertTuppIndex_);

	    for (Lng32 i = 0; i <  convertRowTD->numAttrs(); i++)
	      {
		Attributes * attr = convertRowTD->getAttr(i);
		short len = 0;
		if (attr)
		  {
		    len = *(short*)&convertRow_[attr->getVCLenIndOffset()];

		    switch (i)
		      {
		      case HBASE_ROW_ID_INDEX:
			{
			  insRowId_.assign(&convertRow_[attr->getOffset()], len);
			}
			break;
			
		      case HBASE_COL_DETAILS_INDEX:
			{
			  char * convRow = &convertRow_[attr->getOffset()];

			  retcode = createDirectRowwiseBuffer(convRow);
			}
			break;
			
		      } // switch
		  } // if attr
	      }	// for

	    step_ = PROCESS_INSERT;
	  }
	  break;

	case PROCESS_INSERT:
	  {
	    if (numColsInDirectBuffer() > 0)
	      {
                HbaseStr rowID;
                Int64 time1 = JULIANTIMESTAMP ();

                rowID.val = (char *)insRowId_.data();
                rowID.len = insRowId_.size();
                retcode = ehi_->insertRow(table_,
					  rowID,
					  row_,
                                          hbaseAccessTdb().useHbaseXn(),
					  hbaseAccessTdb().replSync(),
                                          hbaseAccessTdb().incrementalBackup(),
                                          hbaseAccessTdb().useRegionXn(),
					  -1,  //*insColTS_
					  FALSE); // AsyncOperations is always FALSE for native HBase

                if (recordCostTh_ >= 0)
                  {
                    Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                    if (transId > 0)
                      { 
                        callCount[2]++;
                        sumCost[2] += time2;
                      }
                    if (time2 >= recordCostTh_)
                      QRWARN("ExHbaseAccessInsertRowwiseTcb PROCESS_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                             pid, transId, callCount[2], (sumCost[2] / (callCount[2] ? callCount[2] : 1)), sumCost[2], time2, table_.val);
                  }
		if (setupError(retcode, "ExpHbaseInterface::insertRow"))
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		
		if (getHbaseAccessStats())
		  getHbaseAccessStats()->incUsedRows();

		matches_++;
	      }

	    step_ = INSERT_CLOSE;
	  }
	  break;

	case INSERT_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = ehi_->close();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[3]++;
                    sumCost[3] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessInsertRowwiseTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                         pid, transId, callCount[3], (sumCost[3] / (callCount[3] ? callCount[3] : 1)), sumCost[3], time2, table_.val);
              }

	    if (setupError(retcode, "ExpHbaseInterface::close"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;
	    step_ = CLOSE_AND_DONE;
	  }
	  break;

	case DONE:
        case CLOSE_AND_DONE:
	  {
            if (step_ == CLOSE_AND_DONE)
            {
              Int64 time1 = JULIANTIMESTAMP();
              ehi_->close();
              if (recordCostTh_ >= 0)
              {     
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {    
                    callCount[3]++;
                    sumCost[3] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessInsertRowwiseTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                         pid, transId, callCount[3], (sumCost[3] / (callCount[3] ? callCount[3] : 1)), sumCost[3], time2, table_.val);
              }
            }
	    if (handleDone(rc, matches_))
	      return rc;

	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch

    } // while

  return WORK_OK;
}

ExHbaseAccessInsertSQTcb::ExHbaseAccessInsertSQTcb(
          const ExHbaseAccessTdb &tdb, 
          ex_globals * glob ) :
  ExHbaseAccessInsertTcb( tdb, glob)
{
}

ExWorkProcRetcode ExHbaseAccessInsertSQTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId;
    }
  ExMasterStmtGlobals *g = NULL;
  if (hbaseAccessTdb().useTrigger() &&
      getGlobals() != NULL &&
      getGlobals()->castToExExeStmtGlobals() != NULL)
      {
          g = getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      }
  
  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = CLOSE_AND_DONE;

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
            asyncCompleteRetryCount_ = 0;
            asyncOperationTimeout_ = 1;
            asyncOperation_ = hbaseAccessTdb().asyncOperations() && getTransactionIDFromContext();
            if (costThreshold != NULL)
              recordCostTh_ = atoi(costThreshold);
	    step_ = INSERT_INIT;
	  }
	  break;
	  
	case INSERT_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    ehi_->setNoConflictCheckForIndex(hbaseAccessTdb().noConflictCheck());
      fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                         hbaseAccessTdb().getDataUIDName(), 
                         hbaseAccessTdb().replaceNameByUID());

	    step_ = SETUP_INSERT;
	  }
	  break;

	case SETUP_INSERT:
	  {
	    // set up a DDL Validator to monitor for DDL changes
	    if (ddlValidator_.validatingDDL())
	      {
	        ehi_->setDDLValidator(&ddlValidator_);
	      }   

	    rc = evalInsDelPreCondExpr();
	    if (rc == -1)
	      step_ = HANDLE_ERROR;
	    else if (rc == 0)
	      step_ = INSERT_CLOSE;
	    else // expr is true or does not exist
	      step_ = EVAL_INSERT_EXPR;
	  }
	  break;

	case EVAL_INSERT_EXPR:
	  {
	    workAtp_->getTupp(hbaseAccessTdb().convertTuppIndex_)
	      .setDataPointer(convertRow_);
	    
	    if (convertExpr())
	      {
                insertRowlen_ = hbaseAccessTdb().convertRowLen_;
		ex_expr::exp_return_type evalRetCode =
		  convertExpr()->eval(pentry_down->getAtp(), workAtp_,
                                      NULL, -1, &insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

            genAndAssignSyskey(hbaseAccessTdb().convertTuppIndex_, convertRow_);

	    step_ = EVAL_CONSTRAINT;
	  }
	  break;

	case EVAL_CONSTRAINT:
	  {
	    rc = evalConstraintExpr(insConstraintExpr());
	    if (rc == 1)
	      step_ = CREATE_MUTATIONS;
	    else if (rc == 0) 
	      step_ = INSERT_CLOSE;
	    else 
	      step_ = HANDLE_ERROR;
	  }
	  break;

	case CREATE_MUTATIONS:
	  {
	    retcode = createDirectRowBuffer( hbaseAccessTdb().convertTuppIndex_,
                                             convertRow_,
                                             hbaseAccessTdb().listOfUpdatedColNames(),
                                             hbaseAccessTdb().listOfOmittedColNames(),
                                             (hbaseAccessTdb().hbaseSqlIUD() ? FALSE : TRUE));

	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    insColTSval_ = hbaseAccessTdb().hbaseCellTS_;

	    step_ = EVAL_ROWID_EXPR;
	  }
	  break;

	case EVAL_ROWID_EXPR:
	  {
            NABoolean isVC = hbaseAccessTdb().keyInVCformat();
	    if (evalRowIdExpr(isVC) == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    insRowId_.assign(rowId_.val, rowId_.len);

	    if (hbaseAccessTdb().hbaseSqlIUD())
	      step_ = CHECK_AND_INSERT;
	    else
            {
	      step_ = PROCESS_INSERT;
              ehi_->setPutIsUpsert(TRUE);
            }
	  }
	  break;

	case CHECK_AND_INSERT:
	  {
              Int64 time2 =0L, time1 = JULIANTIMESTAMP();

              if (g != NULL && g->getStatement() != NULL &&
                  g->getStatement()->getTriggerExecErr() == TRUE)
                  {
                      retcode = TRIGGER_EXECUTE_EXCEPTION;
                  }
              else
	      {
            HbaseStr rowID;
            rowID.val = (char *)insRowId_.data();
            rowID.len = insRowId_.size();

	    if (hbaseAccessTdb().updateKey())
	      {
		ehi_->settrigger_operation(COM_UPDATE);
	      }
            retcode = ehi_->checkAndInsertRow(table_,
                                              rowID,
	                                      row_,
                                              hbaseAccessTdb().useHbaseXn(),
					      hbaseAccessTdb().replSync(),
                                              hbaseAccessTdb().incrementalBackup(),
                                              hbaseAccessTdb().useRegionXn(),
                                              insColTSval_,
                                              asyncOperation_,
                                              (hbaseAccessTdb().useEncryption() 
                                               ? (char*)&encryptionInfo_ : NULL),
					      (char*)(hbaseAccessTdb().getTriggers(table_.val)),
					      (hbaseAccessTdb().useTrigger() ? getCurExecUtf8sql() : NULL),
					      hbaseAccessTdb().getColIndexOfPK1());
	    ehi_->settrigger_operation(COM_UNKNOWN_IUD);
	      }

	    if (retcode == HBASE_DUP_ROW_ERROR) // row exists, return error
	      {
		ComDiagsArea * diagsArea = NULL;
		ExRaiseSqlError(getHeap(), &diagsArea, 
				(ExeErrorCode)(8102));
		pentry_down->setDiagsArea(diagsArea);
		step_ = HANDLE_ERROR;
		break;
	      }

            if (recordCostTh_ >= 0) {
               time2 = (JULIANTIMESTAMP() - time1) / 1000;
               if (transId > 0)
                 { 
                   callCount[4]++;
                   sumCost[4] += time2;
                 }
             }

	    // execute trigger error, used called function error
	    if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
	      {
                  g->getStatement()->setTriggerExecErr(TRUE);
	      }
	    
	    if (setupError(retcode, "ExpHbaseInterface::checkAndInsertRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
	    if (getHbaseAccessStats())
	      getHbaseAccessStats()->incUsedRows();

	    if (hbaseAccessTdb().returnRow()) {
		step_ = RETURN_ROW;
		if (recordCostTh_ >= 0 && time2 >= recordCostTh_)
		  QRWARN("ExHbaseAccessInsertSQTcb CHECK_AND_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
		         pid, transId, callCount[4], (sumCost[4] / (callCount[4] ? callCount[4] : 1)), sumCost[4], time2, table_.val);
                break;
            }
	    matches_++;

	    if (asyncOperation_) {
                step_ = COMPLETE_ASYNC_INSERT;
                if (recordCostTh_ >= 0 && time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessInsertSQTcb CHECK_AND_INSERT_async PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[4], (sumCost[4] / (callCount[4] ? callCount[4] : 1)), sumCost[4], time2, table_.val);
                return WORK_CALL_AGAIN;
            }
            else {
	        step_ = INSERT_CLOSE;
	        if (recordCostTh_ >= 0 && time2 >= recordCostTh_)
	          QRWARN("ExHbaseAccessInsertSQTcb CHECK_AND_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                         pid, transId, callCount[4], (sumCost[4] / (callCount[4] ? callCount[4] : 1)), sumCost[4], time2, table_.val);
	    }
	  }
	  break;
        case COMPLETE_ASYNC_INSERT:
          {
            if (resultArray_  == NULL)
                resultArray_ = new (getHeap()) NABoolean[1];
            Int32 timeout;
            if (asyncCompleteRetryCount_ < 10)
               timeout = -1;
            else {
               asyncOperationTimeout_ = asyncOperationTimeout_ * 2; 
               timeout = asyncOperationTimeout_;
            }
            retcode = ehi_->completeAsyncOperation(timeout, resultArray_, 1);
            if (retcode == HBASE_RETRY_AGAIN) {
               asyncCompleteRetryCount_++;

               return WORK_CALL_AGAIN;
            }
            asyncCompleteRetryCount_ = 0;
	    if (setupError(retcode, "ExpHbaseInterface::completeAsyncOperation")) {
		step_ = HANDLE_ERROR;
		break;
            }
            if (resultArray_[0] == FALSE) {
		ComDiagsArea * diagsArea = NULL;
		ExRaiseSqlError(getHeap(), &diagsArea, 
				(ExeErrorCode)(8102));
		pentry_down->setDiagsArea(diagsArea);
		step_ = HANDLE_ERROR;
		break;
            }

	    step_ = INSERT_CLOSE;
          }
          break;
	case PROCESS_INSERT:
	  {
            Int64 time2 =0L, time1 = JULIANTIMESTAMP ();

              if (g != NULL && g->getStatement() != NULL &&
                  g->getStatement()->getTriggerExecErr() == TRUE)
                  {
                      retcode = TRIGGER_EXECUTE_EXCEPTION;
                  }
              else
	      {
            HbaseStr rowID;
            rowID.val = (char *)insRowId_.data();
            rowID.len = insRowId_.size();

	    if (hbaseAccessTdb().updateKey())
	      {
		ehi_->settrigger_operation(COM_UPDATE);
	      }

            retcode = ehi_->insertRow(table_,
				      rowID,
				      row_,
                                      hbaseAccessTdb().useHbaseXn(),
				      hbaseAccessTdb().replSync(),
                                      hbaseAccessTdb().incrementalBackup(),
                                      hbaseAccessTdb().useRegionXn(),
				      insColTSval_,
                                      asyncOperation_,
                                      (hbaseAccessTdb().useEncryption() 
                                       ? (char*)&encryptionInfo_ : NULL),
				      (char*)(hbaseAccessTdb().getTriggers(table_.val)),
				      (hbaseAccessTdb().useTrigger() ? getCurExecUtf8sql() : NULL));
	    ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            // execute trigger error, used called function error
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
	      {
                  g->getStatement()->setTriggerExecErr(TRUE);
	      }
	      }
	    
            if (recordCostTh_ >= 0) {
              time2 = (JULIANTIMESTAMP() - time1) / 1000;
              if (transId > 0)
                { 
                  callCount[5]++;
                  sumCost[5] += time2;
                }
            }

	    if (setupError(retcode, "ExpHbaseInterface::insertRow")) {
		step_ = HANDLE_ERROR;
		break;
	    }
	    if (getHbaseAccessStats())
	      getHbaseAccessStats()->incUsedRows();
	    if (hbaseAccessTdb().returnRow()) {
		step_ = RETURN_ROW;
		break;
	    }
	    matches_++;
	    if (asyncOperation_) {
	      if (recordCostTh_ >= 0 && time2 >= recordCostTh_)
	        QRWARN("ExHbaseAccessInsertSQTcb PROCESS_INSERT_async PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                pid, transId, callCount[5], (sumCost[5] / (callCount[5] ? callCount[5] : 1)), sumCost[5], time2, table_.val);
              step_ = COMPLETE_ASYNC_INSERT;
              return WORK_CALL_AGAIN;
            }
            else {
	      step_ = INSERT_CLOSE;
            }
            if (recordCostTh_ >= 0 && time2 >= recordCostTh_)
              QRWARN("ExHbaseAccessInsertSQTcb PROCESS_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                      pid, transId, callCount[5], (sumCost[5] / (callCount[5] ? callCount[5] : 1)), sumCost[5], time2, table_.val);
	  }
	  break;

	case RETURN_ROW:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;
	    
	    if (returnUpdateExpr())
	      {
		ex_queue_entry * up_entry = qparent_.up->getTailEntry();

		// allocate tupps where returned rows will be created
		if (allocateUpEntryTupps(
					 -1,
					 0,
					 hbaseAccessTdb().returnedTuppIndex_,
					 hbaseAccessTdb().returnUpdatedRowLen_,
					 FALSE,
					 &rc))
		  return 1;

		ex_expr::exp_return_type exprRetCode =
		  returnUpdateExpr()->eval(up_entry->getAtp(), workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		
		rc = 0;
		// moveRowToUpQueue also increments matches_
		if (moveRowToUpQueue(&rc))
		  return 1;
	      }
	    else
	      {
		rc = 0;
		// moveRowToUpQueue also increments matches_
		if (moveRowToUpQueue(convertRow_, hbaseAccessTdb().optLargVar() ? convertRowLen_ : hbaseAccessTdb().convertRowLen(), 
				     &rc, FALSE))
		  return 1;
	      }
	    if (asyncOperation_) {
               step_ = COMPLETE_ASYNC_INSERT;
               return WORK_CALL_AGAIN;
            }
            else
	       step_ = INSERT_CLOSE;
	  }
	  break;

	case INSERT_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = ehi_->close();
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[6]++;
                    sumCost[6] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessInsertSQTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[6], (sumCost[6] / (callCount[6] ? callCount[6] : 1)), sumCost[6], time2, table_.val);

              }
	    if (setupError(retcode, "ExpHbaseInterface::close"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;
	    step_ = CLOSE_AND_DONE;
	  }
	  break;

	case DONE:
        case CLOSE_AND_DONE:
	  {
	    ehi_->setDDLValidator(NULL);  

            if (step_ == CLOSE_AND_DONE)
              {
                Int64 time1 = JULIANTIMESTAMP();
                ehi_->close();
                if (recordCostTh_ >= 0)
                {
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    { 
                      callCount[6]++;
                      sumCost[6] += time2;
                    }
                  if (time2 >= recordCostTh_)
                    QRWARN("ExHbaseAccessInsertSQTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                           pid, transId, callCount[6], (sumCost[6] / (callCount[6] ? callCount[6] : 1)), sumCost[6], time2, table_.val);

                }
              }
	    if (NOT hbaseAccessTdb().computeRowsAffected())
	      matches_ = 0;

	    if (handleDone(rc, matches_))
	      return rc;

	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch

    } // while

  return WORK_OK;
}

ExHbaseAccessUpsertVsbbSQTcb::ExHbaseAccessUpsertVsbbSQTcb(
          const ExHbaseAccessTdb &hbaseAccessTdb, 
          ex_globals * glob ) :
  ExHbaseAccessInsertTcb( hbaseAccessTdb, glob)
{
  prevTailIndex_ = 0;

  nextRequest_ = qparent_.down->getHeadIndex();

  numRetries_ = 0;
  rowsInserted_ = 0;
  lastHandledStep_ = NOT_STARTED;
  numRowsInVsbbBuffer_ = 0;
}

ExWorkProcRetcode ExHbaseAccessUpsertVsbbSQTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  ExMasterStmtGlobals *g = getGlobals()->
    castToExExeStmtGlobals()->castToExMasterStmtGlobals();

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId; 
    }

  BOOL isDoDDL = FALSE;
  ContextCli *cliContext = NULL;
  if (GetCliGlobals() != NULL &&
      GetCliGlobals()->currContext() != NULL &&
      GetCliGlobals()->currContext()->execDDLOptions() == TRUE)
      {
          isDoDDL = TRUE;
      }

  while (!qparent_.down->isEmpty())
    {
      nextRequest_ = qparent_.down->getHeadIndex();

      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if ((step_ == HANDLE_ERROR) ||
          (step_ == CLOSE_AND_DONE))
        {
          // move down to error/close case.
        }
      else if (pentry_down->downState.request == ex_queue::GET_NOMORE) {
        step_ = CLOSE_AND_DONE;
        directBufferRowNum_ = 0;
      }
      else if (pentry_down->downState.request == ex_queue::GET_EOD)
        {
          if (currRowNum_ > rowsInserted_)
            {
              step_ = PROCESS_INSERT_AND_CLOSE;
            }
          else
            {
              if (lastHandledStep_ == ALL_DONE)
                matches_=0;
              step_ = ALL_DONE;
            }
        }

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    currRowNum_ = 0;
	    numRetries_ = 0;

	    prevTailIndex_ = 0;
	    lastHandledStep_ = NOT_STARTED;

	    nextRequest_ = qparent_.down->getHeadIndex();

	    rowsInserted_ = 0;
            asyncCompleteRetryCount_ = 0;
            asyncOperationTimeout_ = 1;
            asyncOperation_ = hbaseAccessTdb().asyncOperations() && getTransactionIDFromContext();
            numRowsInVsbbBuffer_ = 0;
	    if (costThreshold != NULL)
	      recordCostTh_ = atoi(costThreshold);
	    step_ = INSERT_INIT;
	  }
	  break;
	  
	case INSERT_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            ehi_->setNoConflictCheckForIndex(hbaseAccessTdb().noConflictCheck());
            fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                               hbaseAccessTdb().getDataUIDName(), 
                               hbaseAccessTdb().replaceNameByUID());

            ExpTupleDesc * rowTD =
              hbaseAccessTdb().workCriDesc_->getTupleDescriptor
              (hbaseAccessTdb().convertTuppIndex_);
            allocateDirectRowBufferForJNI(rowTD->numAttrs(), hbaseAccessTdb().getHbaseRowsetVsbbSize());
            allocateDirectRowIDBufferForJNI(hbaseAccessTdb().getHbaseRowsetVsbbSize());
            if (hbaseAccessTdb().getCanAdjustTrafParams())
              {
                if (hbaseAccessTdb().getWBSize() > 0)
                  {
                    retcode = ehi_->setWriteBufferSize(table_,
                                                       hbaseAccessTdb().getWBSize());
                    if (setupError(retcode, "ExpHbaseInterface::setWriteBufferSize"))
                      {
                        step_ = HANDLE_ERROR;
                        break;
                      }
                  }
                retcode = ehi_->setWriteToWAL(table_,
                                              hbaseAccessTdb().getTrafWriteToWAL());
                if (setupError(retcode, "ExpHbaseInterface::setWriteToWAL"))
                  {
                    step_ = HANDLE_ERROR;
                    break;
                  }
              }

	    step_ = SETUP_INSERT;
	  }
	  break;

	case SETUP_INSERT:
	  {
	    step_ = EVAL_INSERT_EXPR;
	  }
	  break;

	case EVAL_INSERT_EXPR:
	  {
	    workAtp_->getTupp(hbaseAccessTdb().convertTuppIndex_)
	      .setDataPointer(convertRow_);
	    
	    if (convertExpr())
	      {
                insertRowlen_ = hbaseAccessTdb().convertRowLen_;
		ex_expr::exp_return_type evalRetCode =
		  convertExpr()->eval(pentry_down->getAtp(), workAtp_,
                                      NULL, -1, &insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

            genAndAssignSyskey(hbaseAccessTdb().convertTuppIndex_, convertRow_);
	    step_ = EVAL_CONSTRAINT;
	  }
	  break;

	case EVAL_CONSTRAINT:
	  {
	    rc = evalConstraintExpr(insConstraintExpr());
	    if (rc == 1) 
	      step_ = CREATE_MUTATIONS;
	    else if (rc == 0) 
	      step_ = INSERT_CLOSE;
	    else 
	      step_ = HANDLE_ERROR;
	  }
	  break;

	case CREATE_MUTATIONS:
	  {
	    retcode = createDirectRowBuffer(
                 hbaseAccessTdb().convertTuppIndex_,
                 convertRow_,
                 hbaseAccessTdb().listOfUpdatedColNames(),
                 hbaseAccessTdb().listOfOmittedColNames(),
                 TRUE);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    insColTSval_ = -1;
	    step_ = EVAL_ROWID_EXPR;
	  }
	  break;

	case EVAL_ROWID_EXPR:
	  {
            NABoolean isVC = hbaseAccessTdb().keyInVCformat();
	    if (evalRowIdExpr(isVC) == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    copyRowIDToDirectBuffer(rowId_);

	    currRowNum_++;
            
              
            if (!hbaseAccessTdb().returnRow())
              matches_++;
            // if we are returning a row moveRowToUpQueue will increment matches_
            else
              {
                step_ = RETURN_ROW;
                break;
              } 
                        
	    if (currRowNum_ < hbaseAccessTdb().getHbaseRowsetVsbbSize())
	      {
		step_ = DONE;
		break;
	      }

	    step_ = PROCESS_INSERT_AND_CLOSE;
	  }
	  break;

        case RETURN_ROW:
          {
            if (qparent_.up->isFull())
	      return WORK_OK;
	   
	    if (returnUpdateExpr())
	      {
		ex_queue_entry * up_entry = qparent_.up->getTailEntry();

	 	// allocate tupps where returned rows will be created
                if (allocateUpEntryTupps(
                         -1,
                         0,
                         hbaseAccessTdb().returnedTuppIndex_,
                         hbaseAccessTdb().returnUpdatedRowLen_,
                         FALSE,
                         &rc))
                  return rc;

                ex_expr::exp_return_type exprRetCode =
		  returnUpdateExpr()->eval(up_entry->getAtp(), workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		
		rc = 0;
		// moveRowToUpQueue also increments matches_
		if (moveRowToUpQueue(&rc))
		  return rc;
              }
            else
	      {
		rc = 0;
		// moveRowToUpQueue also increments matches_
		if (moveRowToUpQueue(convertRow_, hbaseAccessTdb().optLargVar() ? convertRowLen_ : hbaseAccessTdb().convertRowLen(), 
				     &rc, FALSE))
		  return rc;
	      }

            if (currRowNum_ < hbaseAccessTdb().getHbaseRowsetVsbbSize())
              step_ = DONE;
            else
              step_ = PROCESS_INSERT_AND_CLOSE;

            break;
          }
          break;
	case PROCESS_INSERT_AND_CLOSE:
	  {
            numRowsInVsbbBuffer_ = patchDirectRowBuffers();

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
              {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
              }
            else
              {
                Int64 time1 = JULIANTIMESTAMP ();
                ehi_->setPutIsUpsert(TRUE);
                retcode = ehi_->insertRows(table_,
                                       hbaseAccessTdb().getRowIDLen(),
				       rowIDs_,
                                       rows_,
				       hbaseAccessTdb().useHbaseXn(),
				       hbaseAccessTdb().replSync(),
                                       hbaseAccessTdb().incrementalBackup(),
                                       insColTSval_,
				       asyncOperation_,
                                       (hbaseAccessTdb().useEncryption()
                                        ? (char*)&encryptionInfo_ : NULL),
                                       (isDoDDL ? NULL : (char*)(hbaseAccessTdb().getTriggers(table_.val))),
				       ((hbaseAccessTdb().useTrigger() && (!isDoDDL)) ? getCurExecUtf8sql() : NULL),
                                       hbaseAccessTdb().noConflictCheck());

            if (recordCostTh_ >= 0)
                {
                    Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                    if (transId > 0)
                      { 
                        callCount[7]++;
                        sumCost[7] += time2;
                      }
                    if (time2 >= recordCostTh_ && asyncOperation_)
                        QRWARN("ExHbaseAccessUpsertVsbbSQTcb PROCESS_INSERT_AND_CLOSE_async PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                                pid, transId, callCount[7], (sumCost[7] / (callCount[7] ? callCount[7] : 1)), sumCost[7], time2, table_.val);
                    else if (time2 >= recordCostTh_)
                        QRWARN("ExHbaseAccessUpsertVsbbSQTcb PROCESS_INSERT_AND_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                                pid, transId, callCount[7], (sumCost[7] / (callCount[7] ? callCount[7] : 1)), sumCost[7], time2, table_.val);
                }

            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
            
                }
	    if (setupError(retcode, "ExpHbaseInterface::insertRows")) {
              step_ = HANDLE_ERROR;
              break;
	    }
	    if (getHbaseAccessStats()) {
              getHbaseAccessStats()->incUsedRows((Int64)numRowsInVsbbBuffer_);
	    }
            rowsInserted_ += numRowsInVsbbBuffer_; 
            if (asyncOperation_) {
              lastHandledStep_ = step_;
              step_ = COMPLETE_ASYNC_INSERT;
            }
            else
              step_ = INSERT_CLOSE;
	  }
	  break;
        case COMPLETE_ASYNC_INSERT:
          {
            if (resultArray_  == NULL)
              resultArray_ = new (getHeap()) NABoolean[hbaseAccessTdb().getHbaseRowsetVsbbSize()];
            Int32 timeout;
            if (asyncCompleteRetryCount_ < 10)
              timeout = -1;
            else {
              asyncOperationTimeout_ = asyncOperationTimeout_ * 2;
              timeout = asyncOperationTimeout_;
            }
            Int64 time1 = JULIANTIMESTAMP ();
            retcode = ehi_->completeAsyncOperation(timeout, resultArray_, numRowsInVsbbBuffer_);
            if (recordCostTh_ >= 0)
              {   
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[8]++;
                    sumCost[8] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessUpsertVsbbSQTcb COMPLETE_ASYNC_INSERT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[8], (sumCost[8] / (callCount[8] ? callCount[8] : 1)), sumCost[8], time2, table_.val);
              }
            if (retcode == HBASE_RETRY_AGAIN) {
              asyncCompleteRetryCount_++;
              return WORK_CALL_AGAIN;
            }
            asyncCompleteRetryCount_ = 0;
            if (setupError(retcode, "ExpHbaseInterface::completeAsyncOperation")) {
              step_ = HANDLE_ERROR;
              break;
            }
            for (int i = 0 ; i < numRowsInVsbbBuffer_; i++) {
              if (resultArray_[i] == FALSE) {
                ComDiagsArea * diagsArea = NULL;
                ExRaiseSqlError(getHeap(), &diagsArea,
                                (ExeErrorCode)(8102));
                pentry_down->setDiagsArea(diagsArea);
                step_ = HANDLE_ERROR;
                break;
              }
            }
            if (step_ == HANDLE_ERROR)
              break;
            if (lastHandledStep_ == PROCESS_INSERT_AND_CLOSE)
              step_ = INSERT_CLOSE;
            else
              step_ = ALL_DONE;
          }
          break;
	case INSERT_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = ehi_->close();
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                    callCount[9]++;
                    sumCost[9] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessUpsertVsbbSQTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[9], (sumCost[9] / (callCount[9] ? callCount[9] : 1)), sumCost[9], time2, table_.val);
              }
	    if (setupError(retcode, "ExpHbaseInterface::close"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = ALL_DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;
	    step_ = CLOSE_AND_DONE;
	  }
	  break;

	case DONE:
        case CLOSE_AND_DONE:
	case ALL_DONE:
	  {
            if (step_ == CLOSE_AND_DONE)
            {
              Int64 time1 = JULIANTIMESTAMP();
              ehi_->close();
              if (recordCostTh_ >= 0)
                {
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    {
                      callCount[9]++;
                      sumCost[9] += time2;
                    }
                  if (time2 >= recordCostTh_)
                    QRWARN("ExHbaseAccessUpsertVsbbSQTcb INSERT_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                            pid, transId, callCount[9], (sumCost[9] / (callCount[9] ? callCount[9] : 1)), sumCost[9], time2, table_.val);
                }
            } 
	    if (NOT hbaseAccessTdb().computeRowsAffected())
	      matches_ = 0;

            if ((step_ == DONE) &&
		(qparent_.down->getLength() == 1))
	      {
                
		// only one row in the down queue.

		// Before we send input buffer to hbase, give parent
		// another chance in case there is more input data.
		// If parent doesn't input any more data on second (or
		// later) chances, then process the request.
		if (numRetries_ == 3)
		  {
		    numRetries_ = 0;

		    // Insert the current batch and then done.
		    step_ = PROCESS_INSERT_AND_CLOSE;
		    break;
		  }

		numRetries_++;
		return WORK_CALL_AGAIN;

                break;
              }

	    if (handleDone(rc, (step_ == ALL_DONE  ? matches_ : 0)))
	      return rc;
	    lastHandledStep_ = step_;

	    if (step_ == DONE)
	      step_ = SETUP_INSERT;
	    else
              step_ = NOT_STARTED;
	  }
	  break;

	} // switch

    } // while

  return WORK_OK;
}


// Given the type information available via the argument, return the name of
// the Hive type we use to represent it in the Hive sample table created by
// the bulk load utility.
static const char* TrafToHiveType(Attributes* attrs)
{
  Int64 maxValue = 0;
  Int16 precision = 0;
  Int16 scale = 0;
  Int16 datatype = attrs->getDatatype();

  if (DFS2REC::isInterval(datatype))
  {
    precision = dynamic_cast<SimpleType*>(attrs)->getPrecision();
    scale = dynamic_cast<SimpleType*>(attrs)->getScale();
  }

  switch (datatype)
  {
    case REC_BIN8_SIGNED:
    case REC_BIN8_UNSIGNED:
      return "tinyint";

    case REC_BIN16_SIGNED:
    case REC_BIN16_UNSIGNED:
    case REC_BPINT_UNSIGNED:
      return "smallint";

    case REC_BIN32_SIGNED:
    case REC_BIN32_UNSIGNED:
      return "int";

    case REC_BIN64_SIGNED:
      return "bigint";

    case REC_IEEE_FLOAT32:
      return "float";

    case REC_IEEE_FLOAT64:
      return "double";

    case REC_DECIMAL_UNSIGNED:
    case REC_DECIMAL_LS:
    case REC_DECIMAL_LSE:
      maxValue = (Int64)pow(10, dynamic_cast<SimpleType*>(attrs)->getPrecision());
      break;

    //case REC_NUM_BIG_UNSIGNED: return extFormat? (char *)"NUMERIC":(char *)"REC_NUM_BIG_UNSIGNED";
    //case REC_NUM_BIG_SIGNED: return extFormat? (char *)"NUMERIC":(char *)"REC_NUM_BIG_SIGNED";

    case REC_BYTE_F_ASCII:
    case REC_NCHAR_F_UNICODE:
    case REC_BYTE_V_ASCII:
    case REC_NCHAR_V_UNICODE:
    case REC_BYTE_V_ASCII_LONG:
    case REC_BYTE_V_ANSI:
    case REC_BYTE_V_ANSI_DOUBLE:
    case REC_SBYTE_LOCALE_F:
    case REC_MBYTE_LOCALE_F:
    case REC_MBYTE_F_SJIS:
    case REC_MBYTE_V_SJIS:
      return "string";

    case REC_BINARY_STRING:
    case REC_VARBINARY_STRING:
      return "binary";

    case REC_DATETIME:
      return "timestamp";

    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_MINUTE:
      maxValue = (Int64)pow(10, precision);
      break;

    case REC_INT_SECOND:
      maxValue = (Int64)pow(10, precision + scale);
      break;

    case REC_INT_YEAR_MONTH:
      maxValue = 12 * (Int64)pow(10, precision);
      break;

    case REC_INT_DAY_HOUR:
      maxValue = 24 * (Int64)pow(10, precision);
      break;

    case REC_INT_HOUR_MINUTE:
      maxValue = 60 * (Int64)pow(10, precision);
      break;

    case REC_INT_DAY_MINUTE:
      maxValue = 24 * 60 * (Int64)pow(10, precision);
      break;

    case REC_INT_MINUTE_SECOND:
      maxValue = (Int64)pow(10, precision + 2 + scale);
      break;

    case REC_INT_HOUR_SECOND:
      maxValue = (Int64)pow(10, precision + 4 + scale);
      break;

    case REC_INT_DAY_SECOND:
      maxValue = (Int64)pow(10, precision + 5 + scale);
      break;

    default:

      break;
  }  // switch

  //assert(maxValue > 0);
  if (maxValue < SHRT_MAX)
    return "smallint";
  else if (maxValue <= INT_MAX)
    return "int";
  else
    return "bigint";
}

// UMD (unique UpdMergeDel on Trafodion tables)
ExHbaseUMDtrafUniqueTaskTcb::ExHbaseUMDtrafUniqueTaskTcb
(ExHbaseAccessUMDTcb * tcb)
  :  ExHbaseTaskTcb(tcb)
  , step_(NOT_STARTED)
{
  latestRowTimestamp_ = -1;
  columnToCheck_.val = (char *)(new (tcb->getHeap()) BYTE[MAX_COLNAME_LEN]);
  columnToCheck_.len = MAX_COLNAME_LEN;
  colValToCheck_.val = (char *)(new (tcb->getHeap()) BYTE[tcb->hbaseAccessTdb().getRowIDLen()]);
  colValToCheck_.len = tcb->hbaseAccessTdb().getRowIDLen();
}

void ExHbaseUMDtrafUniqueTaskTcb::init() 
{
  step_ = NOT_STARTED;
}

ExWorkProcRetcode ExHbaseUMDtrafUniqueTaskTcb::work(short &rc)
{
  Lng32 retcode = 0;
  rc = 0;
  char *skvValLen = NULL;
  char *skvValOffset = NULL;
  char *skvBuffer = NULL;


  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId; 
    }

  ExMasterStmtGlobals *g = NULL;
  if (tcb_->hbaseAccessTdb().useTrigger() &&
      tcb_->getGlobals() != NULL &&
      tcb_->getGlobals()->castToExExeStmtGlobals() != NULL)
      {
          g = tcb_->getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      }
  
  while (1)
    {
      ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    rowUpdated_ = FALSE;
            latestRowTimestamp_ = -1;

            asyncCompleteRetryCount_ = 0;
            asyncOperationTimeout_ = 1;
            asyncOperationLocal_ = tcb_->hbaseAccessTdb().asyncOperations() && getTransactionIDFromContext();

	    step_ = SETUP_UMD;
	  }
	  break;

	case SETUP_UMD:
	  {
	    // set up a DDL Validator to monitor for DDL changes
	    if (ddlValidator_.validatingDDL())
	      {
	        tcb_->ehi_->setDDLValidator(&ddlValidator_);
	      }   

	     tcb_->currRowidIdx_ = 0;

             if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
               tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfUpDeldColNames(),
                                         tcb_->deletedColumns_);

	     if (costThreshold != NULL)
	       recordCostTh_ = atoi(costThreshold);
	     step_ = GET_NEXT_ROWID;
	  }
	  break;

	case GET_NEXT_ROWID:
	  {
	    Int64 time1 = JULIANTIMESTAMP ();

	    if (tcb_->currRowidIdx_ ==  tcb_->rowIds_.entries())
	      {
		step_ = GET_CLOSE;
		break;
	      }

            if ((tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_) &&
                (tcb_->hbaseAccessTdb().isIMDeleteDirect()))
              {
                step_ = DELETE_ROW;
                break;
              }

	    if ((tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_) &&
		(tcb_->hbaseAccessTdb().canDoCheckAndUpdel()))
	      {
		if (tcb_->hbaseAccessTdb().hbaseSqlIUD())
		  step_ = CHECK_AND_DELETE_ROW;
		else
		  step_ = DELETE_ROW;
		break;
	      }
	    else if ((tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_) &&
		     (tcb_->hbaseAccessTdb().canDoCheckAndUpdel()))
	      {
		step_ = CREATE_UPDATED_ROW;
		break;
	      }

	    retcode =  tcb_->ehi_->getRowOpen( tcb_->table_,  
					       tcb_->rowIds_[tcb_->currRowidIdx_],
					       tcb_->columns_, -1,
                                               tcb_->hbaseAccessTdb().getNumReplications(),
                                               tcb_->hbaseAccessTdb().getLockMode(),
                                               tcb_->hbaseAccessTdb().getIsolationLevel(),
                                               tcb_->hbaseAccessTdb().getScanMemoryTable(),
                                               true);



	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::getRowOpen"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_ROW;

	    if (recordCostTh_ >= 0)
	      {
	        Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[10]++;
                    sumCost[10] += time2;
                  }
	        if (time2 > recordCostTh_)
	          QRWARN("ExHbaseUMDtrafUniqueTaskTcb GET_NEXT_ROWID PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[10], (sumCost[10] / (callCount[10] ? callCount[10] : 1)), sumCost[10], time2, tcb_->table_.val);
	      }
	  }
	  break;

	case NEXT_ROW:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode =  tcb_->ehi_->nextRow();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[11]++;
                    sumCost[11] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafUniqueTaskTcb NEXT_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[11], (sumCost[11] / (callCount[11] ? callCount[11] : 1)), sumCost[11], time2, tcb_->table_.val);
              }
	    if ( (retcode == HBASE_ACCESS_EOD) || (retcode == HBASE_ACCESS_EOR) )
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::MERGE_)
		  {
		    // didn't find the row, cannot update.
		    // evaluate the mergeInsert expr and insert the row.
		    step_ = CREATE_MERGE_INSERTED_ROW;
		    break;
		  }

		tcb_->currRowidIdx_++;
		step_ = GET_NEXT_ROWID;
		break;
	      }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
	      step_ = HANDLE_ERROR;
	    else if ((tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_) &&
		     (! tcb_->scanExpr()) &&
                     (! tcb_->lobDelExpr()) &&
		     (NOT tcb_->hbaseAccessTdb().returnRow()))
	      step_ = DELETE_ROW;
	    else
	      step_ = CREATE_FETCHED_ROW;
	  }
	  break;

	  case CREATE_FETCHED_ROW:
	    {
	    retcode =  tcb_->createSQRowDirect(&latestRowTimestamp_);
            if (retcode == HBASE_ACCESS_NO_ROW)
	    {
	       step_ = NEXT_ROW;
	       break;
	    }
	    if (retcode < 0)
	    {
	        rc = (short)retcode;
	        tcb_->setupError(rc, "createSQRowDirect");
	        step_ = HANDLE_ERROR;
	        break;
	    }
	    if (retcode != HBASE_ACCESS_SUCCESS)
	    {
	        step_ = HANDLE_ERROR;
	        break;
	    }

	    step_ = APPLY_PRED;
	  }
	  break;

	  case APPLY_PRED:
	  {
	    rc =  tcb_->applyPred(tcb_->scanExpr());
	    if (rc == 1)
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
		  step_ = DELETE_ROW;
		else if ((tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::MERGE_) &&
			 (tcb_->mergeUpdScanExpr()))
		  step_ = APPLY_MERGE_UPD_SCAN_PRED;
		else
		  step_ = CREATE_UPDATED_ROW; 
	      }
	    else if (rc == -1)
	      step_ = HANDLE_ERROR;
	    else
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::MERGE_)
		  {
		    // didn't find the row, cannot update.
		    // evaluate the mergeInsert expr and insert the row.
		    step_ = CREATE_MERGE_INSERTED_ROW;
		    break;
		  }

		tcb_->currRowidIdx_++;
		step_ = GET_NEXT_ROWID;
	      }
	  }
	  break;

	  case APPLY_MERGE_UPD_SCAN_PRED:
	  {
	    rc =  tcb_->applyPred(tcb_->mergeUpdScanExpr());
	    if (rc == 1)
	      {
		step_ = CREATE_UPDATED_ROW; 
	      }
	    else if (rc == -1)
	      step_ = HANDLE_ERROR;
	    else
	      {
		tcb_->currRowidIdx_++;
		step_ = GET_NEXT_ROWID;
	      }
	  }
	  break;

	case CREATE_UPDATED_ROW:
	  {
            if (tcb_->hbaseAccessTdb().updateTuppIndex_ > 0)
              tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().updateTuppIndex_)
                .setDataPointer(tcb_->updateRow_);

            rc = tcb_->evalPartQualPreCondExpr();
            if (rc == -1) {
                step_ = HANDLE_ERROR;
                break;
	    } else if (rc == 0) {
                tcb_->raiseError(8640, NULL, NULL, NULL);
                step_ = HANDLE_ERROR;
                break;
            }

	    if ((! tcb_->updateExpr()) &&
                (!tcb_->hbTagExpr()))
	      {
		tcb_->currRowidIdx_++;
		
		step_ = GET_NEXT_ROWID;
		break;
	      }

	    if (tcb_->updateExpr())
	      {
                tcb_->insertRowlen_ = tcb_->hbaseAccessTdb().updateRowLen_;
		ex_expr::exp_return_type evalRetCode =
		  tcb_->updateExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_,
                                           NULL, -1, &tcb_->insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

	    step_ = EVAL_UPD_CONSTRAINT;
	  }
	  break;

	case EVAL_UPD_CONSTRAINT:
	  {
            rc = 1;
            if (tcb_->updateRow_)
              rc = tcb_->evalConstraintExpr(tcb_->updConstraintExpr(), tcb_->hbaseAccessTdb().updateTuppIndex_,
	        tcb_->updateRow_);
	    if (rc == 1)
	      step_ = CREATE_MUTATIONS;
	    else if (rc == 0) 
	      step_ = GET_CLOSE;
	    else 
	      step_ = HANDLE_ERROR;
	  }
	  break;

	case CREATE_MUTATIONS:
	  {
	    rowUpdated_ = TRUE;

            // Merge can result in inserting rows.
            // Use Number of columns in insert rather number
            // of columns in update if an insert is involved in this tcb
            if (tcb_->hbaseAccessTdb().getAccessType() 
                  == ComTdbHbaseAccess::MERGE_)
            {
              ExpTupleDesc * rowTD = NULL;
              if (tcb_->mergeInsertExpr())
                {
                  rowTD = tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
                    (tcb_->hbaseAccessTdb().mergeInsertTuppIndex_);
                }
              else
                {
                  rowTD = tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
                    (tcb_->hbaseAccessTdb().updateTuppIndex_);
                }
                
               if (rowTD->numAttrs() > 0)
                  tcb_->allocateDirectRowBufferForJNI(rowTD->numAttrs());
            } 


            retcode = 0;
            if ((tcb_->hbaseAccessTdb().updateTuppIndex_ > 0) &&
                (tcb_->updateRow_))
              retcode = tcb_->createDirectRowBuffer( 
                   tcb_->hbaseAccessTdb().updateTuppIndex_,
                   tcb_->updateRow_, 
                   tcb_->hbaseAccessTdb().listOfUpdatedColNames(),
                   tcb_->hbaseAccessTdb().listOfOmittedColNames(),
                   TRUE);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (tcb_->hbaseAccessTdb().canDoCheckAndUpdel())
	      step_ = CHECK_AND_UPDATE_ROW;
	    else
	      step_ = UPDATE_ROW;
	  }
	  break;

	case CREATE_MERGE_INSERTED_ROW:
	  {
	    if (! tcb_->mergeInsertExpr())
	      {
		tcb_->currRowidIdx_++;
		
		step_ = GET_NEXT_ROWID;
		break;
	      }

        if (tcb_->insDelPreCondExpr())
        {
          rc = tcb_->evalInsDelPreCondExpr();
          if (rc == -1)
          {
            step_ = HANDLE_ERROR;
            break;
          }
          else if (rc != 1)
          {
            tcb_->currRowidIdx_++;
            step_ = GET_NEXT_ROWID;
            break;
          }
        }

	    tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().mergeInsertTuppIndex_)
	      .setDataPointer(tcb_->mergeInsertRow_);
	    
	    if (tcb_->mergeInsertExpr())
	      {
                tcb_->insertRowlen_ = tcb_->hbaseAccessTdb().mergeInsertRowLen_;
		ex_expr::exp_return_type evalRetCode =
		  tcb_->mergeInsertExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_,
                                                NULL, -1, &tcb_->insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }
	      if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::MERGE_)
	          rowUpdated_ = FALSE;
              step_ = EVAL_INS_CONSTRAINT;
            }
            break;
         case EVAL_INS_CONSTRAINT:
          {
             rc = tcb_->evalConstraintExpr(tcb_->insConstraintExpr());
             if (rc == 0) {
               step_ = GET_CLOSE;
               break;
            }
            else if (rc != 1) {
               step_ = HANDLE_ERROR;
               break;
            }

	    retcode = tcb_->createDirectRowBuffer( tcb_->hbaseAccessTdb().mergeInsertTuppIndex_,
					    tcb_->mergeInsertRow_,
					    tcb_->hbaseAccessTdb().listOfMergedColNames(),
				            tcb_->hbaseAccessTdb().listOfOmittedColNames());
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::MERGE_)
	      step_ = CHECK_AND_INSERT_ROW;
	    else
              step_ = UPDATE_ROW;
	  }
	  break;

	case UPDATE_ROW:
	  {
	    Int64 time1 = JULIANTIMESTAMP ();

            if (tcb_->row_.len == 0)
              {
                step_ = UPDATE_TAG;
                break;
              }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
	      {
		tcb_->ehi_->settrigger_operation(COM_UPDATE);
	      }

            retcode =  tcb_->ehi_->insertRow(tcb_->table_,
                                             tcb_->rowIds_[tcb_->currRowidIdx_],
	                                     tcb_->row_,
					     (tcb_->hbaseAccessTdb().useHbaseXn() ? TRUE : FALSE),
					     tcb_->hbaseAccessTdb().replSync(),
                                             tcb_->hbaseAccessTdb().incrementalBackup(),
					     tcb_->hbaseAccessTdb().useRegionXn(),
                                             tcb_->hbaseAccessTdb().hbaseCellTS_,
                                             asyncOperationLocal_,
                                             (tcb_->hbaseAccessTdb().useEncryption() 
                                              ? (char*)&tcb_->encryptionInfo_ : NULL),
					     (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
					     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql() : NULL));
	    tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);

            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[12]++;
                    sumCost[12] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDtrafUniqueTaskTcb UPDATE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[12], (sumCost[12] / (callCount[12] ? callCount[12] : 1)), sumCost[12], time2, tcb_->table_.val);
              }

            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
            
                }
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    // matches will get incremented during return row.
	    if (NOT tcb_->hbaseAccessTdb().returnRow())
	      tcb_->matches_++;

      if (asyncOperationLocal_)
      {
        step_ = COMPLETE_ASYNC_UPDATE;
        return WORK_CALL_AGAIN;
      }
      else {
        step_ = UPDATE_TAG_AFTER_ROW_UPDATE;
      }
	    
	  }
	  break;

  case COMPLETE_ASYNC_UPDATE:
  {

    if (tcb_->resultArray_ == NULL)
      tcb_->resultArray_ = new (tcb_->getHeap()) NABoolean[1];
    Int32 timeout;
    if (asyncCompleteRetryCount_ < 10)
      timeout = -1;
    else {
      asyncOperationTimeout_ = asyncOperationTimeout_ * 2;
      timeout = asyncOperationTimeout_;
    }
    retcode = tcb_->ehi_->completeAsyncOperation(timeout, tcb_->resultArray_, 1);
    if (retcode == HBASE_RETRY_AGAIN) {
      asyncCompleteRetryCount_++;

      return WORK_CALL_AGAIN;
    }
    asyncCompleteRetryCount_ = 0;
    if (tcb_->setupError(retcode, "ExpHbaseInterface::completeAsyncOperation")) {
      step_ = HANDLE_ERROR;
      break;
    }

    step_ = UPDATE_TAG_AFTER_ROW_UPDATE;
  }
  break;

	case CHECK_AND_UPDATE_ROW:
	  {
            if (tcb_->row_.len == 0)
              {
                step_ = UPDATE_TAG;
                break;
              }

	    rc = tcb_->evalKeyColValExpr(columnToCheck_, colValToCheck_);
	    if (rc == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    Int64 time1 = JULIANTIMESTAMP ();

	    retcode =  tcb_->ehi_->checkAndUpdateRow(tcb_->table_,
                                                     tcb_->rowIds_[tcb_->currRowidIdx_],
						     tcb_->row_,
						     columnToCheck_,
						     colValToCheck_,
                                                     tcb_->hbaseAccessTdb().useHbaseXn(),
						     tcb_->hbaseAccessTdb().replSync(),
                                                     tcb_->hbaseAccessTdb().incrementalBackup(),

                                                     tcb_->hbaseAccessTdb().useRegionXn(),
						     tcb_->hbaseAccessTdb().hbaseCellTS_, //-1, //colTS_
                                                     tcb_->asyncOperation_);

            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }

            if (recordCostTh_ >= 0)
                {
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    { 
                      callCount[13]++;
                      sumCost[13] += time2;
                    }
                  if (time2 > recordCostTh_)
                    QRWARN("ExHbaseUMDtrafUniqueTaskTcb CHECK_AND_UPDATE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                            pid, transId, callCount[13], (sumCost[13] / (callCount[13] ? callCount[13] : 1)), sumCost[13], time2, tcb_->table_.val);
                }
                }

	    if (retcode == HBASE_ROW_NOTFOUND_ERROR)
	      {
		step_ = NEXT_ROW_AFTER_UPDATE;
		break;
	      }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::checkAndUpdateRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    // matches will get incremented during return row.
	    if (NOT tcb_->hbaseAccessTdb().returnRow())
	      tcb_->matches_++;

	    step_ = UPDATE_TAG_AFTER_ROW_UPDATE;
	  }
	  break;

        case UPDATE_TAG:
        case UPDATE_TAG_AFTER_ROW_UPDATE:
          {
            if (tcb_->hbTagExpr() == NULL)
              {
                step_ = NEXT_ROW_AFTER_UPDATE;
                break;
              }

            if (tcb_->hbaseAccessTdb().hbTagTuppIndex_ > 0)
              tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().hbTagTuppIndex_)
                .setDataPointer(tcb_->hbTagRow_);
	    
            ex_expr::exp_return_type evalRetCode =
              tcb_->hbTagExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
            if (evalRetCode == ex_expr::EXPR_ERROR)
              {
                step_ = HANDLE_ERROR;
                break;
              }

            HbaseStr tagRow;
            tagRow.val = tcb_->hbTagRow_;
            tagRow.len = tcb_->hbaseAccessTdb().hbTagRowLen_;
            Int64 time1 = JULIANTIMESTAMP();
	    retcode =  tcb_->ehi_->updateVisibility(tcb_->table_,
                                                    tcb_->rowIds_[tcb_->currRowidIdx_],
                                                    tagRow,
                                                    tcb_->hbaseAccessTdb().useHbaseXn());
            if (recordCostTh_ >= 0)
                { 
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    { 
                      callCount[14]++;
                      sumCost[14] += time2;
                    }
                  if (time2 > recordCostTh_)
                    QRWARN("ExHbaseUMDtrafUniqueTaskTcb UPDATE_TAG PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                            pid, transId, callCount[14], (sumCost[14] / (callCount[14] ? callCount[14] : 1)), sumCost[14], time2, tcb_->table_.val);
                }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::updateVisibility"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            if (step_ == UPDATE_TAG)
              tcb_->matches_++;

            step_ = NEXT_ROW_AFTER_UPDATE;
          }
          break;

	case CHECK_AND_INSERT_ROW:
	  {
	    Text rowIdRow;
	    if (tcb_->mergeInsertRowIdExpr())
	      {
		tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().mergeInsertRowIdTuppIndex_)
		  .setDataPointer(tcb_->rowIdRow_);

		ex_expr::exp_return_type evalRetCode =
		  tcb_->mergeInsertRowIdExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }

		rowIdRow.assign(tcb_->rowIdRow_, tcb_->hbaseAccessTdb().getRowIDLen());
	      }
            HbaseStr rowID;
            if (tcb_->mergeInsertRowIdExpr())
            {
               rowID.val = (char *)rowIdRow.data();
               rowID.len = rowIdRow.size();
            }
            else
            {
               rowID.val = (char *)tcb_->rowIds_[tcb_->currRowidIdx_].val;
               rowID.len = tcb_->rowIds_[tcb_->currRowidIdx_].len;
            }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
                  Int64 time1 = JULIANTIMESTAMP ();

	    retcode =  tcb_->ehi_->checkAndInsertRow(tcb_->table_,
                                                     rowID,
                                                     tcb_->row_,
                                                     tcb_->hbaseAccessTdb().useHbaseXn(),
						     tcb_->hbaseAccessTdb().replSync(),
                                                     tcb_->hbaseAccessTdb().incrementalBackup(),
                                                     tcb_->hbaseAccessTdb().useRegionXn(),
						     tcb_->hbaseAccessTdb().hbaseCellTS_, //-1, // colTS
                                                     tcb_->asyncOperation_,
                                                     (tcb_->hbaseAccessTdb().useEncryption() 
                                                      ? (char*)&tcb_->encryptionInfo_ : NULL),
						     (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
						     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql() : NULL),
						     tcb_->hbaseAccessTdb().getColIndexOfPK1());
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }

            if (recordCostTh_ >= 0)
                {
                    Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                    if (transId > 0)
                    { 
                      callCount[15]++;
                      sumCost[15] += time2;
                    }
                    if (time2 >= recordCostTh_)
                      QRWARN("ExHbaseUMDtrafUniqueTaskTcb CHECK_AND_INSERT_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                              pid, transId, callCount[15], (sumCost[15] / (callCount[15] ? callCount[15] : 1)), sumCost[15], time2, tcb_->table_.val);
                }
                }

	    if (retcode == HBASE_DUP_ROW_ERROR)
	      {
		ComDiagsArea * diagsArea = NULL;
		ExRaiseSqlError(tcb_->getHeap(), &diagsArea, 
				(ExeErrorCode)(8102));
		pentry_down->setDiagsArea(diagsArea);
		step_ = HANDLE_ERROR;
		break;
	      }
	    else if (tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    // matches will get incremented during return row.
	    if (NOT tcb_->hbaseAccessTdb().returnRow())
	      tcb_->matches_++;

	    step_ = NEXT_ROW_AFTER_UPDATE;
	  }
	  break;

	case NEXT_ROW_AFTER_UPDATE:
	  {
	    tcb_->currRowidIdx_++;
	    
	    if (tcb_->hbaseAccessTdb().returnRow())
	      {
		step_ = EVAL_RETURN_ROW_EXPRS;
		break;
	      }

	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case DELETE_ROW:
	  {
            Int64 time1 = JULIANTIMESTAMP ();

            rc = tcb_->evalPartQualPreCondExpr();
            if (rc == -1) {
                step_ = HANDLE_ERROR;
                break;
	    } else if (rc == 0) {
                tcb_->raiseError(8640, NULL, NULL, NULL);
                step_ = HANDLE_ERROR;
                break;
            }
            
            rc = tcb_->evalInsDelPreCondExpr();
	    if (rc == -1) {
                step_ = HANDLE_ERROR;
                break;
	    }
            if (rc == 0) { // No need to delete
               tcb_->currRowidIdx_++;
               step_ = GET_NEXT_ROWID;
               break;
	    }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    tcb_->ehi_->settrigger_operation(COM_DELETE);      

            retcode =  tcb_->ehi_->deleteRow(tcb_->table_,
                                             tcb_->rowIds_[tcb_->currRowidIdx_],
                                             &tcb_->deletedColumns_,
                                             tcb_->hbaseAccessTdb().useHbaseXn(),
					     tcb_->hbaseAccessTdb().replSync(),
                                             tcb_->hbaseAccessTdb().incrementalBackup(),
                                             tcb_->hbaseAccessTdb().useRegionXn(),
                                             latestRowTimestamp_,
                                             asyncOperationLocal_,
                                             (tcb_->hbaseAccessTdb().getComHbaseAccessOptions() 
                                              ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                             (tcb_->hbaseAccessTdb().useEncryption() 
                                              ? (char*)&tcb_->encryptionInfo_ : NULL),
					     (tcb_->hbaseAccessTdb().updateKey()
					      ? NULL :
					      ((char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)))),
					     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql(tcb_->hbaseAccessTdb().withNoReplicate()) : NULL));
            tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[16]++;
                    sumCost[16] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDtrafUniqueTaskTcb DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[16], (sumCost[16] / (callCount[16] ? callCount[16] : 1)), sumCost[16], time2, tcb_->table_.val);
              }
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
                }
            
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::deleteRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            // delete entries from LOB desc table, if needed
            if (tcb_->lobDelExpr())
              {
                ex_expr::exp_return_type exprRetCode =
		  tcb_->lobDelExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
              }

	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    tcb_->currRowidIdx_++;
	    
	    if (tcb_->hbaseAccessTdb().returnRow())
	      {
		step_ = RETURN_ROW;
		break;
	      }

	    tcb_->matches_++;

           if ((tcb_->hbaseAccessTdb().getFirstNRows() > 0) &&
                (tcb_->matches_ == tcb_->hbaseAccessTdb().getFirstNRows()))
              {
		step_ = GET_CLOSE;
                break;
              }

           if (asyncOperationLocal_) 
           {
             step_ = COMPLETE_ASYNC_DELETE;
             return WORK_CALL_AGAIN;
           }
           else {
             step_ = GET_NEXT_ROWID;
           }

	  }
	  break;


    case COMPLETE_ASYNC_DELETE:
    {

      if (tcb_->resultArray_ == NULL)
        tcb_->resultArray_ = new (tcb_->getHeap()) NABoolean[1];
      Int32 timeout;
      if (asyncCompleteRetryCount_ < 10)
        timeout = -1;
      else {
        asyncOperationTimeout_ = asyncOperationTimeout_ * 2;
        timeout = asyncOperationTimeout_;
      }
      retcode = tcb_->ehi_->completeAsyncOperation(timeout, tcb_->resultArray_, 1);
      if (retcode == HBASE_RETRY_AGAIN) {
        asyncCompleteRetryCount_++;

        return WORK_CALL_AGAIN;
      }
      asyncCompleteRetryCount_ = 0;
      if (tcb_->setupError(retcode, "ExpHbaseInterface::completeAsyncOperation")) {
        step_ = HANDLE_ERROR;
        break;
      }

      step_ = GET_NEXT_ROWID;
    }
    break;

	case CHECK_AND_DELETE_ROW:
	  {
            rc = tcb_->evalInsDelPreCondExpr();
	    if (rc == -1) {
                step_ = HANDLE_ERROR;
                break;
	    }
            if (rc == 0) { // donot delete
               tcb_->currRowidIdx_++;
               step_ = GET_NEXT_ROWID;
               break;
	    }
               
	    rc = tcb_->evalKeyColValExpr(columnToCheck_, colValToCheck_);
	    if (rc == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
                  Int64 time1 = JULIANTIMESTAMP ();

	    retcode =  tcb_->ehi_->checkAndDeleteRow(tcb_->table_,
                                                     tcb_->rowIds_[tcb_->currRowidIdx_],
                                                     &tcb_->deletedColumns_,
						     columnToCheck_, 
						     colValToCheck_,
                                                     tcb_->hbaseAccessTdb().useHbaseXn(),
						     tcb_->hbaseAccessTdb().replSync(),
						     tcb_->hbaseAccessTdb().incrementalBackup(),
                                                     tcb_->hbaseAccessTdb().useRegionXn(),
						     tcb_->hbaseAccessTdb().hbaseCellTS_,
                                                     (tcb_->hbaseAccessTdb().getComHbaseAccessOptions() 
                                                      ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                                     (tcb_->hbaseAccessTdb().useEncryption() 
                                                      ? (char*)&tcb_->encryptionInfo_ : NULL)

						     );

            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[17]++;
                    sumCost[17] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafUniqueTaskTcb CHECK_AND_DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[17], (sumCost[17] / (callCount[17] ? callCount[17] : 1)), sumCost[17], time2, tcb_->table_.val);
              }
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }

                }

	    if (retcode == HBASE_ROW_NOTFOUND_ERROR)
	      {
		tcb_->currRowidIdx_++;
		step_ = GET_NEXT_ROWID;
		break;
	      }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::checkAndDeleteRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
	    tcb_->currRowidIdx_++;
	    
	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    if (tcb_->hbaseAccessTdb().returnRow())
	      {
		step_ = RETURN_ROW;
		break;
	      }

	    tcb_->matches_++;

	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case RETURN_ROW:
	  {
	    if (tcb_->qparent_.up->isFull())
	      {
		rc = WORK_OK;
		return 1;
	      }
	    
	    rc = 0;
	    // moveRowToUpQueue also increments matches_
	    if (tcb_->moveRowToUpQueue(tcb_->convertRow_, tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(), 
				       &rc, FALSE))
	      return 1;

	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == tcb_->matches_))
	      {
		step_ = GET_CLOSE;
		break;
	      }

	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case EVAL_RETURN_ROW_EXPRS:
	  {
	    ex_queue_entry * up_entry = tcb_->qparent_.up->getTailEntry();

	    rc = 0;

	    // allocate tupps where returned rows will be created
	    if (tcb_->allocateUpEntryTupps(
					   tcb_->hbaseAccessTdb().returnedFetchedTuppIndex_,
					   tcb_->hbaseAccessTdb().returnFetchedRowLen_,
					   tcb_->hbaseAccessTdb().returnedUpdatedTuppIndex_,
					   tcb_->hbaseAccessTdb().returnUpdatedRowLen_,
					   FALSE,
					   &rc))
	      return 1;
	    
	    ex_expr::exp_return_type exprRetCode;

	    char * fetchedDataPtr = NULL;
	    char * updatedDataPtr = NULL;
	    char * mergeIUDIndicatorDataPtr = NULL;
	    if (tcb_->returnFetchExpr())
	      {
		exprRetCode =
		  tcb_->returnFetchExpr()->eval(up_entry->getAtp(), tcb_->workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		fetchedDataPtr = up_entry->getAtp()->getTupp(tcb_->hbaseAccessTdb().returnedFetchedTuppIndex_).getDataPointer();
		
	      }
	    if (tcb_->hbaseAccessTdb().mergeIUDIndicatorTuppIndex_ > 0)
	      mergeIUDIndicatorDataPtr = 
		tcb_->workAtp_->
		getTupp(tcb_->hbaseAccessTdb().mergeIUDIndicatorTuppIndex_).
		getDataPointer();
	    
	    if (rowUpdated_)
	      {
		if (tcb_->returnUpdateExpr())
		  {
		    if (mergeIUDIndicatorDataPtr)
		      *mergeIUDIndicatorDataPtr = 'U';
		    exprRetCode =
		      tcb_->returnUpdateExpr()->eval(up_entry->getAtp(), tcb_->workAtp_);
		    if (exprRetCode == ex_expr::EXPR_ERROR)
		      {
			step_ = HANDLE_ERROR;
			break;
		      }
		    updatedDataPtr = 
		      up_entry->getAtp()->getTupp(tcb_->hbaseAccessTdb().returnedUpdatedTuppIndex_).getDataPointer();
		  }
	      }
	    else
	      {
		if (mergeIUDIndicatorDataPtr)
		  *mergeIUDIndicatorDataPtr = 'I';
		if (tcb_->returnMergeInsertExpr())
		  {
		    exprRetCode =
		      tcb_->returnMergeInsertExpr()->eval(up_entry->getAtp(), tcb_->workAtp_);
		    if (exprRetCode == ex_expr::EXPR_ERROR)
		      {
			step_ = HANDLE_ERROR;
			break;
		      }
		    updatedDataPtr = 
		      up_entry->getAtp()->getTupp(tcb_->hbaseAccessTdb().returnedUpdatedTuppIndex_).getDataPointer();
		  }
	      }

	    step_ = RETURN_UPDATED_ROWS;
	  }
	  break;

	case RETURN_UPDATED_ROWS:
	  {
	    rc = 0;
	    // moveRowToUpQueue also increments matches_
	    if (tcb_->moveRowToUpQueue(&rc))
	      return 1;

	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == tcb_->matches_))
	      {
		step_ = GET_CLOSE;
		break;
	      }

	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case GET_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP();
	    retcode =  tcb_->ehi_->getClose();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                    callCount[18]++;
                    sumCost[18] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafUniqueTaskTcb GET_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[18], (sumCost[18] / (callCount[18] ? callCount[18] : 1)), sumCost[18], time2, tcb_->table_.val);
              }
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::getClose"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    tcb_->ehi_->setDDLValidator(NULL);
	    step_ = NOT_STARTED;
      return WORK_BAD_ERROR;
	  }
	  break;

	case DONE:
	  {
	    tcb_->ehi_->setDDLValidator(NULL);
	    step_ = NOT_STARTED;
	    return 0;
	  }
	  break;

	}// switch

    } // while

}

// UMD (unique UpdMergeDel on hbase tables. Well, Merge not supported yet)
ExHbaseUMDnativeUniqueTaskTcb::ExHbaseUMDnativeUniqueTaskTcb
(ExHbaseAccessUMDTcb * tcb)
  :  ExHbaseUMDtrafUniqueTaskTcb(tcb)
  , step_(NOT_STARTED)
{
}

void ExHbaseUMDnativeUniqueTaskTcb::init() 
{
  step_ = NOT_STARTED;
}

ExWorkProcRetcode ExHbaseUMDnativeUniqueTaskTcb::work(short &rc)
{
  Lng32 retcode = 0;
  rc = 0;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId; 
    }

  ExMasterStmtGlobals *g = NULL;
  if (tcb_->hbaseAccessTdb().useTrigger() &&
      tcb_->getGlobals() != NULL &&
      tcb_->getGlobals()->castToExExeStmtGlobals() != NULL)
      {
          g = tcb_->getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      }
  
  while (1)
    {
      ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    rowUpdated_ = FALSE;

	    step_ = SETUP_UMD;
	  }
	  break;

	case SETUP_UMD:
	  {
	     tcb_->currRowidIdx_ = 0;

	     tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfDeletedColNames(),
				       tcb_->deletedColumns_);

	     tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfFetchedColNames(),
				       tcb_->columns_);
	     if (costThreshold != NULL)
	       recordCostTh_ = atoi(costThreshold);
	     step_ = GET_NEXT_ROWID;
	  }
	  break;

	case GET_NEXT_ROWID:
	  {
	    if (tcb_->currRowidIdx_ ==  tcb_->rowIds_.entries())
	      {
		step_ = GET_CLOSE;
		break;
	      }

	    // retrieve columns to be deleted. If none of the columns exist, then
	    // this row cannot be deleted.
	    // But if there is a scan expr, then we need to also retrieve the columns used
	    // in the pred. Add those.
	    LIST(HbaseStr) columns(tcb_->getHeap());
	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
	      {
		columns = tcb_->deletedColumns_;
		if (tcb_->scanExpr())
		  {
		    // retrieve all columns if none is specified.
		    if (tcb_->columns_.entries() == 0)
		      columns.clear();
		    else
		      // append retrieved columns to deleted columns.
		      columns.insert(tcb_->columns_);
		  }
	      }
	    Int64 time1 = JULIANTIMESTAMP ();

	    retcode =  tcb_->ehi_->getRowOpen( tcb_->table_,  
					       tcb_->rowIds_[tcb_->currRowidIdx_],
					       columns, -1, 
                                               tcb_->hbaseAccessTdb().getNumReplications(),
					       tcb_->hbaseAccessTdb().getLockMode(),
                                               tcb_->hbaseAccessTdb().getIsolationLevel(),
                                               tcb_->hbaseAccessTdb().getScanMemoryTable(), true);

	    if (recordCostTh_ >= 0)
	      {
	        Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[19]++;
                    sumCost[19] += time2;
                  }
	        if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseUMDnativeUniqueTaskTcb GET_NEXT_ROWID PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[19], (sumCost[19] / (callCount[19] ? callCount[19] : 1)), sumCost[19], time2, tcb_->table_.val);
	      }
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::getRowOpen"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_ROW;
	  }
	  break;

	case NEXT_ROW:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->nextRow();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[20]++;
                    sumCost[20] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDnativeUniqueTaskTcb NEXT_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[20], (sumCost[20] / (callCount[20] ? callCount[20] : 1)), sumCost[20], time2, tcb_->table_.val);
              }
	    if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR)
	    {
	       step_ = GET_CLOSE;
	       break;
	    }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
	       step_ = HANDLE_ERROR;
	    else
	       step_ = NEXT_CELL;
	  }
	  break;

	case NEXT_CELL:
	  {
	    if (tcb_->colVal_.val == NULL)
	        tcb_->colVal_.val = new (tcb_->getHeap())
	            char[tcb_->hbaseAccessTdb().convertRowLen()];
	    tcb_->colVal_.len = tcb_->hbaseAccessTdb().convertRowLen();
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->nextCell(tcb_->rowId_, tcb_->colFamName_, 
	            tcb_->colName_, tcb_->colVal_, tcb_->colTS_);
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[21]++;
                    sumCost[21] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDnativeUniqueTaskTcb NEXT_CELL PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[21], (sumCost[21] / (callCount[21] ? callCount[21] : 1)), sumCost[21], time2, tcb_->table_.val);
              }
	    if (retcode == HBASE_ACCESS_EOD) 
	    {
	      if ((tcb_->hbaseAccessTdb().getAccessType() 
		    == ComTdbHbaseAccess::DELETE_) && (! tcb_->scanExpr()))
		step_ = DELETE_ROW;
	      else
		step_ = CREATE_FETCHED_ROW;
	      break;
	    }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
	       step_ = HANDLE_ERROR;
	    else
	       step_ = APPEND_CELL_TO_ROW;
	   }
	   break;

	case APPEND_CELL_TO_ROW:
	  {
	    tcb_->copyCell();
	    step_ = NEXT_CELL;
	  }
	  break;

	case CREATE_FETCHED_ROW:
	    {
	    rc =  tcb_->createRowwiseRow();
	    if (rc < 0)
	      {
		if (rc != -1)
		   tcb_->setupError(rc, "createRowwiseRow");
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = APPLY_PRED;
	  }
	  break;

	case APPLY_PRED:
	  {
	    rc =  tcb_->applyPred(tcb_->scanExpr());
	    if (rc == 1)
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
		  step_ = DELETE_ROW;
		else
		  step_ = CREATE_UPDATED_ROW; 
	      }
	    else if (rc == -1)
	      step_ = HANDLE_ERROR;
	    else
	      {
		tcb_->currRowidIdx_++;
		step_ = GET_NEXT_ROWID;
	      }
	  }
	  break;

	case CREATE_UPDATED_ROW:
	  {
	    if (! tcb_->updateExpr())
	      {
		tcb_->currRowidIdx_++;
		
		step_ = GET_NEXT_ROWID;
		break;
	      }

	    tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().updateTuppIndex_)
	      .setDataPointer(tcb_->updateRow_);
	    
	    if (tcb_->updateExpr())
	      {
		ex_expr::exp_return_type evalRetCode =
		  tcb_->updateExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_,
                                           NULL, -1, &tcb_->insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

	    ExpTupleDesc * rowTD =
	      tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
	      (tcb_->hbaseAccessTdb().updateTuppIndex_);
	    
	    Attributes * attr = rowTD->getAttr(0);
 
	    rowUpdated_ = TRUE;
	    retcode = tcb_->createDirectRowwiseBuffer(
			   &tcb_->updateRow_[attr->getOffset()]);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = UPDATE_ROW;
	  }
	  break;

	case DELETE_ROW:
	  {
              if (g != NULL && g->getStatement() != NULL &&
                  g->getStatement()->getTriggerExecErr() == TRUE)
                  {
                      retcode = TRIGGER_EXECUTE_EXCEPTION;
                  }
              else
                  {
	    Int64 time1 = JULIANTIMESTAMP();

	    tcb_->ehi_->settrigger_operation(COM_DELETE);
            retcode =  tcb_->ehi_->deleteRow(tcb_->table_,
                                             tcb_->rowIds_[tcb_->currRowidIdx_],
                                             &tcb_->deletedColumns_,
                                             tcb_->hbaseAccessTdb().useHbaseXn(),
					     tcb_->hbaseAccessTdb().replSync(),
                                             tcb_->hbaseAccessTdb().incrementalBackup(),
                                             tcb_->hbaseAccessTdb().useRegionXn(),
                                             -1 ,
                                             tcb_->asyncOperation_,
                                             (tcb_->hbaseAccessTdb().getComHbaseAccessOptions() 
                                              ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                             (tcb_->hbaseAccessTdb().useEncryption() 
                                              ? (char*)&tcb_->encryptionInfo_ : NULL),
					     (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
					     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql(tcb_->hbaseAccessTdb().withNoReplicate()) : NULL));

            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[22]++;
                    sumCost[22] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDnativeUniqueTaskTcb DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[22], (sumCost[22] / (callCount[22] ? callCount[22] : 1)), sumCost[22], time2, tcb_->table_.val);
              }
            tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }

                  }
              
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::deleteRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
	    tcb_->currRowidIdx_++;
	    
	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    tcb_->matches_++;

	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case UPDATE_ROW:
	  {
	    if (tcb_->numColsInDirectBuffer() > 0)
	      {
                  if (g != NULL && g->getStatement() != NULL &&
                      g->getStatement()->getTriggerExecErr() == TRUE)
                      {
                          retcode = TRIGGER_EXECUTE_EXCEPTION;
                      }
                  else
                  {
                Int64 time1 = JULIANTIMESTAMP ();
                retcode =  tcb_->ehi_->insertRow(tcb_->table_,
                                                 tcb_->rowIds_[tcb_->currRowidIdx_],
                                                 tcb_->row_,
                                                 tcb_->hbaseAccessTdb().useHbaseXn(),
						 tcb_->hbaseAccessTdb().replSync(),
                                                 tcb_->hbaseAccessTdb().incrementalBackup(),
                                                 tcb_->hbaseAccessTdb().useRegionXn(),
                                                 -1, // colTS_
                                                 tcb_->asyncOperation_,
                                                 (tcb_->hbaseAccessTdb().useEncryption() 
                                                  ? (char*)&tcb_->encryptionInfo_ : NULL),
						 (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
						 (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql() : NULL));

                if (recordCostTh_ >= 0)
                  {
                    Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                    if (transId > 0)
                      { 
                        callCount[23]++;
                        sumCost[23] += time2;
                      }
                    if (time2 > recordCostTh_)
                      QRWARN("ExHbaseUMDnativeUniqueTaskTcb UPDATE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                              pid, transId, callCount[23], (sumCost[23] / (callCount[23] ? callCount[23] : 1)), sumCost[23], time2, tcb_->table_.val);
                  }
                if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                    retcode == TRIGGER_PARAMETER_EXCEPTION)
                    {
                        g->getStatement()->setTriggerExecErr(TRUE);
                    }
                  }        

		if ( tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }

		if (tcb_->getHbaseAccessStats())
		  tcb_->getHbaseAccessStats()->incUsedRows();

		tcb_->matches_++;
	      }
          
	    tcb_->currRowidIdx_++;
	    
	    step_ = GET_NEXT_ROWID;
	  }
	  break;

	case GET_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode =  tcb_->ehi_->getClose();
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                     callCount[24]++;
                     sumCost[24] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDnativeUniqueTaskTcb GET_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[24], (sumCost[24] / (callCount[24] ? callCount[24] : 1)), sumCost[24], time2, tcb_->table_.val);
              }
	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::getClose"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    step_ = NOT_STARTED;
	    return -1;
	  }
	  break;

	case DONE:
	  {
	    step_ = NOT_STARTED;
	    return 0;
	  }
	  break;

	}// switch

    } // while

}

ExHbaseUMDtrafSubsetTaskTcb::ExHbaseUMDtrafSubsetTaskTcb
(ExHbaseAccessUMDTcb * tcb)
  :  ExHbaseTaskTcb(tcb)
  , step_(NOT_STARTED)
{
}

void ExHbaseUMDtrafSubsetTaskTcb::init() 
{
  step_ = NOT_STARTED;
}

ExWorkProcRetcode ExHbaseUMDtrafSubsetTaskTcb::work(short &rc)
{
  Lng32 retcode = 0;
  HbaseStr rowID;
  rc = 0;
  Lng32 remainingInBatch = batchSize_;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId; 
    }

  ExMasterStmtGlobals *g = NULL;
  if (tcb_->hbaseAccessTdb().useTrigger() &&
      tcb_->getGlobals() != NULL &&
      tcb_->getGlobals()->castToExExeStmtGlobals() != NULL)
      {
          g = tcb_->getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      }
  
  while (1)
    {
     ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();
 
      switch (step_)
	{
	case NOT_STARTED:
	  {
            // set up a DDL Validator to monitor for DDL changes
            if (ddlValidator_.validatingDDL())
              {
                tcb_->ehi_->setDDLValidator(&ddlValidator_);
              }

            tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfUpDeldColNames(),
                                      tcb_->deletedColumns_);
	    if (costThreshold != NULL)
	      recordCostTh_ = atoi(costThreshold);
	    step_ = SCAN_OPEN;

            tcb_->currRowidIdx_ = 0;
	  }
	  break;

	case SCAN_OPEN:
	  {
            // Bypass scan when beginRowId_ is less than endRowId_
            if (tcb_->compareRowIds() < 0) {
               step_ = DONE;
               break;
            }
            // Pre-fetch is disabled because it interfers with
            // Delete operations
	    Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->scanOpen(tcb_->table_, 
					   tcb_->beginRowId_, tcb_->endRowId_,
					   tcb_->columns_, -1,
					   tcb_->hbaseAccessTdb().readUncommittedScan(),
             tcb_->hbaseAccessTdb().getScanMemoryTable(),
                                           tcb_->hbaseAccessTdb().getLockMode(),
                                           tcb_->hbaseAccessTdb().getIsolationLevel(),
					   TRUE, // as part of IUD, this scan should skip conflict checking
                                           FALSE, // no scan operator
					   tcb_->hbaseAccessTdb().replSync(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScanner(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->numCacheRows(),
					   FALSE, NULL, NULL, NULL,
                                           tcb_->hbaseAccessTdb().getNumReplications(),
	                   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->dopParallelScanner());

	    if (recordCostTh_ >= 0)
	      {
	        Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[25]++;
                    sumCost[25] += time2;
                  }
	        if (time2 > recordCostTh_)
	          QRWARN("ExHbaseUMDtrafSubsetTaskTcb SCAN_OPEN PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[25], (sumCost[25] / (callCount[25] ? callCount[25] : 1)), sumCost[25], time2, tcb_->table_.val);
	      }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_ROW;
	  }
	  break;

	case NEXT_ROW:
	  {
            if (--remainingInBatch <= 0)
              {
                rc = WORK_CALL_AGAIN;
                return 1;
              }

            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->nextRow();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                    callCount[26]++;
                    sumCost[26] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafSubsetTaskTcb NEXT_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[26], (sumCost[26] / (callCount[26] ? callCount[26] : 1)), sumCost[26], time2, tcb_->table_.val);
              }
	    if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR)
	      {
		step_ = SCAN_CLOSE;
		break;
	      }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
	      {
		if ((! tcb_->scanExpr()) &&
		    (NOT tcb_->hbaseAccessTdb().returnRow()))
		  {
		    step_ = DELETE_ROW;
		    break;
		  }
	      }

	      step_ = CREATE_FETCHED_ROW;
	  }
	  break;

	  case CREATE_FETCHED_ROW:
	  {
	    retcode = tcb_->createSQRowDirect();
	    if (retcode == HBASE_ACCESS_NO_ROW)
	    {
	       step_ = NEXT_ROW;
	       break;
	    }
	    if (retcode < 0)
	    {
                rc = (short)retcode;
	        tcb_->setupError(rc, "createSQRowDirect");
	        step_ = HANDLE_ERROR;
	        break;
	    }
	    if (retcode != HBASE_ACCESS_SUCCESS)
	    {
	        step_ = HANDLE_ERROR;
	        break;
	    }
	    step_ = APPLY_PRED;
	  }
	  break;

	  case APPLY_PRED:
	  {
	    rc = tcb_->applyPred(tcb_->scanExpr());
	    if (rc == 1)
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
		  step_ = DELETE_ROW;
		else
		  step_ = CREATE_UPDATED_ROW; 
	      }
	    else if (rc == -1)
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_ROW;
	  }
	  break;

	case CREATE_UPDATED_ROW:
	  {
            if (tcb_->hbaseAccessTdb().updateTuppIndex_ > 0)
              tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().updateTuppIndex_)
                .setDataPointer(tcb_->updateRow_);
	    
	    if (tcb_->updateExpr())
	      {
                tcb_->insertRowlen_ = tcb_->hbaseAccessTdb().updateRowLen_;
		ex_expr::exp_return_type evalRetCode =
		  tcb_->updateExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_,
                                           NULL, -1, &tcb_->insertRowlen_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

	    step_ = EVAL_UPD_CONSTRAINT;
	  }
	  break;

	case EVAL_UPD_CONSTRAINT:
	  {
            rc = 1;
            if (tcb_->updateRow_)
              rc = tcb_->evalConstraintExpr(tcb_->updConstraintExpr(), tcb_->hbaseAccessTdb().updateTuppIndex_,
                    tcb_->updateRow_);
	    if (rc == 1)
	      step_ = CREATE_MUTATIONS;
	    else if (rc == 0)
	      step_ = SCAN_CLOSE; 
	    else // error
	      step_ = HANDLE_ERROR;
	  }
	  break;

	case CREATE_MUTATIONS:
	  {
            // Merge can result in inserting rows
            // Use Number of columns in insert rather number
            // of columns in update if an insert is involved in this tcb
            if (tcb_->hbaseAccessTdb().getAccessType() 
                  == ComTdbHbaseAccess::MERGE_)
            {
              ExpTupleDesc * rowTD = NULL;
              if (tcb_->mergeInsertExpr())
                {
                  rowTD = tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
                    (tcb_->hbaseAccessTdb().mergeInsertTuppIndex_);
                }
              else
                {
                  rowTD = tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
                    (tcb_->hbaseAccessTdb().updateTuppIndex_);
                }
                
               if (rowTD->numAttrs() > 0)
                  tcb_->allocateDirectRowBufferForJNI(rowTD->numAttrs());
            } 

            retcode = 0;
            if ((tcb_->hbaseAccessTdb().updateTuppIndex_ > 0) &&
                (tcb_->updateRow_))
              retcode = tcb_->createDirectRowBuffer(
                   tcb_->hbaseAccessTdb().updateTuppIndex_,
                   tcb_->updateRow_,
                   tcb_->hbaseAccessTdb().listOfUpdatedColNames(),
                   tcb_->hbaseAccessTdb().listOfOmittedColNames(),
                   TRUE);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = UPDATE_ROW;
	  }
	  break;

	case UPDATE_ROW:
	  {
            retcode = tcb_->ehi_->getRowID(rowID);
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
	    {
		step_ = HANDLE_ERROR;
		break;
	    }

            if (tcb_->row_.len == 0)
              {
                step_ = UPDATE_TAG;
                break;
              }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    Int64 time1 = JULIANTIMESTAMP ();
	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
	      {
		tcb_->ehi_->settrigger_operation(COM_UPDATE);
	      }
	    retcode = tcb_->ehi_->insertRow(tcb_->table_,
					    rowID,
					    tcb_->row_,
                                            tcb_->hbaseAccessTdb().useHbaseXn(),
					    tcb_->hbaseAccessTdb().replSync(),
                                            tcb_->hbaseAccessTdb().incrementalBackup(),
                                            tcb_->hbaseAccessTdb().useRegionXn(),
                                            tcb_->hbaseAccessTdb().hbaseCellTS_,
                                            //					    -1, // colTS_
                                            tcb_->asyncOperation_,
                                            (tcb_->hbaseAccessTdb().useEncryption() 
                                             ? (char*)&tcb_->encryptionInfo_ : NULL),
					    (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
					    (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql() : NULL));

	    if (recordCostTh_ >= 0)
	      {
  	        Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[27]++;
                    sumCost[27] += time2;
                  }
	        if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseUMDtrafSubsetTaskTcb UPDATE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[27], (sumCost[27] / (callCount[27] ? callCount[27] : 1)), sumCost[27], time2, tcb_->table_.val);
	      }
	    tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
            
                }
            
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
	    {
		step_ = HANDLE_ERROR;
		break;
	    }

	    if (tcb_->hbaseAccessTdb().returnRow())
	      {
		step_ = EVAL_RETURN_ROW_EXPRS;
		break;
	      }

	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    tcb_->matches_++;

	    step_ = UPDATE_TAG_AFTER_ROW_UPDATE;
	  }
	  break;

        case UPDATE_TAG:
        case UPDATE_TAG_AFTER_ROW_UPDATE:
          {
            if (tcb_->hbTagExpr() == NULL)
              {
                step_ = NEXT_ROW;
                break;
              }

            if (tcb_->hbaseAccessTdb().hbTagTuppIndex_ > 0)
              tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().hbTagTuppIndex_)
                .setDataPointer(tcb_->hbTagRow_);
	    
            ex_expr::exp_return_type evalRetCode =
              tcb_->hbTagExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
            if (evalRetCode == ex_expr::EXPR_ERROR)
              {
                step_ = HANDLE_ERROR;
                break;
              }

            HbaseStr tagRow;
            tagRow.val = tcb_->hbTagRow_;
            tagRow.len = tcb_->hbaseAccessTdb().hbTagRowLen_;
	    retcode =  tcb_->ehi_->updateVisibility(tcb_->table_,
                                              rowID,
                                              tagRow,
                                              tcb_->hbaseAccessTdb().useHbaseXn());

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::updateVisibility"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            if (step_ == UPDATE_TAG)
              tcb_->matches_++;

            step_ = NEXT_ROW;
          }
          break;

	case DELETE_ROW:
	  {
            retcode = tcb_->ehi_->getRowID(rowID);
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
	    {
		step_ = HANDLE_ERROR;
		break;
            }
            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    Int64 time1 = JULIANTIMESTAMP ();
	    tcb_->ehi_->settrigger_operation(COM_DELETE);
	    retcode =  tcb_->ehi_->deleteRow(tcb_->table_,
					     rowID,
					     &tcb_->deletedColumns_,
                                             tcb_->hbaseAccessTdb().useHbaseXn(),
					     tcb_->hbaseAccessTdb().replSync(),
                                             tcb_->hbaseAccessTdb().incrementalBackup(),
                                             tcb_->hbaseAccessTdb().useRegionXn(),
					     -1,
					     tcb_->asyncOperation_,
                                             (tcb_->hbaseAccessTdb().getComHbaseAccessOptions() 
                                              ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                             (tcb_->hbaseAccessTdb().useEncryption() 
                                              ? (char*)&tcb_->encryptionInfo_ : NULL),
					     (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
					     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql(tcb_->hbaseAccessTdb().withNoReplicate()) : NULL));

            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[28]++;
                    sumCost[28] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafSubsetTaskTcb DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[28], (sumCost[28] / (callCount[28] ? callCount[28] : 1)), sumCost[28], time2, tcb_->table_.val);
              }
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
            tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);
                }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::deleteRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
            // delete entries from LOB desc table, if needed
            if (tcb_->lobDelExpr())
              {
                ex_expr::exp_return_type exprRetCode =
		  tcb_->lobDelExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
              }

	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    tcb_->currRowidIdx_++;

	    if (tcb_->hbaseAccessTdb().returnRow())
	      {
		step_ = RETURN_ROW;
		break;
	      }

	    tcb_->matches_++;

            if ((tcb_->hbaseAccessTdb().getFirstNRows() > 0) &&
                (tcb_->matches_ == tcb_->hbaseAccessTdb().getFirstNRows()))
              {
		step_ = SCAN_CLOSE;
                
                break;
              }

	    step_ = NEXT_ROW;
	  }
	  break;

	case RETURN_ROW:
	  {
	    if (tcb_->qparent_.up->isFull())
	      {
		rc = WORK_OK;
		return 1;
	      }
	    
	    rc = 0;
	    // moveRowToUpQueue also increments matches_
	    if (tcb_->moveRowToUpQueue(tcb_->convertRow_, tcb_->hbaseAccessTdb().optLargVar() ? tcb_->convertRowLen_ : tcb_->hbaseAccessTdb().convertRowLen(), 
				       &rc, FALSE))
	      return 1;

	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == tcb_->matches_))
	      {
		step_ = SCAN_CLOSE;
		break;
	      }

            if ((tcb_->hbaseAccessTdb().getFirstNRows() > 0) &&
                (tcb_->currRowidIdx_ >= tcb_->hbaseAccessTdb().getFirstNRows()))
              {
		step_ = SCAN_CLOSE;
                
                break;
              }

	    step_ = NEXT_ROW;
	  }
	  break;

	case EVAL_RETURN_ROW_EXPRS:
	  {
	    ex_queue_entry * up_entry = tcb_->qparent_.up->getTailEntry();

	    rc = 0;

	    // allocate tupps where returned rows will be created
	    if (tcb_->allocateUpEntryTupps(
					   tcb_->hbaseAccessTdb().returnedFetchedTuppIndex_,
					   tcb_->hbaseAccessTdb().returnFetchedRowLen_,
					   tcb_->hbaseAccessTdb().returnedUpdatedTuppIndex_,
					   tcb_->hbaseAccessTdb().returnUpdatedRowLen_,
					   FALSE,
					   &rc))
	      return 1;
	    
	    ex_expr::exp_return_type exprRetCode;

	    char * fetchedDataPtr = NULL;
	    char * updatedDataPtr = NULL;
	    if (tcb_->returnFetchExpr())
	      {
		exprRetCode =
		  tcb_->returnFetchExpr()->eval(up_entry->getAtp(), tcb_->workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		fetchedDataPtr = up_entry->getAtp()->getTupp(tcb_->hbaseAccessTdb().returnedFetchedTuppIndex_).getDataPointer();
		
	      }

	    if (tcb_->returnUpdateExpr())
	      {
		exprRetCode =
		  tcb_->returnUpdateExpr()->eval(up_entry->getAtp(), tcb_->workAtp_);
		if (exprRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		updatedDataPtr = up_entry->getAtp()->getTupp(tcb_->hbaseAccessTdb().returnedUpdatedTuppIndex_).getDataPointer();
	      }

	    step_ = RETURN_UPDATED_ROWS;
	  }
	  break;

	case RETURN_UPDATED_ROWS:
	  {
	    rc = 0;
	    // moveRowToUpQueue also increments matches_
	    if (tcb_->moveRowToUpQueue(&rc))
	      return 1;

	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == tcb_->matches_))
	      {
		step_ = SCAN_CLOSE;
		break;
	      }

	    step_ = NEXT_ROW;
	  }
	  break;

	case SCAN_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->scanClose();
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[29]++;
                    sumCost[29] += time2;
                  }
                if (time2 > recordCostTh_)
                  QRWARN("ExHbaseUMDtrafSubsetTaskTcb DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[29], (sumCost[29] / (callCount[29] ? callCount[29] : 1)), sumCost[29], time2, tcb_->table_.val);
              }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    tcb_->ehi_->setDDLValidator(NULL);
	    step_ = NOT_STARTED;
	    return -1;
	  }
	  break;

	case DONE:
	  {
	    tcb_->ehi_->setDDLValidator(NULL);
	    step_ = NOT_STARTED;
	    return 0;
	  }
	  break;

	}// switch

    } // while

}

ExHbaseUMDnativeSubsetTaskTcb::ExHbaseUMDnativeSubsetTaskTcb
(ExHbaseAccessUMDTcb * tcb)
  :  ExHbaseUMDtrafSubsetTaskTcb(tcb)
  , step_(NOT_STARTED)
{
}

void ExHbaseUMDnativeSubsetTaskTcb::init() 
{
  step_ = NOT_STARTED;
}

ExWorkProcRetcode ExHbaseUMDnativeSubsetTaskTcb::work(short &rc)
{
  Lng32 retcode = 0;
  rc = 0;
  Lng32 remainingInBatch = batchSize_;
  ExMasterStmtGlobals *g = NULL;
  if (tcb_->hbaseAccessTdb().useTrigger() &&
      tcb_->getGlobals() != NULL &&
      tcb_->getGlobals()->castToExExeStmtGlobals() != NULL)
      {
          g = tcb_->getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      }
  
  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId; 
    }

  while (1)
    {
     ex_queue_entry *pentry_down = tcb_->qparent_.down->getHeadEntry();
 
      switch (step_)
	{
	case NOT_STARTED:
	  {
	     tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfDeletedColNames(),
				       tcb_->deletedColumns_);

	     tcb_->setupListOfColNames(tcb_->hbaseAccessTdb().listOfFetchedColNames(),
				       tcb_->columns_);

	     tcb_->currRowidIdx_ = 0;
	     if (costThreshold != NULL)
	       recordCostTh_ = atoi(costThreshold);
             step_ = SCAN_OPEN;
	  }
	  break;

	case SCAN_OPEN:
	  {
	    // retrieve columns to be deleted. If the column doesn't exist, then
	    // this row cannot be deleted.
	    // But if there is a scan expr, then we need to also retrieve the columns used
	    // in the pred. Add those.
            // Bypass scan when beginRowId_ is less than endRowId_
            if (tcb_->compareRowIds() < 0) {
               step_ = DONE;
               break;
            }
	    LIST(HbaseStr) columns(tcb_->getHeap());
	    if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
	      {
		columns = tcb_->deletedColumns_;
		if (tcb_->scanExpr())
		  {
		    // retrieve all columns if none is specified.
		    if (tcb_->columns_.entries() == 0)
		      columns.clear();
		    else
		      // append retrieved columns to deleted columns.
		      columns.insert(tcb_->columns_);
		  }
	      }
            Int64 time1 = JULIANTIMESTAMP ();
	    retcode = tcb_->ehi_->scanOpen(tcb_->table_, 
					   tcb_->beginRowId_, tcb_->endRowId_,
					   columns, -1,
					   tcb_->hbaseAccessTdb().readUncommittedScan(),
             tcb_->hbaseAccessTdb().getScanMemoryTable(),
                                           tcb_->hbaseAccessTdb().getLockMode(),
                                           tcb_->hbaseAccessTdb().getIsolationLevel(),
					   TRUE, FALSE, // no scan operator
					   tcb_->hbaseAccessTdb().replSync(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->cacheBlocks(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->useSmallScanner(),
					   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->numCacheRows(),
					   FALSE, NULL, NULL, NULL,
                                           tcb_->hbaseAccessTdb().getNumReplications(),
	                   tcb_->hbaseAccessTdb().getHbasePerfAttributes()->dopParallelScanner());

	    if (recordCostTh_ >= 0)
	      {
	        Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[30]++;
                    sumCost[30] += time2;
                  }
	        if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseUMDnativeSubsetTaskTcb SCAN_OPEN PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[30], (sumCost[30] / (callCount[30] ? callCount[30] : 1)), sumCost[30], time2, tcb_->table_.val);
	      }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::scanOpen"))
	      step_ = HANDLE_ERROR;
	    else
	      {
		step_ = NEXT_ROW;
		tcb_->isEOD_ = FALSE;
	      }
	  }
	  break;

	case NEXT_ROW:
	  {
            if (--remainingInBatch <= 0)
              {
                rc = WORK_CALL_AGAIN;
                return 1;
              }
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = tcb_->ehi_->nextRow();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[31]++;
                    sumCost[31] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDnativeSubsetTaskTcb NEXT_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[31], (sumCost[31] / (callCount[31] ? callCount[31] : 1)), sumCost[31], time2, tcb_->table_.val);
              }
	    if (retcode == HBASE_ACCESS_EOD || retcode == HBASE_ACCESS_EOR)
	    {
	       tcb_->isEOD_ = TRUE;
	       step_ = SCAN_CLOSE;
	       break;
	    } 
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::nextRow"))
	       step_ = HANDLE_ERROR;
	    else
	       step_ = NEXT_CELL;
	  }
	  break;

	case NEXT_CELL:
	  {
            if (tcb_->colVal_.val == NULL)
                tcb_->colVal_.val = new (tcb_->getHeap())
                     char[tcb_->hbaseAccessTdb().convertRowLen()];
            tcb_->colVal_.len = tcb_->hbaseAccessTdb().convertRowLen();
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = tcb_->ehi_->nextCell( tcb_->rowId_, tcb_->colFamName_, 
					    tcb_->colName_, tcb_->colVal_,
					    tcb_->colTS_);
            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  {
                    callCount[32]++;
                    sumCost[32] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDnativeSubsetTaskTcb NEXT_CELL PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[32], (sumCost[32] / (callCount[32] ? callCount[32] : 1)), sumCost[32], time2, tcb_->table_.val);
              }
	    if (retcode == HBASE_ACCESS_EOD)
	    {
	      if (tcb_->hbaseAccessTdb().getAccessType() 
	           == ComTdbHbaseAccess::DELETE_)
	       {
	          if (! tcb_->scanExpr())
	          {
	             step_ = DELETE_ROW;
		     break;
	          }
		}
	        step_ = CREATE_FETCHED_ROWWISE_ROW;
                break;
            }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::nextCell"))
	    {
		step_ = HANDLE_ERROR;
		break;
	    }
	    step_ = APPEND_CELL_TO_ROW;
	  }
	  break;

	case APPEND_CELL_TO_ROW:
	  {
            tcb_->copyCell();
	    step_ = NEXT_CELL;
	  }
	  break;

	case CREATE_FETCHED_ROWWISE_ROW:
	  {
	    rc = tcb_->createRowwiseRow();
	    if (rc < 0)
	      {
		if (rc != -1)
		  tcb_->setupError(rc, "createRowwiseRow");
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = APPLY_PRED;
	  }
	  break;

	  case APPLY_PRED:
	  {
	    rc = tcb_->applyPred(tcb_->scanExpr());
	    if (rc == 1)
	      {
		if (tcb_->hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
		  step_ = DELETE_ROW;
		else
		  step_ = CREATE_UPDATED_ROWWISE_ROW; 
	      }
	    else if (rc == -1)
	       step_ = HANDLE_ERROR;
	    else
               step_ = NEXT_ROW; 
	  }
	  break;

	case CREATE_UPDATED_ROWWISE_ROW:
	  {
	    tcb_->workAtp_->getTupp(tcb_->hbaseAccessTdb().updateTuppIndex_)
	      .setDataPointer(tcb_->updateRow_);
	    
	    if (tcb_->updateExpr())
	      {
		ex_expr::exp_return_type evalRetCode =
		  tcb_->updateExpr()->eval(pentry_down->getAtp(), tcb_->workAtp_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

	    step_ = CREATE_MUTATIONS;
	  }
	  break;

	case CREATE_MUTATIONS:
	  {
	    ExpTupleDesc * rowTD =
	      tcb_->hbaseAccessTdb().workCriDesc_->getTupleDescriptor
	      (tcb_->hbaseAccessTdb().updateTuppIndex_);
	    
	    Attributes * attr = rowTD->getAttr(0);
 
	    retcode = tcb_->createDirectRowwiseBuffer(
						   &tcb_->updateRow_[attr->getOffset()]);

	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = UPDATE_ROW;
	  }
	  break;

	case UPDATE_ROW:
	  {
	    if (tcb_->numColsInDirectBuffer() > 0)
	      {
                  if (g != NULL && g->getStatement() != NULL &&
                      g->getStatement()->getTriggerExecErr() == TRUE)
                      {
                          retcode = TRIGGER_EXECUTE_EXCEPTION;
                      }
                  else
                      {
		Int64 time1 = JULIANTIMESTAMP ();
		retcode = tcb_->ehi_->insertRow(tcb_->table_,
						tcb_->rowId_,
						tcb_->row_,
                                                tcb_->hbaseAccessTdb().useHbaseXn(),
						tcb_->hbaseAccessTdb().replSync(),
                                                tcb_->hbaseAccessTdb().incrementalBackup(),
                                                tcb_->hbaseAccessTdb().useRegionXn(),
						-1,// colTS_
                                                tcb_->asyncOperation_,
                                                (tcb_->hbaseAccessTdb().useEncryption() 
                                                 ? (char*)&tcb_->encryptionInfo_ : NULL),
						(char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
						(tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql() : NULL));
                
                if (recordCostTh_ >= 0)
                  {
                    Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                    if (transId > 0)
                      { 
                        callCount[33]++;
                        sumCost[33] += time2;
                      }
                    if (time2 >= recordCostTh_)
                      QRWARN("ExHbaseUMDnativeSubsetTaskTcb UPDATE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                              pid, transId, callCount[33], (sumCost[33] / (callCount[33] ? callCount[33] : 1)), sumCost[33], time2, tcb_->table_.val);
                  }
                if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                    retcode == TRIGGER_PARAMETER_EXCEPTION)
                    {
                        g->getStatement()->setTriggerExecErr(TRUE);
                    }
                      }
              
		if (tcb_->setupError(retcode, "ExpHbaseInterface::insertRow"))
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		
		if (tcb_->getHbaseAccessStats())
		  tcb_->getHbaseAccessStats()->incUsedRows();

		tcb_->matches_++;
	      }

	    step_ = NEXT_ROW; 
	  }
	  break;

	case DELETE_ROW:
	  {
              if (g != NULL && g->getStatement() != NULL &&
                  g->getStatement()->getTriggerExecErr() == TRUE)
                  {
                      retcode = TRIGGER_EXECUTE_EXCEPTION;
                  }
              else
                  {
	    Int64 time1 = JULIANTIMESTAMP ();
	    tcb_->ehi_->settrigger_operation(COM_DELETE);
	    retcode =  tcb_->ehi_->deleteRow(tcb_->table_,
					     tcb_->rowId_,
					     &tcb_->deletedColumns_,
                                             tcb_->hbaseAccessTdb().useHbaseXn(),
					     tcb_->hbaseAccessTdb().replSync(),
                                             tcb_->hbaseAccessTdb().incrementalBackup(),
                                             tcb_->hbaseAccessTdb().useRegionXn(),
					     -1,
					     tcb_->asyncOperation_,
                                             (tcb_->hbaseAccessTdb().getComHbaseAccessOptions() 
                                              ? tcb_->hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                             (tcb_->hbaseAccessTdb().useEncryption() 
                                              ? (char*)&tcb_->encryptionInfo_ : NULL),
					     (char*)(tcb_->hbaseAccessTdb().getTriggers(tcb_->table_.val)),
					     (tcb_->hbaseAccessTdb().useTrigger() ? tcb_->getCurExecUtf8sql(tcb_->hbaseAccessTdb().withNoReplicate()) : NULL));

	    if (recordCostTh_ >= 0)
	      {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[34]++;
                    sumCost[34] += time2;
                  }
	        if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseUMDnativeSubsetTaskTcb DELETE_ROW PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[34], (sumCost[34] / (callCount[34] ? callCount[34] : 1)), sumCost[34], time2, tcb_->table_.val);
	      }
	    tcb_->ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
                  }

	    if ( tcb_->setupError(retcode, "ExpHbaseInterface::deleteRow"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
	    
	    tcb_->currRowidIdx_++;
	    
	    if (tcb_->getHbaseAccessStats())
	      tcb_->getHbaseAccessStats()->incUsedRows();

	    tcb_->matches_++;

            step_ = NEXT_ROW;
	  }
	  break;

	case SCAN_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = tcb_->ehi_->scanClose();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[35]++;
                    sumCost[35] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseUMDnativeSubsetTaskTcb SCAN_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[35], (sumCost[35] / (callCount[35] ? callCount[35] : 1)), sumCost[35], time2, tcb_->table_.val);
              }
	    if (tcb_->setupError(retcode, "ExpHbaseInterface::scanClose"))
	      step_ = HANDLE_ERROR;
	    else
	      step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    step_ = NOT_STARTED;
	    return -1;
	  }
	  break;

	case DONE:
	  {
	    step_ = NOT_STARTED;
	    return 0;
	  }
	  break;

	}// switch

    } // while
}

ExHbaseAccessUMDTcb::ExHbaseAccessUMDTcb(
          const ExHbaseAccessTdb &hbaseAccessTdb, 
          ex_globals * glob ) :
  ExHbaseAccessTcb(hbaseAccessTdb, glob),
  step_(NOT_STARTED)
{
  umdSQSubsetTaskTcb_ = NULL;
  umdSQUniqueTaskTcb_ = NULL;

  for (Lng32 i = 0; i < UMD_MAX_TASKS; i++)
    {
      tasks_[i] = FALSE;
    }

  ExHbaseAccessTdb &hbaseTdb = (ExHbaseAccessTdb&)hbaseAccessTdb;

  if (hbaseTdb.listOfScanRows())
    {
      tasks_[UMD_SUBSET_TASK] = TRUE;
  
      if (hbaseTdb.sqHbaseTable())
	umdSQSubsetTaskTcb_ = 
	  new(getGlobals()->getDefaultHeap()) ExHbaseUMDtrafSubsetTaskTcb(this);
      else
	umdSQSubsetTaskTcb_ = 
	  new(getGlobals()->getDefaultHeap()) ExHbaseUMDnativeSubsetTaskTcb(this);
     }

  if ((hbaseTdb.keySubsetGen()) &&
      (NOT hbaseTdb.uniqueKeyInfo()))
    {
      tasks_[UMD_SUBSET_KEY_TASK] = TRUE;

      if (hbaseTdb.sqHbaseTable())
	umdSQSubsetTaskTcb_ = 
	  new(getGlobals()->getDefaultHeap()) ExHbaseUMDtrafSubsetTaskTcb(this);
      else
	umdSQSubsetTaskTcb_ = 
	  new(getGlobals()->getDefaultHeap()) ExHbaseUMDnativeSubsetTaskTcb(this);
    }

  if (hbaseTdb.listOfGetRows())
    {
      tasks_[UMD_UNIQUE_TASK] = TRUE;
      
     if (hbaseTdb.sqHbaseTable())
       umdSQUniqueTaskTcb_ = 
	 new(getGlobals()->getDefaultHeap()) ExHbaseUMDtrafUniqueTaskTcb(this);
     else
       umdSQUniqueTaskTcb_ = 
	 new(getGlobals()->getDefaultHeap()) ExHbaseUMDnativeUniqueTaskTcb(this);
    }

  if ((hbaseTdb.keySubsetGen()) &&
      (hbaseTdb.uniqueKeyInfo()))
    {
      tasks_[UMD_UNIQUE_KEY_TASK] = TRUE;

     if (hbaseTdb.sqHbaseTable())
      umdSQUniqueTaskTcb_ = 
	new(getGlobals()->getDefaultHeap()) ExHbaseUMDtrafUniqueTaskTcb(this);
     else
      umdSQUniqueTaskTcb_ = 
	new(getGlobals()->getDefaultHeap()) ExHbaseUMDnativeUniqueTaskTcb(this);
    }
}

ExWorkProcRetcode ExHbaseAccessUMDTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  ExMasterStmtGlobals *g = getGlobals()->
    castToExExeStmtGlobals()->castToExMasterStmtGlobals();

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if ((pentry_down->downState.request == ex_queue::GET_NOMORE) &&
	  (step_ != DONE))
	{
	  step_ = UMD_CLOSE_NO_ERROR; //DONE;
	}

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    if (costThreshold != NULL)
	      recordCostTh_ = atoi(costThreshold);
	    step_ = UMD_INIT;
	  }
	  break;

	case UMD_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            ehi_->setNoConflictCheckForIndex(hbaseAccessTdb().noConflictCheck());
	    if (hbaseAccessTdb().listOfScanRows())
	      hbaseAccessTdb().listOfScanRows()->position();

	    if (hbaseAccessTdb().listOfGetRows())
	      {
		if (!  rowIdExpr())
		  {
		    setupError(-HBASE_OPEN_ERROR, "", "RowId Expr is empty");
		    step_ = HANDLE_ERROR;
		    break;
		  }

		hbaseAccessTdb().listOfGetRows()->position();
	      }

      fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                         hbaseAccessTdb().getDataUIDName(), 
                         hbaseAccessTdb().replaceNameByUID());

	    if (umdSQSubsetTaskTcb_)
	      umdSQSubsetTaskTcb_->init();

	    if (umdSQUniqueTaskTcb_)
	      umdSQUniqueTaskTcb_->init();

	    step_ = SETUP_SUBSET;
	  }
	  break;

	case SETUP_SUBSET:
	  {
	    if (NOT tasks_[UMD_SUBSET_TASK])
	      {
		step_ = SETUP_UNIQUE;
		break;
	      }

	    hsr_ = 
	      (ComTdbHbaseAccess::HbaseScanRows*)hbaseAccessTdb().listOfScanRows()
	      ->getCurr();

	    retcode = setupSubsetRowIdsAndCols(hsr_);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = PROCESS_SUBSET;
	  }
	  break;

	case PROCESS_SUBSET:
	  {
	    rc = 0;
	    retcode = umdSQSubsetTaskTcb_->work(rc);
	    if (retcode == 1)
	      return rc;
	    else if (retcode < 0)
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_SUBSET;
	  }
	  break;

	case NEXT_SUBSET:
	  {
	    hbaseAccessTdb().listOfScanRows()->advance();

	    if (! hbaseAccessTdb().listOfScanRows()->atEnd())
	      {
		step_ = SETUP_SUBSET;
		break;
	      }

	    step_ = SETUP_UNIQUE;
	  }
	  break;

	case SETUP_UNIQUE:
	  {
	    if (NOT tasks_[UMD_UNIQUE_TASK])
	      {
		step_ = SETUP_SUBSET_KEY; 
		break;
	      }

	    hgr_ = 
	      (ComTdbHbaseAccess::HbaseGetRows*)hbaseAccessTdb().listOfGetRows()
	      ->getCurr();

	    retcode = setupUniqueRowIdsAndCols(hgr_);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = PROCESS_UNIQUE;
	  }
	  break;

	case PROCESS_UNIQUE:
	  {
	    rc = 0;
	    retcode = umdSQUniqueTaskTcb_->work(rc);
	    if (retcode == 1)
	      return rc;
      else if (retcode == WORK_CALL_AGAIN)
        return WORK_CALL_AGAIN;
	    else if (retcode < 0)
	      step_ = HANDLE_ERROR;
	    else
	      step_ = NEXT_UNIQUE;
	  }
	  break;

	case NEXT_UNIQUE:
	  {
	    hbaseAccessTdb().listOfGetRows()->advance();

	    if (! hbaseAccessTdb().listOfGetRows()->atEnd())
	      {
		step_ = SETUP_UNIQUE;
		break;
	      }

	    step_ = SETUP_SUBSET_KEY;
	  }
	  break;

	case SETUP_SUBSET_KEY:
	  {
	    if (NOT tasks_[UMD_SUBSET_KEY_TASK])
	      {
		step_ = SETUP_UNIQUE_KEY;
		break;
	      }

	    retcode = setupSubsetKeysAndCols();
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = PROCESS_SUBSET_KEY;
	  }
	  break;

	case PROCESS_SUBSET_KEY:
	  {
	    rc = 0;
	    retcode = umdSQSubsetTaskTcb_->work(rc);
	    if (retcode == 1)
	      return rc;
	    else if (retcode < 0)
	      step_ = HANDLE_ERROR;
	    else 
	      step_ = SETUP_UNIQUE_KEY;
	  }
	  break;

	case SETUP_UNIQUE_KEY:
	  {
	    if (NOT tasks_[UMD_UNIQUE_KEY_TASK])
	      {
		step_ = UMD_CLOSE;
		break;
	      }

	    retcode = setupUniqueKeyAndCols(TRUE);
	    if (retcode == -1)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    step_ = PROCESS_UNIQUE_KEY;
	  }
	  break;

	case PROCESS_UNIQUE_KEY:
	  {
	    rc = 0;
	    retcode = umdSQUniqueTaskTcb_->work(rc);

      if (retcode == 1)
        return rc;
      else if (retcode == WORK_CALL_AGAIN)
        return WORK_CALL_AGAIN;
	    else if (retcode < 0)
	      step_ = HANDLE_ERROR;
	    else 
	      step_ = UMD_CLOSE;
	  }
	  break;

	case UMD_CLOSE:
	case UMD_CLOSE_NO_ERROR:
	  {
	    retcode = ehi_->close();
	    if (step_ == UMD_CLOSE)
	      {
		if (setupError(retcode, "ExpHbaseInterface::close"))
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;

	    retcode = ehi_->close();

	    step_ = DONE;
	  }
	  break;

	case DONE:
	  {
	    if (NOT hbaseAccessTdb().computeRowsAffected())
	      matches_ = 0;

	    if (handleDone(rc, matches_))
	      return rc;

	    if (umdSQSubsetTaskTcb_)
	      umdSQSubsetTaskTcb_->init();

	    if (umdSQUniqueTaskTcb_)
	      umdSQUniqueTaskTcb_->init();

	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch
    } // while

  return WORK_OK;
}

ExHbaseAccessSQRowsetTcb::ExHbaseAccessSQRowsetTcb(
          const ExHbaseAccessTdb &hbaseAccessTdb, 
          ex_globals * glob ) :
  ExHbaseAccessTcb( hbaseAccessTdb, glob)
  , step_(NOT_STARTED)
{
  prevTailIndex_ = 0;

  nextRequest_ = qparent_.down->getHeadIndex();

  numRetries_ = 0;
  lastHandledStep_ = NOT_STARTED;
  numRowsInVsbbBuffer_ = 0;
}

Lng32 ExHbaseAccessSQRowsetTcb::setupUniqueKey()
{
  ex_queue_entry *pentry_down = qparent_.down->getQueueEntry(nextRequest_);

  if (pentry_down->downState.request == ex_queue::GET_NOMORE
     || pentry_down->downState.request == ex_queue::GET_EOD)
     return 1;

  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;
  keyRangeEx::getNextKeyRangeReturnType keyRangeStatus;

  initNextKeyRange(pool_, pentry_down->getAtp());

  keyRangeStatus =
    keySubsetExeExpr_->getNextKeyRange(pentry_down->getAtp(), FALSE, TRUE);

  if (keyRangeStatus == keyRangeEx::EXPRESSION_ERROR)
     return -1;

  tupp &keyData = keySubsetExeExpr_->getBkData();
  char * beginKeyRow = keyData.getDataPointer();
  HbaseStr rowIdRowText;

  if ((NOT hbaseAccessTdb().sqHbaseTable()) ||
      (hbaseAccessTdb().keyInVCformat())) {
      // Key is in varchar format.
      short keyLen = *(short*)beginKeyRow;
      rowIdRowText.val = beginKeyRow + sizeof(short);
      rowIdRowText.len = keyLen;
   }
  else {
    rowIdRowText.val = beginKeyRow;
    rowIdRowText.len = hbaseAccessTdb().keyLen_;
  }

  if (keyRangeStatus == keyRangeEx::NO_MORE_RANGES)		
  {		
      // To ensure no row is found, add extra byte with "0" value 		
      rowIdRowText.val[rowIdRowText.len] = '\0';		
      rowIdRowText.len += 1;		
  }		
  copyRowIDToDirectBuffer(rowIdRowText);
  return 0;
}


Lng32 ExHbaseAccessSQRowsetTcb::setupRowIds()
{
  Lng32 retcode;
  UInt16 rowsetMaxRows = hbaseAccessTdb().getHbaseRowsetVsbbSize();
  
  queue_index tlindex = qparent_.down->getTailIndex();
  while (nextRequest_ != tlindex) {
     retcode = setupUniqueKey();
     if (retcode != 0)
        return retcode;
     nextRequest_++;
    // Don't buffer more than HBASE_ROWSET_VSBB_SIZE
    if (numRowsInDirectBuffer()  >= rowsetMaxRows)
        return 1;
  }
  return 0;
}

ExWorkProcRetcode ExHbaseAccessSQRowsetTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  Int64 transId = GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1;
  if (transId == -1 || curTransId != transId)
    {
      if (curTransId >= 0) {
        Int64 count = 0;
        double cost = 0;
        for (Int32 idx = 0; idx < 40; idx++) {
          count += callCount[idx];
          cost += sumCost[idx];
        }
        memset(sumCost, 0, sizeof(sumCost));
        memset(callCount, 0, sizeof(callCount));
        if (cost >= recordJniAll && recordJniAll >= 0)
        QRWARN("ExHbaseIUD summary PID %d txID %ld TCC %ld TTC %.2f", pid, curTransId, count, cost);
      }
      curTransId = transId;
    }

  ExMasterStmtGlobals *g = getGlobals()->
    castToExExeStmtGlobals()->castToExMasterStmtGlobals();  

  while (!qparent_.down->isEmpty())
    {

      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = CLOSE_AND_DONE;
      else if (pentry_down->downState.request == ex_queue::GET_EOD) {
         if (numRowsInDirectBuffer() > 0) {
            if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
               step_ = PROCESS_UPDATE_AND_CLOSE;
            else if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
               step_ = PROCESS_DELETE_AND_CLOSE;
            else 
               ex_assert(0, "EOD and Select is not handled here"); 
          }
          else
            step_ = ALL_DONE;
      }
      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    currRowNum_ = 0;
	    numRetries_ = 0;

	    prevTailIndex_ = 0;
            asyncCompleteRetryCount_ = 0;
            asyncOperationTimeout_ = 1;
            asyncOperation_ = hbaseAccessTdb().asyncOperations() && getTransactionIDFromContext();
            numRowsInVsbbBuffer_ = 0;
            lastHandledStep_ = NOT_STARTED;
	    
	    nextRequest_ = qparent_.down->getHeadIndex();
	    if (costThreshold != NULL)
	      recordCostTh_ = atoi(costThreshold);
	    step_ = RS_INIT;
	  }
	  break;
	  
	case RS_INIT:
	  {
	    retcode = ehi_->init(getHbaseAccessStats());
	    if (setupError(retcode, "ExpHbaseInterface::init"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

	    // set up a DDL Validator to monitor for DDL changes
	    if (ddlValidator_.validatingDDL())
	      {
	        ehi_->setDDLValidator(&ddlValidator_);
	      }

      fixObjName4PartTbl(hbaseAccessTdb().getTableName(), 
                         hbaseAccessTdb().getDataUIDName(), 
                         hbaseAccessTdb().replaceNameByUID());

            if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
            {
               ExpTupleDesc * rowTD =
    		hbaseAccessTdb().workCriDesc_->getTupleDescriptor
                (hbaseAccessTdb().updateTuppIndex_);
                allocateDirectRowBufferForJNI(rowTD->numAttrs(),
                                    hbaseAccessTdb().getHbaseRowsetVsbbSize());
            }
	    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_
                 || hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_
                 || hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
                allocateDirectRowIDBufferForJNI(hbaseAccessTdb().getHbaseRowsetVsbbSize());

	    setupListOfColNames(hbaseAccessTdb().listOfFetchedColNames(),
				columns_);
	    if (hbaseAccessTdb().listOfGetRows())
	      {
		if (!  rowIdExpr())
		  {
		    setupError(-HBASE_OPEN_ERROR, "", "RowId Expr is empty");
		    step_ = HANDLE_ERROR;
		    break;
		  }

		hbaseAccessTdb().listOfGetRows()->position();
	      }
	    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_) 
	       step_ = SETUP_SELECT;
            else
	       step_ = SETUP_UMD;
	  }
	  break;
	case SETUP_SELECT:
	  {
            retcode = setupRowIds();
            switch (retcode) {
               case 0:
                  if (qparent_.down->getLength() == 1) {
                     // only one row in the down queue.
                     // Before we send input buffer to hbase, give parent
                     // another chance in case there is more input data.
                     // If parent doesn't input any more data on second (or
                     // later) chances, then process the request.
                     if (numRetries_ == 3)    {
                         numRetries_ = 0;
                         step_ = PROCESS_SELECT;
                      } else {
                          numRetries_++;
                          return WORK_CALL_AGAIN;
                      }
                  }
                  else
                      step_ = PROCESS_SELECT;
                  break;
               case 1:
                  // Reached the max. number of rowIds
                  // Process the rowIds in the buffer 
                  step_ = PROCESS_SELECT;
                  break;
               default:
                  step_ = HANDLE_ERROR;
                  break;
            }
	  }
	  break;
	case SETUP_UMD:
	  {
            rc = evalInsDelPreCondExpr();
            if (rc == -1) {
                step_ = HANDLE_ERROR;
                break;
            }
            if (rc == 0) { // No need to delete
               step_ = NEXT_ROW;
               break;
            }

	    rowIds_.clear();
	    if (keySubsetExeExpr_ != NULL)
	      {
		retcode = setupUniqueKeyAndCols(FALSE);//This sets up rowIds_
		if (retcode == -1) {
		  step_ = HANDLE_ERROR;
		  break;
		}
	      }
	    else
	      {
		//If the searchkey is missing (eliminated due to const key process 
		//in precodegen) . The HbaseSearchKey would have been used in the 
		//codeGen phase to generate  the rowIdExpr_. Use that instead. 
		if (hbaseAccessTdb().listOfGetRows())
		hgr_ = 
		  (ComTdbHbaseAccess::HbaseGetRows*)hbaseAccessTdb().listOfGetRows()
		  ->getCurr();

		retcode = setupUniqueRowIdsAndCols(hgr_); //This sets up rowIds_
		if (retcode == -1) {
		  step_ = HANDLE_ERROR;
		  break;
		}
	      }

            if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
              setupListOfColNames(hbaseAccessTdb().listOfUpDeldColNames(),
                                  deletedColumns_);
            
	    copyRowIDToDirectBuffer(rowIds_[0]);

	    if ((hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_) ||
		(hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_))
	      step_ = NEXT_ROW;
	    else if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
	      step_ = CREATE_UPDATED_ROW;
	    else
	      step_ = HANDLE_ERROR;

	  }
	  break;

	case NEXT_ROW:
	  {
	    currRowNum_++;
	    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_) {
              // matches_ is set to 1 when the row is projected  by moveRowToUpQueue
              // to denote that there is a matching entry
               matches_ = 0;
               retcode = ehi_->nextRow();
              // EOR is end of result set for the current Rowset
              // EOD is no data for the current row
              // But EOD is never returned, instead HBASE_ACCESS_NO_ROW is returned
              // when no row is found in CREATE_ROW step
              if (retcode == HBASE_ACCESS_EOR) {
                 step_ = RS_CLOSE;
                 break;
              }
              if (retcode == HBASE_ACCESS_EOD) {
                 step_ = ROW_DONE;
                 break;
              }
              if (setupError(retcode, "ExpHbaseInterface::nextRow"))
                 step_ = HANDLE_ERROR;
              else
                 step_ = CREATE_ROW;
              break;
            }
	    matches_++;
	    if (numRowsInDirectBuffer() < hbaseAccessTdb().getHbaseRowsetVsbbSize()) {
		step_ = DONE;
		break;
	    }
	    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
	      step_ = PROCESS_DELETE_AND_CLOSE;
	    else if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
	      step_ = PROCESS_UPDATE_AND_CLOSE;
	    else
	      step_ = HANDLE_ERROR;
	  }
	  break;
       case CREATE_ROW:
          {
            if (ehi_->isReadFromMemoryTable())
              retcode = createSQRowDirectFromMemoryTable();
            else
              retcode = createSQRowDirect();
            if (retcode == HBASE_ACCESS_NO_ROW) {
               step_ = ROW_DONE;
               break;
            }
            if (retcode < 0)
            {
                rc = (short)retcode;
                setupError(rc, "createSQRowDirect");
                step_ = HANDLE_ERROR;
                break;
            }
            if (retcode != HBASE_ACCESS_SUCCESS)
            {
                step_ = HANDLE_ERROR;
                break;
            }
            step_ = APPLY_PRED;
          }
          break;
        case APPLY_PRED:
          {
            rc = applyPred(scanExpr());
            if (rc == 1)
              step_ = RETURN_ROW;
            else if (rc == -1)
              step_ = HANDLE_ERROR;
            else
              step_ = ROW_DONE;
          }
          break;
        case RETURN_ROW:
          {
            rc = 0;
            if (moveRowToUpQueue(convertRow_, hbaseAccessTdb().optLargVar() ? convertRowLen_ : hbaseAccessTdb().convertRowLen(), 
                                       &rc, FALSE))
              return rc;
            if (getHbaseAccessStats())
               getHbaseAccessStats()->incUsedRows();
            step_ = ROW_DONE;
          }
          break;
	case PROCESS_DELETE_AND_CLOSE:
	  {
            numRowsInVsbbBuffer_ = patchDirectRowIDBuffers();
            ehi_->settrigger_operation(COM_DELETE);
	    if (rowIDs_.len > hbaseAccessTdb().getRowIDLen() &&
		hbaseAccessTdb().getTriggers(table_.val) != NULL)
	      {
		if (numRowsInVsbbBuffer_ > 0)
		  {
		    // columns is the select columns, deletedColumns_,
		    // deletedColumns_ include lable columns
		    retcode = ehi_->getRowsOpen(table_,
						hbaseAccessTdb().getRowIDLen(),
						rowIDs_,
						columns_,
						// deletedColumns_,
						hbaseAccessTdb().getNumReplications(),
                                                hbaseAccessTdb().getLockMode(),
                                                hbaseAccessTdb().getIsolationLevel(),
                                                hbaseAccessTdb().getScanMemoryTable(),
                                                hbaseAccessTdb().skipReadConflict(),
                                                hbaseAccessTdb().skipTransactionForced(),
						NULL);
		  }
		else
		  {
		    retcode = ehi_->getRowOpen(table_,
					       rowIDs_,
					       // deletedColumns_,
					       columns_,
					       -1,
					       hbaseAccessTdb().getNumReplications(),
					       hbaseAccessTdb().getLockMode(),
                                               hbaseAccessTdb().getIsolationLevel(),
                                               hbaseAccessTdb().getScanMemoryTable(),
					       true,
					       NULL);
		  }
                if (setupError(retcode, "ExpHbaseInterface::getRowsOpen"))
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	      }

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else if (numRowsInVsbbBuffer_ > 0)
                {
            Int64 time1 = JULIANTIMESTAMP ();
            ehi_->setNoConflictCheckForIndex(hbaseAccessTdb().noConflictCheck());
	    retcode = ehi_->deleteRows(table_,
                                       hbaseAccessTdb().getRowIDLen(),
                                       rowIDs_,
                                       &deletedColumns_,
                                       hbaseAccessTdb().useHbaseXn(),
				       hbaseAccessTdb().replSync(),
                                       hbaseAccessTdb().incrementalBackup(),
				       -1,
				       asyncOperation_,
                                       (hbaseAccessTdb().getComHbaseAccessOptions() 
                                        ? hbaseAccessTdb().getComHbaseAccessOptions()->hbaseAuths() : NULL),
                                       (hbaseAccessTdb().useEncryption() 
                                        ? (char*)&encryptionInfo_ : NULL),
				       (char*)(hbaseAccessTdb().getTriggers(table_.val)),
				       (hbaseAccessTdb().useTrigger() ? getCurExecUtf8sql(hbaseAccessTdb().withNoReplicate()) : NULL));

	    if (recordCostTh_ >= 0)
	      {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[36]++;
                    sumCost[36] += time2;
                  }
	        if (time2 >= recordCostTh_ && asyncOperation_)
	          QRWARN("ExHbaseAccessSQRowsetTcb PROCESS_DELETE_AND_CLOSE_async PID %d txID %ld TCC %d ATC %.2f TTC %.2f  TC %ld  %s",
	                  pid, transId, callCount[36], (sumCost[36] / (callCount[36] ? callCount[36] : 1)), sumCost[36], time2, table_.val);
	        else if (time2 >= recordCostTh_)
	          QRWARN("ExHbaseAccessSQRowsetTcb PROCESS_DELETE_AND_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
	                  pid, transId, callCount[36], (sumCost[36] /  (callCount[36] ? callCount[36] : 1)), sumCost[36], time2, table_.val);
	      }
	    ehi_->settrigger_operation(COM_UNKNOWN_IUD);
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
                }
            
            currRowNum_ = 0;	    
	    if (setupError(retcode, "ExpHbaseInterface::deleteRows"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
            if (asyncOperation_ && numRowsInVsbbBuffer_ > 0) {
               lastHandledStep_ = step_;
               step_ = COMPLETE_ASYNC_OPERATION;
               break;
            }
            if (getHbaseAccessStats()) {
	      getHbaseAccessStats()->incUsedRows(numRowsInVsbbBuffer_);
	    }
	    step_ = RS_CLOSE;
	  }
	  break;

	case PROCESS_SELECT:
	  {
            if (numRowsInDirectBuffer() > 0) {
              Int64 time1 = JULIANTIMESTAMP ();
              numRowsInVsbbBuffer_ = patchDirectRowIDBuffers();
              retcode = ehi_->getRowsOpen(
                   table_,
                   hbaseAccessTdb().getRowIDLen(),
                   rowIDs_, 
                   columns_,
                   hbaseAccessTdb().getNumReplications(),
                   hbaseAccessTdb().getLockMode(),
                   hbaseAccessTdb().getIsolationLevel(),
                   hbaseAccessTdb().getScanMemoryTable(),
                   hbaseAccessTdb().skipReadConflict(),
                   hbaseAccessTdb().skipTransactionForced(),
                   NULL);

              if (recordCostTh_ >= 0)
                {
                  Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                  if (transId > 0)
                    { 
                      callCount[37]++;
                      sumCost[37] += time2;
                    }
                  if (time2 > recordCostTh_)
                    QRWARN("ExHbaseAccessSQRowsetTcb PROCESS_SELECT PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                            pid, transId, callCount[37], (sumCost[37] / (callCount[37] ? callCount[37] : 1)), sumCost[37], time2, table_.val);
                }
              currRowNum_ = 0;
	      if (setupError(retcode, "ExpHbaseInterface::getRowsOpen"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
              step_ = NEXT_ROW;

           }
           else
               step_ = SETUP_SELECT;
	  }
	  break;

	case CREATE_UPDATED_ROW:
	  {
	    workAtp_->getTupp(hbaseAccessTdb().updateTuppIndex_)
	      .setDataPointer(updateRow_);
	    
	    if (updateExpr()) {
		ex_expr::exp_return_type evalRetCode =
		  updateExpr()->eval(pentry_down->getAtp(), workAtp_);
		if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
	    }
            step_ = EVAL_CONSTRAINT;
          }
          break;
        case EVAL_CONSTRAINT:
          {
            rc = evalConstraintExpr(updConstraintExpr(), hbaseAccessTdb().updateTuppIndex_,updateRow_);
            if (rc == 0) {
              step_ = RS_CLOSE; 
              break;
            }
            else if (rc != 1) {
              step_ = HANDLE_ERROR;
              break;
            }
	    retcode = createDirectRowBuffer(
				      hbaseAccessTdb().updateTuppIndex_,
				      updateRow_,
			  	      hbaseAccessTdb().listOfUpdatedColNames(),
				      hbaseAccessTdb().listOfOmittedColNames(),
				      TRUE);
	    if (retcode == -1) {
		step_ = HANDLE_ERROR;
		break;
	    }
	    step_ = NEXT_ROW;
	  }
	  break;
	case PROCESS_UPDATE_AND_CLOSE:
	  {
            numRowsInVsbbBuffer_ = patchDirectRowBuffers();

            if (g != NULL && g->getStatement() != NULL &&
                g->getStatement()->getTriggerExecErr() == TRUE)
                {
                    retcode = TRIGGER_EXECUTE_EXCEPTION;
                }
            else
                {
	    Int64 time1 = JULIANTIMESTAMP ();
	    retcode = ehi_->insertRows(table_,
				       hbaseAccessTdb().getRowIDLen(),
                                       rowIDs_,
                                       rows_,
                                       hbaseAccessTdb().useHbaseXn(),
				       hbaseAccessTdb().replSync(),
                                       hbaseAccessTdb().incrementalBackup(),
				       -1,
				       asyncOperation_,
                                       (hbaseAccessTdb().useEncryption() 
                                        ? (char*)&encryptionInfo_ : NULL),
				       (char*)(hbaseAccessTdb().getTriggers(table_.val)),
				       (hbaseAccessTdb().useTrigger() ? getCurExecUtf8sql() : NULL),
                                        FALSE);

            if (recordCostTh_ >= 0)
              {
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[38]++;
                    sumCost[38] += time2;
                  }
                if (time2 >= recordCostTh_ && asyncOperation_)
                  QRWARN("ExHbaseAccessSQRowsetTcb PROCESS_UPDATE_AND_CLOSE_async PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[38], (sumCost[38] / (callCount[38] ? callCount[38] : 1)), sumCost[38], time2, table_.val);
                else if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessSQRowsetTcb PROCESS_UPDATE_AND_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[38], (sumCost[38] / (callCount[38] ? callCount[38] : 1)), sumCost[38], time2, table_.val);
              }
            if (retcode == TRIGGER_EXECUTE_EXCEPTION ||
                retcode == TRIGGER_PARAMETER_EXCEPTION)
                {
                    g->getStatement()->setTriggerExecErr(TRUE);
                }
                }
            currRowNum_ = 0;	    
	    if (setupError(retcode, "ExpHbaseInterface::insertRows"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
            if (asyncOperation_) {
               lastHandledStep_ = step_;
               step_ = COMPLETE_ASYNC_OPERATION;
               break;
            }
            if (getHbaseAccessStats()) {
	      getHbaseAccessStats()->incUsedRows(numRowsInVsbbBuffer_);
	    }
	    step_ = RS_CLOSE;
	  }
	  break;
      case COMPLETE_ASYNC_OPERATION:
         {
            if (resultArray_  == NULL)
                resultArray_ = new (getHeap()) NABoolean[hbaseAccessTdb().getHbaseRowsetVsbbSize()];
            Int32 timeout;
            if (asyncCompleteRetryCount_ < 10)
               timeout = -1;
            else {
               asyncOperationTimeout_ = asyncOperationTimeout_ * 2;
               timeout = asyncOperationTimeout_;
            }
            retcode = ehi_->completeAsyncOperation(timeout, resultArray_, numRowsInVsbbBuffer_);
            if (retcode == HBASE_RETRY_AGAIN) {
               asyncCompleteRetryCount_++;
               return WORK_CALL_AGAIN;
            }
            asyncCompleteRetryCount_ = 0;
            if (setupError(retcode, "ExpHbaseInterface::completeAsyncOperation")) {
                step_ = HANDLE_ERROR;
                break;
            }
            for (int i = 0 ; i < numRowsInVsbbBuffer_; i++) {
                if (resultArray_[i] == FALSE) {
                    ComDiagsArea * diagsArea = NULL;
                    ExRaiseSqlError(getHeap(), &diagsArea,
                                (ExeErrorCode)(8102));
                    pentry_down->setDiagsArea(diagsArea);
                    step_ = HANDLE_ERROR;
                    break;
               }
            }
            if (step_ == HANDLE_ERROR)
               break;
            if (getHbaseAccessStats()) {
	      getHbaseAccessStats()->incUsedRows(numRowsInVsbbBuffer_);
            }
            step_ = RS_CLOSE;
          }
          break;
	case RS_CLOSE:
	  {
            Int64 time1 = JULIANTIMESTAMP();
	    retcode = ehi_->close();
            if (recordCostTh_ >= 0)
              { 
                Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                if (transId > 0)
                  { 
                    callCount[39]++;
                    sumCost[39] += time2;
                  }
                if (time2 >= recordCostTh_)
                  QRWARN("ExHbaseAccessSQRowsetTcb RS_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                          pid, transId, callCount[39], (sumCost[39] / (callCount[39] ? callCount[39] : 1)), sumCost[39], time2, table_.val);
              }
	    if (setupError(retcode, "ExpHbaseInterface::close"))
	      {
		step_ = HANDLE_ERROR;
		break;
	      }
 	    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_)
               step_ = NOT_STARTED;
            else
	       step_ = ALL_DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;
        if (gEnableRowLevelLock)
        {
            Lng32 absRc = ABS(retcode);
            if (absRc >= HBASE_LOCK_TIME_OUT_ERROR &&
                absRc <= HBASE_LOCK_NOT_ENOUGH_RESOURCE)
            {
                return WORK_OK;
            }
        }
	    step_ = CLOSE_AND_DONE;
	  }
	  break;
        case ROW_DONE:
          {
	    if (handleDone(rc, 0))
	      return rc;
            step_ = NEXT_ROW;
          }
          break;
	case DONE:
        case CLOSE_AND_DONE:
	case ALL_DONE:
	  {
            if (step_ == CLOSE_AND_DONE) {
               Int64 time1 = JULIANTIMESTAMP();
               ehi_->close();
               if (recordCostTh_ >= 0)
               { 
                 Int64 time2 = (JULIANTIMESTAMP() - time1) / 1000;
                 if (transId > 0)
                   {
                     callCount[39]++;
                     sumCost[39] += time2;
                   }
                 if (time2 >= recordCostTh_)
                   QRWARN("ExHbaseAccessSQRowsetTcb RS_CLOSE PID %d txID %ld TCC %d ATC %.2f TTC %.2f TC %ld  %s",
                           pid, transId, callCount[39], (sumCost[39] / (callCount[39] ? callCount[39] : 1)), sumCost[39], time2, table_.val);
               }
            }
	    if (NOT hbaseAccessTdb().computeRowsAffected())
	      matches_ = 0;

	    if ((step_ == DONE) &&
                (qparent_.down->getLength() == 1))
	      {
		// only one row in the down queue.
		// Before we send input buffer to hbase, give parent
		// another chance in case there is more input data.
		// If parent doesn't input any more data on second (or
		// later) chances, then process the request.
		if (numRetries_ == 3 || numRowsInDirectBuffer() > 1)
		  {
		    numRetries_ = 0;

		    // Delete/update the current batch and then done.
		    if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::DELETE_)
		      step_ = PROCESS_DELETE_AND_CLOSE;
		    else if (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::UPDATE_)
		      step_ = PROCESS_UPDATE_AND_CLOSE;
		    else
		      {
                         ex_assert(false, "DONE state is invalid in Rowset SELECT");
		      }
		    break;
		  }
		numRetries_++;
		return WORK_CALL_AGAIN;
	      }

	    if (handleDone(rc, (step_ == ALL_DONE ? matches_ : 0)))
	      return rc;

	    if (step_ == DONE)
	       step_ = SETUP_UMD;
	    else 
              {
	        ehi_->setDDLValidator(NULL);
	        step_ = NOT_STARTED;
              }
	  }
	  break;
	} // switch

    } // while
    if (qparent_.down->isEmpty()
           && (hbaseAccessTdb().getAccessType() == ComTdbHbaseAccess::SELECT_)) {
        ehi_->close();
        step_ = NOT_STARTED;
    } 

  return WORK_OK;
}

