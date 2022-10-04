/**********************************************************************
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
**********************************************************************/

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExExeUtilLoad.cpp
 * Description:  
 *               
 *               
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <iostream>
using std::cerr;
using std::endl;
#include <algorithm>

#include <fstream>
using std::ofstream;

#include <stdio.h>

#include "common/ComCextdecs.h"
#include  "cli_stdh.h"
#include  "ex_stdh.h"
#include  "sql_id.h"
#include  "ex_transaction.h"
#include  "ComTdb.h"
#include  "ex_tcb.h"
#include  "ComSqlId.h"

#include  "ExExeUtil.h"
#include  "ex_exe_stmt_globals.h"
#include  "exp_expr.h"
#include  "exp_clause_derived.h"
#include  "ComRtUtils.h"
#include  "ExStats.h"
#include "exp/ExpLOBenums.h"
#include  "ExpLOBinterface.h"
#include  "str.h"
#include "exp/ExpHbaseInterface.h"
#include "ExHbaseAccess.h"
#include "comexe/ComTdbHbaseAccess.h"
#include "ExHbaseAccess.h"
#include "exp/ExpErrorEnums.h"
#include "exp/ExpLOBenums.h"


///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilCreateTableAsTdb::build(ex_globals * glob)
{
  ExExeUtilCreateTableAsTcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilCreateTableAsTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilCreateTableAsTcb
///////////////////////////////////////////////////////////////
ExExeUtilCreateTableAsTcb::ExExeUtilCreateTableAsTcb(
     const ComTdbExeUtil & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob),
       step_(INITIAL_),
       tableExists_(FALSE)
{
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);
}


//////////////////////////////////////////////////////
// work() for ExExeUtilCreateTableAsTcb
//////////////////////////////////////////////////////
short ExExeUtilCreateTableAsTcb::work()
{
  Lng32 cliRC = 0;
  short retcode = 0;
  Int64 rowsAffected = 0;
  NABoolean redriveCTAS = FALSE;
  
  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;
  
  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli *currContext = masterGlob->getStatement()->getContext();
  ExTransaction *ta = currContext->getTransaction();
  
  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    NABoolean xnAlreadyStarted = ta->xnInProgress();

	    // allow a user transaction if NO LOAD was specified
	    if (xnAlreadyStarted && !ctaTdb().noLoad())
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -20123, NULL, NULL, NULL,
                                "This DDL operation");
                step_ = ERROR_;
                break;
              }
            
	    doSidetreeInsert_ = TRUE;
	    if (xnAlreadyStarted)
              doSidetreeInsert_ = FALSE;
	    else if ( ctaTdb().siQuery_ == (NABasicPtr) NULL )
	      doSidetreeInsert_ = FALSE;

	    tableExists_ = FALSE;

	    if (ctaTdb().ctQuery_)
	      step_ = CREATE_;
	    else
	      step_ = INSERT_ROWS_;
	  }
	break;

	case CREATE_:
	  {
	    tableExists_ = FALSE;
	    // issue the create table command 
	    cliRC = cliInterface()->executeImmediate(
		 ctaTdb().ctQuery_);
	    if (cliRC < 0)
	      {
		if (((cliRC == -1055) ||  // SQ table err msg
		     (cliRC == -1390) ||  // Traf err msg
		     (cliRC == -1387)) && // Hive err msg
		    (ctaTdb().loadIfExists()))
		  {
		    SQL_EXEC_ClearDiagnostics(NULL);
		    tableExists_ = TRUE;

		    if (ctaTdb().deleteData())
		      step_ = TRUNCATE_TABLE_;
		    else
                      {
                        doSidetreeInsert_ = FALSE;
                        step_ = INSERT_ROWS_;
                      }
		    break;
		  }
		else
		  {
		    cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		    step_ = ERROR_;
		    break;
		  }
	      }
            //turn off binlog from this point
            cliRC = cliInterface()->executeImmediate("control query default DONOT_WRITE_XDC_DDL 'ON' ");
            if (cliRC < 0)
              {
                step_ = ERROR_;
                break;
              }
	    // a transaction may not have existed prior to the create stmt,
	    // but if autocommit was off, a transaction would now exist.
	    // turn off sidetree insert.
	    if (ta->xnInProgress())
	      doSidetreeInsert_ = FALSE;

	    if (ctaTdb().noLoad())
	      step_ = DONE_;
	    else
	      step_ = INSERT_ROWS_;
	  }
	break;

	case TRUNCATE_TABLE_:
	case TRUNCATE_TABLE_AND_ERROR_:
	  {
	    char * ddQuery = 
	      new(getMyHeap()) char[strlen("TRUNCATE TABLE; ") + 
				   strlen(ctaTdb().getTableName()) +
				   100];
	    strcpy(ddQuery, "TRUNCATE TABLE ");
	    strcat(ddQuery, ctaTdb().getTableName());
	    strcat(ddQuery, ";");
	    cliRC = cliInterface()->executeImmediate(ddQuery, NULL,NULL,TRUE,NULL,TRUE);
	    // Delete new'd characters
	    NADELETEBASIC(ddQuery, getHeap());
	    ddQuery = NULL;

	    if (cliRC < 0)
	      {
		if (step_ == TRUNCATE_TABLE_)
		  {
		    step_ = ERROR_;
		    break;
		  }

		// delete data returned an error.
		// As a last resort, drop this table.
		SQL_EXEC_ClearDiagnostics(NULL);
		step_ = DROP_AND_ERROR_;
		break;
	      }

	    if (step_ == TRUNCATE_TABLE_AND_ERROR_)
	      {
		step_ = ERROR_;
	      }
	    else
	      step_ = INSERT_ROWS_;
	  }
	break;

	case INSERT_ROWS_:
	  {
            if (doSidetreeInsert_)
              step_ = INSERT_SIDETREE_;
            else
              step_ = INSERT_VSBB_;
	  }
	break;

	case INSERT_SIDETREE_:
	  {
            ex_queue_entry * up_entry = qparent_.up->getTailEntry();
            ComDiagsArea *diagsArea = up_entry->getDiagsArea();
	    // issue the insert command
	    cliInterface()->clearGlobalDiags();

	    // All internal queries issued from CliInterface assume that
	    // they are in ISO_MAPPING.
	    // That causes mxcmp to use the default charset as iso88591
	    // for unprefixed literals.
	    // The insert...select being issued out here contains the user
	    // specified query and any literals in that should be using
	    // the default_charset.
	    // So we send the isoMapping charset instead of the
	    // enum ISO_MAPPING.
	    Int32 savedIsoMapping = 
	      currContext->getSessionDefaults()->getIsoMappingEnum();
	    cliInterface()->setIsoMapping
	      (currContext->getSessionDefaults()->getIsoMappingEnum());
            redriveCTAS = currContext->getSessionDefaults()->getRedriveCTAS();
            if (redriveCTAS)
            {
              if (childQueryId_ == NULL)
                childQueryId_ = new (getHeap()) char[ComSqlId::MAX_QUERY_ID_LEN+1];
              childQueryIdLen_ = ComSqlId::MAX_QUERY_ID_LEN;
              cliRC = cliInterface()->executeImmediatePrepare2(
		 ctaTdb().siQuery_,
                 childQueryId_,
                 &childQueryIdLen_,
                 &childQueryCostInfo_,
                 &childQueryCompStatsInfo_,
                 NULL, NULL,
                 &rowsAffected,TRUE);
              if (cliRC >= 0)
              {
                childQueryId_[childQueryIdLen_] = '\0';
                Statement *ctasStmt = masterGlob->getStatement();
                cliRC = ctasStmt->setChildQueryInfo(diagsArea,
                  childQueryId_, childQueryIdLen_,
                  &childQueryCostInfo_, &childQueryCompStatsInfo_);
                if (cliRC < 0)
                {
                  step_ = HANDLE_ERROR_;
                  break;
                }
                if (outputBuf_ == NULL)
                  outputBuf_ = new (getHeap()) char[ComSqlId::MAX_QUERY_ID_LEN+100]; // 
                str_sprintf(outputBuf_, "childQidBegin: %s ", childQueryId_);
                moveRowToUpQueue(outputBuf_);
                step_ = INSERT_SIDETREE_EXECUTE_; 
                return WORK_RESCHEDULE_AND_RETURN;
              }
            }
            else
            {
              cliRC = cliInterface()->executeImmediatePrepare(
		 ctaTdb().siQuery_,
		 NULL, NULL,
		 &rowsAffected,TRUE);
            }
	    cliInterface()->setIsoMapping(savedIsoMapping);
	    if (cliRC < 0)
	    {
              // sidetree insert prepare failed. 
              // Try vsbb insert
	      step_ = INSERT_VSBB_;
	      break;
	    }
            step_ = INSERT_SIDETREE_EXECUTE_;
          }
          break;
        case INSERT_SIDETREE_EXECUTE_:
          {
	    cliRC = cliInterface()->executeImmediateExec(
		 ctaTdb().siQuery_,
		 NULL, NULL, TRUE,
		 &rowsAffected);
	    if (cliRC < 0)
            {
              cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
              step_ = HANDLE_ERROR_;
            }
            else
            {
	      masterGlob->setRowsAffected(rowsAffected);
	      step_ = UPD_STATS_;
            }
            redriveCTAS = currContext->getSessionDefaults()->getRedriveCTAS();
            if (redriveCTAS)
            {
              str_sprintf(outputBuf_, "childQidEnd: %s ", childQueryId_);
              moveRowToUpQueue(outputBuf_);
              return WORK_RESCHEDULE_AND_RETURN;
            }
	  }
	  break;

	case INSERT_VSBB_:
	  {
            ex_queue_entry * up_entry = qparent_.up->getTailEntry();
            ComDiagsArea *diagsArea = up_entry->getDiagsArea();

	    // issue the insert command
	    Int64 rowsAffected = 0;
	    Int32 savedIsoMapping = 
	      currContext->getSessionDefaults()->getIsoMappingEnum();
	    cliInterface()->setIsoMapping
	      (currContext->getSessionDefaults()->getIsoMappingEnum());
            NABoolean redriveCTAS = currContext->getSessionDefaults()->getRedriveCTAS();
            if (redriveCTAS)
            {
              if (childQueryId_ == NULL)
                childQueryId_ = new (getHeap()) char[ComSqlId::MAX_QUERY_ID_LEN+1];
              cliRC = cliInterface()->executeImmediatePrepare2(
		 ctaTdb().viQuery_,
                 childQueryId_,
                 &childQueryIdLen_,
                 &childQueryCostInfo_,
                 &childQueryCompStatsInfo_,
                 NULL, NULL,
                 &rowsAffected,TRUE);
              cliInterface()->setIsoMapping(savedIsoMapping);
	      if (cliRC >= 0)
              {
                childQueryId_[childQueryIdLen_] = '\0';
                Statement *ctasStmt = masterGlob->getStatement();
                cliRC = ctasStmt->setChildQueryInfo(diagsArea,
                  childQueryId_, childQueryIdLen_,
                  &childQueryCostInfo_, &childQueryCompStatsInfo_);
                if (cliRC < 0)
                {
                  step_ = HANDLE_ERROR_;
                  break;
                }
                if (outputBuf_ == NULL)
                  outputBuf_ = new (getHeap()) char[ComSqlId::MAX_QUERY_ID_LEN+100]; // 
                str_sprintf(outputBuf_, "childQidBegin: %s ", childQueryId_);
                moveRowToUpQueue(outputBuf_);
                step_ = INSERT_VSBB_EXECUTE_; 
                return WORK_RESCHEDULE_AND_RETURN;
              }
              else
              {
                step_ = HANDLE_ERROR_;
                break;
              }
            }
            else
            {
	      cliRC = cliInterface()->executeImmediate(
		   ctaTdb().viQuery_,
		   NULL, NULL, TRUE,
		   &rowsAffected,TRUE);
              cliInterface()->setIsoMapping(savedIsoMapping);
	      if (cliRC < 0)
              {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
                step_ = HANDLE_ERROR_;
              }
              else
              {
                masterGlob->setRowsAffected(rowsAffected);
                step_ = UPD_STATS_;
              }
            }
          }       
	  break;

        case INSERT_VSBB_EXECUTE_:
          {
	    cliRC = cliInterface()->executeImmediateExec(
		 ctaTdb().viQuery_,
		 NULL, NULL, TRUE,
		 &rowsAffected);
	    if (cliRC < 0)
	    {
              cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
              step_ = HANDLE_ERROR_;
            }
            else
            {
              str_sprintf(outputBuf_, "childQidEnd: %s ", childQueryId_);
              moveRowToUpQueue(outputBuf_);
              masterGlob->setRowsAffected(rowsAffected);
	      step_ = UPD_STATS_;
              return WORK_RESCHEDULE_AND_RETURN;
            }
	  }
	  break;

	case UPD_STATS_:
	  {
	    if ((ctaTdb().threshold_ == -1) ||
		((ctaTdb().threshold_ > 0) &&
		 (masterGlob->getRowsAffected() < ctaTdb().threshold_)))
	      {
		step_ = DONE_;
		break;
	      }

	    // issue the upd stats command 
	    char * usQuery =
	      new(getHeap()) char[strlen(ctaTdb().usQuery_) + 10 + 1];

	    str_sprintf(usQuery, ctaTdb().usQuery_,
			masterGlob->getRowsAffected());

	    cliRC = cliInterface()->executeImmediate(usQuery,NULL,NULL,TRUE,NULL,TRUE);
	    NADELETEBASIC(usQuery, getHeap());
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		step_ = HANDLE_ERROR_;
		break;
	      }
	    
	    step_ = DONE_;
	  }
	break;

	case HANDLE_ERROR_:
	  {
	    if ((ctaTdb().ctQuery_) &&
		(ctaTdb().loadIfExists()))
	      {
		// error case and 'load if exists' specified.
                // if non-transactional sidetreeInsert was used to load
                // data, do not drop the table, only delete data from it.
                // if transactional insert was used to load data,
                // return error. Xn rollback will undo the added rows.
                if (doSidetreeInsert_)
                  step_ = TRUNCATE_TABLE_AND_ERROR_;
                else
                  step_ = ERROR_;
	      }
	    else
	      step_ = DROP_AND_ERROR_;
	  }
	break;

	case DROP_AND_ERROR_:
	  {
	    if ((ctaTdb().ctQuery_) &&
		(NOT tableExists_))
	      {
		// this is an error case, drop the table
		char * dtQuery = 
		  new(getMyHeap()) char[strlen("DROP TABLE CASCADE; ") + 
				       strlen(ctaTdb().getTableName()) +
				       100];
		strcpy(dtQuery, "DROP TABLE ");
		strcat(dtQuery, ctaTdb().getTableName());
		strcat(dtQuery, " CASCADE;");
		cliRC = cliInterface()->executeImmediate(dtQuery);

		// Delete new'd characters
		NADELETEBASIC(dtQuery, getHeap());
		dtQuery = NULL;
	      }

	    if (step_ == DROP_AND_ERROR_)
	      step_ = ERROR_;
	    else
	      step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
            //restore binlog
            cliInterface()->executeImmediate("control query default DONOT_WRITE_XDC_DDL reset ");
	    retcode = handleDone();
	    if (retcode == 1)
	       return WORK_OK;
	    step_ = INITIAL_;
	    return WORK_OK;
	  }
	break;

	case ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	       return WORK_OK;
	    step_ = DONE_;
	  }
	break;
	} // switch
    } // while
  
  return WORK_OK;

}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilCreateTableAsTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilCreateTableAsPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilCreateTableAsPrivateState::ExExeUtilCreateTableAsPrivateState()
{
}

ExExeUtilCreateTableAsPrivateState::~ExExeUtilCreateTableAsPrivateState()
{
};

#define SETSTEP(s) setStep(s, __LINE__)

static THREAD_P bool sv_checked_yet = false;
static THREAD_P bool sv_logStep = false;

////////////////////////////////////////////////////////////////
// Methods for classes ExExeUtilAqrWnrInsertTdb and 
// ExExeUtilAqrWnrInsertTcb
///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilAqrWnrInsertTdb::build(ex_globals * glob)
{
  // build the child first
  ex_tcb * childTcb = child_->build(glob);

  ExExeUtilAqrWnrInsertTcb *exe_util_tcb;

  exe_util_tcb = 
      new(glob->getSpace()) ExExeUtilAqrWnrInsertTcb(*this, childTcb, glob);
  
  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}


ExExeUtilAqrWnrInsertTcb::ExExeUtilAqrWnrInsertTcb(
     const ComTdbExeUtilAqrWnrInsert & exe_util_tdb,
     const ex_tcb * child_tcb, 
     ex_globals * glob)
  : ExExeUtilTcb( exe_util_tdb, child_tcb, glob)
  , step_(INITIAL_)
  , targetWasEmpty_(false)
{
}

ExExeUtilAqrWnrInsertTcb::~ExExeUtilAqrWnrInsertTcb()
{
  // mjh - tbd :
  // is base class dtor called?
}

Int32 ExExeUtilAqrWnrInsertTcb::fixup()
{
  return ex_tcb::fixup();
}


void ExExeUtilAqrWnrInsertTcb::setStep(Step newStep, int lineNum)
{
  static bool sv_checked_yet = false;
  static bool sv_logStep = false;
  
  step_ = newStep;

  if (!sv_checked_yet)
  {
    sv_checked_yet = true;
  	char *logST = getenv("LOG_AQR_WNR_INSERT");
	  if (logST && *logST == '1')
	    sv_logStep = true;
	}
  if (!sv_logStep)
    return;

  if (NULL ==
      getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals())
    return;

  char *stepStr = (char *) "UNKNOWN";
  switch (step_)
  {
    case INITIAL_:
      stepStr = (char *) "INITIAL_";
      break;
    case LOCK_TARGET_:
      stepStr = (char *) "LOCK_TARGET_";
      break;
    case IS_TARGET_EMPTY_:
      stepStr = (char *) "IS_TARGET_EMPTY_";
      break;
    case SEND_REQ_TO_CHILD_:
      stepStr = (char *) "SEND_REQ_TO_CHILD_";
      break;
    case GET_REPLY_FROM_CHILD_:
      stepStr = (char *) "GET_REPLY_FROM_CHILD_";
      break;
    case CLEANUP_CHILD_:
      stepStr = (char *) "CLEANUP_CHILD_";
      break;
    case CLEANUP_TARGET_:
      stepStr = (char *) "CLEANUP_TARGET_";
      break;
    case ERROR_:
      stepStr = (char *) "ERROR_";
      break;
    case DONE_:
      stepStr = (char *) "DONE_";
      break;
  }

  cout << stepStr << ", line " << lineNum << endl;
}

// Temporary.
#define VERIFY_CLI_UTIL 1

ExWorkProcRetcode ExExeUtilAqrWnrInsertTcb::work()
{
  ExWorkProcRetcode rc = WORK_OK;
  Lng32 cliRC = 0;

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli *currContext = masterGlob->getCliGlobals()->currContext();

  while (! (qparent_.down->isEmpty()  || qparent_.up->isFull()) )
  {
    ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
    ex_queue_entry * pentry_up = qparent_.up->getTailEntry();

    switch (step_)
    {
      case INITIAL_:
      {
        targetWasEmpty_ = false;
        masterGlob->resetAqrWnrInsertCleanedup();

        if (getDiagsArea())
          getDiagsArea()->clear();

        if (ulTdb().doLockTarget())
          SETSTEP(LOCK_TARGET_);
        else
          SETSTEP(IS_TARGET_EMPTY_);

        break;
      }

      case LOCK_TARGET_:
      {
        SQL_EXEC_ClearDiagnostics(NULL);

        query_ = new(getGlobals()->getDefaultHeap()) char[1000];
        str_sprintf(query_, "lock table %s in share mode;",
                    ulTdb().getTableName());
        Lng32 len = 0;
        char dummyArg[128];
  #ifdef VERIFY_CLI_UTIL
        for (size_t i = 0; i < sizeof(dummyArg); i++)
          dummyArg[i] = i;
  #endif
        cliRC = cliInterface()->executeImmediate(
                                    query_, dummyArg, &len, FALSE);

  #ifdef VERIFY_CLI_UTIL
        ex_assert (len == 0, "lock table returned data");
        for (size_t i = 0; i < sizeof(dummyArg); i++)
          ex_assert( dummyArg[i] == i, "lock table returned data");
  #endif

        NADELETEBASIC(query_, getMyHeap());

        if (cliRC < 0)
        {
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
          SETSTEP(ERROR_);
        }
        else
          SETSTEP(IS_TARGET_EMPTY_);

        // Allow the query to be canceled.
        return WORK_CALL_AGAIN;
      }

      case IS_TARGET_EMPTY_:
      {
        SQL_EXEC_ClearDiagnostics(NULL);

        query_ = new(getGlobals()->getDefaultHeap()) char[1000];
        str_sprintf(query_, "select row count from %s;",
                    ulTdb().getTableName());
        Lng32 len = 0;
        Int64 rowCount = 0;
        cliRC = cliInterface()->executeImmediate(query_,
                                  (char*)&rowCount,
                                  &len, FALSE);
        NADELETEBASIC(query_, getMyHeap());
        if (cliRC < 0)
        {
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
          SETSTEP(ERROR_);
        }
        else
        {
          targetWasEmpty_ = (rowCount == 0);
          SETSTEP(SEND_REQ_TO_CHILD_);
        }
	      
        // Allow the query to be canceled.
        return WORK_CALL_AGAIN;
        }

      case SEND_REQ_TO_CHILD_:
      {
        if (qchild_.down->isFull())
          return WORK_OK;

        ex_queue_entry * centry = qchild_.down->getTailEntry();

        centry->downState.request = ex_queue::GET_ALL;
        centry->downState.requestValue = 
                pentry_down->downState.requestValue;
        centry->downState.parentIndex = qparent_.down->getHeadIndex();

        // set the child's input atp
        centry->passAtp(pentry_down->getAtp());

        qchild_.down->insert();

        SETSTEP(GET_REPLY_FROM_CHILD_);
      }
      break;

      case GET_REPLY_FROM_CHILD_:
      {
        // if nothing returned from child. Get outta here.
        if (qchild_.up->isEmpty())
          return WORK_OK;

        ex_queue_entry * centry = qchild_.up->getHeadEntry();

        // DA from child, if any, is copied to parent up entry.
        pentry_up->copyAtp(centry);

        switch(centry->upState.status)
        {
          case ex_queue::Q_NO_DATA:
          {
            SETSTEP(DONE_);
            break;
          }
          case ex_queue::Q_SQLERROR:
          {
            SETSTEP(CLEANUP_CHILD_);
            break;
          }
          default:
          {
            ex_assert(0, "Invalid child_status");
            break;
          }
        }
        qchild_.up->removeHead();
        break;
      }
  
      case CLEANUP_CHILD_:
      {
        bool lookingForQnoData = true;
        do 
        {
          if (qchild_.up->isEmpty())
            return WORK_OK;
          ex_queue_entry * centry = qchild_.up->getHeadEntry();
          if (centry->upState.status == ex_queue::Q_NO_DATA)
          {
            lookingForQnoData = false;
            bool cleanupTarget = false;
            if (targetWasEmpty_ &&
                (pentry_down->downState.request != ex_queue::GET_NOMORE))
            {
              // Find out if any messages were sent to insert TSE sessions,
              // because we'd like to skip CLEANUP_TARGET_ if not.
              const ExStatisticsArea *constStatsArea = NULL;
              Lng32 cliRc = SQL_EXEC_GetStatisticsArea_Internal(
                     SQLCLI_STATS_REQ_QID,
                     masterGlob->getStatement()->getUniqueStmtId(),
                     masterGlob->getStatement()->getUniqueStmtIdLen(),
                     -1, SQLCLI_SAME_STATS,
                     constStatsArea);
              ExStatisticsArea * statsArea = 
                    (ExStatisticsArea *) constStatsArea;

              if (cliRc < 0 || !statsArea)
              {
                // Error or some problem getting stats.
                cleanupTarget = true;
              }
              else if (!statsArea->getMasterStats() ||
                    statsArea->getMasterStats()->getStatsErrorCode() != 0)
              {
                // Partial success getting stats. Can't trust results.
                cleanupTarget = true;
              }
              else if (statsArea->anyHaveSentMsgIUD())  
              {
                // Stats shows that IUD started. Must cleanup.
                cleanupTarget = true;
              }
            }

            if (cleanupTarget)
              SETSTEP(CLEANUP_TARGET_);
            else
              SETSTEP(ERROR_);
          }
          qchild_.up->removeHead();
        } while (lookingForQnoData);
        break;
      }

      case CLEANUP_TARGET_:
      {
        SQL_EXEC_ClearDiagnostics(NULL);

        query_ = new(getGlobals()->getDefaultHeap()) char[1000];
        str_sprintf(query_, "delete with no rollback from %s;", ulTdb().getTableName());
        Lng32 len = 0;
        char dummyArg[128];
        cliRC = cliInterface()->executeImmediate(
                                    query_, dummyArg, &len, FALSE);

        NADELETEBASIC(query_, getMyHeap());

        if (cliRC < 0)
        {
        // mjh - tbd - warning or EMS message to give context to error on
        // delete after error on the insert?
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        }
        else
          masterGlob->setAqrWnrInsertCleanedup();
        SETSTEP(ERROR_);
        break;
      }

      case ERROR_:
      {
        if (pentry_down->downState.request != ex_queue::GET_NOMORE)
        {
          if (getDiagsArea())
          {
            // Any error from child already put a DA into pentry_up.
            if (NULL == pentry_up->getDiagsArea())
            {
              ComDiagsArea * da = ComDiagsArea::allocate(
                                    getGlobals()->getDefaultHeap());
              pentry_up->setDiagsArea(da);
            }
            pentry_up->getDiagsArea()->mergeAfter(*getDiagsArea());
            getDiagsArea()->clear();
            SQL_EXEC_ClearDiagnostics(NULL);
          }
          pentry_up->upState.status = ex_queue::Q_SQLERROR;
          pentry_up->upState.parentIndex = 
                                   pentry_down->downState.parentIndex;
          pentry_up->upState.downIndex = qparent_.down->getHeadIndex();
          qparent_.up->insert();
        }
        SETSTEP(DONE_);
        break;
      }

      case DONE_:
      {
	      // Return EOF.
	      pentry_up->upState.parentIndex =  pentry_down->downState.parentIndex;
        pentry_up->upState.setMatchNo(0);
	      pentry_up->upState.status = ex_queue::Q_NO_DATA;
	        
	      // insert into parent
	      qparent_.up->insert();
	        
	      SETSTEP(INITIAL_);
	      qparent_.down->removeHead();
	        
    	  break;
	    }
    } // switch    
  }  // while

  return WORK_OK;
} 

ExWorkProcRetcode ExExeUtilAqrWnrInsertTcb::workCancel()
{
  if (!(qparent_.down->isEmpty()) &&
      (ex_queue::GET_NOMORE == 
       qparent_.down->getHeadEntry()->downState.request))
  {
    switch (step_)
    {
    case INITIAL_:
	  SETSTEP(DONE_);
	  break;
    case LOCK_TARGET_:
	  ex_assert (0, 
          	"work method doesn't return with step_ set to LOCK_TARGET_.");
	  break;
    case IS_TARGET_EMPTY_:
          ex_assert (0,
                "work method doesn't return with step_ set "
                "to IS_TARGET_EMPTY_.");
	  break;
    case SEND_REQ_TO_CHILD_:
  	SETSTEP(DONE_);
	  break;
    case GET_REPLY_FROM_CHILD_:
	// Assuming that the entire query is canceled. Canceled queries 
	// are not AQR'd. Will cleanup without purging target.
	  qchild_.down->cancelRequest();
	  SETSTEP(CLEANUP_CHILD_);
	break;
    case CLEANUP_CHILD_:
         // CLEANUP_CHILD_ does the right thing when canceling.
	  break;
    case ERROR_:
         // ERROR_ does the right thing when canceling.
  	break;
    case CLEANUP_TARGET_:
	  ex_assert (0, 
          "work method doesn't return with step_ set to CLEANUP_TARGET_,.");
	  break;
    case DONE_:
	ex_assert (0, 
	"work method doesn't return with step_ set to DONE_.");
	break;
    }
  }
  return WORK_OK;
}

#define changeStep(x) changeAndTraceStep(x, __LINE__)

////////////////////////////////////////////////////////////////
// build for class ExExeUtilHbaseLoadTdb
///////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilHBaseBulkLoadTdb::build(ex_globals * glob)
{
  ExExeUtilHBaseBulkLoadTcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilHBaseBulkLoadTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilHbaseLoadTcb
///////////////////////////////////////////////////////////////
ExExeUtilHBaseBulkLoadTcb::ExExeUtilHBaseBulkLoadTcb(
    const ComTdbExeUtil & exe_util_tdb,
    ex_globals * glob)
: ExExeUtilTcb( exe_util_tdb, NULL, glob),
  step_(INITIAL_),
  nextStep_(INITIAL_),
  rowsAffected_(0),
  loggingLocation_(NULL),
  ustatNonEmptyTable_(FALSE)
{
  ehi_ = NULL;
  qparent_.down->allocatePstate(this);
}

ExExeUtilHBaseBulkLoadTcb::~ExExeUtilHBaseBulkLoadTcb()
{
   if (loggingLocation_ != NULL) {
      NADELETEBASIC(loggingLocation_, getHeap());
      loggingLocation_ = NULL;
   }
}

NABoolean ExExeUtilHBaseBulkLoadTcb::generateTrafSampleTable(const char* cTableName,const char* cSampleTableName)
{
  if ( !cTableName )
    return FALSE;

  NABoolean bRet = TRUE;

  sSampleTableName_ = cSampleTableName;
  NAString sQuery = "DROP TABLE IF EXISTS ";
  sQuery.append(sSampleTableName_);
  Lng32 nRet = cliInterface()->executeImmediate(sQuery.data());
  if (nRet < 0)
    return FALSE;

  NAString sOptions = " WITHOUT LOB COLUMNS ";
  Lng32 maxlength = (Lng32) CmpCommon::getDefaultNumeric(USTAT_MAX_CHAR_COL_LENGTH_IN_BYTES);
  char temp[20];
  sprintf(temp,"%d",maxlength);
  sOptions.append("LIMIT COLUMN LENGTH TO ")
          .append(temp);
  NAString sHbaseCatalog;
  CmpCommon::getDefault(HBASE_CATALOG, sHbaseCatalog, FALSE);
  NAString sTableName = cTableName;
  size_t idx = sTableName.index(".");
  NAString sName;
  sName.append(sTableName,idx);
  if ( sName != sHbaseCatalog )
    sOptions.append(" WITH PARTITIONS ");

  sQuery = "CREATE TABLE ";
  sQuery.append(sSampleTableName_)
        .append(" LIKE ")
        .append(NAString(cTableName))
        .append(sOptions);
  nRet = cliInterface()->executeImmediate(sQuery.data());
  if (nRet < 0)
    return FALSE;
  return TRUE;
}

// this method is called if there are params in the SELECT part of
// the user 'load into' query.
// Those param values are input from root and then passed to
// the internal 'load transform' query at hblTdb().ldQuery_.
short ExExeUtilHBaseBulkLoadTcb::loadWithParams(ComDiagsArea *&diagsArea)
{
  Lng32 cliRC = 0;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();

  // buffer where input values will be moved
  char inputParamsBuf[hblTdb().inputRowlen_];
  workAtp_->getTupp(hblTdb().workAtpIndex())
    .setDataPointer(inputParamsBuf);
  
  ex_expr::exp_return_type exprRetCode =
    hblTdb().inputExpr()->eval(pentry_down->getAtp(),
                              workAtp_);
  if (exprRetCode == ex_expr::EXPR_ERROR)
    {
      diagsArea = moveDiagsAreaFromEntry(pentry_down);
      return -1;
    }

  ExpTupleDesc * inputExprTD =
    hblTdb().workCriDesc_->getTupleDescriptor
    (hblTdb().workAtpIndex());
  
  char *loadQuery = hblTdb().ldQuery_;
  cliRC = cliInterface()->fetchRowsPrologue(loadQuery, TRUE/*noexec*/);
  if (cliRC < 0)
    {
      cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea);
      return cliRC;
    }

  // cliInterface()->inputBuf() points to the input row that is sent
  // to the load transform query.
  // Retrieve param values from inputParamsBuf and set those values in
  // inputBuf()
  for (UInt32 i = 0; i < inputExprTD->numAttrs(); i++)
    {
      Attributes *srcAttr = inputExprTD->getAttr(i);

      char *srcVal = NULL;
      Int32 srcLen = 0;
      NABoolean srcIsNull = FALSE;
      if (srcAttr->getNullFlag())
        {
          char *srcNullVal = &inputParamsBuf[srcAttr->getNullIndOffset()];
          if (srcNullVal[0])
            srcIsNull = TRUE;
        }

      if (NOT srcIsNull)
        {
          srcLen = srcAttr->getLength(&inputParamsBuf[srcAttr->getVCLenIndOffset()]);
          srcVal = &inputParamsBuf[srcAttr->getOffset()];
        }

      Lng32 dummy;
      Int32 tgtParamVcIndLen = 0;
      Int32 tgtParamOffset = 0;
      Int32 tgtParamNullIndOffset = 0;
      cliInterface()->getAttributes(
           (i+1), TRUE,
           dummy, dummy, 
           tgtParamVcIndLen, &tgtParamNullIndOffset, &tgtParamOffset);
        
      Int16 nullV = (srcIsNull ? -1 : 0);
      memcpy(&cliInterface()->inputBuf()[tgtParamNullIndOffset], (char*)&nullV, sizeof(nullV));
      
      if (NOT srcIsNull)
        {
          if (tgtParamVcIndLen > 0)
            {
              if (tgtParamVcIndLen == sizeof(short))
                {
                  Int16 len = srcLen;
                  memcpy(&cliInterface()->inputBuf()[tgtParamOffset], (char*)&len, sizeof(len));
                }
              else
                {
                  Int32 len = srcLen;
                  memcpy(&cliInterface()->inputBuf()[tgtParamOffset], (char*)&len, sizeof(len));
                }
            } // is VC

          memcpy(&cliInterface()->inputBuf()[tgtParamOffset + tgtParamVcIndLen],
                 srcVal, srcLen);
        } // !nullVal
    }// for

  // input values are filled in input buffer.
  // execute the internal load query.
  cliRC = cliInterface()->executeImmediateExec(NULL, NULL, NULL, FALSE,
                                               &rowsAffected_);
  if (cliRC < 0)
    {
      cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea);
      return cliRC;
    }
  
  return cliRC;
}

//////////////////////////////////////////////////////
// work() for ExExeUtilHbaseLoadTcb
//////////////////////////////////////////////////////
short ExExeUtilHBaseBulkLoadTcb::work()
{
  Lng32 cliRC = 0;
  short retcode = 0;
  short rc;
  Lng32 errorRowCount = 0;
  int len;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate = *((ExExeUtilPrivateState*) pentry_down->pstate);

  ExMasterStmtGlobals *masterGlob = getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
  ContextCli *currContext = masterGlob->getStatement()->getContext();
  ExTransaction *ta = currContext->getTransaction();



  while (1)
  {
    switch (step_)
    {
    case INITIAL_:
    {
      NABoolean xnAlreadyStarted = ta->xnInProgress();

      if (xnAlreadyStarted  &&
          //a transaction is active when we load/populate an indexe table
          !hblTdb().getIndexTableOnly())
      {
        //8111 - Transactions are not allowed with Bulk load.
        ExRaiseSqlError(getHeap(), &diagsArea_, -8111, NULL, NULL, NULL, "Bulk load");
        step_ = LOAD_ERROR_;
          break;
      }

      if (setStartStatusMsgAndMoveToUpQueue(" LOAD", &rc))
        return rc;

      if (hblTdb().getUpsertUsingLoad())
        hblTdb().setPreloadCleanup(FALSE);

      if (hblTdb().getTruncateTable())
      {
        step_ = TRUNCATE_TABLE_;
        break;
      }

        // Table will not be truncated, so make sure it is empty if Update
        // Stats has been requested. We obviously have to do this check before
        // the load, but if the table is determined to be non-empty, the
        // message is deferred until the UPDATE_STATS_ step.
        if (hblTdb().getUpdateStats())
        {
          NAString selectFirstQuery = "select [first 1] 0 from ";
          selectFirstQuery.append(hblTdb().getTableName()).append(";");
          cliRC = cliInterface()->executeImmediate(selectFirstQuery.data());
          if (cliRC < 0)
          {
            step_ = LOAD_END_ERROR_;
            break;
          }
          else if (cliRC != 100)
            ustatNonEmptyTable_ = TRUE;  // So we can display msg later
        }

      step_ = LOAD_START_;
    }
    break;

    case TRUNCATE_TABLE_:
    {
      if (setStartStatusMsgAndMoveToUpQueue(" PURGE DATA",&rc, 0, TRUE))
        return rc;

        // Set the parserflag to prevent privilege checks in purgedata
        NABoolean parserFlagSet = FALSE;
        if ((masterGlob->getStatement()->getContext()->getSqlParserFlags() & 0x20000) == 0)
        {
          parserFlagSet = TRUE;
          masterGlob->getStatement()->getContext()->setSqlParserFlags(0x20000);
        }

      //for now the purgedata statement does not keep the partitions
      char * ttQuery =
          new(getMyHeap()) char[strlen("PURGEDATA  ; ") +
                                strlen(hblTdb().getTableName()) +
                                100];
      strcpy(ttQuery, "PURGEDATA  ");
      strcat(ttQuery, hblTdb().getTableName());
      strcat(ttQuery, ";");

      Lng32 len = 0;
      Int64 rowCount = 0;
      cliRC = cliInterface()->executeImmediate(ttQuery, NULL,NULL,TRUE,NULL,TRUE);
      NADELETEBASIC(ttQuery, getHeap());
      ttQuery = NULL;

        if (parserFlagSet)
          masterGlob->getStatement()->getContext()->resetSqlParserFlags(0x20000);

      if (cliRC < 0)
      {
        cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        step_ = LOAD_ERROR_;
        break;
      }
      step_ = LOAD_START_;

      setEndStatusMsg(" PURGE DATA", 0, TRUE);
    }
    break;

    case LOAD_START_:
    {
      if (setCQDs() <0)
      {
        step_ = LOAD_END_ERROR_;
        break;
      }
      int jniDebugPort = 0;
      int jniDebugTimeout = 0;
      ehi_ = ExpHbaseInterface::newInstance(getGlobals()->getDefaultHeap(),
                                              (char*)"", //Later may need to change to hblTdb.server_,
                                              (char*)"", //Later may need to change to hblTdb.zkPort_);
                                              COM_STORAGE_HBASE,
                                              FALSE);
      retcode = ehi_->initHBLC();
      if (retcode == 0) 
        retcode = ehi_->createCounterTable(hblTdb().getErrCountTable(), (char *)"ERRORS");
      if (retcode != 0 ) 
      {
         Lng32 cliError = 0;
        Lng32 intParam1 = -retcode;
        ExRaiseSqlError(getHeap(), &diagsArea_,
                          (ExeErrorCode)(8448), NULL, &intParam1,
                          &cliError, NULL,
                          " ",
                          getHbaseErrStr(retcode),
                          (char *)GetCliGlobals()->getJniErrorStr());
        step_ = LOAD_END_ERROR_;
        break;
      }
      if (hblTdb().getPreloadCleanup())
        step_ = PRE_LOAD_CLEANUP_;
      else
      {
        step_ = PREPARATION_;
        if (hblTdb().getRebuildIndexes() || hblTdb().getHasUniqueIndexes())
          step_ = DISABLE_INDEXES_;
      }
    }
    break;

    case PRE_LOAD_CLEANUP_:
    {
      if (setStartStatusMsgAndMoveToUpQueue(" CLEANUP", &rc, 0, TRUE))
           return rc;

        //Cleanup files
        char * clnpQuery =
          new(getMyHeap()) char[strlen("LOAD CLEANUP FOR TABLE ; ") +
                               strlen(hblTdb().getTableName()) +
                               100];
        strcpy(clnpQuery, "LOAD ");
        if (hblTdb().getUpdateStats() && !ustatNonEmptyTable_)
          strcat(clnpQuery,"WITH SAMPLE ");
        strcat(clnpQuery, "CLEANUP FOR TABLE ");
        if (hblTdb().getIndexTableOnly())
          strcat(clnpQuery, "TABLE(INDEX_TABLE ");
        strcat(clnpQuery, hblTdb().getTableName());
        if (hblTdb().getIndexTableOnly())
          strcat(clnpQuery, ")");
        strcat(clnpQuery, ";");

      cliRC = cliInterface()->executeImmediate(clnpQuery, NULL,NULL,TRUE,NULL,TRUE);

      NADELETEBASIC(clnpQuery, getHeap());
      clnpQuery = NULL;
      if (cliRC < 0)
      {
        cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        step_ = LOAD_END_ERROR_;
        break;
      }

      step_ = PREPARATION_;

      if (hblTdb().getRebuildIndexes() || hblTdb().getHasUniqueIndexes())
        step_ = DISABLE_INDEXES_;

      setEndStatusMsg(" CLEANUP", 0, TRUE);
    }
    break;
    case DISABLE_INDEXES_:
    {
      if (setStartStatusMsgAndMoveToUpQueue(" DISABLE INDEXES", &rc, 0, TRUE))
        return rc;

      // disable indexes before starting the load preparation. load preparation phase will
      // give an error if indexes are not disabled
      // For constarints --disabling/enabling constarints is not supported yet. in this case the user
      // needs to disable or drop the constraints manually before starting load. If constarints
      // exist load preparation will give an error
      char * diQuery =
          new(getMyHeap()) char[strlen("ALTER TABLE DISABLE ALL INDEXES   ; ") +
                                strlen(hblTdb().getTableName()) +
                                100];
      strcpy(diQuery, "ALTER TABLE  ");
      strcat(diQuery, hblTdb().getTableName());
      if (hblTdb().getRebuildIndexes())
        strcat(diQuery, " DISABLE ALL INDEXES ;");
      else
        strcat(diQuery, " DISABLE ALL UNIQUE INDEXES ;"); // has unique indexes and rebuild not specified
      cliRC = cliInterface()->executeImmediate(diQuery, NULL,NULL,TRUE,NULL,TRUE);

      NADELETEBASIC(diQuery, getMyHeap());
      diQuery = NULL;
      if (cliRC < 0)
      {
        cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        step_ = LOAD_END_ERROR_;
        break;
      }
      step_ = PREPARATION_;

      setEndStatusMsg(" DISABLE INDEXES", 0, TRUE);
    }
    break;

    case PREPARATION_:
    {
      short bufPos = 0;
      if (!hblTdb().getUpsertUsingLoad())
      {
        setLoggingLocation();
        bufPos = printLoggingLocation(0);
        if (setStartStatusMsgAndMoveToUpQueue(" LOADING DATA", &rc, bufPos, TRUE))
           return rc;
        else {
           step_ = LOADING_DATA_;
           return WORK_CALL_AGAIN;
        }  
      }
      else
          step_ = LOADING_DATA_;
    }
    break;

    case LOADING_DATA_:
    {
      if (!hblTdb().getUpsertUsingLoad())
      {
        if (hblTdb().getNoDuplicates())
          cliRC = holdAndSetCQD("TRAF_LOAD_PREP_SKIP_DUPLICATES", "OFF");
        else
          cliRC = holdAndSetCQD("TRAF_LOAD_PREP_SKIP_DUPLICATES", "ON");
        if (cliRC < 0)
        {
          step_ = LOAD_END_ERROR_;
          break;
        }

        if (loggingLocation_ != NULL)
           cliRC = holdAndSetCQD("TRAF_LOAD_ERROR_LOGGING_LOCATION", loggingLocation_);
        if (cliRC < 0)
        {
          step_ = LOAD_END_ERROR_;
          break;
        }

        rowsAffected_ = 0;
        char *loadQuery = hblTdb().ldQuery_;
          if (ustatNonEmptyTable_)
            {
              // is not empty, we have to retract the WITH SAMPLE option that
              // was added to the LOAD TRANSFORM statement when the original
              // bulk load statement was parsed.
              const char* sampleOpt = " WITH SAMPLE ";
              char* sampleOptPtr = strstr(loadQuery, sampleOpt);
              if (sampleOptPtr)
                memset(sampleOptPtr, ' ', strlen(sampleOpt));
            }
          //printf("*** Load stmt is %s\n",loadQuery);

          // If the WITH SAMPLE clause is included, set the internal exe util
          // parser flag to allow it.
          NABoolean parserFlagSet = FALSE;
          if (hblTdb().getUpdateStats() && !ustatNonEmptyTable_)
          {
            if ((masterGlob->getStatement()->getContext()->getSqlParserFlags() & 0x20000) == 0)
            {
              parserFlagSet = TRUE;
              masterGlob->getStatement()->getContext()->setSqlParserFlags(0x20000);
            }
            if (!generateTrafSampleTable(hblTdb().getTableName(),hblTdb().getSampleTableName()))
              {
                step_ = LOAD_END_ERROR_;
                break;
              }
          }
        ComDiagsArea *diagsArea = getDiagsArea();

        if (! hblTdb().inputExpr())
        {
          cliRC = cliInterface()->executeImmediate(loadQuery,
                                                   NULL,
                                                   NULL,
                                                   TRUE,
                                                   &rowsAffected_,
                                                   FALSE,
                                                   &diagsArea);
        }
      else
        {
          cliRC = loadWithParams(diagsArea);
        }

        if (parserFlagSet)
            masterGlob->getStatement()->getContext()->resetSqlParserFlags(0x20000);
        setDiagsArea(diagsArea);
        if (cliRC < 0)
        {
          rowsAffected_ = 0;
          step_ = ENABLE_INDEXES_AND_ERROR_;
          break;
        }
        else {
           step_ = COMPLETE_BULK_LOAD_;
           if (diagsArea != NULL) {
              ComCondition *cond;
              Lng32 entryNumber;
              while ((cond = diagsArea->findCondition(EXE_ERROR_ROWS_FOUND, &entryNumber)) != NULL) {
                 if (errorRowCount < cond->getOptionalInteger(0))
                    errorRowCount = cond->getOptionalInteger(0);
                 diagsArea->deleteWarning(entryNumber);
              }
              diagsArea->setRowCount(0);
           }
        }
        if (rowsAffected_ == 0)
          step_ = ENABLE_INDEXES_AND_LOAD_END_;

        sprintf(statusMsgBuf_,      "       Rows Processed: %ld %c",rowsAffected_+errorRowCount, '\n' );
        int len = strlen(statusMsgBuf_);
        sprintf(&statusMsgBuf_[len],"       Error Rows:     %d %c",errorRowCount, '\n' );
        len = strlen(statusMsgBuf_);
        setEndStatusMsg(" LOADING DATA", len, TRUE);
      }
      else
      {
        if (setStartStatusMsgAndMoveToUpQueue(" UPSERT USING LOAD ", &rc, 0, TRUE))
          return rc;

        rowsAffected_ = 0;
        char * upsQuery = hblTdb().ldQuery_;
        cliRC = cliInterface()->executeImmediate(upsQuery, NULL, NULL, TRUE, &rowsAffected_);

        upsQuery = NULL;
        if (cliRC < 0)
        {
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
          step_ = LOAD_ERROR_;
          break;
        }

          step_ = LOAD_END_;

          if (hblTdb().getRebuildIndexes() || hblTdb().getHasUniqueIndexes())
            step_ = POPULATE_INDEXES_;

        sprintf(statusMsgBuf_,"       Rows Processed: %ld %c",rowsAffected_, '\n' );
        int len = strlen(statusMsgBuf_);
        setEndStatusMsg(" UPSERT USING LOAD ", len, TRUE);
      }
    }
    break;

    case COMPLETE_BULK_LOAD_:
    {
      if (setStartStatusMsgAndMoveToUpQueue(" COMPLETION", &rc,0, TRUE))
        return rc;


      //TRAF_LOAD_TAKE_SNAPSHOT
      if (hblTdb().getNoRollback() )
        cliRC = holdAndSetCQD("TRAF_LOAD_TAKE_SNAPSHOT", "OFF");
      else
        cliRC = holdAndSetCQD("TRAF_LOAD_TAKE_SNAPSHOT", "ON");

      if (cliRC < 0)
      {
        step_ = LOAD_END_ERROR_;
        break;
      }

      //this case is mainly for debugging
      if (hblTdb().getKeepHFiles() &&
          !hblTdb().getSecure() )
      {
        if (holdAndSetCQD("COMPLETE_BULK_LOAD_N_KEEP_HFILES", "ON") < 0)
        { 
          restoreCQD("TRAF_LOAD_TAKE_SNAPSHOT");//Next step_ is LOAD_END_ERROR_, so don't care about the returns;
          step_ = LOAD_END_ERROR_;
          break;
        }
      }
      
      //Just before load complete, check if online backup lock
      //is in effect.
      if(checkAndWaitSnapshotInProgress((NAHeap *)getMyHeap()))
      {
        restoreCQD("TRAF_LOAD_TAKE_SNAPSHOT"); 
        step_ = LOAD_END_ERROR_;
        break;
      }  
      
      //complete load query
      char * clQuery =
          new(getMyHeap()) char[strlen("LOAD COMPLETE FOR TABLE  ; ") +
                               strlen(hblTdb().getTableName()) +
                               100];
        strcpy(clQuery, "LOAD ");
        if (hblTdb().getUpdateStats() && !ustatNonEmptyTable_)
          strcat(clQuery,"WITH SAMPLE ");
        strcat(clQuery, "COMPLETE FOR TABLE  ");
        if (hblTdb().getIndexTableOnly())
          strcat(clQuery, "TABLE(INDEX_TABLE ");
        strcat(clQuery, hblTdb().getTableName());
        if (hblTdb().getIndexTableOnly())
          strcat(clQuery, ")");
        strcat(clQuery, ";");

      cliRC = cliInterface()->executeImmediate(clQuery, NULL,NULL,TRUE,NULL,TRUE);

      NADELETEBASIC(clQuery, getMyHeap());
      clQuery = NULL;
      if (cliRC < 0)
         rowsAffected_ = 0;
      sprintf(statusMsgBuf_,      "       Rows Loaded:    %ld %c",rowsAffected_, '\n' );
      len = strlen(statusMsgBuf_);
      if (cliRC < 0)
      {
        rowsAffected_ = 0;
        cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        setEndStatusMsg(" COMPLETION", len, TRUE);
        restoreCQD("TRAF_LOAD_TAKE_SNAPSHOT"); 
        step_ = LOAD_END_ERROR_;
        break;
      }
      cliRC = restoreCQD("TRAF_LOAD_TAKE_SNAPSHOT");
      if (hblTdb().getRebuildIndexes() || hblTdb().getHasUniqueIndexes())
        step_ = POPULATE_INDEXES_;
      else if (hblTdb().getUpdateStats())
        step_ = UPDATE_STATS_;
      else
        step_ = LOAD_END_;

      setEndStatusMsg(" COMPLETION", len, TRUE);
    }
    break;

    case POPULATE_INDEXES_:
    {
      if (setStartStatusMsgAndMoveToUpQueue(" POPULATE INDEXES", &rc, 0, TRUE))
        return rc;
      else {
           step_ = POPULATE_INDEXES_EXECUTE_;
           return WORK_CALL_AGAIN;
      }
    }
    break;
    case POPULATE_INDEXES_EXECUTE_:
    {
      char * piQuery =
          new(getMyHeap()) char[strlen("POPULATE ALL INDEXES ON  ; ") +
                                strlen(hblTdb().getTableName()) +
                                100];
      if (hblTdb().getRebuildIndexes())
        strcpy(piQuery, "POPULATE ALL INDEXES ON   ");
      else
        strcpy(piQuery, "POPULATE ALL UNIQUE INDEXES ON   "); // has unique indexes and rebuild not used
      strcat(piQuery, hblTdb().getTableName());
      strcat(piQuery, " PURGEDATA ;");

      cliRC = cliInterface()->executeImmediate(piQuery, NULL,NULL,TRUE,NULL,TRUE);

      NADELETEBASIC(piQuery, getHeap());
      piQuery = NULL;

      if (cliRC < 0)
      {
        cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
        step_ = LOAD_END_ERROR_;
        break;
      }

      if (hblTdb().getUpdateStats())
         step_ = UPDATE_STATS_;
      else
        step_ = LOAD_END_;

        setEndStatusMsg(" POPULATE INDEXES", 0, TRUE);
      }
      break;

      case UPDATE_STATS_:
      {
        if (setStartStatusMsgAndMoveToUpQueue(" UPDATE STATS", &rc, 0, TRUE))
          return rc;
        else {
           step_ = UPDATE_STATS_EXECUTE_;
           return WORK_CALL_AGAIN;
        }
      }
      break;
      case UPDATE_STATS_EXECUTE_:
      {
        if (ustatNonEmptyTable_)
        {
          // Table was not empty prior to the load.
          step_ = LOAD_END_;
          sprintf(statusMsgBuf_,
                  "       UPDATE STATISTICS not executed: table %s not empty before load. %c",
                  hblTdb().getTableName(), '\n' );
          int len = strlen(statusMsgBuf_);
          setEndStatusMsg(" UPDATE STATS", len, TRUE);
          break;
        }

        char * ustatStmt =
          new(getMyHeap()) char[strlen("UPDATE STATS FOR TABLE  ON EVERY COLUMN; ") +
                               strlen(hblTdb().getTableName()) +
                               100];
        strcpy(ustatStmt, "UPDATE STATISTICS FOR TABLE ");
        strcat(ustatStmt, hblTdb().getTableName());
        strcat(ustatStmt, " ON EVERY COLUMN;");

        cliRC = holdAndSetCQD("USTAT_SAMPLE_TABLE_NAME", sSampleTableName_.data());
        if (cliRC < 0)
        {
          step_ = LOAD_END_ERROR_;
          break;
        }

        cliRC = cliInterface()->executeImmediate(ustatStmt, NULL, NULL, TRUE, NULL, TRUE);

        NADELETEBASIC(ustatStmt, getMyHeap());
        ustatStmt = NULL;

        if (cliRC < 0)
        {
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
          step_ = LOAD_END_ERROR_;
        }
        else
          step_ = UPDATE_STATS__END_;

        cliRC = restoreCQD("USTAT_SAMPLE_TABLE_NAME");
        if (cliRC < 0)
          step_ = LOAD_END_ERROR_;

        setEndStatusMsg(" UPDATE STATS", 0, TRUE);
    }
    break;
    case UPDATE_STATS__END_:
    {
      if (sSampleTableName_.length() > 0)
        {
          if (setStartStatusMsgAndMoveToUpQueue(" DROP TABLE", &rc,0, TRUE))
            return rc;
          NAString sQuery = "DROP TABLE ";
          sQuery.append(sSampleTableName_);
          cliRC = cliInterface()->executeImmediate(sQuery.data());
          if (cliRC < 0)
            {
              cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
              step_ = LOAD_END_ERROR_;
            }
          else
              step_ = LOAD_END_;
          setEndStatusMsg(" DROP TABLE", 0, TRUE);
        }
      break;
    }
    case RETURN_STATUS_MSG_:
    {
      if (moveRowToUpQueue(statusMsgBuf_,0,&rc))
        return rc;

      step_ = nextStep_;
    }
    break;

    case ENABLE_INDEXES_AND_ERROR_:
    case ENABLE_INDEXES_AND_LOAD_END_:
      {
        if (hblTdb().getRebuildIndexes() || hblTdb().getHasUniqueIndexes())
          {
            char * diQuery =
              new(getMyHeap()) char[strlen("ALTER TABLE ENABLE ALL INDEXES   ; ") +
                                    strlen(hblTdb().getTableName()) +
                                    100];
            strcpy(diQuery, "ALTER TABLE  ");
            strcat(diQuery, hblTdb().getTableName());
            if (hblTdb().getRebuildIndexes())
              strcat(diQuery, " ENABLE ALL INDEXES ;");
            else
              strcat(diQuery, " ENABLE ALL UNIQUE INDEXES ;");
            cliRC = cliInterface()->executeImmediate(diQuery, NULL,NULL,TRUE,NULL,TRUE);
            
            NADELETEBASIC(diQuery, getMyHeap());
            diQuery = NULL;
            if (cliRC < 0)
              {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
              }
          }

        if (step_ == ENABLE_INDEXES_AND_ERROR_)
          step_ = LOAD_END_ERROR_;
        else
          step_ = LOAD_END_;
      }
      break;

    case LOAD_END_:
    case LOAD_END_ERROR_:
    {
      if (restoreCQDs() < 0)
      {
        step_ = LOAD_ERROR_;
        break;
      }
      if (hblTdb().getContinueOnError() && ehi_)
      {
        ehi_->close();
        ehi_ = NULL;
      }
      if (step_ == LOAD_END_)
        step_ = DONE_;
      else
        step_ = LOAD_ERROR_;
    }
    break;

    case DONE_:
    {
      retcode = handleDone();
      if (retcode == 1)
         return WORK_OK;
      masterGlob->setRowsAffected(rowsAffected_);
      step_ = INITIAL_;
      return WORK_OK;
    }
    break;

    case LOAD_ERROR_:
    {
      retcode = handleError();
      if (retcode == 1)
         return WORK_OK;
      step_ = DONE_;
    }
    break;

    } // switch
  } // while

  return WORK_OK;

}

short ExExeUtilHBaseBulkLoadTcb::setCQDs()
{
  if (holdAndSetCQD("COMP_BOOL_226", "ON") < 0) { return -1;}
  // next cqd required to allow load into date/timestamp Traf columns from string Hive columns.
  // This is a common use case. Cqd can be removed when Traf hive access supports more Hive types.
  if (holdAndSetCQD("ALLOW_INCOMPATIBLE_OPERATIONS", "ON") < 0) { return -1;}
  if (hblTdb().getForceCIF())
  {
    if (holdAndSetCQD("COMPRESSED_INTERNAL_FORMAT", "ON") < 0) {return -1; }
    if (holdAndSetCQD("COMPRESSED_INTERNAL_FORMAT_BMO", "ON") < 0){ return -1; }
    if (holdAndSetCQD("COMPRESSED_INTERNAL_FORMAT_DEFRAG_RATIO", "100") < 0) { return -1;}
  }
  if (holdAndSetCQD("TRAF_LOAD_LOG_ERROR_ROWS", (hblTdb().getLogErrorRows()) ? "ON" : "OFF") < 0)
  { return -1;}

  if (holdAndSetCQD("TRAF_LOAD_CONTINUE_ON_ERROR", (hblTdb().getContinueOnError()) ? "ON" : "OFF") < 0)
  { return -1; }

  char strMaxRR[10];
  sprintf(strMaxRR,"%d", hblTdb().getMaxErrorRows());
  if (holdAndSetCQD("TRAF_LOAD_MAX_ERROR_ROWS", strMaxRR) < 0) { return -1;}
  if (hblTdb().getContinueOnError())
  {
    time_t t;
    time(&t);
    char pt[30];
    struct tm * curgmtime = gmtime(&t);
    strftime(pt, 30, "%Y%m%d_%H%M%S", curgmtime);

    if (holdAndSetCQD("TRAF_LOAD_ERROR_COUNT_ID", pt) < 0) { return -1;}
  }
  return 0;
}

short ExExeUtilHBaseBulkLoadTcb::restoreCQDs()
{
  if (restoreCQD("COMP_BOOL_226") < 0) { return -1;}
  if (restoreCQD("TRAF_LOAD_PREP_SKIP_DUPLICATES") < 0)  { return -1;}
  if (restoreCQD("ALLOW_INCOMPATIBLE_OPERATIONS") < 0)  { return -1;}
  if (hblTdb().getForceCIF())
  {
    if (restoreCQD("COMPRESSED_INTERNAL_FORMAT") < 0) { return -1;}
    if (restoreCQD("COMPRESSED_INTERNAL_FORMAT_BMO") < 0)  { return -1;}
    if (restoreCQD("COMPRESSED_INTERNAL_FORMAT_DEFRAG_RATIO") < 0)  { return -1;}
  }
  if (restoreCQD("TRAF_LOAD_LOG_ERROR_ROWS") < 0)  { return -1;}
  if (restoreCQD("TRAF_LOAD_CONTINUE_ON_ERROR") < 0)  { return -1;}
  if (restoreCQD("TRAF_LOAD_MAX_ERROR_ROWS") < 0)  { return -1;}
  if (restoreCQD("TRAF_LOAD_ERROR_COUNT_ID") < 0) { return -1; }
  if (restoreCQD("TRAF_LOAD_ERROR_LOGGING_LOCATION") < 0) { return -1; }

  return 0;
}

short ExExeUtilHBaseBulkLoadTcb::moveRowToUpQueue(const char * row, Lng32 len,
    short * rc, NABoolean isVarchar)
{
  if (hblTdb().getNoOutput())
    return 0;

  return ExExeUtilTcb::moveRowToUpQueue(row, len, rc, isVarchar);
}


short ExExeUtilHBaseBulkLoadTcb::printLoggingLocation(int bufPos)
{
  short retBufPos = bufPos;
  if (hblTdb().getNoOutput())
    return 0;
  if (loggingLocation_ != NULL) {
     str_sprintf(&statusMsgBuf_[bufPos], "       Logging Location: %s", loggingLocation_);
     retBufPos = strlen(statusMsgBuf_);
     statusMsgBuf_[retBufPos] = '\n';
     retBufPos++;
  }
  return retBufPos;
}


short ExExeUtilHBaseBulkLoadTcb::setStartStatusMsgAndMoveToUpQueue(const char * operation,
    short * rc,
    int bufPos,
    NABoolean   withtime)
{

  if (hblTdb().getNoOutput())
    return 0;

  char timeBuf[200];

  if (withtime)
  {
    startTime_ = NA_JulianTimestamp();
    getTimestampAsString(startTime_, timeBuf);
  }
  getStatusString(operation, "Started",hblTdb().getTableName(), &statusMsgBuf_[bufPos], FALSE, (withtime ? timeBuf : NULL));
  return moveRowToUpQueue(statusMsgBuf_,0,rc);
}


void ExExeUtilHBaseBulkLoadTcb::setEndStatusMsg(const char * operation,
    int bufPos,
    NABoolean   withtime)
{

  if (hblTdb().getNoOutput())
    return ;

  char timeBuf[200];

  nextStep_ = step_;
  step_ = RETURN_STATUS_MSG_;

  Int64 elapsedTime;
  if (withtime)
  {
    endTime_ = NA_JulianTimestamp();
    elapsedTime = endTime_ - startTime_;
    getTimestampAsString(endTime_, timeBuf);
    getStatusString(operation, "Ended", hblTdb().getTableName(),&statusMsgBuf_[bufPos], FALSE, withtime ? timeBuf : NULL);
    bufPos = strlen(statusMsgBuf_); 
    statusMsgBuf_[bufPos] = '\n';
    bufPos++; 
    getTimeAsString(elapsedTime, timeBuf);
  }
  getStatusString(operation, "Ended", hblTdb().getTableName(),&statusMsgBuf_[bufPos], TRUE, withtime ? timeBuf : NULL);
}

void ExExeUtilHBaseBulkLoadTcb::setLoggingLocation()
{
   char * loggingLocation = hblTdb().getLoggingLocation();
   if (loggingLocation_ != NULL) {
      NADELETEBASIC(loggingLocation_, getHeap());
      loggingLocation_ = NULL;
   }
   if (loggingLocation != NULL) { 
      short logLen = strlen(loggingLocation);
      char *tableName = hblTdb().getTableName();
      short tableNameLen = strlen(tableName);
      loggingLocation_ = new (getHeap()) char[logLen+tableNameLen+100];
      ExHbaseAccessTcb::buildLoggingPath(loggingLocation, NULL, tableName, loggingLocation_);
   }
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilHBaseBulkLoadTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilHbaseLoadPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilHbaseLoadPrivateState::ExExeUtilHbaseLoadPrivateState()
{
}

ExExeUtilHbaseLoadPrivateState::~ExExeUtilHbaseLoadPrivateState()
{
};





/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilHbaseUnLoadPrivateState::ExExeUtilHbaseUnLoadPrivateState()
{
}

ExExeUtilHbaseUnLoadPrivateState::~ExExeUtilHbaseUnLoadPrivateState()
{
};





short ExExeUtilLobExtractLibrary(ExeCliInterface *cliInterface,char *libHandle, char *cachedLibName,ComDiagsArea *toDiags)
{
  char buf[strlen(cachedLibName) + strlen(libHandle)+200];
  Int32 cliRC =0;
  str_sprintf(buf, "extract lobtofile(LOB '%s','%s');",libHandle,cachedLibName);
               
  cliRC = cliInterface->executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(toDiags);
      //Ignore error if the target file exists. This could be because the cached
      // file already got created by another process at the same time. So we can ignore
      // the error and use the already cached file.
      ComCondition *cond = NULL;
      Int32 entryNumber;
      cond = toDiags->findCondition(-EXE_ERROR_FROM_LOB_INTERFACE, &entryNumber);
      if (cond)
        {
          if (cond->getOptionalInteger(0) == LOB_TARGET_FILE_EXISTS_ERROR)
            {
              toDiags->deleteError(entryNumber);
              return 0;
            }
        }
                      
      return cliRC;
    }
  return cliRC;
}






////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilUpdataDeleteTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilUpdataDeletePrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilUpdataDeletePrivateState::ExExeUtilUpdataDeletePrivateState()
{
}

ExExeUtilUpdataDeletePrivateState::~ExExeUtilUpdataDeletePrivateState()
{
};

////////////////////////////////////////////////////////////////
// build for class ExExeUtilUpdataDeleteTdb
///////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilUpdataDeleteTdb::build(ex_globals * glob)
{
  ExExeUtilUpdataDeleteTcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilUpdataDeleteTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilUpdataDeleteTcb
///////////////////////////////////////////////////////////////
ExExeUtilUpdataDeleteTcb::ExExeUtilUpdataDeleteTcb(
    const ComTdbExeUtil & exe_util_tdb,
    ex_globals * glob)
: ExExeUtilTcb( exe_util_tdb, NULL, glob),
  step_(INITIAL_),
  nextStep_(INITIAL_),
  rowsAffected_(0)
{
  ehi_ = ExpHbaseInterface::newInstance(getGlobals()->getDefaultHeap(),
                                   hblTdb().server_,
                                   hblTdb().zkPort_,
                                   hblTdb().storageType_,
                                   FALSE);

  ehi_->initBRC(NULL);
  qparent_.down->allocatePstate(this);
}

void ExExeUtilUpdataDeleteTcb::freeResources()
{
  NADELETE(ehi_, ExpHbaseInterface, getGlobals()->getDefaultHeap());
  ehi_ = NULL;
}

Lng32 ExExeUtilUpdataDeleteTcb::lockOperatingTable()
{
  Lng32 cliRC = 0;
  NAString exequery;
 
  // use MD_OBJECTS_TRUNCATE_IN_PROGRESS to block BR op
  exequery.format("update %s.\"%s\".%s set flags = bitor(flags, %d) where object_uid = %ld",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, 
              MD_OBJECTS_TRUNCATE_IN_PROGRESS, hblTdb().objectUid());
  cliRC = cliInterface()->executeImmediate(exequery.data());
  if (cliRC < 0)
  {
    cliInterface()->retrieveSQLDiagnostics(diagsArea_);
    return -1;
  }

  return 0;
}

Lng32 ExExeUtilUpdataDeleteTcb::unLockOperatingTable()
{
  Lng32 cliRC = 0;
  NAString exequery;

  exequery.format("update %s.\"%s\".%s set flags = bitand(flags, %d) where object_uid = %ld",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, 
              ~MD_OBJECTS_TRUNCATE_IN_PROGRESS, hblTdb().objectUid());
  cliRC = cliInterface()->executeImmediate(exequery.data());
  if (cliRC < 0)
  {
    cliInterface()->retrieveSQLDiagnostics(diagsArea_);
    return -1;
  }
  return 0;
}

Lng32 ExExeUtilUpdataDeleteTcb::isBRInProgress()

{
  Int64 objFlags = 0;
  NAString exequery;

  objFlags = getObjectFlags(hblTdb().objectUid());

  if (objFlags < 0)
    return -1;

  if (CmpSeabaseDDL::isMDflagsSet(objFlags, MD_OBJECTS_BR_IN_PROGRESS))
  {
    ExRaiseSqlError(getHeap(), &diagsArea_, -4264, NULL, NULL, NULL, hblTdb().getTableName());
    return 1;
  }
  return 0;
}


ExExeUtilUpdataDeleteTcb::~ExExeUtilUpdataDeleteTcb()
{
  freeResources();
}

short ExExeUtilUpdataDeleteTcb::setCQDs()
{
  if (holdAndSetCQD("TRAF_NO_DTM_XN", "ON") < 0) { return -1;}
  if (holdAndSetCQD("ATTEMPT_ESP_PARALLELISM", "ON") < 0) { return -1;}
  if (holdAndSetCQD("SKIP_CHECK_FOR_TABLE_IB", "ON") < 0) { return -1;}
  return 0;
}

short ExExeUtilUpdataDeleteTcb::restoreCQDs()
{
  if (restoreCQD("TRAF_NO_DTM_XN") < 0) { return -1;}
  if (restoreCQD("ATTEMPT_ESP_PARALLELISM") < 0) { return -1;}
  if (restoreCQD("SKIP_CHECK_FOR_TABLE_IB") < 0) { return -1;}
  return 0;
}

//////////////////////////////////////////////////////
// work() for ExExeUtilUpdataDeleteTcb
//////////////////////////////////////////////////////
// INIT_
//     |
//     |
//     |  ___________________________________________________________
//     v /                                                           |
// GET_NAME_<-------------                                           |
//     |                 |                                           |
//     |                 |                                           |
//     |                 |                                           |
//     v                 |                                           |
//     loop------------->                                            |
//     |\                                                            |
//     |  ----------------------                                     |
//     |                       |                                     |
//     v                       |                                     v
// CREATE_SNAPSHOT_----------->|                                  ERROR_
//     |                       |                                     ^
//     |                    (error)                                  |
//     |                       |                                     |
//     v                       |                                     |
// EXE_QUERY-------(error)---->|---------------                      |
//     |                       |              |                      |
//     |                       |              v                      |
//     |                       |         RESTORE_SNAPSHOT_           |
//     |                       |              |                      |
//     v                       V              |                      |
// CLEANUP_<----------------------------------                       |
//     |        |                                                    |
//     |        |                                                    |
//     |        V                                                    |
// ERROR_<------------------------------------------------------------
//     |
//     v
// DONE_
/////////////////////////////////////////////////////////////////////////
short ExExeUtilUpdataDeleteTcb::work()
{
  Lng32 cliRC = 0;
  short retcode = 0;
  Int64 rowsAffected = 0;
  Queue * indexList = NULL;
  char * indexName ;
  NABoolean isProcessError = FALSE;
  std::vector<Text> vt;
  NAString lockedTag;

  struct snapshotStruct
  {
    NAString  fullTableName;
    NAString  snapshotName;
  };

  NAList<snapshotStruct>  snapshotsList(NULL);
  
  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status. Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate = *((ExExeUtilPrivateState*) pentry_down->pstate);

  ExMasterStmtGlobals *masterGlob = getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
  ContextCli *currContext = masterGlob->getStatement()->getContext();
  ExTransaction *ta = currContext->getTransaction();

  // lock object
  retcode = currContext->lockObjectDDL(hblTdb().getTableName(), COM_BASE_TABLE_OBJECT);
  if (retcode < 0) {
    return WORK_OK;
  }

  while (1)
  {
    switch (step_)
    {
      case INITIAL_:
      {
        NABoolean xnAlreadyStarted = ta->xnInProgress();
        if (xnAlreadyStarted)
        {
          ExRaiseSqlError(getHeap(), &diagsArea_, -8111, NULL, NULL, NULL, "Snapshot update delete");
          step_ = EXE_ERROR_;
          break;
        }

        if ((retcode = ehi_->init(NULL)) != HBASE_ACCESS_SUCCESS)
        {
          ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, retcode, 
                            "ExpHbaseInterface_JNI::init"); 
          step_ = EXE_ERROR_;
          break;
        }

        // check if there a BR lock to block our operator earlly
        if (hblTdb().incrBackupEnabled())
        {
          lockedTag = ehi_->lockHolder();
          if (lockedTag.length() != 0 && lockedTag != "?")
          {
            //   TAG NON_TX_SNAPSHOT is special
            if (lockedTag.contains("NON_TX_SNAPSHOT"))
            {
              ExRaiseSqlError(getHeap(), &diagsArea_, -8146, NULL, NULL, NULL, " Locked BACKUP TAG exist! Lock held by tag: " + lockedTag);
              step_ = EXE_ERROR_;
              break;
            }
          }
        }

        indexList = hblTdb().listOfIndexesAndTable();
        if (!indexList)
        {
          ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, retcode, 
                            "listOfIndexesAndTable is empty"); 
          step_ = EXE_ERROR_;
          break;
        }
        indexList->position();
        indexName = NULL;

        step_ = CHECK_FOR_LOCK_OBJECTS_;
      }
      break;

      case CHECK_FOR_LOCK_OBJECTS_:
      {
        if (isBRInProgress())
        {
          step_ = EXE_ERROR_;
          break;
        }

        if (lockOperatingTable() < 0)
        {
          step_ = EXE_ERROR_;
          break;
        }

        if (isBRInProgress())
        {
          unLockOperatingTable();
          step_ = EXE_ERROR_;
          break;
        }

        // lock object
        retcode = currContext->lockObjectDDL(hblTdb().getTableName(), COM_BASE_TABLE_OBJECT);
        if (retcode < 0)
        {
          step_ = EXE_ERROR_;
          break;
        }
        step_ = EXE_GET_OBJNAME_;
      }
      break;

      case EXE_GET_OBJNAME_:
      {
        char * indexName = (char*)indexList->getNext();
        Text t(indexName);
        vt.push_back(t);

        time_t now = time(0);
        tm *ltm = localtime(&now);
        NAString time_buf;
        time_buf.format("%4d-%02d-%02d_%02d_%02d_%02d", 
          ltm->tm_year + 1900, 
          ltm->tm_mon + 1,
          ltm->tm_mday, 
          ltm->tm_hour, 
          ltm->tm_min, 
          ltm->tm_sec);
        
        // str format is  NAMESPACE_TABLENAME_SNAPSHOT_UD_YYYY-MM-DD_hh_mi_ss
        std::string str = std::string(indexName) + "_SNAPSHOT_UD_" + time_buf.data();
        std::replace(str.begin(), str.end(), ':', '_');

        snapshotStruct snap ;
        snap.fullTableName =  NAString(indexName);
        snap.snapshotName =  NAString(str);

        ex_assert(snap.fullTableName.length()>0 &&
                  snap.snapshotName.length()>0 ,
                  "full table name and snapshot name cannot be empty");

        snapshotsList.insert(snap);

        if (!indexList->atEnd())
          step_ = EXE_GET_OBJNAME_;
        else
          step_ = EXE_CREATE_SNAPSHOT_;
      }
      break;

      case EXE_CREATE_SNAPSHOT_:
      {
        for ( int i = 0 ; i < snapshotsList.entries(); i++)
        {
          retcode = ehi_->createSnapshot( snapshotsList.at(i).fullTableName, snapshotsList.at(i).snapshotName);
          if (retcode != HBC_OK)
          {
            ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, retcode, 
                "HBaseClient_JNI::createSnapshot", 
                snapshotsList.at(i).snapshotName.data() );
            step_ = EXE_CLEANUP_;
            isProcessError = TRUE;
            break;
          }
        }

        if (step_ == EXE_CLEANUP_)
          break;

        step_ = EXE_RUNSQL_;
      }
      break;

      case EXE_RUNSQL_:
      {
        char * exe_query = hblTdb().query_;

        // close tx
        if (setCQDs() < 0)
        {
          isProcessError = TRUE;
          step_ = EXE_CLEANUP_;
          break;
        }

        cliRC = cliInterface()->executeImmediate(exe_query,
                                                 NULL,
                                                 NULL,
                                                 TRUE,
                                                 &rowsAffected,
                                                 FALSE,
                                                 &diagsArea_);

        if (cliRC < 0)
        {
          isProcessError = TRUE;
          cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
          step_ = EXE_RESTORE_SNAPSHOT_;
          break;
        }

        if (hblTdb().incrBackupEnabled())
        {
          cliRC = ehi_->createSnapshotForIncrBackup(vt);
          if (cliRC < 0)
          {
            isProcessError = TRUE;
            ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, cliRC, 
                "HBaseClient_JNI::createSnapshotForIncrBackup error",
                GetCliGlobals()->getJniErrorStr());
            step_ = EXE_RESTORE_SNAPSHOT_;

            NAString lockedTag = ehi_->lockHolder();
            if (lockedTag.length() != 0 && lockedTag != "?")
            {
              // release locked tag
              if (lockedTag.contains("NON_TX_SNAPSHOT"))
              {
                cliRC = ehi_->operationUnlock(lockedTag, FALSE, FALSE);
                if (cliRC < 0)
                {
                  ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, cliRC, 
                    "HBaseClient_JNI::operationUnlock error",
                    GetCliGlobals()->getJniErrorStr());
                }
              }
            }
            break;
          }
        }
        step_ = EXE_CLEANUP_;
      }
      break;

      case EXE_RESTORE_SNAPSHOT_:
      {
        for ( int i = 0 ; i < snapshotsList.entries(); i++)
        {
          retcode = ehi_->restoreSnapshot(snapshotsList.at(i).snapshotName, snapshotsList.at(i).fullTableName);
          if (retcode != HBC_OK)
          {
            ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, retcode, 
                "HBaseClient_JNI::restoreSnapshot", 
                snapshotsList.at(i).snapshotName.data() );
            break;
          }
        }
        step_ = EXE_CLEANUP_;
      }
      break;

      case EXE_CLEANUP_:
      {
        // delete snapshots
        for ( int i = 0 ; i < snapshotsList.entries(); i++)
        {
          retcode = ehi_->deleteSnapshot(snapshotsList.at(i).snapshotName);
          if (retcode != HBC_OK)
          {
            ExHbaseAccessTcb::setupError((NAHeap *)getMyHeap(),qparent_, retcode, 
                "HBaseClient_JNI::deleteSnapshot", 
                 snapshotsList.at(i).snapshotName.data() );
            step_ = EXE_ERROR_;
            break;
          }
        }

        // restore cqd 
        if (restoreCQDs() < 0)
        {
          step_ = EXE_ERROR_;
          break;
        }

        if (isProcessError)
          step_ = EXE_ERROR_;
        else
          step_ = DONE_;
      }
      break;

      case EXE_ERROR_:
      {
        retcode = handleError();
        if (retcode == 1)
          return WORK_OK;
        step_ = DONE_;
      }
      break;

      case DONE_:
      {
        ehi_->close();
        retcode = handleDone();
        if (retcode == 1)
          return WORK_OK;

        if (!isProcessError)
          masterGlob->setRowsAffected(rowsAffected);
        unLockOperatingTable();
        currContext->unlockObjectDDL(hblTdb().getTableName(), COM_BASE_TABLE_OBJECT);
        freeResources();
        step_ = INITIAL_;
        return WORK_OK;
      }
      break;
    } // switch
  } // while
  
  return WORK_OK;
}
