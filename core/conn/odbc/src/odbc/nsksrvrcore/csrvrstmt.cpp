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
********************************************************************/
//
// MODULE: CSrvrStmt.cpp
//
// PURPOSE: Implements the member functions of CSrvrStmt class
//
//

#include <platform_ndcs.h>
#include "CSrvrStmt.h"
#include "sqlinterface.h"
#include "srvrkds.h"
#include "srvrcommon.h"
#include "tdm_odbcSrvrMsg.h"
#include "CommonDiags.h"
#include "commonFunctions.h"

NAHeap _stmt_heap("statement allocator");

//extern "C" Int32   random(void);

//Map<int, long> 	SRVR_STMT_HDL::stmtHandleMapping;
IDL_long SRVR_STMT_HDL::globalKey = 0;
int SRVR_STMT_HDL::EXPECTED_RESPONSE_TIME_IN_MILLIS  = -1;

using namespace SRVR;

SRVR_STMT_HDL::SRVR_STMT_HDL()
{
	if (EXPECTED_RESPONSE_TIME_IN_MILLIS == -1)  {
		char *expectedResTimeStr = getenv("EXPECTED_RESPONSE_TIME_IN_MILLIS");
		if (expectedResTimeStr != NULL)
			EXPECTED_RESPONSE_TIME_IN_MILLIS  = atoi(expectedResTimeStr);
		else
			EXPECTED_RESPONSE_TIME_IN_MILLIS  = 0;
	}
	SRVRTRACE_ENTER(FILE_CSTMT+1);
	cursorNameLen = 0;
	cursorName[0] = '\0';
	previousCursorName[0] = '\0';
	stmtNameLen = 0;
	stmtName[0] = '\0';
	paramCount = 0;
	columnCount = 0;
	stmtType = EXTERNAL_STMT;
	inputDescVarBuffer = NULL;
	outputDescVarBuffer = NULL;
	inputDescVarBufferLen = 0;
	outputDescVarBufferLen = 0;
	inputDescIndBufferLen = 0;
	outputDescIndBufferLen = 0;
	SpjProxySyntaxString = NULL;
	SpjProxySyntaxStringLen = 0;
	recordOutputDescVarBufferLen = 0;

	currentMethod = UNKNOWN_METHOD;
	asyncThread = 0;
	queryTimeoutThread = 0;
	threadStatus = SQL_SUCCESS;
	threadId = 0;
	threadReturnCode = SQL_SUCCESS;

	sqlAsyncEnable = SQL_ASYNC_ENABLE_OFF;
	queryTimeout = 0;
	sqlStringLen = 0;
	sqlString = NULL;
	sqlPlan = NULL;
	sqlPlanLen = 0;
	inputRowCnt = 0;
	maxRowsetSize = ROWSET_NOT_DEFINED;
	maxRowCnt = 0;
	sqlStmtType = TYPE_UNKNOWN;
	sqlQueryType = SQL_UNKNOWN;
	inputDescBufferLength = 0;
	inputDescBuffer = NULL;
	outputDescBufferLength = 0;
	outputDescBuffer = NULL;
	sqlWarningOrErrorLength = 0;
	sqlWarningOrError = NULL;
	delayedSqlWarningOrErrorLength = 0;
	delayedSqlWarningOrError = NULL;
	cliStartTime = 0;
	cliEndTime = 0;
	cliElapseTime = 0;
	sqlBulkFetchPossible = false;
	bFirstSqlBulkFetch = false;
	bLowCost = false;

	sqlUniqueQueryIDLen = 0;
	sqlUniqueQueryID[0] = '\0';
	freeResourceOpt = SQL_CLOSE;
	inputValueList._length = 0;
	inputValueList._buffer = NULL;

	bzero(&cost_info, sizeof(cost_info));
	bzero(&comp_stats_info, sizeof(comp_stats_info));
	rowsAffected = 0;
	delayedRowsAffected = 0;
	rowsAffectedHigherBytes = 0;
	inputDescList._length = 0;
	inputDescList._buffer = NULL;
	outputDescList._length = 0;
	outputDescList._buffer = NULL;
	sqlWarning._length	= 0;
	sqlWarning._buffer = NULL;
	sqlError.errorList._length = 0;
	sqlError.errorList._buffer = NULL;
	outputValueList._length = 0;
	outputValueList._buffer = NULL;
	outputValueVarBuffer = NULL;
	inputValueVarBuffer = NULL;
	clientLCID = srvrGlobal->clientLCID;
	isReadFromModule = FALSE;
	moduleName[0] = '\0';
	inputDescName[0] = '\0';
	outputDescName[0] = '\0';
	isClosed = TRUE;
	IPD = NULL;
	IRD = NULL;
	outputDataValue._length = 0;
	outputDataValue._buffer = NULL;
        //outputDataValue.pad_to_offset_8_ = {'\0', '\0', '\0', '\0'};
        //use trafodional way to initialze array, above is c++11 in gcc 4.8
        //for gcc 4.4 backward compatibilty, Trafodion now is using c++0x , gcc 4.4 cannot support c++11 mode
        //once tested with -std=c++11, may recover this coding style
        memset( outputDataValue.pad_to_offset_8_ , 0, sizeof(outputDataValue.pad_to_offset_8_) );

	inputDataValue._length = 0;
	inputDataValue._buffer = NULL;
	delayedOutputDataValue._length = 0;
	delayedOutputDataValue._buffer = NULL;
	estRowLength = 0;
	bSQLMessageSet = false;
	bSQLValueListSet = false;
	bFetchStarted = false;
	
	SqlDescInfo = NULL;

	GN_SelectParamQuadFields = NULL;
	GN_stmtDrvrHandle = 0;
	GN_maxRowsetSize = 0;
	GN_currentRowsetSize = 0;

    numResultSets  = 0;
    previousSpjRs  = NULL;
    nextSpjRs      = NULL;
    callStmtId     = NULL;
    resultSetIndex = 0;
	callStmtHandle = NULL;

	NA_supported = false;

        preparedWithRowsets = false;  
	inputQuadList       = NULL;
        inputQuadList_recover = NULL;
        transportBuffer     = NULL;
	transportBufferLen  = 0;

        outputQuadList      = NULL;
        outputBuffer        = NULL;
        outputBufferLength  = 0;
	returnCodeForDelayedError = SQL_SUCCESS;

	m_isAdaptiveSegmentAllocated = false;
	m_need_21036_end_msg = false;
	m_adaptive_segment = -1;

	m_internal_Id = 0;
	m_aggPriority = 0;
	m_bSkipWouldLikeToExecute = false;
	m_bDoneWouldLikeToExecute = false;
	m_bqueryFinish = false;

	m_result_set = NULL;
	m_curRowsFetched = 0;
	inState = STMTSTAT_NONE;
	exPlan = UNAVAILABLE;

	current_holdableCursor = SQL_NONHOLDABLE;
	holdableCursor = SQL_NONHOLDABLE;

	sqlNewQueryType = SQL_UNKNOWN;

	bzero(m_con_rule_name, sizeof(m_con_rule_name));
	bzero(m_cmp_rule_name, sizeof(m_cmp_rule_name));
	bzero(m_exe_rule_name, sizeof(m_exe_rule_name));
	//ssd overflow
	memset(&m_execOverflow, '\0', sizeof(EXEC_OVERFLOW));

	m_suspended_ts = 0;
	m_released_ts = 0;
	m_cancelled_ts = 0;

//
// AGGREGATION
//
//
	m_query_rejected = false;
//
//
	m_lastQueryStartTime = 0;
	m_lastQueryEndTime = 0;
	m_lastQueryStartCpuTime = 0;
	m_lastQueryEndCpuTime = 0;
//
	m_mxsrvr_substate = NDCS_INIT;

	m_flushQuery = NULL;

//
	m_bNewQueryId = false;
	bzero(m_shortQueryText, sizeof(m_shortQueryText));
	m_rmsSqlSourceLen = 0;

//
	querySpl = SPEC_QUERY_INIT;
//
	m_pertable_stats = false;
//
	// 64bit work
	globalKey++;
	//globalKey %= 64000;
	myKey = globalKey;	
	srvrGlobal->stmtHandleMap[myKey] = (Long)this;
	//stmtHandleMapping.insert(pair<int, long>(myKey, this));	
	
	reprepareWarn = false;

	//for publishing compiler error to repository
	queryStartTime = 0;

	// Query status in repository
	m_state = QUERY_INIT;
    lobUpdateStmt_ = NULL;
    lobExtractStmt_ = NULL;
    errorInLobUpdateStmt_ = false;
    errorInLobExtractStmt_ = false;
	lobTableUid_ = -1;
	chunk_ = NULL;
	chunkLen_ = 0;
    lobPos_ = -1;
    lobChunkMaxLen_ = 0;

	SRVRTRACE_EXIT(FILE_CSTMT+1);
}
SRVR_STMT_HDL *SRVR_STMT_HDL::getLobExtractStmt(long tableUid, long stmtid,ExceptionStruct  *exception)
{
    SQLRETURN rc;

    if (lobExtractStmt_  != NULL && (! errorInLobExtractStmt_  ) && (tableUid == lobTableUid_))
        return lobExtractStmt_ ;

    char lobExtractStmtName[MAX_STMT_NAME_LEN+1];
    long sqlcode;
    snprintf(lobExtractStmtName, sizeof(lobExtractStmtName), "EXTRACT_LOB_%ld", stmtid);
    if (lobExtractStmt_ == NULL) {
        if((lobExtractStmt_ = getSrvrStmt(lobExtractStmtName, TRUE)) == NULL){
            exception->exception_nr = odbc_SQLSvc_ExtractLob_SQLInvalidhandle_exn_;
            exception->u.SQLInvalidHandle.sqlcode = sqlcode;
            return NULL;
        }
        errorInLobExtractStmt_ = false;
        lobTableUid_ = tableUid;
    }
    else {
        lobExtractStmt_->closeExtractLob();
        errorInLobExtractStmt_ = false;
        lobTableUid_ = tableUid;
    }

    char lobExtractQuery[250];
    snprintf(lobExtractQuery, sizeof(lobExtractQuery), "EXTRACT LOBTOBUFFER(TABLE UID '%ld', LOB ?, LOCATION ? , SIZE ?)", lobTableUid_);
    /*
    SQLValue_def sqlString;
    sqlString.dataCharset = SQLCHARSETCODE_ISO88591;
    sqlString.dataInd = 0;
    sqlString.dataType = SQLTYPECODE_VARCHAR;
    sqlString.dataValue._buffer = (unsigned char *)lobExtractQuery;
    sqlString.dataValue._length = strlen(lobExtractQuery);
    */
    rc = lobExtractStmt_->Prepare(lobExtractQuery, TYPE_UNKNOWN, SQL_NONHOLDABLE,0);
    if (rc == SQL_ERROR) {
        exception->exception_nr = odbc_SQLSvc_ExtractLob_SQLError_exn_;
        exception->u.SQLError.errorList._length = lobExtractStmt_->sqlError.errorList._length;
        exception->u.SQLError.errorList._buffer = lobExtractStmt_->sqlError.errorList._buffer;
        errorInLobExtractStmt_ = true;
        return NULL;
    }
    else
        errorInLobExtractStmt_ = false;
    lobExtractStmt_->setLobChunkMaxLen(getLobChunkMaxLen());
    return lobExtractStmt_;
}
SRVR_STMT_HDL *SRVR_STMT_HDL::getLobUpdateStmt(long tableUid, long stmtId, ExceptionStruct  *exception)
{
    SQLRETURN rc;

    if (lobUpdateStmt_ != NULL && (! errorInLobUpdateStmt_) && (tableUid == lobTableUid_))
        return lobUpdateStmt_ ;

    char lobUpdateStmtName[MAX_STMT_NAME_LEN+1];
    long sqlcode;
    snprintf(lobUpdateStmtName, sizeof(lobUpdateStmtName), "UPDATE_LOB_%ld", stmtId);
    if (lobUpdateStmt_ == NULL) {
        if((lobUpdateStmt_ = getSrvrStmt(lobUpdateStmtName, TRUE)) == NULL){
            exception->exception_nr = odbc_SQLSvc_UpdateLob_SQLInvalidhandle_exn_;
            exception->u.SQLInvalidHandle.sqlcode = sqlcode;
            return NULL;
        }
        errorInLobUpdateStmt_ = false;
        lobTableUid_ = tableUid;
    }
    else {
        lobUpdateStmt_->closeUpdateLob();
        errorInLobUpdateStmt_ = false;
        lobTableUid_ =  tableUid;
    }

    char lobUpdateQuery[250];
    snprintf(lobUpdateQuery, sizeof(lobUpdateQuery), "UPDATE LOB (TABLE UID '%ld', LOB ?, LOCATION ?, SIZE ?, POSITION ?)", lobTableUid_);
  /*
    SQLValue_def sqlString;
    sqlString.dataCharset = SQLCHARSETCODE_ISO88591;
    sqlString.dataInd = 0;
    sqlString.dataType = SQLTYPECODE_VARCHAR;
    sqlString.dataValue._buffer = (unsigned char *)lobUpdateQuery;
    sqlString.dataValue._length = strlen(lobUpdateQuery);
    */
    rc = lobUpdateStmt_->Prepare(lobUpdateQuery, TYPE_UNKNOWN, SQL_NONHOLDABLE,0);
    if (rc == SQL_ERROR) {
        exception->exception_nr = odbc_SQLSvc_UpdateLob_SQLError_exn_;
        exception->u.SQLError.errorList._length = lobUpdateStmt_->sqlError.errorList._length;
        exception->u.SQLError.errorList._buffer = lobUpdateStmt_->sqlError.errorList._buffer;
        errorInLobUpdateStmt_ = true;
        return NULL;
    }
    else
        errorInLobUpdateStmt_ = false;
    lobUpdateStmt_->setLobChunkMaxLen(getLobChunkMaxLen());
    return lobUpdateStmt_;
}   




SRVR_STMT_HDL::~SRVR_STMT_HDL()
{
	SRVRTRACE_ENTER(FILE_CSTMT+2);

	if (m_flushQuery != NULL)
		(*m_flushQuery)((intptr_t)this);
	cleanupAll();
	inState = STMTSTAT_NONE;

	// Added for 64bit work
	srvrGlobal->stmtHandleMap.erase(myKey);
	SRVRTRACE_EXIT(FILE_CSTMT+2);
}
SQLRETURN SRVR_STMT_HDL::extractLob(IDL_string lobHandle, BYTE *&extractData, IDL_long_long &extractLen, 
		ExceptionStruct *exception)  
{
	SQLRETURN rc = SQL_SUCCESS;
    rc = EXTRACTLOB(this, lobHandle, extractData, extractLen);
	switch (rc)
	{
		case SQL_SUCCESS:
		case SQL_SUCCESS_WITH_INFO:
			isClosed = FALSE;
			break;
        case SQL_EOF:
            rc = SQL_SUCCESS_WITH_INFO;
            isClosed = FALSE;
            exception->exception_nr = 100;
            break;
		case SQL_ERROR:
			exception->exception_nr = odbc_SQLSvc_ExtractLob_SQLError_exn_;
			exception->u.SQLError.errorList._length = sqlError.errorList._length;
			exception->u.SQLError.errorList._buffer = sqlError.errorList._buffer;
			break;
		default:
			exception->exception_nr = odbc_SQLSvc_ExtractLob_ParamError_exn_;
			break;
	}
	return rc;
}
SQLRETURN SRVR_STMT_HDL::updateLob(IDL_string lobHandle, BYTE *&updateData, int &updateLen, 
        IDL_long_long &updatePos, ExceptionStruct *exception)  
{
    SQLRETURN rc = SQL_SUCCESS;
    rc = UPDATELOB(this, lobHandle, updateData, updateLen, updatePos);
    switch (rc)
    {
        case SQL_SUCCESS:
        case SQL_SUCCESS_WITH_INFO:
            isClosed = FALSE;
            break;
        case SQL_ERROR:
            exception->exception_nr = odbc_SQLSvc_UpdateLob_SQLError_exn_;
            exception->u.SQLError.errorList._length = sqlError.errorList._length;
            exception->u.SQLError.errorList._buffer = sqlError.errorList._buffer;
            break;
        default:
            exception->exception_nr = odbc_SQLSvc_UpdateLob_SQLError_exn_;
            break;
    }
    return rc;
}

SQLRETURN SRVR_STMT_HDL::closeExtractLob()
{
   SQLRETURN rc = SQL_SUCCESS;
   if (lobExtractStmt_ != NULL) {
      rc = lobExtractStmt_->Close(SQL_CLOSE);
      errorInLobExtractStmt_ = false;
   }
   return rc;
}

SQLRETURN SRVR_STMT_HDL::closeUpdateLob()
{
   SQLRETURN rc = SQL_SUCCESS;
   if (lobUpdateStmt_ != NULL) {
      rc = lobUpdateStmt_->Close(SQL_CLOSE);
      errorInLobUpdateStmt_ = false;
   }
   return rc;
}


SQLRETURN SRVR_STMT_HDL::Prepare(const char *inSqlString, short inStmtType, short inSqlAsyncEnable, 
								 Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+3);

	SQLRETURN rc;
	
	if (isReadFromModule)	// Already SMD label is found
		return SQL_SUCCESS;
	// cleanup all memory allocated in the previous operations
	cleanupAll();
	sqlStringLen = strlen(inSqlString);
	markNewOperator,sqlString  = new char[sqlStringLen+1];
	if (sqlString == NULL)
	{
		SendEventMsg(MSG_MEMORY_ALLOCATION_ERROR, EVENTLOG_ERROR_TYPE,
					srvrGlobal->nskProcessInfo.processId, ODBCMX_SERVER, 
					srvrGlobal->srvrObjRef, 1, "Prepare");
		exit(0);
	}

	strcpy(sqlString, inSqlString);
	stmtType = inStmtType;
	currentMethod = odbc_SQLSvc_Prepare_ldx_;
	rc = (SQLRETURN)ControlProc(this);

	SRVRTRACE_EXIT(FILE_CSTMT+3);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::Execute(const char *inCursorName, IDL_long inInputRowCnt, IDL_short inSqlStmtType, 
								 const SQLValueList_def *inValueList, 
								 short inSqlAsyncEnable, Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+4);
	SQLRETURN rc;
	
	if(bSQLMessageSet)
		cleanupSQLMessage();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	inputRowCnt = inInputRowCnt;
	sqlStmtType = inSqlStmtType;

	if (inCursorName != NULL && inCursorName[0] != '\0')
	{
		cursorNameLen = strlen(inCursorName);
		cursorNameLen = cursorNameLen < sizeof(cursorName)? cursorNameLen : sizeof(cursorName);
		strcpyUTF8(cursorName, inCursorName, sizeof(cursorName));
	}
	else
		cursorName[0] = '\0';


	inputValueList._buffer = inValueList->_buffer;
	inputValueList._length = inValueList->_length;

	currentMethod = odbc_SQLSvc_ExecuteN_ldx_;
//	rc = (SQLRETURN)ControlProc(this);

	rc = EXECUTE(this);

	switch (rc)
	{
	case SQL_SUCCESS:
		break;
	case ODBC_RG_WARNING:
		rc = SQL_SUCCESS_WITH_INFO;
	case SQL_SUCCESS_WITH_INFO:
		GETSQLERROR(bSQLMessageSet, &sqlError);
		break;
	case SQL_ERROR:
		GETSQLERROR(bSQLMessageSet, &sqlError);
		break;
	case ODBC_SERVER_ERROR:
		// Allocate Error Desc
		kdsCreateSQLErrorException(bSQLMessageSet, &sqlError, 1);
		// Add SQL Error
		kdsCopySQLErrorException(&sqlError, NULL_VALUE_ERROR, NULL_VALUE_ERROR_SQLCODE,
				NULL_VALUE_ERROR_SQLSTATE);
		break;
	case -8814:
	case 8814:
		rc = SQL_RETRY_COMPILE_AGAIN;
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+4);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::Close(unsigned short inFreeResourceOpt)
{
	SRVRTRACE_ENTER(FILE_CSTMT+5);

	SQLRETURN rc;

	if (bSQLMessageSet)
		cleanupSQLMessage();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	freeResourceOpt = inFreeResourceOpt;
    currentMethod = odbc_SQLSvc_Close_ldx_;
    if (lobUpdateStmt_ != NULL) {
        lobUpdateStmt_->Close(inFreeResourceOpt);
        if (inFreeResourceOpt == SQL_DROP)
            lobUpdateStmt_ = NULL;
    }
    if (lobExtractStmt_ != NULL) {
        lobExtractStmt_->Close(inFreeResourceOpt);
        if (inFreeResourceOpt == SQL_DROP)
            lobExtractStmt_ = NULL;
    }
	rc = (SQLRETURN)ControlProc(this);
	SRVRTRACE_EXIT(FILE_CSTMT+5);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::InternalStmtClose(unsigned short inFreeResourceOpt)
{
	SRVRTRACE_ENTER(FILE_CSTMT+6);

	SQLRETURN rc;

	if (bSQLMessageSet)
		cleanupSQLMessage();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	freeResourceOpt = inFreeResourceOpt;
	currentMethod = odbc_SQLSvc_Close_ldx_;
	rc = (SQLRETURN)ControlProc(this);
	SRVRTRACE_EXIT(FILE_CSTMT+6);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::Fetch(Int32 inMaxRowCnt, Int32 inMaxRowLen, short inSqlAsyncEnable, Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+7);

	SQLRETURN rc;

	if (bSQLMessageSet)
		cleanupSQLMessage();
	if (outputValueList._buffer == NULL  || maxRowCnt < inMaxRowCnt)
	{
		if(bSQLValueListSet)
			cleanupSQLValueList();
		rc = AllocAssignValueBuffer(bSQLValueListSet,&outputDescList, &outputValueList, outputDescVarBufferLen,
						inMaxRowCnt, outputValueVarBuffer);
		if (rc != SQL_SUCCESS)
			return rc;
	}
	else
		// Reset the length to 0, but the _buffer points to array of required SQLValue_defs
		outputValueList._length = 0;

	maxRowCnt = inMaxRowCnt;
	maxRowLen = inMaxRowLen;
	currentMethod = odbc_SQLSvc_FetchN_ldx_;
	rc = (SQLRETURN)ControlProc(this);
	SRVRTRACE_EXIT(FILE_CSTMT+7);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::ExecDirect(const char *inCursorName, const char *inSqlString, short inStmtType, IDL_short inSqlStmtType, 
									short inSqlAsyncEnable, Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+8);

	SQLRETURN rc;

	cleanupAll();
	sqlStringLen = strlen(inSqlString);
	markNewOperator,sqlString  = new char[sqlStringLen+1];
	if (sqlString == NULL)
	{
		SendEventMsg(MSG_MEMORY_ALLOCATION_ERROR, EVENTLOG_ERROR_TYPE,
					srvrGlobal->nskProcessInfo.processId, ODBCMX_SERVER, 
					srvrGlobal->srvrObjRef, 1, "ExecDirect");
		exit(0);
	}

	strcpy(sqlString, inSqlString);
	stmtType = inStmtType;
	sqlStmtType = inSqlStmtType;

	if (inCursorName != NULL && inCursorName[0] != '\0')
	{
		cursorNameLen = strlen(inCursorName);
		cursorNameLen = cursorNameLen < sizeof(cursorName)? cursorNameLen : sizeof(cursorName);
		strcpyUTF8(cursorName, inCursorName, sizeof(cursorName));
	}
	else
		cursorName[0] = '\0';

	currentMethod = odbc_SQLSvc_ExecDirect_ldx_;
	rc = (SQLRETURN)ControlProc(this);
	switch (rc)
	{
	case SQL_SUCCESS:
	case SQL_SUCCESS_WITH_INFO:
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+8);
	return rc;
}

void  SRVR_STMT_HDL::cleanupSQLMessage()
{
	SRVRTRACE_ENTER(FILE_CSTMT+10);

	UInt32 i;
	ERROR_DESC_def *errorDesc;
	// Cleanup SQLWarning
	bSQLMessageSet = false;
	if (sqlWarning._buffer)
	{
		int len_length = sqlWarning._length;
		ERROR_DESC_def *p_buffer = (ERROR_DESC_def *)sqlWarning._buffer;

		for (i = 0 ; i < len_length ; i++)
		{

			errorDesc = p_buffer + i;
			if (errorDesc->errorText != NULL)
			{
				delete errorDesc->errorText;

			}
		}
		delete sqlWarning._buffer;
		sqlWarning._buffer = NULL;
		sqlWarning._length = 0;
	}
	
	// Cleanup sqlErrror
	if (sqlError.errorList._buffer != NULL)
	{
		int len_length = sqlError.errorList._length;
		ERROR_DESC_def *p_buffer = (ERROR_DESC_def *)sqlError.errorList._buffer;

		for (i = 0 ; i < len_length ; i++)
		{

			errorDesc = p_buffer + i;
			if (errorDesc->errorText != NULL)
			{
				delete errorDesc->errorText;
			}

			if (errorDesc->Param1 != NULL)
			{
				delete errorDesc->Param1;
			}

			if (errorDesc->Param2 != NULL)
			{
				delete errorDesc->Param2;
			}

			if (errorDesc->Param3 != NULL)
			{
				delete errorDesc->Param3;
			}

			if (errorDesc->Param4 != NULL)
			{
				delete errorDesc->Param4;
			}

			if (errorDesc->Param5 != NULL)
			{
				delete errorDesc->Param5;
			}

			if (errorDesc->Param6 != NULL)
			{
				delete errorDesc->Param6;
			}

			if (errorDesc->Param7 != NULL)
			{
				delete errorDesc->Param7;
			}

		}
		delete sqlError.errorList._buffer;
		sqlError.errorList._buffer = NULL;
		sqlError.errorList._length = 0;
	}

	// Cleanup sqlErrorOrWarnings
	if (sqlWarningOrError != NULL)
		delete sqlWarningOrError;
	sqlWarningOrError = NULL;
	sqlWarningOrErrorLength = 0;


	SRVRTRACE_EXIT(FILE_CSTMT+10);
	return;
}

void  SRVR_STMT_HDL::cleanupSQLValueList()
{
	SRVRTRACE_ENTER(FILE_CSTMT+11);

	bSQLValueListSet = false;

	STMT_DEALLOCATE(outputValueList._buffer);
	outputValueList._length = 0;
	STMT_DEALLOCATE(outputValueVarBuffer);
	maxRowCnt = 0;
	SRVRTRACE_EXIT(FILE_CSTMT+11);
	return;
}

void  SRVR_STMT_HDL::cleanupSQLDescList()
{
	SRVRTRACE_ENTER(FILE_CSTMT+12);

	STMT_DEALLOCATE(inputDescBuffer);
	STMT_DEALLOCATE(inputDescVarBuffer);
	STMT_DEALLOCATE(inputDescList._buffer);

	STMT_DEALLOCATE(outputDescBuffer);
	STMT_DEALLOCATE(outputDescVarBuffer);
	STMT_DEALLOCATE(outputDescList._buffer);

	inputDescBufferLength = 0;
	inputDescVarBufferLen = 0;
	inputDescList._length = 0;

	outputDescBufferLength = 0;
	outputDescVarBufferLen = 0;
	outputDescList._length = 0;
	recordOutputDescVarBufferLen = 0;

	STMT_DEALLOCATE(SqlDescInfo);
	SRVRTRACE_EXIT(FILE_CSTMT+12);
	return;
}

void  SRVR_STMT_HDL::cleanupAll()
{
	SRVRTRACE_ENTER(FILE_CSTMT+13);

	if (sqlString != NULL)
	{
		delete[] sqlString;
		sqlString = NULL;
	}
	if (sqlPlan != NULL)
	{
		delete sqlPlan;
		sqlPlan = NULL;
		sqlPlanLen = 0;
    }
    if (lobExtractStmt_ != NULL) {
        lobExtractStmt_->Close(SQL_DROP);
        lobExtractStmt_ = NULL;
    }
    if (lobUpdateStmt_ != NULL) {
        lobUpdateStmt_->Close(SQL_DROP);
        lobUpdateStmt_ = NULL;
    }

    if(chunk_ != NULL)
    {
        delete chunk_;
        chunk_  = NULL;
        chunkLen_ = 0;
        lobPos_ = -1;
    }

	if (bSQLMessageSet)
		cleanupSQLMessage();
	cleanupSQLDescList();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	if (stmtType != EXTERNAL_STMT)
	{
		STMT_DEALLOCATE(inputValueList._buffer);
		inputValueList._length = 0;
	}
	STMT_DEALLOCATE(inputValueVarBuffer);
	STMT_DEALLOCATE(IPD);
	STMT_DEALLOCATE(IRD);
	STMT_DEALLOCATE(SqlDescInfo);

	if (GN_SelectParamQuadFields != NULL)
	{
		delete[] GN_SelectParamQuadFields;
		GN_SelectParamQuadFields = NULL;
	}
        if (inputQuadList != NULL)
          {
	  delete[] inputQuadList;
          inputQuadList = NULL;
	  }
        if (inputQuadList_recover != NULL)
          {
	  delete[] inputQuadList_recover;
          inputQuadList_recover = NULL;
	  }
	if (sqlWarningOrErrorLength > 0)
	  sqlWarningOrErrorLength = 0;
	if (sqlWarningOrError != NULL)
	  {
	  delete sqlWarningOrError;
	  sqlWarningOrError = NULL;
	  }
        if (outputQuadList != NULL)
          {
	  delete[] outputQuadList;
          outputQuadList = NULL;
	  }

	exPlan = UNAVAILABLE;
	if(SpjProxySyntaxString != NULL)
		delete[] SpjProxySyntaxString;
	SpjProxySyntaxString = NULL;
	SpjProxySyntaxStringLen = 0;

	current_holdableCursor = SQL_NONHOLDABLE;
	holdableCursor = SQL_NONHOLDABLE;

	SRVRTRACE_EXIT(FILE_CSTMT+13);
	return;
}

Int32 SRVR_STMT_HDL::GetCharset()
{
	SRVRTRACE_ENTER(FILE_CSTMT+14);

	Int32 rcharset = SQLCHARSETCODE_ISO88591;

	if (clientLCID != SQLCHARSETCODE_UNKNOWN)
		return clientLCID;

	SRVRTRACE_EXIT(FILE_CSTMT+14);
	return rcharset;
}

DWORD WINAPI SRVR::ControlProc(LPVOID pParam)
{
	SRVRTRACE_ENTER(FILE_CSTMT+15);

	SQLRETURN rc = SQL_SUCCESS;
	SRVR_STMT_HDL *pSrvrStmt;

	pSrvrStmt = (SRVR_STMT_HDL *)pParam;
	unsigned short freeResourceOpt;

	switch (pSrvrStmt->currentMethod)
	{
	case odbc_SQLSvc_Prepare_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = PREPARE (pSrvrStmt);
		pSrvrStmt->logDelayedResponseMessage("PREPARE");
		break;
	case odbc_SQLSvc_ExecuteN_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = EXECUTE(pSrvrStmt);
		pSrvrStmt->logDelayedResponseMessage("EXECUTE");
		break;
	case odbc_SQLSvc_Close_ldx_:
		// Copy freeresourceOpt since pSrvrStmt is deleted when it is drop
		// In this case pSrvrStmt becomes invalid.
		freeResourceOpt = pSrvrStmt->freeResourceOpt;
		rc = FREESTATEMENT(pSrvrStmt);
		// Return back immediately since the pSrvrStmt is deleted and return SQL_SUCCESS always
		if (freeResourceOpt == SQL_DROP)
		{
			rc = SQL_SUCCESS;
			return (rc);
		}
		break;
	case odbc_SQLSvc_FetchN_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = FETCH(pSrvrStmt);
		pSrvrStmt->logDelayedResponseMessage("FETCH");
		break;
	case odbc_SQLSvc_ExecDirect_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = EXECDIRECT(pSrvrStmt);
		pSrvrStmt->logDelayedResponseMessage("EXECDIRECT");
		break;
	case odbc_SQLSvc_FetchPerf_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = FETCHPERF(pSrvrStmt, &pSrvrStmt->outputDataValue);
		pSrvrStmt->logDelayedResponseMessage("FETCHPERF");
		break;
	case Allocate_Statement_Handle:
		rc = ALLOCSQLMXHDLS(pSrvrStmt);
		break;
	case odbc_SQLSvc_GetSQLCatalogs_ldx_:
		rc = PREPARE_FROM_MODULE(pSrvrStmt, &pSrvrStmt->inputDescList, &pSrvrStmt->outputDescList);
		break;
	case odbc_SQLSvc_ExecuteCall_ldx_:
		pSrvrStmt->setQueryResponseStartTime();
		rc = EXECUTECALL(pSrvrStmt);
		pSrvrStmt->logDelayedResponseMessage("EXECUTECALL");
		break;
	case Fetch_Catalog_Rowset:
		pSrvrStmt->setQueryResponseStartTime();
		rc = FETCHCATALOGPERF(pSrvrStmt, pSrvrStmt->maxRowCnt, pSrvrStmt->maxRowLen, &pSrvrStmt->rowsAffected, &pSrvrStmt->outputDataValue);
		pSrvrStmt->logDelayedResponseMessage("FETCHCATALOGPERF");
		break;
	default:
		break;
	}

	switch (rc)
	{
	case SQL_SUCCESS:
		break;
	case SQL_SUCCESS_WITH_INFO:
		GETSQLWARNING(pSrvrStmt->bSQLMessageSet, &pSrvrStmt->sqlWarning);
		break;
	case SQL_ERROR:
		GETSQLERROR(pSrvrStmt->bSQLMessageSet, &pSrvrStmt->sqlError);
		break;
	case ODBC_RG_WARNING:
		rc = SQL_SUCCESS_WITH_INFO;
	case ODBC_SERVER_ERROR:
	case ODBC_RG_ERROR:
	default:
		break;
	}
	pSrvrStmt->threadReturnCode = rc;
	SRVRTRACE_EXIT(FILE_CSTMT+15);
	return rc;
}

//======================= Performance ==========================================

SQLRETURN SRVR_STMT_HDL::FetchPerf(Int32 inMaxRowCnt, Int32 inMaxRowLen, short inSqlAsyncEnable, Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+16);

	SQLRETURN rc;

	if (bSQLMessageSet)
		cleanupSQLMessage();

	outputDataValue._length = 0;
	outputDataValue._buffer = 0;

	maxRowCnt = inMaxRowCnt;
	maxRowLen = inMaxRowLen;
	currentMethod = odbc_SQLSvc_FetchPerf_ldx_;
	rc = (SQLRETURN)ControlProc(this);

	switch (rc)
	{
	case SQL_ERROR:
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+16);
	return rc;
}

//========================== Rowset =================================================

SQLRETURN SRVR_STMT_HDL::ExecuteRowset(const char *inCursorName, IDL_long inInputRowCnt, IDL_short inSqlStmtType, 
							const SQL_DataValue_def *inDataValue, 
							short inSqlAsyncEnable, Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+20);

	SQLRETURN rc;
	
	if (bSQLMessageSet)
		cleanupSQLMessage();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	inputRowCnt = inInputRowCnt;
	sqlStmtType = inSqlStmtType;

	if (inCursorName != NULL && inCursorName[0] != '\0')
	{
		cursorNameLen = strlen(inCursorName);
		cursorNameLen = cursorNameLen < sizeof(cursorName)? cursorNameLen : sizeof(cursorName);
		strcpyUTF8(cursorName, inCursorName, sizeof(cursorName));
	}
	else
		cursorName[0] = '\0';

	inputDataValue._buffer = inDataValue->_buffer;
	inputDataValue._length = inDataValue->_length;

	currentMethod = odbc_SQLSvc_ExecuteRowset_ldx_;
	rc = (SQLRETURN)ControlProc(this);

	switch (rc)
	{
	case SQL_SUCCESS:
	case SQL_SUCCESS_WITH_INFO:
		break;
	case ODBC_SERVER_ERROR:
		// Allocate Error Desc
		kdsCreateSQLErrorException(bSQLMessageSet, &sqlError, 1);
		// Add SQL Error
		kdsCopySQLErrorException(&sqlError, NULL_VALUE_ERROR, NULL_VALUE_ERROR_SQLCODE,
				NULL_VALUE_ERROR_SQLSTATE);
		break;
	case -8814:
	case 8814:
		rc = SQL_RETRY_COMPILE_AGAIN;
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+20);
	return rc;
}


SQLRETURN SRVR_STMT_HDL::PrepareFromModule(short inStmtType)
{
	SRVRTRACE_ENTER(FILE_CSTMT+21);

	SQLRETURN rc;
	size_t	len;
	
	if (isReadFromModule)
		return SQL_SUCCESS;
	// cleanup all memory allocated in the previous operations
	cleanupAll();
	stmtType = inStmtType;
	cost_info.totalTime = -1;
	currentMethod = odbc_SQLSvc_GetSQLCatalogs_ldx_;
	rc = (SQLRETURN)ControlProc(this);
	if (rc != SQL_ERROR)
		isReadFromModule = TRUE;
	SRVRTRACE_EXIT(FILE_CSTMT+21);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::freeBuffers(short descType)
{
	SRVRTRACE_ENTER(FILE_CSTMT+22);
	
	SQLRETURN rc = SQL_SUCCESS;

	switch (descType)
	{
	case SQLWHAT_INPUT_DESC:
		SESSION_DEBUG("\n\tSRVR::FREESTATEMENT SRVR_STMT_HDL::freeBuffers enter:\n\tpSrvrStmt = %p,\n\tfreeResourceOpt = %d", this, freeResourceOpt);
		if (trace_memory && inputDescVarBuffer)
			LogDelete("delete VarBuffer;", (void **)&inputDescVarBuffer, inputDescVarBuffer);
		STMT_DEALLOCATE(inputDescVarBuffer);
		inputDescVarBufferLen = 0;
		paramCount = 0;
		break;
	case SQLWHAT_OUTPUT_DESC:
		SESSION_DEBUG("\n\tSRVR::FREESTATEMENT SRVR_STMT_HDL::freeBuffers enter:\n\t\tpSrvrStmt = %p,\n\t\tfreeResourceOpt = %d\n\t\toutputDescVarBufferLen = %d, \n\t\trecordOutputDescVarBufferLen = %d", this, freeResourceOpt, outputDescVarBufferLen, recordOutputDescVarBufferLen);
		if (trace_memory && outputDescVarBuffer)
			LogDelete("delete VarBuffer;", (void **)&outputDescVarBuffer, outputDescVarBuffer);
		STMT_DEALLOCATE(outputDescVarBuffer);
		outputDescVarBufferLen = 0;
		recordOutputDescVarBufferLen = 0;
		columnCount = 0;
		break;
	default:
		rc = SQL_ERROR;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+22);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::allocSqlmxHdls(const char *inStmtName, const char *inModuleName,
			int64 inModuleTimestamp, Int32 inModuleVersion, const char *inInputDescName, 
				const char *inOutputDescName, short inSqlStmtType)
{
	SRVRTRACE_ENTER(FILE_CSTMT+23);

	SQLRETURN rc;

	stmtNameLen = strlen(inStmtName);
	stmtNameLen = stmtNameLen < sizeof(stmtName)? stmtNameLen : sizeof(stmtName);
	strcpyUTF8(stmtName, inStmtName, sizeof(stmtName));

	if (inModuleName != NULL)
	{
		moduleId.version = inModuleVersion;
		strncpy(moduleName, inModuleName, sizeof(moduleName));
		moduleName[sizeof(moduleName)-1] = 0;
		moduleId.module_name = moduleName;
		moduleId.module_name_len = strlen(moduleName);
		moduleId.charset = "ISO88591";
		moduleId.creation_timestamp = inModuleTimestamp;
	}
	else
	{
		moduleId.version = SQLCLI_ODBC_MODULE_VERSION;
		moduleId.module_name = NULL;
		moduleId.module_name_len = 0;
		moduleId.charset = "ISO88591";
		moduleId.creation_timestamp = 0;
	}
	if (inInputDescName != NULL)
		strcpyUTF8(inputDescName, inInputDescName, sizeof(inputDescName));
	if (inOutputDescName != NULL)
		strcpyUTF8(outputDescName, inOutputDescName, sizeof(outputDescName));

	sqlStmtType = inSqlStmtType;
	currentMethod = 0;
	rc = (SQLRETURN)ControlProc(this);
	SRVRTRACE_EXIT(FILE_CSTMT+23);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::ExecuteCall(const SQLValueList_def *inValueList,short inSqlAsyncEnable, 
		Int32 inQueryTimeout)
{
	SRVRTRACE_ENTER(FILE_CSTMT+24);

	SQLRETURN rc;

	if (bSQLMessageSet)
		cleanupSQLMessage();
	if(bSQLValueListSet)
		cleanupSQLValueList();
	inputValueList._buffer = inValueList->_buffer;
	inputValueList._length = inValueList->_length;

	if (outputValueList._buffer == NULL)
	{
		if ((rc = AllocAssignValueBuffer(bSQLValueListSet, &outputDescList, &outputValueList, outputDescVarBufferLen,
			1, outputValueVarBuffer)) != SQL_SUCCESS)
			return rc;
	}
	else
		outputValueList._length = 0;

	currentMethod = odbc_SQLSvc_ExecuteCall_ldx_;
	rc = (SQLRETURN)ControlProc(this);

	switch (rc)
	{
	case SQL_SUCCESS:
	case SQL_SUCCESS_WITH_INFO:
		break;
	case ODBC_SERVER_ERROR:
		// Allocate Error Desc
		kdsCreateSQLErrorException(bSQLMessageSet, &sqlError, 1);
		// Add SQL Error
		kdsCopySQLErrorException(&sqlError, NULL_VALUE_ERROR, NULL_VALUE_ERROR_SQLCODE,
				NULL_VALUE_ERROR_SQLSTATE);
		break;
	case -8814:
	case 8814:
		rc = SQL_RETRY_COMPILE_AGAIN;
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+24);
	return rc;
}

SQLRETURN SRVR_STMT_HDL::FetchCatalogRowset(Int32 inMaxRowCnt, Int32 inMaxRowLen, short inSqlAsyncEnable, Int32 inQueryTimeout, bool resetValues)
{
	SRVRTRACE_ENTER(FILE_CSTMT+25);

	SQLRETURN rc;

	if (resetValues)
	{
		if (bSQLMessageSet)
			cleanupSQLMessage();
		if (outputDataValue._buffer != NULL)
			delete outputDataValue._buffer;
		outputDataValue._length = 0;
		outputDataValue._buffer = 0;
		rowsAffected = 0;
	}

	maxRowCnt = inMaxRowCnt;
	maxRowLen = inMaxRowLen;
	currentMethod = Fetch_Catalog_Rowset;
	rc = (SQLRETURN)ControlProc(this);

	switch (rc)
	{
	case SQL_ERROR:
		break;
	default:
		break;
	}
	SRVRTRACE_EXIT(FILE_CSTMT+25);
	return rc;
}

void  SRVR_STMT_HDL::setclientLCID(UInt32 value)
{
	SRVRTRACE_ENTER(FILE_CSTMT+26);
	clientLCID = value;
	SRVRTRACE_EXIT(FILE_CSTMT+26);
	return;
}

void SRVR_STMT_HDL::logDelayedResponseMessage(char *operation) 
{
   timespec queryResponseEndTime;
   long long diffTime;
   long long diffNanoTime;
   char msg[400];
   long long diffTimeInMillis;
   int compareTimeInMillis;

 
   if (queryTimeout > 0 || EXPECTED_RESPONSE_TIME_IN_MILLIS > 0) {
      if (EXPECTED_RESPONSE_TIME_IN_MILLIS > 0)
          compareTimeInMillis = EXPECTED_RESPONSE_TIME_IN_MILLIS;
      else
          compareTimeInMillis = queryTimeout * 1000;
      clock_gettime(CLOCK_MONOTONIC, &queryResponseEndTime);
      if (queryResponseStartTime.tv_nsec > queryResponseEndTime.tv_nsec) {
         // borrow 1 from tv_sec, convert to nanosec and add to tv_nsec.
         queryResponseEndTime.tv_nsec += 1LL * 1000LL * 1000LL * 1000LL;
         queryResponseEndTime.tv_sec -= 1LL;
      }
      diffTime = (queryResponseEndTime.tv_sec - queryResponseStartTime.tv_sec);
      diffNanoTime = (queryResponseEndTime.tv_nsec - queryResponseStartTime.tv_nsec);
      diffTimeInMillis = (diffTime * 1000) + (diffNanoTime / 1000000LL);
      if (diffTimeInMillis  > compareTimeInMillis) {
         for(int ix = 0; ix < sqlStringLen ; ix++ )
            if(sqlString[ix] == '%' ) sqlString[ix] = '*'; //replace % with * 
         
         if (sqlStringLen > 0  && sqlStringLen < 250)
             snprintf(msg, sizeof(msg), "Operation: %s QueryTimeoutInSecs: %d ExpectedResponseTimeInMillis: %d ResponseTimeInMillis: %lld SqlString: %.*s ", operation, queryTimeout, compareTimeInMillis, diffTimeInMillis, 250, sqlString);
         else if (sqlStringLen != 0)
             snprintf(msg, sizeof(msg), "Operation: %s QueryTimeoutInSecs: %d ExpectedResponseTimeInMillis: %d ResponseTimeInMillis: %lld SqlString: %.*s ", operation, queryTimeout, compareTimeInMillis, diffTimeInMillis, sqlStringLen, sqlString);
         else
             snprintf(msg, sizeof(msg), "Operation: %s QueryTimeoutInSecs: %d ExpectedResponseTimeInMillis: %d ResponseTimeInMillis: %lld ", operation, queryTimeout, compareTimeInMillis, diffTimeInMillis);
         SendEventMsg(MSG_QUERY_STATUS_INFO, EVENTLOG_WARNING_TYPE,
             srvrGlobal->nskProcessInfo.processId, ODBCMX_SERVER, 
             srvrGlobal->srvrObjRef, 1, msg);
      }
   }
}

void SRVR_STMT_HDL::logDDLQuery()
{
  bool isDDLQuery =  (sqlQueryType == SQL_DDL || sqlQueryType == SQL_DDL_WITH_STATUS);
  if (isDDLQuery)
  {
    string logMsg;
    logMsg.reserve(sqlStringLen + 300);
    int max_len = MAX_IP_ADDRESS_LEN + MAX_HOST_NAME_LEN + MAX_APPLICATION_NAME_LEN + 100;
    char tmpBuf[max_len];
    logMsg.append("DDL Query : ");
    logMsg.append(sqlString);
    sprintf(tmpBuf, "; Client IP : %s, Client Host Name : %s, Application Name : %s",
                    srvrGlobal->IpAddress, srvrGlobal->HostName,
                    srvrGlobal->ApplicationName);
    logMsg.append(tmpBuf);
    _mxowarn(__FILE__, __LINE__, logMsg.c_str());
  }
}
