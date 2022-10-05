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
#ifndef SQLCLIDEV_HDR
#define SQLCLIDEV_HDR

/* -*-C++-*-
******************************************************************************
*
* File:         SQLCLIdev.h
* Description:  Declarations for the internal NonStop SQL CLI.  This file
*               replaces the development includes of SQLCLI.h
*
* Created:      2/3/98
* Language:     C and C++
*
*
*
*
******************************************************************************
*/

#include "cli/sqlcli.h"
#include "common/Platform.h"  // 64-BIT
#include <set>
#include <string>

class ComDiagsArea;
class Statement;
class ComTdb;
class ExStatisticsArea;

// For internal use only -- do not document!
void SQL_EXEC_SetParserFlagsForExSqlComp_Internal(
    /*IN*/ ULng32 flagbits);

// For internal use only -- do not document!
int SQL_EXEC_SetParserFlagsForExSqlComp_Internal2(
    /*IN*/ ULng32 flagbits);

// For internal use only -- do not document!
void SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(
    /*IN*/ ULng32 flagbits);

// For internal use only -- do not document!
int SQL_EXEC_ResetParserFlagsForExSqlComp_Internal2(
    /*IN*/ ULng32 flagbits);

int SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(
    /*IN*/ ULng32 flagbits);

int SQL_EXEC_GetParserFlagsForExSqlComp_Internal(
    /*IN*/ ULng32 &flagbits);

int SQL_EXEC_GetTotalTcbSpace(char *tdb, char *otherInfo);

// For internal use only -- do not document!
// This method returns the type of stats that were collected.
// If statement_id is not passed in, this method returns info for
// the last statement that was executed.
// See comexe/Comtdb.cpp, CollectStatsType enum for the numeric values
// corresponding to various stats that could be collected.
// enum SQLCLIDevCollectStatsType declared here has the same values as
// CollectStatsType enum in ComTdb. Should we move it to a common place
// so everyone can access it? TBD. Maybe.
// This method is currenly called by mxci only.
enum SQLCLIDevCollectStatsType {
  SQLCLIDEV_SAME_STATS = SQLCLI_SAME_STATS,
  SQLCLIDEV_NO_STATS = SQLCLI_NO_STATS,
  SQLCLIDEV_ACCUMULATED_STATS = SQLCLI_ACCUMULATED_STATS,  // collect accumulated stats.
  SQLCLIDEV_PERTABLE_STATS = SQLCLI_PERTABLE_STATS,        // collect stats on a per table basis
  SQLCLIDEV_ALL_STATS = SQLCLI_ALL_STATS,                  // collect all stats about all exe operators
  SQLCLIDEV_OPERATOR_STATS = SQLCLI_OPERATOR_STATS         // collect all stats but merge at
                                                           // operator(tdb) granularity.
                                                           // Used to return data at user operator
                                                           // level.

};
// This internal call allows a caller to switch back to the default context.
// In general this is not allow. But it's currently used in one place in
// UdrServer alone
int SQL_EXEC_SwitchContext_Internal(/*IN*/ int context_handle,
                                      /*OUT OPTIONAL*/ int *prev_context_handle,
                                      /*IN*/ int allowSwitchBackToDefault);

enum SQLATTRHOLDABLE_INTERNAL_TYPE {
  SQLCLIDEV_NONHOLDABLE = SQL_NONHOLDABLE,
  SQLCLIDEV_HOLDABLE = SQL_HOLDABLE,
  SQLCLIDEV_ANSI_HOLDABLE = 2,
  SQLCLIDEV_PUBSUB_HOLDABLE = 3
};

int SQL_EXEC_GetCollectStatsType_Internal(
    /*OUT*/ ULng32 *collectStatsType,
    /*IN*/ SQLSTMT_ID *statement_id);

// For internal use only -- do not document!
// Sets the input environ (list of envvars) in cli globals
// so they could be used by executor.
// if propagate is set to 1, then propagate environment to mxcmp now.
// Otherwise, set them in internal cli globals so they could be propagated
// the next time mxcmp is started.
int SQL_EXEC_SetEnviron_Internal(/*IN*/ int propagate);

#ifndef NO_SQLCLIDEV_INCLUDES
#include "common/sql_charset_strings.h"
#endif

// SQLDESC_CHAR_SET_CAT, SQLDESC_CHAR_SET_SCH, SQLDESC_CHAR_SET_NAM
// SQLDESC_COLLATION, SQLDESC_COLL_CAT, SQLDESC_COLL_SCH and SQLDESC_COLL_NAM
// can only be set by SQL/MX engine.
const signed char SQLDESC_TYPE_ORDER = 0,
                  SQLDESC_DATETIME_CODE_ORDER = 1,  // ANSI DATETIME_INTERVAL_CODE
    SQLDESC_LENGTH_ORDER = 6, SQLDESC_OCTET_LENGTH_ORDER = -1, SQLDESC_PRECISION_ORDER = 3,
                  SQLCESC_UNUSED_ITEM1_ORDER = -1, SQLDESC_SCALE_ORDER = 4,
                  SQLDESC_INT_LEAD_PREC_ORDER = 2,  // ANSI DATETIME_INTERVAL_PRECISION
    SQLDESC_NULLABLE_ORDER = -1, SQLDESC_CHAR_SET_ORDER = 5, SQLDESC_CHAR_SET_CAT_ORDER = -1,
                  SQLDESC_CHAR_SET_SCH_ORDER = -1, SQLDESC_CHAR_SET_NAM_ORDER = 5, SQLDESC_COLLATION_ORDER = -1,
                  SQLDESC_COLL_CAT_ORDER = -1, SQLDESC_COLL_SCH_ORDER = -1, SQLDESC_COLL_NAM_ORDER = -1,
                  SQLDESC_NAME_ORDER = -1, SQLDESC_UNNAMED_ORDER = -1, SQLDESC_HEADING_ORDER = -1,
                  SQLDESC_IND_TYPE_ORDER = 7, SQLDESC_VAR_PTR_ORDER = 11, SQLDESC_IND_PTR_ORDER = 10,
                  SQLDESC_RET_LEN_ORDER = -1, SQLDESC_RET_OCTET_LEN_ORDER = -1, SQLDESC_VAR_DATA_ORDER = 11,
                  SQLDESC_IND_DATA_ORDER = 10, SQLDESC_TYPE_ANSI_ORDER = 0, SQLDESC_IND_LENGTH_ORDER = -1,
                  SQLDESC_ROWSET_VAR_LAYOUT_SIZE_ORDER = 9, SQLDESC_ROWSET_IND_LAYOUT_SIZE_ORDER = 8,
                  SQLDESC_ROWSET_SIZE_ORDER = -1, SQLDESC_ROWSET_HANDLE_ORDER = -1,
                  SQLDESC_ROWSET_NUM_PROCESSED_ORDER = -1, SQLDESC_ROWSET_ADD_NUM_PROCESSED_ORDER = -1,
                  SQLDESC_ROWSET_STATUS_PTR_ORDER = -1, SQLDESC_ITEM_ORDER_COUNT = SQLDESC_VAR_DATA_ORDER + 1;

const signed char SQLDESC_ITEM_MAX = SQLDESC_ROWSET_STATUS_PTR;

const signed char SQLDESC_ITEM_ORDER[SQLDESC_ITEM_MAX] = {SQLDESC_TYPE_ORDER,
                                                          SQLDESC_DATETIME_CODE_ORDER,
                                                          SQLDESC_LENGTH_ORDER,
                                                          SQLDESC_OCTET_LENGTH_ORDER,
                                                          SQLDESC_PRECISION_ORDER,
                                                          SQLCESC_UNUSED_ITEM1_ORDER,
                                                          SQLDESC_SCALE_ORDER,
                                                          SQLDESC_INT_LEAD_PREC_ORDER,
                                                          SQLDESC_NULLABLE_ORDER,
                                                          SQLDESC_CHAR_SET_ORDER,
                                                          SQLDESC_CHAR_SET_CAT_ORDER,
                                                          SQLDESC_CHAR_SET_SCH_ORDER,
                                                          SQLDESC_CHAR_SET_NAM_ORDER,
                                                          SQLDESC_COLLATION_ORDER,
                                                          SQLDESC_COLL_CAT_ORDER,
                                                          SQLDESC_COLL_SCH_ORDER,
                                                          SQLDESC_COLL_NAM_ORDER,
                                                          SQLDESC_NAME_ORDER,
                                                          SQLDESC_UNNAMED_ORDER,
                                                          SQLDESC_HEADING_ORDER,
                                                          SQLDESC_IND_TYPE_ORDER,
                                                          SQLDESC_VAR_PTR_ORDER,
                                                          SQLDESC_IND_PTR_ORDER,
                                                          SQLDESC_RET_LEN_ORDER,
                                                          SQLDESC_RET_OCTET_LEN_ORDER,
                                                          SQLDESC_VAR_DATA_ORDER,
                                                          SQLDESC_IND_DATA_ORDER,
                                                          SQLDESC_TYPE_ANSI_ORDER,
                                                          SQLDESC_IND_LENGTH_ORDER,
                                                          SQLDESC_ROWSET_VAR_LAYOUT_SIZE_ORDER,
                                                          SQLDESC_ROWSET_IND_LAYOUT_SIZE_ORDER,
                                                          SQLDESC_ROWSET_SIZE_ORDER,
                                                          SQLDESC_ROWSET_HANDLE_ORDER,
                                                          SQLDESC_ROWSET_NUM_PROCESSED_ORDER,
                                                          SQLDESC_ROWSET_ADD_NUM_PROCESSED_ORDER,
                                                          SQLDESC_ROWSET_STATUS_PTR_ORDER};

enum UDRErrorFlag {
  /* The bit offset in the bitmap vector */
  SQLUDR_SQL_VIOL = 0x01,  /* SQL access mode violation */
  SQLUDR_XACT_VIOL = 0x02, /* attempt to issue transaction statements */
  SQLUDR_UNUSED_1 = 0x04,  /* not used */
  SQLUDR_UNUSED_2 = 0x08,  /* not used */
  SQLUDR_XACT_ABORT = 0x10 /* transaction was aborted */
};

int SQL_EXEC_GetUdrErrorFlags_Internal(/*OUT*/ int *udrErrorFlags);
/* returns a bitmap vector of flags defined in enum UDRErrorFlag */

int SQL_EXEC_ResetUdrErrorFlags_Internal();

int SQL_EXEC_SetUdrAttributes_Internal(/*IN*/ int sqlAccessMode,
                                         /*IN*/ int /* for future use */);

int SQL_EXEC_SetUdrRuntimeOptions_Internal(/*IN*/ const char *options,
                                             /*IN*/ ULng32 optionsLen,
                                             /*IN*/ const char *delimiters,
                                             /*IN*/ ULng32 delimsLen);

// For internal use only -- do not document!
// This method sets flag in CliGlobal to enable break handling.
int SQL_EXEC_BreakEnabled_Internal(/*IN*/ UInt32 enabled);

// For internal use only -- do not document!
// This method checks a flag in CliGlobal to see if a break signal was
// received while executing a stored proc. It also resets this
// flag. This flag is used by mxci to display the appropriate break error
// message for operations that require the RECOVER command to be run.
int SQL_EXEC_SPBreakReceived_Internal(/*OUT*/ UInt32 *breakRecvd);

// For internal use only -- do not document!
// This method merges the CLI diags area into the caller's diags area
int SQL_EXEC_MergeDiagnostics_Internal(/*INOUT*/ ComDiagsArea &newDiags);

// For internal use only -- do not document!
// This method returns the CLI diags area in packed format
int SQL_EXEC_GetPackedDiagnostics_Internal(
    /*OUT*/ char *message_buffer_ptr,
    /*IN*/ ULng32 message_obj_size,
    /*OUT*/ ULng32 *message_obj_size_needed,
    /*OUT*/ int *message_obj_type,
    /*OUT*/ int *message_obj_version);

enum ROWSET_TYPE {
  ROWSET_NOT_SPECIFIED = 0,
  ROWSET_COLUMNWISE = 1,
  ROWSET_ROWWISE = 2,
  ROWSET_ROWWISE_V1 = ROWSET_ROWWISE,
  ROWSET_ROWWISE_V2 = 3
};

enum SQLCLIDevVersionType {
  SQLCLIDEV_MODULE_VERSION = 1,
  SQLCLIDEV_STATIC_STMT_PLAN_VERSION = 2,
  SQLCLIDEV_DYN_STMT_PLAN_VERSION = 3,
  SQLCLIDEV_SYSTEM_VERSION = 4,
  SQLCLIDEV_SYSTEM_MODULE_VPROC_VERSION = 5,
  SQLCLIDEV_MODULE_VPROC_VERSION = 6
};

int SQL_EXEC_GetVersion_Internal(/*IN*/ int versionType,
                                   /*OUT*/ int *versionValue,
                                   /*IN OPTIONAL*/ const char *nodeName,
                                   /*IN OPTIONAL*/ const SQLMODULE_ID *module_name,
                                   /*IN OPTIONAL*/ const SQLSTMT_ID *statement_id);

#ifdef __cplusplus
/* use C linkage */
extern "C" {
#endif

int SQL_EXEC_GetAuthID(const char *authName, int &authID);

int SQL_EXEC_GetAuthName_Internal(int auth_id, char *string_value, int max_string_len, int &len_of_item);

int SQL_EXEC_GetDatabaseUserName_Internal(
    /*IN*/ int user_id,
    /*OUT*/ char *string_value,
    /*IN*/ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item);

int SQL_EXEC_GetDatabaseUserID_Internal(
    /*IN*/ char *string_value,
    /*OUT*/ int *numeric_value);

int SQL_EXEC_SetSessionAttr_Internal(
    /*IN (SESSIONATTR_TYPE)*/ int attrName,
    /*IN OPTIONAL*/ int numeric_value,
    /*IN OPTIONAL*/ const char *string_value);

int SQL_EXEC_SetErrorCodeInRTS(
    /*IN*/ SQLSTMT_ID *statement_id,
    /*IN*/ int sqlErrorCode);

int SQL_EXEC_GetRoleList(int &numEntries, int *&roleIDs, int *&granteeIDs);

int SQL_EXEC_ResetRoleList_Internal();

void SQL_EXEC_InitGlobals();

/*
Statistics info collected for Replicate Operator
ComTdb::ex_REPLICATE in the replicator processes
*/

#define REPLICATOR_STATS_EYE_CATCHER "REOS"

typedef struct SQL_REPLICATOR_OPERATOR_STATS {
  char eye_catcher[4];
  long long operCpuTime;
  char source_filename[52];
  char target_filename[52];
  int blocklen;
  long long total_compress_time;
  long long total_compressed_bytes;
  long long total_uncompress_time;
  long long total_uncompressed_bytes;
  long long rows_read;
  long long total_blocks;
  long long blocks_replicated;
  int percent_done;
  long long blocks_read;
} REPLICATOR_OPERATOR_STATS;

typedef struct SQLCLI_OBJ_ID SQLQUERY_ID;
/*
Registers the query in RMS shared segment for any process that runs in non-priv mode.
This function should be called from the same process that is intending the register
the simulated and/or actual query fragment. This function registers the query
fragement and creates a root operator entry and a operator stats entry based
on  the tdb type.

Tdb Type                    Struct type
ComTdb::ex_REPLICATE        SQL_REPLICATOR_OPERATOR_STATS
*/
int SQL_EXEC_RegisterQuery(SQLQUERY_ID *queryId, int fragId, int tdbId, int explainTdbId,
                             short collectStatsType, int instNum, int tdbType, char *tdbName, int tdbNameLen);

/*
Deregisters the query in RMS shared segment
*/
int SQL_EXEC_DeregisterQuery(SQLQUERY_ID *queryId, int fragId);

enum SECliQueryType {
  SE_CLI_CREATE_CONTEXT,
  SE_CLI_DROP_CONTEXT,
  SE_CLI_SWITCH_CONTEXT,
  SE_CLI_CURRENT_CONTEXT,

  // clear global diags
  SE_CLI_CLEAR_DIAGS,

  // executeImmediate
  SE_CLI_EXEC_IMMED,

  // executeImmediatePrepare
  SE_CLI_EXEC_IMMED_PREP,

  // executeImmediate clearExecFetchClose
  SE_CLI_EXEC_IMMED_CEFC,

  // clearExecFetchClose
  SE_CLI_CEFC,

  // prologue to fetch rows (prepare, set up descriptors...)
  SE_CLI_FETCH_ROWS_PROLOGUE,

  // open cursor
  SE_CLI_EXEC,

  // fetch a row
  SE_CLI_FETCH,

  // close cursor
  SE_CLI_CLOSE,

  SE_CLI_STATUS_XN,
  SE_CLI_BEGIN_XN,
  SE_CLI_COMMIT_XN,
  SE_CLI_ROLLBACK_XN,

  SE_CLI_GET_DATA_OFFSETS,
  SE_CLI_GET_PTR_AND_LEN,

  SE_CLI_GET_IO_LEN,

  // get attributes of the statement.
  SE_CLI_GET_STMT_ATTR,

  // deallocate the statement
  SE_CLI_DEALLOC,

  // queue of TrafSE specific info maintained in context
  SE_CLI_TRAFQ_INSERT,
  SE_CLI_TRAFQ_GET
};

int SQL_EXEC_SEcliInterface(
    SECliQueryType qType,

    void **cliInterface, /* IN: if passed in and not null, use it.
                                                    OUT: if returned, save it and pass it back in */

    const char *inStrParam1 = NULL, const char *inStrParam2 = NULL, int inIntParam1 = -1, int inIntParam2 = -1,

    char **outStrParam1 = NULL, char **outStrParam2 = NULL, int *outIntParam1 = NULL

);

// This method returns the pointer to the CLI ExStatistics area.
// The returned pointer is a read only pointer, its contents cannot be
// modified by the caller.
int SQL_EXEC_GetStatisticsArea_Internal(
    /* IN */ short statsReqType,
    /* IN */ char *statsReqStr,
    /* IN */ int statsReqStrLen,
    /* IN */ short activeQueryNum,
    /* IN */ short statsMergeType,
    /*INOUT*/ const ExStatisticsArea *&exStatsArea);

int SQL_EXEC_GetObjectEpochStats_Internal(
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /* IN */ bool active,
    /*INOUT*/ ExStatisticsArea *&exStatsArea);

int SQL_EXEC_GetObjectLockStats_Internal(
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /*INOUT*/ ExStatisticsArea *&exStatsArea);

int SQL_EXEC_SWITCH_TO_COMPILER_TYPE(
    /*IN*/ int cmpCntxtType);

int SQL_EXEC_SWITCH_TO_COMPILER(
    /*IN*/ void *cmpCntxt);

int SQL_EXEC_SWITCH_BACK_COMPILER();

int SQL_EXEC_SeqGenCliInterface(void **cliInterface, /* IN: if passed in and not null, use it.
                                                                OUT: if returned, save it and pass it back in */

                                  void *seqGenAttrs);

int SQL_EXEC_OrderSeqXDCCliInterface(void **cliInterface, /* IN: if passed in and not null, use it.
                                                                     OUT: if returned, save it and pass it back in */
                                       void *seqGenAttrs, long endValue);

const int NullCliRoutineHandle = -1;

int SQL_EXEC_GetRoutine(
    /* IN */ const char *serializedInvocationInfo,
    /* IN */ int invocationInfoLen,
    /* IN */ const char *serializedPlanInfo,
    /* IN */ int planInfoLen,
    /* IN */ int language,
    /* IN */ int paramStyle,
    /* IN */ const char *externalName,
    /* IN */ const char *containerName,
    /* IN */ const char *externalPath,
    /* IN */ const char *librarySqlName,
    /* OUT */ int *handle

);

int SQL_EXEC_InvokeRoutine(
    /* IN */ int handle,
    /* IN */ int phaseEnumAsInt,
    /* IN */ const char *serializedInvocationInfo,
    /* IN */ int invocationInfoLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN */ const char *serializedPlanInfo,
    /* IN */ int planInfoLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut,
    /* IN */ char *inputRow,
    /* IN */ int inputRowLen,
    /* OUT */ char *outputRow,
    /* IN */ int outputRowLen);

int SQL_EXEC_GetRoutineInvocationInfo(
    /* IN */ int handle,
    /* IN/OUT */ char *serializedInvocationInfo,
    /* IN */ int invocationInfoMaxLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN/OUT */ char *serializedPlanInfo,
    /* IN */ int planInfoMaxLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut);

int SQL_EXEC_PutRoutine(
    /* IN */ int handle);

#ifdef __cplusplus
/* end of C linkage */
}
#endif

#endif /* SQLCLIDEV_HDR */
