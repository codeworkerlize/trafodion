

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExExeUtilVolTab.cpp
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

#include "arkcmp/CmpContext.h"
#include "common/ComRtUtils.h"
#include "common/ComSqlId.h"
#include "comexe/ComTdb.h"
#include "executor/ExExeUtil.h"
#include "ExStats.h"
#include "cli/cli_stdh.h"
#include "common/ComCextdecs.h"
#include "common/ComSizeDefs.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "ex_transaction.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_expr.h"
#include "seabed/ms.h"
#include "sql_id.h"

///////////////////////////////////////////////////////////////////
ex_tcb *ExExeUtilLoadVolatileTableTdb::build(ex_globals *glob) {
  ExExeUtilLoadVolatileTableTcb *exe_util_tcb;

  exe_util_tcb = new (glob->getSpace()) ExExeUtilLoadVolatileTableTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilLoadVolatileTableTcb
///////////////////////////////////////////////////////////////
ExExeUtilLoadVolatileTableTcb::ExExeUtilLoadVolatileTableTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob)
    : ExExeUtilTcb(exe_util_tdb, NULL, glob), step_(INITIAL_) {
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);
}

//////////////////////////////////////////////////////
// work() for ExExeUtilLoadVolatileTableTcb
//////////////////////////////////////////////////////
short ExExeUtilLoadVolatileTableTcb::work() {
  int cliRC = 0;
  short retcode = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState &pstate = *((ExExeUtilPrivateState *)pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli *currContext = masterGlob->getStatement()->getContext();

  while (1) {
    switch (step_) {
      case INITIAL_: {
        step_ = INSERT_;
      } break;

      case INSERT_: {
        // let compiler know that this insert statement should be
        // treated as a regular insert stmt.
        // 0x20000 is the bit to allow this.
        // Bit defined in parser/SqlParserGlobalsCmn.h.
        //	    masterGlob->getStatement()->getContext()->setSqlParserFlags(0x20000); //
        // NO_IMPLICIT_VOLATILE_TABLE_UPD_STATS

        // issue the insert command
        long rowsAffected;

        // All internal queries issued from CliInterface assume that
        // they are in ISO_MAPPING.
        // That causes mxcmp to use the default charset as iso88591
        // for unprefixed literals.
        // The insert...select being issued out here contains the user
        // specified query and any literals in that should be using
        // the default_charset.
        // So we send the isoMapping charset instead of the
        // enum ISO_MAPPING.
        int savedIsoMapping = currContext->getSessionDefaults()->getIsoMappingEnum();
        cliInterface()->setIsoMapping(currContext->getSessionDefaults()->getIsoMappingEnum());
        cliRC = cliInterface()->executeImmediate(lvtTdb().insertQuery_, NULL, NULL, TRUE, &rowsAffected, TRUE);
        cliInterface()->setIsoMapping(savedIsoMapping);
        if (cliRC < 0) {
          ExHandleErrors(qparent_, pentry_down, 0, getGlobals(), NULL, (ExeErrorCode)cliRC, NULL, NULL);
          step_ = ERROR_;
          break;
        }

        masterGlob->setRowsAffected(rowsAffected);
        if (rowsAffected > lvtTdb().threshold_)
          step_ = UPD_STATS_;
        else
          step_ = DONE_;
      } break;

      case UPD_STATS_: {
        // issue the upd stats command
        char *usQuery = new (getHeap()) char[strlen(lvtTdb().updStatsQuery_) + 10 + 1];

        str_sprintf(usQuery, lvtTdb().updStatsQuery_, masterGlob->getRowsAffected());

        cliRC = cliInterface()->executeImmediate(usQuery, NULL, NULL, TRUE, NULL, TRUE);
        NADELETEBASIC(usQuery, getHeap());
        if (cliRC < 0) {
          ExHandleErrors(qparent_, pentry_down, 0, getGlobals(), NULL, (ExeErrorCode)cliRC, NULL, NULL);
          step_ = ERROR_;
          break;
        }

        step_ = DONE_;
      } break;

      case DONE_: {
        // reset special insert processing bit
        //	    masterGlob->getStatement()->getContext()->resetSqlParserFlags(0x20000); //
        // NO_IMPLICIT_VOLATILE_TABLE_UPD_STATS

        // Return EOF.
        ex_queue_entry *up_entry = qparent_.up->getTailEntry();

        up_entry->upState.parentIndex = pentry_down->downState.parentIndex;

        up_entry->upState.setMatchNo(0);
        up_entry->upState.status = ex_queue::Q_NO_DATA;

        // insert into parent
        qparent_.up->insert();

        step_ = INITIAL_;
        qparent_.down->removeHead();

        return WORK_OK;
      } break;

      case ERROR_: {
        step_ = DONE_;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state *ExExeUtilLoadVolatileTableTcb::allocatePstates(int &numElems,  // inout, desired/actual elements
                                                                     int &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilLoadVolatileTablePrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilLoadVolatileTablePrivateState::ExExeUtilLoadVolatileTablePrivateState() {}

ExExeUtilLoadVolatileTablePrivateState::~ExExeUtilLoadVolatileTablePrivateState(){};

///////////////////////////////////////////////////////////////////
ex_tcb *ExExeUtilCleanupVolatileTablesTdb::build(ex_globals *glob) {
  ExExeUtilCleanupVolatileTablesTcb *exe_util_tcb;

  exe_util_tcb = new (glob->getSpace()) ExExeUtilCleanupVolatileTablesTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// class ExExeUtilVolatileTablesTcb
///////////////////////////////////////////////////////////////
ExExeUtilVolatileTablesTcb::ExExeUtilVolatileTablesTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob)
    : ExExeUtilTcb(exe_util_tdb, NULL, glob) {
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);
}

short ExExeUtilVolatileTablesTcb::isCreatorProcessObsolete(const char *name, NABoolean includesCat,
                                                           NABoolean isCSETableName) {
  int retcode = 0;

  // find process start time, node name, cpu and pin of creator process.
  short segmentNum;
  short cpu;
  pid_t pin;
  long nameCreateTime = 0;

  int currPos = 0;

  if (includesCat) {
    // name is of the form:  <CAT>.<SCHEMA>
    // Skip the <CAT> part.
    while (name[currPos] != '.') currPos++;
    currPos++;
  }

  if (isCSETableName) {
    // CSE table names look like this: CSE_TEMP_<name>_MXID..._Snnn_mmm
    const char *startPrefix = "_" COM_SESSION_ID_PREFIX;
    const char *match = &name[currPos];
    const char *prevMatch = NULL;

    // find the last occurrence of the start prefix in the name
    while ((match = strstr(match, startPrefix)) != NULL) prevMatch = ++match;  // position prevMatch on the "MXID"

    if (prevMatch)
      currPos = prevMatch - name;
    else
      return 0;  // name does not fit our pattern, don't delete it
  } else
    // volatile table schema is a fixed prefix, followed by the session id
    currPos += strlen(COM_VOLATILE_SCHEMA_PREFIX);

  long segmentNum_l;
  long cpu_l;
  long pin_l;
  long sessionUniqNum;
  int userNameLen, tenantIdLen = 0;
  int userSessionNameLen = 0;

  ComSqlId::extractSqlSessionIdAttrs(&name[currPos],
                                     -1,  //(strlen(name) - currPos),
                                     segmentNum_l, cpu_l, pin_l, nameCreateTime, sessionUniqNum, userNameLen, NULL,
                                     tenantIdLen, NULL, userSessionNameLen, NULL);
  segmentNum = (short)segmentNum_l;
  cpu = (short)cpu_l;
  pin = (pid_t)pin_l;

  // see if process exists. If it exists, check if it is the same
  // process that is specified in the name.
  short errorDetail = 0;
  long procCreateTime = 0;
  retcode = ComRtGetProcessCreateTime(&cpu, &pin, &segmentNum, procCreateTime, errorDetail);
  if (retcode == XZFIL_ERR_OK) {
    // process specified in name exists.
    if (nameCreateTime != procCreateTime)
      // but is a different process. Schema or name's process is obsolete.
      return -1;
    else
      // schema or name's process is still alive.
      return 0;
  } else {
    if (retcode == XZFIL_ERR_NOSUCHDEV)
      return -1;
    else
      return 0;
  }
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state *ExExeUtilVolatileTablesTcb::allocatePstates(int &numElems,      // inout, desired/actual elements
                                                                  int &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilVolatileTablesPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilVolatileTablesPrivateState::ExExeUtilVolatileTablesPrivateState() {}

ExExeUtilVolatileTablesPrivateState::~ExExeUtilVolatileTablesPrivateState(){};

////////////////////////////////////////////////////////////////
// class ExExeUtilCleanupVolatileTablesTcb
///////////////////////////////////////////////////////////////
ExExeUtilCleanupVolatileTablesTcb::ExExeUtilCleanupVolatileTablesTcb(const ComTdbExeUtil &exe_util_tdb,
                                                                     ex_globals *glob)
    : ExExeUtilVolatileTablesTcb(exe_util_tdb, glob),
      step_(INITIAL_),
      schemaNamesList_(NULL),
      schemaQuery_(NULL),
      someSchemasCouldNotBeDropped_(FALSE) {}

//////////////////////////////////////////////////////
// work() for ExExeUtilCleanupVolatileTablesTcb
//////////////////////////////////////////////////////
static const QueryString getAllVolatileSchemasQuery[] = {{" select O.schema_name from "},
                                                         {" TRAFODION.\"_MD_\".OBJECTS O "},
                                                         {" where O.schema_name like 'VOLATILE_SCHEMA_%%' "},
                                                         {"           and O.object_type = 'SS' "},
                                                         {" order by 1 "},
                                                         {" for read uncommitted access "}};

short ExExeUtilCleanupVolatileTablesTcb::work() {
  int cliRC = 0;
  short retcode = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState &pstate = *((ExExeUtilPrivateState *)pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();

  while (1) {
    switch (step_) {
      case INITIAL_: {
        step_ = FETCH_SCHEMA_NAMES_;
        errorSchemas_[0] = 0;

      } break;

      case FETCH_SCHEMA_NAMES_: {
        int schema_qry_array_size = sizeof(getAllVolatileSchemasQuery) / sizeof(QueryString);

        const QueryString *schemaCleanupQueryString = getAllVolatileSchemasQuery;

        char *gluedQuery;
        int gluedQuerySize;
        glueQueryFragments(schema_qry_array_size, schemaCleanupQueryString, gluedQuery, gluedQuerySize);

        schemaQuery_ = new (getHeap()) char[gluedQuerySize + 10 + 1];

        str_sprintf(schemaQuery_, "%s", gluedQuery);
        NADELETEBASIC(gluedQuery, getMyHeap());

        if (initializeInfoList(schemaNamesList_)) {
          step_ = ERROR_;
          break;
        }

        if (fetchAllRows(schemaNamesList_, schemaQuery_, 1, FALSE, retcode) < 0) {
          // Delete new'd characters
          NADELETEBASIC(schemaQuery_, getHeap());
          schemaQuery_ = NULL;

          step_ = ERROR_;
          break;
        }

        // Delete new'd characters
        NADELETEBASIC(schemaQuery_, getHeap());
        schemaQuery_ = NULL;

        step_ = START_CLEANUP_;
      } break;

      case START_CLEANUP_: {
        schemaNamesList_->position();

        someSchemasCouldNotBeDropped_ = FALSE;

        if (masterGlob->getStatement()->getContext()->getTransaction()->xnInProgress()) {
          // cannot have a transaction running.
          // Return error.
          ExRaiseSqlError(getHeap(), &diagsArea_, -EXE_BEGIN_TRANSACTION_ERROR);
          step_ = ERROR_;
          break;
        }

        step_ = CHECK_FOR_OBSOLETE_CREATOR_PROCESS_;
      } break;

      case CHECK_FOR_OBSOLETE_CREATOR_PROCESS_: {
        if (schemaNamesList_->atEnd()) {
          step_ = END_CLEANUP_;
          break;
        }

        OutputInfo *vi = (OutputInfo *)schemaNamesList_->getCurr();
        char *schemaName = vi->get(0);
        if ((cvtTdb().cleanupAllTables()) || (isCreatorProcessObsolete(schemaName, FALSE, FALSE))) {
          // schema is obsolete, drop it.
          // Or we need to cleanup all schemas, active or obsolete.
          step_ = DO_CLEANUP_;
        } else {
          schemaNamesList_->advance();
        }
      } break;

      case DO_CLEANUP_: {
        OutputInfo *vi = (OutputInfo *)schemaNamesList_->getCurr();
        char *schemaName = vi->get(0);
        retcode = dropVolatileSchema(masterGlob->getStatement()->getContext(), schemaName, getHeap(), getDiagsArea(),
                                     getGlobals());
        if (retcode < 0) {
          // changes errors to warnings and move on to next schema.
          getDiagsArea()->negateAllErrors();

          // clear diags and move on to next schema.
          // Remember that an error was returned, we will
          // return a warning at the end.
          SQL_EXEC_ClearDiagnostics(NULL);
          retcode = 0;

          if ((strlen(errorSchemas_) + strlen(schemaName)) < 1000) {
            strcat(errorSchemas_, schemaName);
            strcat(errorSchemas_, " ");
          } else if (strlen(errorSchemas_) < 1005)  // maxlen = 1010
            strcat(errorSchemas_, "...");           // could not fit

          someSchemasCouldNotBeDropped_ = TRUE;
        }

        schemaNamesList_->advance();
        step_ = CHECK_FOR_OBSOLETE_CREATOR_PROCESS_;
      } break;

      case END_CLEANUP_: {
        if (someSchemasCouldNotBeDropped_) {
          // add a warning to indicate that some schemas were not
          // dropped.
          ExRaiseSqlError(getHeap(), &diagsArea_, 1069, NULL, NULL, NULL, errorSchemas_);
        }
        step_ = CLEANUP_HIVE_TABLES_;
      } break;

      case CLEANUP_HIVE_TABLES_: {
        step_ = DONE_;
      } break;

      case ERROR_: {
        if (handleError()) return WORK_OK;

        getDiagsArea()->clear();

        step_ = DONE_;
      } break;

      case DONE_: {
        if (handleDone()) return WORK_OK;

        step_ = INITIAL_;

        return WORK_OK;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}

short ExExeUtilCleanupVolatileTablesTcb::dropVolatileSchema(ContextCli *currContext, char *schemaName, CollHeap *heap,
                                                            ComDiagsArea *&diagsArea, ex_globals *glob) {
  const char *parentQid = NULL;
  if (glob) {
    ExExeStmtGlobals *stmtGlobals = glob->castToExExeStmtGlobals();
    if (stmtGlobals->castToExMasterStmtGlobals())
      parentQid = stmtGlobals->castToExMasterStmtGlobals()->getStatement()->getUniqueStmtId();
    else {
      ExEspStmtGlobals *espGlobals = stmtGlobals->castToExEspStmtGlobals();
      if (espGlobals && espGlobals->getStmtStats()) parentQid = espGlobals->getStmtStats()->getQueryId();
    }
  }
  ExeCliInterface cliInterface(heap, 0, currContext, parentQid);

  char *dropSchema = NULL;

  if (schemaName) {
    dropSchema = new (heap) char[strlen("DROP VOLATILE SCHEMA CLEANUP CASCADE; ") + strlen(schemaName) + 1];
    strcpy(dropSchema, "DROP VOLATILE SCHEMA ");
    strcat(dropSchema, schemaName);
    strcat(dropSchema, " CLEANUP CASCADE;");
  } else {
    dropSchema = new (heap) char[strlen("DROP IMPLICIT VOLATILE SCHEMA CLEANUP CASCADE; ") + 1];
    strcpy(dropSchema, "DROP IMPLICIT VOLATILE SCHEMA CLEANUP CASCADE;");
  }

  // let compiler know that volatile schema could be dropped.
  // 0x8000 is the bit to allow this.
  // Bit defined in parser/SqlParserGlobalsCmn.h.
  //  currContext->setSqlParserFlags(0x8000); // ALLOW_VOLATILE_SCHEMA_CREATION

  // issue the drop schema command
  int cliRC = cliInterface.executeImmediate(dropSchema);
  cliInterface.allocAndRetrieveSQLDiagnostics(diagsArea);

  // reset volatile schema bit
  //  currContext->resetSqlParserFlags(0x8000); // ALLOW_VOLATILE_SCHEMA_CREATION

  NADELETEBASIC(dropSchema, heap);
  currContext->resetVolTabList();
  return cliRC;
}

short ExExeUtilCleanupVolatileTablesTcb::dropVolatileTables(ContextCli *currContext, CollHeap *heap) {
  HashQueue *volTabList = currContext->getVolTabList();

  if ((!volTabList) || (volTabList->numEntries() == 0)) return 0;

  ExeCliInterface cliInterface(heap, 0, currContext);

  char *dropSchema = new (heap) char[strlen("DROP IMPLICIT VOLATILE SCHEMA TABLES CLEANUP CASCADE; ") + 1];
  strcpy(dropSchema, "DROP IMPLICIT VOLATILE SCHEMA TABLES CLEANUP CASCADE;");

  // let compiler know that volatile schema could be dropped.
  // 0x8000 is the bit to allow this.
  // Bit defined in parser/SqlParserGlobalsCmn.h.
  // currContext->setSqlParserFlags(0x8000); // ALLOW_VOLATILE_SCHEMA_CREATION

  // issue the drop schema command
  int cliRC = cliInterface.executeImmediate(dropSchema);

  // reset volatile schema bit
  // currContext->resetSqlParserFlags(0x8000); // ALLOW_VOLATILE_SCHEMA_CREATION

  NADELETEBASIC(dropSchema, heap);

  char *sendCQD = new (heap) char[strlen("CONTROL QUERY DEFAULT VOLATILE_SCHEMA_IN_USE 'FALSE';") + 1];
  strcpy(sendCQD, "CONTROL QUERY DEFAULT VOLATILE_SCHEMA_IN_USE 'FALSE';");
  cliInterface.executeImmediate(sendCQD);
  NADELETEBASIC(sendCQD, heap);
  currContext->resetVolTabList();
  return cliRC;
}

///////////////////////////////////////////////////////////////////
// class ExExeUtilGetVolatileInfoTdb
///////////////////////////////////////////////////////////////
ex_tcb *ExExeUtilGetVolatileInfoTdb::build(ex_globals *glob) {
  ExExeUtilGetVolatileInfoTcb *exe_util_tcb;

  exe_util_tcb = new (glob->getSpace()) ExExeUtilGetVolatileInfoTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// class ExExeUtilGetVolatileInfoTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetVolatileInfoTcb::ExExeUtilGetVolatileInfoTcb(const ComTdbExeUtil &exe_util_tdb, ex_globals *glob)
    : ExExeUtilVolatileTablesTcb(exe_util_tdb, glob), step_(INITIAL_), prevInfo_(NULL), infoQuery_(NULL) {
  infoQuery_ = new (glob->getDefaultHeap()) char[10000];
}

//////////////////////////////////////////////////////
// class ExExeUtilGetVolatileInfoTcb::work
//////////////////////////////////////////////////////

static const QueryString getAllVolatileTablesQuery[] = {
    {" select O.schema_name, O.object_type, O.object_name from "},
    {" TRAFODION.\"_MD_\".OBJECTS O "},
    {" where O.schema_name like 'VOLATILE_SCHEMA_%%' "},
    {"           and (O.object_type = 'BT' or O.object_type = 'IX') "},
    {"          order by 1,2,3 "},
    {" for read uncommitted access "}};

static const QueryString getAllVolatileTablesInASessionQuery[] = {
    {" select O.schema_name, O.object_type, O.object_name from "},
    {" TRAFODION.\"_MD_\".OBJECTS O "},
    {" where O.schema_name like 'VOLATILE_SCHEMA_' || trim(substr('%s', 1, 42)) || '%%' "},
    {"           and (O.object_type = 'BT' or O.object_type = 'IX') "},
    {"          order by 1,2 "},
    {" for read uncommitted access "}};

short ExExeUtilGetVolatileInfoTcb::work() {
  int cliRC = 0;
  short retcode = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState &pstate = *((ExExeUtilPrivateState *)pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();

  while (1) {
    switch (step_) {
      case INITIAL_: {
        step_ = APPEND_NEXT_QUERY_FRAGMENT_;
      } break;

      case APPEND_NEXT_QUERY_FRAGMENT_: {
        int info_qry_array_size = -1;
        const QueryString *infoQueryString = NULL;

        // extra space to be allocated to fill with "%s" fillers
        // in the query text.
        int extraSpace = 0;

        if (gviTdb().allSchemas()) {
          info_qry_array_size = sizeof(getAllVolatileSchemasQuery) / sizeof(QueryString);

          infoQueryString = getAllVolatileSchemasQuery;
        } else if (gviTdb().allTables()) {
          info_qry_array_size = sizeof(getAllVolatileTablesQuery) / sizeof(QueryString);

          infoQueryString = getAllVolatileTablesQuery;
        } else if (gviTdb().allTablesInASession()) {
          info_qry_array_size = sizeof(getAllVolatileTablesInASessionQuery) / sizeof(QueryString);

          infoQueryString = getAllVolatileTablesInASessionQuery;
        }

        char *param1 = gviTdb().param1_;
        char *param2 = gviTdb().param2_;

        char *gluedQuery;
        int gluedQuerySize;
        glueQueryFragments(info_qry_array_size, infoQueryString, gluedQuery, gluedQuerySize);

        str_sprintf(infoQuery_, gluedQuery, param1, param2);

        // Delete new'd characters
        NADELETEBASIC(gluedQuery, getHeap());
        gluedQuery = NULL;

        step_ = FETCH_ALL_ROWS_;
      } break;

      case FETCH_ALL_ROWS_: {
        int numOutputEntries = 0;

        if (gviTdb().allSchemas()) {
          numOutputEntries = 1;
        } else if (gviTdb().allTables()) {
          numOutputEntries = 3;
        } else if (gviTdb().allTablesInASession()) {
          numOutputEntries = 3;
        }

        if (initializeInfoList(infoList_)) {
          step_ = ERROR_;
          break;
        }

        if (fetchAllRows(infoList_, infoQuery_, numOutputEntries, FALSE, retcode) < 0) {
          step_ = ERROR_;

          NADELETEBASIC(infoQuery_, getHeap());
          infoQuery_ = NULL;

          break;
        }

        NADELETEBASIC(infoQuery_, getHeap());
        infoQuery_ = NULL;

        infoList_->position();

        if (gviTdb().allSchemas()) {
          step_ = RETURN_ALL_SCHEMAS_;
        } else if (gviTdb().allTables()) {
          step_ = RETURN_ALL_TABLES_;
        } else if (gviTdb().allTablesInASession()) {
          step_ = RETURN_TABLES_IN_A_SESSION_;
        } else
          step_ = ERROR_;
      } break;

      case RETURN_ALL_SCHEMAS_: {
        if (infoList_->atEnd()) {
          step_ = DONE_;
          break;
        }

        if (qparent_.up->isFull()) return WORK_OK;

        char outBuf[400 + ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES];

        OutputInfo *vi = (OutputInfo *)infoList_->getCurr();
        char *schemaName = vi->get(0);

        char state[10];
        if (isCreatorProcessObsolete(schemaName, FALSE, FALSE))
          strcpy(state, "Obsolete");
        else
          strcpy(state, "Active  ");

        str_sprintf(outBuf, "Schema(%8s): %s", state, schemaName);
        moveRowToUpQueue(outBuf);

        infoList_->advance();
      } break;

      case RETURN_ALL_TABLES_:
      case RETURN_TABLES_IN_A_SESSION_: {
        if (infoList_->atEnd()) {
          step_ = DONE_;
          break;
        }

        if ((qparent_.up->getSize() - qparent_.up->getLength()) < 4) return WORK_CALL_AGAIN;

        char outBuf[400 + ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES];

        OutputInfo *vi = (OutputInfo *)infoList_->getCurr();

        char *schemaName = vi->get(0);
        if ((!prevInfo_) || (strcmp(prevInfo_->get(0), schemaName) != 0)) {
          char state[10];
          if (isCreatorProcessObsolete(schemaName, FALSE, FALSE))
            strcpy(state, "Obsolete");
          else
            strcpy(state, "Active  ");

          str_sprintf(outBuf, "Schema(%8s): %s", state, schemaName);
          moveRowToUpQueue(outBuf);

          prevInfo_ = (OutputInfo *)infoList_->getCurr();
        }

        char objectType[6];
        if (strcmp(vi->get(1), "BT") == 0)
          strcpy(objectType, "Table");
        else if (strcmp(vi->get(1), "IX") == 0)
          strcpy(objectType, "Index");

        str_sprintf(outBuf, "  %5s: %s", objectType, vi->get(2));
        moveRowToUpQueue(outBuf);

        infoList_->advance();
      } break;

      case ERROR_: {
        step_ = DONE_;
      } break;

      case DONE_: {
        if (qparent_.up->isFull()) return WORK_OK;

        // Return EOF.
        ex_queue_entry *up_entry = qparent_.up->getTailEntry();

        up_entry->upState.parentIndex = pentry_down->downState.parentIndex;

        up_entry->upState.setMatchNo(0);
        up_entry->upState.status = ex_queue::Q_NO_DATA;

        // insert into parent
        qparent_.up->insert();

        step_ = INITIAL_;
        qparent_.down->removeHead();

        return WORK_OK;
      } break;

    }  // switch
  }    // while

  return WORK_OK;
}
