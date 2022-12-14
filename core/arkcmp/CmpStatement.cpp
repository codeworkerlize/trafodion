
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpStatement.C
 * Description:  This file contains the routines to process Executor requesters
 *               into replys back to executor by calling the compiler internal
 *               routines. (This file should perform the same jobs as cmpmain.)
 *
 * Created:      06/24/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_NADEFAULTS  // should precede all other #include's

#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iostream>

// declaration of the yacc parser and its result
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#ifndef SQLPARSERGLOBALS_LEX_AND_PARSE
#define SQLPARSERGLOBALS_LEX_AND_PARSE
#endif

#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS_SET
//#include "parser/SqlParserGlobalsCmn.h"
#include "optimizer/EstLogProp.h"  // Pick up definition of GLOBAL_EMPTY_INPUT_LOGPROP
#include "arkcmp/ProcessEnv.h"
#include "arkcmp/CmpErrLog.h"
#include "arkcmp/CmpErrors.h"
#include "arkcmp/CmpStatement.h"
#include "arkcmp/CmpStoredProc.h"
#include "cli/Context.h"
#include "cli/Globals.h"
#include "common/BaseTypes.h"
#include "common/CmpCommon.h"
#include "common/ComMPLoc.h"
#include "common/NAMemory.h"
#include "common/NAString.h"
#include "common/NAUserId.h"
#include "common/QueryText.h"
#include "exp/ExpError.h"
#include "export/ComDiags.h"
#include "generator/Generator.h"
#include "optimizer/Analyzer.h"
#include "optimizer/CompilationStats.h"
#include "optimizer/ControlDB.h"
#include "optimizer/RelExeUtil.h"
#include "optimizer/RelMisc.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/UdfDllInteraction.h"
#include "optimizer/opt.h"            // to initialize the memo and task_list variables
#include "parser/SqlParserGlobals.h"  // must be the last #include.
#include "parser/StmtDDLNode.h"
#include "sqlcomp/CmpDescribe.h"
#include "sqlcomp/CmpMain.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseDDLupgrade.h"
#include "sqlcomp/QCache.h"
#include "sqlmsg/ErrorMessage.h"
#include "sqludr/sqludr.h"

//#include "parser/SqlParserGlobals.h"  // must be the last #include.

extern THREAD_P jmp_buf ExportJmpBuf;

// -----------------------------------------------------------------------
// helper routines for CmpStatement class
// -----------------------------------------------------------------------

NABoolean CmpStatement::error(int no, const char *s) {
  if (diags()->getNumber())
    return TRUE;  // means the underlying routines have put the errors into
                  // diags.
  *diags() << DgSqlCode(no) << DgString0(s);

  return TRUE;
}

// -----------------------------------------------------------------------
// implementation of CmpStatement class
// -----------------------------------------------------------------------

// extern THREAD_P TaskMonitor* simpleFSOMonPtr;
// extern THREAD_P TaskMonitor* complexFSOMonPtr;

CmpStatement::CmpStatement(CmpContext *context, CollHeap *outHeap) : parserStmtLiteralList_(outHeap) {
  exceptionRaised_ = FALSE;
  reply_ = NULL;
  bound_ = NULL;
  context_ = context;
  storedProc_ = 0;
  prvCmpStatement_ = 0;
  sqlTextStr_ = NULL;
  sqlTextLen_ = 0;
  sqlTextCharSet_ = (int)SQLCHARSETCODE_UNKNOWN;
  recompiling_ = FALSE;
  recompUseNATableCache_ = FALSE;
  isDDL_ = FALSE;
  isSMDRecompile_ = FALSE;
  displayGraph_ = FALSE;
  cses_ = NULL;
  detailsOnRefusedRequirements_ = NULL;
  numOfCompilationRetries_ = 0;
  sentryPrivRecheck_ = FALSE;

#ifndef NDEBUG
  if (getenv("ARKCMP_NO_STATEMENTHEAP"))
    heap_ = 0;
  else
#endif
  {
    // set up statement heap with 32 KB allocation units
    size_t memLimit = (size_t)1024 * CmpCommon::getDefaultLong(MEMORY_LIMIT_CMPSTMT_UPPER_KB);
    heap_ = new (context_->heap()) NAHeap((const char *)"Cmp Statement Heap", context_->heap(), (int)32768, memLimit);
    heap_->setErrorCallback(&CmpErrLog::CmpErrLogCallback);
  }

  // Embedded arkcmp reply is consumed by the caller before the CmpStatement
  // is deleted, hence use CmpStatement Heap itself to avoid any leaks

  if (context_->isEmbeddedArkcmp())
    outHeap_ = heap_;
  else
    outHeap_ = context_->heap();

  context->setStatement(this);

  compStats_ = new (heap_) CompilationStats();

  optGlobals_ = new (heap_) OptGlobals(heap_);

  queryAnalysis_ = NULL;

  cqsWA_ = NULL;

  CostMethodHead_ = NULL;
  ItemExprOrigOpTypeBeingBound_ = NO_OPERATOR_TYPE;
  ItemExprOrigOpTypeCounter_ = 0;

  localizedTextBufSize_ = 0;
  localizedTextBuf_ = NULL;

  complexFSOTaskMonitor_ = new (heap_) TaskMonitor();
  // complexFSOMonPtr = complexFSOTaskMonitor_;

  simpleFSOTaskMonitor_ = new (heap_) TaskMonitor();
  // simpleFSOMonPtr = simpleFSOTaskMonitor_;
  parserStmtLiteralList_.setHeap(heap_);
}

CmpStatement::~CmpStatement() {
  // We have to delete this member since the destructor of storedProc_
  // handles the proper communication with stored procedure implementation
  // to end the interface.
  delete storedProc_;

  if (reply_ != NULL) reply_->decrRefCount();

  /*
    // At times, this delete can cause corruption in the heap
    // Hence, it is commented out for now - Selva
    // To miminze the leak from this the heap_ that used for this
    // objects comes from CmpStatement Heap in case of embedded arkcmp,
    // and from CmpContext Heap in case of standalone arkcmp.
    if (bound_ != NULL)
      bound_->decrRefCount();
  */

  // reset the empty input log props, otherwise the destructor
  // will try to destroy them after the statement heap has been
  // deleted
  emptyInLogProp_.reset();

  context_->unsetStatement(this, exceptionRaised_);

  delete heap_;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageEnvs &envMessage) {
  switch (envMessage.getOperator()) {
    case CmpMessageEnvs::EXGLOBALS: {
      envs()->setEnv(envMessage.envs(), envMessage.nEnvs());

      envs()->chdir(envMessage.cwd());
      // call CLI to set the transId
      long transid = (envMessage.activeTrans()) ? envMessage.transId() : long(-1);

      const char *env;

      env = getenv("SQLMX_REGRESS");
      if (env) {
        context_->setSqlmxRegress(atoi(env));

        // turn mode_special_1 OFF during regressions run.
        // Special1 features cause
        // many regressions to return mismatches due to special TD semantics.
        // When some
        // of the special1 features are externalized and enabled for general
        // NEO users, then we can remove these lines.
        NAString value("OFF");
        ActiveSchemaDB()->getDefaults().validateAndInsert("MODE_SPECIAL_1", value, FALSE);
      }

    } break;
    case CmpMessageEnvs::UNSETENV:
      envs()->unsetEnv(*(envMessage.envs()));
      break;
    default:
      break;
  }  // end of switch(env_message.operator())

  return CmpStatement_SUCCESS;
}

static NABoolean getCharsetsToUse(int msgCharSet, int &inputCS, int &defaultCS) {
  if (msgCharSet == SQLCHARSETCODE_ISO_MAPPING) {
    // return the value isoMapping set for this system.
    NAString cs;
    CmpCommon::getDefault(ISO_MAPPING, cs);
    inputCS = (int)CharInfo::getCharSetEnum(cs.data());
    defaultCS = (int)SQLCHARSETCODE_ISO88591;

    SetSqlParser_DEFAULT_CHARSET(CharInfo::ISO88591);
  } else {
    inputCS = msgCharSet;
    defaultCS = (int)SQLCHARSETCODE_UNKNOWN;

    // no change to default charset, if ISO_MAPPING was not specified.
    // Just set it to the same value as the original charset.
    SetSqlParser_DEFAULT_CHARSET(SqlParser_ORIG_DEFAULT_CHARSET);
  }

  return FALSE;
}

// extract and return the sql str for this request from data().
// Assign the recompLateNameInfoList, if passed in, to context.
// Make the cat & schema names current, if passed in.
static NABoolean processRecvdCmpCompileInfo(CmpStatement *cmpStmt, const CmpMessageRequest &msg,
                                            CmpCompileInfo *cmpInfo, CmpContext *context, char *&sqlStr, int &sqlStrLen,
                                            int &inputCS, NABoolean &catSchNameRecvd, NAString &currCatName,
                                            NAString &currSchName, NABoolean &nametypeNsk, NABoolean &odbcProcess,
                                            NABoolean &noTextCache, NABoolean &aqrPrepare, NABoolean &standaloneQuery,
                                            NABoolean &sentryPrivRecheck, NABoolean &doNotCachePlan) {
  char *catSchStr = NULL;
  cmpInfo->getUnpackedFields(sqlStr, catSchStr);
  sqlStrLen = cmpInfo->getSqlTextLen();

  // begin instrument to fix to mantis 9407
  if (cmpStmt) {
    CmpContext *cxt = cmpStmt->getCmpContext();

    if (cxt) {
      cxt->setLastSqlStmt(sqlStr, sqlStrLen);
    }
  }
  // end instrument to fix to mantis 9407

  // begin instrument fix to Mantis 9407
  // end instrument fix to Mantis 9407

  catSchNameRecvd = FALSE;
  nametypeNsk = FALSE;
  odbcProcess = FALSE;
  noTextCache = FALSE;
  aqrPrepare = FALSE;
  standaloneQuery = FALSE;
  sentryPrivRecheck = FALSE;
  doNotCachePlan = FALSE;

  if (!sqlStr) {
    if (cmpStmt) {
      cmpStmt->error(-CLI_EMPTY_SQL_STMT, "");
      sqlStrLen = 0;
      return TRUE;
    }
  }

  int defaultCS;
  getCharsetsToUse(msg.charSet(), inputCS, defaultCS);
  // assert(cmpInfo->sqlTextCharset_ == inputCS);
  cmpInfo->setSqlTextCharSet(inputCS);

  noTextCache = cmpInfo->noTextCache();
  aqrPrepare = cmpInfo->aqrPrepare();
  standaloneQuery = cmpInfo->standaloneQuery();
  sentryPrivRecheck = cmpInfo->sentryPrivRecheck();
  doNotCachePlan = cmpInfo->doNotCachePlan();

  if (catSchStr) {
    // The cat.sch name was received.  It may or may not need to be SET here,
    // but our caller will still need to RESET to the original/current values
    // (unless the caller can guarantee that the processed statement did not
    // alter the cat.sch).
    catSchNameRecvd = TRUE;

    // Save current values, for caller to restore later on.
    context->schemaDB_->getDefaults().getCatalogAndSchema(currCatName, currSchName);

    // make sure no unnecessary set schema is done
    // set schema will affect default_schema_nametype feature
    ComSchemaName currCatSchName(currCatName, currSchName);
    ComSchemaName catSchName(catSchStr);
    if (catSchName.getExternalName() != currCatSchName.getExternalName()) {
      // catSchStr is a string of form 'cat.sch',
      // *different* from the current cat.sch names.
      // Replace existing current cat and schema names with the names that
      // came with this request --
      // as NADefaults must parse the string,
      // here we must use setSchema(),
      // *NOT* setSchemaTrustedFast()!
      NAString catSch(catSchStr);
      context->schemaDB_->getDefaults().setSchema(catSch);
    }
  }

  odbcProcess = cmpInfo->odbcProcess();

  return FALSE;  // no error
}

NABoolean CmpStatement::isUserDMLQuery(char *query, int len) {
  const char *prefix[] = {"CONTROL QUERY DEFAULTS",
                          "CONTROL QUERY SHAPE",
                          "SET TRANSACTION",
                          "SET SESSION DEFAULT",
                          "CONTROL QUERY DEFAULT SESSION_ID",
                          "CONTROL QUERY DEFAULT SHOWCONTROL_SHOW_ALL",
                          "CONTROL QUERY DEFAULT * RESET RESET"};

  int prefixLen[] = {
      22,  // strlen("CONTROL QUERY DEFAULTS"
      19,  // strlen("CONTROL QUERY SHAPE"
      15,  // strlen("SET TRANSACTION"
      19,  // strlen("SET SESSION DEFAULT"
      32,  // strlen("CONTROL QUERY DEFAULT SESSION_ID"
      42,  // strlen("CONTROL QUERY DEFAULT SHOWCONTROL_SHOW_ALL"
      35   // strlen("CONTROL QUERY DEFAULT * RESET RESET")
  };

  for (int i = 0; i < sizeof(prefixLen) / sizeof(int); i++) {
    if (len > prefixLen[i] && !strncmp(query, prefix[i], prefixLen[i])) {
      return FALSE;
    }
  }

  return TRUE;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageSQLText &sqltext) {
  CmpMain cmpmain;

  CMPASSERT(sqltext.getCmpCompileInfo());

  char *sqlStr = NULL;
  int sqlStrLen = 0;
  int inputCS = 0;
  NAString currCatName;
  NAString currSchName;
  NABoolean isSchNameRecvd;
  NABoolean nametypeNsk;
  NABoolean odbcProcess;
  NABoolean noTextCache;
  NABoolean aqrPrepare;
  NABoolean standaloneQuery;
  NABoolean doNotCachePlan;
  NABoolean recompUseNATableCache = FALSE;
  if (processRecvdCmpCompileInfo(this, sqltext, sqltext.getCmpCompileInfo(), context_, sqlStr,
                                 sqlStrLen,  // out - long &
                                 inputCS, isSchNameRecvd, currCatName, currSchName, nametypeNsk, odbcProcess,
                                 noTextCache, aqrPrepare, standaloneQuery, sentryPrivRecheck_, doNotCachePlan))
    return CmpStatement_ERROR;

  recompUseNATableCache = sqltext.getCmpCompileInfo()->recompUseNATableCache();
  reply_ = new (outHeap_) CmpMessageReplyCode(outHeap_, sqltext.id(), 0, 0, outHeap_);

  if ((sqlStr) && inputCS == SQLCHARSETCODE_ISO88591 &&
      (strncmp(sqlStr, "select $ZZDEBUG", strlen("select $ZZDEBUG")) == 0)) {
    NADebug();
  }

  // QUERY_LOGGING == 1: DMLs
  if (CmpCommon::getDefaultLong(QUERY_LOGGING) == 1 &&
      CmpCommon::context()->getCIClass() != CmpContextInfo::CMPCONTEXT_TYPE_META && isUserDMLQuery(sqlStr, sqlStrLen)) {
    fstream &out = getPrintHandle();

    for (int i = 0; i < sqlStrLen; i++) out << sqlStr[i];

    out << endl << endl;
    out.close();
  }

  // If this is an ODBC query transmit inputArrayMaxsize and rowsetAtomicity information
  // (used for binding rowsets as input parameters) from CLI into cmpmain
  // so that it can be used during binding.
  if ((CmpCommon::getDefault(ODBC_PROCESS) == DF_ON) || (CmpCommon::getDefault(JDBC_PROCESS) == DF_ON)) {
    cmpmain.setInputArrayMaxsize(sqltext.getCmpCompileInfo()->getInputArrayMaxsize());
    cmpmain.setRowsetAtomicity(sqltext.getCmpCompileInfo()->getRowsetAtomicity());
  }
  cmpmain.setHoldableAttr(sqltext.getCmpCompileInfo()->getHoldableAttr());

  FragmentDir *fragmentDir = NULL;
  IpcMessageObjType typ = sqltext.getType();

  //
  // if this is a recompilation
  if (typ == CmpMessageObj::SQLTEXT_RECOMPILE) {
    recompiling_ = TRUE;
    if (recompUseNATableCache) recompUseNATableCache_ = TRUE;
  }

  sqlTextStr_ = sqlStr;
  sqlTextLen_ = sqlStrLen;
  sqlTextCharSet_ = inputCS;

  QueryText qText(sqlStr, inputCS);
  cmpmain.setSqlParserFlags(sqltext.getFlags());

  NABoolean qtcChanged = FALSE;
  if ((CmpCommon::getDefault(QUERY_TEXT_CACHE) == DF_SYSTEM) && (aqrPrepare || noTextCache)) {
    CMPASSERT(NOT(aqrPrepare && noTextCache));

    qtcChanged = TRUE;
    NAString op(((aqrPrepare && standaloneQuery) ? "SKIP" : "OFF"));
    context_->schemaDB_->getDefaults().validateAndInsert("QUERY_TEXT_CACHE", op, FALSE);
  }

  CmpMain::ReturnStatus rs = CmpMain::SUCCESS;
  try {
    rs = cmpmain.sqlcomp(qText, 0, &(reply_->data()), &(reply_->size()), reply_->outHeap(), CmpMain::END, &fragmentDir,
                         typ, doNotCachePlan ? CmpMain::EXPLAIN : CmpMain::NORMAL);
  } catch (...) {
    error(arkcmpErrorNoDiags, sqlStr);
    return CmpStatement_ERROR;
  }

  sqlTextStr_ = NULL;

  ((CmpMessageReplyCode *)reply_)->setFragmentDir(fragmentDir);

  // restore the original cat & schema names before returning.
  if (isSchNameRecvd) {
    context_->schemaDB_->getDefaults().setCatalogTrustedFast(currCatName);
    context_->schemaDB_->getDefaults().setSchemaTrustedFast(currSchName);
  }

  if (qtcChanged) {
    // restore the original query text cache setting
    NAString op("SYSTEM");
    context_->schemaDB_->getDefaults().validateAndInsert("QUERY_TEXT_CACHE", op, FALSE);
  }

  if (rs) {
    error(arkcmpErrorNoDiags, sqlStr);
    return CmpStatement_ERROR;
  }

  return CmpStatement_SUCCESS;
}

// static compilation
CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageCompileStmt &compilestmt) {
  CmpMain cmpmain;

  CMPASSERT(compilestmt.getCmpCompileInfo());

  char *sqlStr = NULL;
  int sqlStrLen = 0;
  int inputCS = 0;
  NAString currCatName;
  NAString currSchName;
  NABoolean isSchNameRecvd;
  NABoolean nametypeNsk;
  NABoolean odbcProcess;
  NABoolean noTextCache;
  NABoolean aqrPrepare;
  NABoolean standaloneQuery;
  NABoolean sentryPrivRecheck;
  NABoolean doNotCachePlan;
  if (processRecvdCmpCompileInfo(this, compilestmt, compilestmt.getCmpCompileInfo(), context_, sqlStr,
                                 sqlStrLen,  // out - long &
                                 inputCS, isSchNameRecvd, currCatName, currSchName, nametypeNsk, odbcProcess,
                                 noTextCache, aqrPrepare, standaloneQuery, sentryPrivRecheck, doNotCachePlan))
    return CmpStatement_ERROR;

  reply_ = new (outHeap_) CmpMessageReplyCode(outHeap_, compilestmt.id(), 0, 0, outHeap_);

  // A pointer to user SQL query is stored in CmpStatement; if an exception is
  // thrown the user query is copied from here. It is reset upon return from
  // sqlcompStatic() method.

  sqlTextStr_ = sqlStr;
  sqlTextLen_ = sqlStrLen;

  // set ODBC_PROCESS default.
  NABoolean odbcProcessChanged = FALSE;
  if (odbcProcess) {
    if (CmpCommon::getDefault(ODBC_PROCESS) != DF_ON) {
      odbcProcessChanged = TRUE;
      NAString op("ON");
      context_->schemaDB_->getDefaults().validateAndInsert("ODBC_PROCESS", op, FALSE);
    }
    if (CmpCommon::getDefault(JDBC_PROCESS) != DF_ON) {
      odbcProcessChanged = TRUE;
      NAString op("ON");
      context_->schemaDB_->getDefaults().validateAndInsert("JDBC_PROCESS", op, FALSE);
    }
  }

  QueryText qText(sqlStr, inputCS);
  cmpmain.setSqlParserFlags(compilestmt.getFlags());

  if (compilestmt.getCmpCompileInfo()->isSystemModuleStmt()) {
    CMPASSERT(FALSE);
  }

  int flags = 0;

  NABoolean qtcChanged = FALSE;
  if ((CmpCommon::getDefault(QUERY_TEXT_CACHE) == DF_SYSTEM) && (aqrPrepare || noTextCache)) {
    CMPASSERT(NOT(aqrPrepare && noTextCache));

    qtcChanged = TRUE;
    NAString op(((aqrPrepare && standaloneQuery) ? "SKIP" : "OFF"));
    context_->schemaDB_->getDefaults().validateAndInsert("QUERY_TEXT_CACHE", op, FALSE);
  }

  CmpMain::ReturnStatus rs = cmpmain.sqlcompStatic(qText, 0, &(reply_->data()), &(reply_->size()), reply_->outHeap(),
                                                   CmpMain::END, compilestmt.getType());
  sqlTextStr_ = NULL;

  // restore the original cat & schema names before returning.
  if (isSchNameRecvd) {
    context_->schemaDB_->getDefaults().setCatalogTrustedFast(currCatName);
    context_->schemaDB_->getDefaults().setSchemaTrustedFast(currSchName);
  }

  if (odbcProcessChanged) {
    // restore the original odbc process setting
    NAString op("OFF");
    context_->schemaDB_->getDefaults().validateAndInsert("ODBC_PROCESS", op, FALSE);
    context_->schemaDB_->getDefaults().validateAndInsert("JDBC_PROCESS", op, FALSE);
  }

  if (qtcChanged) {
    // restore the original query text cache setting
    NAString op("SYSTEM");
    context_->schemaDB_->getDefaults().validateAndInsert("QUERY_TEXT_CACHE", op, FALSE);
  }

  if (rs) {
    error(arkcmpErrorNoDiags, sqlStr);
    return CmpStatement_ERROR;
  }

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDDL &statement) {
  CmpMain cmpmain;

  CMPASSERT(statement.getCmpCompileInfo());

  char *sqlStr = NULL;
  int sqlStrLen = 0;
  int inputCS = 0;
  NAString currCatName;
  NAString currSchName;
  NABoolean isSchNameRecvd;
  NABoolean nametypeNsk;
  NABoolean odbcProcess;
  NABoolean noTextCache;
  NABoolean aqrPrepare;
  NABoolean standaloneQuery;
  NABoolean sentryPrivRecheck;
  NABoolean doNotCachePlan;
  isDDL_ = TRUE;

  if (processRecvdCmpCompileInfo(this, statement, statement.getCmpCompileInfo(), context_, sqlStr,
                                 sqlStrLen,  // out - long &
                                 inputCS, isSchNameRecvd, currCatName, currSchName, nametypeNsk, odbcProcess,
                                 noTextCache, aqrPrepare, standaloneQuery, sentryPrivRecheck, doNotCachePlan))
    return CmpStatement_ERROR;

  CmpCommon::context()->sqlSession()->setParentQid(statement.getParentQid());

  cmpmain.setSqlParserFlags(statement.getFlags());

  // set the current catalog and schema names.
  InitSchemaDB();

  // C control character embedded in sqlStr is not handled.  Now replace
  // control characters tabs, line feeds, spaces with spaces. (no longer
  // substitute for \n so we can recognized embedded comments)
  for (int i = 0; sqlStr[i]; i++)
    if (sqlStr[i] != '\n' && isSpace8859_1((unsigned char)sqlStr[i])) sqlStr[i] = ' ';

  // skip leading blanks
  NAString ns(sqlStr);
  ns = ns.strip(NAString::leading, ' ');

  // if this is an "update statistics..." request,
  // then do not send it catalog manager.
  int foundUpdStat = 0;

  // check if the first token is UPDATE
  size_t position = ns.index("UPDATE", 0, NAString::ignoreCase);
  if (position == 0) {
    // found UPDATE. See if the next token is STATISTICS.
    ns = ns(6, ns.length() - 6);  // skip over UPDATE
    ns = ns.strip(NAString::leading, ' ');

    position = ns.index("STATISTICS", 0, NAString::ignoreCase);
    if (position == 0) foundUpdStat = -1;
  }

  if (foundUpdStat) {
    char *userStr = new (heap()) char[2000];
    int len = strlen(sqlStr);

    if (len > 1999) len = 1999;

    strncpy(userStr, sqlStr, len);
    userStr[len] = '\0';

    sqlTextStr_ = userStr;
    sqlTextLen_ = len;

    sqlTextStr_ = NULL;
    sqlTextLen_ = 0;

    return CmpStatement_SUCCESS;
  }

  ReturnStatus status = CmpStatement_SUCCESS;
  if (statement.getCmpCompileInfo()->isHbaseDDL() || statement.getCmpCompileInfo()->isGenLoadQueryCache()) {
    CmpMain::ReturnStatus rs = CmpMain::SUCCESS;

    QueryText qText(sqlStr, inputCS);

    //      CmpMain cmpmain;
    Set_SqlParser_Flags(DELAYED_RESET);  // sqlcompCleanup resets for us
    Parser parser(CmpCommon::context());
    BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE);

    // save parser flags
    int savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
    ExprNode *exprNode = NULL;
    if (parser.parseDML(qText, &exprNode, NULL)) {
      error(arkcmpErrorNoDiags, statement.data());
      sqlTextStr_ = NULL;
      return CmpStatement_ERROR;
    }

    RelExpr *rRoot = NULL;
    if (exprNode->getOperatorType() EQU STM_QUERY) {
      rRoot = (RelRoot *)exprNode->getChild(0);
    } else if (exprNode->getOperatorType() EQU REL_ROOT) {
      rRoot = (RelRoot *)exprNode;
    }

    CMPASSERT(rRoot);

    ExprNode *boundDDL = rRoot->bindNode(&bindWA);
    CMPASSERT(boundDDL);

    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) {
      return CmpStatement_ERROR;
    }

    ExprNode *ddlNode = NULL;
    DDLExpr *ddlExpr = NULL;

    ddlExpr = (DDLExpr *)rRoot->getChild(0);
    ddlNode = ddlExpr->getDDLNode();

    if (ddlNode) {
      boundDDL = ddlNode->castToStmtDDLNode()->bindNode(&bindWA);
      CMPASSERT(boundDDL);

      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) {
        return CmpStatement_ERROR;
      }

      ddlNode = boundDDL;
    }

    // reset saved flags
    Set_SqlParser_Flags(savedParserFlags);
    CmpSeabaseDDL cmpSBD(heap_);
    cmpSBD.setDDLQuery(parser.inputStr());
    cmpSBD.setDDLLen(parser.inputStrLen());
    if (cmpSBD.executeSeabaseDDL(ddlExpr, ddlNode, currCatName, currSchName)) {
      Set_SqlParser_Flags(0);
      return CmpStatement_ERROR;
    }
    Set_SqlParser_Flags(0);

    return CmpStatement_SUCCESS;
  }  // hbaseDDL

  // This is a normal DDL request, call Catalog manager
  *diags() << DgSqlCode(-4222) << DgString0("SQL Compiler DDL");
  return CmpStatement_ERROR;
}

short CmpStatement::getDDLExprAndNode(char *sqlStr, int inputCS, DDLExpr *&ddlExpr, ExprNode *&ddlNode) {
  ddlNode = NULL;
  ddlExpr = NULL;

  if (!sqlStr) return 0;

  // C control character embedded in sqlStr is not handled.  Now replace
  // control characters tabs, line feeds, spaces with spaces. (no longer
  // substitute for \n so we can recognized embedded comments)
  for (int i = 0; sqlStr[i]; i++)
    if (sqlStr[i] != '\n' && isSpace8859_1((unsigned char)sqlStr[i])) sqlStr[i] = ' ';

  // skip leading blanks
  NAString ns(sqlStr);
  ns = ns.strip(NAString::leading, ' ');

  ReturnStatus status = CmpStatement_SUCCESS;
  CmpMain::ReturnStatus rs = CmpMain::SUCCESS;

  QueryText qText(sqlStr, inputCS);

  Set_SqlParser_Flags(DELAYED_RESET);  // sqlcompCleanup resets for us
  Parser parser(CmpCommon::context());
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE);

  ExprNode *boundDDL = NULL;
  RelExpr *rRoot = NULL;

  // save parser flags
  int savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
  ExprNode *exprNode = NULL;
  if (parser.parseDML(qText, &exprNode, NULL)) {
    error(arkcmpErrorNoDiags, sqlStr);
    sqlTextStr_ = NULL;
    goto label_error;
  }

  if (exprNode->getOperatorType() EQU STM_QUERY) {
    rRoot = (RelRoot *)exprNode->getChild(0);
  } else if (exprNode->getOperatorType() EQU REL_ROOT) {
    rRoot = (RelRoot *)exprNode;
  }

  CMPASSERT(rRoot);

  boundDDL = rRoot->bindNode(&bindWA);
  CMPASSERT(boundDDL);

  if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) {
    goto label_error;
  }

  ddlExpr = (DDLExpr *)rRoot->getChild(0);
  ddlNode = ddlExpr->getDDLNode();

  if (ddlNode) {
    boundDDL = ddlNode->castToStmtDDLNode()->bindNode(&bindWA);
    CMPASSERT(boundDDL);

    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) {
      goto label_error;
    }

    ddlNode = boundDDL;
  }

  Set_SqlParser_Flags(savedParserFlags);
  return 0;

label_error:
  // reset saved flags
  Set_SqlParser_Flags(savedParserFlags);
  return CmpStatement_ERROR;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDDLwithStatus &statement, NABoolean needToDoWork) {
  CmpMain cmpmain;

  CMPASSERT(statement.getCmpCompileInfo());

  char *sqlStr = NULL;
  int sqlStrLen = 0;
  int inputCS = 0;
  NAString currCatName;
  NAString currSchName;
  NABoolean isSchNameRecvd;
  NABoolean nametypeNsk;
  NABoolean odbcProcess;
  NABoolean noTextCache;
  NABoolean aqrPrepare;
  NABoolean standaloneQuery;
  NABoolean sentryPrivRecheck;
  NABoolean doNotCachePlan;
  isDDL_ = TRUE;

  CmpDDLwithStatusInfo *dws = statement.getCmpDDLwithStatusInfo();

  if (needToDoWork) {
    if (processRecvdCmpCompileInfo(NULL, statement, statement.getCmpCompileInfo(), context_, sqlStr,
                                   sqlStrLen,  // out - long &
                                   inputCS, isSchNameRecvd, currCatName, currSchName, nametypeNsk, odbcProcess,
                                   noTextCache, aqrPrepare, standaloneQuery, sentryPrivRecheck, doNotCachePlan))
      return CmpStatement_ERROR;
    CmpCommon::context()->sqlSession()->setParentQid(statement.getParentQid());

    cmpmain.setSqlParserFlags(statement.getFlags());

    // set the current catalog and schema names.
    InitSchemaDB();

    DDLExpr *ddlExpr = NULL;
    ExprNode *ddlNode = NULL;
    if (sqlStr) {
      if (getDDLExprAndNode(sqlStr, inputCS, ddlExpr, ddlNode)) {
        return CmpStatement_ERROR;
      }
    }

    if ((dws->getMDupgrade()) || (dws->getMDVersion()) || (dws->getSWVersion())) {
      CmpSeabaseMDupgrade cmpMDU(heap_);

      cmpMDU.disableAndClearSystemObjectCache();

      NABoolean ddlXns = (CmpCommon::getDefault(DDL_TRANSACTIONS) == DF_ON);
      if (cmpMDU.executeSeabaseMDupgrade(dws, ddlXns, currCatName, currSchName)) return CmpStatement_ERROR;

      cmpMDU.enableSystemObjectCache();
    } else if (dws->getMDcleanup() || dws->getInitTraf()) {
      CmpSeabaseDDL cmpSBD(heap_);
      cmpSBD.disableAndClearSystemObjectCache();
      if (cmpSBD.executeSeabaseDDL(ddlExpr, ddlNode, currCatName, currSchName, dws)) {
        Set_SqlParser_Flags(0);
        return CmpStatement_ERROR;
      }
      Set_SqlParser_Flags(0);
      cmpSBD.enableSystemObjectCache();
    } else {
      CmpSeabaseDDL cmpSBD(heap_);
      cmpSBD.setDDLQuery(sqlStr);
      cmpSBD.setDDLLen(sqlStrLen);
      if (cmpSBD.executeSeabaseDDL(ddlExpr, ddlNode, currCatName, currSchName, dws)) {
        Set_SqlParser_Flags(0);
        return CmpStatement_ERROR;
      }
      Set_SqlParser_Flags(0);
    }
  } else {
    // If no work is needed (e.g., for a resource group based tenant),
    // just mark it done in dws.
    if (dws) dws->setDone(TRUE);
  }
  /*
  CmpDDLwithStatusInfo * replyDWS = NULL;
  replyDWS = new(outHeap_) CmpDDLwithStatusInfo();
  replyDWS->copyStatusInfo(dws);
  */

  dws->init();

  int replyDataLen = dws->getLength();
  char *replyData = new (outHeap_) char[replyDataLen];
  dws->pack(replyData);

  CmpDDLwithStatusInfo *replyDWS = (CmpDDLwithStatusInfo *)replyData;

  reply_ = new (outHeap_) CmpMessageReplyCode(outHeap_, statement.id(), 0, 0, outHeap_);
  reply_->data() = replyData;
  reply_->size() = replyDataLen;

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDescribe &statement) {
  ReturnStatus ret = CmpStatement_SUCCESS;

  bound_ = new (outHeap_) CmpMessageReplyCode(outHeap_, statement.id(), 0, 0, outHeap_);
  reply_ = new (outHeap_) CmpMessageReplyCode(outHeap_, statement.id(), 0, 0, outHeap_);

  char *userStr = (char *)(heap())->allocateMemory(sizeof(char) * (2000));
  int len = strlen(statement.data());

  if (len > 1999) len = 1999;

  strncpy(userStr, statement.data(), len);
  userStr[len] = '\0';

  sqlTextStr_ = userStr;

  int inputCS;
  int defaultCS;
  getCharsetsToUse(statement.charSet(), inputCS, defaultCS);

  QueryText qText(statement.data(), inputCS);

  // Parse and bind the statement, getting query expr tree in bound->data;
  // pass this (casting to RelExpr, which it really is) to CmpDescribe
  CmpMain cmpmain;
  if (cmpmain.sqlcomp(qText, 0,                                             // IN
                      &bound_->data(), &bound_->size(), bound_->outHeap(),  // OUT
                      CmpMain::BIND)                                        // IN
      || CmpDescribe(statement.data(),                                      // IN
                     (RelExpr *)bound_->data(),                             // IN
                     reply_->data(), reply_->size(), reply_->outHeap()))    // OUT
  {
    error(arkcmpErrorNoDiags, statement.data());
    sqlTextStr_ = NULL;
    return CmpStatement_ERROR;
  }

  sqlTextStr_ = NULL;
  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageUpdateHist &statement) {
  char *userStr = new (heap()) char[2000];
  int len = strlen(statement.data());

  if (len > 1999) len = 1999;

  strncpy(userStr, statement.data(), len);
  userStr[len] = '\0';

  sqlTextStr_ = userStr;

  sqlTextStr_ = NULL;
  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageXnOper &statement) {
  // TransStmtType tst = (TransStmtType)(*(int*)statement.data());
  CmpMessageXnOperData *data = (CmpMessageXnOperData *)statement.data();
  TransStmtType tst = (TransStmtType)(data->type);
  long svptId = data->svptId;

  if (tst == BEGIN_) {
    CmpCommon::transMode()->setXnInProgress(TRUE);
    CmpCommon::transMode()->setSavepointInProgress(FALSE);
  } else if ((tst == COMMIT_) || (tst == ROLLBACK_)) {
    CmpCommon::transMode()->setXnInProgress(FALSE);
    CmpCommon::transMode()->setSavepointInProgress(FALSE);
  } else if ((tst == BEGIN_SAVEPOINT_) || (tst == ROLLBACK_SAVEPOINT_)) {
    // CmpCommon::transMode()->saveAutoCommitSavepointOption();
    // CmpCommon::transMode()->setAutoCommitSavepointOff();
    CmpCommon::transMode()->setSavepointInProgress(TRUE);
    CmpCommon::transMode()->setCurrSavepointId(svptId);
  } else if (tst == COMMIT_SAVEPOINT_ || tst == RELEASE_SAVEPOINT_) {
    // CmpCommon::transMode()->restoreAutoCommitSavepointOption();
    CmpCommon::transMode()->setSavepointInProgress(FALSE);
  }

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageSetTrans &statement) {
  CmpCommon::transMode()->updateTransMode((TransMode *)statement.data());
  GetCliGlobals()->updateTransMode((TransMode *)statement.data());
  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDDLNATableInvalidate &statement) {
  // TransStmtType tst = (TransStmtType)(*(int*)statement.data());
  CmpMessageXnOperData *data = (CmpMessageXnOperData *)statement.data();
  TransStmtType tst = (TransStmtType)(data->type);
  long svptId = data->svptId;

  int processSP = 0;
  /*
    there are two steps
          1. clear ddlObjsInSPList or ddlObjsList
          2. ddlInvalidateNATables, this can remove the natable cache

    if we commit SP. it means that we accept the ddl change, so we don't need remove the NATable cache.
    but for TX, we must remove the NATable cache both of local session and other user

    for SP
        if rollback SP, clear ddlObjsInSPList and do "ddlInvalidateNATables"
        if commit SP, move ddlObjsInSPList to ddlObjsList.
    for TX
        both rollback and commit TX, clear ddlObjsList and do "ddlInvalidateNATables"

    for ddllock, savepoint not handle it
  */
  NABoolean doAbort = (tst == ROLLBACK_WAITED_ || tst == ROLLBACK_);

  if (tst == ROLLBACK_SAVEPOINT_)
    processSP = 1;
  else if (tst == COMMIT_SAVEPOINT_)
    processSP = -1;

  NABoolean rmNATCache = (tst == ROLLBACK_SAVEPOINT_ || tst == ROLLBACK_WAITED_ || tst == ROLLBACK_ || tst == COMMIT_);
  CmpSeabaseDDL cmpSBD(heap_);
  CmpContext *cmpContext = CmpCommon::context();
  if (rmNATCache && cmpSBD.ddlInvalidateNATables(doAbort, processSP, svptId)) {
    if (processSP == 1) {
      for (int i = cmpContext->ddlObjsInSPList().entries(); i > 0; i--) {
        if (cmpContext->ddlObjsInSPList()[i - 1].getSvptId() >= svptId) cmpContext->ddlObjsInSPList().removeAt(i - 1);
      }
    } else if (processSP == 0) {
      cmpContext->ddlObjsList().clear();
      LOGTRACE(CAT_SQL_LOCK, "Release all DML/DDL object locks");
      cmpContext->releaseAllObjectLocks();
    }
    return CmpStatement_ERROR;
  } else if (processSP == -1) {
    // we can only get COMMIT_SAVEPOINT_ here, move the ddlObjs from ddlObjsInSPList to ddlObjsList
    for (int i = cmpContext->ddlObjsInSPList().entries(); i > 0; i--) {
      if (cmpContext->ddlObjsInSPList()[i - 1].getSvptId() <= svptId) {
        cmpContext->ddlObjsList().insertEntry(cmpContext->ddlObjsInSPList()[i - 1]);
        cmpContext->ddlObjsInSPList().removeAt(i - 1);
      }
    }
  }

  // TDB - how to handle errors?
  cmpSBD.ddlResetObjectEpochs(doAbort, TRUE /* clear the list */);

  if (NOT processSP) {
    LOGTRACE(CAT_SQL_LOCK, "Release all DML/DDL object locks");
    cmpContext->releaseAllObjectLocks();
  }
  return CmpStatement_SUCCESS;
}

// -----------------------------------------------------------------------------
// If a transaction performs DDL requests, the list of affected objects is
// stored.  During commit time, this list is traversed and applied to
// shared cache.  This is the message that commit logic sends to cause
// the shared cache updates.
// ----------------------------------------------------------------------------
CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDDLCommitSharedCache &statement) {
  CmpSeabaseDDL cmpSBD(heap_);
  if (cmpSBD.finalizeSharedCache()) {
    return CmpStatement_ERROR;
  }

  return CmpStatement_SUCCESS;
}

// -----------------------------------------------------------------------------
// If a transaction performs DDL requests, the list of affected objects is
// stored.  If a rollback occurs, then we throw away (clear) this list.
// This is the message that rollback logic sends to clear DDL operations list
// ----------------------------------------------------------------------------
CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDDLRollbackSharedCache &statement) {
  CmpContext *cmpContext = CmpCommon::context();
  cmpContext->sharedCacheDDLInfoList().clear();
  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageDatabaseUser &statement) {
  NABoolean doDebug = FALSE;

  // The message contains the following:
  //   (the fields are separated by commas)
  //     authorization state (0 - off, 1 - on)
  //     integer user ID
  //     database user name
  //     integer tenant ID
  //     tenant name
  //     tenant nodes (serialized form)
  //     tenant default schema
  //
  // TDB:  change this message structure into a class with methods that
  //       generate the message and later decomposes it into its pieces

  NAString message = statement.data();
  NAList<NAString> elements(heap_, 7);  // expect 7 entries
  message.split(',', elements /*out*/);

  CMPASSERT(elements.entries() == 7);

  NAString &authStateStr = elements[0];
  NABoolean authState = (authStateStr.data()[0] == '0') ? FALSE : TRUE;

  NAString &userIDStr = elements[1];
  int userID = atoi(userIDStr.data());

  NAString &userNameStr = elements[2];
  CMPASSERT(userNameStr.length() <= MAX_AUTHID_AS_STRING_LEN);
  const char *userName = userNameStr.data();

  NAString &tenantIDStr = elements[3];
  int tenantID = atoi(tenantIDStr.data());

  NAString &tenantNameStr = elements[4];
  CMPASSERT(tenantNameStr.length() <= MAX_AUTHID_AS_STRING_LEN);
  const char *tenantName = tenantNameStr.data();

  NAString &tenantNodesStr = elements[5];
  const char *tenantNodes = tenantNodesStr.data();

  NAString &tenantDefaultSchemaStr = elements[6];
  CMPASSERT(tenantNameStr.length() <= COL_MAX_SCHEMA_LEN);
  const char *tenantDefaultSchema = tenantDefaultSchemaStr.data();

  if (doDebug) {
    printf("[DBUSER:%d]   Received auth state %d\n", (int)getpid(), (int)authState);
    printf("[DBUSER:%d]   Received user ID %d\n", (int)getpid(), (int)userID);
    printf("[DBUSER:%d]   Received user name %s\n", (int)getpid(), userName);
    printf("[DBUSER:%d]   Received tenant ID %d\n", (int)getpid(), (int)tenantID);
    printf("[DBUSER:%d]   Received tenant name %s\n", (int)getpid(), tenantName);
    printf("[DBUSER:%d]   Received tenant nodes %s\n", (int)getpid(), tenantNodes);
    printf("[DBUSER:%d]   Received tenant default schema %s\n", (int)getpid(), tenantDefaultSchema);
  }

  CmpSqlSession *session = CmpCommon::context()->sqlSession();
  CMPASSERT(session);

  int sqlcode =
      session->setDatabaseUserAndTenant(userID, userName, tenantID, tenantName, tenantNodes, tenantDefaultSchema);
  if (doDebug) printf("[DBUSER:%d]   session->setDatabaseUser() returned %d\n", (int)getpid(), (int)sqlcode);
  if (sqlcode < 0) return CmpStatement_ERROR;

  CmpCommon::context()->setAuthorizationState((int)authState);
  CMPASSERT(GetCliGlobals()->currContext());
  GetCliGlobals()->currContext()->setAuthStateInCmpContexts(authState, authState);
  if (msg_license_multitenancy_enabled()) {
    NAString tdSch(tenantDefaultSchema);
    NAList<NAString> catsch(heap_, 2);
    if (tdSch.length() != 0) {
      tdSch.split('.', catsch);
      NAString &schStr = catsch[1];

      if (schStr.length() != 0) {
        CmpCommon::context()->getSchemaDB()->getDefaults().validateAndInsert("SCHEMA", schStr, FALSE);
      }
    }
  }

  // Security session attributes may need to be propagated to child arkcmp
  // processes. Call updateMxcmpSession found in cli/Context.cpp.
  // Also may want to do things like clear caches.
  sqlcode = GetCliGlobals()->currContext()->updateMxcmpSession();
  if (doDebug) printf("[DBUSER:%d]   ContextCli->updateMxcmpSession() returned %d\n", (int)getpid(), (int)sqlcode);

  if (sqlcode < 0) return CmpStatement_ERROR;

  if (doDebug) printf("[DBUSER:%d] END process(CmpMessageDatabaseUser)\n", (int)getpid());

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageEndSession &es) {
  if ((((CmpMessageEndSession &)es).cleanupEsps()) && (NOT context_->isSecondaryMxcmp())) {
    // For now, don't cleanup ESPs started by mxcmp. This is temp.
    // After some testing, we can uncomment the following line.
    // exeImmedOneStmt(NULL, "set session default sql_session 'END:CLEANUP_ESPS_ONLY';");
  }

  if (((CmpMessageEndSession &)es).resetAttrs()) {
    context_->schemaDB_->getDefaults().resetSessionOnlyDefaults();
  }

  if (((CmpMessageEndSession &)es).clearCache()) {
    CURRENTQCACHE->makeEmpty();
  }

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatement::process(const CmpMessageObj &request) {
  ReturnStatus ret = CmpStatement_SUCCESS;
  // For the requests with the following message type the parent qid may not be passed
  // CmpMessageDescribe
  // CmpMessageUpdateHist
  // CmpMessageSetTrans
  // CmpMessageEndSession
  // Reset the parent qid and the requests that has parent qid will set it later
  CmpCommon::context()->sqlSession()->setParentQid(NULL);

  switch (request.getType()) {
    case CmpMessageObj::SQLTEXT_COMPILE:
      ret = process(*(CmpMessageSQLText *)(&request));
      break;

    case CmpMessageObj::SQLTEXT_STATIC_COMPILE:
      ret = process(*(CmpMessageCompileStmt *)(&request));
      break;

    case (CmpMessageObj::EXIT_CONNECTION):
      break;

    case (CmpMessageObj::ENVS_REFRESH):
      ret = process(*(CmpMessageEnvs *)(&request));
      break;

    case (CmpMessageObj::DDL):
      ret = process(*(CmpMessageDDL *)(&request));
      break;

    case (CmpMessageObj::DESCRIBE):
      ret = process(*(CmpMessageDescribe *)(&request));
      break;

    case (CmpMessageObj::UPDATE_HIST_STAT):
      ret = process(*(CmpMessageUpdateHist *)(&request));
      break;

    case (CmpMessageObj::SET_TRANS):
      ret = process(*(CmpMessageSetTrans *)(&request));
      break;

    case (CmpMessageObj::XN_OPER):
      ret = process(*(CmpMessageXnOper *)(&request));
      break;

    case (CmpMessageObj::DDL_NATABLE_INVALIDATE):
      ret = process(*(CmpMessageDDLNATableInvalidate *)(&request));
      break;

    case (CmpMessageObj::DDL_ROLLBACK_SHARED_CACHE):
      ret = process(*(CmpMessageDDLRollbackSharedCache *)(&request));
      break;

    case (CmpMessageObj::DDL_COMMIT_SHARED_CACHE):
      ret = process(*(CmpMessageDDLCommitSharedCache *)(&request));
      break;

    case (CmpMessageObj::DATABASE_USER):
      ret = process(*(CmpMessageDatabaseUser *)(&request));
      break;

    case (CmpMessageObj::DDL_WITH_STATUS):
      ret = process(*(CmpMessageDDLwithStatus *)(&request));
      break;

    case (CmpMessageObj::INTERNALSP_REQUEST):
      ret = ((CmpStatementISP *)(this))->process(*(CmpMessageISPRequest *)(&request));
      break;

    case (CmpMessageObj::INTERNALSP_GETNEXT):
      ret = ((CmpStatementISP *)(this))->process(*(CmpMessageISPGetNext *)(&request));
      break;

    case (CmpMessageObj::END_SESSION):
      ret = process(*(CmpMessageEndSession *)(&request));
      break;

    default:
      break;
  }
  return ret;
}

CmpStoredProc *CmpStatement::setStoredProc(CmpStoredProc *p) {
  // if there is one already, delete it
  // This assumes that there will be no nested CmpStoredProc situation.
  if (storedProc_) delete storedProc_;
  storedProc_ = p;
  return p;
}

void CmpStatement::exceptionRaised() {
  // reset the SP, when exception happens, the SP needs to be closed appropriately
  // for either compile time or execution time. The destructor of CmpStoredProc
  // will handle the exit calls appropriately.
  setStoredProc(0);
  exceptionRaised_ = TRUE;
}

// -----------------------------------------------------------------------
// implementation of CmpStatementISP class
// -----------------------------------------------------------------------

CmpStatementISP::CmpStatementISP(CmpContext *context, CollHeap *outHeap) : CmpStatement(context, outHeap) {
  ISPReqId_ = 0;
}

CmpStatementISP::~CmpStatementISP() {
  reply_->takeData();  // this was previously deleted in CmpSPExecDataItemOutput
}

NABoolean CmpStatementISP::moreData() {
  // check the CmpISPDataObject to see whether there is more data to be fetched.

  CmpISPDataObject *ispData;
  return (storedProc_ && (ispData = ((CmpInternalSP *)storedProc_)->ispData()) && ispData->moreData());
}

// helper routines to fetch data from isp and put a row of data into ispData
// return TRUE is buffer is full, FALSE otherwise

static NABoolean ISPFetchPut(CmpInternalSP *storedProc,  // to fetch data
                             CmpISPDataObject *ispData)  // to put data
{
  NABoolean bufferFull = FALSE;
  // fetch until there is no more data
  CmpStoredProc::ExecStatus execStatus;
  short putStatus;
  while (!bufferFull && (execStatus = storedProc->fetch(*ispData)) == CmpStoredProc::MOREDATA) {
    if ((putStatus = ispData->output()->AddARow()) == 1) bufferFull = TRUE;
    CMPASSERT(putStatus != -1);
  }
  // close the ISP
  if (!bufferFull) {
    storedProc->close(*ispData);
    if ((putStatus = ispData->output()->AddEOR()) == 1) bufferFull = TRUE;
    CMPASSERT(putStatus != -1);
  }
  return bufferFull;
}

static NABoolean ISPPrepareReply(CmpISPDataObject *ispData, CmpMessageReply *reply, NABoolean moreData) {
  CMPASSERT(ispData->output()->prepare());
  ((CmpMessageReplyISP *)reply)->setAreMore(moreData);
  reply->data() = (char *)(ispData->output()->data());
  reply->size() = ispData->output()->dataSize();
  return TRUE;
}

CmpStatement::ReturnStatus CmpStatementISP::process(CmpMessageISPRequest &isp) {
  ReturnStatus ret = CmpStatement_ERROR;

#ifdef _DEBUG
  if (getenv("DEBUG_SP2")) DebugBreak();
#endif
  CmpCommon::context()->sqlSession()->setParentQid(isp.getParentQid());

  // Instantiate a CmpInternalSP
  CmpInternalSP *storedProc = new (heap_) CmpInternalSP(isp.procName(), context_);
  CMPASSERT(storedProc);
  setStoredProc(storedProc);
  reply_ = new (outHeap_) CmpMessageReplyISP(outHeap_, isp.id(), 0, 0, outHeap_);

  // prepare the data for execution
  // Make sure the pointer that ispData owns won't be deleted until ispData is
  // out of scope. Because of the performance reason, the pointers are copied,
  // not the contents.
  // The procedure flow is :
  // .CmpContext contains CmpStatements
  // .one CmpStatementISP is created per CmpMessageISPRequest, there might be many CmpMessageGetNext to fetch more data,
  // but they all share the same CmpStatementISP. This CmpStatementISP will be deleted when the execution of ISP is
  // finished. .CmpStatementISP owns a CmpInternalSP, the interface to stored procedure execution. CmpInternalSP will be
  // deleted in CmpStatement::~CmpStatement . CmpInternalSP owns a CmpISPDataObject which contains data passed from
  // executor for ISP execution. this CmpISPDataObject will only be deleted when CmpInternalSP is out of scope.
  // .CmpISPDataObject is constructed from the CmpMessageISPRequest, for better performance
  // the data pointers are copied instead of duplicating the data, so it should own the
  // data member and only delete them when CmpISPDataObject is out of scope.

  // storedProc_ owns this ispData, it should be deleted in storedProc is out of scope.
  CmpISPDataObject *ispData = new (heap_) CmpISPDataObject(&isp, storedProc, context_->heap(), context_);
  ISPReqId_ = isp.id();

  // open ISP
  short inputStatus = 0;
  NABoolean bufferFull = FALSE;
  for (; !bufferFull && (inputStatus = ispData->input()->next()) == 0;) {
    if (storedProc->open(*ispData) == CmpStoredProc::SUCCESS)
      bufferFull = ISPFetchPut(storedProc, ispData);
    else {
      if (ispData->output()->AddEOR() == 1) bufferFull = TRUE;
    }
  }

  CMPASSERT(inputStatus != -1);  // fail for retrieving input data

  // prepare to send the data back to executor
  ISPPrepareReply(ispData, reply_, bufferFull);

  return CmpStatement_SUCCESS;
}

CmpStatement::ReturnStatus CmpStatementISP::process(const CmpMessageISPGetNext &getNext) {
  // This routine is to process the getNext request

  // 1. It first allocate the output data with size specified.
  // 2. it then fetched the remaining data from previous ISP execution.
  // 3. continue open/fetch/close for ISP execution.

  CmpCommon::context()->sqlSession()->setParentQid(getNext.getParentQid());
  CmpInternalSP &internalSP = *((CmpInternalSP *)storedProc_);
  CmpISPDataObject &ispData = *(CmpISPDataObject *)(internalSP.ispData());
  ispData.output()->allocateData(getNext.bufSize());

  NABoolean bufferFull = FALSE;
  short putStatus;
  if (ispData.output()->rowExist()) {
    if ((putStatus = ispData.output()->AddARow()) == 1) bufferFull = TRUE;
    CMPASSERT(putStatus != -1);
    if (!bufferFull) bufferFull = ISPFetchPut(&internalSP, &ispData);
  }

  else if (ispData.output()->EORExist()) {
    if ((putStatus = ispData.output()->AddEOR()) == 1) bufferFull = TRUE;
    CMPASSERT(putStatus != -1);
  }

  // open ISP again for remaining input.
  short inputStatus = 0;
  for (; !bufferFull && (inputStatus = ispData.input()->next()) == 0;) {
    if (internalSP.open(ispData) == CmpStoredProc::SUCCESS)
      bufferFull = ISPFetchPut(&internalSP, &ispData);
    else {
      if (ispData.output()->AddEOR() == 1) bufferFull = TRUE;
    }
  }

  CMPASSERT(inputStatus != -1);  // fail for retrieving input data

  ISPPrepareReply(&ispData, reply_, bufferFull);
  return CmpStatement_SUCCESS;
}

NABoolean CmpStatementISP::readyToDie() {
  if (exceptionRaised_ || !moreData())
    return TRUE;
  else
    return FALSE;
}

QueryAnalysis *CmpStatement::initQueryAnalysis() {
  NABoolean analysis = (CmpCommon::getDefault(QUERY_ANALYSIS) == DF_ON);
  queryAnalysis_ = new (CmpCommon::statementHeap()) QueryAnalysis(CmpCommon::statementHeap(), analysis);
  // do any necessary initialization work here (unless this
  // initialization work fits in the constructor)

  // Initialize the global "empty input logprop"
  if (emptyInLogProp_ == NULL)
    emptyInLogProp_ = EstLogPropSharedPtr(
        new (STMTHEAP) EstLogProp(1, NULL, EstLogProp::NOT_SEMI_TSJ, new (STMTHEAP) CANodeIdSet(STMTHEAP), TRUE));

  //++MV
  // This input cardinality is not estimated , so we keep this knowledge
  // in a special attribute.
  (*GLOBAL_EMPTY_INPUT_LOGPROP)->setCardinalityEqOne();

#ifdef _DEBUG
  NABoolean debug_code = TRUE;
#else
  NABoolean debug_code = FALSE;
#endif

  CompCCAssert::setUseCCMPAssert(CmpCommon::getDefault(USE_CCMPASSERT_AS_CMPASSERT) == DF_ON || debug_code);

  return queryAnalysis_;
}

void CmpStatement::prepareForCompilationRetry() {
  // The compiler may retry compiling a statement several times,
  // sharing the same CmpStatement object. Initialize any data
  // structures that need it here.
  numOfCompilationRetries_++;

  if (cses_) cses_->clear();
  if (detailsOnRefusedRequirements_) detailsOnRefusedRequirements_->clear();
}

void CmpStatement::initCqsWA() { cqsWA_ = new (heap_) CqsWA(); }

void CmpStatement::clearCqsWA() { cqsWA_ = NULL; }

void CmpStatement::setTMUDFRefusedRequirements(const char *details) {
  if (!detailsOnRefusedRequirements_) {
    detailsOnRefusedRequirements_ = new (heap_) LIST(const NAString *)(heap_);
  } else {
    // check whether this string already has been recorded
    for (CollIndex i = 0; i < detailsOnRefusedRequirements_->entries(); i++)
      if (*((*detailsOnRefusedRequirements_)[i]) == details) return;
  }

  detailsOnRefusedRequirements_->insert(new (heap_) NAString(details, heap_));
}

CSEInfo *CmpStatement::getCSEInfo(const char *cseName) const {
  if (cses_)
    for (CollIndex i = 0; i < cses_->entries(); i++) {
      if ((*cses_)[i]->getName() == cseName) return (*cses_)[i];
    }

  // no match found
  return NULL;
}

CSEInfo *CmpStatement::getCSEInfoForMainQuery() const {
  // the first entry is reserved for the main query
  return getCSEInfoById(getCSEIdForMainQuery());
}

CSEInfo *CmpStatement::getCSEInfoById(int cseId) const {
  DCMPASSERT(cses_);
  CSEInfo *result = (*cses_)[cseId];

  CMPASSERT(result->getCSEId() == cseId);

  return result;
}

void CmpStatement::addCSEInfo(CSEInfo *info) {
  if (cses_ == NULL) {
    cses_ = new (CmpCommon::statementHeap()) LIST(CSEInfo *)(CmpCommon::statementHeap());

    // add an entry for the main query, so we can
    // record the CSE references of the main query
    DCMPASSERT(cses_->entries() == getCSEIdForMainQuery());
    addCSEInfo(new (CmpCommon::statementHeap()) CSEInfo("", CmpCommon::statementHeap()));
  }

  info->setCSEId(cses_->entries());
  cses_->insert(info);
}
