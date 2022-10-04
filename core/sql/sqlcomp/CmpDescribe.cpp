

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/Platform.h"

#include <ctype.h>
#include <iostream>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"  // must be the last #include.

#include "common/CmpCommon.h"
#include "arkcmp/CmpContext.h"
#include "sqlcomp/CmpMain.h"
#include "arkcmp/CmpErrors.h"
#include "export/ComDiags.h"
#include "common/ComObjectName.h"
#include "common/ComOperators.h"
#include "common/ComSpace.h"
#include "comexe/ComTdbRoot.h"
#include "common/ComSmallDefs.h"
#include "common/ComUser.h"
#include "optimizer/ControlDB.h"
#include "common/DatetimeType.h"
#include "comexe/FragDir.h"
#include "export/HeapLog.h"
#include "sqlcomp/parser.h"
#include "optimizer/RelControl.h"
#include "optimizer/RelExpr.h"
#include "optimizer/RelExeUtil.h"
#include "optimizer/RelMisc.h"
#include "optimizer/RelRoutine.h"
#include "optimizer/RelScan.h"
#include "cli/sql_id.h"
#include "optimizer/Sqlcomp.h"
#include "optimizer/ItemOther.h"
#include "optimizer/NARoutine.h"
#include "langman/LmJavaSignature.h"
#include "common/QueryText.h"
#include "common/wstr.h"
#include "parser/SqlParserGlobals.h"
#include "common/csconvert.h"
#include "common/charinfo.h"
#include "sqlcomp/CmpDescribe.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"

#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseDDLauth.h"
#include "sqlcomp/CmpSeabaseTenant.h"

#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"

#include "optimizer/Analyzer.h"
#include "common/ComSqlId.h"
#include "executor/ExExeUtilCli.h"
#include "executor/HBaseClient_JNI.h"
#include "exp/ExpHbaseInterface.h"
#include "sqlcat/TrafDDLdesc.h"
#include "seabed/ms.h"
#include "exp/ExpSeqGen.h"

#define CM_SIM_NAME_LEN 32

enum PrivTarget { PRIV_SCHEMA = 2, PRIV_TABLE, PRIV_PROCEDURE, PRIV_LIBRARY };

enum Display_Schema_Object {
  SCHEMA_TABLE,
  SCHEMA_TABLE_INDEX,
  SCHEMA_TABLE_VIEW,
  SCHEMA_TABLE_MV,
  SCHEMA_TABLE_TRIGGER,
  SCHEMA_TABLE_SYNONYM,
  SCHEMA_TABLE_STORED_PROCEDURE,
  SCHEMA_TABLE_TABLE_MAPPING_FUNCTION,
  SCHEMA_TABLE_SCALAR_FUNCTION,
  SCHEMA_TABLE_UNIVERSAL_FUNCTION,
  SCHEMA_TABLE_ROUTINE_ACTION
};

extern "C" {
int SQLCLI_GetRootTdb_Internal(/*IN*/ CliGlobals *cliGlobals,
                                 /*INOUT*/ char *roottdb_ptr,
                                 /*IN*/ Int32 roottdb_len,
                                 /*INOUT*/ char *srcstr_ptr,
                                 /*IN*/ Int32 srcstr_len,
                                 /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_GetRootTdbSize_Internal(/*IN*/ CliGlobals *cliGlobals,
                                     /*INOUT*/ int *root_tdb_size,
                                     /*INOUT*/ int *srcstr_size,
                                     /*IN*/ SQLSTMT_ID *statement_id);
}

extern CliGlobals *GetCliGlobals();

void outputLine(Space &space, NAString &outputText, size_t indent, size_t indentDeltaOfLineFollowingLineBreak = 2,
                NABoolean commentOut = FALSE, const char *testSchema = NULL);

void outputLine(Space &space, const char *buf, size_t indent, size_t indentDeltaOfLineFollowingLineBreak = 2,
                NABoolean commentOut = FALSE, const char *testSchema = NULL);

static size_t indexLastNewline(const NAString &text, size_t startPos, size_t maxLen);

static bool CmpDescribeLibrary(const CorrName &corrName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap);

static short CmpDescribePackage(const CorrName &corrName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap);

static short CmpDescribeRoutine(const CorrName &corrName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap);

static short CmpDescribeTrigger(const CorrName &corrName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap);

static short CmpDescribePlan(const char *query, ULng32 flags, char *&outbuf, ULng32 &outbuflen, NAMemory *heap);

static short CmpDescribeShape(const char *query, char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeTransaction(char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeUUID(Describe *d, char *&outbuf, ULng32 &outbuflen, CollHeap *heap);

short CmpDescribeSequence(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap, Space *inSpace,
                          NABoolean sqlCommentOut = FALSE);

bool CmpDescribeIsAuthorized(CollHeap *heap = NULL, SQLOperation operation = SQLOperation::UNKNOWN,
                             PrivMgrUserPrivs *privs = NULL, ComObjectType objectType = COM_UNKNOWN_OBJECT,
                             Int32 objectOwner = NA_UserIdDefault);

static short CmpDescribeTableHDFSCache(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeSchemaHDFSCache(const NAString &schemaText, char *&outbuf, ULng32 &outbuflen, NAMemory *heap);

static short CmpDescribeShowViews(char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeDescView(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeShowEnv(char *&outbuf, ULng32 &outbuflen, NAMemory *h);

static short CmpDescribeTablePartitions(NATable *naTable, Space &space, char *buf, short type);

static short CmpDescribePartitions(Int32 partType, NAPartition *naPartition, Space &space, char *buf,
                                   NABoolean isFirstPartition, NABoolean isFirst);

// This method is to describe the normal index information of the partition entity table
static short CmpDescribePartIndexDesc(BindWA *bindWA, short type, NATable *naTable, Space &space, char *buf,
                                      const CorrName &dtName, const CorrName *likeTabName);

short cmpDisplayPrimaryKey(const NAColumnArray &naColArr, int numKeys, NABoolean displaySystemCols, Space &space,
                           char *buf, NABoolean displayCompact, NABoolean displayAscDesc, NABoolean displayParens);

// ## This only applies to CatSim; remove it!
#define CONSTRAINT_IS_NAMED(constrdesc) \
  (constrdesc->constrnts_desc.constrntname && *constrdesc->constrnts_desc.constrntname)

// Define a shorter synonym.
#define SpacePrefix CmpDescribeSpaceCountPrefix

static short CmpDescribePlan(const char *query, ULng32 flags, char *&outbuf, ULng32 &outbuflen, NAMemory *h);

// DZC - outputToStream does nothing for SHOWDDL because outStream() returns NULL
// needed for SHOWCONTROL though, so they must be left in.
static void outputToStream(const char *buf, NABoolean commentOut = FALSE) {
  if (ActiveControlDB()->outStream()) *(ActiveControlDB()->outStream()) << (commentOut ? "--" : "") << buf << endl;
}

/*
static void outputToStreamNoNewline(const char *buf, NABoolean commentOut = FALSE)
{
  if (ActiveControlDB()->outStream())
    *(ActiveControlDB()->outStream()) << (commentOut ? "--" : "") << buf;
}
*/

void outputShortLine(Space &space, const char *buf, NABoolean commentOut = FALSE) {
  // DZC - if commentOut = TRUE, prepend buf with "--"
  NAString outputString(buf);
  if (commentOut) outputString.prepend("--");
  space.outputbuf_ = TRUE;
  space.allocateAndCopyToAlignedSpace(outputString, outputString.length(), SpacePrefix);
  outputToStream(buf ? buf : "", commentOut);  // DZC - this does nothing
}

void getPartitionIndexName(char *buf, int bufLen, const NAString &baseIndexName, const char *partitionTableName) {
  if (buf != NULL && bufLen > 0) {
    snprintf(buf, bufLen, "INDEX_%s_%s", baseIndexName.data(), partitionTableName);
  }
}

void outputColumnLine(Space &space, NAString pretty, Int32 &colcount, NABoolean isForMV = FALSE,
                      NABoolean isDivisionColumn = FALSE) {
  // Must linebreak before prepending comma+space, else long colname won't
  // display correctly.
  if (isDivisionColumn)
    LineBreakSqlText(pretty, FALSE /*showddlView*/, 79 /*maxlen*/, 6 /*pfxlen*/, 4 /*pfxinitlen*/, ' ' /*pfxchar*/,
                     NULL /*testSchema*/, isDivisionColumn /*commentOut*/);
  else
    LineBreakSqlText(pretty, FALSE, 79, 6, 4);  // initial indent 4, subseq 6
  if (colcount++)                               // if not first line,
    pretty[(size_t)2] = ',';                    // change "    " to "  , "
  space.outputbuf_ = TRUE;
  space.allocateAndCopyToAlignedSpace(pretty, pretty.length(), SpacePrefix);
  if (isDivisionColumn) {
    // Do not prepend the leading "--" comment prefix because we have
    // already done that in the LineBreakSqlText() call.
    outputToStream(pretty, FALSE /*commentOut*/);
  } else
    outputToStream(pretty, isForMV);
}

void outputLine(Space &space, const char *buf, size_t indent, size_t indentDeltaOfLineFollowingLineBreak,
                NABoolean commentOut, const char *testSchema) {
  NAString pretty(buf);
  outputLine(space, pretty, indent, indentDeltaOfLineFollowingLineBreak, commentOut, testSchema);
}

// NAstring version of outputLine so there are fewer conversions
void outputLine(Space &space, NAString &outputText, size_t indent, size_t indentDeltaOfLineFollowingLineBreak,
                NABoolean commentOut, const char *testSchema) {
  if (commentOut) outputText.prepend("--");

  LineBreakSqlText(outputText, FALSE, 79, indent + indentDeltaOfLineFollowingLineBreak, 0, ' ', testSchema);
  space.outputbuf_ = TRUE;
  space.allocateAndCopyToAlignedSpace(outputText, outputText.length(), SpacePrefix);
  outputToStream(outputText, commentOut);
}

// To output lines with more than 3000 characters
void outputLongLine(Space &space, NAString &outputText, size_t indent, NABoolean lineBreakSqlText = TRUE,
                    const char *testSchema = NULL, NABoolean commentOut = FALSE) {
  if (lineBreakSqlText) {
    if (commentOut)
      LineBreakSqlText(outputText, TRUE, 79, indent + 2, 0, ' ', testSchema, commentOut);
    else
      LineBreakSqlText(outputText, TRUE, 79, indent + 2, 0, ' ', testSchema);
  }

  // Can only print out 3000 characters at a time due to generator limitation
  // This limitation only applies to views because view DDL is printed out as
  // a continuous string of text (vs. other DDL which is outputted piecemeal).
  // This code assumes that there must be at least one space in every 3000
  // chars of text.

  const size_t CHUNK_SIZE = 3000;
  size_t textLen = outputText.length();
  space.outputbuf_ = TRUE;

  // If there's only one chunk, print it
  if (textLen <= CHUNK_SIZE) {
    space.allocateAndCopyToAlignedSpace(outputText, textLen, SpacePrefix);
  } else {
    // Output multiple chunks
    size_t textStartPos = 0;
    size_t finalTextStartPos = textLen - CHUNK_SIZE;
    size_t chunkLen = 0;
    NABoolean isFinalChunk = FALSE;
    do {
      isFinalChunk = (textStartPos >= finalTextStartPos);
      // Final chunk may be smaller than CHUNK_SIZE
      chunkLen = (isFinalChunk) ? (textLen - textStartPos) : CHUNK_SIZE;
      if (!isFinalChunk) {
        // Use the last newline in this chunk as the boundary between this
        // chunk and the next chunk.
        chunkLen = indexLastNewline(outputText, textStartPos, chunkLen);
        CMPASSERT(chunkLen != NA_NPOS);
      }
      space.allocateAndCopyToAlignedSpace(outputText(textStartPos, chunkLen).data(), chunkLen, SpacePrefix);
      textStartPos += chunkLen + 1;  // + 1 skips over newline
    } while (!isFinalChunk);
  }
}

// Return index of closing right quote that matches the left quote
// text(startPos).  Return NA_NPOS if closing right quote is not found.
// Searches text(startPos+1) through text(endPos-1).
static size_t skipQuotedText(const NAString &text, size_t startPos, size_t endPos) {
  size_t closeQuotePos = NA_NPOS;

  char quoteChar = text(startPos);

  for (size_t pos = startPos + 1; pos < endPos; pos++) {
    char c = text(pos);
    if (c == quoteChar) {
      if ((pos != (endPos - 1)) && (text(pos + 1) == quoteChar))
        pos++;  // skip embedded quote
      else {
        closeQuotePos = pos;
        break;
      }
    }
  }

  return closeQuotePos;
}

// Return the offset from startPos of the last unquoted newline that occurs in
// the substring text(startPos, maxLen).  Return NA_NPOS if the substring
// contains fewer than maxLen characters (and therefore doesn't need to be
// broken up) or we can't find an unquoted newline.
//
static size_t indexLastNewline(const NAString &text, size_t startPos, size_t maxLen) {
  size_t newlinePos = NA_NPOS;

  size_t endPos = startPos + maxLen;
  if (text.length() <= endPos) return NA_NPOS;

  // search text(startPos) through text(endPos - 1)
  for (size_t pos = startPos; pos < endPos; pos++) {
    char c = text[pos];
    if ((c == '"') OR(c == '\'')) {
      pos = skipQuotedText(text, pos, endPos);
      // If a quoted string was split across the chunk, we won't find the
      // right-hand quote; break out of loop and return the offset of last
      // found (if any) newline character.
      if (pos == NA_NPOS) break;
    } else if (c == '\n') {
      newlinePos = pos;
    }
  }

  // Convert newlinePos to offset from startPos
  if (newlinePos != NA_NPOS) newlinePos = (newlinePos - startPos);

  return newlinePos;
}

static Int32 displayDefaultValue(const char *defVal, const char *colName, NAString &displayableDefVal) {
  displayableDefVal.append(defVal);
  return 0;
}

static short CmpDescribeShowQryStats(const char *query, char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  // make sure we have a showstats query to process
  short rc = -1;
  if (!query) return rc;

  // Skip leading blanks
  const char *cc = query;
  while (isSpace8859_1(*cc)) cc++;

  // scan "showstats" token
  char lowertok[10];
  const char *ctok = cc;
  while (isAlpha8859_1(*cc)) cc++;
  strncpy(lowertok, ctok, 9);
  lowertok[9] = '\0';
  if (stricmp(lowertok, "showstats")) return rc;

  // skip blanks
  while (isSpace8859_1(*cc)) cc++;

  // scan "for" token
  ctok = cc;
  while (isAlpha8859_1(*cc)) cc++;
  strncpy(lowertok, ctok, 3);
  lowertok[3] = '\0';
  if (stricmp(lowertok, "for")) return rc;

  // skip blanks
  while (isSpace8859_1(*cc)) cc++;

  // scan "query" token
  ctok = cc;
  while (isAlpha8859_1(*cc)) cc++;
  strncpy(lowertok, ctok, 5);
  lowertok[5] = '\0';
  if (stricmp(lowertok, "query")) return rc;

  // skip blanks
  while (isSpace8859_1(*cc)) cc++;

  // a "showstats for query <q>" does its work by:
  //   1) at start, CmpCommon::context()->setShowQueryStats()
  //   2) sqlcomp -- analyzer & cardinality estimation code will consult
  //      CmpCommon::context()->showQueryStats() to do their part in
  //      showing internal histogram stuff
  //   3) at end, CmpCommon::context()->resetShowQueryStats()
  CmpCommon::context()->setShowQueryStats();

  // prepare this query.
  char *qTree = NULL;
  ULng32 dummyLen;

  CmpMain sqlcomp;
  CmpMain::CompilerPhase phase = CmpMain::ANALYSIS;

  // "showstats for query <q>" will cause QueryAnalysis::analyzeThis() to
  // call QueryAnalysis::setHistogramsToDisplay() which will set
  // QueryAnalysis::Instance()->statsToDisplay_ which will be displayed
  // by QueryAnalysis::Instance()->showQueryStats() below.
  QueryText qText((char *)cc, SQLCHARSETCODE_ISO88591);
  CmpMain::ReturnStatus rs = sqlcomp.sqlcomp(qText, 0, &qTree, &dummyLen, heap, phase);
  CmpCommon::context()->resetShowQueryStats();
  if (rs) {
    return rc;
  }

  Space space;
  char buf[10000];
  QueryAnalysis::Instance()->showQueryStats(query, &space, buf);
  outputShortLine(space, buf);

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);
  return 0;
}

// Returns -1, if error.
short CmpDescribe(const char *query, const RelExpr *queryExpr, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  short rc = 0;  // assume success

  // save the current parserflags setting

  ULng32 savedParserFlags;
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedParserFlags);

  // OK, folks, we are about to locally change a global variable, so any returns
  // must insure that the global variable gets reset back to its original value
  // (saved above in "savedParserFlags"). So, if you are tempted to code a
  // "return", don't do it. Set the rc instead and goto the "finally" label.

  Set_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);

  // add an exception handler around all the SQL specific code
  try {
    // Display triggers using this object
    NABoolean showUsingTriggers = !!getenv("SQLMX_SHOW_USING_TRIGGERS");

    NABoolean showUsingMVs = !!getenv("SQLMX_SHOW_USING_MVS");
    // MV
    NABoolean showUnderlyingBT = !!getenv("SQLMX_SHOW_MVS_UNDERLYING_BASE_TABLE");

    NABoolean logFormat = (CmpCommon::getDefault(SHOWDDL_DISPLAY_FORMAT) == DF_LOG);
    int replIOVersion = (int)CmpCommon::getDefaultNumeric(REPLICATE_IO_VERSION);
    // Init SHOWDDL MX-format table regression test support for schema
    // names longer than 'SCH'.
    const char *testSchema = NULL;
    NAString testSchemaName;
    if (CmpCommon::context()->getSqlmxRegress() == 1) {
      const char *env = getenv("TEST_SCHEMA_NAME");
      if (env) {
        const size_t SCH_LEN = 3;  // 'SCH' schema length
        testSchemaName = env;
        if (testSchemaName.length() > SCH_LEN) testSchema = testSchemaName.data();
      }
    }

    char *buf = NULL;
    Space space;

    // emit an initial newline
    outputShortLine(space, " ");

    // initialize the returned values
    outbuf = NULL;
    outbuflen = 0;

    CMPASSERT(queryExpr->getOperatorType() == REL_ROOT);
    Describe *d = (Describe *)(queryExpr->child(0)->castToRelExpr());
    CMPASSERT(d->getOperatorType() == REL_DESCRIBE);

    if (d->getIsSchema() && d->getFormat() == Describe::SHOWSCHEMAHDFSCACHE_) {
      return CmpDescribeSchemaHDFSCache(d->getSchemaName(), outbuf, outbuflen, heap);
    } else if (d->getIsSchema()) {
      bool checkPrivs = false;
      if (!CmpDescribeIsAuthorized(heap)) {
        checkPrivs = true;
        CmpCommon::diags()->clear();
      }
      NAString schemaText;
      std::vector<std::string> outlines;
      QualifiedName objQualName(d->getDescribedTableName().getQualifiedNameObj(), STMTHEAP);

      NABoolean isHiveRegistered = FALSE;
      if (objQualName.isHive()) {
        CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, objQualName.getSchemaName(), objQualName.getCatalogName());
        cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

        BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
        NATable *naTable = bindWA.getNATable(cn);
        if ((naTable == NULL) || (bindWA.errStatus())) {
          CmpCommon::diags()->clear();

          *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(objQualName.getCatalogName())
                              << DgString1(objQualName.getSchemaName());

          rc = -1;
          goto finally;
        }

        isHiveRegistered = naTable->isRegistered();
      }

      if (!CmpSeabaseDDL::describeSchema(objQualName.getCatalogName(), objQualName.getSchemaName(), checkPrivs,
                                         isHiveRegistered, outlines))
      // schemaText))
      {
        rc = -1;
        goto finally;
      }

      for (int i = 0; i < outlines.size(); i++) outputLine(space, outlines[i].c_str(), 0);

      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL TENANT, go get description and return
    if (d->getIsTenant()) {
      NAString tenantText;
      CmpSeabaseDDLtenant tenantInfo;
      if (!tenantInfo.describe(d->getTypeIDName(), tenantText)) {
        rc = -1;
        goto finally;
      }

      outputLine(space, tenantText, 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL USER, go get description and return
    if (d->getIsUser()) {
      NAString userText;
      CmpSeabaseDDLuser userInfo;
      if (!userInfo.describe(d->getTypeIDName(), userText)) {
        rc = -1;
        goto finally;
      }

      outputLine(space, userText, 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL GROUP go get description and return
    if (d->getIsUserGroup()) {
      NABoolean enabled = msg_license_advanced_enabled();
      if (!enabled) {
        *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Advanced feature is not enabled.");
        return -1;
      }

      NAString groupText;
      CmpSeabaseDDLgroup groupInfo;
      if (!groupInfo.describe(d->getTypeIDName(), groupText)) {
        rc = -1;
        goto finally;
      }

      outputLine(space, groupText, 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL ROLE, go get description and return
    if (d->getIsRole()) {
      NAString roleText;
      CmpSeabaseDDLrole roleInfo(ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG));
      if (!roleInfo.describe(d->getTypeIDName(), roleText)) {
        rc = -1;
        goto finally;
      }

      outputLine(space, roleText, 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL RESOURCE GROUP, go get description and return
    if (d->getIsRGroup()) {
      NABoolean enabled = msg_license_multitenancy_enabled();
      if (!enabled) {
        *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Multi-tenant feature is not enabled.");
        return -1;
      }

      NAString groupText;
      ExeCliInterface cliInterface(CmpCommon::statementHeap(), 0, NULL,
                                   CmpCommon::context()->sqlSession()->getParentQid());

      TenantResource rgroupInfo(&cliInterface, CmpCommon::statementHeap());
      if (!rgroupInfo.describe(d->getTypeIDName(), groupText)) {
        rc = -1;
        goto finally;
      }

      outputLine(space, groupText, 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      goto finally;  // we are done and rc is already 0
    }

    // If SHOWDDL COMPONENT, go get description and return
    if (d->getIsComponent()) {
      if (!CmpDescribeIsAuthorized(heap, SQLOperation::MANAGE_COMPONENTS)) {
        rc = -1;
        goto finally;
      }
      std::vector<std::string> outlines;
      std::string privMDLoc = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);
      privMDLoc += ".\"";
      privMDLoc += SEABASE_PRIVMGR_SCHEMA;
      privMDLoc += "\"";
      PrivMgrCommands privMgrInterface(privMDLoc, CmpCommon::diags());

      // get description in REGISTER, CREATE, GRANT statements.
      CmpSeabaseDDL cmpSBD((NAHeap *)heap);
      if (cmpSBD.switchCompiler()) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
        rc = -1;
        goto finally;
      }

      if (!privMgrInterface.describeComponents(d->getComponentName().data(), outlines)) {
        *CmpCommon::diags() << DgSqlCode(-CAT_TABLE_DOES_NOT_EXIST_ERROR) << DgTableName(d->getComponentName().data());
        cmpSBD.switchBackCompiler();
        rc = -1;
        goto finally;
      }
      cmpSBD.switchBackCompiler();

      for (int i = 0; i < outlines.size(); i++) outputLine(space, outlines[i].c_str(), 0);
      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);
      goto finally;  // we are done and rc is already 0
    }

    if (d->getDescribedTableName().getQualifiedNameObj().getObjectNameSpace() == COM_LIBRARY_NAME) {
      if (!CmpDescribeLibrary(d->getDescribedTableName(), outbuf, outbuflen, heap)) {
        rc = -1;
        goto finally;
      }

      goto finally;  // we are done and rc is already 0
    }

    if (d->getDescribedTableName().getQualifiedNameObj().getObjectNameSpace() == COM_UDF_NAME) {
      if (d->isPackage()) {
        if (!CmpDescribePackage(d->getDescribedTableName(), outbuf, outbuflen, heap)) {
          rc = -1;
          goto finally;
        }
        goto finally;
      }

      if (!CmpDescribeRoutine(d->getDescribedTableName(), outbuf, outbuflen, heap)) {
        rc = -1;
        goto finally;
      }

      goto finally;  // we are done and rc is already 0
    }

    if (d->getDescribedTableName().getQualifiedNameObj().getObjectNameSpace() == COM_TRIGGER_NAME) {
      if (!CmpDescribeTrigger(d->getDescribedTableName(), outbuf, outbuflen, heap)) {
        rc = -1;
        goto finally;
      }
      goto finally;  // we are done and rc is already 0
    }

    ExtendedQualName::SpecialTableType tType = d->getDescribedTableName().getSpecialType();
    // Don't reveal exception table (special table syntax)
    if ((CmpCommon::getDefault(IS_DB_TRANSPORTER) == DF_OFF) && (tType == ExtendedQualName::EXCEPTION_TABLE))
      tType = ExtendedQualName::NORMAL_TABLE;

    NAString upshiftedLocName(d->getDescribedTableName().getLocationName());
    upshiftedLocName.toUpper();
    NAString *pLocName = NULL;
    if (d->getDescribedTableName().isLocationNameSpecified()) pLocName = &upshiftedLocName;

    if (d->getFormat() == Describe::PLAN_) {
      // Return runtime plan.
      // This command (SHOWPLAN) is for internal debugging use only.

      CMPASSERT(query);

      // Skip leading blanks
      for (; *query == ' '; query++)
        ;

      // Skip the SHOWPLAN token & option (if specified)(length to be determined) in front of the input query.
      Int32 i = 8;
      while (query[i] == ' ') i++;
      if (str_cmp(query + i, "option", 6) == 0 || str_cmp(query + i, "OPTION", 6) == 0) {
        while (query[i] != '\'' && query[i] != '\0') i++;
        i++;
        while (query[i] != '\'' && query[i] != '\0') i++;
        i++;
      }
      rc = CmpDescribePlan(&query[i], d->getFlags(), outbuf, outbuflen, heap);
      goto finally;  // we are done
    }

    if (d->getFormat() == Describe::SHAPE_) {
      // Return CONTROL QUERY SHAPE statement for this query.

      CMPASSERT(query);

      // Skip leading blanks
      for (; *query == ' '; query++)
        ;

      // Skip the SHOWSHAPE token (length 9) in front of the input query.
      rc = CmpDescribeShape(&query[9], outbuf, outbuflen, heap);
      goto finally;  // we are done
    }

    if (d->getFormat() == Describe::SHOWQRYSTATS_) {
      // show histogram for root of this query.
      rc = CmpDescribeShowQryStats(query, outbuf, outbuflen, heap);
      goto finally;  // we are done
    }

    if (d->getFormat() == Describe::UUID_) {
      rc = CmpDescribeUUID(d, outbuf, outbuflen, heap);
      goto finally;  // we are done
    }

    if (d->getIsControl()) {
      rc = CmpDescribeControl(d, outbuf, outbuflen, heap);
      goto finally;  // we are done
    }

    if (d->getFormat() == Describe::LEAKS_) {
#ifdef NA_DEBUG_HEAPLOG
      if ((d->getFlags()) & LeakDescribe::FLAG_ARKCMP)
        outbuflen = HeapLogRoot::getPackSize();
      else
        outbuflen = 8;
      HEAPLOG_OFF();
      outbuf = (char *)heap->allocateMemory(outbuflen);
      HEAPLOG_ON();
      HeapLogRoot::pack(outbuf, d->getFlags());
#endif
      goto finally;  // we are done and rc is already 0
    }

    if (d->getFormat() == Describe::TRANSACTION_) {
      rc = CmpDescribeTransaction(outbuf, outbuflen, heap);
      goto finally;  // we are done
    }
    if (d->getFormat() == Describe::SHOWVIEWS_) {
      rc = CmpDescribeShowViews(outbuf, outbuflen, heap);
      goto finally;
    }
    if (d->getFormat() == Describe::DESCVIEW_) {
      rc = CmpDescribeDescView(d->getDescribedTableName(), outbuf, outbuflen, heap);
      goto finally;
    }
    if (d->getFormat() == Describe::SHOWENV_) {
      rc = CmpDescribeShowEnv(outbuf, outbuflen, heap);
      goto finally;
    }

    // Start SHOWDDL code

    if (d->getLabelAnsiNameSpace() == COM_SEQUENCE_GENERATOR_NAME) {
      rc = CmpDescribeSequence(d->getDescribedTableName(), outbuf, outbuflen, heap, NULL);
      goto finally;  // we are done
    }

    // check if this is an hbase/seabase table. If so, describe using info from NATable.
    if (((d->getFormat() == Describe::INVOKE_) || (d->getFormat() == Describe::SHOWDDL_)) &&
        ((d->getDescribedTableName().isHbase()) || (d->getDescribedTableName().isSeabase()) ||
         (d->getDescribedTableName().getSpecialType() == ExtendedQualName::VIRTUAL_TABLE))) {
      rc = CmpDescribeSeabaseTable(d->getDescribedTableName(), (d->getFormat() == Describe::INVOKE_ ? 1 : 2), outbuf,
                                   outbuflen, heap, FALSE, NULL, NULL, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
                                   FALSE, UINT_MAX, FALSE, FALSE, NULL, 0, NULL, NULL, NULL, d->getIsDetail());
      goto finally;  // we are done
    }

    // Show cache for HDFS cache
    if (d->getFormat() == Describe::SHOWTABLEHDFSCACHE_) {
      return CmpDescribeTableHDFSCache(d->getDescribedTableName(), outbuf, outbuflen, heap);
    }

    TrafDesc *tabledesc = NULL;
    if (ExtendedQualName::isDescribableTableType(tType)) {
      *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("DESCRIBE");
    } else  // special virtual table
    {
      {
        // not supported
        *CmpCommon::diags() << DgSqlCode(-3131);
        rc = -1;
        goto finally;
      }
    }

    if (!tabledesc) {
      rc = -1;
      goto finally;
    }

    NAString tableName(tabledesc->tableDesc()->tablename);

    NABoolean external = d->getIsExternal();
    // strip catalog off of table name
    if (external) {
      ComObjectName externName(tableName, COM_TABLE_NAME);
      if (logFormat) {
        if ((replIOVersion >= 5 && replIOVersion <= 10) || (replIOVersion >= 14))
          tableName = externName.getObjectNamePartAsAnsiString();
        else
          tableName = externName.getExternalName(FALSE, TRUE);
      } else
        tableName = externName.getExternalName(FALSE, TRUE);
    }

    NABoolean isVolatile = FALSE;
    NABoolean isInMemoryObjectDefn = FALSE;

    if (isVolatile) Set_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

    // Allocate enough for a big view or constraint definition
    const Int32 LOCAL_BIGBUF_SIZE = 30000;
    buf = new (CmpCommon::statementHeap()) char[LOCAL_BIGBUF_SIZE];
    CMPASSERT(buf);

    TrafDesc *viewdesc = tabledesc->tableDesc()->views_desc;

    if (d->getFormat() == Describe::INVOKE_) {
      time_t tp;
      time(&tp);

      if (viewdesc)
        sprintf(buf,
                "-- Definition of view %s\n"
                "-- Definition current %s",
                tableName.data(), ctime(&tp));
      else if (tType == ExtendedQualName::TRIGTEMP_TABLE)
        sprintf(buf,
                "-- Definition of table (TEMP_TABLE %s)\n"
                "-- Definition current  %s",
                tableName.data(), ctime(&tp));
      else if (tType == ExtendedQualName::EXCEPTION_TABLE)
        sprintf(buf,
                "-- Definition of table (EXCEPTION_TABLE %s)\n"
                "-- Definition current  %s",
                tableName.data(), ctime(&tp));
      else if (tType == ExtendedQualName::IUD_LOG_TABLE)
        sprintf(buf,
                "-- Definition of table (IUD_LOG_TABLE %s)\n"
                "-- Definition current  %s",
                tableName.data(), ctime(&tp));
      else if (tType == ExtendedQualName::RANGE_LOG_TABLE)
        sprintf(buf,
                "-- Definition of table (RANGE_LOG_TABLE %s)\n"
                "-- Definition current  %s",
                tableName.data(), ctime(&tp));

      else {
        if (isVolatile) {
          ComObjectName cn(tableName.data());
          sprintf(buf,
                  "-- Definition of %svolatile table %s\n"
                  "-- Definition current  %s",
                  (isInMemoryObjectDefn ? "InMemory " : ""), cn.getObjectNamePartAsAnsiString().data(), ctime(&tp));
        } else if (isInMemoryObjectDefn) {
          sprintf(buf,
                  "-- Definition of InMemory table %s\n"
                  "-- Definition current  %s",
                  tableName.data(), ctime(&tp));
        } else {
          sprintf(buf,
                  "-- Definition of table %s\n"
                  "-- Definition current  %s",
                  tableName.data(), ctime(&tp));
        }
      }

      outputShortLine(space, buf);

      outputShortLine(space, "  ( ");
      NABoolean displayAddedCols = FALSE;  // dummy variable
      // this may be a problem when describing virtual tables like EXPLAIN
      *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("INVOKE for TABLE");
      outputShortLine(space, "  )");

      outbuflen = space.getAllocatedSpaceSize();
      outbuf = new (heap) char[outbuflen];
      space.makeContiguous(outbuf, outbuflen);

      NADELETEBASIC(buf, CmpCommon::statementHeap());
      goto finally;  // we are done and rc is already 0
    }

    // This is done for SHOWDDL only. Not for SHOWLABEL
    if (logFormat) outputShortLine(space, "--> BT ");
    sprintf(buf, "CREATE TABLE %s", tableName.data());

    outputLine(space, buf, 0, 2, FALSE, testSchema);

    const NABoolean commentOut = FALSE;
    const NABoolean mustShowBT = TRUE;
    if (mustShowBT) {
      outputShortLine(space, "  (", commentOut);
    }

    NABoolean displayAddedCols = FALSE;

    // this may be a problem when describing virtual tables like EXPLAIN
    *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("SHOWDDL for TABLE");

    Int32 pkey_colcount = 0;

    // Flag for additional alter table statements for constraints
    NABoolean displayAlterTable = FALSE;
    // Flag for whether primary key belongs in a ALTER TABLE statement
    NABoolean alterTablePrimaryKey = FALSE;

    buf[0] = '\0';

    if (mustShowBT) {
      outputShortLine(space, "  ;", commentOut);
    }

    outbuflen = space.getAllocatedSpaceSize();
    outbuf = new (heap) char[outbuflen];
    space.makeContiguous(outbuf, outbuflen);

    Reset_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

    NADELETEBASIC(buf, CmpCommon::statementHeap());

  }  // end of try block

  // exception handling
  catch (...) {
    // Check diags area, if it doesn't contain an error, add an
    // internal exception
    if (CmpCommon::diags()->getNumber() == 0) {
      *CmpCommon::diags() << DgSqlCode(-2006);
      *CmpCommon::diags() << DgString0("Unknown error returned while retrieving metadata");
      *CmpCommon::diags() << DgString1(__FILE__);
      *CmpCommon::diags() << DgInt0(__LINE__);
    }
    rc = -1;
  }

  finally :

      // Restore parser flags settings to what they originally were
      Assign_SqlParser_Flags(savedParserFlags);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedParserFlags);

  return rc;

}  // SHOWDDL

short exeImmedCQD(const char *cqd, NABoolean hold) {
  Int32 retcode = 0;

  ExeCliInterface cliInterface(CmpCommon::statementHeap());
  if (hold)
    retcode = cliInterface.holdAndSetCQD(cqd, "ON");
  else
    retcode = cliInterface.restoreCQD(cqd);
  if (retcode < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

    return retcode;
  }

  return 0;
}

short exeImmedOneStmt(const char *controlStmt) {
  Int32 retcode = 0;

  ExeCliInterface cliInterface(CmpCommon::statementHeap());
  retcode = cliInterface.executeImmediate(controlStmt);
  if (retcode < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return retcode;
  }

  return 0;
}

short sendAllControls(NABoolean copyCQS, NABoolean sendAllCQDs, NABoolean sendUserCQDs,
                      enum COM_VERSION versionOfCmplrRcvCntrlInfo, NABoolean sendUserCSs, CmpContext *prevContext) {
  // -----------------------------------------------------------------------
  //  Given a SQL query, this procedure invokes the appropriate
  //  CLI calls to prepare and execute the statement.
  // -----------------------------------------------------------------------
  Int32 retcode = 0;

  char *buf = new (CmpCommon::statementHeap()) char[15000];

  if (copyCQS) {
    if (ActiveControlDB()->getRequiredShape()) {
      retcode = exeImmedOneStmt(ActiveControlDB()->getRequiredShape()->getShapeText());
      if (retcode) return (short)retcode;
    } else if (ActiveControlDB()->requiredShapeWasOnceNonNull()) {
      // Genesis solution 10-040528-6511: If we possibly
      // sent a CQS down once before, we need to send a
      // CQS CUT to get rid of it if it is no longer
      // active.
      strcpy(buf, "CONTROL QUERY SHAPE CUT;");
      retcode = exeImmedOneStmt(buf);
      if (retcode) return ((short)retcode);
    }
  }

  CollIndex i;
  ControlDB *cdb = NULL;

  // if prevContext is defined, get the controlDB from the previous context so the
  // defaults are copied from the previous cmp context to the new cmp context. This is only
  // required for embedded compilers where a SWITCH is taking place
  if (prevContext)
    cdb = prevContext->getControlDB();
  else
    cdb = ActiveControlDB();

  if ((sendAllCQDs) || (sendUserCQDs)) {
    // send all externalized CQDs.
    NADefaults &defs = CmpCommon::context()->schemaDB_->getDefaults();

    for (i = 0; i < defs.numDefaultAttributes(); i++) {
      const char *attrName, *val;
      if (defs.getCurrentDefaultsAttrNameAndValue(i, attrName, val, sendUserCQDs)) {
        if (NOT defs.isReadonlyAttribute(attrName)) {
          if (strcmp(attrName, "ISO_MAPPING") == 0) {
            // ISO_MAPPING is a read-only default attribute --
            // But for internal development and testing purpose,
            // we sometimes want to change the setting within
            // an MXCI session. Make sure that we send the CQD
            // COMP_BOOL_58 'ON' before sending CQD ISO_MAPPING
            // so that the child/receiving MXCMP does not reject
            // the CQD ISO_MAPPING request.
            sprintf(buf, "CONTROL QUERY DEFAULT COMP_BOOL_58 'ON';");
            exeImmedOneStmt(buf);
          }

          if (strcmp(attrName, "TERMINAL_CHARSET") == 0 && versionOfCmplrRcvCntrlInfo < COM_VERS_2300) {
            sprintf(buf, "CONTROL QUERY DEFAULT TERMINAL_CHARSET 'ISO88591';");
          } else {
            if (strcmp(attrName, "REPLICATE_IO_VERSION") == 0) {
              ULng32 originalParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
              SQL_EXEC_SetParserFlagsForExSqlComp_Internal(originalParserFlags);
            }
            NAString quotedString;
            ToQuotedString(quotedString, NAString(val), TRUE);
            sprintf(buf, "CONTROL QUERY DEFAULT %s %s;", attrName, quotedString.data());
          }
          retcode = exeImmedOneStmt(buf);
          if (retcode) return ((short)retcode);
        }
      }
    }
  }

  if (NOT sendUserCQDs && prevContext) {
    // Send only to different context (prevContext != NULL)
    // and send the CQDs that were entered by user and were not 'reset reset'
    for (i = 0; i < cdb->getCQDList().entries(); i++) {
      ControlQueryDefault *cqd = cdb->getCQDList()[i];
      NAString quotedString;
      ToQuotedString(quotedString, cqd->getValue());

      if (strcmp(cqd->getToken().data(), "TERMINAL_CHARSET") == 0 && versionOfCmplrRcvCntrlInfo < COM_VERS_2300) {
        sprintf(buf, "CONTROL QUERY DEFAULT TERMINAL_CHARSET 'ISO88591';");
      } else {
        if (strcmp(cqd->getToken().data(), "REPLICATE_IO_VERSION") == 0) {
          ULng32 originalParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
          SQL_EXEC_SetParserFlagsForExSqlComp_Internal(originalParserFlags);
        }
        sprintf(buf, "CONTROL QUERY DEFAULT %s %s;", cqd->getToken().data(), quotedString.data());
      }
      retcode = exeImmedOneStmt(buf);
      if (retcode) return ((short)retcode);
    }
  }

  for (i = 0; i < cdb->getCTList().entries(); i++) {
    ControlTableOptions *cto = cdb->getCTList()[i];

    for (CollIndex j = 0; j < cto->numEntries(); j++) {
      sprintf(buf, "CONTROL TABLE %s %s '%s';", cto->tableName().data(), cto->getToken(j).data(),
              cto->getValue(j).data());
      retcode = exeImmedOneStmt(buf);
      if (retcode) return ((short)retcode);
    }  // j
  }    // i

  // pass on the parserflags to the new compiler
  // used to enable SHOWPLAN for MV INTERNAL REFRESH
  if (CmpCommon::getDefault(MV_ENABLE_INTERNAL_REFRESH_SHOWPLAN) == DF_ON) {
    ULng32 originalParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal(originalParserFlags);
  }

  if (sendUserCSs) {
    for (i = 0; i < cdb->getCSList().entries(); i++) {
      ControlSessionOption *cso = cdb->getCSList()[i];

      sprintf(buf, "CONTROL SESSION '%s' '%s';", cso->getToken().data(), cso->getValue().data());
      retcode = exeImmedOneStmt(buf);
      if (retcode) return ((short)retcode);
    }
  }

  if (CmpCommon::context()->sqlSession()->volatileSchemaInUse()) {
    // sendAllControls is called by mxcmp when it prepares internal
    // queries. Tablenames used in these queries may already be fully
    // qualified with volatile schema name.
    // Set the flag to indicate that an error should not be returned
    // if volatile schema name has been explicitely specified.
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  } else {
    SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  }

  return 0;
}

void sendParserFlag(ULng32 flag) { SQL_EXEC_SetParserFlagsForExSqlComp_Internal(flag); }

short setParentQidAtSession(NAHeap *heap, const char *parentQid) {
  Int32 retcode = 0;

  short len = (short)strlen(parentQid);
  char *srcBuf = new (heap) char[len + 50];
  strcpy(srcBuf, "SET SESSION DEFAULT PARENT_QID '");
  strcat(srcBuf, parentQid);
  strcat(srcBuf, "'");
  retcode = exeImmedOneStmt(srcBuf);
  NADELETEBASIC(srcBuf, heap);
  return (short)retcode;
}

/*
static long describeError(long retcode)
{
  *CmpCommon::diags() << DgSqlCode(retcode);
  return -1;
}
*/

/////////////////////////////////////////////////////////////////
//
// flags:
//        0x00000001   display all expressions
//        0x00000002   display pcode expressions
//        0x00000004   display clause expressions
//        0x00000008   display TDBs
//        0x00000010   do not regenerate pcode for showplan.
//        0x00000010   do downrev compile for R2. Used when pcode is
//                     regenerated during display
//        0x00000020   do downrev compile for RR. Used when pcode is
//                     regenerated during display
/////////////////////////////////////////////////////////////////
static short CmpGetPlan(SQLSTMT_ID &stmt_id, ULng32 flags, Space &space, CollHeap *heap, char *&rootTdbBuf,
                        int &rootTdbSize, char *&srcStrBuf, int &srcStrSize) {
  int retcode = 0;
  ULong stmtHandle = (ULong)stmt_id.handle;

  // Now get the generated code to describe the plan.
  // do not advance to next statement yet, if all stmts are to be retrieved.
  if ((stmt_id.handle) && (stmt_id.name_mode != stmt_handle) && ((stmtHandle & 0x0001) != 0))
    stmt_id.handle = (void *)(stmtHandle & ~0x0004);
  retcode = SQLCLI_GetRootTdbSize_Internal(GetCliGlobals(), &rootTdbSize, &srcStrSize, &stmt_id);

  if (retcode) return ((retcode < 0) ? -1 : (short)retcode);

  rootTdbBuf = new (heap) char[rootTdbSize];
  srcStrBuf = new (heap) char[srcStrSize + 1];
  // now advance to the next statement
  stmtHandle = (ULong)stmt_id.handle;
  if ((stmt_id.handle) && (stmt_id.name_mode != stmt_handle) && ((stmtHandle & 0x0001) != 0))
    stmt_id.handle = (void *)(stmtHandle | 0x0004);
  retcode = SQLCLI_GetRootTdb_Internal(GetCliGlobals(), rootTdbBuf, rootTdbSize, srcStrBuf, srcStrSize, &stmt_id);
  if (retcode) return ((retcode < 0) ? -1 : (short)retcode);

  if (srcStrSize > 0) srcStrBuf[srcStrSize] = 0;

  return (short)retcode;
}

/////////////////////////////////////////////////////////////////
//
// flags:
//        0x00000001   display all expressions
//        0x00000002   display pcode expressions
//        0x00000004   display clause expressions
//        0x00000008   display TDBs
//        0x00000010   do downrev compile. Used when pcode is regenerated
//                     during display
/////////////////////////////////////////////////////////////////
static int CmpFormatPlan(ULng32 flags, char *rootTdbBuf, int rootTdbSize, char *srcStrBuf, int srcStrSize,
                           NABoolean outputSrcInfo, Space &space, CollHeap *heap) {
  char buf[200];

  ComTdbRoot *rootTdb = (ComTdbRoot *)rootTdbBuf;

  // unpack the root TDB
  ComTdb dummyTdb;
  if ((rootTdb = (ComTdbRoot *)rootTdb->driveUnpack((void *)rootTdb, &dummyTdb, NULL)) == NULL) {
    // ERROR during unpacking. Shouldn't occur here since we have just
    // freshly prepared the statement using the latest version of the
    // compiler. This is an unexpected error.
    //
    return -1;
  }

  UInt32 planVersion = rootTdb->getPlanVersion();

  // This fragment corresponds to the MASTER.
  // Get to the fragment directory from rootTdb.
  void *baseAddr = (void *)rootTdb;

  ExFragDir *fragDir = rootTdb->getFragDir();
  ComTdb *fragTdb = rootTdb;

  // a sanity check to see that it was the MASTER fragment.
  CMPASSERT(fragDir->getType(0) == ExFragDir::MASTER);

  // unpack each fragment independently;
  // unpacking converts offsets to actual pointers.
  for (Int32 i = 0; i < fragDir->getNumEntries(); i++) {
    void *fragBase = (void *)((char *)baseAddr + fragDir->getGlobalOffset(i));
    void *fragStart = (void *)((char *)fragBase + fragDir->getTopNodeOffset(i));

    switch (fragDir->getType(i)) {
      case ExFragDir::MASTER: {
        // already been unpacked. Nothing to be done.
        sprintf(buf, "MASTER Executor fragment");
        outputShortLine(space, buf);
        sprintf(buf, "========================\n");
        outputShortLine(space, buf);
        sprintf(buf, "Fragment ID: %d, Length: %d\n", i, fragDir->getFragmentLength(i));
        outputShortLine(space, buf);
      } break;

      case ExFragDir::ESP: {
        sprintf(buf, "ESP fragment");
        outputShortLine(space, buf);
        sprintf(buf, "===========\n");
        outputShortLine(space, buf);
        sprintf(buf, "Fragment ID: %d, Parent ID: %d, Length: %d\n", i, fragDir->getParentId(i),
                fragDir->getFragmentLength(i));
        outputShortLine(space, buf);
        ComTdb tdb;
        fragTdb = (ComTdb *)fragStart;
        fragTdb = (ComTdb *)(fragTdb->driveUnpack(fragBase, &tdb, NULL));
      } break;

      case ExFragDir::DP2: {
        sprintf(buf, "EID fragment");
        outputShortLine(space, buf);
        sprintf(buf, "===========\n");
        outputShortLine(space, buf);

        sprintf(buf, "Fragment ID: %d, Parent ID: %d, Length: %d\n", i, fragDir->getParentId(i),
                fragDir->getFragmentLength(i));
        outputShortLine(space, buf);
      } break;
      case ExFragDir::EXPLAIN:
        // Not using explain frag here!
        fragTdb = NULL;
        break;
    }  // switch

    // Now display expressions.
    flags = flags | 0x00000001;
    if (fragTdb) {
      if ((planVersion > COM_VERS_R1_8) && (planVersion < COM_VERS_R2_1)) {
        // downrev compile needed.
        flags |= 0x00000020;
      } else if (planVersion == COM_VERS_R2_1)
        flags |= 0x0000020;

      if ((CmpCommon::getDefaultLong(QUERY_OPTIMIZATION_OPTIONS) & QO_EXPR_OPT) == 0) {
        flags |= 0x00000020;
      }

      fragTdb->displayContents(&space, flags);
    }

  }  // for

  return 0;
}

static short CmpDescribePlan(const char *query, ULng32 flags, char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  // prepare this query.
  int retcode;
  int resetRetcode;

  Space space;

  char *genCodeBuf = NULL;
  int genCodeSize = 0;

  ExeCliInterface cliInterface(CmpCommon::statementHeap());

  // The arkcmp that will be used to compile this query does not
  // know about the control statements that were issued.
  // Send all the CONTROL statements before preparing this statement.

  // First reset any previously set cqds.
  retcode = cliInterface.executeImmediate("cqd * reset;");
  if (retcode < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // and then send current cqds
  retcode = sendAllControls(TRUE, FALSE, TRUE);
  if (retcode) goto label_error;

  // send control session to indicate showplan is being done
  retcode = cliInterface.executeImmediate("CONTROL SESSION SET 'SHOWPLAN' 'ON';");
  if (retcode < 0) goto label_error;

  // prepare it and get the generated code size
  retcode = cliInterface.executeImmediatePrepare2(query, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, &genCodeSize);
  if ((retcode == -CLI_GENCODE_BUFFER_TOO_SMALL) && (genCodeSize > 0)) {
    cliInterface.clearGlobalDiags();

    // retrieve the generated code
    genCodeBuf = new (heap) char[genCodeSize];
    retcode = cliInterface.getGeneratedCode(genCodeBuf, genCodeSize);
  }

  if (retcode < 0)  // ignore warnings
  {
    resetRetcode = SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());

    goto label_error_1;
  }

  retcode = CmpFormatPlan(flags, genCodeBuf, genCodeSize, NULL, 0, FALSE, space, heap);
  if (retcode) {
    goto label_error_1;
  }

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  retcode = cliInterface.executeImmediate("CONTROL SESSION RESET 'SHOWPLAN';");
  if (retcode < 0) goto label_error;

  return 0;

label_error_1:
  resetRetcode = cliInterface.executeImmediate("CONTROL SESSION RESET 'SHOWPLAN';");

label_error:
  return ((retcode < 0) ? -1 : (short)retcode);
}  // CmpDescribePlan

// TRUE if ident is nonempty and contained in comparand string c1.
inline static NABoolean identSpecifiedMatches(const NAString &id, const char *c1) {
  return (!id.isNull() && strstr(c1, id));
}  // strstr(fullstring, substring)

// TRUE if ident is empty (not specified), or is contained in c1.
inline static NABoolean identMatches(const NAString &id, const char *c1) {
  return (id.isNull() || strstr(c1, id));
}  // strstr(fullstring, substring)

// If not exact match:
//   TRUE if ident is empty, or is contained in c1, or c1 equals c2.
// If exact:
//   TRUE if ident equals c1, period.
inline static NABoolean identMatches2(const NAString &id, const char *c1, const char *c2, NABoolean exactMatch) {
  return exactMatch ? (strcmp(id, c1) == 0) : (identMatches(id, c1) || strcmp(c1, c2) == 0);
}

#define IDENTMATCHES(c1, c2) identMatches2(ident, c1, c2, exactMatch)

// defined in generator/GenExplain.cpp
NABoolean displayDuringRegressRun(DefaultConstants attr);

// these CQDs are used for internal purpose and are set/sent by SQL.
// Do not display them.
static NABoolean isInternalCQD(DefaultConstants attr) {
  if ((attr == SESSION_ID) || (attr == SESSION_IN_USE) || (attr == LDAP_USERNAME) || (attr == VOLATILE_CATALOG) ||
      (attr == VOLATILE_SCHEMA_IN_USE) || (attr == IS_SQLCI) || (attr == IS_TRAFCI))
    return TRUE;

  if ((getenv("SQLMX_REGRESS")) && ((NOT displayDuringRegressRun(attr)) || (attr == TRAF_ALIGNED_ROW_FORMAT)))
    return TRUE;

  return FALSE;
}
short CmpDescribeControl(Describe *d, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  // Two kludgy passing mechanisms from SqlParser.y ...
  NAString ident = d->getDescribedTableName().getQualifiedNameObj().getObjectName();
  NABoolean exactMatch = !d->getDescribedTableName().getCorrNameAsString().isNull();
  CMPASSERT(!(ident.isNull() && exactMatch));

  NAString fmtdIdent;
  if (!ident.isNull()) fmtdIdent = NAString(' ') + ident;
  static const char fmtNoMatch[] = "No CONTROL %s%s settings are in effect.";
  static const char fmtNoMatch2[] = "No DEFAULTS attribute%s exists.";
  static const char fmtNoMatch3[] = "No externalized DEFAULTS attribute%s exists.";

  // static const char fmtExactMatch[] = "\n%-32s\t%s";
  static const char fmtFuzzyMatch[] = "  %-30s\t%s";
  // const char *fmtInfo = exactMatch ? fmtExactMatch : fmtFuzzyMatch;
  const char *fmtInfo = fmtFuzzyMatch;

  // SHOWCONTROL DEFAULT CAT will display both CATALOG and SCHEMA defaults;
  // SHOWCONTROL DEFAULT SCH will too.
  static const char *defCatSchStrings[] = {"", "CATALOG", "SCHEMA"};
  size_t ix = identSpecifiedMatches(ident, "CATALOG") ? 2 : identSpecifiedMatches(ident, "SCHEMA") ? 1 : 0;
  const char *defCatSch = defCatSchStrings[ix];

  NABoolean showShape, showSession, showTable, showDefault, match = FALSE;
  NABoolean showAll = d->getFormat() == Describe::CONTROL_ALL_;

  if (showAll) {
    showShape = TRUE;
    showSession = TRUE;
    showTable = TRUE;
    showDefault = TRUE;
  } else {
    showShape = d->getFormat() == Describe::CONTROL_SHAPE_;
    showSession = d->getFormat() == Describe::CONTROL_SESSION_;
    showTable = d->getFormat() == Describe::CONTROL_TABLE_;
    showDefault = d->getFormat() == Describe::CONTROL_DEFAULTS_;
  }

  Space space;
  char *buf = new (heap) char[15000];
  CMPASSERT(buf);

  ControlDB *cdb = ActiveControlDB();

  if (showShape) {
    outputShortLine(space, "");
    if ((cdb->getRequiredShape()) && (cdb->getRequiredShape()->getShape())) {
      NAString shape(cdb->getRequiredShape()->getShapeText());
      PrettifySqlText(shape);
      outputLine(space, shape, 2);
    } else {
      // Only one shape at a time (no history-list), so no ident-matching...
      sprintf(buf, fmtNoMatch, "QUERY SHAPE", "");
      outputLine(space, buf, 0);
    }
  }  // SHOWCONTROL SHAPE

  if (showSession) {
    outputShortLine(space, "");
    match = FALSE;
    for (CollIndex i = 0; i < cdb->getCSList().entries(); i++) {
      ControlSessionOption *cso = cdb->getCSList()[i];
      if (IDENTMATCHES(cso->getToken().data(), "*")) {
        if (!match) outputShortLine(space, "CONTROL SESSION");
        sprintf(buf, fmtInfo, cso->getToken().data(), cso->getValue().data());
        outputLine(space, buf, 2);
        match = TRUE;
      }
    }
    if (!match) {
      sprintf(buf, fmtNoMatch, "SESSION", fmtdIdent.data());
      outputLine(space, buf, 0);
    }
  }  // SHOWCONTROL SESSION

  if (showTable) {
    outputShortLine(space, "");
    match = FALSE;
    for (CollIndex i = 0; i < cdb->getCTList().entries(); i++) {
      ControlTableOptions *cto = cdb->getCTList()[i];
      if (IDENTMATCHES(cto->tableName().data(), "*") && cto->numEntries() > 0) {
        sprintf(buf, "CONTROL TABLE %s", cto->tableName().data());
        outputLine(space, buf, 0);
        match = TRUE;
        for (CollIndex j = 0; j < cto->numEntries(); j++) {
          sprintf(buf, fmtInfo, cto->getToken(j).data(), cto->getValue(j).data());
          outputLine(space, buf, 2);
        }  // j
      }    // ident absent or it matches
    }      // i
    if (!match) {
      sprintf(buf, fmtNoMatch, "TABLE", fmtdIdent.data());
      outputLine(space, buf, 0);
    }
  }  // SHOWCONTROL TABLE

  if (showDefault) {
    {
      if (d->getHeader()) outputShortLine(space, "");
      match = FALSE;
      for (CollIndex i = 0; i < cdb->getCQDList().entries(); i++) {
        ControlQueryDefault *cqd = cdb->getCQDList()[i];
        if (IDENTMATCHES(cqd->getToken().data(), defCatSch)) {
          // This is the NO HEADER option. We just want the value of the default.
          if (!(d->getHeader())) {
            strcpy(buf, cqd->getValue().data());
            outputLine(space, buf, 2);
            outbuflen = space.getAllocatedSpaceSize();
            outbuf = new (heap) char[outbuflen];
            space.makeContiguous(outbuf, outbuflen);
            NADELETEBASIC(buf, heap);
            return 0;
          } else {
            if (NOT isInternalCQD(cqd->getAttrEnum())) {
              if (!match) outputShortLine(space, "CONTROL QUERY DEFAULT");
              sprintf(buf, fmtInfo, cqd->getToken().data(), cqd->getValue().data());
              outputLine(space, buf, 2);
              match = TRUE;
            }
          }
        }
      }
      if (!match && d->getHeader()) {
        sprintf(buf, fmtNoMatch, "QUERY DEFAULT", fmtdIdent.data());
        outputLine(space, buf, 0);
      }
    }

    // This is a nice little extra for partial-match, and
    // essential for exactMatch:
    if (!ident.isNull()) showAll = TRUE;

  }  // SHOWCONTROL DEFAULTS

  if (showAll) {
    if (d->getHeader()) outputShortLine(space, "");
    match = FALSE;
    NADefaults &defs = CmpCommon::context()->schemaDB_->getDefaults();

    for (CollIndex i = 0; i < defs.numDefaultAttributes(); i++) {
      const char *attrName, *val;
      if (defs.getCurrentDefaultsAttrNameAndValue(i, attrName, val, FALSE) && IDENTMATCHES(attrName, defCatSch)) {
        // This is the NO HEADER option. We just want the value of the default.
        if (!(d->getHeader())) {
          strcpy(buf, val);
          outputLine(space, buf, 2);
          outbuflen = space.getAllocatedSpaceSize();
          outbuf = new (heap) char[outbuflen];
          space.makeContiguous(outbuf, outbuflen);
          NADELETEBASIC(buf, heap);
          return 0;
        } else {
          if (!match) outputShortLine(space, "Current DEFAULTS");
          sprintf(buf, fmtInfo, attrName, val);
          outputLine(space, buf, 2);
          match = TRUE;
        }
      }
    }
    if (!match) {
      if (CmpCommon::getDefault(SHOWCONTROL_SHOW_ALL) != DF_ON)
        sprintf(buf, fmtNoMatch3, fmtdIdent.data());
      else
        sprintf(buf, fmtNoMatch2, fmtdIdent.data());
      outputLine(space, buf, 0);
    }
  }

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  NADELETEBASIC(buf, heap);

  return 0;
}

static short CmpDescribeShape(const char *query, char *&outbuf, ULng32 &outbuflen, NAMemory *heap)

{
  // prepare this query.
  char *qTree = NULL;
  ULng32 dummyLen;

  CmpMain sqlcomp;

  CmpMain::CompilerPhase phase = CmpMain::PRECODEGEN;
#ifdef _DEBUG
  char *sp = getenv("SHOWSHAPE_PHASE");
  if (sp) {
    switch (sp[0]) {
      case 'S':
        phase = CmpMain::PARSE;
        break;

      case 'B':
        phase = CmpMain::BIND;
        break;

      case 'T':
        phase = CmpMain::TRANSFORM;
        break;

      case 'N':
        phase = CmpMain::NORMALIZE;
        break;

      case 'O':
        phase = CmpMain::OPTIMIZE;
        break;

      case 'P':
        phase = CmpMain::PRECODEGEN;
        break;

      case 'G':
        phase = CmpMain::GENERATOR;
        break;

      default:
        phase = CmpMain::PRECODEGEN;
        break;
    }  // switch
  }    // if
#endif

  QueryText qText((char *)query, SQLCHARSETCODE_UTF8);
  if (sqlcomp.sqlcomp(qText, 0, &qTree, &dummyLen, heap, phase)) {
    CMPASSERT(query);
    return -1;
  }

  RelExpr *queryTree = (RelExpr *)qTree;

  Space space;
  char buf[1000];

  sprintf(buf, "control query shape ");

  queryTree->generateShape(&space, buf);

  strcat(buf, ";");
  outputShortLine(space, buf);

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);
  return 0;
}

// show transaction
static short CmpDescribeTransaction(char *&outbuf, ULng32 &outbuflen, NAMemory *heap)

{
  Space space;
  char buf[100];

  if (CmpCommon::transMode()->getIsolationLevel() == TransMode::READ_UNCOMMITTED_)
    outputShortLine(space, "ISOLATION LEVEL      : READ UNCOMMITTED");
  else if (CmpCommon::transMode()->getIsolationLevel() == TransMode::READ_COMMITTED_)
    outputShortLine(space, "ISOLATION LEVEL      : READ COMMITTED");
  else if (CmpCommon::transMode()->getIsolationLevel() == TransMode::REPEATABLE_READ_)
    outputShortLine(space, "ISOLATION LEVEL      : REPEATABLE READ");
  else if (CmpCommon::transMode()->getIsolationLevel() == TransMode::SERIALIZABLE_)
    outputShortLine(space, "ISOLATION LEVEL      : SERIALIZABLE");
  else if (CmpCommon::transMode()->getIsolationLevel() == TransMode::IL_NOT_SPECIFIED_)
    outputShortLine(space, "ISOLATION LEVEL      : NOT SPECIFIED");

  if (CmpCommon::transMode()->getAccessMode() == TransMode::READ_ONLY_ ||
      CmpCommon::transMode()->getAccessMode() == TransMode::READ_ONLY_SPECIFIED_BY_USER_)
    outputShortLine(space, "ACCESS MODE          : READ ONLY");
  else if (CmpCommon::transMode()->getAccessMode() == TransMode::READ_WRITE_)
    outputShortLine(space, "ACCESS MODE          : READ WRITE");
  else if (CmpCommon::transMode()->getAccessMode() == TransMode::AM_NOT_SPECIFIED_)
    outputShortLine(space, "ACCESS MODE          : NOT SPECIFIED");

  if (CmpCommon::transMode()->getAutoCommit() == TransMode::ON_)
    outputShortLine(space, "AUTOCOMMIT           : ON");
  else if (CmpCommon::transMode()->getAutoCommit() == TransMode::OFF_)
    outputShortLine(space, "AUTOCOMMIT           : OFF");
  else if (CmpCommon::transMode()->getAutoCommit() == TransMode::AC_NOT_SPECIFIED_)
    outputShortLine(space, "AUTOCOMMIT           : NOT SPECIFIED");

  if (CmpCommon::transMode()->getRollbackMode() == TransMode::NO_ROLLBACK_)
    outputShortLine(space, "NO ROLLBACK          : ON");
  else if (CmpCommon::transMode()->getRollbackMode() == TransMode::ROLLBACK_MODE_WAITED_)
    outputShortLine(space, "NO ROLLBACK          : OFF");
  else
    outputShortLine(space, "NO ROLLBACK          : NOT SPECIFIED");

  sprintf(buf, "DIAGNOSTICS SIZE     : %d", CmpCommon::transMode()->getDiagAreaSize());
  outputLine(space, buf, 0);

  int val = CmpCommon::transMode()->getAutoAbortIntervalInSeconds();
  if (val == -2)  // user specified never abort setting
    val = 0;
  if (val == -3)  // user specified reset to TMFCOM setting
    val = -1;
  sprintf(buf, "AUTOABORT            : %d SECONDS", val);
  outputLine(space, buf, 0);

  if (CmpCommon::transMode()->getMultiCommit() == TransMode::MC_ON_)
    outputShortLine(space, "MULTI COMMIT         : ON");
  else if (CmpCommon::transMode()->getMultiCommit() == TransMode::MC_OFF_)
    outputShortLine(space, "MULTI COMMIT         : OFF");
  else if (CmpCommon::transMode()->getMultiCommit() == TransMode::MC_NOT_SPECIFIED_)
    outputShortLine(space, "MULTI COMMIT         : NOT SPECIFIED");

  if (CmpCommon::transMode()->getAutoBeginOn())
    outputShortLine(space, "AUTOBEGIN            : ON");
  else if (CmpCommon::transMode()->getAutoBeginOff())
    outputShortLine(space, "AUTOBEGIN            : OFF");
  else
    outputShortLine(space, "AUTOBEGIN            : NOT SPECIFIED");

  if (CmpCommon::transMode()->getAutoCommitSavepointOn())
    outputShortLine(space, "AUTOCOMMIT SAVEPOINT : ON");
  else if (CmpCommon::transMode()->getAutoCommitSavepointOff())
    outputShortLine(space, "AUTOCOMMIT SAVEPOINT : OFF");
  else
    outputShortLine(space, "AUTOCOMMIT SAVEPOINT : NOT SPECIFIED");

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  return 0;
}

static NAString CmpDescribe_ptiToInfCS(const NAString &inputInLatin1) {
  return Latin1StrToUTF8(inputInLatin1  // in - const NAString & latin1Str
                         ,
                         STMTHEAP  // in - NAMemory * heap
  );
}

// type:  1, invoke. 2, showddl. 3, create_like
short cmpDisplayColumn(const NAColumn *nac, char *inColName, const NAType *inNAT, short displayType, Space *inSpace,
                       char *buf, int &ii, NABoolean namesOnly, NABoolean &identityCol, NABoolean isExternalTable,
                       NABoolean isAlignedRowFormat, UInt32 columnLengthLimit,
                       NAList<const NAColumn *> *truncatedColumnList) {
  Space lSpace;

  Space *space;
  if (inSpace)
    space = inSpace;
  else
    space = &lSpace;

  identityCol = FALSE;

  NAString colName(inColName ? inColName : nac->getColName());
  NATable *naTable = (NATable *)nac->getNATable();

  NAString colFam;
  if ((nac->getNATable() && nac->getNATable()->isSQLMXAlignedTable()) ||
      (nac->getHbaseColFam() == SEABASE_DEFAULT_COL_FAMILY) || isExternalTable)
    colFam = "";
  else if (nac->getNATable() && nac->getNATable()->isSeabaseTable()) {
    const char *col_fam = NULL;
    if (nac->getNATable()->isHbaseMapTable()) {
      col_fam = nac->getHbaseColFam().data();
    } else {
      int index = 0;
      CmpSeabaseDDL::extractTrafColFam(nac->getHbaseColFam(), index);

      if (index >= naTable->allColFams().entries()) return -1;

      col_fam = naTable->allColFams()[index].data();
    }

    colFam = ANSI_ID(col_fam);

    colFam += ".";
  }

  if (displayType == 3) {
    NAString quotedColName = "\"";
    quotedColName += colName.data();
    quotedColName += "\"";
    sprintf(buf, "%s%-*s ", colFam.data(), CM_SIM_NAME_LEN, quotedColName.data());
  } else {
    sprintf(buf, "%s%-*s ", colFam.data(), CM_SIM_NAME_LEN - (int)colFam.length(), ANSI_ID(colName.data()));
  }

  if (namesOnly) {
    NAString colString(buf);
    Int32 j = ii;
    outputColumnLine(*space, colString, j);

    return 0;
  }

  const NAType *nat = (inNAT ? inNAT : nac->getType());

  NAString nas;
  nat->getMyTypeAsText(&nas, FALSE, TRUE);

  // if it is a character type and it is longer than the length
  // limit in bytes, then shorten the target type

  if ((nat->getTypeQualifier() == NA_CHARACTER_TYPE) && (!nat->isLob()) && (columnLengthLimit < UINT_MAX) &&
      (truncatedColumnList)) {
    const CharType *natc = (const CharType *)nat;
    if (natc->getDataStorageSize() > columnLengthLimit) {
      CharType *newType = (CharType *)natc->newCopy(NULL);
      newType->setDataStorageSize(columnLengthLimit);
      nas.clear();
      newType->getMyTypeAsText(&nas, FALSE);
      delete newType;
      truncatedColumnList->insert(nac);
    }
  }

  NAString attrStr;

  NAString defVal;
  if (nac->getDefaultClass() == COM_NO_DEFAULT)
    defVal = "NO DEFAULT";
  else if (nac->getDefaultClass() == COM_NULL_DEFAULT)
    defVal = "DEFAULT NULL";
  else if (nac->getDefaultClass() == COM_CURRENT_DEFAULT) {
    if (DFS2REC::isDateTime(nat->getFSDatatype())) {
      if (((DatetimeType *)nat)->getRecDateTimeCode() == REC_DTCODE_DATE)
        defVal = "DEFAULT CURRENT_DATE";
      else if (((DatetimeType *)nat)->getRecDateTimeCode() == REC_DTCODE_TIME)
        defVal = "DEFAULT CURRENT_TIME";
      else
        defVal = "DEFAULT CURRENT_TIMESTAMP";
    }
  } else if (nac->getDefaultClass() == COM_CURRENT_UT_DEFAULT)
    defVal = "DEFAULT CURRENT UNIXTIME";
  else if (nac->getDefaultClass() == COM_USER_FUNCTION_DEFAULT)
    defVal = "DEFAULT USER";
  else if (nac->getDefaultClass() == COM_UUID_DEFAULT)
    defVal = "DEFAULT UUID()";
  else if (nac->getDefaultClass() == COM_USER_DEFINED_DEFAULT ||
           nac->getDefaultClass() == COM_FUNCTION_DEFINED_DEFAULT) {
    defVal = "DEFAULT ";

    if (displayDefaultValue(nac->getDefaultValue(), colName.data(), defVal)) {
      return -1;
    }

  } else if ((nac->getDefaultClass() == COM_IDENTITY_GENERATED_BY_DEFAULT) ||
             (nac->getDefaultClass() == COM_IDENTITY_GENERATED_ALWAYS)) {
    if (nac->getDefaultClass() == COM_IDENTITY_GENERATED_BY_DEFAULT)
      defVal = "GENERATED BY DEFAULT AS IDENTITY";
    else
      defVal = "GENERATED ALWAYS AS IDENTITY";

    NAString idOptions;
    if ((displayType != 1) && (nac->getNATable()->getSGAttributes()))
      nac->getNATable()->getSGAttributes()->display(NULL, &idOptions, TRUE);

    if (NOT idOptions.isNull()) defVal += " (" + idOptions + " )";

    identityCol = TRUE;
  } else if (nac->getDefaultClass() == COM_CATALOG_FUNCTION_DEFAULT)
    defVal = "DEFAULT CURRENT CATALOG";
  else if (nac->getDefaultClass() == COM_SCHEMA_FUNCTION_DEFAULT)
    defVal = "DEFAULT CURRENT SCHEMA";
  else if (nac->getDefaultClass() == COM_SYS_GUID_FUNCTION_DEFAULT)
    defVal = "DEFAULT SYS_GUID()";
  else
    defVal = "NO DEFAULT";

  attrStr = defVal;

  if (nac->getHeading()) {
    NAString heading = "HEADING ";

    NAString externalHeading = "";
    ToQuotedString(externalHeading, nac->getHeading());
    heading += externalHeading;

    attrStr += " ";
    attrStr += heading;
  }

  if (NOT nat->supportsSQLnull()) {
    NAString nullVal = "NOT NULL NOT DROPPABLE";

    attrStr += " ";
    attrStr += nullVal;
  }

  char *sqlmxRegr = getenv("SQLMX_REGRESS");
  if ((!sqlmxRegr) || (displayType == 3)) {
    if ((NOT isAlignedRowFormat) && CmpSeabaseDDL::isSerialized(nac->getHbaseColFlags()))
      attrStr += " SERIALIZED";
    else if ((CmpCommon::getDefault(HBASE_SERIALIZATION) == DF_ON) || (displayType == 3))
      attrStr += " NOT SERIALIZED";
  }

  if (displayType != 3) {
    if (nac->isAlteredColumn()) {
      attrStr += " /*altered_col*/ ";
    } else if (nac->isAddedColumn()) {
      attrStr += " /*added_col*/ ";
    }
  }

  sprintf(&buf[strlen(buf)], "%s %s", nas.data(), attrStr.data());

  NAString colString(buf);
  Int32 j = ii;
  if (inSpace) outputColumnLine(*space, colString, j);

  return 0;
}

// type:  1, invoke. 2, showddl. 3, create_like
short cmpDisplayColumns(const NAColumnArray &naColArr, short displayType, Space &space, char *buf,
                        NABoolean displaySystemCols, NABoolean namesOnly, int &identityColPos,
                        NABoolean isExternalTable, NABoolean isAlignedRowFormat, NABoolean omitLobColumns = FALSE,
                        char *inColName = NULL,
                        short ada = 0,  // 0,add. 1,drop. 2,alter
                        const NAColumn *nacol = NULL, const NAType *natype = NULL, UInt32 columnLengthLimit = UINT_MAX,
                        NAList<const NAColumn *> *truncatedColumnList = NULL) {
  int ii = 0;
  identityColPos = -1;
  NABoolean identityCol = FALSE;
  for (Int32 i = 0; i < (Int32)naColArr.entries(); i++) {
    NAColumn *nac = naColArr[i];

    if ((NOT displaySystemCols) && (nac->isSystemColumn())) {
      continue;
    }

    if (omitLobColumns && (nac->getType()->isLob())) {
      continue;
    }

    const NAString &colName = nac->getColName();

    if ((inColName) && (ada == 1)) {
      if (colName == inColName)  // remove this column
        continue;
    }

    if ((inColName) && (colName == inColName) && (ada == 2) && (nacol) && (natype)) {
      if (cmpDisplayColumn(nac, NULL, natype, displayType, &space, buf, ii, namesOnly, identityCol, isExternalTable,
                           isAlignedRowFormat, columnLengthLimit, truncatedColumnList))
        return -1;
    } else {
      if (cmpDisplayColumn(nac, NULL, NULL, displayType, &space, buf, ii, namesOnly, identityCol, isExternalTable,
                           isAlignedRowFormat, columnLengthLimit, truncatedColumnList))
        return -1;
    }

    if (identityCol) identityColPos = i;

    ii++;
  }

  if ((inColName) && (ada == 0) && (nacol)) {
    if (cmpDisplayColumn(nacol, NULL, NULL, displayType, &space, buf, ii, namesOnly, identityCol, isExternalTable,
                         isAlignedRowFormat, columnLengthLimit, truncatedColumnList))
      return -1;
  }

  return 0;
}

short cmpDisplayPrimaryKey(const NAColumnArray &naColArr, int numKeys, NABoolean displaySystemCols, Space &space,
                           char *buf, NABoolean displayCompact, NABoolean displayAscDesc, NABoolean displayParens) {
  if (numKeys > 0) {
    if (displayParens) {
      if (displayCompact)
        sprintf(&buf[strlen(buf)], "(");
      else {
        outputShortLine(space, "  ( ");
      }
    }

    NABoolean isFirst = TRUE;
    Int32 j = -1;
    for (Int32 jj = 0; jj < numKeys; jj++) {
      NAColumn *nac = naColArr[jj];

      if ((NOT displaySystemCols) && (nac->isSystemColumn())) {
        continue;
      } else
        j++;  // increment num of columns displayed

      const NAString &keyName = nac->getColName();
      if (displayCompact) {
        sprintf(&buf[strlen(buf)], "%s%s%s", (NOT isFirst ? ", " : ""), ANSI_ID(keyName.data()),
                (displayAscDesc ? (!naColArr.isAscending(jj) ? " DESC" : " ASC") : " "));
      } else {
        sprintf(buf, "%s%s", ANSI_ID(keyName.data()),
                (displayAscDesc ? (!naColArr.isAscending(jj) ? " DESC" : " ASC") : " "));

        NAString colString(buf);
        outputColumnLine(space, colString, j);
      }

      isFirst = FALSE;
    }  // for

    if (displayParens) {
      if (displayCompact) {
        sprintf(&buf[strlen(buf)], ")");
        outputLine(space, buf, 2);
      } else {
        outputShortLine(space, "  )");
      }
    }
  }  // if

  return 0;
}

// doOutputLine: output in 'space'. Used later to display for invoke/showddl
// unnamed: created unnamed constraint
short CmpGenUniqueConstrStr(AbstractRIConstraint *ariConstr, Space *space, char *buf, NABoolean doOutputLine,
                            NABoolean unnamed, const CorrName *likeCorrName) {
  UniqueConstraint *uniqConstr = (UniqueConstraint *)ariConstr;

  if ((ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT) || (uniqConstr->isPrimaryKeyConstraint()) || (!space) ||
      (!buf))
    return -1;

  const NAString &ansiTableName = uniqConstr->getDefiningTableName().getQualifiedNameAsAnsiString(TRUE);

  const NAString &ansiConstrName = uniqConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);

  // get new unique name
  NAString newUniqueName = ansiConstrName;
  if (likeCorrName) {
    // fix "DELETE"_nnnnnnnn_nnnn to DELETE_nnnnnnnnn_nnnn should not use func getUnqualifiedObjectNameAsAnsiString()
    const NAString &likeObjectName = likeCorrName->getQualifiedNameObj().getObjectName();
    ComDeriveRandomInternalName(ComGetNameInterfaceCharSet(), likeObjectName, newUniqueName, STMTHEAP);
    newUniqueName = ToAnsiIdentifier(newUniqueName);
  }
  sprintf(buf, "%sALTER TABLE %s ADD %s %s UNIQUE ", (doOutputLine ? "\n" : ""),
          (likeCorrName != NULL) ? likeCorrName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data()
                                 : ansiTableName.data(),
          (unnamed ? "" : "CONSTRAINT"), (unnamed ? "" : newUniqueName.data()));
  if (doOutputLine)
    outputLine(*space, buf, 0);
  else
    strcat(buf, " ");

  NAColumnArray nacarr;

  for (int j = 0; j < uniqConstr->keyColumns().entries(); j++) {
    nacarr.insert(uniqConstr->keyColumns()[j]);
  }

  cmpDisplayPrimaryKey(nacarr, uniqConstr->keyColumns().entries(), FALSE, *space, &buf[strlen(buf)],

                       // if line is to be output, dont displayCompact
                       (doOutputLine ? FALSE : TRUE),

                       FALSE, TRUE);

  if (doOutputLine)
    outputShortLine(*space, ";");
  else
    strcat(buf, ";");

  return 0;
}

// doOutputLine: output in 'space'. Used later to display for invoke/showddl
// unnamed: created unnamed constraint
short CmpGenRefConstrStr(AbstractRIConstraint *ariConstr, Space *space, char *buf, NABoolean doOutputLine,
                         NABoolean unnamed, const CorrName *likeCorrName) {
  RefConstraint *refConstr = (RefConstraint *)ariConstr;

  if ((ariConstr->getOperatorType() != ITM_REF_CONSTRAINT) || (!space) || (!buf)) return -1;

  const ComplementaryRIConstraint &uniqueConstraintReferencedByMe = refConstr->getUniqueConstraintReferencedByMe();

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE);

  NATable *otherNaTable = NULL;
  CorrName otherCN(uniqueConstraintReferencedByMe.getTableName());
  otherNaTable = bindWA.getNATable(otherCN);
  if (otherNaTable == NULL || bindWA.errStatus()) return -1;

  const NAString &ansiTableName = refConstr->getDefiningTableName().getQualifiedNameAsAnsiString(TRUE);

  const NAString &ansiConstrName = refConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);

  NAString newRefName = ansiConstrName;
  if (likeCorrName) {
    // fix "DELETE"_nnnnnnnn_nnnn to DELETE_nnnnnnnnn_nnnn should not use func getUnqualifiedObjectNameAsAnsiString()
    const NAString &likeObjectName = likeCorrName->getQualifiedNameObj().getObjectName();
    ComDeriveRandomInternalName(ComGetNameInterfaceCharSet(), likeObjectName, newRefName, STMTHEAP);
    newRefName = ToAnsiIdentifier(newRefName);
  }
  sprintf(buf, "%sALTER TABLE %s ADD %s %s FOREIGN KEY ", (doOutputLine ? "\n" : ""),
          (likeCorrName != NULL) ? likeCorrName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data()
                                 : ansiTableName.data(),
          (unnamed ? "" : "CONSTRAINT"), (unnamed ? "" : newRefName.data()));
  if (doOutputLine)
    outputLine(*space, buf, 0);
  else
    strcat(buf, " ");

  NAColumnArray nacarr;

  for (int j = 0; j < refConstr->keyColumns().entries(); j++) {
    nacarr.insert(refConstr->keyColumns()[j]);
  }

  cmpDisplayPrimaryKey(nacarr, refConstr->keyColumns().entries(), FALSE, *space, &buf[strlen(buf)],

                       // if line is to be output, dont displayCompact
                       (doOutputLine ? FALSE : TRUE),

                       FALSE, TRUE);

  const NAString &ansiOtherTableName = uniqueConstraintReferencedByMe.getTableName().getQualifiedNameAsAnsiString(TRUE);
  if (doOutputLine) {
    sprintf(buf, " REFERENCES %s ", ansiOtherTableName.data());

    outputLine(*space, buf, 0);
  } else {
    strcat(buf, " REFERENCES ");
    strcat(buf, ansiOtherTableName.data());
    strcat(buf, " ");
  }

  AbstractRIConstraint *otherConstr =
      refConstr->findConstraint(&bindWA, refConstr->getUniqueConstraintReferencedByMe());

  NAColumnArray nacarr2;
  for (int j = 0; j < otherConstr->keyColumns().entries(); j++) {
    nacarr2.insert(otherConstr->keyColumns()[j]);
  }

  cmpDisplayPrimaryKey(nacarr2, otherConstr->keyColumns().entries(), FALSE, *space, &buf[strlen(buf)],

                       // if line is to be output, dont displayCompact
                       (doOutputLine ? FALSE : TRUE),

                       FALSE, TRUE);

  if (NOT refConstr->getIsEnforced()) {
    if (doOutputLine)
      outputShortLine(*space, " NOT ENFORCED ");
    else
      strcat(buf, " NOT ENFORCED ");
  }

  if (doOutputLine)
    outputShortLine(*space, ";");
  else
    strcat(buf, ";");

  return 0;
}

// type:  1, invoke. 2, showddl. 3, create_like
short CmpDescribeSeabaseTable(const CorrName &dtName, short type, char *&outbuf, ULng32 &outbuflen, CollHeap *heap,
                              NABoolean isCreateForPart, const char *pkeyName, const char *pkeyStr,
                              NABoolean withPartns, NABoolean withoutSalt, NABoolean withoutDivisioning,
                              NABoolean withoutRowFormat, NABoolean withoutLobColumns, NABoolean withoutNamespace,
                              NABoolean withoutRegionReplication, NABoolean withoutIncrBackup, UInt32 columnLengthLimit,
                              NABoolean noTrailingSemi, NABoolean noPrivs, char *colName, short ada,
                              const NAColumn *nacol, const NAType *natype, Space *inSpace, NABoolean isDetail,
                              CorrName *likeTabName, short subType) {
  if (dtName.isPartitionV2() && dtName.getSpecialType() == ExtendedQualName::INDEX_TABLE) {
    int cliRC = 0;
    NAString partitionIndexName;
    ExeCliInterface cliInterface(CmpCommon::statementHeap());
    cliRC = CmpSeabaseDDL::getPartIndexName(&cliInterface, dtName.getQualifiedNameObj().getSchemaName(),
                                            dtName.getQualifiedNameObj().getObjectName(),
                                            dtName.getPartnClause().getPartitionName(), partitionIndexName);

    if (cliRC < 0) {
      return -1;
    }

    const_cast<CorrName &>(dtName) =
        CorrName(partitionIndexName, CmpCommon::statementHeap(), dtName.getQualifiedNameObj().getSchemaName(),
                 dtName.getQualifiedNameObj().getCatalogName());

    const_cast<CorrName &>(dtName).setSpecialType(ExtendedQualName::INDEX_TABLE);
  }

  const NAString &tableName = dtName.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE);

  const NAString &objectName = dtName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString();

  NABoolean bCreateTableLikeOnlyTable = (type == 3 && subType == 0);
  NABoolean bCreateTableLikeGetIndexDesc = (type == 3 && subType == 1);
  NABoolean bCreateTableLikeGetConstraintDesc = (type == 3 && subType == 2);

  if (CmpCommon::context()->isUninitializedSeabase()) {
    *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum());

    return -1;
  }

  // set isExternalTable to allow Hive External tables to be described
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);

  NATable *naTable = bindWA.getNATableInternal((CorrName &)dtName);

  if (naTable == NULL || bindWA.errStatus()) return -1;

  // for debugging
  if (type == 2 && naTable->isPartitionEntityTable()) naTable->displayPartitonV2();

  TableDesc *tdesc = bindWA.createTableDesc(naTable, (CorrName &)dtName);
  if (bindWA.errStatus()) return -1;

  if ((naTable->getTableType() != ExtendedQualName::VIRTUAL_TABLE) && (NOT naTable->isHbaseTable())) {
    if (CmpCommon::diags()->getNumber() == 0) *CmpCommon::diags() << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
    return -1;
  }

  NABoolean isVolatile = naTable->isVolatileTable();
  NABoolean isPartitionV2Table = naTable->isPartitionV2Table();
  NABoolean isExternalTable = naTable->isTrafExternalTable();
  NABoolean isImplicitExternalTable = naTable->isImplicitTrafExternalTable();
  ComStorageType storageType = naTable->storageType();
  NABoolean isHbaseMapTable = naTable->isHbaseMapTable();
  NABoolean isHbaseCellOrRowTable = (naTable->isHbaseCellTable() || naTable->isHbaseRowTable());

  NABoolean isExternalHbaseTable = FALSE;
  NABoolean isExternalHiveTable = FALSE;
  NAString extName;
  if (isExternalTable) {
    extName = ComConvertTrafNameToNativeName(dtName.getQualifiedNameObj().getCatalogName(),
                                             dtName.getQualifiedNameObj().getUnqualifiedSchemaNameAsAnsiString(),
                                             dtName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString());

    QualifiedName qn(extName, 3);
    if (qn.getCatalogName() == HBASE_SYSTEM_CATALOG)
      isExternalHbaseTable = TRUE;
    else if (qn.getCatalogName() == HIVE_SYSTEM_CATALOG)
      isExternalHiveTable = TRUE;
  }

  char *buf = new (heap) char[15000];
  CMPASSERT(buf);

  time_t tp;
  time(&tp);

  Space lSpace;

  Space *space;
  if (inSpace)
    space = inSpace;
  else
    space = &lSpace;

  char *sqlmxRegr = getenv("SQLMX_REGRESS");

  NABoolean displayPrivilegeGrants = TRUE;
  if (((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && sqlmxRegr) ||
      (CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF) || (noPrivs))
    displayPrivilegeGrants = FALSE;

  // display syscols for invoke if not running regrs
  //
  NABoolean displaySystemCols = (type == 1);

  NABoolean isView = (naTable->getViewText() ? TRUE : FALSE);

  // emit an initial newline
  outputShortLine(*space, " ");

  // Used for context switches
  CmpSeabaseDDL cmpSBD((NAHeap *)heap);

  // Verify that user can perform the describe command
  // No need to check privileges for create like operations (type 3)
  // since the create code performs authorization checks
  // Nor for hiveExternal tables - already checked
  if (!isExternalHiveTable) {
    if (type != 3) {
      // For native HBase tables that have no objectUID, we can't check
      // user privileges so only allow operation for privileged users
      if (isHbaseCellOrRowTable && !ComUser::isRootUserID() && !ComUser::currentUserHasRole(HBASE_ROLE_ID) &&
          naTable->objectUid().get_value() == 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
        return -1;
      }

      PrivMgrUserPrivs privs;
      PrivMgrUserPrivs *pPrivInfo = NULL;

      // metadata tables do not cache privilege information, go get it now
      if (CmpCommon::context()->isAuthorizationEnabled() && naTable->getPrivInfo() == NULL) {
        std::string privMDLoc(ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG));
        privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");
        PrivMgrCommands privInterface(privMDLoc, CmpCommon::diags(), PrivMgr::PRIV_INITIALIZED);

        // we should switch to another CI only if we are in an embedded CI
        if (cmpSBD.switchCompiler()) {
          *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
          return -1;
        }

        PrivStatus retcode = privInterface.getPrivileges((int64_t)naTable->objectUid().get_value(),
                                                         naTable->getObjectType(), ComUser::getCurrentUser(), privs);

        // switch back the original commpiler, ignore error for now
        cmpSBD.switchBackCompiler();

        if (retcode == STATUS_ERROR) {
          *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
          return -1;
        }
        pPrivInfo = &privs;
      } else
        pPrivInfo = naTable->getPrivInfo();

      // Allow object owners to perform showddl operation
      if ((naTable->getOwner() != ComUser::getCurrentUser()) && !ComUser::currentUserHasRole(naTable->getOwner())) {
        if (!CmpDescribeIsAuthorized(heap, SQLOperation::UNKNOWN, pPrivInfo, COM_BASE_TABLE_OBJECT,
                                     naTable->getOwner()))
          return -1;
      }
    }
  }

  long objectUID = (long)naTable->objectUid().get_value();
  long objectDataUID = (long)naTable->objDataUID().get_value();

  if ((type == 2) && (isView)) {
    NAString viewtext(naTable->getViewText());

    viewtext = viewtext.strip(NAString::trailing, ';');
    viewtext += " ;";

    outputLongLine(*space, viewtext, 0);

    // display comment for VIEW
    if (objectUID > 0) {
      if (cmpSBD.switchCompiler()) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
        return -1;
      }

      ComTdbVirtObjCommentInfo objCommentInfo;
      if (cmpSBD.getSeabaseObjectComment(objectUID, COM_VIEW_OBJECT, objCommentInfo, heap)) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
        cmpSBD.switchBackCompiler();
        return -1;
      }

      // display VIEW COMMENT statements
      if (objCommentInfo.objectComment != NULL) {
        // new line
        outputLine(*space, "", 0);

        sprintf(buf, "COMMENT ON VIEW %s IS '%s' ;", tableName.data(), objCommentInfo.objectComment);
        outputLine(*space, buf, 0);
      }

      if (objCommentInfo.numColumnComment > 0 && objCommentInfo.columnCommentArray != NULL) {
        // display Column COMMENT statements
        outputLine(*space, "", 0);
        for (int idx = 0; idx < objCommentInfo.numColumnComment; idx++) {
          sprintf(buf, "COMMENT ON COLUMN %s.%s IS '%s' ;", tableName.data(),
                  objCommentInfo.columnCommentArray[idx].columnName,
                  objCommentInfo.columnCommentArray[idx].columnComment);
          outputLine(*space, buf, 0);
        }
      }

      // do a comment info memory clean
      NADELETEARRAY(objCommentInfo.columnCommentArray, objCommentInfo.numColumnComment, ComTdbVirtColumnCommentInfo,
                    heap);

      cmpSBD.switchBackCompiler();
    }

    // Display grant statements
    if (CmpCommon::context()->isAuthorizationEnabled() && displayPrivilegeGrants) {
      std::string privMDLoc(ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG));
      privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");
      PrivMgrCommands privInterface(privMDLoc, CmpCommon::diags(), PrivMgr::PRIV_INITIALIZED);
      std::string privilegeText;
      PrivMgrObjectInfo objectInfo(naTable);
      if (cmpSBD.switchCompiler()) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
        return -1;
      }

      if (privInterface.describePrivileges(objectInfo, privilegeText)) {
        outputShortLine(*space, " ");
        outputLine(*space, privilegeText.c_str(), 0);
      }

      cmpSBD.switchBackCompiler();
    }

    outbuflen = space->getAllocatedSpaceSize();
    outbuf = new (heap) char[outbuflen];
    space->makeContiguous(outbuf, outbuflen);

    NADELETEBASIC(buf, heap);

    return 0;
  }

  if (type == 1) {
    NAString extNameForHbase = CmpSeabaseDDL::genHBaseObjName(
        dtName.getQualifiedNameObj().getCatalogName(), dtName.getQualifiedNameObj().getSchemaName(),
        dtName.getQualifiedNameObj().getObjectName(), naTable->getNamespace(), objectDataUID);

    if (isView)
      sprintf(buf,
              "-- Definition of Trafodion view %s\n"
              "-- Definition current  %s",
              tableName.data(), ctime(&tp));
    else
      sprintf(
          buf,
          "-- Definition of %s%stable %s\n"
          "-- %sDefinition of hbase table %s\n"
          "-- Definition current  %s",
          ((storageType == COM_STORAGE_MONARCH) ? "Monarch"
                                                : ((storageType == COM_STORAGE_BIGTABLE) ? "Bigtable" : "Trafodion")),
          (isVolatile ? " volatile " : (isHbaseMapTable ? " HBase mapped " : (isExternalTable ? " external " : " "))),
          (isHbaseMapTable ? objectName.data() : tableName.data()), (isPartitionV2Table ? "NO " : ""),
          (isPartitionV2Table ? "" : extNameForHbase.data()), ctime(&tp));
    outputShortLine(*space, buf);
  } else if (type == 2) {
    if (isHbaseCellOrRowTable) outputShortLine(*space, "/*");

    NAString tabType;
    if (isVolatile)
      tabType = " VOLATILE ";
    else if (isExternalTable) {
      if (isImplicitExternalTable)
        tabType = " /*IMPLICIT*/ EXTERNAL ";
      else
        tabType = " EXTERNAL ";
    } else
      tabType = " ";

    sprintf(buf, "CREATE%sTABLE %s", tabType.data(), (isExternalTable ? objectName.data() : tableName.data()));
    outputShortLine(*space, buf);
  }

  int identityColPos = -1;
  NABoolean closeParan = FALSE;
  NAList<const NAColumn *> truncatedColumnList(heap);
  if ((NOT isExternalTable && (subType != 1 && subType != 2)) ||
      ((isExternalTable) &&
       ((isExternalHbaseTable && ((type == 1) || bCreateTableLikeOnlyTable)) ||
        (isExternalHbaseTable && naTable->isHbaseMapTable() && (type == 2)) || (isExternalHiveTable && (type != 2)) ||
        (isExternalHiveTable && (type == 2) && (naTable->hiveExtColAttrs()))))) {
    outputShortLine(*space, "  ( ");
    cmpDisplayColumns(naTable->getNAColumnArray(), type, *space, buf, displaySystemCols, FALSE, identityColPos,
                      (isExternalTable && (NOT isHbaseMapTable)), naTable->isSQLMXAlignedTable(), withoutLobColumns,
                      colName, ada, nacol, natype, columnLengthLimit, &truncatedColumnList);
    closeParan = TRUE;
  }

  Int32 nonSystemKeyCols = 0;
  NABoolean isStoreBy = FALSE;
  NABoolean forceStoreBy = FALSE;
  NABoolean isSalted = FALSE;
  NABoolean isDivisioned = FALSE;
  NABoolean isTrafReplica = FALSE;
  Int16 numTrafReplicas = 0;
  ItemExpr *saltExpr = NULL;
  LIST(NAString) divisioningExprs(heap);
  LIST(NABoolean) divisioningExprAscOrders(heap);

  if (naTable->getClusteringIndex()) {
    NAFileSet *naf = naTable->getClusteringIndex();
    for (int i = 0; i < naf->getIndexKeyColumns().entries(); i++) {
      NAColumn *nac = naf->getIndexKeyColumns()[i];
      if (nac->isComputedColumnAlways()) {
        if (nac->isSaltColumn() && !withoutSalt) {
          isSalted = TRUE;
          ItemExpr *saltBaseCol = tdesc->getColumnList()[nac->getPosition()].getItemExpr();
          CMPASSERT(saltBaseCol->getOperatorType() == ITM_BASECOLUMN);
          saltExpr = ((BaseColumn *)saltBaseCol)->getComputedColumnExpr().getItemExpr();
        } else if (nac->isDivisioningColumn() && !withoutDivisioning) {
          NABoolean divColIsAsc = naf->getIndexKeyColumns().isAscending(i);
          // any other case of computed column is treated as divisioning for now
          isDivisioned = TRUE;
          divisioningExprs.insert(NAString(nac->getComputedColumnExprString()));
          divisioningExprAscOrders.insert(divColIsAsc);
        }
      }
      if (nac->isReplicaColumn()) {
        isTrafReplica = TRUE;
        numTrafReplicas = naf->numTrafReplicas();
      }
      if (NOT nac->isSystemColumn())
        nonSystemKeyCols++;
      else if (nac->isSyskeyColumn())
        isStoreBy = TRUE;

      // if we are shortening a column, set isStoreBy to TRUE since truncating
      // a character string may cause formerly distinct values to become equal
      // (that is, we want to change PRIMARY KEY to STORE BY)
      for (int j = 0; j < truncatedColumnList.entries(); j++) {
        const NAColumn *truncatedColumn = truncatedColumnList[j];
        if (nac->getColName() == truncatedColumn->getColName()) {
          isStoreBy = TRUE;
          forceStoreBy = TRUE;
        }
      }
    }
  }

  if ((nonSystemKeyCols == 0) && (!forceStoreBy)) isStoreBy = FALSE;

  if (type == 1) {
    outputShortLine(*space, "  )");
    // invoke partition info
    if (isPartitionV2Table && !isCreateForPart) {
      CmpDescribeTablePartitions(naTable, *space, buf, type);
    }
  }

  int numBTpkeys = 0;

  NAFileSet *naf = naTable->getClusteringIndex();
  NABoolean isAudited = (naf ? naf->isAudited() : TRUE);

  NABoolean isAligned = naTable->isSQLMXAlignedTable();
  ComReplType xnRepl = naTable->xnRepl();

  NABoolean encrypt = naTable->useEncryption();
  NABoolean storedDesc = naTable->isStoredDesc();

  NABoolean incrBackupEnabled = naTable->incrBackupEnabled();
  NABoolean readOnlyEnabled = naTable->readOnlyEnabled();
  if (withoutIncrBackup) incrBackupEnabled = FALSE;
  NABoolean nullablePkey = (naf ? naf->isNullablePkey() : FALSE);

  if (bCreateTableLikeOnlyTable && (pkeyStr)) {
    if (pkeyName) {
      NAString pkeyPrefix(", CONSTRAINT ");
      pkeyPrefix += NAString(pkeyName) + " PRIMARY KEY ";
      outputLine(*space, pkeyPrefix.data(), 0);
    } else {
      outputShortLine(*space, " , PRIMARY KEY ");
    }

    outputLine(*space, pkeyStr, 2);
  } else {
    if ((naf) && (nonSystemKeyCols > 0) && (NOT isStoreBy) && subType != 1 && subType != 2) {
      NAString pkeyConstrName;
      NAString pkeyConstrObjectName;
      if (type == 2)  // showddl
      {
        const AbstractRIConstraintList &uniqueList = naTable->getUniqueConstraints();

        for (Int32 i = 0; i < uniqueList.entries(); i++) {
          AbstractRIConstraint *ariConstr = uniqueList[i];

          UniqueConstraint *uniqConstr = (UniqueConstraint *)ariConstr;
          if (uniqConstr->isPrimaryKeyConstraint()) {
            pkeyConstrName = uniqConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);
            pkeyConstrObjectName = uniqConstr->getConstraintName().getObjectName();
            break;
          }
        }  // for
      }    // type 2

      numBTpkeys = naf->getIndexKeyColumns().entries();

      if (type == 1)
        sprintf(buf, "  PRIMARY KEY %s", (nullablePkey ? "NULLABLE " : ""));
      else {
        // Display primary key name for showddl (type == 2).
        // First check to see if pkey name is a generated random name or
        // a user specified name.
        //
        // If name is in new Traf_UnNamed_Constr format, dont display.
        //
        // Otherwise,
        // If it is a generated random name, then dont display it.
        // This is being done for backward compatibility in showddl
        // output as well as avoid the need to update multiple
        // regressions files.
        // Currently we check to see if the name has random generated
        // format to determine whether to display it or not.
        // If it so happens that a user specified primary key constraint
        // has that exact format, then it will not be displayed.
        // At some point in future, we can store in metadata if pkey
        // name was internally generated or user specified.
        //
        NABoolean dontDisplay = FALSE;
        if ((type == 3) || (pkeyConstrObjectName.isNull()))
          dontDisplay = TRUE;
        else if ((pkeyConstrObjectName.length() >= (strlen(TRAF_UNNAMED_CONSTR_PREFIX))) &&
                 (pkeyConstrObjectName.index(TRAF_UNNAMED_CONSTR_PREFIX) == 0))
          // new format unnamed constr
          dontDisplay = TRUE;
        else if (ComIsRandomInternalName(pkeyConstrObjectName))
          // old format unnamed constr
          dontDisplay = TRUE;

        if (dontDisplay)
          sprintf(buf, "  , PRIMARY KEY %s", (nullablePkey ? "NULLABLE " : ""));
        else
          sprintf(buf, " , CONSTRAINT %s PRIMARY KEY %s", pkeyConstrName.data(), (nullablePkey ? "NULLABLE " : ""));
      }

      // if all primary key columns are 'not serialized primary key',
      // then display that.
      NABoolean serialized = FALSE;
      for (Int32 jj = 0; ((NOT serialized) && (jj < naf->getIndexKeyColumns().entries())); jj++) {
        NAColumn *nac = (naf->getIndexKeyColumns())[jj];
        if (NOT nac->isPrimaryKeyNotSerialized()) serialized = TRUE;
      }

      if (((type == 1) || (type == 2)) && (NOT serialized)) {
        strcat(&buf[strlen(buf)], "NOT SERIALIZED ");
      }

      cmpDisplayPrimaryKey(naf->getIndexKeyColumns(), naf->getIndexKeyColumns().entries(), displaySystemCols, *space,
                           buf, TRUE, TRUE, TRUE);
    }  // if
  }

  if (type != 1 && subType != 1 && subType != 2) {
    if (closeParan) outputShortLine(*space, "  )");

    // create table like
    if (isPartitionV2Table && !isCreateForPart) {
      CmpDescribeTablePartitions(naTable, *space, buf, type);
    }
    if (isStoreBy) {
      sprintf(buf, "  STORE BY %s", (nullablePkey ? "NULLABLE " : ""));
      cmpDisplayPrimaryKey(naf->getIndexKeyColumns(), naf->getIndexKeyColumns().entries(), displaySystemCols, *space,
                           buf, TRUE, TRUE, TRUE);
    }

    if ((isSalted) && !withoutSalt) {
      int currRegions = naf->getCountOfPartitions();
      int numSaltPartitions = naf->numSaltPartns();
      int numInitialSaltRegions = naf->numInitialSaltRegions();
      NAString saltString;

      if (numSaltPartitions == numInitialSaltRegions)
        sprintf(buf, "  SALT USING %d PARTITIONS", numSaltPartitions);
      else
        sprintf(buf, "  SALT USING %d PARTITIONS IN %d REGIONS", numSaltPartitions, numInitialSaltRegions);
      saltString += buf;

      if (numInitialSaltRegions != currRegions) {
        sprintf(buf, " /* ACTUAL REGIONS %d */", currRegions);
        saltString += buf;
      }

      outputShortLine(*space, saltString.data());

      ValueIdList saltCols;

      // get leaf nodes in left-to-right order from the salt expression,
      // those are the salt columns
      saltExpr->getLeafValueIds(saltCols);

      // remove any nodes that are not base columns (e.g. # of partns)
      CollIndex i = 0;
      while (i < saltCols.entries())
        if (saltCols[i].getItemExpr()->getOperatorType() == ITM_BASECOLUMN)
          i++;
        else
          saltCols.removeAt(i);

      // print a list of salt columns if they are a subset of the
      // (non-system) clustering key columns or if they appear
      // in a different order
      NABoolean printSaltCols = (saltCols.entries() < nonSystemKeyCols);

      if (!printSaltCols)
        for (CollIndex j = 0; j < saltCols.entries(); j++) {
          BaseColumn *bc = (BaseColumn *)saltCols[j].getItemExpr();
          // compare col # with clustering key col #,
          // but skip leading salt col in clustering key
          if (bc->getColNumber() != naf->getIndexKeyColumns()[j + 1]->getPosition()) printSaltCols = TRUE;
        }

      if (printSaltCols) {
        NAString sc = "       ON (";
        for (CollIndex k = 0; k < saltCols.entries(); k++) {
          BaseColumn *bc = (BaseColumn *)saltCols[k].getItemExpr();
          if (k > 0) sc += ", ";
          sc += ANSI_ID(bc->getColName().data());
        }
        sc += ")";
        outputShortLine(*space, sc.data());
      }
    }
    if (isTrafReplica) {
      sprintf(buf, "  REPLICATE %d WAYS", numTrafReplicas);
      outputShortLine(*space, buf);
    }
    if ((NOT isSalted) && (NOT isTrafReplica) && (withPartns)) {
      int currPartitions = naf->getCountOfPartitions();
      ExeCliInterface cliInterface(CmpCommon::statementHeap());
      NAString splitText;

      if (CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic(), &cliInterface,
                                       naTable->objectUid().get_value(), COM_HBASE_SPLIT_TEXT, 0, splitText))
        return -1;

      if (!splitText.isNull()) outputShortLine(*space, splitText.data());

      if (currPartitions > 1) {
        sprintf(buf, "  /* ACTUAL REGIONS %d */", currPartitions);

        outputShortLine(*space, buf);
      }
    }

    if (isDivisioned && !withoutDivisioning) {
      NAString divByClause = "  DIVISION BY (";

      for (CollIndex d = 0; d < divisioningExprs.entries(); d++) {
        if (d > 0) divByClause += ", ";
        divByClause += divisioningExprs[d];
        if (!divisioningExprAscOrders[d]) divByClause += " DESC";
      }
      outputShortLine(*space, divByClause.data());
      divByClause = "     NAMED AS (";

      NAFileSet *naf = naTable->getClusteringIndex();
      NABoolean firstDivCol = TRUE;

      for (int i = 0; i < naf->getIndexKeyColumns().entries(); i++) {
        NAColumn *nac = naf->getIndexKeyColumns()[i];
        if (nac->isDivisioningColumn()) {
          if (firstDivCol)
            firstDivCol = FALSE;
          else
            divByClause += ", ";
          divByClause += "\"";
          divByClause += nac->getColName();
          divByClause += "\"";
        }
      }

      divByClause += "))";
      outputShortLine(*space, divByClause.data());
    }

    NABoolean attributesSet = FALSE;
    NABoolean formatSet = FALSE;
    char attrs[2000];
    if ((bCreateTableLikeOnlyTable /*create like*/) && (NOT withoutRowFormat)) {
      strcpy(attrs, " ATTRIBUTES ");
      if (isAligned)
        strcat(attrs, "ALIGNED FORMAT ");
      else
        strcat(attrs, "HBASE FORMAT ");
      attributesSet = TRUE;
      formatSet = TRUE;
    }

    if (((NOT isAudited) || (isAligned)) || encrypt || storedDesc ||
        ((sqlmxRegr) && bCreateTableLikeOnlyTable && ((NOT isAudited) || (isAligned))) ||
        ((NOT naTable->defaultUserColFam().isNull()) && (naTable->defaultUserColFam() != SEABASE_DEFAULT_COL_FAMILY)) ||
        ((type == 2) && (NOT naTable->getNamespace().isNull()))) {
      if (NOT attributesSet) strcpy(attrs, " ATTRIBUTES ");

      if (NOT isAudited) strcat(attrs, "NO AUDIT ");
      if ((NOT formatSet) && isAligned) strcat(attrs, "ALIGNED FORMAT ");
      if ((NOT naTable->defaultUserColFam().isNull()) && (naTable->defaultUserColFam() != SEABASE_DEFAULT_COL_FAMILY)) {
        strcat(attrs, "DEFAULT COLUMN FAMILY '");
        strcat(attrs, naTable->defaultUserColFam());
        strcat(attrs, "' ");
      }

      if ((NOT naTable->getNamespace().isNull()) && (NOT withoutNamespace)) {
        strcat(attrs, "NAMESPACE '");
        strcat(attrs, naTable->getNamespace().data());
        strcat(attrs, "' ");
      }

      if (encrypt) {
        sprintf(&attrs[strlen(attrs)], "USE ENCRYPTION OPTIONS '%s%s' ", (naTable->useRowIdEncryption() ? "r" : ""),
                (naTable->useDataEncryption() ? "d" : ""));
      }

      if ((type == 2 || type == 3) && (NOT sqlmxRegr) && (storedDesc)) {
        sprintf(&attrs[strlen(attrs)], "STORED DESC ");
      }

      attributesSet = TRUE;
    }

    if (xnRepl != COM_REPL_NONE) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }

      if (xnRepl == COM_REPL_SYNC)
        strcat(attrs, "SYNCHRONOUS REPLICATION ");
      else if (xnRepl == COM_REPL_ASYNC)
        strcat(attrs, "ASYNCHRONOUS REPLICATION ");
    }

    if (storageType == COM_STORAGE_MONARCH) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }
      strcat(attrs, "STORAGE MONARCH ");
    }

    if (storageType == COM_STORAGE_BIGTABLE) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }
      strcat(attrs, "STORAGE BIGTABLE ");
    }

    if ((incrBackupEnabled) && ((type == 2) || bCreateTableLikeOnlyTable)) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }

      strcat(attrs, "INCREMENTAL BACKUP ");
    } else if (!incrBackupEnabled && bCreateTableLikeOnlyTable) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }

      strcat(attrs, "NO INCREMENTAL BACKUP ");
    }

    if (readOnlyEnabled && (type == 2 || type == 3)) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }
      strcat(attrs, "READ_ONLY ");
    } else if (!readOnlyEnabled && type == 3) {
      if (NOT attributesSet) {
        strcpy(attrs, " ATTRIBUTES ");
        attributesSet = TRUE;
      }
      strcat(attrs, "NOT READ_ONLY ");
    }

    if (attributesSet) outputShortLine(*space, attrs);

    NAList<HbaseCreateOption *> *hbaseCreateOptions = NULL;

    // MDTables use hard coded definitions that may not have all the hbase
    // options.  Retrieve hbase options from the text table.
    if (naTable->isSeabaseMDTable() || naTable->isSeabasePrivSchemaTable()) {
      // Ignore any errors trying to retrieve hbase options.
      NAString hbaseObjectStr;
      ExeCliInterface cliInterface(CmpCommon::statementHeap());
      CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic(), &cliInterface,
                                   naTable->objectUid().get_value(), COM_HBASE_OPTIONS_TEXT, 0, hbaseObjectStr);

      CmpSeabaseDDL::genHbaseCreateOptions(hbaseObjectStr.data(), hbaseCreateOptions, STMTHEAP, NULL, NULL);
    } else if (!isView)
      hbaseCreateOptions = naTable->hbaseCreateOptions();

    if (!isView && hbaseCreateOptions && (hbaseCreateOptions->entries() > 0)) {
      NABoolean bSkipHbaseOptions = FALSE;
      if (withoutRegionReplication &&
          hbaseCreateOptions->entries() ==
              1) {  // If we have only one hbase option and the option is REGION_REPLICATION,
                    // if we specified without region replication, skip creating hbase options.
        HbaseCreateOption *hco = (*hbaseCreateOptions)[0];
        if (hco->key() == "REGION_REPLICATION") bSkipHbaseOptions = TRUE;
      }

      if (!bSkipHbaseOptions) {
        outputShortLine(*space, "  HBASE_OPTIONS ");
        outputShortLine(*space, "  ( ");

        for (int i = 0; i < hbaseCreateOptions->entries(); i++) {
          HbaseCreateOption *hco = (*hbaseCreateOptions)[i];
          if (withoutRegionReplication && hco->key() == "REGION_REPLICATION") {
            continue;
          }
          NABoolean comma = FALSE;
          if (i < hbaseCreateOptions->entries() - 1) {
            HbaseCreateOption *tmp = (*hbaseCreateOptions)[i + 1];
            // If REGION_REPLICATION is the last options and withoutRegionReplication is true,
            // don't need add comma
            if (!((i + 1 == hbaseCreateOptions->entries() - 1) && withoutRegionReplication && tmp &&
                  tmp->key() == "REGION_REPLICATION"))
              comma = TRUE;
          }
          sprintf(buf, "    %s = '%s'%s", hco->key().data(), hco->val().data(), (comma ? "," : " "));
          outputShortLine(*space, buf);
        }

        outputShortLine(*space, "  ) ");
      }
    }

    if ((isExternalTable) && (NOT isHbaseMapTable) && (type == 2)) {
      sprintf(buf, "  FOR %s", extName.data());
      outputShortLine(*space, buf);
    }

    if ((isHbaseMapTable) && (type != 3)) {
      NAString objNameWithNamespace;
      objNameWithNamespace += dtName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString();

      sprintf(buf, "  MAP TO HBASE TABLE %s DATA FORMAT %s", objNameWithNamespace.data(),
              (naTable->isHbaseDataFormatString() ? "VARCHAR" : "NATIVE"));
      outputShortLine(*space, buf);
    }

    if (NOT noTrailingSemi) outputShortLine(*space, ";");

    if ((type == 2) && isHbaseCellOrRowTable) outputShortLine(*space, "*/");
  }

  // showddl internal sequences created for identity cols
  if ((identityColPos >= 0) && (type == 2) && (NOT sqlmxRegr)) {
    NAString seqName;
    SequenceGeneratorAttributes::genSequenceName(dtName.getQualifiedNameObj().getCatalogName(),
                                                 dtName.getQualifiedNameObj().getSchemaName(),
                                                 dtName.getQualifiedNameObj().getObjectName(),
                                                 naTable->getNAColumnArray()[identityColPos]->getColName(), seqName);

    CorrName csn(seqName, STMTHEAP, dtName.getQualifiedNameObj().getSchemaName(),
                 dtName.getQualifiedNameObj().getCatalogName());
    outputLine(*space, "\n-- The following sequence is a system created sequence --", 0);

    char *dummyBuf;
    ULng32 dummyLen;
    CmpDescribeSequence(csn, dummyBuf, dummyLen, STMTHEAP, &*space, TRUE);
  }

  if (((type == 1) && (NOT sqlmxRegr)) || (type == 2) || bCreateTableLikeGetIndexDesc ||
      bCreateTableLikeGetConstraintDesc) {
    const NAFileSetList &indexList = naTable->getIndexList();

    if (subType != 2)  // not constraint
    {
      for (Int32 i = 0; i < indexList.entries(); i++) {
        const NAFileSet *naf = indexList[i];
        isAligned = naf->isSqlmxAlignedRowFormat();
        if (naf->getKeytag() == 0) continue;

        const QualifiedName &qn = naf->getFileSetName();
        const NAString &indexName = qn.getUnqualifiedObjectNameAsAnsiString();
        if (type == 1) {
          NAString extNameForHbase = CmpSeabaseDDL::genHBaseObjName(
              dtName.getQualifiedNameObj().getCatalogName(), dtName.getQualifiedNameObj().getSchemaName(), indexName,
              naTable->getNamespace(), naf->getIndexUID());

          char vu[40];
          strcpy(vu, " ");
          if (isVolatile) strcat(vu, "volatile ");
          if (naf->uniqueIndex()) strcat(vu, "unique ");
          // ngram index
          if (naf->ngramIndex()) strcat(vu, "ngram ");

          sprintf(buf,
                  "\n-- Definition of%s%s%sindex %s\n"
                  "-- %sDefinition of hbase table %s\n"
                  "-- Definition current  %s",
                  ((NOT naf->isCreatedExplicitly()) ? " implicit " : " "),
                  ((storageType == COM_STORAGE_MONARCH)
                       ? "Monarch"
                       : ((storageType == COM_STORAGE_BIGTABLE) ? "Bigtable" : "Trafodion")),
                  vu, indexName.data(), (isPartitionV2Table ? "NO " : ""),
                  (isPartitionV2Table ? "" : extNameForHbase.data()), ctime(&tp));
          outputShortLine(*space, buf);
        } else {
          char vu[40];
          strcpy(vu, " ");
          if (naf->partLocalBaseIndex() && isCreateForPart && type == 3) strcat(vu, "PARTITION LOCAL ");
          if (isVolatile) strcat(vu, "VOLATILE ");
          if (naf->uniqueIndex()) strcat(vu, "UNIQUE ");
          // ngram index
          if (naf->ngramIndex()) strcat(vu, "NGRAM ");

          if (bCreateTableLikeGetIndexDesc && (NOT naf->isCreatedExplicitly())) continue;

          if (NOT naf->isCreatedExplicitly()) {
            outputLine(*space, "\n-- The following index is a system created index --", 0);
          }

          // get newindex name
          NAString newIndexName;
          if (bCreateTableLikeGetIndexDesc) {
            if (naTable->isPartitionV2Table() && naf->partLocalBaseIndex()) {
              char partitionIndexName[256];
              getPartitionIndexName(partitionIndexName, 256, indexName,
                                    likeTabName->getQualifiedNameObj().getObjectName());
              newIndexName = NAString(partitionIndexName);
            } else {
              // fix "DELETE"_nnnnnnnn_nnnn to DELETE_nnnnnnnnn_nnnn should not use func
              // getUnqualifiedObjectNameAsAnsiString()
              ComDeriveRandomInternalName(ComGetNameInterfaceCharSet(),
                                          likeTabName->getQualifiedNameObj().getObjectName(), newIndexName, STMTHEAP);
            }
            newIndexName = ToAnsiIdentifier(newIndexName);
          }
          sprintf(buf, "%sCREATE%sINDEX %s ON %s", (naf->isCreatedExplicitly() ? "\n" : ""), vu,
                  bCreateTableLikeGetIndexDesc ? newIndexName.data() : indexName.data(),
                  bCreateTableLikeGetIndexDesc
                      ? likeTabName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data()
                      : tableName.data());
          outputLine(*space, buf, 0);
        }

        if (type == 1) {
          int dummy;
          outputShortLine(*space, "  ( ");
          cmpDisplayColumns(naf->getAllColumns(), type, *space, buf, displaySystemCols, (type == 2), dummy,
                            isExternalTable, isAligned, withoutLobColumns);
          outputShortLine(*space, "  )");

          sprintf(buf, "  PRIMARY KEY ");
          outputShortLine(*space, buf);
        }

        int numIndexCols = ((type == 1) ? naf->getIndexKeyColumns().entries()
                                          : naf->getCountOfColumns(TRUE,
                                                                   TRUE,  // user-specified index cols only
                                                                   FALSE, FALSE));

        cmpDisplayPrimaryKey(naf->getIndexKeyColumns(), numIndexCols, displaySystemCols, *space, buf, FALSE, TRUE,
                             TRUE);

        if ((NOT sqlmxRegr) && isAligned) {
          char attrs[2000];
          strcpy(attrs, " ATTRIBUTES ");
          if (isAligned) strcat(attrs, "ALIGNED FORMAT ");
          outputShortLine(*space, attrs);
        }

        // DISPLAY partition index info
        if (naf->partLocalBaseIndex())
          outputShortLine(*space, " LOCAL");
        else if (naf->partGlobalIndex())
          outputShortLine(*space, " GLOBAL");

        if ((naf->hbaseCreateOptions()) && (type == 2 || bCreateTableLikeGetIndexDesc) &&
            (naf->hbaseCreateOptions()->entries() > 0)) {
          outputShortLine(*space, "  HBASE_OPTIONS ");
          outputShortLine(*space, "  ( ");

          for (int i = 0; i < naf->hbaseCreateOptions()->entries(); i++) {
            HbaseCreateOption *hco = (*naf->hbaseCreateOptions())[i];
            char separator = ((i < naf->hbaseCreateOptions()->entries() - 1) ? ',' : ' ');
            sprintf(buf, "    %s = '%s'%c", hco->key().data(), hco->val().data(), separator);
            outputShortLine(*space, buf);
          }

          outputShortLine(*space, "  ) ");
        }

        if ((naf->numSaltPartns() > 0) && (type == 2 || bCreateTableLikeGetIndexDesc))
          outputShortLine(*space, " SALT LIKE TABLE ");

        // display additional table columns
        if (type == 2 || bCreateTableLikeGetIndexDesc) {
          NABoolean firstTime = TRUE;
          NAString colString;
          const NAColumnArray &nacArr = naf->getAllColumns();
          for (int i = 0; i < nacArr.entries(); i++) {
            const NAColumn *nac = nacArr[i];
            if (NOT nac->isAddnlColumn()) continue;

            if (firstTime)
              colString += NAString("  (");
            else
              colString += NAString(", ");
            colString += NAString(ANSI_ID(nac->getColName()));

            if (firstTime) {
              outputShortLine(*space, " WITH TABLE COLUMNS ");
              firstTime = FALSE;
            }
          }  // for

          if (NOT firstTime) colString += NAString(")");

          if (NOT colString.isNull()) outputLine(*space, colString.data(), 2);
        }  // type == 2

        if (type == 2 || bCreateTableLikeGetIndexDesc) outputShortLine(*space, ";");

      }  // for
    }

    //  showddl or invoke index for parytition entity table (isPartitionV2Table && type != 3)
    //  normal create table like will get in to create normal partion index
    //  create partition table like will not get in because we can not create normal partition index
    if (isPartitionV2Table && ((bCreateTableLikeGetIndexDesc && !isCreateForPart) || type != 3))
      CmpDescribePartIndexDesc(&bindWA, type, naTable, *space, buf, dtName, likeTabName);

    if (type == 2 || bCreateTableLikeGetConstraintDesc)  // showddl
    {
      const AbstractRIConstraintList &uniqueList = naTable->getUniqueConstraints();

      for (Int32 i = 0; i < uniqueList.entries(); i++) {
        AbstractRIConstraint *ariConstr = uniqueList[i];

        if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT) continue;

        UniqueConstraint *uniqConstr = (UniqueConstraint *)ariConstr;
        if (uniqConstr->isPrimaryKeyConstraint()) continue;

        if (CmpGenUniqueConstrStr(ariConstr, space, buf, TRUE, FALSE, likeTabName)) {
          // internal error. Assert.
          CMPASSERT(0);
          return -1;
        }
      }  // for

      const AbstractRIConstraintList &refList = naTable->getRefConstraints();

      for (Int32 i = 0; i < refList.entries(); i++) {
        AbstractRIConstraint *ariConstr = refList[i];

        if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT) continue;

        if (CmpGenRefConstrStr(ariConstr, space, buf, TRUE, FALSE, likeTabName)) {
          // internal error. Assert.
          CMPASSERT(0);
          return -1;
        }

      }  // for

      const CheckConstraintList &checkList = naTable->getCheckConstraints();

      for (Int32 i = 0; i < checkList.entries(); i++) {
        CheckConstraint *checkConstr = (CheckConstraint *)checkList[i];

        const NAString &ansiConstrName = checkConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);

        NAString checkConstText = checkConstr->getConstraintText();
        NAString newCheckName = ansiConstrName;
        // get new check name
        if (bCreateTableLikeGetConstraintDesc && likeTabName) {
          // fix "DELETE"_nnnnnnnn_nnnn to DELETE_nnnnnnnnn_nnnn should not use func
          // getUnqualifiedObjectNameAsAnsiString()
          ComDeriveRandomInternalName(ComGetNameInterfaceCharSet(), likeTabName->getQualifiedNameObj().getObjectName(),
                                      newCheckName, STMTHEAP);
          size_t pos = checkConstText.index(tableName);
          while (pos && (pos < checkConstText.length())) {
            checkConstText = checkConstText.replace(
                pos, tableName.length(), likeTabName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE));
            pos = checkConstText.index(
                tableName.data(), pos + likeTabName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).length(),
                NAString::ignoreCase);
          }
        }
        sprintf(buf, "\nALTER TABLE %s ADD CONSTRAINT %s CHECK %s",
                bCreateTableLikeGetConstraintDesc
                    ? likeTabName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data()
                    : tableName.data(),
                newCheckName.data(), checkConstText.data());
        outputLine(*space, buf, 0);
        outputShortLine(*space, ";");

      }  // for

    }  // showddl
  }

  if ((type == 2) && (naTable->isHbaseCellTable() || naTable->isHbaseRowTable()) && (NOT isView)) {
    outputShortLine(*space, " ");

    outputShortLine(*space, "/* HBase DDL */");
    sprintf(buf, "CREATE HBASE TABLE %s ( COLUMN FAMILY '%s') ", naTable->getTableName().getObjectName().data(), "#1");
    outputShortLine(*space, buf);

    // if this hbase table is registered in traf metadata, show that.
    if (naTable->isRegistered()) {
      outputShortLine(*space, " ");

      sprintf(buf, "REGISTER%sHBASE %s %s;", (naTable->isInternalRegistered() ? " /*INTERNAL*/ " : " "), "TABLE",
              naTable->getTableName().getObjectName().data());

      NAString bufnas(buf);
      outputLongLine(*space, bufnas, 0);

      str_sprintf(buf, "/* ObjectUID = %ld */", objectUID);
      outputShortLine(*space, buf);
    } else if (isDetail) {
      // show a comment that this object should be registered
      outputShortLine(*space, " ");

      outputShortLine(*space, "-- Object is not registered in Trafodion Metadata.");
      outputShortLine(*space, "-- Register it using the next command:");
      sprintf(buf, "--   REGISTER HBASE TABLE %s;", naTable->getTableName().getQualifiedNameAsAnsiString().data());
      NAString bufnas(buf);
      outputLongLine(*space, bufnas, 0);
    }
  }

  // display comments
  if (type == 2 && objectUID > 0) {
    enum ComObjectType objType = COM_BASE_TABLE_OBJECT;

    if (isView) {
      objType = COM_VIEW_OBJECT;
    }

    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      return -1;
    }

    ComTdbVirtObjCommentInfo objCommentInfo;
    if (cmpSBD.getSeabaseObjectComment(objectUID, objType, objCommentInfo, heap)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      cmpSBD.switchBackCompiler();
      return -1;
    }

    // display Table COMMENT statements
    if (objCommentInfo.objectComment != NULL) {
      // new line
      outputLine(*space, "", 0);

      sprintf(buf, "COMMENT ON %s %s IS '%s' ;", objType == COM_BASE_TABLE_OBJECT ? "TABLE" : "VIEW", tableName.data(),
              objCommentInfo.objectComment);
      outputLine(*space, buf, 0);
    }

    // display Column COMMENT statements
    if (objCommentInfo.numColumnComment > 0 && objCommentInfo.columnCommentArray != NULL) {
      outputLine(*space, "", 0);
      for (int idx = 0; idx < objCommentInfo.numColumnComment; idx++) {
        sprintf(buf, "COMMENT ON COLUMN %s.%s IS '%s' ;", tableName.data(),
                objCommentInfo.columnCommentArray[idx].columnName,
                objCommentInfo.columnCommentArray[idx].columnComment);
        outputLine(*space, buf, 0);
      }
    }

    // display Index COMMENT statements
    if (objCommentInfo.numIndexComment > 0 && objCommentInfo.indexCommentArray != NULL) {
      outputLine(*space, "", 0);
      for (int idx = 0; idx < objCommentInfo.numIndexComment; idx++) {
        sprintf(buf, "COMMENT ON INDEX %s IS '%s' ;", objCommentInfo.indexCommentArray[idx].indexFullName,
                objCommentInfo.indexCommentArray[idx].indexComment);
        outputLine(*space, buf, 0);
      }
    }

    // do a comment info memory clean
    NADELETEARRAY(objCommentInfo.columnCommentArray, objCommentInfo.numColumnComment, ComTdbVirtColumnCommentInfo,
                  heap);
    NADELETEARRAY(objCommentInfo.indexCommentArray, objCommentInfo.numIndexComment, ComTdbVirtIndexCommentInfo, heap);

    cmpSBD.switchBackCompiler();
  }

  // If SHOWDDL and authorization is enabled, display GRANTS
  if (type == 2) {
    // objectUID = (int64_t)naTable->objectUid().get_value();
    if ((CmpCommon::context()->isAuthorizationEnabled()) && displayPrivilegeGrants && (objectUID > 0)) {
      // now get the grant stmts
      std::string privMDLoc(ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG));
      privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");
      PrivMgrCommands privInterface(privMDLoc, CmpCommon::diags(), PrivMgr::PRIV_INITIALIZED);
      PrivMgrObjectInfo objectInfo(naTable);
      std::string privilegeText;
      if (cmpSBD.switchCompiler()) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
        return -1;
      }

      if (privInterface.describePrivileges(objectInfo, privilegeText)) {
        outputShortLine(*space, " ");
        outputLine(*space, privilegeText.c_str(), 0);
      }

      cmpSBD.switchBackCompiler();
    }
  }

  outbuflen = space->getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space->makeContiguous(outbuf, outbuflen);

  NADELETEBASIC(buf, heap);

  return 0;
}

short CmpDescribeSequence(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap, Space *inSpace,
                          NABoolean sqlCommentOut) {
  CorrName cn(dtName, heap);

  cn.setSpecialType(ExtendedQualName::SG_TABLE);

  // remove NATable for this table so latest values in the seq table could be read.
  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_MINE_ONLY, COM_SEQUENCE_GENERATOR_OBJECT,
                                                  FALSE, FALSE);

  ULng32 savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
  Set_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NATable *naTable = bindWA.getNATable(cn);

  Assign_SqlParser_Flags(savedParserFlags);

  TableDesc *tdesc = NULL;
  if (naTable == NULL || bindWA.errStatus()) return -1;

  // Verify user can perform commands
  if (!CmpDescribeIsAuthorized(heap, SQLOperation::UNKNOWN, naTable->getPrivInfo(), COM_SEQUENCE_GENERATOR_OBJECT,
                               naTable->getOwner()))
    return -1;

  const SequenceGeneratorAttributes *sga_orig = naTable->getSGAttributes();
  SequenceGeneratorAttributes *sga = new (heap) SequenceGeneratorAttributes(*sga_orig, heap);
  CmpSeabaseDDL cmpSBD((NAHeap *)heap);

  if (sga->getSGOrder())  // order sequence
  {
    // get currval
    ExpHbaseInterface *ehi = cmpSBD.allocEHI(COM_STORAGE_HBASE);
    long seqVal = 0;
    if (ehi != 0) {
      NAString tabName(ORDER_SEQ_MD_TABLE);
      NAString famName(SEABASE_DEFAULT_COL_FAMILY);
      NAString qualName(ORDER_SEQ_DEFAULT_QUAL);
      NAString rowId = Int64ToNAString(sga->getSGObjectUID().get_value());
      ehi->getNextValue(tabName, rowId, famName, qualName, 0, seqVal, true);
      if (seqVal == 0) {
        *CmpCommon::diags() << DgSqlCode(-1582) << DgString0(rowId);
        if (sga) NADELETEBASIC(sga, heap);
        return -1;
      }

      // validate seqVal
      long maxVal = sga->getSGMaxValue();
      long step = sga->getSGIncrement();

      if (seqVal > maxVal) {
        if (sga->getSGCycleOption()) {
          long seqValTmp = seqVal + step;
          long minVal = sga->getSGMinValue();
          long modOp = maxVal - minVal + 1;
          long valueToAdd = modOp - (maxVal + 1) % (modOp);
          seqVal = (seqValTmp + valueToAdd) % modOp + minVal;
        } else {
          // should never happen
        }
      } else if (seqVal <= maxVal) {
        if (seqVal + step < maxVal)
          seqVal = seqVal + step;
        else
          seqVal = maxVal;
      }
      sga->setSGNextValue(seqVal);
    }
  }

  const NAString &seqName = cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE);

  Space lSpace;

  Space *space;
  if (inSpace)
    space = inSpace;
  else
    space = &lSpace;

  char buf[1000] = "";
  //  CMPASSERT(buf);

  outputShortLine(*space, " ");

  int64_t objectUID = (int64_t)naTable->objectUid().get_value();

  char seqType[100] = "";
  if (sga->getSGType() == COM_INTERNAL_SG) {
    str_sprintf(seqType, "/* INTERNAL, SEQ_UID = %ld */", objectUID);
  } else if (sga->getSGType() == COM_EXTERNAL_SG) {
    strcpy(seqType, " ");
  } else if (sga->getSGType() == COM_SYSTEM_SG) {
    strcpy(seqType, "/* SYSTEM */ ");
  } else
    strcpy(seqType, " ");

  if (sqlCommentOut)
    sprintf(buf, "--CREATE SEQUENCE %s %s", seqName.data(), seqType);
  else
    sprintf(buf, "CREATE SEQUENCE %s %s", seqName.data(), seqType);

  outputShortLine(*space, buf);

  sga->display(space, NULL, FALSE, TRUE, sqlCommentOut);

  outputShortLine(*space, ";", sqlCommentOut);

  if (sga) NADELETEBASIC(sga, heap);
  char *sqlmxRegr = getenv("SQLMX_REGRESS");
  NABoolean displayPrivilegeGrants = TRUE;
  if (((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && sqlmxRegr) ||
      (CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF))
    displayPrivilegeGrants = FALSE;

  // display comment
  if (objectUID > 0) {
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      return -1;
    }

    ComTdbVirtObjCommentInfo objCommentInfo;
    if (cmpSBD.getSeabaseObjectComment(objectUID, COM_SEQUENCE_GENERATOR_OBJECT, objCommentInfo, heap)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      cmpSBD.switchBackCompiler();
      return -1;
    }

    if (objCommentInfo.objectComment != NULL) {
      // new line
      outputLine(*space, "", 0);

      sprintf(buf, "COMMENT ON SEQUENCE %s IS '%s' ;",
              cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data(), objCommentInfo.objectComment);
      outputLine(*space, buf, 0);
    }

    cmpSBD.switchBackCompiler();
  }

  // If authorization enabled, display grant statements
  if (CmpCommon::context()->isAuthorizationEnabled() && displayPrivilegeGrants) {
    // now get the grant stmts
    NAString privMDLoc;
    CONCAT_CATSCH(privMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_MD_SCHEMA);
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_PRIVMGR_SCHEMA);
    PrivMgrCommands privInterface(std::string(privMDLoc.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());

    std::string privilegeText;
    PrivMgrObjectInfo objectInfo(naTable);
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    if (privInterface.describePrivileges(objectInfo, privilegeText)) {
      outputShortLine(*space, " ");
      outputLine(*space, privilegeText.c_str(), 0);
    }

    cmpSBD.switchBackCompiler();
  }

  if (!inSpace) {
    outbuflen = space->getAllocatedSpaceSize();
    outbuf = new (heap) char[outbuflen];
    space->makeContiguous(outbuf, outbuflen);
  }

  return 0;
}

// ----------------------------------------------------------------------------
// Function: CmpDescribeIsAuthorized
//
// Determines if current user is authorized to perform operations
//
// parameters:  PrivMgrUserPrivs *privs - pointer to granted privileges
//              operation - SQLOperation to check in addition to SHOW
//
// returns:  true if authorized, false otherwise
// ----------------------------------------------------------------------------
bool CmpDescribeIsAuthorized(CollHeap *heap, SQLOperation operation, PrivMgrUserPrivs *privs, ComObjectType objectType,
                             Int32 owner)

{
  if (!CmpCommon::context()->isAuthorizationEnabled()) return true;

  if (ComUser::isRootUserID()) return true;

  if (owner != NA_UserIdDefault)
    if (ComUser::getCurrentUser() == owner || ComUser::currentUserHasRole(owner)) return true;

  // check to see if user has select privilege
  if (privs) {
    switch (objectType) {
      case COM_LIBRARY_OBJECT:
      case COM_SEQUENCE_GENERATOR_OBJECT:
        if (privs->hasUsagePriv()) return true;
        break;
      case COM_STORED_PROCEDURE_OBJECT:
      case COM_USER_DEFINED_ROUTINE_OBJECT:
        if (privs->hasExecutePriv()) return true;
        break;
      case COM_PRIVATE_SCHEMA_OBJECT:
      case COM_SHARED_SCHEMA_OBJECT:
        break;
      case COM_VIEW_OBJECT:
      case COM_BASE_TABLE_OBJECT:
      default:
        if (privs->hasSelectPriv()) return true;
    }
  }

  // check to see if user has requested component privilege (or SHOW)
  NAList<SQLOperation> compPrivList(heap);
  PrivMgrComponentPrivileges componentPrivileges;
  compPrivList.insert(SQLOperation::SHOW);
  if (operation != SQLOperation::UNKNOWN) compPrivList.insert(operation);

  bool hasPriv = false;
  if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(), compPrivList, hasPriv) < 0) return false;

  if (hasPriv) return true;

  *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
  return false;
}

// *****************************************************************************
// *                                                                           *
// * Function: CmpDescribeLibrary                                              *
// *                                                                           *
// *    Describes the DDL for a library object with normalized syntax.         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <corrName>                      const CorrName  &               In       *
// *    is a reference to correlation name for the library object.             *
// *                                                                           *
// *  <outbuf>                        char * &                        Out      *
// *    is a reference to a pointed to a character array.  The desribe output  *
// *    is stored here.                                                        *
// *                                                                           *
// *  <outbuflen>                     ULng32 &                        Out      *
// *    is the number of characters stored in outbuf.  Note, ULng32 is an      *
// *    unsigned 32-bit integer, aka uint32_t or unsigned int.  Not a long.    *
// *                                                                           *
// *  <heap>                          CollHeap *                      In       *
// *    is the heap to use for any dynamic allocations.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Describe text added to outbuf.                                     *
// * false: Desribe text not added. A CLI error is put into the diags area.    *
// *                                                                           *
// *****************************************************************************
bool CmpDescribeLibrary(const CorrName &corrName, char *&outbuf, ULng32 &outbuflen, CollHeap *heap)

{
  CmpSeabaseDDL cmpSBD((NAHeap *)heap);
  CorrName cn(corrName, heap);

  cn.setSpecialType(ExtendedQualName::LIBRARY_TABLE);

  const NAString &libraryName = cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE);

  ComObjectName libName(libraryName);
  NAString libCatNamePart = libName.getCatalogNamePartAsAnsiString();
  NAString libSchNamePart = libName.getSchemaNamePartAsAnsiString(TRUE);
  NAString libObjNamePart = libName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibraryName = libName.getExternalName(TRUE);

  ExeCliInterface cliInterface(heap);

  TrafDesc *tDesc = cmpSBD.getSeabaseLibraryDesc(libCatNamePart, libSchNamePart, libObjNamePart);
  if (tDesc == NULL) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(extLibraryName.data());
    return false;
  }

  long libraryUID = tDesc->libraryDesc()->libraryUID;

  if (libraryUID <= 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_LIBRARY_DOES_NOT_EXIST) << DgTableName(extLibraryName.data());
    return false;
  }

  // For libraries, we need to check if the user has the USAGE privilege;
  // if so, they can perform SHOWDDL on this library.

  NAString privMgrMDLoc;

  CONCAT_CATSCH(privMgrMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_PRIVMGR_SCHEMA);

  PrivMgrCommands privInterface(privMgrMDLoc.data(), CmpCommon::diags());

  if (CmpCommon::context()->isAuthorizationEnabled()) {
    PrivMgrUserPrivs privs;
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    PrivStatus retcode = privInterface.getPrivileges(libraryUID, COM_LIBRARY_OBJECT, ComUser::getCurrentUser(), privs);

    cmpSBD.switchBackCompiler();

    // Verify user can perform the SHOWDDL LIBRARY command on the specified library.
    if (!CmpDescribeIsAuthorized(heap, SQLOperation::MANAGE_LIBRARY, &privs, COM_LIBRARY_OBJECT)) return false;
  }

  Space localSpace;
  Space *space = &localSpace;

  char buf[1000];

  sprintf(buf, "CREATE LIBRARY %s FILE '%s'", extLibraryName.data(), tDesc->libraryDesc()->libraryFilename);

  outputShortLine(*space, buf);
  outputShortLine(*space, ";");

  // Determine if privilege grants should be displayed
  NABoolean displayPrivilegeGrants =
      ((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF) ||
       ((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && getenv("SQLMX_REGRESS")))
          ? FALSE
          : TRUE;

  // display library comment
  if (libraryUID > 0) {
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    ComTdbVirtObjCommentInfo objCommentInfo;
    if (cmpSBD.getSeabaseObjectComment(libraryUID, COM_LIBRARY_OBJECT, objCommentInfo, heap)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      cmpSBD.switchBackCompiler();
      return -1;
    }

    if (objCommentInfo.objectComment != NULL) {
      // new line
      outputLine(*space, "", 0);

      sprintf(buf, "COMMENT ON LIBRARY %s IS '%s' ;",
              cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data(), objCommentInfo.objectComment);
      outputLine(*space, buf, 0);
    }

    cmpSBD.switchBackCompiler();
  }

  // If authorization is enabled, display grant statements for library
  if (CmpCommon::context()->isAuthorizationEnabled() && displayPrivilegeGrants) {
    // now get the grant stmts
    NAString trafMDLoc;

    CONCAT_CATSCH(trafMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_MD_SCHEMA);

    PrivMgrCommands privInterface(std::string(trafMDLoc.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());

    std::string privilegeText;
    PrivMgrObjectInfo objectInfo(libraryUID, extLibraryName.data(), tDesc->libraryDesc()->libraryOwnerID,
                                 tDesc->libraryDesc()->librarySchemaOwnerID, COM_LIBRARY_OBJECT);
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    if (privInterface.describePrivileges(objectInfo, privilegeText)) {
      outputShortLine(*space, " ");
      outputLine(*space, privilegeText.c_str(), 0);
    }

    cmpSBD.switchBackCompiler();
  }

  outbuflen = space->getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space->makeContiguous(outbuf, outbuflen);

  return true;
}
//************************ End of CmpDescribeLibrary ***************************

// Routine to support SHOWDDL PACKAGE <package-name>
short CmpDescribePackage(const CorrName &cn, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  NABoolean logFormat = (CmpCommon::getDefault(SHOWDDL_DISPLAY_FORMAT) == DF_LOG);

  Space localSpace;
  Space *space = &localSpace;

  outputShortLine(*space, " ");

  NAString body;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  NAString quotedSchName;
  ToQuotedString(quotedSchName, cn.getQualifiedNameObj().getSchemaName(), FALSE);
  NAString quotedObjName;
  ToQuotedString(quotedObjName, cn.getQualifiedNameObj().getObjectName(), FALSE);

  CmpSeabaseDDL cmpSBD((NAHeap *)heap);
  long objUID =
      cmpSBD.getObjectUID(&cliInterface, CmpSeabaseDDL::getSystemCatalogStatic().data(), quotedSchName.data(),
                          quotedObjName.data(), COM_PACKAGE_OBJECT_LIT, NULL, NULL, NULL, false, true);
  if (objUID < 0) {
    return -1;
  }

  if (CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic().data(), &cliInterface, objUID,
                                   COM_ROUTINE_TEXT, 1, /*package definition*/
                                   body, false))
    return -1;
  body.append('\n');
  if (CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic().data(), &cliInterface, objUID,
                                   COM_ROUTINE_TEXT, 2, /*package body definiton*/
                                   body, false))
    return -1;
  outputLongLine(*space, body, 0, FALSE);
  outbuflen = space->getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space->makeContiguous(outbuf, outbuflen);
  return 1;
}

// Routine to support SHOWDDL [ PROCEDURE | FUNCTION | TABLE_MAPPING FUNCTION ]
// <routine-name>
short CmpDescribeRoutine(const CorrName &cn, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NARoutine *routine = bindWA.getNARoutine(cn.getQualifiedNameObj());
  const NAString &rName = cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE);
  if (routine == NULL || bindWA.errStatus()) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(rName.data());
    return 0;
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  if (CmpSeabaseDDL::isSPSQLPackageRoutine(cn.getQualifiedNameObj().getObjectName().data(), &cliInterface,
                                           cn.getQualifiedNameObj()) == 1) {
    *CmpCommon::diags() << DgSqlCode(-4459) << DgString0(rName.data());
    return 0;
  }

  // Verify user can perform commands
  if (!CmpDescribeIsAuthorized(heap, SQLOperation::UNKNOWN, routine->getPrivInfo(), COM_USER_DEFINED_ROUTINE_OBJECT))
    return 0;

  NABoolean logFormat = (CmpCommon::getDefault(SHOWDDL_DISPLAY_FORMAT) == DF_LOG);

  Space localSpace;
  Space *space = &localSpace;

  outputShortLine(*space, " ");

  // Start populating our output buffer
  // buffer is reused for each line of output. Longets line will be the
  // java signature for SPJs. Currently this has an upper limit of 8K.
  // We allocate another 2K bytes for the name and any small changes
  // in size due to formatting.
  char *buf = new (STMTHEAP) char[10000];
  CMPASSERT(buf);

  NABoolean buildRETURNSclause = TRUE;
  switch (routine->getRoutineType()) {
    case COM_SCALAR_UDF_TYPE:
      sprintf(buf, "CREATE FUNCTION %s", rName.data());
      break;
    case COM_TABLE_UDF_TYPE:
      sprintf(buf, "CREATE TABLE_MAPPING FUNCTION %s", rName.data());
      break;
    case COM_PROCEDURE_TYPE:
      sprintf(buf, "CREATE PROCEDURE %s", rName.data());
      buildRETURNSclause = FALSE;
      break;
    default:
      ComASSERT(FALSE);
  }
  if (logFormat) outputShortLine(*space, "--> UR ");
  outputShortLine(*space, buf);
  outputShortLine(*space, "  (");

  // Format the parameters
  Int32 numParams = routine->getParamCount();
  Int32 firstRETURNSparamIndex = -1;
  const NAColumnArray &paramList = routine->getParams();
  for (Int32 i = 0; i < numParams; i++) {
    NAColumn &param = *(paramList[i]);
    ComColumnDirection direction = param.getColumnMode();

    if (buildRETURNSclause AND direction EQU COM_OUTPUT_COLUMN) {
      firstRETURNSparamIndex = i;
      i = numParams;  // i.e., exit loop
      continue;
    }

    if (i == 0)
      strcpy(buf, "    ");
    else
      strcpy(buf, "  , ");

    switch (direction) {
      case COM_INPUT_COLUMN:
        strcat(buf, "IN ");
        break;
      case COM_OUTPUT_COLUMN:
        strcat(buf, "OUT ");
        break;
      case COM_INOUT_COLUMN:
        strcat(buf, "INOUT ");
        break;
    }  // switch

    // Next step for this parameter is to print its name. If a heading
    // is defined we print that. Otherwise if a column name is defined
    // we print that. Otherwise it is an unnamed parameter and we do
    // nothing.
    const NAString &heading = param.getHeading();
    const NAString &colName = param.getColName();
    if (heading.compareTo("") != 0) {
      strcat(buf, ANSI_ID(heading));
      strcat(buf, " ");
    } else if (colName.compareTo("") != 0) {
      strcat(buf, ANSI_ID(colName));
      strcat(buf, " ");
    }

    char typeString[80];
    strcpy(typeString, (param.getType())->getTypeSQLname(TRUE));
    if (routine->getLanguage() EQU COM_LANGUAGE_JAVA) {
      // Java does not support unsigned / signed as
      // it converts NUMERIC types to a Java BigDecimal.
      Int32 typeLen = static_cast<Int32>(strlen(typeString));
      if (typeLen >= 9 && 0 == strcmp(&typeString[typeLen - 9], " UNSIGNED")) {
        typeString[typeLen - 9] = '\0';
      } else if (typeLen >= 7 && 0 == strcmp(&typeString[typeLen - 7], " SIGNED")) {
        typeString[typeLen - 7] = '\0';
      }
    }  // if Language Java
    strcat(buf, typeString);

    outputShortLine(*space, buf);
  }  // for

  // add the closing ")" for the formal parameter list
  strcpy(buf, "  )");
  outputShortLine(*space, buf);

  if (buildRETURNSclause AND firstRETURNSparamIndex > -1) {
    strcpy(buf, "  RETURNS ");
    outputShortLine(*space, buf);
    outputShortLine(*space, "  (");
    // Build RETURN[S] clause from param list
    for (Int32 i7 = firstRETURNSparamIndex; i7 < numParams; i7++) {
      NAColumn &param7 = *(paramList[i7]);
      ComColumnDirection direction7 = param7.getColumnMode();

      if (i7 EQU firstRETURNSparamIndex)
        strcpy(buf, "    ");
      else
        strcpy(buf, "  , ");

      switch (direction7) {
        case COM_INPUT_COLUMN:
          strcat(buf, "IN ");  // not supposed to happen
          break;
        case COM_OUTPUT_COLUMN:
          strcat(buf, "OUT ");
          break;
        case COM_INOUT_COLUMN:
          strcat(buf, "INOUT ");  // not supposed to happen
          break;
      }  // switch

      // Next step for this parameter is to print its name. If a heading
      // is defined we print that. Otherwise if a column name is defined
      // we print that. Otherwise it is an unnamed parameter and we do
      // nothing.
      const NAString &heading7 = param7.getHeading();
      const NAString &colName7 = param7.getColName();
      if (heading7.compareTo("") != 0) {
        strcat(buf, ANSI_ID(heading7));
        strcat(buf, " ");
      } else if (colName7.compareTo("") != 0) {
        strcat(buf, ANSI_ID(colName7));
        strcat(buf, " ");
      }

      char typeString7[80];
      strcpy(typeString7, (param7.getType())->getTypeSQLname(TRUE));
      if (routine->getLanguage() EQU COM_LANGUAGE_JAVA) {
        // Java does not support UNSIGNED and SIGNED as
        // it converts NUMERIC types to a Java BigDecimal.
        Int32 typeLen7 = static_cast<Int32>(strlen(typeString7));
        if (typeLen7 >= 9 AND 0 EQU strcmp(&typeString7[typeLen7 - 9], " UNSIGNED")) {
          typeString7[typeLen7 - 9] = '\0';
        } else if (typeLen7 >= 7 AND 0 EQU strcmp(&typeString7[typeLen7 - 7], " SIGNED")) {
          typeString7[typeLen7 - 7] = '\0';
        }
      }  // if Language Java
      strcat(buf, typeString7);

      outputShortLine(*space, buf);
    }  // for

    strcpy(buf, "  )");
    outputShortLine(*space, buf);

  }  // if (buildRETURNSclause AND firstRETURNSparamIndex > -1)

  // EXTERNAL NAME clause
  NABoolean isProcedure = (routine->getRoutineType() EQU COM_PROCEDURE_TYPE ? TRUE : FALSE);
  // Check if this is an SPSQL procedure/function
  NABoolean isSPSQL = (str_cmp_ne(routine->getFile(), SEABASE_SPSQL_CONTAINER) == 0 &&
                       (str_cmp_ne(routine->getExternalName(), SEABASE_SPSQL_CALL) == 0 ||
                        str_cmp_ne(routine->getExternalName(), SEABASE_SPSQL_CALL_FUNC) == 0));

  if (isProcedure && !isSPSQL) {
    strcpy(buf, "  EXTERNAL NAME '");

    NAString exNamePrefixInStrLitFormat44;
    NAString exNamePrefixInInternalFormat;
    exNamePrefixInInternalFormat += routine->getFile();
    exNamePrefixInInternalFormat += ".";
    exNamePrefixInInternalFormat += routine->getExternalName();
    ToQuotedString(exNamePrefixInStrLitFormat44  // out - NAString &
                   ,
                   exNamePrefixInInternalFormat  // in  - const NAString &
                   ,
                   FALSE  // in  - NABoolean encloseInQuotes
    );
    strcat(buf, exNamePrefixInStrLitFormat44.data());

    // Get the actual signature size

    LmJavaSignature lmSig(routine->getSignature().data(), heap);
    Int32 sigSize = lmSig.getUnpackedSignatureSize();

    if (0 == sigSize) {
      *CmpCommon::diags() << DgSqlCode(-11223) << DgString0(". Unable to determine signature size.");
      return 0;
    }

    char *sigBuf = new (STMTHEAP) char[sigSize + 1];

    if (lmSig.unpackSignature(sigBuf) == -1) {
      *CmpCommon::diags() << DgSqlCode(-11223) << DgString0(". Unable to determine signature.");
      return 0;
    }

    sigBuf[sigSize] = '\0';

    strcat(buf, " ");
    strcat(buf, sigBuf);
    strcat(buf, "'");
    outputShortLine(*space, buf);

  } else if (!isSPSQL)  // this is not a procedure
  {
    strcpy(buf, "  EXTERNAL NAME ");

    NAString externalNameInStrLitFormat(STMTHEAP);
    ToQuotedString(externalNameInStrLitFormat  // out - NAString &
                   ,
                   routine->getExternalName()  // in  - const NAString &
                   ,
                   TRUE  // in  - NABoolean encloseInQuotes
    );
    strcat(buf, externalNameInStrLitFormat.data());
    outputShortLine(*space, buf);
  }

  if (!isSPSQL) {
    // LIBRARY clause
    NAString libName(routine->getLibrarySqlName().getExternalName());
    if ((libName.length() > 0) && (libName.data()[0] != ' ')) {
      strcpy(buf, "  LIBRARY ");
      strcat(buf, libName.data());
      outputShortLine(*space, buf);
    }
  }

  // EXTERNAL SECURITY clause
  if (isProcedure) {
    switch (routine->getExternalSecurity()) {
      case COM_ROUTINE_EXTERNAL_SECURITY_INVOKER:
        outputShortLine(*space, "  EXTERNAL SECURITY INVOKER");
        break;
      case COM_ROUTINE_EXTERNAL_SECURITY_DEFINER:
        outputShortLine(*space, "  EXTERNAL SECURITY DEFINER");
        break;
      default:
        ComASSERT(FALSE);
        break;
    }
  }

  // Fix the langauge for SPSQL
  if (isSPSQL) {
    outputShortLine(*space, "  LANGUAGE SQL");
  } else {
    switch (routine->getLanguage()) {
      case COM_LANGUAGE_JAVA:
        outputShortLine(*space, "  LANGUAGE JAVA");
        break;
      case COM_LANGUAGE_C:
        outputShortLine(*space, "  LANGUAGE C");
        break;
      case COM_LANGUAGE_SQL:
        outputShortLine(*space, "  LANGUAGE SQL");
        break;
      case COM_LANGUAGE_CPP:
        outputShortLine(*space, "  LANGUAGE CPP");
        break;
      default:
        ComASSERT(FALSE);
        break;
    }

    switch (routine->getParamStyle()) {
      case COM_STYLE_GENERAL:
        outputShortLine(*space, "  PARAMETER STYLE GENERAL");
        break;
      case COM_STYLE_JAVA_CALL:
        outputShortLine(*space, "  PARAMETER STYLE JAVA");
        break;
      case COM_STYLE_SQL:
        outputShortLine(*space, "  PARAMETER STYLE SQL");
        break;
      case COM_STYLE_SQLROW:
        outputShortLine(*space, "  PARAMETER STYLE SQLROW");
        break;
      case COM_STYLE_JAVA_OBJ:
      case COM_STYLE_CPP_OBJ:
      case COM_STYLE_SQLROW_TM:
        break;
      default:
        ComASSERT(FALSE);
        break;
    }
  }

  switch (routine->getSqlAccess()) {
    case COM_NO_SQL:
      outputShortLine(*space, "  NO SQL");
      break;
    case COM_MODIFIES_SQL:
      outputShortLine(*space, "  MODIFIES SQL DATA");
      break;
    case COM_CONTAINS_SQL:
      outputShortLine(*space, "  CONTAINS SQL");
      break;
    case COM_READS_SQL:
      outputShortLine(*space, "  READS SQL DATA");
      break;
    default:
      // Unknown SQL access mode
      ComASSERT(FALSE);
      break;
  }  // switch

  if (isProcedure) {
    // max result sets
    strcpy(buf, "  DYNAMIC RESULT SETS ");
    sprintf(&buf[strlen(buf)], "%d", (Int32)routine->getMaxResults());
    outputShortLine(*space, buf);

    // transaction required clause needs to be shown in the output from M9
    switch (routine->getTxAttrs()) {
      case COM_NO_TRANSACTION_REQUIRED:
        strcpy(buf, "  NO TRANSACTION REQUIRED");
        break;
      case COM_TRANSACTION_REQUIRED:
        strcpy(buf, "  TRANSACTION REQUIRED");
        break;
      default:
        ComASSERT(FALSE);
        break;
    }
    outputShortLine(*space, buf);
  } else {
    if (routine->getLanguage() == COM_LANGUAGE_C) {
      // final call
      if (routine->isFinalCall())  // same as isExtraCall()
        strcpy(buf, "  FINAL CALL");
      else
        strcpy(buf, "  NO FINAL CALL");
      outputShortLine(*space, buf);

      // state area size
      if (routine->getStateAreaSize() > 0) {
        strcpy(buf, "  STATE AREA SIZE ");
        sprintf(&buf[strlen(buf)], "%d", routine->getStateAreaSize());
      } else {
        strcpy(buf, "  NO STATE AREA");
      }
      outputShortLine(*space, buf);
    }
  }

  if (routine->isScalarUDF()) {
    if (routine->getParallelism() == "AP")
      strcpy(buf, "  ALLOW ANY PARALLELISM");
    else
      strcpy(buf, "  NO PARALLELISM");
    outputShortLine(*space, buf);

    // Deterministic clause
    if (routine->isDeterministic())
      strcpy(buf, "  DETERMINISTIC");
    else
      strcpy(buf, "  NOT DETERMINISTIC");
    outputShortLine(*space, buf);
  }

  if (!isProcedure) {
    switch (routine->getExecutionMode()) {
      case COM_ROUTINE_FAST_EXECUTION:
        strcpy(buf, "  FAST EXECUTION MODE");
        break;
      case COM_ROUTINE_SAFE_EXECUTION:
        strcpy(buf, "  SAFE EXECUTION MODE");
        break;
      default:
        ComASSERT(FALSE);
        break;
    }
    outputShortLine(*space, buf);
  }

  if (isProcedure) {
    if (routine->isIsolate()) {
      strcpy(buf, "  ISOLATE");
    } else {
      strcpy(buf, "  NO ISOLATE");
    }
    outputShortLine(*space, buf);
  }  // routine type is SPJ

  // For SPSQL, output the routine body
  if (isSPSQL) {
    NAString body;
    if (CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic().data(), &cliInterface,
                                     routine->getRoutineID(), COM_ROUTINE_TEXT, 0, body, false))
      return -1;

    // NOTE2ME: This depends on buildQuery, which added ' AS '
    // between the header and body of routine definition.
    body = strstr(body.data(), " AS ");
    outputLongLine(*space, body, 0, FALSE);
  } else {
    outputShortLine(*space, "  ;");
  }

  CmpSeabaseDDL cmpSBD((NAHeap *)heap);

  char *sqlmxRegr = getenv("SQLMX_REGRESS");
  NABoolean displayPrivilegeGrants = TRUE;
  if (((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && sqlmxRegr) ||
      (CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF))
    displayPrivilegeGrants = FALSE;

  // display comment of routine
  long routineUID = routine->getRoutineID();
  if (routineUID > 0) {
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    ComTdbVirtObjCommentInfo objCommentInfo;
    if (cmpSBD.getSeabaseObjectComment(routineUID, COM_USER_DEFINED_ROUTINE_OBJECT, objCommentInfo, heap)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
      cmpSBD.switchBackCompiler();
      return -1;
    }

    if (objCommentInfo.objectComment != NULL) {
      // new line
      outputLine(*space, "", 0);

      sprintf(buf, "COMMENT ON %s %s IS '%s' ;",
              routine->getRoutineType() == COM_PROCEDURE_TYPE ? "PROCEDURE" : "FUNCTION",
              cn.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data(), objCommentInfo.objectComment);
      outputLine(*space, buf, 0);
    }

    cmpSBD.switchBackCompiler();
  }

  // If authorization enabled, display grant statements
  if (CmpCommon::context()->isAuthorizationEnabled() && displayPrivilegeGrants) {
    // now get the grant stmts
    int64_t objectUID = (int64_t)routine->getRoutineID();
    NAString privMDLoc;
    CONCAT_CATSCH(privMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_MD_SCHEMA);
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_PRIVMGR_SCHEMA);
    PrivMgrCommands privInterface(std::string(privMDLoc.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());

    std::string objectName(rName);
    std::string privilegeText;
    PrivMgrObjectInfo objectInfo(objectUID, objectName, (int32_t)routine->getObjectOwner(),
                                 (int32_t)routine->getSchemaOwner(), COM_USER_DEFINED_ROUTINE_OBJECT);

    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return -1;
    }

    if (privInterface.describePrivileges(objectInfo, privilegeText)) {
      outputShortLine(*space, " ");
      outputLine(*space, privilegeText.c_str(), 0);
    }

    cmpSBD.switchBackCompiler();
  }

  outbuflen = space->getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space->makeContiguous(outbuf, outbuflen);

  NADELETEBASIC(buf, CmpCommon::statementHeap());
  return 1;
}  // CmpDescribeShowddlProcedure

short CmpDescribeTrigger(const CorrName &cn, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  CmpSeabaseDDL cmpSBD((NAHeap *)heap);
  QualifiedName triggerName = cn.getQualifiedNameObj();
  long triggerId =
      cmpSBD.getObjectUID(&cliInterface, triggerName.getCatalogName().data(), triggerName.getSchemaName().data(),
                          triggerName.getObjectName().data(), COM_TRIGGER_OBJECT_LIT, NULL, NULL, NULL, false, true);
  if (triggerId < 1) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(triggerName.getQualifiedNameAsAnsiString(TRUE).data());
    return 0;
  }
  NAString triggerInfo;

  if (CmpSeabaseDDL::getTextFromMD(CmpSeabaseDDL::getSystemCatalogStatic().data(), &cliInterface, triggerId,
                                   COM_TRIGGER_TEXT, 2, triggerInfo, false)) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(triggerName.getQualifiedNameAsAnsiString(TRUE).data());
    return 0;
  }

  Space localSpace;
  Space *space = &localSpace;
  outputLongLine(*space, triggerInfo, 0, FALSE);
  outbuflen = space->getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space->makeContiguous(outbuf, outbuflen);
  return 1;
}

short CmpDescribeUUID(Describe *d, char *&outbuf, ULng32 &outbuflen, CollHeap *heap) {
  int cliRC = 0;
  Space space;
  char buf[4000];
  long uuid = atoInt64(d->getComponentName().data());
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  outputShortLine(space, " ");

  Queue *objectsQueue = NULL;
  sprintf(buf,
          "SELECT OBJECT_TYPE, CATALOG_NAME ||'.'|| SCHEMA_NAME ||'.'|| OBJECT_NAME, 0 FROM %s.\"%s\".%s WHERE "
          "OBJECT_UID = %ld;",
          CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, uuid);

  cliRC = cliInterface.fetchAllRows(objectsQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  objectsQueue->position();

  NAString tempInfo;
  if (objectsQueue->numEntries() > 1) {
    outputShortLine(space, "Inconsistent data exists in the metadata table. Please check the metadata");
  } else if (objectsQueue->numEntries() == 1) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
    NAString objType = vi->get(0);
    NAString objName = vi->get(1);
    long dataUID = *(long *)vi->get(2);

    tempInfo.format("OBJECT       UID: %ld", uuid);
    outputShortLine(space, tempInfo);
    tempInfo.format("OBJECT      TYPE: %s", objType.data());
    outputShortLine(space, tempInfo);
    tempInfo.format("OBJECT      NAME: %s", objName.data());
    outputShortLine(space, tempInfo);
    tempInfo.format("OBJECT  DATA UID: %ld", dataUID);
    outputShortLine(space, tempInfo);
  } else {
    tempInfo.format("INPUT UID:%ld DOES NOT EXIST IN DATABASE!", uuid);
    outputShortLine(space, tempInfo.data());
    ;
  }

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  return 0;
}

// SHOW CACHE FOR TABLE table_name
// Display centralized HDFS cache information for a table.
static short CmpDescribeTableHDFSCache(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  // test if this table is exist
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NATable *naTable = bindWA.getNATable((CorrName &)dtName);
  if (naTable == NULL || bindWA.errStatus()) return -1;

  TextVec tableList;
  tableList.push_back(naTable->getTableName().getQualifiedNameAsAnsiString().data());
  // Call Java method to get hdfs cache information for this table.
  CmpSeabaseDDL cmpSBD(STMTHEAP);
  ExpHbaseInterface *ehi = cmpSBD.allocEHI((naTable->isMonarch() ? COM_STORAGE_MONARCH : COM_STORAGE_HBASE));
  NAArray<HbaseStr> *rows = ehi->showTablesHDFSCache(tableList);
  if (rows == NULL) return -1;
  int rowNum = rows->entries();
  Space space;
  if (rowNum > 1) {
    NAString total;
    total.format("%d directives.", rowNum - 1);
    outputShortLine(space, total.data());
    // calculate column width
    // currently, we have 9 columns
    const int COLNUM = 9;
    const char *headerFields[COLNUM] = {"ID",           "POOL",         "REPL",         "EXPIRY",      "PATH",
                                        "BYTES_NEEDED", "BYTES_CACHED", "FILES_NEEDED", "FILES_CACHED"};
    int columnWidths[COLNUM];
    int headerlen = 0;
    // extract max width of each field
    HbaseStr *hbaseStr;
    hbaseStr = &rows->at(rowNum - 1);
    // std::string * lastRow = rows->get(rowNum-1);
    NAString nsRow(hbaseStr->val, hbaseStr->len, heap);
    NAList<NAString> fields(heap);
    nsRow.split('|', fields);
    // initialize width of each column.
    for (int i = 0; i < COLNUM; i++) {
      int fieldWidth = atoi(fields[i].data());
      int headerFieldWidth = strlen(headerFields[i]);
      columnWidths[i] = fieldWidth > headerFieldWidth ? fieldWidth : headerFieldWidth;
      columnWidths[i] += 2;
      // calculate total length of header line.
      headerlen += columnWidths[i];
    }

    // write header, same output as hdfs cacheadmin -listDirectives -stats.
    NAString line(' ', headerlen, heap);
    for (int i = 0, pos = 0; i < COLNUM; i++) {
      line.replace(pos, strlen(headerFields[i]), headerFields[i]);
      pos += columnWidths[i];
    }
    outputShortLine(space, line.data());

    // write each row.
    for (int i = 0; i < rowNum - 1; i++) {
      line.fill(0, ' ', line.length());
      HbaseStr *hbaseStr;
      hbaseStr = &rows->at(i);
      // std::string * oneRow = rows->get(i);
      NAString nsRow(hbaseStr->val, hbaseStr->len, heap);
      NAList<NAString> fields(heap);
      nsRow.split('|', fields);
      CMPASSERT(fields.entries() == 9);
      for (int j = 0, pos = 0; j < fields.entries(); j++) {
        line.replace(pos, fields[j].length(), fields[j]);
        pos += columnWidths[j];
      }
      outputShortLine(space, line.data());
    }

  } else {  // this table has no entry in hdfs cache.
    std::string msg;
    msg += "Table ";
    msg += naTable->getTableName().getQualifiedNameAsAnsiString();
    msg += " is not in HDFS cache.";
    outputShortLine(space, "");
    outputShortLine(space, msg.c_str());
  }
  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);
  return 0;
}

// SHOW CACHE FOR SCHEMA schema_name
// Display centralized HDFS cache information for a table.
static short CmpDescribeSchemaHDFSCache(const NAString &schemaText, char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  int cliRC = 0;
  int retCode = 0;
  char buf[4000];

  ComSchemaName schemaName(schemaText);
  NAString catName = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schName = schNameAsComAnsi.getInternalName();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  ComObjectType objectType;

  long schemaUID = CmpSeabaseDDL::getObjectTypeandOwner(&cliInterface, catName.data(), schName.data(),
                                                         SEABASE_SCHEMA_OBJECTNAME, objectType, schemaOwnerID);

  // if schemaUID == -1, then either the schema does not exist or an unexpected error occurred
  if (schemaUID == -1) {
    // If an error occurred, return
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) return -1;

    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catName.data())
                        << DgString1(schName.data());
    return -1;
  }

  // get all tables in the schema.
  Queue *objectsQueue = NULL;
  sprintf(buf,
          " select object_name  from   %s.\"%s\".%s "
          "  where catalog_name = '%s' and "
          "        schema_name = '%s'  and "
          "        object_type = 'BT'  "
          "  for read uncommitted access "
          "  order by 1 "
          "  ; ",
          CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(),
          (char *)schName.data());

  cliRC = cliInterface.fetchAllRows(objectsQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  objectsQueue->position();

  TextVec tableList;
  for (int i = 0; i < objectsQueue->numEntries(); i++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
    char *ptr = vi->get(0);
    sprintf(buf, "%s.%s.%s", (char *)catName.data(), (char *)schName.data(), ptr);
    tableList.push_back(buf);
  }
  CmpSeabaseDDL cmpSBD(STMTHEAP);
  // Call Java method to get hdfs cache information for this table.
  ExpHbaseInterface *ehi = cmpSBD.allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) return -1;
  NAArray<HbaseStr> *rows = ehi->showTablesHDFSCache(tableList);
  if (rows == NULL) return -1;
  int rowNum = rows->entries();
  Space space;
  if (rowNum > 1) {
    NAString total;
    total.format("%d directives.", rowNum - 1);
    outputShortLine(space, total.data());
    // calculate column width
    // currently, we have 9 columns
    const int COLNUM = 9;
    const char *headerFields[COLNUM] = {"ID",           "POOL",         "REPL",         "EXPIRY",      "PATH",
                                        "BYTES_NEEDED", "BYTES_CACHED", "FILES_NEEDED", "FILES_CACHED"};
    int columnWidths[COLNUM];
    int headerlen = 0;
    // extract max width of each field
    HbaseStr *hbaseStr = &rows->at(rowNum - 1);
    // std::string * lastRow = rows->get(rowNum-1);
    NAString nsRow(hbaseStr->val, hbaseStr->len, heap);
    NAList<NAString> fields(heap);
    nsRow.split('|', fields);
    // initialize width of each column.
    for (int i = 0; i < COLNUM; i++) {
      int fieldWidth = atoi(fields[i].data());
      int headerFieldWidth = strlen(headerFields[i]);
      columnWidths[i] = fieldWidth > headerFieldWidth ? fieldWidth : headerFieldWidth;
      columnWidths[i] += 2;
      // calculate total length of header line.
      headerlen += columnWidths[i];
    }
    // write header, same output as hdfs cacheadmin -listDirectives -stats.
    NAString line(' ', headerlen, heap);
    for (int i = 0, pos = 0; i < COLNUM; i++) {
      line.replace(pos, strlen(headerFields[i]), headerFields[i]);
      pos += columnWidths[i];
    }
    outputShortLine(space, line.data());

    // write each row.
    for (int i = 0; i < rowNum - 1; i++) {
      line.fill(0, ' ', line.length());
      HbaseStr *hbaseStr = &rows->at(i);
      // std::string * oneRow = rows->get(i);
      NAString nsRow(hbaseStr->val, hbaseStr->len, heap);
      NAList<NAString> fields(heap);
      nsRow.split('|', fields);
      CMPASSERT(fields.entries() == 9);
      for (int j = 0, pos = 0; j < fields.entries(); j++) {
        line.replace(pos, fields[j].length(), fields[j]);
        pos += columnWidths[j];
      }
      outputShortLine(space, line.data());
    }
  } else {  // this table has no entry in hdfs cache.
    std::string msg;
    msg += "Schema ";
    msg += schemaText.data();
    msg += " is not in HDFS cache.";
    outputShortLine(space, "");
    outputShortLine(space, msg.c_str());
  }
  deleteNAArray(STMTHEAP, rows);
  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);
  return 0;
}

static short CmpDescribeShowEnv(char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  Space space;

#ifdef _EMPTYSTRING_EQUIVALENT_NULL
  outputShortLine(space, "EMPTY STRING EQUIVALENT NULL : ENABLE");
#else
  outputShortLine(space, "EMPTY STRING EQUIVALENT NULL : DISABLE");
#endif

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  return 0;
}

static short CmpDescribeTablePartitions(NATable *naTable, Space &space, char *buf, short type) {
  Int32 partType = naTable->partitionType();
  NAString colName;
  const NAPartitionArray &partArray = naTable->getPartitionArray();
  naTable->getParitionColNameAsString(colName, FALSE);
  // range list hash
  sprintf(buf, "  PARTITION BY %s (%s)", naTable->partitionSchemeToString((ComPartitioningSchemeV2)partType),
          colName.data());
  outputShortLine(space, buf);

  // SUBPARTITION DISPLAY THERE
  if (FALSE) {
    sprintf(buf, "SUBPARTITION BY  ()");
    outputShortLine(space, buf);
  }

  outputShortLine(space, "  ( ");

  NAString tempInfo;

  for (Int32 i = 0; i < naTable->FisrtLevelPartitionCount(); i++) {
    if (type == 1) {
      tempInfo.format(
          "\n    -- Definition of Trafodion Table %s\n"
          "    -- Definition of hbase table %s",
          partArray[i]->getPartitionEntityName(), partArray[i]->getPartitionEntityName());
      outputShortLine(space, tempInfo.data());
    }
    CmpDescribePartitions(partType, partArray[i], space, buf, TRUE, (i == 0));
    if (partArray[i]->hasSubPartition()) CmpDescribePartitions(partType, partArray[i], space, buf, FALSE, (i == 0));
  }
  outputShortLine(space, "  ) ");
  return 0;
}

static short CmpDescribePartitions(Int32 partType, NAPartition *naPartition, Space &space, char *buf,
                                   NABoolean isFirstPartition, NABoolean isFirst) {
  NAString dummyText, valueText;
  naPartition->boundaryValueToString(dummyText, valueText);
  if (partType == COM_RANGE_PARTITION)
    sprintf(buf, "  %s%s %s VALUES LESS THAN (%s)", !isFirst ? ", " : "  ",
            isFirstPartition ? "PARTITION" : "SUBPARTITION", ToAnsiIdentifier(naPartition->getPartitionName()).data(),
            valueText.data());
  else if (partType == COM_LIST_PARTITION)
    sprintf(buf, "  %s%s %s VALUES (%s)", !isFirst ? ", " : "  ", isFirstPartition ? "PARTITION" : "SUBPARTITION",
            ToAnsiIdentifier(naPartition->getPartitionName()).data(), valueText.data());
  else {
    // hash partition and other partition type will added there
  }
  outputShortLine(space, buf);
  return 0;
}

static short CmpDescribePartIndexDesc(BindWA *bindWA, short type, NATable *naTable, Space &space, char *buf,
                                      const CorrName &dtName, const CorrName *likeTabName) {
  const NAPartitionArray &partArray = naTable->getPartitionArray();
  for (Int32 i = 0; i < naTable->FisrtLevelPartitionCount(); i++) {
    // get natable for partition entity table
    CorrName partEntityName(partArray[i]->getPartitionEntityName(), STMTHEAP,
                            dtName.getQualifiedNameObj().getSchemaName(),
                            dtName.getQualifiedNameObj().getCatalogName());

    NATable *partNATable = bindWA->getNATableInternal((CorrName &)partEntityName);

    const NAFileSetList &indexList = partNATable->getIndexList();

    // partition table name
    const NAString &tableName = dtName.getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE);
    // Should we extract the code that shows index?
    for (Int32 j = 0; j < indexList.entries(); j++) {
      const NAFileSet *naf = indexList[j];

      // show normal partion index
      if ((naf->getKeytag() == 0) || (naf->partLocalIndex()) || (naf->partGlobalIndex())) continue;

      const QualifiedName &qn = naf->getFileSetName();
      const NAString &indexName = qn.getUnqualifiedObjectNameAsAnsiString();

      NAString newIndexName;
      if (type == 3) {
        char partitionIndexName[256];
        getPartitionIndexName(partitionIndexName, 256, indexName, likeTabName->getQualifiedNameObj().getObjectName());
        newIndexName = NAString(partitionIndexName);
      }

      char vu[40];
      strcpy(vu, " ");
      if (naf->uniqueIndex()) strcat(vu, "UNIQUE ");
      // ngram index
      if (naf->ngramIndex()) strcat(vu, "NGRAM ");
      sprintf(
          buf, "%sCREATE%sINDEX %s ON %s", (naf->isCreatedExplicitly() ? "\n" : ""), vu,
          type == 3 ? newIndexName.data() : indexName.data(),
          type == 3 ? likeTabName->getQualifiedNameObj().getQualifiedNameAsAnsiString(TRUE).data() : tableName.data());

      outputLine(space, buf, 0);

      int numIndexCols = naf->getCountOfColumns(TRUE,
                                                  TRUE,  // user-specified index cols only
                                                  FALSE, FALSE);
      cmpDisplayPrimaryKey(naf->getIndexKeyColumns(), numIndexCols, FALSE, space, buf, FALSE, TRUE, TRUE);

      // now is only process parttiion, subpartition will added if it support
      sprintf(buf, "  PARTITION (%s)", partArray[i]->getPartitionName());
      outputShortLine(space, buf);

      // does we need salt and hbase information for partition index ?

      outputShortLine(space, ";");
    }
  }

  return 0;
}

static short CmpDescribeShowViews(char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  Space space;

  outputShortLine(space, "");
  outputShortLine(space, "v$querycache");
  outputShortLine(space, "v$querycacheentries");
  outputShortLine(space, "v$hybridquerycache");
  outputShortLine(space, "v$hybridquerycacheentries");
  outputShortLine(space, "v$natablecache");
  outputShortLine(space, "v$natablecacheentries");
  outputShortLine(space, "v$naroutinecache");
  outputShortLine(space, "v$naroutineactioncache");
  outputShortLine(space, "v$clusterstats");
  outputShortLine(space, "v$regionstats");

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  return 0;
}

static short CmpDescribeDescView(const CorrName &dtName, char *&outbuf, ULng32 &outbuflen, NAMemory *heap) {
  Space space;

  outputShortLine(space, "");

  NAString viewName = dtName.getQualifiedNameObj().getObjectName();
  if (viewName == NAString("QUERYCACHE")) {
    outputShortLine(space, "AVG_TEMPLATE_SIZE         INT UNSIGNED");
    outputShortLine(space, "CURRENT_SIZE              INT UNSIGNED");
    outputShortLine(space, "MAX_CACHE_SIZE            INT UNSIGNED");
    outputShortLine(space, "MAX_NUM_VICTIMS           INT UNSIGNED");
    outputShortLine(space, "NUM_ENTRIES               INT UNSIGNED");
    outputShortLine(space, "NUM_PLANS                 INT UNSIGNED");
    outputShortLine(space, "NUM_COMPILES              INT UNSIGNED");
    outputShortLine(space, "NUM_RECOMPILES            INT UNSIGNED");
    outputShortLine(space, "NUM_RETRIES               INT UNSIGNED");
    outputShortLine(space, "NUM_CACHEABLE_PARSING     INT UNSIGNED");
    outputShortLine(space, "NUM_CACHEABLE_BINDING     INT UNSIGNED");
    outputShortLine(space, "NUM_CACHE_HITS_PARSING    INT UNSIGNED");
    outputShortLine(space, "NUM_CACHE_HITS_BINDING    INT UNSIGNED");
    outputShortLine(space, "NUM_CACHEABLE_TOO_LARGE   INT UNSIGNED");
    outputShortLine(space, "NUM_DISPLACED             INT UNSIGNED");
    outputShortLine(space, "OPTIMIZATION_LEVEL        CHAR(10 BYTES) character set utf8");
    outputShortLine(space, "TEXT_CACHE_HITS           INT UNSIGNED");
    outputShortLine(space, "AVG_TEXT_SIZE             INT UNSIGNED");
    outputShortLine(space, "TEXT_ENTRIES              INT UNSIGNED");
    outputShortLine(space, "DISPLACED_TEXTS           INT UNSIGNED");
    outputShortLine(space, "NUM_LOOKUPS               INT UNSIGNED");
  } else if (viewName == NAString("QUERYCACHEENTRIES")) {
    outputShortLine(space, "ROW_ID                        INT UNSIGNED");
    outputShortLine(space, "PLAN_ID                       LARGEINT");
    outputShortLine(space, "TEXT                          VARCHAR(4096 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "ENTRY_SIZE                    INT UNSIGNED");
    outputShortLine(space, "PLAN_LENGTH                   INT UNSIGNED");
    outputShortLine(space, "NUM_HITS                      INT UNSIGNED");
    outputShortLine(space, "PHASE                         CHAR(10 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "OPTIMIZATION_LEVEL            CHAR(10 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "CATALOG_NAME                  CHAR(40 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "SCHEMA_NAME                   CHAR(40 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "NUM_PARAMS                    INT UNSIGNED");
    outputShortLine(space, "PARAM_TYPES                   VARCHAR(1024 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "COMPILATION_TIME              INT UNSIGNED");
    outputShortLine(space, "AVERAGE_HIT_TIME              INT UNSIGNED");
    outputShortLine(space, "SHAPE                         VARCHAR(1024 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "ISOLATION_LEVEL               CHAR(20 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "ISOLATION_LEVEL_FOR_UPDATES   CHAR(20 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "ACCESS_MODE                   CHAR(20 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "AUTO_COMMIT                   CHAR(15 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "ROLLBACK_MODE                 CHAR(15 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "HASH                          LARGEINT");
    outputShortLine(space, "HDFS_OFFSET                   LARGEINT");
    outputShortLine(space, "FLAGS                         SMALLINT");
  } else if (viewName == NAString("HYBRIDQUERYCACHE")) {
    outputShortLine(space, "NUM_HKEYS                     INT UNSIGNED");
    outputShortLine(space, "NUM_SKEYS                     INT UNSIGNED");
    outputShortLine(space, "NUM_MAX_VALUES_PER_KEY        INT UNSIGNED");
    outputShortLine(space, "NUM_HASH_TABLE_BUCKETS        INT UNSIGNED");
    outputShortLine(space, "HQC_HEAP_SIZE                 INT UNSIGNED");
  } else if (viewName == NAString("HYBRIDQUERYCACHEENTRIES")) {
    outputShortLine(space, "PLAN_ID                  LARGEINT");
    outputShortLine(space, "HKEY                     VARCHAR(4096 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "SKEY                     VARCHAR(4096 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "NUM_HITS                 INT UNSIGNED");
    outputShortLine(space, "NUM_PLITERALS            INT UNSIGNED");
    outputShortLine(space, "PLITERALS                VARCHAR(4096 BYTES) CHARACTER SET UTF8");
    outputShortLine(space, "NUM_NPLITERALS           INT UNSIGNED");
    outputShortLine(space, "NPLITERALS               VARCHAR(4096 BYTES) CHARACTER SET UTF8");
  } else if (viewName == NAString("NATABLECACHE")) {
    outputShortLine(space, "CONTEXT                  CHAR(8) CHARACTER SET ISO88591");
    outputShortLine(space, "NUM_LOOKUPS              INT UNSIGNED");
    outputShortLine(space, "NUM_CACHE_HITS           INT UNSIGNED");
    outputShortLine(space, "NUM_ENTRIES              INT UNSIGNED");
    outputShortLine(space, "CURRENT_CACHE_SIZE       INT UNSIGNED");
    outputShortLine(space, "HIGH_WATERMARK           INT UNSIGNED");
    outputShortLine(space, "MAX_CACHE_SIZE           INT UNSIGNED");
  } else if (viewName == NAString("NATABLECACHEENTRIES")) {
    outputShortLine(space, "ROW_ID           INT UNSIGNED");
    outputShortLine(space, "CATALOG_NAME     VARCHAR(128) CHARACTER SET UTF8");
    outputShortLine(space, "SCHEMA_NAME      VARCHAR(128) CHARACTER SET UTF8");
    outputShortLine(space, "OBJECT_NAME      VARCHAR(128) CHARACTER SET UTF8");
    outputShortLine(space, "ENTRY_SIZE       INT");
  } else if (viewName == NAString("NAROUTINECACHE")) {
    outputShortLine(space, "NUM_LOOKUPS              INT UNSIGNED");
    outputShortLine(space, "NUM_CACHE_HITS           INT UNSIGNED");
    outputShortLine(space, "NUM_ENTRIES              INT UNSIGNED");
    outputShortLine(space, "CURRENT_CACHE_SIZE       INT UNSIGNED");
    outputShortLine(space, "HIGH_WATERMARK           INT UNSIGNED");
    outputShortLine(space, "MAX_CACHE_SIZE           INT UNSIGNED");
  } else if (viewName == NAString("NAROUTINEACTIONCACHE")) {
    outputShortLine(space, "NUM_LOOKUPS              INT UNSIGNED");
    outputShortLine(space, "NUM_CACHE_HITS           INT UNSIGNED");
    outputShortLine(space, "NUM_ENTRIES              INT UNSIGNED");
    outputShortLine(space, "CURRENT_CACHE_SIZE       INT UNSIGNED");
    outputShortLine(space, "HIGH_WATERMARK           INT UNSIGNED");
    outputShortLine(space, "MAX_CACHE_SIZE           INT UNSIGNED");
  } else if (viewName == NAString("CLUSTERSTATS")) {
    outputShortLine(space, "REGION_SERVER            CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "REGION_NAME              CHAR(512) CHARACTER SET UTF8");
    outputShortLine(space, "REGION_NUM               INT");
    outputShortLine(space, "CATALOG_NAME             CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "SCHEMA_NAME              CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "OBJECT_NAME              CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "NUM_STORES               INT");
    outputShortLine(space, "NUM_STORE_FILES          INT");
    outputShortLine(space, "STORE_FILE_UNCOMP_SIZE   LARGEINT");
    outputShortLine(space, "STORE_FILE_SIZE          LARGEINT");
    outputShortLine(space, "MEM_STORE_SIZE           LARGEINT");
    outputShortLine(space, "READ_REQUESTS_COUNT      LARGEINT");
    outputShortLine(space, "WRITE_REQUESTS_COUNT     LARGEINT");
  } else if (viewName == NAString("REGIONSTATS")) {
    outputShortLine(space, "CATALOG_NAME             CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "SCHEMA_NAME              CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "OBJECT_NAME              CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "REGION_SERVER            CHAR(256) CHARACTER SET UTF8");
    outputShortLine(space, "REGION_NUM               INT");
    outputShortLine(space, "REGION_NAME              CHAR(512) CHARACTER SET UTF8");
    outputShortLine(space, "NUM_STORES               INT");
    outputShortLine(space, "NUM_STORE_FILES          INT");
    outputShortLine(space, "STORE_FILE_UNCOMP_SIZE   LARGEINT");
    outputShortLine(space, "STORE_FILE_SIZE          LARGEINT");
    outputShortLine(space, "MEM_STORE_SIZE           LARGEINT");
    outputShortLine(space, "READ_REQUESTS_COUNT      LARGEINT");
    outputShortLine(space, "WRITE_REQUESTS_COUNT     LARGEINT");
  }

  outbuflen = space.getAllocatedSpaceSize();
  outbuf = new (heap) char[outbuflen];
  space.makeContiguous(outbuf, outbuflen);

  return 0;
}
