

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLcommentOn.cpp
 * Description:  Implements comments for SQL objects
 *
 *
 * Created:     8/17/2017
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/ComCextdecs.h"
#include "common/ComObjectName.h"
#include "common/ComUser.h"
#include "common/NumericType.h"
#include "executor/ExExeUtilCli.h"
#include "exp/ExpHbaseInterface.h"
#include "generator/Generator.h"
#include "optimizer/SchemaDB.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "parser/StmtDDLCommentOn.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlcomp/CmpDescribe.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"

static char *doEscapeComment(char *src, CollHeap *heap) {
  char *ret = NULL;
  int head = 0;
  int tail = 0;
  int src_len = strlen(src);

  if (NULL == src) {
    NAAssert("invalid comment string fetched", __FILE__, __LINE__);
  }

  ret = new (heap) char[src_len * 2 + 1];
  ret[0] = '\0';
  for (; tail <= src_len; tail++) {
    if (src[tail] == '\'') {
      strncat(ret, src + head, tail - head + 1);
      strcat(ret, "'");
      head = tail + 1;
    } else if (src[tail] == '\0') {
      strncat(ret, src + head, tail - head + 1);
    }
  }

  return ret;
}

short CmpSeabaseDDL::getSeabaseObjectComment(long object_uid, enum ComObjectType object_type,
                                             ComTdbVirtObjCommentInfo &comment_info, CollHeap *heap) {
  int retcode = 0;
  int cliRC = 0;

  char query[4000];

  comment_info.objectUid = object_uid;
  comment_info.objectComment = NULL;
  comment_info.numColumnComment = 0;
  comment_info.columnCommentArray = NULL;
  comment_info.numIndexComment = 0;
  comment_info.indexCommentArray = NULL;

  ExeCliInterface cliInterface(STMTHEAP, NULL, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  // get object comment
  sprintf(query, "select TEXT from %s.\"%s\".%s where TEXT_UID = %ld and TEXT_TYPE = %d and SUB_ID = %d ; ",
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT, object_uid, COM_OBJECT_COMMENT_TEXT, 0);

  Queue *objQueue = NULL;
  cliRC = cliInterface.fetchAllRows(objQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return -1;
  }

  // We should have only 1 comment for object
  if (objQueue->numEntries() > 0) {
    objQueue->position();
    OutputInfo *vi = (OutputInfo *)objQueue->getNext();
    comment_info.objectComment = doEscapeComment((char *)vi->get(0), heap);
  }

  // get index comments of table
  if (COM_BASE_TABLE_OBJECT == object_type) {
    sprintf(query,
            "select O.CATALOG_NAME||'.'||O.SCHEMA_NAME||'.'||O.OBJECT_NAME as INDEX_QUAL, T.TEXT "
            "from %s.\"%s\".%s as O, %s.\"%s\".%s as T, %s.\"%s\".%s as I "
            "where I.BASE_TABLE_UID = %ld and O.OBJECT_UID = I.INDEX_UID and T.TEXT_UID = O.OBJECT_UID "
            "  and T.TEXT_TYPE = %d and SUB_ID = %d "
            "order by INDEX_QUAL ; ",
            getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
            getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES, object_uid, COM_OBJECT_COMMENT_TEXT, 0);

    Queue *indexQueue = NULL;
    cliRC = cliInterface.fetchAllRows(indexQueue, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return -1;
    }

    if (indexQueue->numEntries() > 0) {
      comment_info.numIndexComment = indexQueue->numEntries();
      comment_info.indexCommentArray = new (heap) ComTdbVirtIndexCommentInfo[comment_info.numIndexComment];

      indexQueue->position();
      for (int idx = 0; idx < comment_info.numIndexComment; idx++) {
        OutputInfo *oi = (OutputInfo *)indexQueue->getNext();
        ComTdbVirtIndexCommentInfo &indexComment = comment_info.indexCommentArray[idx];

        // get the index full name
        indexComment.indexFullName = new (heap) char[strlen((char *)oi->get(0)) + 1];
        strcpy((char *)indexComment.indexFullName, (char *)oi->get(0));
        indexComment.indexComment = doEscapeComment((char *)oi->get(1), heap);
      }
    }
  }

  // get column comments of table and view
  if (COM_BASE_TABLE_OBJECT == object_type || COM_VIEW_OBJECT == object_type) {
    sprintf(
        query,
        "select C.COLUMN_NAME, T.TEXT from %s.\"%s\".%s as C, %s.\"%s\".%s as T "
        "where C.OBJECT_UID = %ld and T.TEXT_UID = C.OBJECT_UID and T.TEXT_TYPE = %d and C.COLUMN_NUMBER = T.SUB_ID "
        "order by C.COLUMN_NUMBER ; ",
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
        object_uid, COM_COLUMN_COMMENT_TEXT);

    Queue *colQueue = NULL;
    cliRC = cliInterface.fetchAllRows(colQueue, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return -1;
    }

    if (colQueue->numEntries() > 0) {
      comment_info.numColumnComment = colQueue->numEntries();
      comment_info.columnCommentArray = new (heap) ComTdbVirtColumnCommentInfo[comment_info.numColumnComment];

      colQueue->position();
      for (int idx = 0; idx < comment_info.numColumnComment; idx++) {
        OutputInfo *oi = (OutputInfo *)colQueue->getNext();
        ComTdbVirtColumnCommentInfo &colComment = comment_info.columnCommentArray[idx];

        // get the column name
        colComment.columnName = new (heap) char[strlen((char *)oi->get(0)) + 1];
        strcpy((char *)colComment.columnName, (char *)oi->get(0));
        colComment.columnComment = doEscapeComment((char *)oi->get(1), heap);
      }
    }
  }

  return 0;
}

void CmpSeabaseDDL::doSeabaseCommentOn(StmtDDLCommentOn *commentOnNode, NAString &currCatName, NAString &currSchName) {
  int cliRC;
  int retcode;

  enum ComObjectType enMDObjType = COM_UNKNOWN_OBJECT;

  ComObjectName objectName(commentOnNode->getObjectName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  objectName.applyDefaults(currCatAnsiName, currSchAnsiName);

  enum StmtDDLCommentOn::COMMENT_ON_TYPES commentObjectType = commentOnNode->getObjectType();

  switch (commentObjectType) {
    case StmtDDLCommentOn::COMMENT_ON_TYPE_TABLE:
      enMDObjType = COM_BASE_TABLE_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_COLUMN:
      if (TRUE == commentOnNode->getIsViewCol()) {
        enMDObjType = COM_VIEW_OBJECT;
      } else {
        enMDObjType = COM_BASE_TABLE_OBJECT;
      }
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_INDEX:
      enMDObjType = COM_INDEX_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_SCHEMA:
      enMDObjType = COM_PRIVATE_SCHEMA_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_VIEW:
      enMDObjType = COM_VIEW_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_LIBRARY:
      enMDObjType = COM_LIBRARY_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_PROCEDURE:
    case StmtDDLCommentOn::COMMENT_ON_TYPE_FUNCTION:
      enMDObjType = COM_USER_DEFINED_ROUTINE_OBJECT;
      break;

    case StmtDDLCommentOn::COMMENT_ON_TYPE_SEQUENCE:
      enMDObjType = COM_SEQUENCE_GENERATOR_OBJECT;

    default:
      break;
  }

  NAString catalogNamePart = objectName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = objectName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objNamePart = objectName.getObjectNamePartAsAnsiString(TRUE);

  const NAString extObjName = objectName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, NULL, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  long objUID = 0;
  int objectOwnerID = ROOT_USER_ID;
  int schemaOwnerID = ROOT_USER_ID;
  long objectFlags = 0;
  long objDataUID = 0;

  // get UID of object
  objUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objNamePart.data(), enMDObjType,
                         objectOwnerID, schemaOwnerID, objectFlags, objDataUID);
  if (objUID < 0 || objectOwnerID == 0 || schemaOwnerID == 0) {
    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extObjName);
    processReturn();
    return;
  }

  // Verify that the requester has COMMENT privilege.
  if (!isDDLOperationAuthorized(SQLOperation::COMMENT, schemaOwnerID, schemaOwnerID, objUID, enMDObjType, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  // check for overflow
  if (commentOnNode->getComment().length() > COM_MAXIMUM_LENGTH_OF_COMMENT) {
    *CmpCommon::diags() << DgSqlCode(-8402);
    processReturn();
    return;
  }

  NAString comment = commentOnNode->getCommentEscaped();

  // add, remove, change comment of object/column
  enum ComTextType textType = COM_OBJECT_COMMENT_TEXT;
  int subID = 0;

  if (StmtDDLCommentOn::COMMENT_ON_TYPE_COLUMN == commentObjectType) {
    textType = COM_COLUMN_COMMENT_TEXT;
    subID = commentOnNode->getColNum();
  }

  /* Not using function updateTextTable(), because can not insert Chinese properly by function updateTextTable().
   * For storing COMMENT in TEXT table is a temp solution, so updating TEXT table directly here.
   * Will change this implementation until next upgrade of MD.
   */
  // like function updateTextTable(), delete entry first
  cliRC = deleteFromTextTable(&cliInterface, objUID, textType, subID);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  if (comment.length() > 0) {
    // add or modify comment
    char query[2048];

    str_sprintf(query, "insert into %s.\"%s\".%s values (%ld, %d, %d, %d, 0, '%s') ; ", getSystemCatalog(),
                SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID, textType, subID, 0, comment.data());
    cliRC = cliInterface.executeImmediate(query);

    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return;
    }
  }
  if (enMDObjType == COM_BASE_TABLE_OBJECT || enMDObjType == COM_INDEX_OBJECT ||
      enMDObjType == COM_PRIVATE_SCHEMA_OBJECT || enMDObjType == COM_SEQUENCE_GENERATOR_OBJECT) {
    if (isMDflagsSet(objectFlags, MD_OBJECTS_INCR_BACKUP_ENABLED)) {
      cliRC = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objNamePart.data(),
                                       comObjectTypeLit(enMDObjType));
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        processReturn();
        return;
      }
    }
  }

  processReturn();
  return;
}
