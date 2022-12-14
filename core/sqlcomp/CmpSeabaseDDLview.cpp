

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLview.cpp
 * Description:  Implements ddl views for SQL/seabase tables.
 *
 *
 * Created:     6/30/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/ComObjectName.h"
#include "common/ComUser.h"
#include "common/ComViewColUsage.h"
#include "executor/ExExeUtilCli.h"
#include "exp/ExpHbaseInterface.h"
#include "generator/Generator.h"
#include "optimizer/SchemaDB.h"
#include "parser/ElemDDLColDefArray.h"
#include "parser/ElemDDLColRefArray.h"
#include "parser/StmtDDLCreateView.h"
#include "parser/StmtDDLDropView.h"
#include "sqlcomp/CmpSeabaseDDLincludes.h"

// for privilege checking
#include <bitset>

#include "common/ComCextdecs.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrDefs.h"
#include "sqlcomp/PrivMgrPrivileges.h"

static bool checkAccessPrivileges(const ParTableUsageList &vtul, const ParViewColTableColsUsageList &vctcul,
                                  NABoolean viewCreator, int userID, PrivMgrBitmap &privilegesBitmap,
                                  PrivMgrBitmap &grantableBitmap);

short CmpSeabaseDDL::buildViewText(StmtDDLCreateView *createViewParseNode, NAString &viewText) {
  const ParNameLocList &nameLocList = createViewParseNode->getNameLocList();
  const char *pInputStr = nameLocList.getInputStringPtr();

  StringPos inputStrPos = createViewParseNode->getStartPosition();

  for (CollIndex i = 0; i < nameLocList.entries(); i++) {
    const ParNameLoc &nameLoc = nameLocList[i];
    const NAString &nameExpanded = nameLoc.getExpandedName(FALSE /*no assert*/);
    size_t nameAsIs = 0;
    size_t nameLenInBytes = 0;
    size_t nameLenInNAWchars = 0;

    //
    // When the character set of the input string is a variable-length/width
    // multi-byte characters set, the value returned by getNameLength()
    // may not be numerically equal to the number of bytes in the original
    // input string that we need to skip.  So, we get the character
    // conversion routines to tell us how many bytes we need to skip.
    //
    CMPASSERT(nameLocList.getInputStringCharSet() EQU CharInfo::UTF8);
    enum cnv_charset eCnvCS = convertCharsetEnum(nameLocList.getInputStringCharSet());

    const char *str_to_test = (const char *)&pInputStr[nameLoc.getNamePosition()];
    const int max_bytes2cnv = createViewParseNode->getEndPosition() - nameLoc.getNamePosition() + 1;
    const char *tmp_out_bufr = new (STMTHEAP) char[max_bytes2cnv * 4 + 10 /* Ensure big enough! */];
    char *p1stUnstranslatedChar = NULL;
    UInt32 iTransCharCountInChars = 0;
    int cnvErrStatus = LocaleToUTF16(cnv_version1  // in  - const enum cnv_version version
                                     ,
                                     str_to_test  // in  - const char *in_bufr
                                     ,
                                     max_bytes2cnv  // in  - const int in_len
                                     ,
                                     tmp_out_bufr  // out - const char *out_bufr
                                     ,
                                     max_bytes2cnv * 4 + 1  // in  - const int out_len
                                     ,
                                     eCnvCS  // in  - enum cnv_charset charset
                                     ,
                                     p1stUnstranslatedChar  // out - char * & first_untranslated_char
                                     ,
                                     NULL  // out - unsigned int *output_data_len_p
                                     ,
                                     0  // in  - const int cnv_flags
                                     ,
                                     (int)TRUE  // in  - const int addNullAtEnd_flag
                                     ,
                                     &iTransCharCountInChars  // out - unsigned int * translated_char_cnt_p
                                     ,
                                     nameLoc.getNameLength()  // in  - unsigned int max_NAWchars_to_convert
    );
    // NOTE: No errors should be possible -- string has been converted before.

    NADELETEBASIC(tmp_out_bufr, STMTHEAP);
    nameLenInBytes = p1stUnstranslatedChar - str_to_test;

    // If name not expanded, then use the original name as is
    if (nameExpanded.isNull()) nameAsIs = nameLenInBytes;

    // Copy from (last position in) input string up to current name
    viewText += NAString(&pInputStr[inputStrPos], nameLoc.getNamePosition() - inputStrPos + nameAsIs);

    if (NOT nameAsIs)  // original name to be replaced with expanded
    {
      size_t namePos = nameLoc.getNamePosition();
      size_t nameLen = nameLoc.getNameLength();

      if ((/* case #1 */ pInputStr[namePos] EQU '*' OR
               /* case #2 */ pInputStr[namePos] EQU '"')AND nameExpanded.data()[0] NEQ '"' AND namePos >
          1 AND(pInputStr[namePos - 1] EQU '_' OR isAlNumIsoMapCS((unsigned char)pInputStr[namePos - 1]))) {
        // insert a blank separator to avoid syntax error
        // WITHOUT FIX
        // ex#1: CREATE VIEW C.S.V AS SELECTC.S.T.COL FROM C.S.T
        // ex#2: CREATE VIEW C.S.V AS SELECTC.S.T.COL FROM C.S.T
        viewText += " ";  // the FIX
        // WITH FIX
        // ex#1: CREATE VIEW C.S.V AS SELECT C.S.T.COL FROM C.S.T
        // ex#2: CREATE VIEW C.S.V AS SELECT C.S.T.COL FROM C.S.T
      }

      // Add the expanded (fully qualified) name (if exists)
      viewText += nameExpanded;

      if ((/* case #3 */ (pInputStr[namePos] EQU '*' AND nameLen EQU 1)OR
               /* case #4 */ pInputStr[namePos + nameLen - 1] EQU '"')
              AND nameExpanded.data()[nameExpanded.length() - 1] NEQ '"' AND pInputStr[namePos + nameLen] NEQ '\0' AND(
                  pInputStr[namePos + nameLen] EQU '_' OR isAlNumIsoMapCS(
                      (unsigned char)pInputStr[namePos + nameLen]))) {
        // insert a blank separator to avoid syntax error
        // WITHOUT FIX
        // ex: CREATE VIEW C.S.V AS SELECT C.S.T.COLFROM C.S.T
        viewText += " ";  // the FIX
        // WITH FIX
        // ex: CREATE VIEW C.S.V AS SELECT C.S.T.COL FROM C.S.T
      }
    }  // if (NOT nameAsIs)

    // Advance input pointer beyond original name in input string
    inputStrPos = nameLoc.getNamePosition() + nameLenInBytes /* same as nameLenInNAWchars */;

  }  // for

  if (createViewParseNode->getEndPosition() >= inputStrPos) {
    viewText += NAString(&pInputStr[inputStrPos], createViewParseNode->getEndPosition() + 1 - inputStrPos);
  } else
    CMPASSERT(createViewParseNode->getEndPosition() == inputStrPos - 1);

  PrettifySqlText(viewText, CharType::getCharSetAsPrefix(SqlParser_NATIONAL_CHARSET));

  return 0;
}  // CmpSeabaseDDL::buildViewText()

short CmpSeabaseDDL::buildViewColInfo(StmtDDLCreateView *createViewParseNode, ElemDDLColDefArray *colDefArray) {
  // Builds the list of ElemDDLColDef parse nodes from the list of
  // NAType parse nodes derived from the query expression parse sub-tree
  // and the list of ElemDDLColViewDef parse nodes from the parse tree.
  // This extra step is needed to invoke (reuse) global func CatBuildColumnList.

  CMPASSERT(createViewParseNode->getQueryExpression()->getOperatorType() EQU REL_ROOT);

  RelRoot *pQueryExpr = (RelRoot *)createViewParseNode->getQueryExpression();

  const ValueIdList &valIdList = pQueryExpr->compExpr();  // select-list
  CMPASSERT(valIdList.entries() > 0);

  CollIndex numOfCols(createViewParseNode->getViewColDefArray().entries());
  if (numOfCols NEQ valIdList.entries()) {
    *CmpCommon::diags() << DgSqlCode(-1108)  // CAT_NUM_OF_VIEW_COLS_NOT_MATCHED
                        << DgInt0(numOfCols) << DgInt1(valIdList.entries());
    return -1;
  }

  const ElemDDLColViewDefArray &viewColDefArray = createViewParseNode->getViewColDefArray();
  for (CollIndex i = 0; i < numOfCols; i++) {
    // ANSI 11.19 SR8
    if (viewColDefArray[i]->getColumnName().isNull()) {
      *CmpCommon::diags() << DgSqlCode(-1099)  // CAT_VIEW_COLUMN_UNNAMED
                          << DgInt0(i + 1);
      return -1;
    }

    colDefArray->insert(new (STMTHEAP)
                            ElemDDLColDef(NULL, &viewColDefArray[i]->getColumnName(), (NAType *)&valIdList[i].getType(),
                                          NULL  // col attr list (not needed)

                                          ,
                                          STMTHEAP));

    if (viewColDefArray[i]->isHeadingSpecified()) {
      (*colDefArray)[i]->setIsHeadingSpecified(TRUE);
      (*colDefArray)[i]->setHeading(viewColDefArray[i]->getHeading());
    }
  }

  return 0;
}

// Build view column usages -> relate view-col <=> referenced-col
// This relationship is a string of values that gets stored in the TEXT table
short CmpSeabaseDDL::buildViewTblColUsage(const StmtDDLCreateView *createViewParseNode,
                                          const ComTdbVirtTableColumnInfo *colInfoArray, const long objUID,
                                          NAString &viewColUsageText) {
  const ParViewUsages &vu = createViewParseNode->getViewUsages();
  const ParViewColTableColsUsageList &vctcul = vu.getViewColTableColsUsageList();
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);

  for (size_t i = 0; i < vctcul.entries(); i++) {
    const ParViewColTableColsUsage &vctcu = vctcul[i];
    int32_t usingColNum = vctcu.getUsingViewColumnNumber();

    // Get column number for referenced table
    const ColRefName &usedColRef = vctcu.getUsedObjectColumnName();
    ComObjectName usedObjName;
    usedObjName = usedColRef.getCorrNameObj().getQualifiedNameObj().getQualifiedNameAsAnsiString();

    const NAString catalogNamePart = usedObjName.getCatalogNamePartAsAnsiString();
    const NAString schemaNamePart = usedObjName.getSchemaNamePartAsAnsiString(TRUE);
    const NAString objectNamePart = usedObjName.getObjectNamePartAsAnsiString(TRUE);
    CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
    cn.setSpecialType(usedColRef.getCorrNameObj().getSpecialType());

    NATable *naTable = bindWA.getNATableInternal(cn);
    if (naTable == NULL) {
      SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in createSeabaseView");
      return -1;
    }

    const NAColumnArray &nacolArr = naTable->getNAColumnArray();
    ComString usedObjColName(usedColRef.getColName());
    const NAColumn *naCol = nacolArr.getColumn(usedObjColName);
    if (naCol == NULL) {
      *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR) << DgColumnName(usedObjColName);
      return -1;
    }

    ComViewColUsage colUsage(objUID, usingColNum, naTable->objectUid().get_value(), naCol->getPosition(),
                             naTable->getObjectType());
    NAString viewColUsageStr;
    colUsage.packUsage(viewColUsageStr);
    viewColUsageText += viewColUsageStr;
  }
  return 0;
}

const char *CmpSeabaseDDL::computeCheckOption(StmtDDLCreateView *createViewParseNode) {
  if (createViewParseNode->isWithCheckOptionSpecified()) {
    switch (createViewParseNode->getCheckOptionLevel()) {
      case COM_CASCADED_LEVEL:
        return COM_CASCADE_CHECK_OPTION_LIT;
        break;
      case COM_LOCAL_LEVEL:
        return COM_LOCAL_CHECK_OPTION_LIT;
        break;
      case COM_UNKNOWN_LEVEL:
        return COM_UNKNOWN_CHECK_OPTION_LIT;
        break;
      default:
        return COM_NONE_CHECK_OPTION_LIT;
        break;
    }  // switch
  } else {
    return COM_NONE_CHECK_OPTION_LIT;
  }

  return NULL;

}  // CmpSeabaseDDL::computeCheckOption()

short CmpSeabaseDDL::updateViewUsage(StmtDDLCreateView *createViewParseNode, long viewUID,
                                     ExeCliInterface *cliInterface, NAList<objectRefdByMe> *lockedBaseTableList) {
  const ParViewUsages &vu = createViewParseNode->getViewUsages();
  const ParTableUsageList &vtul = vu.getViewTableUsageList();

  char query[1000];
  char hiveObjsNoUsage[1010];
  hiveObjsNoUsage[0] = 0;
  for (CollIndex i = 0; i < vtul.entries(); i++) {
    ComObjectName usedObjName(vtul[i].getQualifiedNameObj().getQualifiedNameAsAnsiString(), vtul[i].getAnsiNameSpace());

    NAString catalogNamePart = usedObjName.getCatalogNamePartAsAnsiString();
    NAString schemaNamePart = usedObjName.getSchemaNamePartAsAnsiString(TRUE);
    const NAString objectNamePart = usedObjName.getObjectNamePartAsAnsiString(TRUE);
    const NAString extUsedObjName = usedObjName.getExternalName(TRUE);

    if (usedObjName.isHBaseMappedExtFormat()) {
      ComConvertHBaseMappedExtToInt(catalogNamePart, schemaNamePart, catalogNamePart, schemaNamePart);
    }

    char objType[10];
    long usedObjUID = -1;
    CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);

    // If a sequence, set correct type to get a valid NATable entry
    bool isSeq = (vtul[i].getSpecialType() == ExtendedQualName::SG_TABLE) ? true : false;
    if (isSeq) cn.setSpecialType(ExtendedQualName::SG_TABLE);

    BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);

    NATable *naTable = bindWA.getNATableInternal(cn);
    if (naTable == NULL) {
      SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in updateViewUsage");
      return -1;
    }

    if (isLockedForBR(naTable->objectUid().castToInt64(), naTable->getSchemaUid().castToInt64(), cliInterface, cn)) {
      return -1;
    }

    // Create a DDL lock, if this is a base table
    // A subsequent commit/rollback will remove it
    if ((naTable->getObjectType() == COM_BASE_TABLE_OBJECT) && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
      // Table exists, update the epoch value to lock out other user access
      short locked = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (locked < 0)
        return -1;
      else if ((locked > 0) && lockedBaseTableList) {
        // A DDL lock was obtained; enter the name in the lockedBaseTableList
        // if the caller wants it
        objectRefdByMe objectInfo;
        objectInfo.objectType = naTable->getObjectType();
        objectInfo.objectUID = naTable->objectUid().castToInt64();
        const QualifiedName &qualifiedName = naTable->getTableName();
        objectInfo.catalogName = qualifiedName.getCatalogName();
        objectInfo.schemaName = qualifiedName.getSchemaName();
        objectInfo.objectName = qualifiedName.getObjectName();
        lockedBaseTableList->insert(objectInfo);
      }
    }

    if ((naTable->hasCompositeColumns()) && (naTable->isHiveTable()) &&
        (CmpCommon::getDefault(HIVE_COMPOSITE_DATATYPE_SUPPORT) != DF_ALL)) {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Cannot create Trafodion view on a Hive table with composite columns.");
      return -1;
    }

    if (((CmpCommon::getDefault(HIVE_VIEWS) == DF_ON) && (catalogNamePart == HIVE_SYSTEM_CATALOG)) ||
        (catalogNamePart == HBASE_SYSTEM_CATALOG)) {
      if (((naTable->isHiveTable()) && (CmpCommon::getDefault(HIVE_NO_REGISTER_OBJECTS) == DF_OFF)) ||
          ((naTable->isHbaseCellTable()) || (naTable->isHbaseRowTable()))) {
        // register this object in traf metadata, if not already
        str_sprintf(query, "register internal %s %s if not exists %s.\"%s\".\"%s\" %s",
                    (naTable->isHiveTable() ? "hive" : "hbase"), (naTable->isView() ? "view" : "table"),
                    catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                    (naTable->isView() ? "cascade" : " "));
        int cliRC = cliInterface->executeImmediate(query);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        // remove NATable and reload it to include object uid of register
        // operation.
        ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_MINE_ONLY,
                                                        (naTable->isView() ? COM_VIEW_OBJECT : COM_BASE_TABLE_OBJECT),
                                                        FALSE, FALSE);

        naTable = bindWA.getNATableInternal(cn);
        if (naTable == NULL) {
          SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in updateViewUsage");
          return -1;
        }
      }  // hive or hbase row/cell

      if (naTable->objectUid().get_value() > 0) {
        usedObjUID = naTable->objectUid().get_value();
        strcpy(objType, (naTable->isView() ? COM_VIEW_OBJECT_LIT : COM_BASE_TABLE_OBJECT_LIT));
      }

      // do not put in view usage list if it is not registered in traf
      if (usedObjUID == -1) {
        if (!naTable->getViewText()) appendErrorObjName(hiveObjsNoUsage, extUsedObjName);
        continue;
      }
    }  // hive or hbase table

    if (usedObjUID == -1)
      usedObjUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                                NULL, NULL, NULL, objType);
    if (usedObjUID < 0) {
      return -1;
    }

    // save the current parserflags setting
    /*      int savedCliParserFlags;
    SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedCliParserFlags_);
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
    */
    saveAllFlags();
    setAllFlags();
    str_sprintf(query, "upsert into %s.\"%s\".%s values (%ld, %ld, '%s', 0 )", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_VIEWS_USAGE, viewUID, usedObjUID, objType);
    int cliRC = cliInterface->executeImmediate(query);
    restoreAllFlags();
    // SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags);

    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      return -1;
    }

  }  // for

  // Views can also reference functions.  Add the list of functions
  // referenced to the VIEWS_USAGE table.
  const LIST(OptUDFInfo *) &uul = createViewParseNode->getUDFList();
  for (CollIndex u = 0; u < uul.entries(); u++) {
    char query[1000];
    str_sprintf(query, "upsert into %s.\"%s\".%s values (%ld, %ld, '%s', 0 )", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_VIEWS_USAGE, viewUID, uul[u]->getUDFUID(), COM_USER_DEFINED_ROUTINE_OBJECT_LIT);
    int cliRC = cliInterface->executeImmediate(query);

    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      return -1;
    }

  }  // for

  if (strlen(hiveObjsNoUsage) > 0) {
    NAString reason;
    reason = hiveObjsNoUsage;
    *CmpCommon::diags() << DgSqlCode(CAT_HIVE_VIEW_USAGE_UNAVAILABLE) << DgString0(reason);
  }

  return 0;
}

// ****************************************************************************
// method: gatherViewPrivileges
//
// For each referenced object (table or view) directly associated with the new
// view, combine privilege and grantable bitmaps together.  The list of
// privileges gathered will be assigned as default privileges as the privilege
// owner values.
//
// TBD:  when column privileges are added, need to check only the affected
//       columns
//
// Parameters:
//    createViewNode - for list of objects and isUpdatable/isInsertable flags
//    cliInterface - used to get UID of referenced object
//    viewCreator - determines which authID to use to gather privs
//    userID - userID to use when root is performing operations on behalf
//      of another user.
//    privilegeBitmap - returns privileges this user has on the view
//    grantableBitmap - returns privileges this user can grant
//
// returns:
//    0 - successful
//   -1 - user does not have the privilege
// ****************************************************************************
short CmpSeabaseDDL::gatherViewPrivileges(const StmtDDLCreateView *createViewNode, ExeCliInterface *cliInterface,
                                          NABoolean viewCreator, int userID, PrivMgrBitmap &privilegesBitmap,
                                          PrivMgrBitmap &grantableBitmap) {
  if (!isAuthorizationEnabled()) return 0;

  // set all bits to true initially, we will be ANDing with privileges
  // from all referenced objects
  // default table and view privileges are the same, set up default values
  PrivMgr::setTablePrivs(privilegesBitmap);
  PrivMgr::setTablePrivs(grantableBitmap);

  const ParViewUsages &vu = createViewNode->getViewUsages();
  const ParTableUsageList &vtul = vu.getViewTableUsageList();
  const ParViewColTableColsUsageList &vctcul = vu.getViewColTableColsUsageList();

  // If DB__ROOT, no need to gather privileges
  if (!checkAccessPrivileges(vtul, vctcul, viewCreator, userID, privilegesBitmap, grantableBitmap)) return -1;

  // If view is not updatable or insertable, turn off privs in bitmaps
  if (!createViewNode->getIsUpdatable()) {
    privilegesBitmap.set(UPDATE_PRIV, false);
    grantableBitmap.set(UPDATE_PRIV, false);
    privilegesBitmap.set(DELETE_PRIV, false);
    grantableBitmap.set(DELETE_PRIV, false);
  }

  if (!createViewNode->getIsInsertable()) {
    privilegesBitmap.set(INSERT_PRIV, false);
    grantableBitmap.set(INSERT_PRIV, false);
  }

  // Remove usage and executor privilege if set
  privilegesBitmap.set(EXECUTE_PRIV, false);
  grantableBitmap.set(EXECUTE_PRIV, false);
  privilegesBitmap.set(USAGE_PRIV, false);
  grantableBitmap.set(USAGE_PRIV, false);

  // Remove create privilege if set
  privilegesBitmap.set(CREATE_PRIV, false);
  grantableBitmap.set(CREATE_PRIV, false);

  return 0;
}

// ****************************************************************************
// method: getListOfReferencedTables
//
// Returns a list of all tables that are being referenced by the passed in
// view UID
//
// Parameters:
//    cliInterface - used to get the list of object usages
//    objectUID - the UID being processed
//    tableList - a list of objectRefdByMe structures describing each usage
//
// returns:
//    0 - successful
//   -1 - unexpected error occurred
// ****************************************************************************
short CmpSeabaseDDL::getListOfReferencedTables(ExeCliInterface *cliInterface, const long objectUID,
                                               NAList<objectRefdByMe> &tablesList) {
  int retcode = 0;

  NAList<objectRefdByMe> tempRefdList(STMTHEAP);
  retcode = getListOfDirectlyReferencedObjects(cliInterface, objectUID, tempRefdList);

  // If unexpected error - return
  if (retcode < 0) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting list of referenced tables");
    return -1;
  }

  // For each view in the list, call getReferencedTables recursively
  for (CollIndex i = 0; i < tempRefdList.entries(); i++) {
    objectRefdByMe objectRefd = tempRefdList[i];

    // views should only be referencing tables, other views, or functions
    CMPASSERT(objectRefd.objectType == COM_BASE_TABLE_OBJECT_LIT ||
              objectRefd.objectType == COM_USER_DEFINED_ROUTINE_OBJECT_LIT ||
              objectRefd.objectType == COM_SEQUENCE_GENERATOR_OBJECT_LIT ||
              objectRefd.objectType == COM_VIEW_OBJECT_LIT);

    // found a table, add to list
    if (objectRefd.objectType == COM_BASE_TABLE_OBJECT_LIT) {
      // First make sure it has not already been added to the list
      NABoolean foundEntry = FALSE;
      for (CollIndex j = 0; j < tablesList.entries(); j++) {
        if (tablesList[j].objectUID == objectRefd.objectUID) foundEntry = TRUE;
      }
      if (!foundEntry) tablesList.insert(objectRefd);
    }

    // found a view, get objects associated with the view
    if (objectRefd.objectType == COM_VIEW_OBJECT_LIT)
      getListOfReferencedTables(cliInterface, objectRefd.objectUID, tablesList);
  }

  return 0;
}

// ****************************************************************************
// method: getListOfDirectlyReferencedObjects
//
// Returns a list of objects that are being directly referenced by the passed
// in objectUID
//
// Parameters:
//    cliInterface - used to get the list of object usages
//    objectUID - the UID being processed
//    objectList - a list of objectRefdByMe structures describing each usage
//
// returns:
//    0 - successful
//   -1 - unexpected error occurred
// ****************************************************************************
short CmpSeabaseDDL::getListOfDirectlyReferencedObjects(ExeCliInterface *cliInterface, const long objectUID,
                                                        NAList<objectRefdByMe> &objectsList) {
  // Select all the rows from views_usage associated with the passed in
  // objectUID
  int cliRC = 0;
  char buf[4000];
  str_sprintf(buf,
              "select object_type, object_uid, catalog_name,"
              "schema_name, object_name from %s.\"%s\".%s T, %s.\"%s\".%s VU "
              "where VU.using_view_uid = %ld "
              "and T.object_uid = VU.used_object_uid",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_VIEWS_USAGE, objectUID);

  Queue *usingObjectsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(usingObjectsQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // set up an objectRefdByMe struct for each returned row
  usingObjectsQueue->position();
  for (int idx = 0; idx < usingObjectsQueue->numEntries(); idx++) {
    OutputInfo *oi = (OutputInfo *)usingObjectsQueue->getNext();
    objectRefdByMe objectInfo;
    objectInfo.objectType = NAString(oi->get(0));
    objectInfo.objectUID = *(long *)oi->get(1);
    objectInfo.catalogName = NAString(oi->get(2));
    objectInfo.schemaName = NAString(oi->get(3));
    objectInfo.objectName = NAString(oi->get(4));
    objectsList.insert(objectInfo);
  }

  return 0;
}

// ----------------------------------------------------------------------------
// method: createSeabaseView
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::createSeabaseView(StmtDDLCreateView *createViewNode, NAString &currCatName, NAString &currSchName) {
  int retcode = 0;
  int cliRC = 0;

  ComObjectName viewName(createViewNode->getViewName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  viewName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = viewName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = viewName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = viewName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extViewName = viewName.getExternalName(TRUE);
  const NAString extNameForHbase = catalogNamePart + "." + schemaNamePart + "." + objectNamePart;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  long schemaUID = 0;
  ComSchemaClass schemaClass;

  retcode = verifyDDLCreateOperationAuthorized(&cliInterface, SQLOperation::CREATE_VIEW, catalogNamePart,
                                               schemaNamePart, schemaClass, objectOwnerID, schemaOwnerID, &schemaUID);
  if (retcode != 0) {
    handleDDLCreateAuthorizationError(retcode, catalogNamePart, schemaNamePart);
    return;
  }

  ExpHbaseInterface *ehi = NULL;

  ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    processReturn();

    return;
  }

  if (((isSeabaseReservedSchema(viewName)) || (ComIsTrafodionExternalSchemaName(schemaNamePart))) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
    *CmpCommon::diags() << DgSqlCode(-1118) << DgTableName(extViewName);

    return;
  }

  // if metadata views are being created and seabase is uninitialized, then this
  // indicates that these views are being created during 'initialize trafodion'
  // and this compiler contains stale version.
  // Reload version info.
  //
  if ((isSeabaseMD(viewName)) && (CmpCommon::context()->isUninitializedSeabase())) {
    CmpCommon::context()->setIsUninitializedSeabase(FALSE);
    CmpCommon::context()->uninitializedSeabaseErrNum() = 0;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_UNKNOWN_OBJECT,
                                   FALSE, FALSE);
  if (retcode < 0) {
    processReturn();

    return;
  }

  if (retcode == 1)  // already exists
  {
    if (createViewNode->createIfNotExists()) {
      deallocEHI(ehi);
      processReturn();
      return;
    }

    if (NOT((createViewNode->isCreateOrReplaceViewCascade()) || (createViewNode->isCreateOrReplaceView()))) {
      *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extViewName);
      processReturn();

      return;
    }
  }

  char *query = NULL;
  int64_t objectUID = -1;
  std::vector<ObjectPrivsRow> viewPrivsRows;
  bool replacingView = false;
  long origObjUID = -1;
  long objDataUID = 0;

  if ((retcode == 1) &&  // exists
      ((createViewNode->isCreateOrReplaceViewCascade()) || (createViewNode->isCreateOrReplaceView()))) {
    // Replace view. Drop this view and recreate it.

    int objectOwnerID = 0;
    int schemaOwnerID = 0;
    long objectFlags = 0;
    origObjUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                               COM_VIEW_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

    if (origObjUID < 0 || objectOwnerID == 0) {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("getting object UID and owner for create or replace view");

      processReturn();

      return;
    }

    if (isAuthorizationEnabled()) {
      // Verify user can perform operation
      if (!isDDLOperationAuthorized(SQLOperation::ALTER_VIEW, objectOwnerID, schemaOwnerID, origObjUID, COM_VIEW_OBJECT,
                                    NULL)) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

        processReturn();
        return;
      }

      // Initiate the privilege manager interface class
      NAString privMgrMDLoc;
      CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
      PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());

      PrivStatus privStatus = privInterface.getPrivRowsForObject(origObjUID, viewPrivsRows);
      if (privStatus != PrivStatus::STATUS_GOOD) {
        SEABASEDDL_INTERNAL_ERROR("Unable to retrieve privileges for replaced view");

        processReturn();

        return;
      }
    }

    if (dropOneTableorView(cliInterface, extViewName.data(), COM_VIEW_OBJECT, false))

    {
      processReturn();

      return;
    }
    replacingView = true;
  }

  // Gather the object and grantable privileges that the view creator has.
  // This code also verifies that the current user has the necessary
  // privileges to create the view.
  PrivMgrBitmap privilegesBitmap;
  PrivMgrBitmap grantableBitmap;
  PrivMgr::setTablePrivs(privilegesBitmap);
  PrivMgr::setTablePrivs(grantableBitmap);

  // The view creator may not be the same as the view owner.
  // For shared schemas, the view creator is always the same as the view owner.
  // For private schemas, the view owner is the schema owner. However, the
  // user that is issuing the CREATE statement is not always the schema owner.
  NABoolean viewOwnerIsViewCreator =
      ((schemaClass == COM_SCHEMA_CLASS_SHARED) ? TRUE : ((ComUser::getCurrentUser() == schemaOwnerID) ? TRUE : FALSE));

  // If DB ROOT is running command on behalf of another user, then the view
  // owner is the same of the view creator
  if (ComUser::isRootUserID() && (ComUser::getRootUserID() != schemaOwnerID)) viewOwnerIsViewCreator = TRUE;

  // Gather privileges for the view creator
  NABoolean viewCreator = TRUE;
  if (gatherViewPrivileges(createViewNode, &cliInterface, viewCreator, schemaOwnerID, privilegesBitmap,
                           grantableBitmap)) {
    processReturn();

    return;
  }

  PrivMgrBitmap ownerPrivBitmap;
  PrivMgrBitmap ownerGrantableBitmap;
  PrivMgr::setTablePrivs(ownerPrivBitmap);
  PrivMgr::setTablePrivs(ownerGrantableBitmap);

  // If view owner is the same as view creator, owner and creator privileges
  // are the same
  if (viewOwnerIsViewCreator) {
    ownerPrivBitmap = privilegesBitmap;
    ownerGrantableBitmap = grantableBitmap;
  }

  // If view creator is not the same as the view owner, gather the
  // view owner privileges
  else {
    if (gatherViewPrivileges(createViewNode, &cliInterface, !viewCreator, schemaOwnerID, ownerPrivBitmap,
                             ownerGrantableBitmap)) {
      processReturn();

      return;
    }
  }

  // Allow creator to alter and/or drop view (without WGO)
  privilegesBitmap.set(ALTER_PRIV);
  privilegesBitmap.set(DROP_PRIV);

  NAString viewText(STMTHEAP);
  buildViewText(createViewNode, viewText);

  ElemDDLColDefArray colDefArray(STMTHEAP);
  if (buildViewColInfo(createViewNode, &colDefArray)) {
    processReturn();

    return;
  }

  int numCols = colDefArray.entries();
  ComTdbVirtTableColumnInfo *colInfoArray = new (STMTHEAP) ComTdbVirtTableColumnInfo[numCols];

  if (buildColInfoArray(COM_VIEW_OBJECT, FALSE, &colDefArray, colInfoArray, FALSE, FALSE)) {
    processReturn();

    return;
  }

  ComTdbVirtTableTableInfo *tableInfo = new (STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = NULL, tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = 0;
  tableInfo->objDataUID = 0;
  tableInfo->schemaUID = schemaUID;
  tableInfo->objOwnerID = objectOwnerID;
  tableInfo->schemaOwnerID = schemaOwnerID;
  tableInfo->isAudited = 1;
  tableInfo->validDef = 1;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->numSaltPartns = 0;
  tableInfo->rowFormat = COM_UNKNOWN_FORMAT_TYPE;
  tableInfo->objectFlags = 0;

  long objUID = -1;
  if (updateSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_VIEW_OBJECT, "N",
                           tableInfo, numCols, colInfoArray, 0, NULL, 0, NULL, objUID)) {
    processReturn();

    return;
  }

  if (objUID < 0) {
    processReturn();
    return;
  }

  NAString viewColUsageText;
  if (buildViewTblColUsage(createViewNode, colInfoArray, objUID, viewColUsageText)) {
    processReturn();
    return;
  }

  // grant privileges for view
  if (isAuthorizationEnabled()) {
    // Initiate the privilege manager interface class
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
    PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());

    // Calculate the view owner (grantee)
    int32_t grantee = (viewOwnerIsViewCreator) ? ComUser::getCurrentUser() : schemaOwnerID;
    if (ComUser::isRootUserID() && (ComUser::getRootUserID() != schemaOwnerID)) grantee = schemaOwnerID;

    if (replacingView) {
      PrivStatus privStatus = privInterface.insertPrivRowsForObject(objUID, viewPrivsRows);

      if (privStatus != PrivStatus::STATUS_GOOD) {
        SEABASEDDL_INTERNAL_ERROR("Unable to restore privileges for replaced view");
        processReturn();
        return;
      }
    }

    else {
      // Grant view ownership - grantor is the SYSTEM
      retcode = privInterface.grantObjectPrivilege(objUID, std::string(extViewName.data()), COM_VIEW_OBJECT,
                                                   SYSTEM_USER, grantee, ownerPrivBitmap, ownerGrantableBitmap);
      if (retcode != STATUS_GOOD && retcode != STATUS_WARNING) {
        processReturn();
        return;
      }

      // if the view creator is different than view owner, assign creator
      // privileges (assigned by view owner to view creator)
      if (!viewOwnerIsViewCreator) {
        retcode =
            privInterface.grantObjectPrivilege(objUID, std::string(extViewName.data()), COM_VIEW_OBJECT, schemaOwnerID,
                                               ComUser::getCurrentUser(), privilegesBitmap, grantableBitmap);
        if (retcode != STATUS_GOOD && retcode != STATUS_WARNING) {
          processReturn();
          return;
        }
      }
    }
  }

  query = new (STMTHEAP) char[1000];
  str_sprintf(query, "upsert into %s.\"%s\".%s values (%ld, '%s', %d, %d, 0)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_VIEWS, objUID, computeCheckOption(createViewNode), (createViewNode->getIsUpdatable() ? 1 : 0),
              (createViewNode->getIsInsertable() ? 1 : 0));

  cliRC = cliInterface.executeImmediate(query);

  NADELETEBASIC(query, STMTHEAP);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

    processReturn();

    return;
  }

  if (updateTextTable(&cliInterface, objUID, COM_VIEW_TEXT, 0, viewText)) {
    processReturn();
    return;
  }

  // Views that contain UNION clauses do not have col usages available so
  // viewColUsageText could be null - see TRAFODION-2153
  if (!viewColUsageText.isNull()) {
    if (updateTextTable(&cliInterface, objUID, COM_VIEW_REF_COLS_TEXT, 0, viewColUsageText)) {
      processReturn();
      return;
    }
  }

  NAList<objectRefdByMe> tablesRefdList(STMTHEAP);
  if (updateViewUsage(createViewNode, objUID, &cliInterface, &tablesRefdList)) {
    processReturn();

    return;
  }

  if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_VIEW_OBJECT_LIT, "Y")) {
    processReturn();

    return;
  }

  if (updateObjectRedefTime(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_VIEW_OBJECT_LIT, -2,
                            objUID)) {
    processReturn();

    return;
  }

  if (!ComIsTrafodionReservedSchemaName(schemaNamePart)) {
    retcode = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_VIEW_OBJECT_LIT);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-8448) << DgString0("CmpSeabaseDDL::createSeabaseView")
                          << DgString1("cannot update XDCDDLTable");
      processReturn();
      return;
    }
  }

  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  if (replacingView && origObjUID >= 0) {
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_VIEW_OBJECT,
                                                    createViewNode->ddlXns(), FALSE, origObjUID);
  }
  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_MINE_ONLY, COM_VIEW_OBJECT,
                                                  createViewNode->ddlXns(), FALSE);

  // Now remove referenced base tables from cache and generate QI keys
  // for any base tables that have DDL locks (ObjectEpochCache). We
  // need to send QI keys so that any compilers having metadata cached
  // for a given base table will be forced to re-read the metadata with
  // the updated epoch number. Note that DDL locks are needed, since
  // they prevent concurrent DDL against the base table while CREATE VIEW
  // is in flight.
  //
  // Note: An alternative fix that does not require QI keys would be to
  // create a new mode of ObjectEpochCache DDL lock where reads and writes
  // are allowed, and the epoch is not incremented when the DDL operation
  // completes. That's more involved, so we'll leave that to later work.
  // When that is done, then this logic can be removed.
  for (CollIndex i = 0; i < tablesRefdList.entries(); i++) {
    CorrName cn(tablesRefdList[i].objectName, STMTHEAP, tablesRefdList[i].schemaName, tablesRefdList[i].catalogName);
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    createViewNode->ddlXns(), FALSE);
  }

  processReturn();

  return;
}

void CmpSeabaseDDL::dropSeabaseView(StmtDDLDropView *dropViewNode, NAString &currCatName, NAString &currSchName) {
  int cliRC = 0;
  int retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = dropViewNode->getViewName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable *naTable = NULL;
  CorrName cn;
  QualifiedName qn;
  retcode = setupAndErrorChecks(tabName, qn, currCatName, currSchName, catalogNamePart, schemaNamePart, objectNamePart,
                                extTableName, extNameForHbase, cn, NULL, FALSE, FALSE, &cliInterface, COM_VIEW_OBJECT);

  if (retcode == -2) {
    // table doesnt exist. return if 'if exists' clause is specified.
    if (dropViewNode->dropIfExists()) {
      // clear diags
      CmpCommon::diags()->clear();
      processReturn();
      return;
    }
  }

  if (retcode < 0) {
    processReturn();
    return;
  }

  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;
  long objUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                              COM_VIEW_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

  if (objUID < 0 || objectOwnerID == 0) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting object UID and owner for drop view");

    processReturn();
    return;
  }

  // Verify user can perform operation
  if (!isDDLOperationAuthorized(SQLOperation::DROP_VIEW, objectOwnerID, schemaOwnerID, objUID, COM_VIEW_OBJECT, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  Queue *usingViewsQueue = NULL;
  if (dropViewNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR) {
    NAString usingObjName;
    cliRC = getUsingObject(&cliInterface, objUID, usingObjName);
    if (cliRC < 0) {
      processReturn();
      return;
    }

    if (cliRC != 100)  // found an object
    {
      *CmpCommon::diags() << DgSqlCode(-1047) << DgTableName(usingObjName);

      processReturn();
      return;
    }
  } else if (dropViewNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR) {
    cliRC = getUsingViews(&cliInterface, objUID, usingViewsQueue);
    if (cliRC < 0) {
      processReturn();
      return;
    }
  }

  // get the list of all tables referenced by the view.  Save this list so
  // referenced tables can be removed from cache later
  NAList<objectRefdByMe> tablesRefdList(STMTHEAP);
  short status = getListOfReferencedTables(&cliInterface, objUID, tablesRefdList);

  NABoolean monarchObject = FALSE;
  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    processReturn();
    return;
  }

  // if any underlying table is being backed up, this view cannot be dropped
  // until that operation is over.
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  for (CollIndex i = 0; i < tablesRefdList.entries(); i++) {
    CorrName cn(tablesRefdList[i].objectName, STMTHEAP, tablesRefdList[i].schemaName, tablesRefdList[i].catalogName);

    // get uid of the corresponding schema
    long schUID = -1;
    if (isLockedForBR(tablesRefdList[i].objectUID, schUID, &cliInterface, cn)) {
      processReturn();
      return;
    }

    NATable *naTable = bindWA.getNATableInternal(cn);
    if (naTable == NULL) {
      SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in updateViewUsage");
      return;
    }

    // Create a DDL lock, if this is a base table
    // A subsequent commit/rollback will remove it
    if ((naTable->getObjectType() == COM_BASE_TABLE_OBJECT) && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
      if (lockObjectDDL(naTable) < 0) {
        processReturn();
        return;
      }

      // Table exists, update the epoch value to lock out other user access
      if (ddlSetObjectEpoch(naTable, STMTHEAP) < 0) {
        processReturn();
        return;
      }
    }

  }  // for

  long redefTime = NA_JulianTimestamp();

  // Now remove referenced tables from cache.
  if (usingViewsQueue) {
    usingViewsQueue->position();

    for (int idx = 0; idx < usingViewsQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)usingViewsQueue->getNext();

      char *usingViewName = vi->get(0);
      long usingViewUID = *(long *)vi->get(1);

      // add to list of affected DDL operations
      DDLObjInfo ddlObj(usingViewName, usingViewUID, COM_VIEW_OBJECT);
      ddlObj.setQIScope(REMOVE_FROM_ALL_USERS);
      ddlObj.setRedefTime(redefTime);
      ddlObj.setDDLOp(FALSE);
      CmpCommon::context()->ddlObjsList().insertEntry(ddlObj);

      if (dropSeabaseObject(ehi, usingViewName, currCatName, currSchName, COM_VIEW_OBJECT, dropViewNode->ddlXns(), TRUE,
                            TRUE, monarchObject, NAString(""))) {
        deallocEHI(ehi);
        processReturn();
        return;
      }
    }
  }

  if (dropSeabaseObject(ehi, tabName, currCatName, currSchName, COM_VIEW_OBJECT, dropViewNode->ddlXns(), TRUE, TRUE,
                        monarchObject, NAString(""))) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (!ComIsTrafodionReservedSchemaName(schemaNamePart)) {
    retcode = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_VIEW_OBJECT_LIT);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-8448) << DgString0("CmpSeabaseDDL::dropSeabaseView")
                          << DgString1("cannot update XDCDDLTable");
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  deallocEHI(ehi);

  // clear view definition from my cache only.
  // TDB - is this still needed?
#if 0
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_MINE_ONLY, COM_VIEW_OBJECT,
     dropViewNode->ddlXns(), FALSE);
#endif

  // clear current view and any using views from caches here.
  // at commit/rollback time, the DDLObjInfoList will send out appropriate qiKeys
  DDLObjInfo ddlObj(cn.getQualifiedNameAsString(), objUID, COM_VIEW_OBJECT);
  ddlObj.setQIScope(REMOVE_FROM_ALL_USERS);
  ddlObj.setRedefTime(redefTime);
  ddlObj.setDDLOp(FALSE);
  CmpCommon::context()->ddlObjsList().insertEntry(ddlObj);

  // Now remove referenced tables from cache.
  // When a query that references a view is compiled, all views are converted
  // to the underlying base tables.  Query plans are generated to access the
  // tables, and the views are no longer relevant.
  // When dropping a view, query plans that reference the dropped view will
  // continue to work if the plans are cached.  This code removes the
  // referenced tables from caches to force recompilations so dropped views
  // are noticed.
  for (CollIndex i = 0; i < tablesRefdList.entries(); i++) {
    CorrName cn(tablesRefdList[i].objectName, STMTHEAP, tablesRefdList[i].schemaName, tablesRefdList[i].catalogName);
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    dropViewNode->ddlXns(), FALSE);
  }

  processReturn();
  return;
}

void CmpSeabaseDDL::glueQueryFragments(int queryArraySize, const QString *queryArray, char *&gluedQuery,
                                       int &gluedQuerySize) {
  int i = 0;
  gluedQuerySize = 0;
  gluedQuery = NULL;

  for (i = 0; i < queryArraySize; i++) {
    UInt32 j = 0;
    const char *partn_frag = queryArray[i].str;
    while ((j < strlen(queryArray[i].str)) && (partn_frag[j] == ' ')) j++;
    if (j < strlen(queryArray[i].str)) gluedQuerySize += strlen(&partn_frag[j]);
  }

  gluedQuery = new (STMTHEAP) char[gluedQuerySize + 100];
  gluedQuery[0] = 0;
  for (i = 0; i < queryArraySize; i++) {
    UInt32 j = 0;
    const char *partn_frag = queryArray[i].str;
    while ((j < strlen(queryArray[i].str)) && (partn_frag[j] == ' ')) j++;

    if (j < strlen(queryArray[i].str)) strcat(gluedQuery, &partn_frag[j]);
  }
}

short CmpSeabaseDDL::createMetadataViews(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int retcode = 0;

  char queryBuf[5000];

  for (int i = 0; i < sizeof(allMDviewsInfo) / sizeof(MDViewInfo); i++) {
    const MDViewInfo &mdi = allMDviewsInfo[i];

    if (!mdi.viewName) continue;

    for (int j = 0; j < NUM_MAX_PARAMS; j++) {
      param_[j] = NULL;
    }

    const QString *qs = NULL;
    int sizeOfqs = 0;

    qs = mdi.viewDefnQuery;
    sizeOfqs = mdi.sizeOfDefnArr;

    int qryArraySize = sizeOfqs / sizeof(QString);
    char *gluedQuery;
    int gluedQuerySize;
    glueQueryFragments(qryArraySize, qs, gluedQuery, gluedQuerySize);

    if (strcmp(mdi.viewName, TRAF_TABLES_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_TABLES;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = COM_BASE_TABLE_OBJECT_LIT;
    } else if (strcmp(mdi.viewName, TRAF_COLUMNS_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_COLUMNS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = COM_BASE_TABLE_OBJECT_LIT;
    } else if (strcmp(mdi.viewName, TRAF_INDEXES_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_INDEXES;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_OBJECTS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = SEABASE_OBJECTS;
      param_[11] = getSystemCatalog();
      param_[12] = SEABASE_MD_SCHEMA;
    } else if (strcmp(mdi.viewName, TRAF_KEYS_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_TABLE_CONSTRAINTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_OBJECTS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = SEABASE_OBJECTS;
      param_[11] = getSystemCatalog();
      param_[12] = SEABASE_MD_SCHEMA;
      param_[13] = SEABASE_KEYS;
      param_[14] = getSystemCatalog();
      param_[15] = SEABASE_MD_SCHEMA;
    } else if (strcmp(mdi.viewName, TRAF_REF_CONSTRAINTS_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_REF_CONSTRAINTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_OBJECTS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = SEABASE_OBJECTS;
      param_[11] = getSystemCatalog();
      param_[12] = SEABASE_MD_SCHEMA;
      param_[13] = SEABASE_OBJECTS;
      param_[14] = getSystemCatalog();
      param_[15] = SEABASE_MD_SCHEMA;
      param_[16] = SEABASE_TABLE_CONSTRAINTS;
      param_[17] = getSystemCatalog();
      param_[18] = SEABASE_MD_SCHEMA;
    } else if (strcmp(mdi.viewName, TRAF_SEQUENCES_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_SEQ_GEN;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = COM_SEQUENCE_GENERATOR_OBJECT_LIT;
    } else if (strcmp(mdi.viewName, TRAF_VIEWS_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_VIEWS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = COM_VIEW_OBJECT_LIT;
    } else if (strcmp(mdi.viewName, TRAF_OBJECT_COMMENT_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_TEXT;
      param_[8] = "3";  // COM_OBJECT_COMMENT_TEXT
    } else if (strcmp(mdi.viewName, TRAF_COLUMN_COMMENT_VIEW) == 0) {
      param_[0] = getSystemCatalog();
      param_[1] = SEABASE_MD_SCHEMA;
      param_[2] = getSystemCatalog();
      param_[3] = SEABASE_MD_SCHEMA;
      param_[4] = SEABASE_OBJECTS;
      param_[5] = getSystemCatalog();
      param_[6] = SEABASE_MD_SCHEMA;
      param_[7] = SEABASE_COLUMNS;
      param_[8] = getSystemCatalog();
      param_[9] = SEABASE_MD_SCHEMA;
      param_[10] = SEABASE_TEXT;
      param_[11] = "12";  // COM_COLUMN_COMMENT_TEXT
    } else {
      NADELETEBASICARRAY(gluedQuery, STMTHEAP);
      continue;
    }

    str_sprintf(queryBuf, gluedQuery, param_[0], param_[1], param_[2], param_[3], param_[4], param_[5], param_[6],
                param_[7], param_[8], param_[9], param_[10], param_[11], param_[12], param_[13], param_[14], param_[15],
                param_[16], param_[17], param_[18]);

    NADELETEBASICARRAY(gluedQuery, STMTHEAP);

    NABoolean xnWasStartedHere = FALSE;
    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1390)  // view already exists
    {
      // ignore the error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) return -1;

  }  // for

  return 0;
}

short CmpSeabaseDDL::dropMetadataViews(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int retcode = 0;

  char queryBuf[5000];

  for (int i = 0; i < sizeof(allMDviewsInfo) / sizeof(MDViewInfo); i++) {
    const MDViewInfo &mdi = allMDviewsInfo[i];

    if (!mdi.viewName) continue;

    str_sprintf(queryBuf, "drop view %s.\"%s\".%s", getSystemCatalog(), SEABASE_MD_SCHEMA, mdi.viewName);

    NABoolean xnWasStartedHere = FALSE;
    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1389)  // does not exist, ignore
    {
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) return -1;

  }  // for

  return 0;
}

// *****************************************************************************
// *                                                                           *
// * Function: checkAccessPrivileges                                           *
// *                                                                           *
// *   This function determines if a user has the requesite privileges to      *
// * access the referenced objects that comprise the view. In addition it      *
// * returns the privileges bitmap containing privileges to be granted to the  *
// * view.                                                                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <vtul>                   const ParTableUsageList &              In       *
// *    is a reference to a list of objects used by the view.                  *
// *                                                                           *
// *  <vctcul>                 const ParViewColTableColsUsageList &   In       *
// *    is a reference to the list of columns used by the view.                *
// *                                                                           *
// *  <viewCreator>            NABoolean                              In       *
// *    If TRUE, gather privileges for the view creator, if FALSE,             *
// *    gather privileges for the view owner                                   *
// *                                                                           *
// *  <userID>               int                                  In         *
// *     userID to use when root is performing operations on behalf            *
// *     of another user.                                                      *
// *                                                                           *
// *  <privilegesBitmap>       PrivMgrBitmap &                        Out      *
// *    passes back the union of privileges the user has on the referenced     *
// *    objects.                                                               *
// *                                                                           *
// *  <grantableBitmap>        PrivMgrBitmap &                        Out      *
// *    passes back the union of the with grant option authority the user has  *
// *    on the referenced objects.                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: User has requisite privileges; bitmap unions returned.             *
// * false: Could not retrieve privileges or user does not have requesite      *
// *        privileges; see diags area for error details.                      *
// *                                                                           *
// *****************************************************************************
static bool checkAccessPrivileges(const ParTableUsageList &vtul, const ParViewColTableColsUsageList &vctcul,
                                  NABoolean viewCreator, int userID, PrivMgrBitmap &privilegesBitmap,
                                  PrivMgrBitmap &grantableBitmap)

{
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  PrivStatus retcode = STATUS_GOOD;

  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());

  // generate the list of privileges and grantable privileges to assign to the view
  // a side effect is to return an error if basic privileges are not granted
  for (CollIndex i = 0; i < vtul.entries(); i++) {
    ComObjectName usedObjName(vtul[i].getQualifiedNameObj().getQualifiedNameAsAnsiString(), vtul[i].getAnsiNameSpace());

    const NAString catalogNamePart = usedObjName.getCatalogNamePartAsAnsiString();
    const NAString schemaNamePart = usedObjName.getSchemaNamePartAsAnsiString(TRUE);
    const NAString objectNamePart = usedObjName.getObjectNamePartAsAnsiString(TRUE);
    NAString refdUsedObjName = usedObjName.getExternalName(TRUE);
    CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);

    // If a sequence, set correct type to get a valid NATable entry
    bool isSeq = (vtul[i].getSpecialType() == ExtendedQualName::SG_TABLE) ? true : false;
    if (isSeq) cn.setSpecialType(ExtendedQualName::SG_TABLE);

    NATable *naTable = bindWA.getNATableInternal(cn);
    if (naTable == NULL) {
      SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in checkAccessPrivileges");
      return false;
    }

    PrivMgrUserPrivs privs;
    PrivMgrUserPrivs *pPrivInfo = NULL;

    // If hive or ORC table and table does not have an external table,
    // set no privs unless running as DB__ROOT in a DB__ROOT owned schema
    if ((naTable->isHiveTable() || naTable->isORC() || naTable->isParquet() || naTable->isAvro()) &&
        ((NOT naTable->isRegistered()) || (!naTable->hasExternalTable()))) {
      if (ComUser::isRootUserID() && ComUser::getRootUserID() == userID) privs.setOwnerDefaultPrivs();
      pPrivInfo = &privs;
    } else {
      // If gathering privileges for the view creator, the NATable structure
      // contains the privileges we want to use to create bitmaps
      if (viewCreator) {
        // If the viewCreator is not the current user (DB__ROOT running request)
        // on behalf of the schema owner) or privileges not yet initialized,
        // get actual owner privs.
        if (ComUser::isRootUserID()) {
          if ((ComUser::getRootUserID() != naTable->getSchemaOwner()) || (naTable->getPrivInfo() == NULL)) {
            // For metadata views, the object_uid may not be setup, go get it
            if (naTable->objectUid().get_value() == 0) naTable->lookupObjectUid();

            retcode = privInterface.getPrivileges((int64_t)naTable->objectUid().get_value(), naTable->getObjectType(),
                                                  userID, privs);

            if (retcode == STATUS_ERROR) {
              *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
              return false;
            }
            pPrivInfo = &privs;
          } else
            return true;
        } else {
          pPrivInfo = naTable->getPrivInfo();
          CMPASSERT(pPrivInfo != NULL);
        }
      }

      // If the view owner is not the view creator, then we need to get schema
      // owner privileges from PrivMgr.
      else {
        PrivStatus retcode = privInterface.getPrivileges(naTable, naTable->getSchemaOwner(), TRUE, privs, NULL);

        if (retcode == STATUS_ERROR) {
          *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
          return false;
        }
        pPrivInfo = &privs;
      }
    }

    // Requester must have at least select privilege
    // For sequence generators USAGE is needed instead of SELECT
    bool noObjRequiredPriv = true;
    if (isSeq)
      noObjRequiredPriv = !pPrivInfo->hasUsagePriv() ? true : false;
    else
      noObjRequiredPriv = !pPrivInfo->hasSelectPriv() ? true : false;

    // Summarize privileges
    privilegesBitmap &= pPrivInfo->getObjectBitmap();
    privilegesBitmap |= pPrivInfo->getSchemaPrivBitmap();
    grantableBitmap &= pPrivInfo->getGrantableBitmap();
    grantableBitmap |= pPrivInfo->getSchemaGrantableBitmap();

    // Gather column level privs to attach to the bitmap.
    // Even though privileges are granted on the column, they show up as
    // object privileges on the view.
    PrivColumnBitmap colPrivBitmap;
    PrivColumnBitmap colGrantableBitmap;

    PrivMgrPrivileges::setColumnPrivs(colPrivBitmap);
    PrivMgrPrivileges::setColumnPrivs(colGrantableBitmap);

    // Check for column privileges on each view column
    // This loop is performed for each object referenced by the view.
    // Only those columns that belong to the referenced object have their
    // privileges summarized.
    for (size_t i = 0; i < vctcul.entries(); i++) {
      const ParViewColTableColsUsage &vctcu = vctcul[i];
      const ColRefName &usedColRef = vctcu.getUsedObjectColumnName();
      ComObjectName usedObjName = usedColRef.getCorrNameObj().getQualifiedNameObj().getQualifiedNameAsAnsiString();
      NAString colUsedObjName = usedObjName.getExternalName(TRUE);

      // If column is part of the used object, summarize column privs
      if (colUsedObjName == refdUsedObjName) {
        // Get the refd object details
        const NAColumnArray &nacolArr = naTable->getNAColumnArray();
        ComString usedObjColName(usedColRef.getColName());
        const NAColumn *naCol = nacolArr.getColumn(usedObjColName);
        if (naCol == NULL) {
          *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR) << DgColumnName(usedObjColName);
          return false;
        }
        int32_t usedColNumber = naCol->getPosition();

        // If the user is missing SELECT at the object level and on at least one
        // column, view cannot be created.  No need to proceed.
        // Can't have sequences on views, so only need to check for SELECT
        if (noObjRequiredPriv && !pPrivInfo->hasColSelectPriv(usedColNumber)) {
          *CmpCommon::diags() << DgSqlCode(-4481) << DgString0("SELECT") << DgString1(colUsedObjName.data());
          return false;
        }

        colPrivBitmap &= pPrivInfo->getColumnPrivBitmap(usedColNumber);
        colGrantableBitmap &= pPrivInfo->getColumnGrantableBitmap(usedColNumber);
      }  // done with current view col
    }    // done checking privs for all view cols

    // Add summarize column privileges to the official bitmaps, bit is only
    // set if all cols have priv set.
    for (size_t i = FIRST_DML_COL_PRIV; i <= LAST_DML_COL_PRIV; i++) {
      if (colPrivBitmap.test(PrivType(i))) privilegesBitmap.set(PrivType(i));

      if (colGrantableBitmap.test(PrivType(i))) grantableBitmap.set(PrivType(i));
    }
  }

  return true;
}
//*********************** End of checkAccessPrivileges *************************
