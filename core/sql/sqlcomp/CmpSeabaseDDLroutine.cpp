

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/ComObjectName.h"
#include "common/ComUser.h"
#include "sqlcomp/CmpSeabaseDDLroutine.h"

#include "parser/StmtDDLCreateRoutine.h"
#include "parser/StmtDDLDropRoutine.h"
#include "parser/StmtDDLCreateLibrary.h"
#include "parser/StmtDDLDropLibrary.h"
#include "parser/StmtDDLAlterLibrary.h"

#include "parser/ElemDDLColDefArray.h"
#include "parser/ElemDDLColRefArray.h"
#include "parser/ElemDDLParamDefArray.h"
#include "parser/ElemDDLParamDef.h"

#include "optimizer/SchemaDB.h"
#include "sqlcomp/CmpSeabaseDDL.h"

#include "exp/ExpHbaseInterface.h"
#include "executor/ExExeUtilCli.h"
#include "generator/Generator.h"
#include "common/ComSmallDefs.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"

#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "common/ComUser.h"

#include "common/NumericType.h"
#include "common/DatetimeType.h"
#include "langman/LmJavaSignature.h"

#include "common/ComCextdecs.h"
#include <sys/stat.h>
#include "optimizer/TriggerDB.h"

short ExExeUtilLobExtractLibrary(ExeCliInterface *cliInterface, char *libHandle, char *cachedLibName,
                                 ComDiagsArea *toDiags);

// *****************************************************************************
// *                                                                           *
// * Function: validateLibraryFileExists                                       *
// *                                                                           *
// *    Determines if a library file exists, and if not, reports an error.     *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <libraryFilename>               const ComString &               In       *
// *    is the file whose existence is to be validated.                        *
// *                                                                           *
// *  <isSystemObject>                bool                            In       *
// *    if true, indicates the filename should be prepended with the value of  *
// *  TRAF_HOME before validating existence.                                   *
// *                                                                            *
// *****************************************************************************
static int validateLibraryFileExists(const NAString &libraryFilename, bool isSystemObject)

{
  NAString completeLibraryFilename(libraryFilename);

  if (isSystemObject) {
    completeLibraryFilename.insert(0, '/');
    completeLibraryFilename.insert(0, getenv("TRAF_HOME"));
  } else if (CmpCommon::getDefault(CAT_LIBRARY_PATH_RELATIVE) == DF_ON)
    completeLibraryFilename.insert(0, getenv("MY_UDR_ROOT"));

  char *libraryFilenameString = convertNAString(completeLibraryFilename, STMTHEAP);
  struct stat sts;

  if ((stat(libraryFilenameString, &sts)) == -1 && errno == ENOENT) {
    *CmpCommon::diags() << DgSqlCode(-1382) << DgString0(libraryFilename);
    return 1;
  }

  return 0;
}
//********************* End of validateLibraryFileExists ***********************

static void getRoutineTypeLit(ComRoutineType val, NAString &result) {
  if (val == COM_PROCEDURE_TYPE)
    result = COM_PROCEDURE_TYPE_LIT;
  else if (val == COM_SCALAR_UDF_TYPE)
    result = COM_SCALAR_UDF_TYPE_LIT;
  else if (val == COM_TABLE_UDF_TYPE)
    result = COM_TABLE_UDF_TYPE_LIT;
  else
    result = COM_UNKNOWN_ROUTINE_TYPE_LIT;
}

static void getLanguageTypeLit(ComRoutineLanguage val, NAString &result) {
  if (val == COM_LANGUAGE_JAVA)
    result = COM_LANGUAGE_JAVA_LIT;
  else if (val == COM_LANGUAGE_C)
    result = COM_LANGUAGE_C_LIT;
  else if (val == COM_LANGUAGE_CPP)
    result = COM_LANGUAGE_CPP_LIT;
  else if (val == COM_LANGUAGE_SQL)
    result = COM_LANGUAGE_SQL_LIT;
  else
    result = COM_UNKNOWN_ROUTINE_LANGUAGE_LIT;
}

static void getSqlAccessLit(ComRoutineSQLAccess val, NAString &result) {
  if (val == COM_NO_SQL)
    result = COM_NO_SQL_LIT;
  else if (val == COM_CONTAINS_SQL)
    result = COM_CONTAINS_SQL_LIT;
  else if (val == COM_READS_SQL)
    result = COM_READS_SQL_LIT;
  else if (val == COM_MODIFIES_SQL)
    result = COM_MODIFIES_SQL_LIT;
  else
    result = COM_UNKNOWN_ROUTINE_SQL_ACCESS_LIT;
}

static void getParamStyleLit(ComRoutineParamStyle val, NAString &result) {
  if (val == COM_STYLE_GENERAL)
    result = COM_STYLE_GENERAL_LIT;
  else if (val == COM_STYLE_JAVA_CALL)
    result = COM_STYLE_JAVA_CALL_LIT;
  else if (val == COM_STYLE_JAVA_OBJ)
    result = COM_STYLE_JAVA_OBJ_LIT;
  else if (val == COM_STYLE_SQL)
    result = COM_STYLE_SQL_LIT;
  else if (val == COM_STYLE_SQLROW)
    result = COM_STYLE_SQLROW_LIT;
  else if (val == COM_STYLE_SQLROW_TM)
    result = COM_STYLE_SQLROW_TM_LIT;
  else if (val == COM_STYLE_CPP_OBJ)
    result = COM_STYLE_CPP_OBJ_LIT;
  else
    result = COM_UNKNOWN_ROUTINE_PARAM_STYLE_LIT;
}

static void getTransAttributesLit(ComRoutineTransactionAttributes val, NAString &result) {
  if (val == COM_NO_TRANSACTION_REQUIRED)
    result = COM_NO_TRANSACTION_REQUIRED_LIT;
  else if (val == COM_TRANSACTION_REQUIRED)
    result = COM_TRANSACTION_REQUIRED_LIT;
  else
    result = COM_UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE_LIT;
}

static void getParallelismLit(ComRoutineParallelism val, NAString &result) {
  if (val == COM_ROUTINE_NO_PARALLELISM)
    result = COM_ROUTINE_NO_PARALLELISM_LIT;
  else
    result = COM_ROUTINE_ANY_PARALLELISM_LIT;
}

static void getExternalSecurityLit(ComRoutineExternalSecurity val, NAString &result) {
  if (val == COM_ROUTINE_EXTERNAL_SECURITY_DEFINER)
    result = COM_ROUTINE_EXTERNAL_SECURITY_DEFINER_LIT;
  else if (val == COM_ROUTINE_EXTERNAL_SECURITY_IMPLEMENTATION_DEFINED)
    result = COM_ROUTINE_EXTERNAL_SECURITY_IMPLEMENTATION_DEFINED_LIT;
  else
    result = COM_ROUTINE_EXTERNAL_SECURITY_INVOKER_LIT;  // the default
}

static void getExecutionModeLit(ComRoutineExecutionMode val, NAString &result) {
  if (val == COM_ROUTINE_SAFE_EXECUTION)
    result = COM_ROUTINE_SAFE_EXECUTION_LIT;
  else
    result = COM_ROUTINE_FAST_EXECUTION_LIT;
}

short CmpSeabaseDDL::getUsingRoutines(ExeCliInterface *cliInterface, long objUID, Queue *&usingRoutinesQueue) {
  int retcode = 0;
  int cliRC = 0;

  char buf[4000];
  str_sprintf(buf,
              "select trim(catalog_name) || '.' || trim(schema_name) || '.' || trim(object_name), "
              "object_type, object_uid from %s.\"%s\".%s T, %s.\"%s\".%s LU "
              "where LU.using_library_uid = %ld and "
              "T.object_uid = LU.used_udr_uid  and T.valid_def = 'Y' ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES_USAGE, objUID);

  usingRoutinesQueue = NULL;
  cliRC = cliInterface->fetchAllRows(usingRoutinesQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return cliRC;
  }

  if (usingRoutinesQueue->numEntries() == 0) return 100;

  return 0;
}

void CmpSeabaseDDL::createSeabaseLibrary(StmtDDLCreateLibrary *createLibraryNode, NAString &currCatName,
                                         NAString &currSchName) {
  int retcode = 0;

  ComObjectName libraryName(createLibraryNode->getLibraryName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  libraryName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = libraryName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = libraryName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = libraryName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibraryName = libraryName.getExternalName(TRUE);
  const NAString extNameForHbase = catalogNamePart + "." + schemaNamePart + "." + objectNamePart;

  // Verify that the requester has MANAGE_LIBRARY privilege.
  if (isAuthorizationEnabled() && !ComUser::isRootUserID()) {
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);

    PrivMgrComponentPrivileges componentPrivileges(std::string(privMgrMDLoc.data()), CmpCommon::diags());

    if (!componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(), SQLOperation::MANAGE_LIBRARY, true)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }
  }

  // Check to see if user has the authority to create the library
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  long schemaUID = 0;
  ComSchemaClass schemaClass;

  retcode = verifyDDLCreateOperationAuthorized(&cliInterface, SQLOperation::CREATE_LIBRARY, catalogNamePart,
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

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (retcode == 1)  // already exists
  {
    *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extLibraryName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  NAString libFileName = createLibraryNode->getFilename();
  // strip blank spaces
  libFileName = libFileName.strip(NAString::both, ' ');
  if (validateLibraryFileExists(libFileName, FALSE)) {
    deallocEHI(ehi);
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
  if (updateSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT, "N",
                           tableInfo, 0, NULL, 0, NULL, 0, NULL, objUID)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (objUID == -1) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  char *query = new (STMTHEAP) char[1000];

  // We come here only if CQD says use the old style without blobs .
  // So insert a NULL into the blob column.
  str_sprintf(query, "insert into %s.\"%s\".%s values (%ld, '%s',NULL, %d, 0)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES, objUID, libFileName.data(), createLibraryNode->getVersion());

  int cliRC = cliInterface.executeImmediate(query);

  NADELETEBASIC(query, STMTHEAP);
  if (cliRC < 0) {
    deallocEHI(ehi);
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  // hope to remove this call soon by setting thevalid flag to Y sooner
  if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT_LIT,
                           "Y")) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  processReturn();

  return;
}
short CmpSeabaseDDL::isLibBlobStoreValid(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char buf[4000];
  char *query = new (STMTHEAP) char[1000];
  str_sprintf(query, "select [first 1] library_storage from %s.\"%s\".%s ", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES);

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();
  cliRC = cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    CmpCommon::diags()->rewind(diagsMark);
    NADELETEBASIC(query, STMTHEAP);
    return -1;
  }

  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    CmpCommon::diags()->rewind(diagsMark);
    NADELETEBASIC(query, STMTHEAP);
    return -1;
  }

  if (cliRC >= 0) {
    // found the new column.
    NADELETEBASIC(query, STMTHEAP);
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  return 0;
  NADELETEBASIC(query, STMTHEAP);
}

void CmpSeabaseDDL::createSeabaseLibrary2(StmtDDLCreateLibrary *createLibraryNode, NAString &currCatName,
                                          NAString &currSchName) {
  int retcode = 0;

  ComObjectName libraryName(createLibraryNode->getLibraryName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  libraryName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = libraryName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = libraryName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = libraryName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibraryName = libraryName.getExternalName(TRUE);
  const NAString extNameForHbase = catalogNamePart + "." + schemaNamePart + "." + objectNamePart;

  // Verify that the requester has MANAGE_LIBRARY privilege.
  if (isAuthorizationEnabled() && !ComUser::isRootUserID()) {
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);

    PrivMgrComponentPrivileges componentPrivileges(std::string(privMgrMDLoc.data()), CmpCommon::diags());

    if (!componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(), SQLOperation::MANAGE_LIBRARY, true)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }
  }

  // Check to see if user has the authority to create the library
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  long schemaUID = 0;
  ComSchemaClass schemaClass;

  retcode = verifyDDLCreateOperationAuthorized(&cliInterface, SQLOperation::CREATE_LIBRARY, catalogNamePart,
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

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (retcode == 1)  // already exists
  {
    *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extLibraryName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  NAString libFileName = createLibraryNode->getFilename();
  // strip blank spaces
  libFileName = libFileName.strip(NAString::both, ' ');

  // Source file needs to exist on local node for LOB function
  // filetolob to succeed
  if (validateLibraryFileExists(libFileName, FALSE)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }
  size_t lastSlash = libFileName.last('/');
  NAString libNameNoPath;
  if (lastSlash != NA_NPOS)
    libNameNoPath = libFileName(lastSlash + 1, libFileName.length() - lastSlash - 1);
  else {
    /**CmpCommon::diags() << DgSqlCode(-1382)
                      << DgString0(libFileName);
    deallocEHI(ehi);
    processReturn();
    return;*/
    libNameNoPath = libFileName;
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
  if (updateSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT, "N",
                           tableInfo, 0, NULL, 0, NULL, 0, NULL, objUID)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (objUID == -1) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  char *query = new (STMTHEAP) char[1000];

  str_sprintf(query, "insert into %s.\"%s\".%s values (%ld, '%s',filetolob('%s'), %d, 0)", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, objUID, libNameNoPath.data(), libFileName.data(),
              createLibraryNode->getVersion());

  // do not use LOB inlined data for libraries since libraries will be extracted
  cliInterface.holdAndSetCQD("TRAF_LOB_INLINED_DATA_MAXBYTES", "0");
  int cliRC = cliInterface.executeImmediate(query);

  NADELETEBASIC(query, STMTHEAP);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  cliInterface.restoreCQD("TRAF_LOB_INLINED_DATA_MAXBYTES");

  // hope to remove this call soon by setting thevalid flag to Y sooner
  if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT_LIT,
                           "Y")) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  processReturn();

  return;
}

void CmpSeabaseDDL::dropSeabaseLibrary(StmtDDLDropLibrary *dropLibraryNode, NAString &currCatName,
                                       NAString &currSchName) {
  int cliRC = 0;
  int retcode = 0;

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  const NAString &objName = dropLibraryNode->getLibraryName();

  ComObjectName libraryName(objName);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  libraryName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = libraryName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = libraryName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = libraryName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibraryName = libraryName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) return;

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_LIBRARY_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extLibraryName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;
  long objUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                               COM_LIBRARY_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);
  if (objUID < 0 || objectOwnerID == 0 || schemaOwnerID == 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (!isDDLOperationAuthorized(SQLOperation::DROP_LIBRARY, objectOwnerID, schemaOwnerID, objUID, COM_LIBRARY_OBJECT,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  Queue *usingRoutinesQueue = NULL;
  cliRC = getUsingRoutines(&cliInterface, objUID, usingRoutinesQueue);
  if (cliRC < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }
  // If RESTRICT and the library is being used, return an error
  if (cliRC != 100 && dropLibraryNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR) {
    *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_ROUTINES_EXIST);

    deallocEHI(ehi);
    processReturn();
    return;
  }

  usingRoutinesQueue->position();
  for (size_t i = 0; i < usingRoutinesQueue->numEntries(); i++) {
    OutputInfo *rou = (OutputInfo *)usingRoutinesQueue->getNext();

    char *routineName = rou->get(0);
    ComObjectType objectType = PrivMgr::ObjectLitToEnum(rou->get(1));

    if (dropSeabaseObject(ehi, routineName, currCatName, currSchName, objectType, dropLibraryNode->ddlXns(), TRUE,
                          FALSE, FALSE, NAString(""))) {
      deallocEHI(ehi);
      processReturn();
      return;
    }

    // Remove routine from DBRoutinCache
    ComObjectName objectName(routineName);
    QualifiedName qualRoutineName(objectName, STMTHEAP);
    NARoutineDBKey key(qualRoutineName, STMTHEAP);
    NARoutine *cachedNARoutine = pRoutineDBCache->get(&bindWA, &key);

    if (cachedNARoutine) {
      long routineUID = *(long *)rou->get(2);
      pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, routineUID,
                                       dropLibraryNode->ddlXns(), FALSE);
    }
  }

  // can get a slight perf. gain if we pass in objUID
  if (dropSeabaseObject(ehi, objName, currCatName, currSchName, COM_LIBRARY_OBJECT, dropLibraryNode->ddlXns(), TRUE,
                        FALSE, FALSE, NAString(""))) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  deallocEHI(ehi);
  processReturn();
  return;
}

void CmpSeabaseDDL::alterSeabaseLibrary2(StmtDDLAlterLibrary *alterLibraryNode, NAString &currCatName,
                                         NAString &currSchName) {
  int cliRC;
  int retcode;

  NAString libraryName = alterLibraryNode->getLibraryName();
  NAString libFileName = alterLibraryNode->getFilename();

  ComObjectName libName(libraryName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  libName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NAString catalogNamePart = libName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = libName.getSchemaNamePartAsAnsiString(TRUE);
  NAString libNamePart = libName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibName = libName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, libNamePart, COM_LIBRARY_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extLibName);
    processReturn();
    return;
  }

  // strip blank spaces
  libFileName = libFileName.strip(NAString::both, ' ');
  if (validateLibraryFileExists(libFileName, FALSE)) {
    processReturn();
    return;
  }

  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;
  long libUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), libNamePart.data(),
                               COM_LIBRARY_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

  // Check for error getting metadata information
  if (libUID == -1 || objectOwnerID == 0) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting object UID and owner for alter library");
    processReturn();
    return;
  }

  // Verify that the current user has authority to perform operation
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_LIBRARY, objectOwnerID, schemaOwnerID, libUID, COM_LIBRARY_OBJECT,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  long redefTime = NA_JulianTimestamp();
  size_t lastSlash = libFileName.last('/');
  NAString libNameNoPath;
  if (lastSlash != NA_NPOS)
    libNameNoPath = libFileName(lastSlash + 1, libFileName.length() - lastSlash - 1);
  else {
    *CmpCommon::diags() << DgSqlCode(-1382) << DgString0(libFileName);
    processReturn();
    return;
  }
  char buf[2048];  // filename max length is 512. Additional bytes for long
  // library names.
  str_sprintf(
      buf,
      "update %s.\"%s\".%s set library_filename = '%s' , library_storage = filetolob('%s') where library_uid = %ld",
      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, libNameNoPath.data(), libFileName.data(), libUID);

  // do not use LOB inlined data for libraries since libraries will be extracted
  cliInterface.holdAndSetCQD("TRAF_LOB_INLINED_DATA_MAXBYTES", "0");
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }
  cliInterface.restoreCQD("TRAF_LOB_INLINED_DATA_MAXBYTES");

  if (updateObjectRedefTime(&cliInterface, catalogNamePart, schemaNamePart, libNamePart, COM_LIBRARY_OBJECT_LIT,
                            redefTime)) {
    processReturn();
    return;
  }
  SQL_QIKEY qiKey;

  qiKey.ddlObjectUID = libUID;
  qiKey.operation[0] = 'O';
  qiKey.operation[1] = 'R';

  cliRC = SQL_EXEC_SetSecInvalidKeys(1, &qiKey);
  if (cliRC < 0) {
    processReturn();
    return;
  }
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  Queue *usingRoutinesQueue = NULL;
  cliRC = getUsingRoutines(&cliInterface, libUID, usingRoutinesQueue);
  if (cliRC < 0) {
    processReturn();
    return;
  }
  usingRoutinesQueue->position();
  for (size_t i = 0; i < usingRoutinesQueue->numEntries(); i++) {
    OutputInfo *rou = (OutputInfo *)usingRoutinesQueue->getNext();
    char *routineName = rou->get(0);
    ComObjectType objectType = PrivMgr::ObjectLitToEnum(rou->get(1));
    // Remove routine from DBRoutinCache
    ComObjectName objectName(routineName);
    QualifiedName qualRoutineName(objectName, STMTHEAP);
    NARoutineDBKey key(qualRoutineName, STMTHEAP);
    NARoutine *cachedNARoutine = pRoutineDBCache->get(&bindWA, &key);
    if (cachedNARoutine) {
      long routineUID = *(long *)rou->get(2);
      pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, routineUID,
                                       alterLibraryNode->ddlXns(), FALSE);
    }
  }

  return;
}

void CmpSeabaseDDL::alterSeabaseLibrary(StmtDDLAlterLibrary *alterLibraryNode, NAString &currCatName,
                                        NAString &currSchName) {
  int cliRC;
  int retcode;

  NAString libraryName = alterLibraryNode->getLibraryName();
  NAString libFileName = alterLibraryNode->getFilename();

  ComObjectName libName(libraryName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  libName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NAString catalogNamePart = libName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = libName.getSchemaNamePartAsAnsiString(TRUE);
  NAString libNamePart = libName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibName = libName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, libNamePart, COM_LIBRARY_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extLibName);
    processReturn();
    return;
  }

  // strip blank spaces
  libFileName = libFileName.strip(NAString::both, ' ');
  if (validateLibraryFileExists(libFileName, FALSE)) {
    processReturn();
    return;
  }

  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;
  long libUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), libNamePart.data(),
                               COM_LIBRARY_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

  // Check for error getting metadata information
  if (libUID == -1 || objectOwnerID == 0) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting object UID and owner for alter library");
    processReturn();
    return;
  }

  // Verify that the current user has authority to perform operation
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_LIBRARY, objectOwnerID, schemaOwnerID, libUID, COM_LIBRARY_OBJECT,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  long redefTime = NA_JulianTimestamp();

  char buf[2048];  // filename max length is 512. Additional bytes for long
  // library names.
  str_sprintf(buf, "update %s.\"%s\".%s set library_filename = '%s' where library_uid = %ld", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, libFileName.data(), libUID);

  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  if (updateObjectRedefTime(&cliInterface, catalogNamePart, schemaNamePart, libNamePart, COM_LIBRARY_OBJECT_LIT,
                            redefTime)) {
    processReturn();
    return;
  }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  Queue *usingRoutinesQueue = NULL;
  cliRC = getUsingRoutines(&cliInterface, libUID, usingRoutinesQueue);
  if (cliRC < 0) {
    processReturn();
    return;
  }
  usingRoutinesQueue->position();
  for (size_t i = 0; i < usingRoutinesQueue->numEntries(); i++) {
    OutputInfo *rou = (OutputInfo *)usingRoutinesQueue->getNext();
    char *routineName = rou->get(0);
    ComObjectType objectType = PrivMgr::ObjectLitToEnum(rou->get(1));
    // Remove routine from DBRoutinCache
    ComObjectName objectName(routineName);
    QualifiedName qualRoutineName(objectName, STMTHEAP);
    NARoutineDBKey key(qualRoutineName, STMTHEAP);
    NARoutine *cachedNARoutine = pRoutineDBCache->get(&bindWA, &key);
    if (cachedNARoutine) {
      long routineUID = *(long *)rou->get(2);
      pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, routineUID,
                                       alterLibraryNode->ddlXns(), FALSE);
    }
  }

  return;
}

short CmpSeabaseDDL::createSPSQLProcs(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  long libUID =
      getObjectUID(cliInterface, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SPSQL_LIBRARY, COM_LIBRARY_OBJECT_LIT);

  size_t numSPSQLProcs = sizeof(allSPSQLProcs) / sizeof(SPSQLProc);
  for (int i = 0; i < numSPSQLProcs; i++) {
    const SPSQLProc &proc = allSPSQLProcs[i];
    NAString quotedSpjObjName;
    ToQuotedString(quotedSpjObjName, NAString(proc.routineInfo->routine_name), FALSE);
    cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_MD_SCHEMA, quotedSpjObjName,
                                   COM_USER_DEFINED_ROUTINE_OBJECT, TRUE, FALSE);
    if (cliRC < 0)  // Error
    {
      return -1;
    }
    if (cliRC == 1)  // Already exists
    {
      if (deleteFromSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_MD_SCHEMA, quotedSpjObjName,
                                   COM_USER_DEFINED_ROUTINE_OBJECT)) {
        return -1;
      }
    }
    if (updateSeabaseMDSPJ(cliInterface, libUID, SUPER_USER, SUPER_USER, proc.routineInfo, proc.numCols,
                           proc.columnInfo)) {
      return -1;
    }
  }
  return 0;
}

short buildQuery(NAString &sql, const StmtDDLCreateRoutine *stmtDDLCreateRoutine, NABoolean noCreate = FALSE,
                 NABoolean noResultSet = FALSE) {
  const NAString &routineName = stmtDDLCreateRoutine->getRoutineName();
  const ElemDDLParamDefArray &routineParamArray = stmtDDLCreateRoutine->getParamArray();
  const NAString *routineBody = stmtDDLCreateRoutine->getBody();

  int numParams = routineParamArray.entries();

  if (!noCreate) {
    sql.append("CREATE");
  }

  if (stmtDDLCreateRoutine->getRoutineType() == COM_PROCEDURE_TYPE) {
    sql.append(" PROCEDURE ");
  } else if (stmtDDLCreateRoutine->getRoutineType() == COM_SCALAR_UDF_TYPE) {
    sql.append(" FUNCTION ");
  } else {
    CMPASSERT(0);
    return -1;
  }

  sql.append(routineName);
  sql.append(" (");

  for (int i = 0; i < numParams; i++) {
    ElemDDLParamDef *p = routineParamArray[i];
    if (i > 0) {
      sql.append(",");
    }
    switch (p->getParamDirection()) {
      case COM_INPUT_PARAM:
        sql.append("IN ");
        break;
      case COM_OUTPUT_PARAM:
        sql.append("OUT ");
        break;
      case COM_INOUT_PARAM:
        sql.append("INOUT ");
        break;
      default:
        // FIXME
        CMPASSERT(0);
        return -1;
    }
    if (p->getParamName().isNull()) {
      char buf[100];
      str_sprintf(buf, "_arg_%d_", i);
      sql.append(buf);
    } else {
      sql.append(p->getParamName());
    }
    sql.append(" ");
    sql.append(p->getParamDataType()->getSPSQLTypeName());
  }
  sql.append(")");

  if (stmtDDLCreateRoutine->getRoutineType() == COM_SCALAR_UDF_TYPE) {
    // return type
    sql.append(" RETURN ");
    size_t numberOfOutputParams = stmtDDLCreateRoutine->getParamArray().entries() -
                                  stmtDDLCreateRoutine->getFirstReturnedParamPosWithinParamArray();
    CMPASSERT(numberOfOutputParams == 1);

    ElemDDLParamDef *p = routineParamArray[numParams - 1];
    sql.append(p->getParamDataType()->getSPSQLTypeName());
  } else if (!noResultSet) {
    // PROCEDURE dynamic reslut sets
    char buf[100];
    sprintf(buf, " DYNAMIC RESULT SETS %d", stmtDDLCreateRoutine->getMaxResults());
    sql.append(buf);
  }

  // NOTE2ME: This is used to identify the begin of body for SHOWDDL
  sql.append(" AS ");
  sql.append(*routineBody);
  sql.append(";");

  return 0;
}

void CmpSeabaseDDL::createSeabasePackage(ExeCliInterface *cliInterface, StmtDDLCreatePackage *createPackageNode,
                                         NAString &currCatName, NAString &currSchName) {
  int retcode = 0;

  ComObjectName packageName(createPackageNode->getPackageName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  packageName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = packageName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = packageName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = packageName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extPackageName = packageName.getExternalName(TRUE);

  // sub_id column value for SPSQL body in MD text table, 1 for
  // PACKAGE SPEC definition, and 2 for PACKAGE BODY
  int subID = createPackageNode->isCreatePackageBody() ? 2 : 1;

  // Check to see if user has the authority to create the package
  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  ComSchemaClass schemaClass;

  retcode = verifyDDLCreateOperationAuthorized(cliInterface, SQLOperation::CREATE_ROUTINE, catalogNamePart,
                                               schemaNamePart, schemaClass, objectOwnerID, schemaOwnerID);
  if (retcode != 0) {
    handleDDLCreateAuthorizationError(retcode, catalogNamePart, schemaNamePart);
    return;
  }

  retcode = existsInSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_PACKAGE_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    processReturn();
    return;
  }

  // If we are creating package body, we only need to add the body
  // text if the spec is already defined. We do not allow the
  // opposite, i.e. creating package body first and then creating the
  // spec, but it's allowed to create a package body without a spec.
  long objUID = -1;
  NABoolean addTextOnly = FALSE;
  if (retcode == 1 && createPackageNode->isCreatePackageBody()) {
    objUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                          COM_PACKAGE_OBJECT_LIT);
    if (objUID < 0) {
      processReturn();
      return;
    }

    // Check if the package BODY text exists
    char query[1000];
    str_sprintf(query,
                "select count(*) from %s.\"%s\".%s "
                " where text_uid=%ld and sub_id=%d",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID, subID);
    long rowCount = 0;
    int length = 0;
    int cliRC = cliInterface->executeImmediate(query, (char *)&rowCount, &length, FALSE);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return;
    }
    if (rowCount == 0) {
      retcode = 0;
      addTextOnly = TRUE;
    }
  }

  if (retcode == 1)  // already exists
  {
    if (!createPackageNode->isReplace()) {
      if (!createPackageNode->createIfNotExists()) *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extPackageName);
      processReturn();
      return;
    } else {
      if (dropPackageFromMDAndSPSQL(cliInterface, currCatName, currSchName, createPackageNode->getPackageName(),
                                    createPackageNode->ddlXns())) {
        processReturn();
        return;
      }
    }
  }

  if (!addTextOnly) {
    objUID = -1;
    if (updateSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_PACKAGE_OBJECT, "N",
                             NULL, 0, NULL, 0, NULL, 0, NULL, objUID)) {
      processReturn();
      return;
    }

    if (objUID == -1) {
      processReturn();
      return;
    }
  }

  CMPASSERT(objUID != -1);

  // build the SQL to be stored in TEXT and executed in SPSQL
  NAString sql;
  sql.append("CREATE PACKAGE ");
  if (createPackageNode->isCreatePackageBody()) {
    sql.append("BODY ");
  }
  sql.append(createPackageNode->getPackageName());
  sql.append(" AS ");
  sql.append(*createPackageNode->getBody());
  sql.append(";");

  // Execute the SQL qeury in SPSQL
  if (createSPSQLRoutine(cliInterface, &sql)) {
    *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extPackageName);
    processReturn();
    return;
  }

  // store the package SPEC or BODY into table TEXT
  int length = sql.length();
  if (updateTextTable(cliInterface, objUID, COM_ROUTINE_TEXT, subID, sql, NULL, length, false)) {
    processReturn();
    return;
  }

  if (!addTextOnly) {
    // hope to remove this call soon by setting the valid flag to Y sooner
    if (updateObjectValidDef(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_PACKAGE_OBJECT_LIT,
                             "Y")) {
      processReturn();
      return;
    }
  }

  processReturn();
  return;
}

short CmpSeabaseDDL::dropPackageFromMDAndSPSQL(ExeCliInterface *cliInterface, NAString &currCatName,
                                               NAString &currSchName, const NAString &packageName, NABoolean ddlXns) {
  ComObjectName qualifiedName(packageName);
  NAString catalogNamePart = qualifiedName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = qualifiedName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = qualifiedName.getObjectNamePartAsAnsiString(TRUE);

  // Drop procedures and functions defined in the package.
  char query[1000];
  str_sprintf(query,
              "select object_name from %s.\"%s\".%s"
              " where catalog_name = '%s' and schema_name = '%s'"
              " and object_type='%s' and object_name like '%s.%%'",
              TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogNamePart.data(),
              schemaNamePart.data(), COM_USER_DEFINED_ROUTINE_OBJECT_LIT, objectNamePart.data());
  Queue *queue = NULL;
  int cliRC = cliInterface->fetchAllRows(queue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    return -1;
  }
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  QualifiedName qualRoutineName(packageName, schemaNamePart, catalogNamePart, STMTHEAP);
  long oldObjUID;
  queue->position();
  for (size_t i = 0; i < queue->numEntries(); i++) {
    OutputInfo *ri = (OutputInfo *)queue->getNext();
    char *name = ri->get(0);
    char buf[1000];
    str_sprintf(buf, "%s.%s.\"%s\"", catalogNamePart.data(), schemaNamePart.data(), name);
    if (dropSeabaseObject(ehi, buf, currCatName, currSchName, COM_USER_DEFINED_ROUTINE_OBJECT, ddlXns, TRUE, FALSE,
                          FALSE, NAString(""), TRUE, &oldObjUID)) {
      deallocEHI(ehi);
      return -1;
    }
    pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, oldObjUID, ddlXns, FALSE);
  }

  // Removed package from metadata
  if (dropSeabaseObject(ehi, packageName, currCatName, currSchName, COM_PACKAGE_OBJECT, ddlXns, TRUE, FALSE, FALSE,
                        NAString(""))) {
    deallocEHI(ehi);
    return -1;
  }

  deallocEHI(ehi);

  // Drop package from SPSQL
  str_sprintf(query, "call %s.\"%s\".%s('DROP PACKAGE %s;')", TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA,
              SEABASE_SPSQL_DROP_SPJ, packageName.data());
  if (cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/)) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

void CmpSeabaseDDL::dropSeabasePackage(ExeCliInterface *cliInterface, StmtDDLDropPackage *dropPackageNode,
                                       NAString &currCatName, NAString &currSchName) {
  int retcode = 0;

  ComObjectName packageName(dropPackageNode->getPackageName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  packageName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = packageName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = packageName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = packageName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extPackageName = packageName.getExternalName(TRUE);

  retcode = existsInSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_PACKAGE_OBJECT,
                                   TRUE, FALSE);
  if (retcode < 0) {
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    if (NOT dropPackageNode->dropIfExists()) *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extPackageName);
    processReturn();
    return;
  }

  long objUID = 0;
  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;
  objUID = getObjectInfo(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                         COM_PACKAGE_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);
  if (objUID < 0 || objectOwnerID == 0 || schemaOwnerID == 0) {
    processReturn();
    return;
  }

  // Verify user has privilege to drop package
  if (!isDDLOperationAuthorized(SQLOperation::DROP_ROUTINE, objectOwnerID, schemaOwnerID, objUID, COM_PACKAGE_OBJECT,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  if (dropPackageFromMDAndSPSQL(cliInterface, currCatName, currSchName, dropPackageNode->getPackageName(),
                                dropPackageNode->ddlXns())) {
    processReturn();
    return;
  }

  processReturn();
  return;
}

short CmpSeabaseDDL::createSPSQLRoutine(ExeCliInterface *cliInterface, NAString *sql) {
  NAString quotedSql;
  ToQuotedString(quotedSql, *sql);
  char *query = new (STMTHEAP) char[quotedSql.length() + 1000];
  str_sprintf(query, "call %s.\"%s\".%s(%s)", getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SPSQL_CREATE_SPJ,
              quotedSql.data());
  int cliRC = cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/);
  NADELETEBASIC(query, STMTHEAP);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}
short CmpSeabaseDDL::extractLibrary(ExeCliInterface *cliInterface, char *libHandle, char *cachedLibName) {
  struct stat statbuf;
  long libUID = 0;
  short retcode = 0;
  if (stat(cachedLibName, &statbuf) != 0) {
    retcode = ExExeUtilLobExtractLibrary(cliInterface, libHandle, cachedLibName, CmpCommon::diags());
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-4316) << DgString0(cachedLibName);
      processReturn();
    }
  }

  return retcode;
}

void CmpSeabaseDDL::createSeabaseRoutine(StmtDDLCreateRoutine *createRoutineNode, NAString &currCatName,
                                         NAString &currSchName) {
  int retcode = 0;

  ComObjectName routineName(createRoutineNode->getRoutineName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  routineName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = routineName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = routineName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = routineName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extRoutineName = routineName.getExternalName(TRUE);
  ComRoutineType rType = createRoutineNode->getRoutineType();
  ComRoutineLanguage language = createRoutineNode->getLanguageType();
  ComRoutineParamStyle ddlStyle = createRoutineNode->getParamStyle();
  ComRoutineParamStyle style = ddlStyle;
  NABoolean isJava = (language == COM_LANGUAGE_JAVA);
  NABoolean isSPSQL = (createRoutineNode->getBody() != NULL);

  // Check to see if user has the authority to create the routine
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  long schemaUID = 0;
  ComSchemaClass schemaClass;
  NAString libSuffix, libPrefix;
  retcode = verifyDDLCreateOperationAuthorized(&cliInterface, SQLOperation::CREATE_ROUTINE, catalogNamePart,
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

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_USER_DEFINED_ROUTINE_OBJECT, TRUE, FALSE);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  long oldObjUID = -1;  // old object been replaced
  if (retcode == 1)      // already exists
  {
    if (!createRoutineNode->isReplace()) {
      if (!createRoutineNode->createIfNotExists()) *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extRoutineName);
      deallocEHI(ehi);
      processReturn();
      return;
    } else {
      // OR REPLACE exists, drop the previous one
      retcode =
          dropRoutineFromMDAndSPSQL(&cliInterface, ehi, currCatName, currSchName, createRoutineNode->getRoutineName(),
                                    createRoutineNode->getRoutineType(), createRoutineNode->ddlXns(), oldObjUID);
      if (retcode < 0) {
        deallocEHI(ehi);
        processReturn();
        return;
      }
    }
  }

  ComObjectName libName(createRoutineNode->getLibraryName().getQualifiedNameAsAnsiString());
  libName.applyDefaults(currCatAnsiName, currSchAnsiName);
  NAString libCatNamePart = libName.getCatalogNamePartAsAnsiString();
  NAString libSchNamePart = libName.getSchemaNamePartAsAnsiString(TRUE);
  NAString libObjNamePart = libName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extLibraryName = libName.getExternalName(TRUE);
  char externalPath[512];
  char libBlobHandle[LOB_HANDLE_LEN];
  int cliRC = 0;
  long redefTime = 0;
  // this call needs to change
  long libUID = 0;
  long objDataUID = 0;

  int dummy32;
  long dummy64;

  libUID = getObjectInfo(&cliInterface, libCatNamePart, libSchNamePart, libObjNamePart, COM_LIBRARY_OBJECT, dummy32,
                         dummy32, dummy64, objDataUID, TRUE, FALSE, &dummy64, &redefTime);

  if (libUID < 0) {
    processReturn();
    return;
  }

  if (libUID == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-1361) << DgString0(extLibraryName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // read the library  name from the LIBRARIES metadata table
  char *buf = new (STMTHEAP) char[200];

  str_sprintf(buf,
              "select library_filename, library_storage from %s.\"%s\".%s"
              " where library_uid = %ld for read uncommitted access",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, libUID);

  cliRC = cliInterface.fetchRowsPrologue(buf, TRUE /*no exec*/);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    deallocEHI(ehi);
    processReturn();
    return;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    deallocEHI(ehi);
    processReturn();
    return;
  }
  if (cliRC == 100)  // did not find the row
  {
    *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  char *ptr = NULL;
  int len = 0;

  cliInterface.getPtrAndLen(1, ptr, len);
  str_cpy_all(externalPath, ptr, len);
  externalPath[len] = '\0';

  cliInterface.getPtrAndLen(2, ptr, len);
  str_cpy_all(libBlobHandle, ptr, len);
  libBlobHandle[len] = '\0';

  NAString extPath(externalPath);
  size_t lastDot = extPath.last('.');

  if (lastDot != NA_NPOS) libSuffix = extPath(lastDot, extPath.length() - lastDot);

  // determine language and parameter styles based on the library
  // type, unless already specified
  if (!createRoutineNode->isLanguageTypeSpecified()) {
    libSuffix.toUpper();

    if (libSuffix == ".JAR") {
      isJava = TRUE;
      language = COM_LANGUAGE_JAVA;
    } else if (libSuffix == ".SO" || libSuffix == ".DLL") {
      // a known C/C++ library, set
      // language and parameter style below
    } else {
      // language not specified and library name
      // is inconclusive, issue an error
      *CmpCommon::diags() << DgSqlCode(-3284) << DgString0(externalPath);
      processReturn();
    }
  }

  // set parameter style and also language, if not already
  // specified, based on routine type and type of library
  if (isJava) {
    // library is a jar file

    if (rType == COM_PROCEDURE_TYPE)
      // Java stored procedures use the older Java style
      style = COM_STYLE_JAVA_CALL;
    else
      // Java UDFs use the newer Java object style
      style = COM_STYLE_JAVA_OBJ;
  } else if (!isSPSQL) {
    // assume the library is a DLL with C or C++ code
    if (rType == COM_TABLE_UDF_TYPE &&
        (language == COM_LANGUAGE_CPP || !createRoutineNode->isLanguageTypeSpecified())) {
      // Table UDFs (TMUDFs) default to the C++ interface
      language = COM_LANGUAGE_CPP;
      style = COM_STYLE_CPP_OBJ;
    } else if (rType == COM_SCALAR_UDF_TYPE &&
               (language == COM_LANGUAGE_C || !createRoutineNode->isLanguageTypeSpecified())) {
      // scalar UDFs default to C and SQL parameter style
      language = COM_LANGUAGE_C;
      style = COM_STYLE_SQL;
    } else {
      // some invalid combination of routine type, language and
      // library type
      *CmpCommon::diags() << DgSqlCode(-3286);
      processReturn();
      return;
    }
  }  // C/C++ DLL

  if (createRoutineNode->isParamStyleSpecified() && ddlStyle != style) {
    // An unsupported PARAMETER STYLE was specified
    *CmpCommon::diags() << DgSqlCode(-3280);
    processReturn();
    return;
  }

  NAString externalName;
  if (language == COM_LANGUAGE_JAVA && style == COM_STYLE_JAVA_CALL) {
    // the external name is a Java method signature
    externalName = createRoutineNode->getJavaClassName();
    externalName += ".";
    externalName += createRoutineNode->getJavaMethodName();
  } else
    // the external name is a C/C++ entry point or a
    // Java class name
    externalName = createRoutineNode->getExternalName();

  // Verify that current user has authority to create the routine
  // User must be DB__ROOT or have privileges
  if (isAuthorizationEnabled() && !ComUser::isRootUserID()) {
    // For now, go get privileges directly.  If we ever cache routines, then
    // make sure privileges are stored in the cache.
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
    PrivMgrCommands privInterface(privMgrMDLoc.data(), CmpCommon::diags());
    PrivMgrUserPrivs privs;
    PrivStatus retcode = privInterface.getPrivileges(libUID, COM_LIBRARY_OBJECT, ComUser::getCurrentUser(), privs);
    if (retcode != STATUS_GOOD) {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("checking routine privilege");
      processReturn();
      return;
    }

    // Requester must have USAGE privilege on the library
    //
    // SPSQL is disguised as SPJ, no need to have USAGE privilege on
    // the library
    NABoolean hasPriv = TRUE;
    if (!isSPSQL && !privs.hasUsagePriv()) {
      *CmpCommon::diags() << DgSqlCode(-4481) << DgString0("USAGE") << DgString1(extLibraryName.data());
      processReturn();
      return;
    }
  }

  ElemDDLParamDefArray &routineParamArray = createRoutineNode->getParamArray();
  int numParams = routineParamArray.entries();

  if ((createRoutineNode->getRoutineType() == COM_SCALAR_UDF_TYPE) && (numParams > 32)) {
    *CmpCommon::diags() << DgSqlCode(-1550) << DgString0(extRoutineName) << DgInt0(numParams);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // Allocate buffer for generated signature
  char sigBuf[MAX_SIGNATURE_LENGTH] = {'\0'};

  // The quoted SQL string for creating SPSQL procedure
  NAString sql;
  if (isSPSQL) {
    if (buildQuery(sql, createRoutineNode)) {
      *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
      deallocEHI(ehi);
      processReturn();
      return;
    }
    if (createSPSQLRoutine(&cliInterface, &sql)) {
      *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  else if (style == COM_STYLE_JAVA_CALL) {
    // validate routine for Java call based on signature
    int numJavaParam = 0;
    ComFSDataType *paramType = new ComFSDataType[numParams];
    ComUInt32 *subType = new ComUInt32[numParams];
    ComColumnDirection *direction = new ComColumnDirection[numParams];
    NAType *genericType;

    // Gather the param attributes for LM from the paramDefArray previously
    // populated and from the routineparamList generated from paramDefArray.

    for (CollIndex i = 0; (int)i < numParams; i++) {
      paramType[i] = (ComFSDataType)routineParamArray[i]->getParamDataType()->getFSDatatype();
      subType[i] = 0;  // default
      // Set subType for special cases detected by LM
      switch (paramType[i]) {
        case COM_SIGNED_BIN8_FSDT:
        case COM_UNSIGNED_BIN8_FSDT:
        case COM_SIGNED_BIN16_FSDT:
        case COM_SIGNED_BIN32_FSDT:
        case COM_SIGNED_BIN64_FSDT:
        case COM_UNSIGNED_BIN16_FSDT:
        case COM_UNSIGNED_BIN32_FSDT:
        case COM_UNSIGNED_BPINT_FSDT: {
          genericType = routineParamArray[i]->getParamDataType();
          if (genericType->getTypeName() == LiteralNumeric)
            subType[i] = genericType->getPrecision();
          else
            subType[i] = 0;

          break;
        }

        case COM_DATETIME_FSDT: {
          genericType = routineParamArray[i]->getParamDataType();
          DatetimeType &datetimeType = (DatetimeType &)*genericType;
          if (datetimeType.getSimpleTypeName() EQU "DATE")
            subType[i] = 1;
          else if (datetimeType.getSimpleTypeName() EQU "TIME")
            subType[i] = 2;
          else if (datetimeType.getSimpleTypeName() EQU "TIMESTAMP")
            subType[i] = 3;
        }
      }  // end switch paramType[i]

      direction[i] = (ComColumnDirection)routineParamArray[i]->getParamDirection();
    }

    // If the syntax specified a signature, pass that to LanguageManager.
    NAString specifiedSig(createRoutineNode->getJavaSignature());
    char *optionalSig;
    if (specifiedSig.length() == 0)
      optionalSig = NULL;
    else
      optionalSig = (char *)specifiedSig.data();

    ComBoolean isJavaMain = ((str_cmp_ne(createRoutineNode->getJavaMethodName(), "main") == 0) ? TRUE : FALSE);

    LmResult createSigResult;
    LmJavaSignature *lmSignature = new (STMTHEAP) LmJavaSignature(NULL, STMTHEAP);
    createSigResult = lmSignature->createSig(paramType, subType, direction, numParams, COM_UNKNOWN_FSDT, 0,
                                             createRoutineNode->getMaxResults(), optionalSig, isJavaMain, sigBuf,
                                             MAX_SIGNATURE_LENGTH, CmpCommon::diags());
    NADELETE(lmSignature, LmJavaSignature, STMTHEAP);
    delete[] paramType;
    delete[] subType;
    delete[] direction;

    // Lm returned error. Lm fills diags area, so no need to worry about diags.
    if (createSigResult == LM_ERR) {
      *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
      deallocEHI(ehi);
      processReturn();
      return;
    }

    numJavaParam = (isJavaMain ? 1 : numParams);

    if (libBlobHandle[0] != '\0') {
      NAString dummyUser;
      NAString cachedLibName, cachedLibPath;

      if (ComGenerateUdrCachedLibName(extPath, redefTime, libSchNamePart, dummyUser, cachedLibName, cachedLibPath)) {
        *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
        deallocEHI(ehi);
        processReturn();
        return;
      }

      NAString cachedFullName = cachedLibPath + "/" + cachedLibName;

      if (extractLibrary(&cliInterface, libBlobHandle, (char *)cachedFullName.data())) {
        *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
        deallocEHI(ehi);
        processReturn();
        return;
      }

      if (validateRoutine(&cliInterface, createRoutineNode->getJavaClassName(), createRoutineNode->getJavaMethodName(),
                          cachedFullName, sigBuf, numJavaParam, createRoutineNode->getMaxResults(), optionalSig)) {
        *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
        deallocEHI(ehi);
        processReturn();
        return;
      }

    }

    else {
      if (validateRoutine(&cliInterface, createRoutineNode->getJavaClassName(), createRoutineNode->getJavaMethodName(),
                          externalPath, sigBuf, numJavaParam, createRoutineNode->getMaxResults(), optionalSig))

      {
        *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
        deallocEHI(ehi);
        processReturn();
        return;
      }
    }
  }

  else if (style == COM_STYLE_JAVA_OBJ || style == COM_STYLE_CPP_OBJ) {
    // validate existence of the C++ or Java class in the library
    int routineHandle = NullCliRoutineHandle;
    NAString externalPrefix(externalPath);
    NAString externalNameForValidation(externalName);
    NAString containerName;

    if (language == COM_LANGUAGE_C || language == COM_LANGUAGE_CPP) {
      if (libBlobHandle[0] != '\0') {
        NAString dummyUser;
        NAString cachedLibName, cachedLibPath;

        if (ComGenerateUdrCachedLibName(externalPrefix, redefTime, libSchNamePart, dummyUser, cachedLibName,
                                        cachedLibPath)) {
          *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
          deallocEHI(ehi);
          processReturn();
          return;
        }

        NAString cachedFullName = cachedLibPath + "/" + cachedLibName;

        if (extractLibrary(&cliInterface, libBlobHandle, (char *)cachedFullName.data())) {
          *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
          deallocEHI(ehi);
          processReturn();
          return;
        }
        externalPrefix = cachedLibPath;
        containerName = cachedLibName;

      } else {
        // separate the actual DLL name from the prefix
        char separator = '/';
        size_t separatorPos = externalPrefix.last(separator);

        if (separatorPos != NA_NPOS) {
          containerName = externalPrefix(separatorPos + 1, externalPrefix.length() - separatorPos - 1);
          externalPrefix.remove(separatorPos, externalPrefix.length() - separatorPos);
        } else {
          // assume the entire string is a local name
          containerName = externalPrefix;
          externalPrefix = ".";
        }
      }
    } else {
      // For Java, the way the language manager works is that the
      // external path is the fully qualified name of the jar and
      // the container is the class name (external name).  We load
      // the container (the class) by searching in the path (the
      // jar). The external name is the method name, which in this
      // case is the constructor of the class, <init>.

      // leave externalPrevix unchanged, fully qualified jar file

      if (libBlobHandle[0] != '\0') {
        NAString dummyUser;
        NAString cachedLibName, cachedLibPath;
        NAString libSchema(libSchNamePart);
        if (ComGenerateUdrCachedLibName(extPath, redefTime, libSchNamePart, dummyUser, cachedLibName, cachedLibPath)) {
          *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
          deallocEHI(ehi);
          processReturn();
          return;
        }

        NAString cachedFullName = cachedLibPath + "/" + cachedLibName;

        if (extractLibrary(&cliInterface, libBlobHandle, (char *)cachedFullName.data())) {
          *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(extRoutineName);
          deallocEHI(ehi);
          processReturn();
          return;
        }
        externalPrefix = cachedFullName;
        containerName = externalName;
        externalNameForValidation = "<init>";
      } else {
        containerName = externalName;
        externalNameForValidation = "<init>";
      }
    }

    // use a CLI call to validate that the library contains the routine
    if (cliInterface.getRoutine(NULL,  // No InvocationInfo specified in this step
                                0, NULL, 0, (int)language, (int)style, externalNameForValidation.data(),
                                containerName.data(), externalPrefix.data(), extLibraryName.data(), &routineHandle,
                                CmpCommon::diags()) != LME_ROUTINE_VALIDATED) {
      if (routineHandle != NullCliRoutineHandle) cliInterface.putRoutine(routineHandle, CmpCommon::diags());

      CMPASSERT(CmpCommon::diags()->mainSQLCODE() < 0);
      processReturn();
      return;
    }

    cliInterface.putRoutine(routineHandle, CmpCommon::diags());
  }

  ComTdbVirtTableColumnInfo *colInfoArray =
      (ComTdbVirtTableColumnInfo *)new (STMTHEAP) ComTdbVirtTableColumnInfo[numParams];

  if (buildColInfoArray(&routineParamArray, colInfoArray)) {
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
  if (updateSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                           COM_USER_DEFINED_ROUTINE_OBJECT, "N", tableInfo, numParams, colInfoArray, 0, NULL, 0, NULL,
                           objUID)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (objUID == -1) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  NAString udrType;
  getRoutineTypeLit(createRoutineNode->getRoutineType(), udrType);
  NAString languageType;
  if (isSPSQL) {
    // FIXME: For SPSQL function, we need to change the LANGUAGE to
    // JAVA right before writing to metadata.
    getLanguageTypeLit(COM_LANGUAGE_JAVA, languageType);
  } else {
    getLanguageTypeLit(language, languageType);
  }
  NAString sqlAccess;
  getSqlAccessLit(createRoutineNode->getSqlAccess(), sqlAccess);
  NAString paramStyle;
  if (isSPSQL) {
    // FIXME: For SPSQL function, we need to change the STYLE to
    // JAVA_CALL right before writing to metadata.
    getParamStyleLit(COM_STYLE_JAVA_CALL, paramStyle);
  } else {
    getParamStyleLit(style, paramStyle);
  }
  NAString transactionAttributes;
  ComRoutineTransactionAttributes transAttrs = createRoutineNode->getTransactionAttributes();
  if (isSPSQL && transAttrs == COM_UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE &&
      createRoutineNode->getRoutineType() == COM_SCALAR_UDF_TYPE) {
    // Default transaction attribute of SPSQL function to
    // TRANSACTION REQUIRED
    transAttrs = COM_TRANSACTION_REQUIRED;
  }
  getTransAttributesLit(transAttrs, transactionAttributes);
  NAString parallelism;
  getParallelismLit(createRoutineNode->getParallelism(), parallelism);
  NAString externalSecurity;
  getExternalSecurityLit(createRoutineNode->getExternalSecurity(), externalSecurity);
  NAString executionMode;
  getExecutionModeLit(createRoutineNode->getExecutionMode(), executionMode);

  if (0 == strlen(sigBuf)) sigBuf[0] = ' ';
  char *query = new (STMTHEAP) char[2000 + MAX_SIGNATURE_LENGTH];
  str_sprintf(query,
              "insert into %s.\"%s\".%s values (%ld, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', "
              "'%s', '%s', '%s', '%s', %ld, '%s', 0)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_ROUTINES, objUID, udrType.data(), languageType.data(),
              createRoutineNode->isDeterministic() ? "Y" : "N", sqlAccess.data(),
              createRoutineNode->isCallOnNull() ? "Y" : "N", createRoutineNode->isIsolate() ? "Y" : "N",
              paramStyle.data(), transactionAttributes.data(), createRoutineNode->getMaxResults(),
              createRoutineNode->getStateAreaSize(), externalName.data(), parallelism.data(),
              createRoutineNode->getUserVersion().data(), externalSecurity.data(), executionMode.data(), libUID,
              sigBuf);

  cliRC = cliInterface.executeImmediate(query);
  NADELETEBASIC(query, STMTHEAP);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  char *query1 = new (STMTHEAP) char[1000];
  str_sprintf(query1, "insert into %s.\"%s\".%s values (%ld, %ld, 0)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES_USAGE, libUID, objUID);

  cliRC = cliInterface.executeImmediate(query1);
  NADELETEBASIC(query1, STMTHEAP);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  if (isSPSQL) {
    // store the SPSQL body into table trafodion._MD_.text only field
    // TEXT_UID and TEXT in table TEXT are filled
    int length = sql.length();
    cliRC = updateTextTable(&cliInterface, objUID, COM_ROUTINE_TEXT, 0, sql, NULL, length, false);
    if (cliRC < 0) {
      processReturn();
      return;
    }
  }

  // hope to remove this call soon by setting the valid flag to Y sooner
  if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                           COM_USER_DEFINED_ROUTINE_OBJECT_LIT, "Y")) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // Remove cached entries in other processes
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  QualifiedName qualRoutineName(routineName, STMTHEAP);
  if (oldObjUID != -1) {
    // If replaced existing object, remove the cached entries for the
    // old object in other processes.
    pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, oldObjUID,
                                     createRoutineNode->ddlXns(), FALSE);
  }
  pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, objUID,
                                   createRoutineNode->ddlXns(), FALSE);

  processReturn();
  return;
}

int isSPSQLRoutine(ExeCliInterface *cliInterface, long objUID) {
  char query[1000];
  str_sprintf(query, "select external_name from %s.\"%s\".%s where udr_uid = %ld", TRAFODION_SYSTEM_CATALOG,
              SEABASE_MD_SCHEMA, SEABASE_ROUTINES, objUID);
  char extName[1024];
  int length;
  int cliRC = cliInterface->executeImmediate(query, (char *)&extName, &length);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  if (cliRC == 100)  // not found in routines table
  {
    *CmpCommon::diags() << DgSqlCode(-4092);
    return -1;
  }
  if (str_cmp_ne(extName, SEABASE_SPSQL_CONTAINER "." SEABASE_SPSQL_CALL) == 0 ||
      str_cmp_ne(extName, SEABASE_SPSQL_CONTAINER "." SEABASE_SPSQL_CALL_FUNC) == 0) {
    return 1;
  }
  return 0;
}

int isSPSQLPackageRoutine(ExeCliInterface *cliInterface, long objUID) {
  // For SPSQL routines, we check if they are belong to an package by
  // check the name
  char query[1000];
  str_sprintf(query, "select object_name from %s.\"%s\".%s where object_uid = %ld", TRAFODION_SYSTEM_CATALOG,
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS, objUID);
  char objName[1024];
  int length = 0;
  int cliRC = cliInterface->executeImmediate(query, (char *)&objName, &length);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  if (cliRC == 100)  // routine object not found
  {
    *CmpCommon::diags() << DgSqlCode(-4092);
    return -1;
  }
  if (strchr(objName, '.') != NULL) {
    // There is a '.' in the name, assume it to be a routine within
    // a package
    return 1;
  }
  return 0;
}

inline int skipDropSPSQLRoutine(ExeCliInterface *cliInterface, ComRoutineType routineType, long objUID) {
  // SPSQL can only support PROCEDURE and SCALAR_UDF, so routines of
  // other types are always skipped
  if (routineType != COM_PROCEDURE_TYPE && routineType != COM_SCALAR_UDF_TYPE) {
    return 1;
  }

  int cliRC = isSPSQLRoutine(cliInterface, objUID);
  if (cliRC < 0) {
    return cliRC;
  }
  if (cliRC == 0) {
    // Not an SPSQL Routine
    return 1;
  }

  // Routines belong to a package do not need to be dropped from SPSQL
  return isSPSQLPackageRoutine(cliInterface, objUID);
}

int dropSPSQLRoutine(ExeCliInterface *cliInterface, const NAString &routineName, ComRoutineType routineType) {
  int cliRC = 0;
  const char *type = NULL;
  switch (routineType) {
    case COM_PROCEDURE_TYPE:
      type = "PROCEDURE";
      break;
    default:
      type = "FUNCTION";
  }
  char query[1000];
  str_sprintf(query, "call %s.\"%s\".%s('DROP %s %s;')", TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA,
              SEABASE_SPSQL_DROP_SPJ, type, routineName.data());
  cliRC = cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  }
  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return cliRC;
}

short CmpSeabaseDDL::dropRoutineFromMDAndSPSQL(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                               NAString &currCatName, NAString &currSchName,
                                               const NAString &routineName, ComRoutineType routineType,
                                               NABoolean ddlXns, long &objUID) {
  ComObjectName qualifiedName(routineName);
  NAString catalogNamePart = qualifiedName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = qualifiedName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = qualifiedName.getObjectNamePartAsAnsiString(TRUE);

  if (objUID == -1) {
    objUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                          COM_USER_DEFINED_ROUTINE_OBJECT_LIT);
    if (objUID < 0) {
      return -1;
    }
  }

  // If this is not an standalone SPSQL routine, then we should skip
  // dropping it from SPSQL
  int cliRC = skipDropSPSQLRoutine(cliInterface, routineType, objUID);
  if (cliRC < 0) {
    return -1;
  }
  NABoolean skipDropSPSQL = (cliRC == 1);

  // Removed routine from metadata
  if (dropSeabaseObject(ehi, routineName, currCatName, currSchName, COM_USER_DEFINED_ROUTINE_OBJECT, ddlXns, TRUE,
                        FALSE, FALSE, NAString(""))) {
    return -1;
  }

  if (skipDropSPSQL) {
    return 0;
  }
  return dropSPSQLRoutine(cliInterface, routineName, routineType);
}

void CmpSeabaseDDL::dropSeabaseRoutine(StmtDDLDropRoutine *dropRoutineNode, NAString &currCatName,
                                       NAString &currSchName) {
  int retcode = 0;

  ComObjectName routineName(dropRoutineNode->getRoutineName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  routineName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = routineName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = routineName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = routineName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extRoutineName = routineName.getExternalName(TRUE);

  ExpHbaseInterface *ehi = NULL;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_USER_DEFINED_ROUTINE_OBJECT, TRUE, FALSE);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    if (NOT dropRoutineNode->dropIfExists()) *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extRoutineName);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // get objectOwner
  long objUID = 0;
  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;

  // see if routine is cached
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  NARoutineDB *pRoutineDBCache = ActiveSchemaDB()->getNARoutineDB();
  QualifiedName qualRoutineName(routineName, STMTHEAP);
  NARoutineDBKey key(qualRoutineName, STMTHEAP);

  NARoutine *cachedNARoutine = pRoutineDBCache->get(&bindWA, &key);
  if (cachedNARoutine) {
    objUID = cachedNARoutine->getRoutineID();
    objectOwnerID = cachedNARoutine->getObjectOwner();
    schemaOwnerID = cachedNARoutine->getSchemaOwner();
  } else {
    objUID = getObjectInfo(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                           COM_USER_DEFINED_ROUTINE_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);
    if (objUID < 0 || objectOwnerID == 0 || schemaOwnerID == 0) {
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  // Verify user has privilege to drop routine
  if (!isDDLOperationAuthorized(SQLOperation::DROP_ROUTINE, objectOwnerID, schemaOwnerID, objUID,
                                COM_USER_DEFINED_ROUTINE_OBJECT, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // Determine if this function is referenced by any other objects.
  int cliRC = 0;
  Queue *usingViewsQueue = NULL;
  if (dropRoutineNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR) {
    NAString usingObjName;
    cliRC = getUsingObject(&cliInterface, objUID, usingObjName);
    if (cliRC < 0) {
      deallocEHI(ehi);
      processReturn();

      return;
    }

    if (cliRC != 100)  // found an object
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_VIEW_EXISTS) << DgTableName(usingObjName);

      deallocEHI(ehi);
      processReturn();

      return;
    }
  } else if (dropRoutineNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR) {
    cliRC = getUsingViews(&cliInterface, objUID, usingViewsQueue);
    if (cliRC < 0) {
      deallocEHI(ehi);
      processReturn();

      return;
    }
  }

  if (usingViewsQueue) {
    usingViewsQueue->position();
    for (int idx = 0; idx < usingViewsQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)usingViewsQueue->getNext();

      char *viewName = vi->get(0);

      if (dropOneTableorView(cliInterface, viewName, COM_VIEW_OBJECT, false))

      {
        deallocEHI(ehi);
        processReturn();

        return;
      }
    }
  }

  // Removed routine from metadata and SPSQL if needed
  if (dropRoutineFromMDAndSPSQL(&cliInterface, ehi, currCatName, currSchName, dropRoutineNode->getRoutineName(),
                                dropRoutineNode->getRoutineType(), dropRoutineNode->ddlXns(), objUID)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // Remove cached entries in other processes
  pRoutineDBCache->removeNARoutine(qualRoutineName, ComQiScope::REMOVE_FROM_ALL_USERS, objUID,
                                   dropRoutineNode->ddlXns(), FALSE);

  deallocEHI(ehi);
  processReturn();
  return;
}

short CmpSeabaseDDL::validateRoutine(ExeCliInterface *cliInterface, const char *className, const char *methodName,
                                     const char *externalPath, char *signature, int numSqlParam, int maxResultSets,
                                     const char *optionalSig) {
  if (str_cmp_ne(className, SEABASE_SPSQL_CONTAINER) == 0 && str_cmp_ne(methodName, SEABASE_SPSQL_CALL) == 0) {
    // This is a SPSQL in disguise, skip validation.
    return 0;
  }

  //
  // Now proceed with the internal CALL statement...
  //

  int sigLen = 0;
  if (signature) sigLen = str_len(signature) + 1;

  char *query = new (STMTHEAP) char[2000 + sigLen];
  str_sprintf(query, "call %s.\"%s\".%s ('%s', '%s', '%s', '%s', %d, %d, %d, ?x, ?y, ?z)", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_VALIDATE_SPJ, className, methodName, externalPath, signature, numSqlParam,
              maxResultSets, optionalSig ? 1 : 0);

  int cliRC = cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  NADELETEBASIC(query, STMTHEAP);

  char *ptr = NULL;
  int len = 0;
  int errCode = 0;

  cliInterface->getPtrAndLen(1, ptr, len);
  str_cpy_all(signature, ptr, len);
  signature[len] = '\0';
  cliInterface->getPtrAndLen(2, ptr, len);
  errCode = *(int *)ptr;

  // Check for errors returned from VALIDATEROUTINE
  switch (errCode) {
    case 0:  // Success - Check to see if returned signature is null
      if (signature[0] NEQ '\0')
        return 0;
      else
        return -1;
      break;
    case 11205:  // Class not found
      *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(className) << DgString1(externalPath);
      break;
    case 11206:  // Class definition not found
      *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(className);
      break;
    case 11230:  // Overloaded methods were found
      *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(methodName) << DgString1(className);
      break;
    case 11239:  // No compatible methods were found
      *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(methodName) << DgString1(className);
      break;
    case 11231:  // Method found but not public
      if (signature[0] NEQ '\0')
        *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(NAString(methodName) + signature)
                            << DgString1(className);
      break;
    case 11232:  // Method found but not static
      if (signature[0] NEQ '\0')
        *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(NAString(methodName) + signature)
                            << DgString1(className);
      break;
    case 11233:  // Method found but not void
      if (signature[0] NEQ '\0')
        *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(NAString(methodName) + signature)
                            << DgString1(className);
      break;
    case 11234:  // Method not found
      if (signature[0] NEQ '\0')
        *CmpCommon::diags() << DgSqlCode(-errCode) << DgString0(NAString(methodName) + signature)
                            << DgString1(className);
      break;
    default:  // Unknown error code
      break;
  }
  return -1;

}  // CmpSeabaseDDL::validateRoutine

short CmpSeabaseDDL::createSeabaseLibmgr(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  int cliRC = 0;

  if ((CmpCommon::context()->isUninitializedSeabase()) &&
      (CmpCommon::context()->uninitializedSeabaseErrNum() == -TRAF_NOT_INITIALIZED)) {
    *CmpCommon::diags() << DgSqlCode(-TRAF_NOT_INITIALIZED);
    return -1;
  }

  NAString jarLocation(getenv("TRAF_HOME"));
  jarLocation += "/export/lib/lib_mgmt.jar";
  char queryBuf[strlen(getSystemCatalog()) + sizeof(SEABASE_LIBMGR_SCHEMA) + sizeof(SEABASE_LIBMGR_LIBRARY) +
                sizeof(DB__LIBMGRROLE) + jarLocation.length() + 100];
  char nameSpaceBuf[strlen(TRAF_RESERVED_NAMESPACE1) + 100];

  if (CmpCommon::context()->useReservedNamespace())
    snprintf(nameSpaceBuf, sizeof(nameSpaceBuf), "namespace '%s'", TRAF_RESERVED_NAMESPACE1);
  else
    nameSpaceBuf[0] = '\0';

  // Create the SEABASE_LIBMGR_SCHEMA schema
  snprintf(queryBuf, sizeof(queryBuf), "create schema if not exists %s.\"%s\" authorization %s %s", getSystemCatalog(),
           SEABASE_LIBMGR_SCHEMA, DB__ROOT, nameSpaceBuf);

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // Create the SEABASE_LIBMGR_LIBRARY library
  snprintf(queryBuf, sizeof(queryBuf), "create library %s.\"%s\".%s file '%s'", getSystemCatalog(),
           SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY, jarLocation.data());

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (createSeabaseLibmgrCPPLib(cliInterface) < 0) return -1;

  return (createLibmgrProcs(cliInterface));
}

short CmpSeabaseDDL::createLibmgrProcs(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  // Create SPSQL internal SPJs
  if (createSPSQLProcs(cliInterface)) {
    return -1;
  }

  // Create the UDRs if they don't already exist
  for (int i = 0; i < sizeof(allLibmgrRoutineInfo) / sizeof(LibmgrRoutineInfo); i++) {
    // Get the next routine details
    const LibmgrRoutineInfo &prd = allLibmgrRoutineInfo[i];

    const QString *qs = NULL;
    int sizeOfqs = 0;
    const char *libName = NULL;

    qs = prd.newDDL;
    sizeOfqs = prd.sizeOfnewDDL;

    int qryArraySize = sizeOfqs / sizeof(QString);
    char *gluedQuery;
    int gluedQuerySize;
    glueQueryFragments(qryArraySize, qs, gluedQuery, gluedQuerySize);

    switch (prd.whichLib) {
      case LibmgrRoutineInfo::JAVA_LIB:
        libName = SEABASE_LIBMGR_LIBRARY;
        break;
      case LibmgrRoutineInfo::CPP_LIB:
        libName = SEABASE_LIBMGR_LIBRARY_CPP;
        break;
      default:
        CMPASSERT(0);
    }

    param_[0] = getSystemCatalog();
    param_[1] = SEABASE_LIBMGR_SCHEMA;
    param_[2] = getSystemCatalog();
    param_[3] = SEABASE_LIBMGR_SCHEMA;
    param_[4] = libName;

    // Review comment - make sure size of queryBuf is big enough to hold
    // generated text.
    char queryBuf[strlen(getSystemCatalog()) * 2 + sizeof(SEABASE_LIBMGR_SCHEMA) * 2 + sizeof(SEABASE_LIBMGR_LIBRARY) +
                  gluedQuerySize + 200];

    snprintf(queryBuf, sizeof(queryBuf), gluedQuery, param_[0], param_[1], param_[2], param_[3], param_[4]);
    NADELETEBASICARRAY(gluedQuery, STMTHEAP);

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }  // for

  return (grantLibmgrPrivs(cliInterface));
}

// If authorization is enabled, grant privileges to DB__LIBMGRROLE
short CmpSeabaseDDL::grantLibmgrPrivs(ExeCliInterface *cliInterface) {
  if (!isAuthorizationEnabled()) return 0;

  ULng32 flagsToSet = DISABLE_EXTERNAL_AUTH_CHECKS;
  flagsToSet |= INTERNAL_QUERY_FROM_EXEUTIL;
  PushAndSetSqlParserFlags savedParserFlags(flagsToSet);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(flagsToSet);

  int cliRC = 0;
  char queryBuf[strlen(getSystemCatalog()) + sizeof(SEABASE_LIBMGR_SCHEMA) + sizeof(SEABASE_LIBMGR_LIBRARY) +
                MAXOF(sizeof(DB__LIBMGRROLE), sizeof(PUBLIC_AUTH_NAME)) + 200];
  for (int i = 0; i < sizeof(allLibmgrRoutineInfo) / sizeof(LibmgrRoutineInfo); i++) {
    // Get the next procedure routine details
    const LibmgrRoutineInfo &prd = allLibmgrRoutineInfo[i];
    const char *grantee = NULL;
    const char *grantOption = "";

    switch (prd.whichRole) {
      case LibmgrRoutineInfo::LIBMGR_ROLE:
        grantee = DB__LIBMGRROLE;
        grantOption = " with grant option";
        break;
      case LibmgrRoutineInfo::PUBLIC:
        grantee = PUBLIC_AUTH_NAME;
        break;
      default:
        CMPASSERT(0);
    }

    snprintf(queryBuf, sizeof(queryBuf), "grant execute on %s %s.\"%s\".%s to %s%s", prd.udrType, getSystemCatalog(),
             SEABASE_LIBMGR_SCHEMA, prd.newName, grantee, grantOption);
    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC < 0) {
      SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(flagsToSet);
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(flagsToSet);
  return 0;
}

short CmpSeabaseDDL::upgradeSeabaseLibmgr(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  int cliRC = 0;

  cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY,
                                 COM_LIBRARY_OBJECT, TRUE, FALSE);
  if (cliRC < 0) return -1;

  if (cliRC == 0)  // does not exist
  {
    // give an error if the Java library does not exist, since that is
    // an indication that we never ran
    // INITIALIZE TRAFODION, CREATE LIBRARY MANAGEMENT
    NAString libraryName(getSystemCatalog());
    libraryName + ".\"" + SEABASE_LIBMGR_SCHEMA + "\"" + SEABASE_LIBMGR_LIBRARY;
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(libraryName.data());
    return -1;
  }

  // Update the jar locations for system procedures and functions.  This should
  // be done before adding any new jar's since we use a system procedure to add
  // procedures.
  NAString jarLocation(getenv("TRAF_HOME"));
  jarLocation += "/export/lib";

  char queryBuf[1000];

  // trafodion-sql_currversion.jar
  int stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                            "update %s.\"%s\".%s  "
                            "set library_filename = '%s/trafodion-sql-currversion.jar' "
                            "where library_uid = "
                            "(select object_uid from %s.\"%s\".%s "
                            " where object_name = '%s'  and object_type = 'LB')",
                            getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, jarLocation.data(),
                            getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_VALIDATE_LIBRARY);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // lib_mgmt.jar
  stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                      "update %s.\"%s\".%s  "
                      "set library_filename = '%s/lib_mgmt.jar' "
                      "where library_uid = "
                      "(select object_uid from %s.\"%s\".%s "
                      " where object_name = '%s'  and object_type = 'LB')",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, jarLocation.data(), getSystemCatalog(),
                      SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_LIBMGR_LIBRARY);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // libudr_predef.so
  NAString dllLocation(getenv("TRAF_HOME"));
  dllLocation += "/export/lib64";
  if (strcmp(getenv("SQ_MBTYPE"), "64d") == 0) dllLocation += "d";

  stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                      "update %s.\"%s\".%s  "
                      "set library_filename = '%s/libudr_predef.so' "
                      "where library_uid = "
                      "(select object_uid from %s.\"%s\".%s "
                      " where object_name = '%s'  and object_type = 'LB')",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, dllLocation.data(), getSystemCatalog(),
                      SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_LIBMGR_LIBRARY_CPP);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // update the redef_time of the system library, or the .jar or .so file int $UDR_CACHE_LIBDIR will not be UPGRADE
  stmtSize =
      snprintf(queryBuf, sizeof(queryBuf),
               "update %s.\"%s\".%s set redef_time = %ld where object_name in ('%s', '%s') and object_type = 'LB'",
               getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, NA_JulianTimestamp(), SEABASE_LIBMGR_LIBRARY,
               SEABASE_LIBMGR_LIBRARY_CPP);
  CMPASSERT(stmtSize < sizeof(queryBuf));
  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // now check for the C++ library, which was added in Trafodion 2.3
  cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY_CPP,
                                 COM_LIBRARY_OBJECT, TRUE, FALSE);
  if (cliRC < 0) return -1;

  if (cliRC == 0) {
    // The Java library exists, but the C++ library does not yet
    // exist. This means that we last created or upgraded the
    // library management subsystem in Trafodion 2.2 or earlier.
    // Create the C++ library, as it is needed for Trafodion 2.3
    // and higher.
    if (createSeabaseLibmgrCPPLib(cliInterface) < 0) return -1;
  }

  return (createLibmgrProcs(cliInterface));
}

short CmpSeabaseDDL::upgradeSeabaseLibmgr2(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  int cliRC = 0;

  cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY,
                                 COM_LIBRARY_OBJECT, TRUE, FALSE);
  if (cliRC < 0) return -1;

  if (cliRC == 0)  // does not exist
  {
    // give an error if the Java library does not exist, since that is
    // an indication that we never ran
    // INITIALIZE TRAFODION, CREATE LIBRARY MANAGEMENT
    NAString libraryName(getSystemCatalog());
    libraryName +=
        NAString(".\"") + NAString(SEABASE_LIBMGR_SCHEMA) + NAString("\".") + NAString(SEABASE_LIBMGR_LIBRARY);
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(libraryName.data());
    return -1;
  }

  // Update the jar locations for system procedures and functions.  This should
  // be done before adding any new jar's since we use a system procedure to add
  // procedures.
  NAString jarLocation(getenv("TRAF_HOME"));
  jarLocation += "/export/lib";

  char queryBuf[1000];

  // trafodion-sql_currversion.jar
  int stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                            "update %s.\"%s\".%s  "
                            "set library_filename = 'trafodion-sql-currversion.jar', "
                            "library_storage =   NULL "
                            "where library_uid = "
                            "(select object_uid from %s.\"%s\".%s "
                            " where object_name = '%s'  and object_type = 'LB')",
                            getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, getSystemCatalog(),
                            SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_VALIDATE_LIBRARY);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // do not use LOB inlined data for libraries since libraries will be extracted
  cliInterface->holdAndSetCQD("TRAF_LOB_INLINED_DATA_MAXBYTES", "0");

  // lib_mgmt.jar
  stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                      "update %s.\"%s\".%s  "
                      "set library_filename = 'lib_mgmt.jar', "
                      "library_storage = filetolob('%s/lib_mgmt.jar') "
                      "where library_uid = "
                      "(select object_uid from %s.\"%s\".%s "
                      " where object_name = '%s'  and object_type = 'LB')",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, jarLocation.data(), getSystemCatalog(),
                      SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_LIBMGR_LIBRARY);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // libudr_predef.so
  NAString dllLocation(getenv("TRAF_HOME"));
  dllLocation += "/export/lib64";
  if (strcmp(getenv("SQ_MBTYPE"), "64d") == 0) dllLocation += "d";

  stmtSize = snprintf(queryBuf, sizeof(queryBuf),
                      "update %s.\"%s\".%s  "
                      "set library_filename = 'libudr_predef.so', "
                      "library_storage = filetolob('%s/libudr_predef.so') "
                      "where library_uid = "
                      "(select object_uid from %s.\"%s\".%s "
                      " where object_name = '%s'  and object_type = 'LB')",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, dllLocation.data(), getSystemCatalog(),
                      SEABASE_MD_SCHEMA, SEABASE_OBJECTS, SEABASE_LIBMGR_LIBRARY_CPP);
  CMPASSERT(stmtSize < sizeof(queryBuf));

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  cliInterface->restoreCQD("TRAF_LOB_INLINED_DATA_MAXBYTES");

  // update the redef_time of the system library, or the .jar or .so file int $UDR_CACHE_LIBDIR will not be UPGRADE
  stmtSize =
      snprintf(queryBuf, sizeof(queryBuf),
               "update %s.\"%s\".%s set redef_time = %ld where object_name in ('%s', '%s') and object_type = 'LB'",
               getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, NA_JulianTimestamp(), SEABASE_LIBMGR_LIBRARY,
               SEABASE_LIBMGR_LIBRARY_CPP);
  CMPASSERT(stmtSize < sizeof(queryBuf));
  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // now check for the C++ library, which was added in Trafodion 2.3
  cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY_CPP,
                                 COM_LIBRARY_OBJECT, TRUE, FALSE);
  if (cliRC < 0) return -1;

  if (cliRC == 0) {
    // The Java library exists, but the C++ library does not yet
    // exist. This means that we last created or upgraded the
    // library management subsystem in Trafodion 2.2 or earlier.
    // Create the C++ library, as it is needed for Trafodion 2.3
    // and higher.
    if (createSeabaseLibmgrCPPLib(cliInterface) < 0) return -1;
  }

  return (createLibmgrProcs(cliInterface));
}

short CmpSeabaseDDL::dropSeabaseLibmgr(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  int cliRC = 0;

  ULng32 flagsToSet = DISABLE_EXTERNAL_AUTH_CHECKS;
  flagsToSet |= INTERNAL_QUERY_FROM_EXEUTIL;
  PushAndSetSqlParserFlags savedParserFlags(flagsToSet);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(flagsToSet);

  char queryBuf[strlen(getSystemCatalog()) + sizeof(SEABASE_LIBMGR_SCHEMA) + 100];

  str_sprintf(queryBuf, "drop schema if exists %s.\"%s\" cascade ", getSystemCatalog(), SEABASE_LIBMGR_SCHEMA);

  // Drop the SEABASE_LIBMGR_SCHEMA schema
  cliRC = cliInterface->executeImmediate(queryBuf);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(flagsToSet);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

short CmpSeabaseDDL::createSeabaseLibmgrCPPLib(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  NAString dllLocation(getenv("TRAF_HOME"));
  dllLocation += "/export/lib64";
  if (strcmp(getenv("SQ_MBTYPE"), "64d") == 0) dllLocation += "d";
  // for now we use the same DLL as for the predefined UDRs
  dllLocation += "/libudr_predef.so";
  char queryBuf[strlen(getSystemCatalog()) + strlen(SEABASE_LIBMGR_SCHEMA) + strlen(SEABASE_LIBMGR_LIBRARY_CPP) +
                dllLocation.length() + 100];

  // Create the SEABASE_LIBMGR_LIBRARY_CPP library
  snprintf(queryBuf, sizeof(queryBuf), "create library %s.\"%s\".%s file '%s'", getSystemCatalog(),
           SEABASE_LIBMGR_SCHEMA, SEABASE_LIBMGR_LIBRARY_CPP, dllLocation.data());

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

void CmpSeabaseDDL::createSeabaseTriggers(ExeCliInterface *cliInterface, StmtDDLCreateTrigger *CreateTrigger,
                                          NAString &currCatName, NAString &currSchName) {
  int retcode = 0;
  ComObjectName triggername(CreateTrigger->getTriggerName());
  ComObjectName tableName(CreateTrigger->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  triggername.applyDefaults(currCatAnsiName, currSchAnsiName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = triggername.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = triggername.getSchemaNamePartAsAnsiString(TRUE);
  const NAString triggerNamePart = triggername.getObjectNamePartAsAnsiString(TRUE);
  const NAString tableNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

  int objectOwnerID = SUPER_USER;
  int schemaOwnerID = SUPER_USER;
  ComSchemaClass schemaClass;
  retcode = verifyDDLCreateOperationAuthorized(cliInterface, SQLOperation::CREATE_TRIGGER, catalogNamePart,
                                               schemaNamePart, schemaClass, objectOwnerID, schemaOwnerID);
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

  retcode = lockObjectDDL(tableName, COM_BASE_TABLE_OBJECT, CreateTrigger->isVolatile(), CreateTrigger->isExternal());
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, triggerNamePart, COM_TRIGGER_OBJECT,
                                   TRUE, FALSE);

  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  long triggerUID = -1;
  if (retcode == 1)  // Already exists
  {
    if (!CreateTrigger->isReplace()) {
      *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(triggerNamePart);
      deallocEHI(ehi);
      processReturn();
      return;

    } else {
      retcode = dropTriggerFromMDAndSPSQL(cliInterface, ehi, currCatName, currSchName, CreateTrigger->getTriggerName(),
                                          CreateTrigger->ddlXns(), -1);

      if (retcode < 0) {
        deallocEHI(ehi);
        processReturn();
        return;
      }
    }
  }
  // write info to objects
  ComTdbVirtTableTableInfo *tableInfo = new (STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = NULL, tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = 0;
  tableInfo->objDataUID = 0;
  tableInfo->objOwnerID = objectOwnerID;
  tableInfo->schemaOwnerID = schemaOwnerID;
  tableInfo->isAudited = 1;
  tableInfo->validDef = 1;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->numSaltPartns = 0;
  tableInfo->rowFormat = COM_UNKNOWN_FORMAT_TYPE;
  tableInfo->objectFlags = 0;

  if (updateSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, triggerNamePart, COM_TRIGGER_OBJECT,
                           COM_NO_LIT, tableInfo, 0, NULL, 0, NULL, 0, NULL, triggerUID)) {
    processReturn();
    return;
  }

  if (triggerUID < 0) {
    processReturn();
    return;
  }

  long tableUID = -1;
  tableUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), tableNamePart.data(),
                          COM_BASE_TABLE_OBJECT_LIT);

  if (tableUID < 0) {
    processReturn();
    return;
  }

  //
  // because not exist trigger system table, so save trigger info to
  // objects, routines and text table
  //

  /**********************************************************************
   * 1. write routines table
   * triggerid(udr_uid),
   * udr_type(TR), // COM_TRIGGER_OBJECT_LIT
   * language_type("B/A"), // BEFORE | AFTER
   * deterministic_bool("Y/N"), // FOR EACH {ROW|STATEMENT},Y:STATEMENT, N:ROW
   * sql_access(S/P), //S:sql, P:procedure
   * max_results(ComOperation), //COM_UNKNOWN_IUD(0), COM_INSERT(1), COM_DELETE(2), COM_UPDATE(3), COM_SELECT(4),
   *COM_ROUTINE(5), COM_INSERT_UPDATE(6), COM_INSERT_DELETE(7), COM_INSERT_UPDATE_DELETE(8), COM_UPDATE_DELETE(9)
   * library_uid(tableid) to routines
   *************************************************************************/
  // 0:default 1:procedure 2:sql
  int subID = CreateTrigger->getBodyType();
  char *query1 = new (STMTHEAP) char[2000 + MAX_SIGNATURE_LENGTH];
  str_sprintf(query1,
              "insert into %s.\"%s\".%s values (%ld, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', "
              "'%s', '%s', '%s', '%s', %ld, '%s', 0)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_ROUTINES, triggerUID, COM_TRIGGER_OBJECT_LIT,
              CreateTrigger->isAfter() ? "A" : "B", CreateTrigger->isStatement() ? "Y" : "N", subID == 1 ? "P" : "S",
              "N",
              1 ? "Y" : "N",                 // not used isolate_bool now
              COM_STYLE_JAVA_CALL_LIT,       // not used param_style now
              COM_TRANSACTION_REQUIRED_LIT,  // not used transaction_attributes now
              (int)CreateTrigger->getIUDEvent(),
              0,                                          // not used state_area_size now
              "udr",                                      // not used external_name now
              COM_ROUTINE_NO_PARALLELISM_LIT,             // not used parallelism now
              "0",                                        // not used user_version now
              COM_ROUTINE_EXTERNAL_SECURITY_INVOKER_LIT,  // not used external_security now
              COM_ROUTINE_FAST_EXECUTION_LIT,             // not used execution_mode now
              tableUID,
              "(Ljava/lang/String;IILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILjava/"
              "lang/String;ILjava/lang/String;I)V"  // not used signature now
  );                                                // not used flags now

  int cliRC = cliInterface->executeImmediate(query1);
  NADELETEBASIC(query1, STMTHEAP);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    processReturn();
    return;
  }

  // because we used spj, so we need call create procedure after write info to metadata
  NAString sql;
  /*
   * we can get create trigger info from getNameLocList().getInputStringPtr(),
   * but not include catalog and schema, so do not use it.
   * sql.append(CreateTrigger->getNameLocList().getInputStringPtr());
   */
  if (CreateTrigger->isReplace()) {
    sql.append("CREATE OR REPLACE TRIGGER ");
  } else {
    sql.append("CREATE TRIGGER ");
  }
  sql.append(catalogNamePart);
  sql.append(".");
  sql.append(triggername.getSchemaNamePartAsAnsiString());
  sql.append(".");
  sql.append(triggername.getObjectNamePartAsAnsiString());
  // sql.append(" (");
  /*****************************************************************/
  // now not support parameter, if support this, should modify here
  /*****************************************************************/
  // sql.append(")");
  // PROCEDURE dynamic reslut sets
  // sql.append(" DYNAMIC RESULT SETS 255");
  if (CreateTrigger->isAfter()) {
    sql.append(" AFTER");
  } else {
    sql.append(" BEFORE");
  }

  if ((int)CreateTrigger->getIUDEvent() == 1) {
    sql.append(" INSERT");
  } else if ((int)CreateTrigger->getIUDEvent() == 2) {
    sql.append(" DELETE");
  } else if ((int)CreateTrigger->getIUDEvent() == 3) {
    sql.append(" UPDATE");
  } else if ((int)CreateTrigger->getIUDEvent() == 4) {
    sql.append(" SELECT");
  } else if ((int)CreateTrigger->getIUDEvent() == 5) {
    sql.append(" ROUTINE");
  } else if ((int)CreateTrigger->getIUDEvent() == 6) {
    sql.append(" INSERT OR UPDATE");
  } else if ((int)CreateTrigger->getIUDEvent() == 7) {
    sql.append(" INSERT OR DELETE");
  } else if ((int)CreateTrigger->getIUDEvent() == 8) {
    sql.append(" INSERT OR UPDATE OR DELETE");
  } else if ((int)CreateTrigger->getIUDEvent() == 9) {
    sql.append(" UPDATE OR DELETE");
  } else if ((int)CreateTrigger->getIUDEvent() == 0) {
    sql.append(" UNKNOWN");
  }

  sql.append(" ON ");
  sql.append(catalogNamePart);
  sql.append(".");
  sql.append(schemaNamePart);
  sql.append(".");
  sql.append(tableNamePart);
  sql.append(" FOR");
  if (CreateTrigger->isStatement()) {
    sql.append(" EACH STATEMENT");
  } else {
    sql.append(" EACH ROW");
  }
  sql.append(" AS ");
  sql.append(CreateTrigger->getBody()->data());
  sql.append(";");

  if (createSPSQLRoutine(cliInterface, &sql)) {
    *CmpCommon::diags() << DgSqlCode(-1231) << DgString0(triggerNamePart);
    processReturn();
    return;
  }
  // 2. write body, bodytype and triggerid to text table
  int length = sql.length();
  if (updateTextTable(cliInterface, triggerUID, COM_TRIGGER_TEXT, subID, sql, NULL, length, false)) {
    processReturn();
    return;
  }

  // hope to remove this call soon by setting the valid flag to Y sooner
  if (updateObjectValidDef(cliInterface, catalogNamePart, schemaNamePart, triggerNamePart, COM_TRIGGER_OBJECT_LIT,
                           "Y")) {
    processReturn();
    return;
  }

  CorrName cn(tableNamePart, STMTHEAP, currSchName, currCatName);
  // add trigger flag to table flag
  long flags = MD_OBJECTS_INCLUDE_TRIGGER;
  if (updateObjectFlags(cliInterface, tableUID, flags, FALSE) < 0) goto bad;
  // remove store info from text table, reset restore flag
  flags = MD_OBJECTS_STORED_DESC;
  if (updateObjectFlags(cliInterface, tableUID, flags, TRUE) < 0) goto bad;
  if (deleteFromTextTable(cliInterface, tableUID, COM_STORED_DESC_TEXT, 0) < 0) goto bad;

  // insert into "_XDC_MD_".xdc_ddl
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart)) {
    int retcode = updateSeabaseXDCDDLTable(cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                             triggerNamePart.data(), COM_TRIGGER_OBJECT_LIT);
    if (retcode < 0) goto bad;
  }

  // if default cache, update it
  if (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON) {
    insertIntoSharedCacheList(currCatName, currSchName, tableNamePart, COM_BASE_TABLE_OBJECT,
                              SharedCacheDDLInfo::DDL_DELETE);
  }
  if (!xnInProgress(cliInterface)) {
    finalizeSharedCache();
  }
  // remove this table from NATable
  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                  CreateTrigger->ddlXns(), FALSE);
  // remote trigger info from context
  TriggerDB::removeTriggers(cn.getQualifiedNameObj(), COM_INSERT);

  return;
bad:
  *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(triggerNamePart);
  processReturn();
  return;
}

int dropSPSQLTrigger(ExeCliInterface *cliInterface, const NAString &triggerName) {
  int cliRC = 0;
  const char *type = NULL;

  char query[1000];
  str_sprintf(query, "call %s.\"%s\".%s('DROP TRIGGER %s;')", TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA,
              SEABASE_SPSQL_DROP_SPJ, triggerName.data());
  cliRC = cliInterface->fetchRowsPrologue(query, TRUE /*no exec*/);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  }
  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return cliRC;
}

short CmpSeabaseDDL::dropTriggerFromMDAndSPSQL(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                               NAString &currCatName, NAString &currSchName,
                                               const NAString &triggerName, NABoolean ddlXns, long objUID) {
  ComObjectName qualifiedName(triggerName);
  NAString catalogNamePart = qualifiedName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = qualifiedName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = qualifiedName.getObjectNamePartAsAnsiString(TRUE);

  if (objUID == -1) {
    objUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                          COM_TRIGGER_OBJECT_LIT);
    if (objUID < 0) {
      return -1;
    }
  }

  // set table flags, if the table only one trigger, reset MD_OBJECTS_INCLUDE_TRIGGER flas
  // 1) get table id
  char query[1000] = {0};
  str_sprintf(query,
              "SELECT R2.LIBRARY_UID, count(R2.LIBRARY_UID) FROM %s.\"%s\".%s R1, \
                      %s.\"%s\".%s R2 where R1.UDR_UID =  %ld and R2.LIBRARY_UID = R1.LIBRARY_UID \
                      group by R2.LIBRARY_UID;",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_ROUTINES, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_ROUTINES, objUID);

  Queue *objectsInfo = NULL;
  int cliRC = cliInterface->fetchAllRows(objectsInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    return -1;
  }
  objectsInfo->position();
  OutputInfo *oi = (OutputInfo *)objectsInfo->getNext();
  long tableUID = *(long *)oi->get(0);
  int rowCount = *(int *)oi->get(1);

  // Removed routine from metadata
  if (dropSeabaseObject(ehi, triggerName, currCatName, currSchName, COM_TRIGGER_OBJECT, ddlXns, TRUE, FALSE, FALSE,
                        NAString(""))) {
    return -1;
  }

  short reCode = dropSPSQLTrigger(cliInterface, triggerName);

  if (reCode == 0 && rowCount == 1) {
    // delete trigger flag to table flag
    long flags = MD_OBJECTS_INCLUDE_TRIGGER;
    cliRC = updateObjectFlags(cliInterface, tableUID, flags, TRUE);
    if (cliRC < 0) return -1;

    flags = MD_OBJECTS_STORED_DESC;
    cliRC = updateObjectFlags(cliInterface, tableUID, flags, TRUE);
    if (cliRC < 0) return -1;
    cliRC = deleteFromTextTable(cliInterface, tableUID, COM_STORED_DESC_TEXT, 0);
    if (cliRC < 0) return -1;
  }

  return reCode;
}

void CmpSeabaseDDL::dropSeabaseTriggers(ExeCliInterface *cliInterface, StmtDDLDropTrigger *DropTrigger,
                                        NAString &currCatName, NAString &currSchName) {
  int retcode = 0;
  ComObjectName triggername(DropTrigger->getTriggerName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  triggername.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = triggername.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = triggername.getSchemaNamePartAsAnsiString(TRUE);
  const NAString triggerNamePart = triggername.getObjectNamePartAsAnsiString(TRUE);

  retcode = existsInSeabaseMDTable(cliInterface, catalogNamePart, schemaNamePart, triggerNamePart, COM_TRIGGER_OBJECT,
                                   TRUE, FALSE);

  if (retcode < 0) {
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(triggerNamePart);
    processReturn();
    return;
  }

  long triggerUID = 0;
  int objectOwnerID = 0;
  int schemaOwnerID = 0;
  long objectFlags = 0;
  long objDataUID = 0;

  // if replace trigger, need triggerUID
  triggerUID = getObjectInfo(cliInterface, catalogNamePart.data(), schemaNamePart.data(), triggerNamePart.data(),
                             COM_TRIGGER_OBJECT, objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

  if (triggerUID < 0 || objectOwnerID == 0 || schemaOwnerID == 0) {
    processReturn();
    return;
  }

  // Verify user has privilege to drop package
  if (!isDDLOperationAuthorized(SQLOperation::DROP_TRIGGER, objectOwnerID, schemaOwnerID, triggerUID,
                                COM_TRIGGER_OBJECT, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    processReturn();
    return;
  }

  // remove this table from NATable
  char query[1000];
  str_sprintf(query,
              "SELECT CATALOG_NAME, SCHEMA_NAME, OBJECT_NAME FROM  %s.\"%s\".%s WHERE OBJECT_UID=(SELECT \
              LIBRARY_UID FROM %s.\"%s\".%s WHERE UDR_UID = %ld) ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_ROUTINES, triggerUID);

  Queue *objectsInfo = NULL;
  int cliRC = cliInterface->fetchAllRows(objectsInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    processReturn();
    return;
  }
  objectsInfo->position();
  for (int i = 0; i < objectsInfo->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)objectsInfo->getNext();
    NAString CatName((char *)oi->get(0));
    NAString SchName((char *)oi->get(1));
    NAString TabName((char *)oi->get(2));

    CorrName cn(TabName, STMTHEAP, SchName, CatName);
    retcode = lockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, DropTrigger->isVolatile(),
                            DropTrigger->isExternal());
    if (retcode < 0) {
      processReturn();
      return;
    }

    if (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON) {
      insertIntoSharedCacheList(CatName, SchName, TabName, COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_DELETE);
    }
    if (!xnInProgress(cliInterface)) {
      finalizeSharedCache();
    }

    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    DropTrigger->ddlXns(), FALSE);

    // remote trigger info from context
    TriggerDB::removeTriggers(cn.getQualifiedNameObj(), COM_INSERT);
  }

  // Removed routine from metadata and SPSQL if needed
  ExpHbaseInterface *ehi = NULL;
  ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    processReturn();
    return;
  }

  if (dropTriggerFromMDAndSPSQL(cliInterface, ehi, currCatName, currSchName, DropTrigger->getTriggerName(),
                                DropTrigger->ddlXns(), triggerUID)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart)) {
    updateSeabaseXDCDDLTable(cliInterface, catalogNamePart.data(), schemaNamePart.data(), triggerNamePart.data(),
                             COM_TRIGGER_OBJECT_LIT);
  }

  deallocEHI(ehi);
  return;
}

short CmpSeabaseDDL::upgradeLibraries(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  int cliRC = 0;

  while (1)  // exit via return stmt in switch
  {
    switch (mdui->subStep()) {
      case 0: {
        mdui->setMsg("Upgrade Libraries: Started");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 1: {
        mdui->setMsg("  Start: Check For Old Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 2: {
        // check for old libraries
        if (checkForOldLibraries(cliInterface)) return -3;  // error, but no recovery needed

        mdui->setMsg("  End:   Check For Old Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 3: {
        mdui->setMsg("  Start: Rename Current Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 4: {
        // rename current libraries tables to *_OLD_LIBRARIES
        if (alterRenameLibraries(cliInterface, TRUE)) return -2;  // error, need to undo the rename only

        mdui->setMsg("  End:   Rename Current Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 5: {
        mdui->setMsg("  Start: Create New Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 6: {
        // create new libraries
        if (createLibraries(cliInterface)) return -1;  // error, need to drop new libraies then undo rename

        mdui->setMsg("  End:   Create New Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 7: {
        mdui->setMsg("  Start: Copy Old Libraries Contents ");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 8: {
        // turn on autCommit
        cliRC = autoCommit(cliInterface, TRUE);  // set autocommit ON.
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        // copy old contents into new
        cliRC = copyOldLibrariesToNew(cliInterface);
        if (cliRC < 0) {
          autoCommit(cliInterface, FALSE);  // set autocommit OFF.
          mdui->setMsg(" Copy Old Libraries failed ! ");
          return cliRC;  // error, need to drop new libraries then undo rename
        }

        // turn off autoCommit
        cliRC = autoCommit(cliInterface, FALSE);  // set autocommit OFF.
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        mdui->setMsg("  End:   Copy Old Libraries Contents ");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 9: {
        mdui->setMsg("Upgrade Libraries: Done except for cleaning up");
        mdui->setSubstep(0);
        mdui->setEndStep(TRUE);

        return 0;
      } break;

      default:
        return -1;
    }
  }  // while

  return 0;
}

short CmpSeabaseDDL::upgradeLibrariesComplete(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  switch (mdui->subStep()) {
    case 0: {
      mdui->setMsg("Upgrade Libraries: Drop old libraries");
      mdui->subStep()++;
      mdui->setEndStep(FALSE);

      return 0;
    } break;
    case 1: {
      // drop old libraries; ignore errors
      dropLibraries(cliInterface, TRUE /*old repos*/, FALSE /*no schema drop*/);

      mdui->setMsg("Upgrade Libraries: Drop Old Libraries done");
      mdui->setEndStep(TRUE);
      mdui->setSubstep(0);

      return 0;
    } break;

    default:
      return -1;
  }

  return 0;
}

short CmpSeabaseDDL::upgradeLibrariesUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  int cliRC = 0;

  while (1)  // exit via return stmt in switch
  {
    switch (mdui->subStep()) {
      // error return codes from upgradeLibraries can be mapped to
      // the right recovery substep by this formula: substep = -(retcode + 1)
      case 0:  // corresponds to -1 return code from upgradeRepos (or
               // to full recovery after some error after upgradeRepos)
      case 1:  // corresponds to -2 return code from upgradeRepos
      case 2:  // corresponds to -3 return code from upgradeRepos
      {
        mdui->setMsg("Upgrade Libraries: Restoring Old Libraries");
        mdui->setSubstep(2 * mdui->subStep() + 3);  // go to appropriate case
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 3: {
        mdui->setMsg(" Start: Drop New Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 4: {
        // drop new Libraries; ignore errors
        dropLibraries(cliInterface, FALSE /*new repos*/, TRUE /* don't drop new tables that haven't been upgraded */);
        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End: Drop New Libraries");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 5: {
        mdui->setMsg(" Start: Rename Old Libraries back to New");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 6: {
        // rename old Libraries to current; ignore errors
        alterRenameLibraries(cliInterface, FALSE);
        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End: Rename Old Libraries back to New");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 7: {
        mdui->setMsg("Upgrade Libraries: Restore done");
        mdui->setSubstep(0);
        mdui->setEndStep(TRUE);

        return 0;
      } break;

      default:
        return -1;
    }
  }  // while

  return 0;
}

short CmpSeabaseDDL::createLibraries(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  char queryBuf[20000];

  NABoolean xnWasStartedHere = FALSE;

  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &lti = allLibrariesUpgradeInfo[i];

    if (!lti.newName) continue;

    for (int j = 0; j < NUM_MAX_PARAMS; j++) {
      param_[j] = NULL;
    }

    const QString *qs = NULL;
    int sizeOfqs = 0;

    qs = lti.newDDL;
    sizeOfqs = lti.sizeOfnewDDL;

    int qryArraySize = sizeOfqs / sizeof(QString);
    char *gluedQuery;
    int gluedQuerySize;
    glueQueryFragments(qryArraySize, qs, gluedQuery, gluedQuerySize);

    param_[0] = getSystemCatalog();
    param_[1] = SEABASE_MD_SCHEMA;
    param_[2] = TRAF_RESERVED_NAMESPACE1;
    str_sprintf(queryBuf, gluedQuery, param_[0], param_[1], param_[2]);
    NADELETEBASICARRAY(gluedQuery, STMTHEAP);

    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) goto label_error;

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1390)  // table already exists
    {
      // ignore error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) goto label_error;

  }  // for

  return 0;

label_error:

  return -1;
}

short CmpSeabaseDDL::checkForOldLibraries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int bufSize = (strlen(TRAFODION_SYSCAT_LIT) * 2) + strlen(SEABASE_MD_SCHEMA) * 2 + strlen(SEABASE_OBJECTS) +
                  strlen(SEABASE_LIBRARIES_OLD) + (3 * 60);
  char queryBuf[bufSize];

  str_sprintf(queryBuf,
              "select count(*) from %s.\"%s\".%s "
              "where catalog_name = '%s' and schema_name = '%s' and "
              "object_type = 'BT' and object_name = '%s'",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES_OLD);

  int len = 0;
  long rowCount = 0;
  cliRC = cliInterface->executeImmediate(queryBuf, (char *)&rowCount, &len, FALSE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (rowCount > 0) {
    SEABASEDDL_INTERNAL_ERROR(
        "CmpSeabaseDDL::checkForOldLibraries failed, tables with the _OLD_MD suffix exist in the _MD_ SCHEMA");
    return 1;
  }
  return 0;
}

short CmpSeabaseDDL::dropLibraries(ExeCliInterface *cliInterface, NABoolean oldLibrary, NABoolean inRecovery) {
  int cliRC = 0;
  NABoolean xnWasStartedHere = FALSE;
  char queryBuf[1000];

  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &lti = allLibrariesUpgradeInfo[i];

    // If we are dropping the new repository as part of a recovery action,
    // and there is no "old" table (because the table didn't change in this
    // upgrade), then don't drop the new table. (If we did, we would be
    // dropping the existing data.)
    if (!oldLibrary && inRecovery && !lti.oldName) continue;

    if ((oldLibrary && !lti.oldName) || (NOT oldLibrary && !lti.newName)) continue;

    str_sprintf(queryBuf, "drop table %s.\"%s\".%s cascade; ", getSystemCatalog(), SEABASE_MD_SCHEMA,
                (oldLibrary ? lti.oldName : lti.newName));

    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1389)  // table doesn't exist
    {
      // ignore the error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    if (cliRC < 0) {
      return -1;
    }
  }

  return 0;
}

short CmpSeabaseMDupgrade::dropLibrariesTables(ExpHbaseInterface *ehi, NABoolean oldLibraries) {
  int retcode = 0;
  int errcode = 0;

  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &lti = allLibrariesUpgradeInfo[i];

    if ((NOT oldLibraries) && (!lti.newName)) continue;

    HbaseStr hbaseTable;
    NAString extNameForHbase = TRAFODION_SYSCAT_LIT;
    extNameForHbase += ".";
    extNameForHbase += SEABASE_MD_SCHEMA;
    extNameForHbase += ".";

    if (oldLibraries) {
      if (!lti.oldName) continue;

      extNameForHbase += lti.oldName;
    } else
      extNameForHbase += lti.newName;

    hbaseTable.val = (char *)extNameForHbase.data();
    hbaseTable.len = extNameForHbase.length();

    retcode = dropHbaseTable(ehi, &hbaseTable, FALSE, FALSE);
    if (retcode < 0) {
      errcode = -1;
    }

  }  // for

  return errcode;
}

short CmpSeabaseDDL::alterRenameLibraries(ExeCliInterface *cliInterface, NABoolean newToOld) {
  int cliRC = 0;

  char queryBuf[10000];

  NABoolean xnWasStartedHere = FALSE;

  // alter table rename cannot run inside of a transaction.
  // return an error if a xn is in progress
  if (xnInProgress(cliInterface)) {
    *CmpCommon::diags() << DgSqlCode(-20123);
    return -1;
  }

  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &lti = allLibrariesUpgradeInfo[i];

    if ((!lti.newName) || (!lti.oldName) || (NOT lti.upgradeNeeded)) continue;

    if (newToOld)
      str_sprintf(queryBuf, "alter table %s.\"%s\".%s rename to %s ; ", getSystemCatalog(), SEABASE_MD_SCHEMA,
                  lti.newName, lti.oldName);
    else
      str_sprintf(queryBuf, "alter table %s.\"%s\".%s rename to %s ; ", getSystemCatalog(), SEABASE_MD_SCHEMA,
                  lti.oldName, lti.newName);

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1389 || cliRC == -1390 || cliRC == -1127) {
      // ignore.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }
  }

  return 0;
}

short CmpSeabaseDDL::copyOldLibrariesToNew(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  NAString failedLibraries;

  char queryBuf[10000];
  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo lti = allLibrariesUpgradeInfo[i];

    if ((!lti.newName) || (!lti.oldName) || (NOT lti.upgradeNeeded)) continue;

    // Update all existing libraries  so the blob contains the library
    char *sbuf = new (STMTHEAP) char[200];
    char *ubuf = new (STMTHEAP) char[500];
    NABoolean libsToUpgrade = TRUE;
    Queue *userLibsQ = NULL;
    str_sprintf(sbuf,
                "select library_filename,library_uid from %s.\"%s\".%s"
                " for read uncommitted access",
                getSystemCatalog(), SEABASE_MD_SCHEMA, lti.oldName);
    cliRC = cliInterface->fetchAllRows(userLibsQ, sbuf, 0, FALSE, FALSE, TRUE /*no exec*/);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    str_sprintf(queryBuf, "upsert using load into %s.\"%s\".%s %s%s%s select %s from %s.\"%s\".%s SRC %s;",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, lti.newName, (lti.insertedCols ? "(" : ""),
                (lti.insertedCols ? lti.selectedCols : ""),  // insert only the original column values
                (lti.insertedCols ? ")" : ""), (lti.selectedCols ? lti.selectedCols : "*"), TRAFODION_SYSCAT_LIT,
                SEABASE_MD_SCHEMA, lti.oldName, (lti.wherePred ? lti.wherePred : ""));

    long rowCount;
    cliRC = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);

    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    // update contents into  the new library table so the libname doesn't contain the path and the
    // lib contents get loaded into the blob column.
    userLibsQ->position();
    for (size_t i = 0; i < userLibsQ->numEntries(); i++) {
      OutputInfo *userLibRow = (OutputInfo *)userLibsQ->getNext();
      char *libName = userLibRow->get(0);
      NAString libFileName(libName);
      long libuid = *(long *)userLibRow->get(1);

      size_t lastSlash = NA_NPOS;
      lastSlash = libFileName.last('/');
      NAString libNameNoPath(libName);
      if (lastSlash != NA_NPOS) libNameNoPath = libFileName(lastSlash + 1, libFileName.length() - lastSlash - 1);

      // the library UDF_LIBRARY is special and won't be converted
      if (libNameNoPath == "trafodion-sql-currversion.jar") continue;

      str_sprintf(ubuf,
                  " update %s.\"%s\".%s set library_filename = '%s', "
                  "library_storage = filetolob('%s') where library_uid = %ld",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, lti.newName, libNameNoPath.data(), libName, libuid);

      cliRC = cliInterface->executeImmediate(ubuf, NULL, NULL, TRUE, &rowCount);

      if (cliRC < 0) {
        if (failedLibraries.length() == 0) failedLibraries += "Libraries Upgrade failed for :";
        failedLibraries += libFileName;
        failedLibraries += ";";

        // If unable to convert a system library, fail operation, then we can retry
        // it again when initialize trafodion, upgrade library management is called.
#if 0
              if (libNameNoPath == "lib_mgmt.jar" || libNameNoPath == "libudr_predef.so")
                {
                  NAString msg("copyOldLibrariesToNew: Unable to upgrade system library: ");
                  msg += libName;
                  SEABASEDDL_INTERNAL_ERROR(msg.data());
                  cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
                  return -1;
                }
#endif
      }
    }  // end for
  }    // end for

  if (failedLibraries.length()) SQLMXLoggingArea::logSQLMXPredefinedEvent(failedLibraries, LL_ERROR);

  return 0;
}
