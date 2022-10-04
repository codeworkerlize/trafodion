

#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "parser/StmtDDLCreateSchema.h"
#include "parser/StmtDDLDropSchema.h"
#include "parser/StmtDDLAlterSchema.h"
#include "parser/StmtDDLGive.h"
#include "parser/StmtDDLSchGrant.h"
#include "parser/StmtDDLSchRevoke.h"
#include "parser/ElemDDLColDefault.h"
#include "common/NumericType.h"
#include "common/ComUser.h"
#include "optimizer/keycolumns.h"
#include "parser/ElemDDLColRef.h"
#include "parser/ElemDDLColName.h"
#include "parser/StmtDDLAlterSchemaHDFSCache.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "cli/Globals.h"
#include "sqlcomp/CmpMain.h"
#include "cli/Context.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrObjects.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/CmpSeabaseTenant.h"
#include "sqlcomp/SharedCache.h"
#include <vector>

static bool transferObjectPrivs(const char *systemCatalogName, const char *catalogName, const char *schemaName,
                                const int32_t newOwnerID, const char *newOwnerName);

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::addSchemaObject                                  *
// *                                                                           *
// *    Inserts a schema object row into the OBJECTS table.                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface &               In       *
// *    is a reference to an Executor CLI interface handle.                    *
// *                                                                           *
// *  <schemaName>                    const ComSchemaName &           In       *
// *    is a reference to a ComSchemaName instance.  The catalog name must be  *
// *  set.                                                                     *
// *                                                                           *
// *  <schemaClass>                   ComSchemaClass                  In       *
// *    is the class (private or shared) of the schema to be added.            *
// *                                                                           *
// *  <ownerID>                       Int32                           In       *
// *    is the authorization ID that will own the schema.                      *
// *                                                                           *
// *  <ignoreIfExists>                NABoolean                       In       *
// *    do not return an error is schema already exists                        *
// *****************************************************************************
// *                                                                           *
// * Returns: status
// *                                                                           *
// *   0: Schema was added                                                     *
// *  -1: Schema was not added.  A CLI error is put into the diags area.       *
// *   1: Schema already exists and ignoreIfExists is specified.               *
// *      No error is added to the diags area.                                 *
// *                                                                           *
// *****************************************************************************
int CmpSeabaseDDL::addSchemaObject(ExeCliInterface &cliInterface, const ComSchemaName &schemaName,
                                   ComSchemaClass schemaClass, Int32 ownerID, NABoolean ignoreIfExists,
                                   NAString namespace1, NABoolean rowIdEncrypt, NABoolean dataEncrypt,
                                   NABoolean storedDesc, NABoolean incrBackupEnabled) {
  NAString catalogNamePart = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schemaNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schemaNamePart = schemaNameAsComAnsi.getInternalName();

  ComObjectName objName(catalogNamePart, schemaNamePart, NAString(SEABASE_SCHEMA_OBJECTNAME), COM_TABLE_NAME,
                        ComAnsiNamePart::INTERNAL_FORMAT);

  if (isSeabaseReservedSchema(objName) && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_METADATA_SCHEMA_NAME)
                        << DgSchemaName(schemaName.getExternalName().data());
    return -1;
  }

  NAString objectNamePart = objName.getObjectNamePartAsAnsiString(TRUE);

  int retcode =
      existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_UNKNOWN_OBJECT, FALSE);
  if (retcode < 0) return -1;

  if (retcode == 1)  // already exists
  {
    if (ignoreIfExists)
      return 1;
    else
      *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_ALREADY_EXISTS) << DgSchemaName(schemaName.getExternalName().data());
    return -1;
  }

  char buf[4000];

  ComUID schemaUID;

  schemaUID.make_UID();

  long schemaObjectUID = schemaUID.get_value();

  long createTime = NA_JulianTimestamp();

  NAString quotedSchName;
  NAString quotedObjName;

  ToQuotedString(quotedSchName, schemaNamePart, FALSE);
  ToQuotedString(quotedObjName, NAString(SEABASE_SCHEMA_OBJECTNAME), FALSE);

  char schemaObjectLit[3] = {0};
  ComObjectType objType(COM_UNKNOWN_OBJECT);

  switch (schemaClass) {
    case COM_SCHEMA_CLASS_PRIVATE: {
      strncpy(schemaObjectLit, COM_PRIVATE_SCHEMA_OBJECT_LIT, 2);
      objType = COM_PRIVATE_SCHEMA_OBJECT;
      break;
    }
    case COM_SCHEMA_CLASS_SHARED: {
      strncpy(schemaObjectLit, COM_SHARED_SCHEMA_OBJECT_LIT, 2);
      objType = COM_SHARED_SCHEMA_OBJECT;
      break;
    }
    default:
    case COM_SCHEMA_CLASS_DEFAULT: {
      // Schemas are private by default, but could choose a different
      // default class here based on CQD or other attribute.
      strncpy(schemaObjectLit, COM_PRIVATE_SCHEMA_OBJECT_LIT, 2);
      objType = COM_PRIVATE_SCHEMA_OBJECT;
      break;
    }
  }

  if (rowIdEncrypt) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("Order preserving encryption of rowid(primary key) not yet available.");

    return -1;
  }

  long flags = 0;
  if (rowIdEncrypt) CmpSeabaseDDL::setMDflags(flags, MD_OBJECTS_ENCRYPT_ROWID_FLG);
  if (dataEncrypt) CmpSeabaseDDL::setMDflags(flags, MD_OBJECTS_ENCRYPT_DATA_FLG);
  if (storedDesc) CmpSeabaseDDL::setMDflags(flags, MD_OBJECTS_STORED_DESC);
  if (incrBackupEnabled) CmpSeabaseDDL::setMDflags(flags, MD_OBJECTS_INCR_BACKUP_ENABLED);

  str_sprintf(buf, "insert into %s.\"%s\".%s values ('%s', '%s', '%s', '%s', %ld, %ld, %ld, '%s', '%s', %d, %d, %ld)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogNamePart.data(), quotedSchName.data(),
              quotedObjName.data(), schemaObjectLit, schemaObjectUID, createTime, createTime,
              COM_YES_LIT,  // valid_def
              COM_NO_LIT,   // droppable
              ownerID, ownerID, flags);

  Int32 cliRC = cliInterface.executeImmediate(buf);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (NOT namespace1.isNull()) {
    if ((namespace1.index(TRAF_RESERVED_NAMESPACE_PREFIX) == 0) &&
        (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
      *CmpCommon::diags() << DgSqlCode(-1072) << DgString0(namespace1.data());
      return -1;
    }

    ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
    if (ehi == NULL) {
      *CmpCommon::diags() << DgSqlCode(-1067) << DgString0(namespace1.data());

      return -1;
    }

    // check if specified namespace exists
    retcode = ehi->namespaceOperation(COM_CHECK_NAMESPACE_EXISTS, namespace1.data(), 0, NULL, NULL, NULL);
    deallocEHI(ehi);
    if (retcode == -HBC_ERROR_NAMESPACE_NOT_EXIST) {
      *CmpCommon::diags() << DgSqlCode(-1067) << DgString0(namespace1.data());
      return -1;
    }

    cliRC = updateTextTable(&cliInterface, schemaObjectUID, COM_OBJECT_NAMESPACE, 0, namespace1);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  // not reserved schema
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && schemaNamePart != SEABASE_SYSTEM_SCHEMA &&
      incrBackupEnabled) {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), quotedSchName.data(), quotedObjName.data(),
                                     schemaObjectLit);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  // For now, do not store descriptors for reserved schemas
  NABoolean cqdSet = (CmpCommon::getDefault(TRAF_STORE_OBJECT_DESC) == DF_ON);
  if ((storedDesc || cqdSet) && !ComIsTrafodionReservedSchemaName(schemaNamePart)) {
    if (updateObjectRedefTime(&cliInterface, schemaName.getCatalogNamePartAsAnsiString(), schemaNamePart,
                              SEABASE_SCHEMA_OBJECTNAME, schemaObjectLit, -1, schemaUID.get_value(), TRUE,
                              FALSE))  // genStoredDesc))
      return -1;

    // Add entry to list of DDL operations
    insertIntoSharedCacheList(schemaName.getCatalogNamePartAsAnsiString(), schemaNamePart,
                              SEABASE_SCHEMA_OBJECTNAME_EXT, objType, SharedCacheDDLInfo::DDL_INSERT);
  }

  return 0;
}
//******************* End of CmpSeabaseDDL::addSchemaObject ********************

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::createSeabaseSchema                              *
// *                                                                           *
// *    Implements the CREATE SCHEMA command.                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <createSchemaNode>              StmtDDLCreateSchema  *          In       *
// *    is a pointer to a create schema parser node.                           *
// *                                                                           *
// *  <currentCatalogName>            NAString &                      In       *
// *    is the name of the current catalog.                                    *
// *                                                                           *
// *****************************************************************************
void CmpSeabaseDDL::createSeabaseSchema(StmtDDLCreateSchema *createSchemaNode, NAString &currentCatalogName)

{
  ComSchemaName schemaName(createSchemaNode->getSchemaName());

  if (schemaName.getCatalogNamePart().isEmpty()) schemaName.setCatalogNamePart(currentCatalogName);

  NAString catName = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schName = schNameAsComAnsi.getInternalName();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  ComSchemaClass schemaClass;
  Int32 objectOwner = NA_UserIdDefault;
  Int32 schemaOwner = NA_UserIdDefault;

  // If creating the hive statistics schema, make owners
  // the HIVE_ROLE_ID and skip authorization check.
  // Schema is being created as part of an update statistics cmd
  if (schName == HIVE_STATS_SCHEMA_NO_QUOTES && Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    objectOwner = HIVE_ROLE_ID;
    schemaOwner = HIVE_ROLE_ID;
  } else {
    int32_t retCode = verifyDDLCreateOperationAuthorized(&cliInterface, SQLOperation::CREATE_SCHEMA, catName, schName,
                                                         schemaClass, objectOwner, schemaOwner);
    if (retCode != 0) {
      handleDDLCreateAuthorizationError(retCode, catName, schName);
      return;
    }
  }

  Int32 schemaOwnerID = NA_UserIdDefault;

  // If the AUTHORIZATION clause was not specified, the current user becomes
  // the schema owner.

  if (createSchemaNode->getAuthorizationID().isNull())
    schemaOwnerID = ComUser::getCurrentUser();
  else {
    // If ID not found in metadata, ComDiags is populated with
    // error 8732: <auth name> is not a registered user or role
    if (ComUser::getAuthIDFromAuthName(createSchemaNode->getAuthorizationID().data(), schemaOwnerID,
                                       CmpCommon::diags()) != 0)
      return;

    if (CmpSeabaseDDLauth::isTenantID(schemaOwnerID) || CmpSeabaseDDLauth::isUserGroupID(schemaOwnerID)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID)
                          << DgString0(createSchemaNode->getAuthorizationID().data()) << DgString1("user or role");
      processReturn();
      return;
    }
  }

  // determine namespace to use.
  bool enabled = msg_license_multitenancy_enabled();
  NAString schNS;
  NAString cqdNS = CmpCommon::getDefaultString(TRAF_OBJECT_NAMESPACE);
  NAString tenantNS = enabled ? NAString(TRAF_NAMESPACE_PREFIX + ComTenant::getCurrentTenantIDAsString()) : "";

  if (createSchemaNode->getNamespace())
    schNS = *createSchemaNode->getNamespace();
  else {
    if (cqdNS.isNull())
      schNS = tenantNS;
    else {
      // validate namespace sets up schNS
      if (!ComValidateNamespace(&cqdNS, TRUE, schNS)) {
        *CmpCommon::diags() << DgSqlCode(-1065);
        return;
      }
    }
  }

  // Cannot create schema in another tenant's space, unless you have the  privilege
  if (enabled && (schNS != tenantNS) && !ComIsTrafodionReservedSchemaName(schName)) {
    if (isAuthorizationEnabled() && !ComUser::isRootUserID()) {
      NAString privMgrMDLoc;
      CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
      PrivMgrComponentPrivileges compPrivs(std::string(privMgrMDLoc.data()), CmpCommon::diags());
      if (!compPrivs.hasSQLPriv(ComUser::getCurrentUser(), SQLOperation::MANAGE_TENANTS, true)) {
        *CmpCommon::diags() << DgSqlCode(-CAT_WRONG_NAMESPACE) << DgString0(schNS.data()) << DgString1(schName.data())
                            << DgString2(tenantNS.data());
        return;
      }
    }
  }

  NABoolean encryptRowid = FALSE;
  NABoolean encryptData = FALSE;
  if (NOT createSchemaNode->useEncryption()) {
    NAString enc = CmpCommon::getDefaultString(TRAF_OBJECT_ENCRYPTION);
    if ((NOT enc.isNull()) && (NOT ComEncryption::validateEncryptionOptions(&enc, encryptRowid, encryptData))) {
      *CmpCommon::diags() << DgSqlCode(-3066)
                          << DgString0(
                                 "Invalid encryption options specified in default TRAF_OBJECT_ENCRYPTION. Valid "
                                 "options must be 'r', 'd' or a combination of them.");
      return;
    }
  } else {
    encryptRowid = createSchemaNode->encryptRowid();
    encryptData = createSchemaNode->encryptData();
  }

  if (addSchemaObject(cliInterface, schemaName, createSchemaNode->getSchemaClass(), schemaOwnerID,
                      createSchemaNode->createIfNotExists(), schNS, encryptRowid, encryptData,
                      createSchemaNode->storedDesc(), createSchemaNode->incrBackupEnabled()))
    return;

  // Create histogram tables for schema, if the schema is not volatile and
  // not reserved
  NAString tableNotCreated;

  if (!ComIsTrafodionReservedSchemaName(schName)) {
    if (createHistogramTables(&cliInterface, schemaName.getExternalName(), FALSE, tableNotCreated)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_HISTOGRAM_TABLE_NOT_CREATED) << DgTableName(tableNotCreated);

      return;
    }
  }
}
//***************** End of CmpSeabaseDDL::createSeabaseSchema ******************

// *****************************************************************************
// *
// * Function: CmpSeabaseDDL::describeSchema
// *
// *    Provides text for SHOWDDL SCHEMA comnmand.
// *
// *  Parameters:
// *
// *  <catalogName>      is a reference to a catalog name.
// *  <schemaName>       is a reference to a schema name.
// *  <checkPrivs>       perform additional privilege checks.
// *  <isHiveRegistered> it is a hive table and is registered in metadata
// *  <outlines>          passes back text for the SHOWDDL SCHEMA command
// *
// * Returns: bool
// *
// *   true: Text returned for specified schema.
// *   false: Could not retrieve information for specified schema.
// *
// *****************************************************************************
bool CmpSeabaseDDL::describeSchema(const NAString &catalogName, const NAString &schemaName, const bool checkPrivs,
                                   NABoolean isHiveRegistered, std::vector<std::string> &outlines) {
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
  CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, schemaName, catalogName);
  cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

  // See if authorization is enabled.  If so, need to list any grants of this
  // schema.
  bool displayGrants = true;
  char *sqlmxRegr = getenv("SQLMX_REGRESS");
  if (!CmpCommon::context()->isAuthorizationEnabled() ||
      ((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && sqlmxRegr) ||
      (CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF))
    displayGrants = false;

  NAString output;

  // Get the schema object, should exist
  NATable *naTable = bindWA.getNATable(cn);
  if (naTable == NULL || bindWA.errStatus()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catalogName)
                        << DgString1(schemaName);
    return false;
  }
  PrivMgrUserPrivs *privs = naTable->getPrivInfo();

  if (checkPrivs) {
    if (!privs) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return false;
    }
    if (!privs->hasSelectPriv()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return false;
    }
  }

  // Generate output text
  output = "CREATE ";
  switch (naTable->getObjectType()) {
    case COM_PRIVATE_SCHEMA_OBJECT:
      output += "PRIVATE";
      break;
    case COM_SHARED_SCHEMA_OBJECT:
      output += "SHARED";
      break;
    default:
      SEABASEDDL_INTERNAL_ERROR("Unrecognized schema type in schema");
      return false;
  }

  output += " SCHEMA ";
  output += ToAnsiIdentifier(catalogName.data());
  output += ".";
  output += ToAnsiIdentifier(schemaName.data());

  // AUTHORIZATION clause is rarely used, but include it for replay.
  char ownerName[MAX_DBUSERNAME_LEN + 1];
  int32_t length = 0;

  // just display what getAuthNameFromAuthID returns
  ComUser::getAuthNameFromAuthID(naTable->getSchemaOwner(), ownerName, sizeof(ownerName), length, TRUE,
                                 NULL /*don't update diags*/);
  output += " AUTHORIZATION ";
  output += ToAnsiIdentifier(ownerName);

  if (NOT naTable->getNamespace().isNull()) {
    output += " NAMESPACE '";
    output += naTable->getNamespace().data();
    output += "' ";
  }

  if (naTable->useEncryption()) {
    output += " USE ENCRYPTION ";
  }

  if (naTable->incrBackupEnabled()) {
    output += " INCREMENTAL BACKUP ";
  }

  if (naTable->isStoredDesc()) {
    output += " STORED DESC ";
  }

  output += ";";

  outlines.push_back(output.data());

  CmpSeabaseDDL cmpSBD(STMTHEAP);
  if (displayGrants) {
    if (privs == NULL) {
      output += "\n-- schema owner grants are unavailable";
      outlines.push_back(output.data());

      return true;
    }

    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_PRIVMGR_SCHEMA);
    PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());
    std::string privilegeText;
    PrivMgrObjectInfo objectInfo(naTable);
    if (cmpSBD.switchCompiler()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return false;
    }

    if (privInterface.describePrivileges(objectInfo, privilegeText)) {
      output = NAString(privilegeText.c_str());
      outlines.push_back(output.data());
    }
    cmpSBD.switchBackCompiler();
  }

  // Display Comment of schema
  ComTdbVirtObjCommentInfo objCommentInfo;
  if (cmpSBD.getSeabaseObjectComment(naTable->objectUid().get_value(), naTable->getObjectType(), objCommentInfo,
                                     STMTHEAP)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_COMMENTS);
    return false;
  }

  if (objCommentInfo.objectComment != NULL) {
    outlines.push_back(" ");

    output = "COMMENT ON SCHEMA ";
    output += catalogName.data();
    output += ".";
    output += schemaName.data();
    output += " IS '";
    output += objCommentInfo.objectComment;
    output += "' ;";

    outlines.push_back(output.data());
  }

  return true;
}
//******************* End of CmpSeabaseDDL::describeSchema *********************

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::dropSeabaseSchema                                *
// *                                                                           *
// *    Implements the DROP SCHEMA command.                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <dropSchemaNode>                StmtDDLDropSchema *             In       *
// *    is a pointer to a create schema parser node.                           *
// *                                                                           *
// *****************************************************************************
void CmpSeabaseDDL::dropSeabaseSchema(StmtDDLDropSchema *dropSchemaNode)

{
  int cliRC = 0;

  ComSchemaName schemaName(dropSchemaNode->getSchemaName());
  NAString catName = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schName = schNameAsComAnsi.getInternalName();
  NAString schTblName(SEABASE_SCHEMA_OBJECTNAME_EXT);
  ComObjectName objName(catName, schName, schTblName, COM_TABLE_NAME, TRUE);
  QualifiedName qn(schTblName, schName, catName);
  NABoolean isIncrBackupEnabled = false;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  ComObjectType objectType;

  bool isVolatile = (memcmp(schName.data(), "VOLATILE_SCHEMA", strlen("VOLATILE_SCHEMA")) == 0);
  int32_t length = 0;
  long rowCount = 0;
  bool someObjectsCouldNotBeDropped = false;
  char errorObjs[1010];
  Queue *objectsQueue = NULL;
  Queue *otherObjectsQueue = NULL;

  // revoke stuff
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());
  std::string revokeSchema(schName.data());

  // tenant stuff
  TenantSchemaInfoList *tenantSchemaList = NULL;

  NABoolean dirtiedMetadata = FALSE;

  errorObjs[0] = 0;

  long schemaUID = getObjectTypeandOwner(&cliInterface, catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                                          objectType, schemaOwnerID);

  // if schemaUID == -1, then either the schema does not exist or an unexpected error occurred
  if (schemaUID == -1) {
    // If an error occurred, return
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) goto label_error;

    // schema does not exist and IF EXISTS specified, then ignore and continue
    if (dropSchemaNode->dropIfExists()) goto label_error;

    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catName.data())
                        << DgString1(schName.data());
    goto label_error;
  }

  if (!isDDLOperationAuthorized(SQLOperation::DROP_SCHEMA, schemaOwnerID, schemaOwnerID, schemaUID, objectType, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    goto label_error;
  }

  if ((isSeabaseReservedSchema(objName) || (schName == SEABASE_SYSTEM_SCHEMA)) &&
      !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_USER_CANNOT_DROP_SMD_SCHEMA)
                        << DgSchemaName(schemaName.getExternalName().data());
    goto label_error;
  }

  // Can't drop a schema whose name begins with VOLATILE_SCHEMA unless the
  // keyword VOLATILE was specified in the DROP SCHEMA command.
  if (isVolatile && !dropSchemaNode->isVolatile()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_METADATA_SCHEMA_NAME) << DgTableName(schName);
    goto label_error;
  }

  // Get a list of all objects in the schema, excluding the schema object itself.
  char query[4000];

  // select objects in the schema to drop, don't return PRIMARY_KEY_CONSTRAINTS,
  // they always get removed when the parent table is dropped.
  // Filter out the LOB depenedent tables too - they will get dropped when
  // the main LOB table is dropped.
  str_sprintf(query,
              "SELECT distinct TRIM(object_name), TRIM(object_type) "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND schema_name = '%s' AND "
              "object_name <> '" SEABASE_SCHEMA_OBJECTNAME
              "' AND "
              "bitand(flags, %d) = 0 AND "
              "object_type <> 'PK' "
              "FOR READ COMMITTED ACCESS",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data(),
              MD_PARTITION_V2);

  cliRC = cliInterface.fetchAllRows(objectsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }
  // trun off binlog writing
  cliInterface.holdAndSetCQD("DONOT_WRITE_XDC_DDL", "ON");

  // Check to see if non histogram objects exist in schema, if so, then
  // cascade is required
  if (dropSchemaNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR) {
    objectsQueue->position();
    for (size_t i = 0; i < objectsQueue->numEntries(); i++) {
      OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
      NAString objName = vi->get(0);

      if (!isHistogramTable(objName)) {
        OutputInfo *oi = (OutputInfo *)objectsQueue->getCurr();

        *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_IS_NOT_EMPTY) << DgTableName(objName.data());
        goto label_error;
      }
    }
  }

  // Drop procedures (SPJs), UDFs (functions), and views
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();

    char *objName = vi->get(0);
    NAString objectTypeLit = vi->get(1);
    ComObjectType objectType = PrivMgr::ObjectLitToEnum(objectTypeLit.data());
    char buf[1000];
    NAString objectTypeString;
    NAString cascade = " ";

    switch (objectType) {
      // These object types are handled later and can be ignored for now.
      case COM_BASE_TABLE_OBJECT:
      case COM_INDEX_OBJECT:
      case COM_CHECK_CONSTRAINT_OBJECT:
      case COM_NOT_NULL_CONSTRAINT_OBJECT:
      case COM_REFERENTIAL_CONSTRAINT_OBJECT:
      case COM_SEQUENCE_GENERATOR_OBJECT:
      case COM_UNIQUE_CONSTRAINT_OBJECT:
      case COM_LIBRARY_OBJECT: {
        continue;
      }

      // If the library where procedures and functions reside is dropped
      // before its procedures and routines, then these objects may
      // not exist anymore, use the IF EXISTS to prevent the drop from
      // incurring errors.
      case COM_STORED_PROCEDURE_OBJECT: {
        objectTypeString = "PROCEDURE IF EXISTS ";
        break;
      }
      case COM_USER_DEFINED_ROUTINE_OBJECT: {
        objectTypeString = "FUNCTION IF EXISTS ";
        cascade = "CASCADE";
        break;
      }
      case COM_VIEW_OBJECT: {
        objectTypeString = "VIEW IF EXISTS ";
        cascade = "CASCADE";
        break;
      }
      case COM_PACKAGE_OBJECT: {
        objectTypeString = "PACKAGE IF EXISTS ";
        break;
      }
      case COM_TRIGGER_OBJECT: {
        objectTypeString = "TRIGGER ";
        break;
      }
      // These object types should not be seen.
      case COM_MV_OBJECT:
      case COM_MVRG_OBJECT:
      case COM_LOB_TABLE_OBJECT:
      case COM_TRIGGER_TABLE_OBJECT:
      case COM_SYNONYM_OBJECT:
      case COM_PRIVATE_SCHEMA_OBJECT:
      case COM_SHARED_SCHEMA_OBJECT:
      case COM_EXCEPTION_TABLE_OBJECT:
      case COM_LOCK_OBJECT:
      case COM_MODULE_OBJECT:
      default:
        SEABASEDDL_INTERNAL_ERROR("Unrecognized object type in schema");
        goto label_error;
    }

    dirtiedMetadata = TRUE;
    str_sprintf(buf, "drop %s \"%s\".\"%s\".\"%s\" %s", objectTypeString.data(), (char *)catName.data(),
                (char *)schName.data(), objName, cascade.data());

    cliRC = cliInterface.executeImmediate(buf);
    if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) {
      appendErrorObjName(errorObjs, objName);
      if (dropSchemaNode->ddlXns())
        goto label_error;
      else
        someObjectsCouldNotBeDropped = true;
    }
  }

  // Drop libraries in the schema
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();

    char *objName = vi->get(0);
    NAString objType = vi->get(1);

    if (objType == COM_LIBRARY_OBJECT_LIT) {
      char buf[1000];

      dirtiedMetadata = TRUE;
      str_sprintf(buf, "DROP LIBRARY \"%s\".\"%s\".\"%s\" CASCADE", (char *)catName.data(), (char *)schName.data(),
                  objName);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) {
        appendErrorObjName(errorObjs, objName);
        if (dropSchemaNode->ddlXns())
          goto label_error;
        else
          someObjectsCouldNotBeDropped = true;
      }
    }
  }

  // Drop all tables in the schema.  This will also drop any associated constraints.

  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();

    NAString objName = vi->get(0);
    NAString objType = vi->get(1);

    // drop user objects first
    if (objType == COM_BASE_TABLE_OBJECT_LIT) {
      // Histogram tables are dropped later. Sample tables
      // are dropped when their corresponding tables are dropped
      // so we don't need to drop them directly. Also,
      // avoid any tables that match LOB dependent tablenames
      // (there is no special type for these tables).
      if (!isHistogramTable(objName) && !isSampleTable(objName) && !isLOBDependentNameMatch(objName)) {
        dirtiedMetadata = TRUE;

        // Lock the next table in the schema
        BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
        CorrName cn(objName, STMTHEAP, schName, catName);

        short retcode = lockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, isVolatile, cn.isExternal());
        if (retcode < 0) {
          return;
        }

        NATable *naTable = bindWA.getNATable(cn);
        if (naTable == NULL || bindWA.errStatus()) {
          *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                              << DgString0(cn.getExposedNameAsAnsiString());
          return;
        }

        retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
        if (retcode < 0) return;

        if (dropOneTable(cliInterface, (char *)catName.data(), (char *)schName.data(), (char *)objName.data(),
                         isVolatile, FALSE, dropSchemaNode->ddlXns())) {
          appendErrorObjName(errorObjs, objName.data());
          if (dropSchemaNode->ddlXns())
            goto label_error;
          else
            someObjectsCouldNotBeDropped = true;
        }
      }
    }
  }

  // If there are any user tables having the LOB dependent name pattern, they
  // will still be around. Drop those. The real LOB dependent tables, would
  // have been dropped in the previous step

  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();

    NAString objName = vi->get(0);
    NAString objType = vi->get(1);

    if (objType == COM_BASE_TABLE_OBJECT_LIT) {
      if (!isHistogramTable(objName) && isLOBDependentNameMatch(objName)) {
        dirtiedMetadata = TRUE;
        // Pass in TRUE for "ifExists" since the lobDependent tables
        // would have already been dropped and we don't want those to
        // raise errors. We just want to catch any user tables that
        // happen to have the same name patterns.
        if (dropOneTable(cliInterface, (char *)catName.data(), (char *)schName.data(), (char *)objName.data(),
                         isVolatile, TRUE, dropSchemaNode->ddlXns())) {
          appendErrorObjName(errorObjs, objName.data());
          if (dropSchemaNode->ddlXns())
            goto label_error;
          else
            someObjectsCouldNotBeDropped = true;
        }
      }
    }
  }

  // Drop any remaining indexes.

  str_sprintf(query,
              "SELECT TRIM(object_name), TRIM(object_type) "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND "
              "      schema_name = '%s' AND "
              "      object_type = '%s' "
              "FOR READ COMMITTED ACCESS ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data(),
              COM_INDEX_OBJECT_LIT);
  cliRC = cliInterface.fetchAllRows(otherObjectsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  otherObjectsQueue->position();
  for (int idx = 0; idx < otherObjectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)otherObjectsQueue->getNext();
    char *objName = vi->get(0);
    NAString objType = vi->get(1);

    if (objType == COM_INDEX_OBJECT_LIT) {
      char buf[1000];

      dirtiedMetadata = TRUE;
      str_sprintf(buf, "DROP INDEX \"%s\".\"%s\".\"%s\" CASCADE", (char *)catName.data(), (char *)schName.data(),
                  objName);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) {
        appendErrorObjName(errorObjs, objName);
        if (dropSchemaNode->ddlXns())
          goto label_error;
        else
          someObjectsCouldNotBeDropped = true;
      }
    }
  }

  // Drop any remaining sequences.

  str_sprintf(query,
              "SELECT TRIM(object_name), TRIM(object_type) "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND "
              "      schema_name = '%s' AND "
              "      object_type = '%s' "
              "FOR READ COMMITTED ACCESS ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data(),
              COM_SEQUENCE_GENERATOR_OBJECT_LIT);

  cliRC = cliInterface.fetchAllRows(otherObjectsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  otherObjectsQueue->position();
  for (int idx = 0; idx < otherObjectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)otherObjectsQueue->getNext();
    char *objName = vi->get(0);
    NAString objType = vi->get(1);

    if (objType == COM_SEQUENCE_GENERATOR_OBJECT_LIT) {
      char buf[1000];

      dirtiedMetadata = TRUE;
      str_sprintf(buf, "DROP SEQUENCE \"%s\".\"%s\".\"%s\"", (char *)catName.data(), (char *)schName.data(), objName);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) {
        appendErrorObjName(errorObjs, objName);
        if (dropSchemaNode->ddlXns())
          goto label_error;
        else
          someObjectsCouldNotBeDropped = true;
      }
    }
  }

  // Remove privileges
  if (isAuthorizationEnabled()) {
    if (privInterface.revokeSchemaPrivilege(schemaUID, revokeSchema) == STATUS_ERROR) {
      if (dropSchemaNode->ddlXns())
        goto label_error;
      else
        someObjectsCouldNotBeDropped = true;
      goto label_error;
    }
  }

  // Drop histogram tables last
  objectsQueue->position();
  for (size_t i = 0; i < objectsQueue->numEntries(); i++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
    NAString objName = vi->get(0);

    if (isHistogramTable(objName)) {
      dirtiedMetadata = TRUE;
      if (dropOneTable(cliInterface, (char *)catName.data(), (char *)schName.data(), (char *)objName.data(), isVolatile,
                       FALSE, dropSchemaNode->ddlXns())) {
        appendErrorObjName(errorObjs, objName.data());

        if (dropSchemaNode->ddlXns())
          goto label_error;
        else
          someObjectsCouldNotBeDropped = true;
      }
    }
  }

  if (someObjectsCouldNotBeDropped) {
    NAString reason;
    reason = "Reason: Some objects could not be dropped in schema " + schName + ". ObjectsInSchema: " + errorObjs;
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_SCHEMA) << DgSchemaName(catName + "." + schName)
                        << DgString0(reason);
    goto label_error;
  }

  // For volatile schemas, sometimes only the objects get dropped.
  // If the dropObjectsOnly flag is set, just exit now, we are done.
  if (dropSchemaNode->dropObjectsOnly()) return;

  // Verify all objects in the schema have been dropped.
  str_sprintf(query,
              "SELECT COUNT(*) "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND schema_name = '%s' AND "
              "object_name <> '" SEABASE_SCHEMA_OBJECTNAME
              "'"
              "FOR READ COMMITTED ACCESS",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data());

  cliRC = cliInterface.executeImmediate(query, (char *)&rowCount, &length, FALSE);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  if (rowCount > 0) {
    // CmpCommon::diags()->clear();  don't clear diags out as that
    // sometimes obscures issues, for example, failures in dropping tables

    str_sprintf(query,
                "SELECT TRIM(object_name) "
                "FROM %s.\"%s\".%s "
                "WHERE catalog_name = '%s' AND schema_name = '%s' AND "
                "object_name <> '" SEABASE_SCHEMA_OBJECTNAME
                "' AND "
                "object_type <> 'PK' "
                "FOR READ COMMITTED ACCESS",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data());

    cliRC = cliInterface.fetchAllRows(objectsQueue, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    for (int i = 0; i < objectsQueue->numEntries(); i++) {
      OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
      NAString objName = vi->get(0);

      appendErrorObjName(errorObjs, objName.data());
    }

    NAString reason;
    reason = "Reason: schema " + schName + " is not empty. ObjectsInSchema: " + errorObjs;
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_SCHEMA) << DgSchemaName(catName + "." + schName)
                        << DgString0(reason);
    goto label_error;
  }

  if (!ComIsTrafodionReservedSchemaName(schName) && schName != SEABASE_SYSTEM_SCHEMA) {
    BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE /*inDDL*/);
    CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, schName, catName);
    cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

    NATable *naTable = bindWA.getNATable(cn);
    if (naTable) isIncrBackupEnabled = naTable->incrBackupEnabled();
  }

  // After all objects in the schema have been dropped, drop the schema object itself.

  char buf[1000];

  dirtiedMetadata = TRUE;
  str_sprintf(buf,
              "DELETE FROM %s.\"%s\".%s "
              "WHERE CATALOG_NAME = '%s' AND SCHEMA_NAME = '%s' AND "
              "OBJECT_NAME = '" SEABASE_SCHEMA_OBJECTNAME "'",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) {
    NAString reason;
    reason = "Reason: Delete of object " + NAString(SEABASE_SCHEMA_OBJECTNAME) + " failed.";
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_SCHEMA) << DgSchemaName(catName + "." + schName)
                        << DgString0(reason);
    goto label_error;
  }

  // if this schema has an associated namespace, remove it from TEXT table
  str_sprintf(buf,
              "DELETE FROM %s.\"%s\".%s "
              "WHERE TEXT_UID = %ld and TEXT_TYPE = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT, schemaUID, COM_OBJECT_NAMESPACE);
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    NAString reason;
    reason = "Reason: Delete of namespace for object " + NAString(SEABASE_SCHEMA_OBJECTNAME) + " failed.";
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_SCHEMA) << DgSchemaName(catName + "." + schName)
                        << DgString0(reason);
    goto label_error;
  }

  // If schema is part of tenant, remove tenant usage
  if (msg_license_multitenancy_enabled()) {
    tenantSchemaList = new (STMTHEAP) TenantSchemaInfoList(STMTHEAP);
    if (getTenantSchemaList(&cliInterface, tenantSchemaList) == -1) goto label_error;
    TenantSchemaInfo *tenantSchemaInfo = tenantSchemaList->find(schemaUID);
    if (tenantSchemaInfo) {
      if (tenantSchemaInfo->removeSchemaUsage() == -1) goto label_error;
    }
    NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
  }

  {
    CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, schName, getSystemCatalog());
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT, FALSE,
                                                    FALSE);
  }

  // Drop comment in TEXT table for schema
  str_sprintf(buf, "delete from %s.\"%s\".%s where text_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              schemaUID);
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }
  cliRC = cliInterface.restoreCQD("DONOT_WRITE_XDC_DDL");
  if (!ComIsTrafodionReservedSchemaName(schName) && schName != SEABASE_SYSTEM_SCHEMA && isIncrBackupEnabled) {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface, catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                                     COM_PRIVATE_SCHEMA_OBJECT_LIT);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }
  }

  // remove any objects from shared cache
  insertIntoSharedCacheList(schemaName.getCatalogNamePartAsAnsiString(), schemaName.getSchemaNamePartAsAnsiString(),
                            SEABASE_SCHEMA_OBJECTNAME_EXT, objectType, SharedCacheDDLInfo::DDL_DELETE);
  // Everything succeeded, return
  return;

label_error:

  if (tenantSchemaList) NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);

  // If metadata has not been changed, just return
  if (!dirtiedMetadata) {
    return;
  }

  // Add an error asking for user to cleanup schema
  *CmpCommon::diags() << DgSqlCode(-CAT_ATTEMPT_CLEANUP_SCHEMA) << DgSchemaName(catName + "." + schName);

  return;
}
//****************** End of CmpSeabaseDDL::dropSeabaseSchema *******************

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::alterSeabaseSchema                               *
// *                                                                           *
// *    Implements the ALTER SCHEMA command.                                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <alterSchemaNode>                StmtDDLAlterSchema *             In     *
// *    is a pointer to a create schema parser node.                           *
// *                                                                           *
// *****************************************************************************
void CmpSeabaseDDL::alterSeabaseSchema(StmtDDLAlterSchema *alterSchemaNode)

{
  int cliRC = 0;

  ComSchemaName schemaName(alterSchemaNode->getSchemaName());
  NAString catName = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schName = schNameAsComAnsi.getInternalName();
  ComObjectName objName(catName, schName, NAString("dummy"), COM_TABLE_NAME, TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  ComObjectType objectType;

  bool isVolatile = (memcmp(schName.data(), "VOLATILE_SCHEMA", strlen("VOLATILE_SCHEMA")) == 0);
  int32_t length = 0;
  long rowCount = 0;
  bool someObjectsCouldNotBeAltered = false;
  char errorObjs[1010];
  Queue *objectsQueue = NULL;
  Queue *otherObjectsQueue = NULL;

  NABoolean dirtiedMetadata = FALSE;
  Int32 checkErr = 0;

  StmtDDLAlterTableStoredDesc::AlterStoredDescType sdo = alterSchemaNode->getStoredDescOperation();

  errorObjs[0] = 0;

  long schemaUID = getObjectTypeandOwner(&cliInterface, catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                                          objectType, schemaOwnerID);

  // if schemaUID == -1, then either the schema does not exist or an unexpected error occurred
  if (schemaUID == -1) {
    // If an error occurred, return
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) goto label_error;

    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catName.data())
                        << DgString1(schName.data());
    goto label_error;
  }

  if (!isDDLOperationAuthorized(SQLOperation::ALTER_SCHEMA, schemaOwnerID, schemaOwnerID, schemaUID, objectType,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    goto label_error;
  }

  if ((isSeabaseReservedSchema(objName) || (schName == SEABASE_SYSTEM_SCHEMA)) &&
      !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_USER_CANNOT_DROP_SMD_SCHEMA)
                        << DgSchemaName(schemaName.getExternalName().data());
    goto label_error;
  }

  // Can't alter a schema whose name begins with VOLATILE_SCHEMA unless the
  // keyword VOLATILE was specified in the ALTER SCHEMA command.
  if (isVolatile && !alterSchemaNode->isVolatile()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_METADATA_SCHEMA_NAME) << DgTableName(schName);
    goto label_error;
  }

  if (alterSchemaNode->isDropAllTables()) {
    // should not reach here, is transformed to DropSchema during parsing
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0(
                               "Should not reach here. Should have been transformed to DropSchema during parsing.");
    goto label_error;
  }

  if (alterSchemaNode->isRenameSchema()) {
    // Not yet supported
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Cannot rename a schema.");
    goto label_error;
  }

  if (NOT alterSchemaNode->isAlterStoredDesc()) {
    // unsupported option
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Unsupported option specified.");
    goto label_error;
  }

  // Perform descriptor generation for the schema table;
  cliRC = alterSchemaTableDesc(&cliInterface, sdo, schemaUID, objectType, catName, schName, alterSchemaNode->ddlXns());
  if (cliRC < 0) {
    // append error and move on
    if (sdo == StmtDDLAlterTableStoredDesc::CHECK_DESC) checkErr = cliRC;
    NAString errorName = "schema-" + schName;
    appendErrorObjName(errorObjs, errorName);
    someObjectsCouldNotBeAltered = true;

    CmpCommon::diags()->clear();
  }

  // Get a list of all table objects in the schema, excluding the schema table.
  char query[4000];

  // select objects in the schema to alter
  // We do not support generate/delete stored desc for IX seperately.
  // for IX, we do not need to deal with it individually. When generate/delete stored
  // desc for BT, secondary index has already been taken into consideration.
  str_sprintf(query,
              "SELECT distinct TRIM(object_name), TRIM(object_type), object_uid "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND schema_name = '%s' AND "
              "object_name not in ('__SCHEMA__', 'SB_HISTOGRAMS',"
              "                    'SB_HISTOGRAM_INTERVALS',"
              "                    'SB_PERSISTENT_SAMPLES') AND "
              "(object_type in ('BT')) "
              "FOR READ COMMITTED ACCESS",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data());

  cliRC = cliInterface.fetchAllRows(objectsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();

    NAString tblObjName = vi->get(0);
    NAString objType = vi->get(1);
    long objUID = *(long *)vi->get(2);
    NAString tblCatName = getSystemCatalog();
    ComObjectName tblName(tblCatName, schName, tblObjName, COM_TABLE_NAME, ComAnsiNamePart::INTERNAL_FORMAT, STMTHEAP);

    if (sdo == StmtDDLAlterTableStoredDesc::GENERATE_DESC) {
      str_sprintf(query, "alter table %s generate stored desc ", tblName.getExternalName().data());
      cliRC = cliInterface.executeImmediate(query);
      if (cliRC < 0) {
        // append error and move on to next one
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;

        CmpCommon::diags()->clear();
      }
    } else if (sdo == StmtDDLAlterTableStoredDesc::GENERATE_STATS) {
      str_sprintf(query, "alter table %s generate stored stats ", tblName.getExternalName().data());
      cliRC = cliInterface.executeImmediate(query);
      if (cliRC < 0) {
        // append error and move on to next one
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;

        CmpCommon::diags()->clear();
      }
    } else if (sdo == StmtDDLAlterTableStoredDesc::DELETE_DESC) {
      str_sprintf(query, "alter table %s delete stored desc ", tblName.getExternalName().data());
      cliRC = cliInterface.executeImmediate(query);
      if (cliRC < 0) {
        // append error and move on to next one
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;

        CmpCommon::diags()->clear();
      }
    }  // delete stored desc
    else if (sdo == StmtDDLAlterTableStoredDesc::DELETE_STATS) {
      str_sprintf(query, "alter table %s delete stored stats ", tblName.getExternalName().data());
      cliRC = cliInterface.executeImmediate(query);
      if (cliRC < 0) {
        // append error and move on to next one
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;

        CmpCommon::diags()->clear();
      }
    }  // delete stored stats
#if 0
   // ENABLE and DISABLE not ready from prime time
       else if (sdo == StmtDDLAlterTableStoredDesc::ENABLE)
         {
           str_sprintf(query, "alter table %s enable stored desc ",
                       tblName.getExternalName().data());
           cliRC = cliInterface.executeImmediate(query);
           if (cliRC < 0)
             {
             {
               appendErrorObjName(errorObjs, tblObjName);
               someObjectsCouldNotBeAltered = true;  

               CmpCommon::diags()->clear();
             }    
         }
      else if (sdo == StmtDDLAlterTableStoredDesc::DISABLE)
         {
           str_sprintf(query, "alter table %s disable stored desc ",
                       tblName.getExternalName().data());
           cliRC = cliInterface.executeImmediate(query);
           if (cliRC < 0)
             {
             {
               appendErrorObjName(errorObjs, tblObjName);
               someObjectsCouldNotBeAltered = true;  

               CmpCommon::diags()->clear();
             }    
         }

#endif
    else if (sdo == StmtDDLAlterTableStoredDesc::CHECK_DESC) {
      cliRC = checkAndGetStoredObjectDesc(&cliInterface, objUID, NULL, FALSE, NULL);
      CmpCommon::diags()->clear();
      if (cliRC < 0) {
        checkErr = cliRC;
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;
      }
    } else if (sdo == StmtDDLAlterTableStoredDesc::CHECK_STATS) {
      cliRC = checkAndGetStoredObjectDesc(&cliInterface, objUID, NULL, TRUE, NULL);
      CmpCommon::diags()->clear();
      if (cliRC < 0) {
        checkErr = cliRC;
        appendErrorObjName(errorObjs, tblObjName);
        someObjectsCouldNotBeAltered = true;
      }
    }
  }  // for

  if (someObjectsCouldNotBeAltered) {
    NAString reason;
    if (sdo == StmtDDLAlterTableStoredDesc::CHECK_DESC) {
      reason = "Reason: Following objects failed stored descriptor check";
      if (checkErr == -1)
        reason += " (object could not be accessed) ";
      else if (checkErr == -2)
        reason += " (object does not exist) ";
      else if (checkErr == -3)
        reason += " (change in stored structures) ";
      reason += ": ";
      reason += errorObjs;
    } else
      reason = "Reason: Some objects could not be accessed in schema " + schName + ". ObjectsInSchema: " + errorObjs;
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_ALTER_SCHEMA) << DgSchemaName(catName + "." + schName)
                        << DgString0(reason);
    goto label_error;
  } else if (sdo == StmtDDLAlterTableStoredDesc::CHECK_DESC || sdo == StmtDDLAlterTableStoredDesc::CHECK_STATS)
    *CmpCommon::diags() << DgSqlCode(4493) << DgString0("Uptodate and current.");

  if (!ComIsTrafodionReservedSchemaName(schName)) {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface, catName.data(), schName.data(),
                                     NAString(SEABASE_SCHEMA_OBJECTNAME).data(), COM_PRIVATE_SCHEMA_OBJECT_LIT);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }
  }

  // Everything succeeded, return
  return;

label_error:
  return;
}
//****************** End of CmpSeabaseDDL::alterSeabaseSchema *****************

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::alterSchemaTableDesc                             *
// *                                                                           *
// *    Alters a schema for stored descriptor options                          *
// *                                                                           *
// *****************************************************************************
int CmpSeabaseDDL::alterSchemaTableDesc(ExeCliInterface *cliInterface,
                                          const StmtDDLAlterTableStoredDesc::AlterStoredDescType sdo,
                                          const long schUID, const ComObjectType objType, const NAString catName,
                                          const NAString schName, const NABoolean ddlXns) {
  int cliRC = 0;
  char objTypeLit[3] = {0};
  strncpy(objTypeLit, PrivMgr::ObjectEnumToLit(objType), 2);
  NABoolean removeFromCache = FALSE;
  QualifiedName qn(('"' + SEABASE_SCHEMA_OBJECTNAME + '"'), schName, catName);

  switch (sdo) {
    case StmtDDLAlterTableStoredDesc::GENERATE_DESC: {
      long flags = MD_OBJECTS_STORED_DESC;
      int rc = updateObjectFlags(cliInterface, schUID, flags, FALSE);
      cliRC = updateObjectRedefTime(cliInterface, getSystemCatalog(), schName, SEABASE_SCHEMA_OBJECTNAME, objTypeLit,
                                    -1, schUID, TRUE);
      if (cliRC < 0 || rc < 0)
        cliRC = -1;
      else
        removeFromCache = TRUE;

      insertIntoSharedCacheList(catName, schName, SEABASE_SCHEMA_OBJECTNAME_EXT, objType,
                                SharedCacheDDLInfo::DDL_INSERT);
      if (!xnInProgress(cliInterface)) finalizeSharedCache();

      break;
    }

    case StmtDDLAlterTableStoredDesc::DELETE_DESC: {
      long flags = MD_OBJECTS_DISABLE_STORED_DESC | MD_OBJECTS_STORED_DESC;
      int rc = updateObjectFlags(cliInterface, schUID, flags, TRUE);
      cliRC = deleteFromTextTable(cliInterface, schUID, COM_STORED_DESC_TEXT, 0);
      if (cliRC < 0 || rc < 0)
        cliRC = rc;
      else
        removeFromCache = TRUE;

      // In cache?
      SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();
      if (sharedDescCache) {
        insertIntoSharedCacheList(catName, schName, SEABASE_SCHEMA_OBJECTNAME_EXT, objType,
                                  SharedCacheDDLInfo::DDL_DELETE);

        if (!xnInProgress(cliInterface)) finalizeSharedCache();
      }
      break;
    }

    case StmtDDLAlterTableStoredDesc::ENABLE: {
      long flags = MD_OBJECTS_DISABLE_STORED_DESC;
      cliRC = updateObjectFlags(cliInterface, schUID, flags, TRUE);
      if (cliRC == 0) removeFromCache = TRUE;
      break;
    }

    case StmtDDLAlterTableStoredDesc::DISABLE: {
      long flags = MD_OBJECTS_DISABLE_STORED_DESC;
      cliRC = updateObjectFlags(cliInterface, schUID, flags, FALSE);
      if (cliRC == 0) removeFromCache = TRUE;
      break;
    }
    case StmtDDLAlterTableStoredDesc::CHECK_DESC: {
      cliRC = checkAndGetStoredObjectDesc(cliInterface, schUID, NULL, FALSE, NULL);
      break;
    }
    default:
      break;
  }

  // Remove NATable from cache
  if (removeFromCache) {
    CorrName cn = CorrName(NAString(SEABASE_SCHEMA_OBJECTNAME), STMTHEAP, schName, catName);
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, objType, ddlXns, FALSE);
  }

  return cliRC;
}

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::alterSchemaTableDesc                             *
// *                                                                           *
// *    Alters a schema for stored descriptor options                          *
// *    Wrapper function around alterSchemaTableDesc above                     *
// *                                                                           *
// *****************************************************************************
int CmpSeabaseDDL::alterSchemaTableDesc(ExeCliInterface *cliInterface,
                                          const StmtDDLAlterTableStoredDesc::AlterStoredDescType sdo,
                                          const NAString objName, const NABoolean ddlXns) {
  ComObjectName objNameParts(objName, COM_TABLE_NAME);
  const NAString schName = objNameParts.getSchemaNamePartAsAnsiString(TRUE);
  const NAString catName = objNameParts.getCatalogNamePartAsAnsiString(TRUE);

  ComObjectType objType;
  Int32 schOwner = 0;
  long schUID =
      getObjectTypeandOwner(cliInterface, catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME, objType, schOwner);

  // if schUID == -1, then either the schema does not exist or an unexpected error occurred
  if (schUID == -1) {
    // If an error occurred, return
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) return -1;

    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catName.data())
                        << DgString1(schName.data());
    return -1;
  }

  if (ComIsTrafodionReservedSchemaName(schName)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_METADATA_SCHEMA_NAME) << DgSchemaName(schName.data());
    return -1;
  }

  // Is locked for BR
  // if (isLockedForBR(schUID, cliInterface, schName) < 0)
  //  return -1;

  // Volatile schema check?
  bool isVolatile = (memcmp(schName.data(), "VOLATILE_SCHEMA", strlen("VOLATILE_SCHEMA")) == 0);
  if (isVolatile) {
    *CmpCommon::diags() << DgSqlCode(-CAT_REGULAR_OPERATION_ON_VOLATILE_OBJECT) << DgTableName(schName.data());
    return -1;
  }

  // Update stored descriptor for schema table
  if (alterSchemaTableDesc(cliInterface, sdo, schUID, objType, catName, schName, ddlXns) < 0)
    // return -1;
    return 0;

  return 0;
}

// *****************************************************************************
// *                                                                           *
// * Function: CmpSeabaseDDL::giveSeabaseSchema                                *
// *                                                                           *
// *    Implements the GIVE SCHEMA command.                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <giveSchemaNode>                StmtDDLGiveSchema *             In       *
// *    is a pointer to a create schema parser node.                           *
// *                                                                           *
// *  <currentCatalogName>            NAString &                      In       *
// *    is the name of the current catalog.                                    *
// *                                                                           *
// *****************************************************************************
void CmpSeabaseDDL::giveSeabaseSchema(StmtDDLGiveSchema *giveSchemaNode, NAString &currentCatalogName)

{
  ComDropBehavior dropBehavior = giveSchemaNode->getDropBehavior();
  NAString catalogName = giveSchemaNode->getCatalogName();
  NAString schemaName = giveSchemaNode->getSchemaName();

  if (catalogName.isNull()) catalogName = currentCatalogName;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  ComObjectType objectType;

  long schemaUID = getObjectTypeandOwner(&cliInterface, catalogName.data(), schemaName.data(),
                                          SEABASE_SCHEMA_OBJECTNAME, objectType, schemaOwnerID);

  if (schemaUID == -1) {
    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catalogName.data())
                        << DgString1(schemaName.data());
    return;
  }

  // *****************************************************************************
  // *                                                                           *
  // *    A schema owner can give their own schema to another authID, but they   *
  // * cannot give the objects in a shared schema to another authID.  Only       *
  // * DB__ROOT or a user with the ALTER_SCHEMA privilege can change the owners  *
  // * of objects in a shared schema.  So if the schema is private, or if only   *
  // * the schema is being given, we do standard authentication checking.  But   *
  // * if giving all the objects in a shared schema, we change the check ID to   *
  // * the default user to force the ALTER_SCHEMA privilege check.               *
  // *                                                                           *
  // *****************************************************************************

  int32_t checkID = schemaOwnerID;

  if (objectType == COM_SHARED_SCHEMA_OBJECT && dropBehavior == COM_CASCADE_DROP_BEHAVIOR) checkID = NA_UserIdDefault;

  if (!isDDLOperationAuthorized(SQLOperation::ALTER_SCHEMA, checkID, checkID, schemaUID, objectType, NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  ComObjectName objName(catalogName, schemaName, NAString("dummy"), COM_TABLE_NAME, TRUE);

  if (isSeabaseReservedSchema(objName) && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_USER_CANNOT_DROP_SMD_SCHEMA) << DgSchemaName(schemaName.data());
    return;
  }

  bool isVolatile = (memcmp(schemaName.data(), "VOLATILE_SCHEMA", strlen("VOLATILE_SCHEMA")) == 0);

  // Can't give a schema whose name begins with VOLATILE_SCHEMA.
  if (isVolatile) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_METADATA_SCHEMA_NAME) << DgTableName(schemaName);
    return;
  }

  int32_t newOwnerID = -1;

  // If ID not found in metadata, ComDiags is populated with
  // error 8732: <authID> is not a registered user or role
  if (ComUser::getAuthIDFromAuthName(giveSchemaNode->getAuthID().data(), newOwnerID, CmpCommon::diags()) != 0) return;

  if (CmpSeabaseDDLauth::isTenantID(newOwnerID) || CmpSeabaseDDLauth::isUserGroupID(newOwnerID)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(giveSchemaNode->getAuthID().data())
                        << DgString1("user or role");
    processReturn();
    return;
  }

  // *****************************************************************************
  // *                                                                           *
  // *   Drop behavior is only relevant for shared schemas.  For shared schemas, *
  // * ownership of the schema OR the schema and all its objects may be given to *
  // * another authorization ID.  For private schemas, all objects are owned by  *
  // * the schema owner, so the drop behavior is always CASCADE.                 *
  // *                                                                           *
  // * NOTE: The syntax for drop behavior always defaults to RESTRICT; for       *
  // *       private schemas this is simply ignored, as opposed to requiring     *
  // *       users to always specify CASCASE.                                    *
  // *                                                                           *
  // *****************************************************************************

  int cliRC = 0;
  char buf[4000];

  if (objectType == COM_SHARED_SCHEMA_OBJECT && dropBehavior == COM_RESTRICT_DROP_BEHAVIOR) {
    str_sprintf(buf,
                "UPDATE %s.\"%s\".%s "
                "SET object_owner = %d "
                "WHERE object_UID = %ld",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, newOwnerID, schemaUID);
    cliRC = cliInterface.executeImmediate(buf);
    if (cliRC < 0) cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

    return;
  }
  //
  // At this point, we are giving all objects in the schema (as well as the
  // schema itself) to the new authorization ID.  If authentication is enabled,
  // update the privileges first.
  //
  if (isAuthorizationEnabled()) {
    int32_t rc = transferObjectPrivs(getSystemCatalog(), catalogName.data(), schemaName.data(), newOwnerID,
                                     giveSchemaNode->getAuthID().data());
    if (rc != 0) {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0) {
        // TODO: add error
      }
      return;
    }
  }

  // Now update the object owner for all objects in the schema.

  str_sprintf(buf,
              "UPDATE %s.\"%s\".%s "
              "SET object_owner = %d "
              "WHERE catalog_name = '%s' AND schema_name = '%s'",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, newOwnerID, catalogName.data(),
              schemaName.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  // Verify all objects in the schema have been given to the new owner.
  str_sprintf(buf,
              "SELECT COUNT(*) "
              "FROM %s.\"%s\".%s "
              "WHERE catalog_name = '%s' AND schema_name = '%s' AND "
              "object_name <> '" SEABASE_SCHEMA_OBJECTNAME
              "' AND "
              "object_owner <> %d "
              "FOR READ COMMITTED ACCESS",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogName.data(), schemaName.data(),
              newOwnerID);

  int32_t length = 0;
  long rowCount = 0;

  cliRC = cliInterface.executeImmediate(buf, (char *)&rowCount, &length, FALSE);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  if (rowCount > 0) {
    SEABASEDDL_INTERNAL_ERROR("Not all objects in schema were given");
    return;
  }
}
//****************** End of CmpSeabaseDDL::giveSeabaseSchema *******************

void CmpSeabaseDDL::alterSeabaseSchemaHDFSCache(StmtDDLAlterSchemaHDFSCache *alterSchemaHdfsCache) {
  int cliRC = 0;
  int retCode = 0;
  char buf[4000];

  ComSchemaName schemaName(alterSchemaHdfsCache->schemaName().getSchemaNameAsAnsiString());
  NAString catName = schemaName.getCatalogNamePartAsAnsiString();
  ComAnsiNamePart schNameAsComAnsi = schemaName.getSchemaNamePart();
  NAString schName = schNameAsComAnsi.getInternalName();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  ComObjectType objectType;

  long schemaUID = getObjectTypeandOwner(&cliInterface, catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                                          objectType, schemaOwnerID);

  // if schemaUID == -1, then either the schema does not exist or an unexpected error occurred
  if (schemaUID == -1) {
    // If an error occurred, return
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) return;

    // A Trafodion schema does not exist if the schema object row is not
    // present: CATALOG-NAME.SCHEMA-NAME.__SCHEMA__.
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                        << DgString0(schemaName.getCatalogNamePart().getExternalName().data())
                        << DgString1(schemaName.getSchemaNamePart().getExternalName().data());
    return;
  }

  if (!isDDLOperationAuthorized(SQLOperation::ALTER_SCHEMA, schemaOwnerID, schemaOwnerID, schemaUID, objectType,
                                NULL)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
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
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName.data(), (char *)schName.data());

  cliRC = cliInterface.fetchAllRows(objectsQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  objectsQueue->position();

  TextVec tableList;
  for (int i = 0; i < objectsQueue->numEntries(); i++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
    char *ptr = vi->get(0);
    sprintf(buf, "%s.%s.%s", (char *)catName.data(), (char *)schName.data(), ptr);
    tableList.push_back(buf);
  }

  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) return;
  if (alterSchemaHdfsCache->isAddToCache()) {
    retCode = ehi->addTablesToHDFSCache(tableList, alterSchemaHdfsCache->poolName());
  } else {
    retCode = ehi->removeTablesFromHDFSCache(tableList, alterSchemaHdfsCache->poolName());
  }

  if (retCode == HBC_ERROR_POOL_NOT_EXIST_EXCEPTION) {
    *CmpCommon::diags() << DgSqlCode(-4081) << DgString0(alterSchemaHdfsCache->poolName());
  }
}

// *****************************************************************************
// *                                                                           *
// * Function: createHistogramTables                                           *
// *                                                                           *
// *    Creates all the histogram tables                                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface *               In       *
// *    is a reference to an Executor CLI interface handle.                    *
// *                                                                           *
// *  <schemaName>                    NAString &                      In       *
// *    is the catalog.schema of the histogram table to create.                *
// *                                                                           *
// *  <ignoreIfExists>                NABoolean                       In       *
// *    do not return an error if table already exists                         *
// *                                                                           *
// *  <tableNotCreeated>              NAString &                      Out      *
// *    returns the name of first histogram table that could not be created    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: Int32                                                            *
// *                                                                           *
// * -mainSQLCODE: Could not create histogram tables.                          *
// *            0: Create was successful.                                      *
// *                                                                           *
// *****************************************************************************
short CmpSeabaseDDL::createHistogramTables(ExeCliInterface *cliInterface, const NAString &schemaName,
                                           const NABoolean ignoreIfExists, NAString &tableNotCreated) {
  Int32 cliRC = 0;
  tableNotCreated = "";

  // If the caller does not send in cliInterface, instantiate one now
  ExeCliInterface *cli = NULL;
  if (cliInterface == NULL) {
    cli = new (STMTHEAP) ExeCliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  } else
    cli = cliInterface;

  // allMDHistInfo (CmpSeabaseDDLmd.h) is the list of all histogram tables,
  // MDTableInfo describes the table attributes,
  // create each table found in the list
  Int32 numHistTables = sizeof(allMDHistInfo) / sizeof(MDTableInfo);
  NAString prefixText = ignoreIfExists ? "IF NOT EXISTS " : "";
  for (Int32 i = 0; i < numHistTables; i++) {
    const MDTableInfo &mdh = allMDHistInfo[i];
    Int32 qryArraySize = mdh.sizeOfnewDDL / sizeof(QString);

    // Concatenate the create table text into a single string
    NAString concatenatedQuery;
    for (Int32 j = 0; j < qryArraySize; j++) {
      NAString tempStr = mdh.newDDL[j].str;
      concatenatedQuery += tempStr.strip(NAString::leading, ' ');
    }

    // qualify create table text with (optional) "IF NOT EXISTS" & schema name
    // and place in front of the table name:
    //     "create table <textInsertion> hist-table ..."
    std::string tableDDL(concatenatedQuery.data());
    NAString textInsertion = prefixText + schemaName + ".";
    size_t pos = tableDDL.find_first_of(mdh.newName);
    if (pos == string::npos) {
      NAString errorText("Unexpected error occurred while parsing create text for histogram table ");
      errorText += mdh.newName;
      SEABASEDDL_INTERNAL_ERROR(errorText.data());
      tableNotCreated = mdh.newName;
      return -CAT_INTERNAL_EXCEPTION_ERROR;
    }
    tableDDL = tableDDL.insert(pos, textInsertion.data());

    // Create the table
    cliRC = cli->executeImmediate(tableDDL.c_str());
    if (cliRC < 0) {
      cli->retrieveSQLDiagnostics(CmpCommon::diags());
      tableNotCreated = mdh.newName;

      if (cliInterface == NULL) delete cli;

      return cliRC;
    }
  }

  if (cliInterface == NULL) delete cli;

  return 0;
}
//************************ End of createHistogramTables ************************

// *****************************************************************************
// *                                                                           *
// * Function: adjustHiveExternalSchemas                                       *
// *                                                                           *
// *    Changes the ownership and privilege grants to DB__HIVEROLE             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface *               In       *
// *    is a reference to an Executor CLI interface handle.                    *
// *****************************************************************************
// *                                                                           *
// * Returns: Int32                                                            *
// *                                                                           *
// *            0: Adjustment was successful                                   *
// *           -1: Adjustment failed                                           *
// *                                                                           *
// *****************************************************************************
short CmpSeabaseDDL::adjustHiveExternalSchemas(ExeCliInterface *cliInterface) {
  char buf[sizeof(SEABASE_MD_SCHEMA) + sizeof(SEABASE_OBJECTS) + strlen(getSystemCatalog()) + 300];

  // get all the objects in special hive schemas
  sprintf(buf,
          "SELECT catalog_name, schema_name, object_name, object_uid, object_type, object_owner "
          " from %s.\"%s\".%s WHERE schema_name like '_HV_%c_'",
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, '%');

  Queue *objectsQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(objectsQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // adjust owner and privilege information for external hive objects
  objectsQueue->position();
  for (size_t i = 0; i < objectsQueue->numEntries(); i++) {
    OutputInfo *vi = (OutputInfo *)objectsQueue->getNext();
    NAString catName = vi->get(0);
    NAString schName = vi->get(1);
    NAString objName = vi->get(2);
    long objUID = *(long *)vi->get(3);
    NAString objectTypeLit = vi->get(4);
    Int32 objOwner = *(Int32 *)vi->get(5);
    ComObjectType objType = PrivMgr::ObjectLitToEnum(objectTypeLit.data());

    // If object owner is already the HIVE_ROLE_ID, then we are done.
    if (objOwner == HIVE_ROLE_ID)
      continue;
    else {
      // only need to adjust privileges on securable items
      if (PrivMgr::isSecurableObject(objType)) {
        ComObjectName tblName(catName, schName, objName, COM_TABLE_NAME, ComAnsiNamePart::INTERNAL_FORMAT, STMTHEAP);

        NAString extTblName = tblName.getExternalName(TRUE);

        // remove existing privs on object
        if (!deletePrivMgrInfo(extTblName, objUID, objType)) return -1;

        // add owner privs
        if (!insertPrivMgrInfo(objUID, extTblName, objType, HIVE_ROLE_ID, HIVE_ROLE_ID, ComUser::getCurrentUser()))
          return -1;
      }

      // update schema_owner and objectOwner for object
      sprintf(buf,
              "UPDATE %s.\"%s\".%s SET object_owner = %d "
              ", schema_owner = %d WHERE object_uid = %ld ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, HIVE_ROLE_ID, HIVE_ROLE_ID, objUID);
      cliRC = cliInterface->executeImmediate(buf);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }
  }

  return 0;
}
//********************* End of adjustHiveExternalTables ************************

// *****************************************************************************
// *                                                                           *
// * Function: dropOneTable                                                    *
// *                                                                           *
// *    Drops a table and all its dependent objects.                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  ExeCliInterface &               In       *
// *    is a reference to an Executor CLI interface handle.                    *
// *                                                                           *
// *  <catalogName>                   const char *                    In       *
// *    is the catalog of the table to drop.                                   *
// *                                                                           *
// *  <schemaName>                    const char *                    In       *
// *    is the schema of the table to drop.                                    *
// *                                                                           *
// *  <objectName>                    const char *                    In       *
// *    is the name of the table to drop.                                      *
// *                                                                           *
// *  <isVolatile>                    bool                            In       *
// *    is true if the object is volatile or part of a volatile schema.        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Could not drop table or one of its dependent objects.               *
// * false: Drop successful or could not set CQD for NATable cache reload.     *
// *                                                                           *
// *****************************************************************************
bool CmpSeabaseDDL::dropOneTable(ExeCliInterface &cliInterface, const char *catalogName, const char *schemaName,
                                 const char *objectName, bool isVolatile, bool ifExists, bool ddlXns)

{
  char buf[1000];

  bool someObjectsCouldNotBeDropped = false;

  char volatileString[20] = {0};
  char ifExistsString[20] = {0};
  int cliRC = 0;

  if (isVolatile && strcmp(objectName, HBASE_HIST_NAME) != 0 && strcmp(objectName, HBASE_HISTINT_NAME) != 0 &&
      strcmp(objectName, HBASE_PERS_SAMP_NAME) != 0)
    strcpy(volatileString, "VOLATILE");

  // remove from shared cache immediately
  if (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON) {
    int32_t diagsMark = CmpCommon::diags()->mark();
    insertIntoSharedCacheList(catalogName, schemaName, objectName, COM_BASE_TABLE_OBJECT,
                              SharedCacheDDLInfo::DDL_DELETE);
    finalizeSharedCache();
    CmpCommon::diags()->rewind(diagsMark);
  }

  if (ifExists) strcpy(ifExistsString, "IF EXISTS");

  if (ComIsTrafodionExternalSchemaName(schemaName))
    str_sprintf(buf, "DROP EXTERNAL TABLE \"%s\" FOR \"%s\".\"%s\".\"%s\" CASCADE", objectName, catalogName, schemaName,
                objectName);
  else
    str_sprintf(buf, "DROP %s  TABLE %s \"%s\".\"%s\".\"%s\" CASCADE", volatileString, ifExistsString, catalogName,
                schemaName, objectName);

  ULng32 savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);

  UInt32 savedCliParserFlags = 0;
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  try {
    Set_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL);
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal(CONTEXT_IN_DROP_SCHEMA);

    cliRC = cliInterface.executeImmediate(buf);
  } catch (...) {
    // Restore parser flags settings to what they originally were
    Assign_SqlParser_Flags(savedParserFlags);
    SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags);

    throw;
  }

  // Restore parser flags settings to what they originally were
  Assign_SqlParser_Flags(savedParserFlags);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  // Each error must be returned, otherwise the transaction will be broken, but the loop still executes
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

    someObjectsCouldNotBeDropped = true;
  }

  // remove NATable entry for this table
  CorrName cn(objectName, STMTHEAP, schemaName, catalogName);

  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT, ddlXns,
                                                  FALSE);

  return someObjectsCouldNotBeDropped;
}
//**************************** End of dropOneTable *****************************

// *****************************************************************************
// *                                                                           *
// * Function: transferObjectPrivs                                             *
// *                                                                           *
// *    Transfers object privs from current owner to new owner.                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <systemCatalogName>             const char *                    In       *
// *    is the location of the system catalog.                                 *
// *                                                                           *
// *  <catalogName>                   const char *                    In       *
// *    is the catalog of the schema whose objects are getting a new owner.    *
// *                                                                           *
// *  <schemaName>                    const char *                    In       *
// *    is the schema whose objects are getting a new owner.                   *
// *                                                                           *
// *  <newOwnerID>                    const int32_t                   In       *
// *    is the ID of the new owner for the objects.                            *
// *                                                                           *
// *  <newOwnerName                   const char *                    In       *
// *    is the database username or role name of the new owner for the objects.*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Privileges for object(s) transferred to new owner.                  *
// * false: Privileges for object(s) NOT transferred to new owner.             *
// *                                                                           *
// *****************************************************************************
static bool transferObjectPrivs(const char *systemCatalogName, const char *catalogName, const char *schemaName,
                                const int32_t newOwnerID, const char *newOwnerName)

{
  PrivStatus privStatus = STATUS_GOOD;

  // Initiate the privilege manager interface class
  NAString privMgrMDLoc;

  CONCAT_CATSCH(privMgrMDLoc, systemCatalogName, SEABASE_PRIVMGR_SCHEMA);

  PrivMgrCommands privInterface(std::string(privMgrMDLoc.data()), CmpCommon::diags());

  std::vector<UIDAndOwner> objectRows;
  std::string whereClause(" WHERE catalog_name = '");

  whereClause += catalogName;
  whereClause += "' AND schema_name = '";
  whereClause += schemaName;
  whereClause += "'";

  std::string orderByClause(" ORDER BY OBJECT_OWNER");
  std::string metadataLocation(systemCatalogName);

  metadataLocation += ".\"";
  metadataLocation += SEABASE_MD_SCHEMA;
  metadataLocation += "\"";

  PrivMgrObjects objects(metadataLocation, CmpCommon::diags());

  privStatus = objects.fetchUIDandOwner(whereClause, orderByClause, objectRows);

  if (privStatus != STATUS_GOOD || objectRows.size() == 0) return false;

  int32_t lastOwner = objectRows[0].ownerID;
  std::vector<int64_t> objectUIDs;

  for (size_t i = 0; i < objectRows.size(); i++) {
    if (objectRows[i].ownerID != lastOwner) {
      privStatus = privInterface.givePrivForObjects(lastOwner, newOwnerID, newOwnerName, objectUIDs);

      objectUIDs.clear();
    }
    objectUIDs.push_back(objectRows[i].UID);
    lastOwner = objectRows[i].ownerID;
  }

  privStatus = privInterface.givePrivForObjects(lastOwner, newOwnerID, newOwnerName, objectUIDs);

  return true;
}
//************************ End of transferObjectPrivs **************************

// ****************************************************************************
// method:  grantRevokeSchema
//
// Grants and revokes DML privileges for the requested schema.
// ****************************************************************************
void CmpSeabaseDDL::grantRevokeSchema(StmtDDLNode *stmtDDLNode, NABoolean isGrant, NAString &currCatName,
                                      NAString &currSchName) {
  Int32 retcode = 0;

  if (!isAuthorizationEnabled()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHORIZATION_NOT_ENABLED);
    return;
  }

  StmtDDLSchGrant *grantNode = NULL;
  StmtDDLSchRevoke *revokeNode = NULL;
  SchemaName schemaQualName;
  NAString grantedByName;
  NABoolean isGrantedBySpecified = FALSE;

  if (isGrant) {
    grantNode = stmtDDLNode->castToStmtDDLSchGrant();
    schemaQualName = grantNode->getSchemaQualName();
    isGrantedBySpecified = grantNode->isByGrantorOptionSpecified();
    grantedByName = isGrantedBySpecified ? grantNode->getByGrantor()->getAuthorizationIdentifier() : "";
  } else {
    revokeNode = stmtDDLNode->castToStmtDDLSchRevoke();
    schemaQualName = revokeNode->getSchemaQualName();
    isGrantedBySpecified = revokeNode->isByGrantorOptionSpecified();
    grantedByName = isGrantedBySpecified ? revokeNode->getByGrantor()->getAuthorizationIdentifier() : "";
  }

  NAString catalogName = schemaQualName.getCatalogName();
  NAString schemaName = schemaQualName.getSchemaName();
  ComObjectName objName(catalogName, schemaName, NAString(SEABASE_SCHEMA_OBJECTNAME), COM_TABLE_NAME, TRUE);
  NAString objectName = objName.getObjectNamePartAsAnsiString(TRUE);

  // Verify that the schema exists and determine the object_type
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  char outObjType[10];
  long objectUID = getObjectUID(&cliInterface, catalogName.data(), schemaName.data(), objectName.data(), NULL, NULL,
                                 "object_type = '" COM_PRIVATE_SCHEMA_OBJECT_LIT
                                 "' or "
                                 "object_type = '" COM_SHARED_SCHEMA_OBJECT_LIT "' ",
                                 outObjType);
  if (objectUID < 0) {
    // Remove object does not exist error since it reports the internal schema
    // name and report schema does not exist error instead
    CollIndex i = CmpCommon::diags()->returnIndex(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION);
    if (i != NULL_COLL_INDEX) CmpCommon::diags()->deleteError(i);

    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catalogName.data())
                        << DgString1(schemaName.data());
    return;
  }

  // Get schema details
  Int32 objectOwnerID;
  Int32 schemaOwnerID;
  long objectFlags;
  long objDataUID = 0;
  ComObjectType objectType = PrivMgr::ObjectLitToEnum(outObjType);

  objectUID = getObjectInfo(&cliInterface, catalogName.data(), schemaName.data(), objectName.data(), objectType,
                            objectOwnerID, schemaOwnerID, objectFlags, objDataUID);

  if (objectUID == -1 || objectOwnerID == 0) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting object UID and object owner for grant/revoke request");
    return;
  }

  // Privilege manager commands run in the metadata context so cqd's set by the
  // user are not propagated.  Determine CQD values before changing contexts.
  bool useAnsiPrivs = (CmpCommon::getDefault(CAT_ANSI_PRIVS_FOR_TENANT) == DF_ON);

  // Prepare to call privilege manager
  NAString MDLoc;
  CONCAT_CATSCH(MDLoc, getSystemCatalog(), SEABASE_MD_SCHEMA);
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrCommands command(std::string(MDLoc.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());

  if (useAnsiPrivs) command.setTenantPrivChecks(false);

  // Determine effective grantor ID and grantor name based on GRANTED BY clause
  // current user, and object owner
  Int32 effectiveGrantorID;
  std::string effectiveGrantorName;
  PrivStatus result = command.getGrantorDetailsForObject(isGrantedBySpecified, std::string(grantedByName.data()),
                                                         objectOwnerID, effectiveGrantorID, effectiveGrantorName);

  if (result != STATUS_GOOD) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting grantor ID and grantor name");
    return;
  }

  ElemDDLGranteeArray &pGranteeArray = (isGrant ? grantNode->getGranteeArray() : revokeNode->getGranteeArray());

  ElemDDLPrivActArray &privActsArray =
      (isGrant ? grantNode->getPrivilegeActionArray() : revokeNode->getPrivilegeActionArray());

  NABoolean allPrivs = (isGrant ? grantNode->isAllPrivilegesSpecified() : revokeNode->isAllPrivilegesSpecified());

  NABoolean isWGOSpecified =
      (isGrant ? grantNode->isWithGrantOptionSpecified() : revokeNode->isGrantOptionForSpecified());

  // create list of requested privs
  std::vector<PrivType> schemaPrivs;
  if (allPrivs)
    schemaPrivs.push_back(ALL_PRIVS);
  else {
    for (CollIndex k = 0; k < privActsArray.entries(); k++) {
      PrivType privType;
      if (!(privActsArray[k]->elmPrivToPrivType(privType)) || (!isDMLPrivType(privType) && !isDDLPrivType(privType))) {
        NAString schemaInfo = "schema ";
        schemaInfo += schemaName;

        *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_PRIV_FOR_OBJECT)
                            << DgString0(PrivMgrUserPrivs::convertPrivTypeToLiteral(privType).c_str())
                            << DgString1(schemaInfo.data());
        return;
      }
      schemaPrivs.push_back(privType);
    }
  }

  // For now, only support one grantee per request
  if (pGranteeArray.entries() > 1) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ONLY_ONE_GRANTEE_ALLOWED);
    return;
  }

  // convert grantee name to grantee ID
  NAString authName(pGranteeArray[0]->getAuthorizationIdentifier());
  Int32 grantee;
  if (pGranteeArray[0]->isPublic()) {
    grantee = PUBLIC_USER;
    authName = PUBLIC_AUTH_NAME;
  } else {
    // only allow grants to users, roles, and public
    // If authName not found in metadata, ComDiags is populated with
    // error 8732: <authName> is not a registered user or role
    Int16 retcode = ComUser::getAuthIDFromAuthName(authName.data(), grantee, CmpCommon::diags());
    if (retcode != 0) return;

    if (!CmpSeabaseDDLauth::isUserID(grantee) && !CmpSeabaseDDLauth::isRoleID(grantee) &&
        !ComUser::isPublicUserID(grantee)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_DOES_NOT_EXIST_ERROR) << DgString0("User or role")
                          << DgString1(authName.data());
      return;
    }
  }

  if (isGrant && isWGOSpecified && CmpSeabaseDDLauth::isRoleID(grantee) &&
      (CmpCommon::getDefault(ALLOW_WGO_FOR_ROLES) == DF_OFF)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_WGO_NOT_ALLOWED);
    processReturn();
    return;
  }

  std::string granteeName(authName.data());
  NAString extSchName = ToAnsiIdentifier(catalogName.data());
  extSchName += ".";
  extSchName += ToAnsiIdentifier(schemaName.data());

  std::string schema(schemaName.data());
  if (isGrant) {
    PrivStatus result = command.grantSchemaPrivilege(objectUID, std::string(extSchName), objectType, effectiveGrantorID,
                                                     effectiveGrantorName, grantee, granteeName, schemaOwnerID,
                                                     schemaPrivs, allPrivs, isWGOSpecified);
  } else {
    PrivStatus result = command.revokeSchemaPrivilege(objectUID, std::string(extSchName), objectType,
                                                      effectiveGrantorID, effectiveGrantorName, grantee, granteeName,
                                                      schemaOwnerID, schemaPrivs, allPrivs, isWGOSpecified);
  }

  if (result == STATUS_ERROR) return;

  // Adjust the stored descriptor
  char objectTypeLit[3] = {0};
  strncpy(objectTypeLit, PrivMgr::ObjectEnumToLit(objectType), 2);

  NABoolean genStoredDesc =
      isMDflagsSet(objectFlags, MD_OBJECTS_STORED_DESC) || (CmpCommon::getDefault(TRAF_STORE_OBJECT_DESC) == DF_ON);

  if (updateObjectRedefTime(&cliInterface, catalogName, schemaName, objectName, objectTypeLit, -1, objectUID,
                            genStoredDesc) > 0) {
    processReturn();
    return;
  }

  if (genStoredDesc) {
    insertIntoSharedCacheList(catalogName, schemaName, SEABASE_SCHEMA_OBJECTNAME_EXT, objectType,
                              SharedCacheDDLInfo::DDL_UPDATE);
  }
}
